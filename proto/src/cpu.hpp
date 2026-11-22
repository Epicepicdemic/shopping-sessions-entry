#pragma once

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>

#ifdef __linux__
#include <sched.h>
#endif
#if defined(__x86_64__)
#include <cpuid.h>
#endif

namespace shopping_cpu {

static inline bool enabled(const char* name) {
    const char* value = std::getenv(name);
    return value && std::strcmp(value, "1") == 0;
}

#if defined(__x86_64__)
struct X86Features {
    bool pclmul = false, avx2 = false, avx512 = false, v3 = false, v4 = false;
};

static inline X86Features x86_features() {
    X86Features features;
    unsigned a, b, c, d;
    constexpr unsigned os_avx = (1u << 27) | (1u << 28);
    if (!__get_cpuid(1, &a, &b, &c, &d))
        return features;
    const unsigned leaf1 = c;
    features.pclmul = leaf1 & (1u << 1);
    if ((leaf1 & os_avx) != os_avx)
        return features;
    unsigned xcr0, high;
    __asm__("xgetbv" : "=a"(xcr0), "=d"(high) : "c"(0));
    if ((xcr0 & 6) != 6 || !__get_cpuid_count(7, 0, &a, &b, &c, &d))
        return features;
    const unsigned leaf7 = b;
    features.avx2 = leaf7 & (1u << 5);
    constexpr unsigned avx512bw = (1u << 16) | (1u << 30);
    const bool zmm_state = (xcr0 & 0xe6) == 0xe6;
    features.avx512 = zmm_state && (leaf7 & avx512bw) == avx512bw;
    // The full v3 pipeline also permits FMA, MOVBE, F16C, BMI1/2, LZCNT
    // and PCLMUL. The baseline has the x86-64-v2 instruction set.
    constexpr unsigned v3_leaf1 = (1u << 1) | (1u << 12) | (1u << 22) | (1u << 29);
    constexpr unsigned v3_leaf7 = (1u << 3) | (1u << 5) | (1u << 8);
    features.v3 = (leaf1 & v3_leaf1) == v3_leaf1 && (leaf7 & v3_leaf7) == v3_leaf7 &&
        __get_cpuid(0x80000001u, &a, &b, &c, &d) && (c & (1u << 5));
    constexpr unsigned v4_leaf7 = avx512bw | (1u << 17) | (1u << 28) | (1u << 31);
    features.v4 = features.v3 && zmm_state && (leaf7 & v4_leaf7) == v4_leaf7;
    return features;
}
#endif

#ifdef __linux__
static inline bool has_controller(std::string_view controllers, std::string_view target) {
    while (!controllers.empty()) {
        const auto comma = controllers.find(',');
        if (controllers.substr(0, comma) == target)
            return true;
        if (comma == std::string_view::npos)
            break;
        controllers.remove_prefix(comma + 1);
    }
    return false;
}

static inline std::string mount_path(std::string_view escaped) {
    std::string result;
    for (size_t i = 0; i < escaped.size(); ++i) {
        if (escaped[i] == '\\' && i + 3 < escaped.size()) {
            result += char((escaped[i + 1] - '0') * 64 + (escaped[i + 2] - '0') * 8 + escaped[i + 3] - '0');
            i += 3;
        } else {
            result += escaped[i];
        }
    }
    return result;
}

static inline unsigned quota_workers(long long quota, long long period, unsigned limit) {
    if (quota > 0 && period > 0)
        return unsigned(std::min<long long>(limit, 1 + (quota - 1) / period));
    return limit;
}

static inline unsigned cgroup_workers(unsigned limit) {
    namespace fs = std::filesystem;
    std::string unified, legacy;
    {
        std::ifstream groups("/proc/self/cgroup");
        std::string line;
        while (std::getline(groups, line)) {
            const size_t first = line.find(':');
            const size_t last = line.find(':', first + 1);
            if (first == std::string::npos || last == std::string::npos)
                continue;
            const auto controllers = std::string_view(line).substr(first + 1, last - first - 1);
            if (controllers.empty() && line[0] == '0')
                unified = line.substr(last + 1);
            else if (has_controller(controllers, "cpu"))
                legacy = line.substr(last + 1);
        }
    }
    std::ifstream mounts("/proc/self/mountinfo");
    std::string line;
    while (std::getline(mounts, line)) {
        const size_t split = line.find(" - ");
        if (split == std::string::npos)
            continue;
        std::istringstream suffix(line.substr(split + 3));
        std::string type, source, options;
        suffix >> type >> source >> options;
        const bool v2 = type == "cgroup2";
        if (!v2 && (type != "cgroup" || !has_controller(options, "cpu")))
            continue;
        const std::string& group = v2 ? unified : legacy;
        if (group.empty())
            continue;
        std::istringstream prefix(line.substr(0, split));
        std::string skip, root_name, mount_name;
        prefix >> skip >> skip >> skip >> root_name >> mount_name;
        const std::string root = mount_path(root_name);
        const fs::path mount = mount_path(mount_name);
        std::string_view relative = group;
        if (root != "/" && (relative == root ||
            (relative.starts_with(root) && relative[root.size()] == '/')))
            relative.remove_prefix(root.size());
        if (relative.starts_with('/'))
            relative.remove_prefix(1);
        // A cgroup namespace can expose / at a mount whose host root is a
        // deeper path. Resolve the namespace-relative path below that mount.
        fs::path current = (mount / relative).lexically_normal();
        for (;;) {
            long long quota = -1, period = 0;
            if (v2) {
                std::ifstream(current / "cpu.max") >> quota >> period;
            } else {
                std::ifstream(current / "cpu.cfs_quota_us") >> quota;
                std::ifstream(current / "cpu.cfs_period_us") >> period;
            }
            limit = quota_workers(quota, period, limit);
            if (current == mount || current == current.root_path())
                break;
            current = current.parent_path();
        }
    }
    return limit;
}
#endif

static inline unsigned default_workers() {
#ifdef __linux__
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    const unsigned visible = sched_getaffinity(0, sizeof(cpus), &cpus) == 0
        ? unsigned(CPU_COUNT(&cpus)) : std::thread::hardware_concurrency();
    return cgroup_workers(std::clamp(visible, 1u, 64u));
#else
    return std::clamp(std::thread::hardware_concurrency(), 1u, 8u);
#endif
}

} // namespace shopping_cpu
