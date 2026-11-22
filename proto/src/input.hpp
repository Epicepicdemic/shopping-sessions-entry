#pragma once

#include "cpu.hpp"

#ifndef SHOPPING_NAMESPACE
#define SHOPPING_NAMESPACE shopping
#endif

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#if defined(__aarch64__)
#include <arm_neon.h>
#elif defined(__x86_64__)
#include <cpuid.h>
#include <immintrin.h>
#endif

namespace SHOPPING_NAMESPACE {

namespace fs = std::filesystem;
using Bytes = std::string_view;
using u8 = uint8_t;
using u16 = uint16_t;
using u32 = uint32_t;
using u64 = uint64_t;
using i64 = int64_t;

static_assert(std::endian::native == std::endian::little);

inline constexpr i64 day_ms = 86'400'000;
inline constexpr i64 week_ms = 7 * day_ms;
inline constexpr i64 gap_ms = 1'800'000;
inline constexpr i64 no_time = std::numeric_limits<i64>::min();

inline bool option(const char* name) {
    const char* value = std::getenv(name);
    return value && !std::strcmp(value, "1");
}

inline int number_option(const char* name, int fallback, int lo, int hi) {
    if (const char* value = std::getenv(name)) {
        int result = 0;
        auto end = value + std::strlen(value);
        auto parsed = std::from_chars(value, end, result);
        if (parsed.ec == std::errc{} && parsed.ptr == end && result >= lo && result <= hi)
            return result;
    }
    return fallback;
}

inline u64 clock_ns() {
    timespec t{};
    clock_gettime(CLOCK_MONOTONIC, &t);
    return u64(t.tv_sec) * 1'000'000'000 + t.tv_nsec;
}

[[noreturn]] inline void io_error(const fs::path& path) {
    throw std::system_error(errno, std::generic_category(), path.string());
}

inline i64 floor_div(i64 value, i64 divisor) {
    return value / divisor - (value % divisor < 0);
}

template <class T>
inline T load(const char* p) {
    T value;
    std::memcpy(&value, p, sizeof(value));
    return value;
}

struct ByteHash {
    using is_transparent = void;
    size_t operator()(Bytes s) const noexcept {
        return std::hash<Bytes>{}(s);
    }
};

template <class V>
using ByteMap = std::unordered_map<std::string, V, ByteHash, std::equal_to<>>;

// A fixed worker team, with an optional worker-local completion callback.
class Team {
    using Work = std::function<void(unsigned, size_t)>;
    using Finish = std::function<void(unsigned)>;

    std::mutex mutex_;
    std::condition_variable start_, done_;
    std::vector<std::thread> threads_;
    std::atomic<size_t> next_{0};
    size_t count_ = 0;
    unsigned epoch_ = 0, finished_ = 0;
    bool stopping_ = false;
    Work work_;
    Finish finish_;
    std::exception_ptr error_;

    void execute(unsigned worker) {
        try {
            for (size_t i; (i = next_.fetch_add(1, std::memory_order_relaxed)) < count_; )
                work_(worker, i);
            if (finish_)
                finish_(worker);
        } catch (...) {
            next_.store(count_, std::memory_order_relaxed);
            std::lock_guard lock(mutex_);
            if (!error_)
                error_ = std::current_exception();
        }
    }

public:
    explicit Team(unsigned count) {
        threads_.reserve(count - 1);
        try {
            for (unsigned id = 1; id < count; ++id) {
                threads_.emplace_back([this, id] {
                    unsigned last_epoch = 0;
                    while (true) {
                        std::unique_lock lock(mutex_);
                        start_.wait(lock, [&] { return stopping_ || epoch_ != last_epoch; });
                        if (stopping_)
                            return;
                        last_epoch = epoch_;
                        lock.unlock();
                        execute(id);
                        lock.lock();
                        if (++finished_ == threads_.size())
                            done_.notify_one();
                    }
                });
            }
        } catch (...) {
            stop();
            throw;
        }
    }

    ~Team() { stop(); }

    void stop() {
        {
            std::lock_guard lock(mutex_);
            stopping_ = true;
        }
        start_.notify_all();
        for (auto& thread : threads_)
            if (thread.joinable())
                thread.join();
    }

    void run(size_t count, Work work, Finish finish = {}) {
        {
            std::lock_guard lock(mutex_);
            count_ = count;
            next_.store(0, std::memory_order_relaxed);
            work_ = std::move(work);
            finish_ = std::move(finish);
            finished_ = 0;
            error_ = nullptr;
            ++epoch_;
        }
        start_.notify_all();
        execute(0);
        std::unique_lock lock(mutex_);
        done_.wait(lock, [&] { return finished_ == threads_.size(); });
        work_ = {};
        finish_ = {};
        if (error_)
            std::rethrow_exception(error_);
    }
};

enum class ReadMode { Mapped, Sequential, WillNeed, Positioned };

inline ReadMode read_mode() {
    const char* mode = std::getenv("SHOPPING_IO_MODE");
    if (!mode)
        mode = std::getenv("W4_IO");
    if (!mode)
        mode = std::getenv("W3_IO");
    if (mode) {
        if (!std::strcmp(mode, "pread"))
            return ReadMode::Positioned;
        if (!std::strcmp(mode, "sequential"))
            return ReadMode::Sequential;
        if (!std::strcmp(mode, "willneed"))
            return ReadMode::WillNeed;
        if (std::strcmp(mode, "mmap"))
            throw std::runtime_error("SHOPPING_IO_MODE must be mmap, sequential, willneed or pread");
    }
#ifdef __linux__
    return ReadMode::Mapped;
#else
    return mode ? ReadMode::Mapped : ReadMode::Positioned;
#endif
}

class ReadBuffer {
    char* bytes_ = nullptr;
    size_t capacity_ = 0;

public:
    ReadBuffer() = default;
    ReadBuffer(const ReadBuffer&) = delete;
    ReadBuffer& operator=(const ReadBuffer&) = delete;
    ~ReadBuffer() { std::free(bytes_); }

    char* data() { return bytes_; }
    char operator[](size_t i) const { return bytes_[i]; }

    void reserve(size_t length, size_t preserve = 0) {
        if (capacity_ >= length)
            return;
        constexpr size_t alignment = 16 * 1024;
        size_t rounded = (length + alignment - 1) & ~(alignment - 1);
        auto* data = static_cast<char*>(std::aligned_alloc(alignment, rounded));
        if (!data)
            throw std::bad_alloc();
        if (preserve)
            std::memcpy(data, bytes_, preserve);
        std::free(bytes_);
        bytes_ = data;
        capacity_ = rounded;
    }
};

class Input {
    int fd_ = -1;
    const char* map_ = nullptr;

public:
    fs::path path;
    size_t size;

    Input(fs::path name, ReadMode mode) : path(std::move(name)), size(fs::file_size(path)) {
        if (!size)
            return;
        fd_ = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd_ < 0)
            io_error(path);
#ifdef __linux__
        if (mode == ReadMode::Positioned)
            (void)posix_fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
        if (mode != ReadMode::Positioned) {
            void* p = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd_, 0);
            if (p == MAP_FAILED) {
                int error = errno;
                close(fd_);
                errno = error;
                io_error(path);
            }
            map_ = static_cast<const char*>(p);
            if (mode == ReadMode::Sequential)
                madvise(p, size, MADV_SEQUENTIAL);
            if (mode == ReadMode::WillNeed)
                madvise(p, size, MADV_WILLNEED);
            close(fd_);
            fd_ = -1;
        }
    }

    Input(const Input&) = delete;
    Input& operator=(const Input&) = delete;
    Input(Input&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)), map_(std::exchange(other.map_, nullptr)),
          path(std::move(other.path)), size(other.size) {}
    Input& operator=(Input&& other) noexcept {
        std::swap(fd_, other.fd_);
        std::swap(map_, other.map_);
        path.swap(other.path);
        std::swap(size, other.size);
        return *this;
    }
    ~Input() {
        if (map_)
            munmap(const_cast<char*>(map_), size);
        if (fd_ >= 0)
            close(fd_);
    }

    Bytes mapped() const { return {map_, map_ ? size : 0}; }

    void read(char* at, size_t offset, size_t length) const {
        if (map_) {
            std::memcpy(at, map_ + offset, length);
            return;
        }
        while (length) {
            ssize_t n = pread(fd_, at, length, static_cast<off_t>(offset));
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                io_error(path);
            }
            if (!n)
                throw std::runtime_error("input was truncated: " + path.string());
            at += n;
            offset += n;
            length -= n;
        }
    }

    // Padding belongs to this worker and is overwritten for the following chunk.
    Bytes chunk(size_t start, size_t end, ReadBuffer& buffer) const {
        if (map_) {
            auto bytes = mapped();
            size_t lo = line_boundary(bytes, start);
            size_t hi = line_boundary(bytes, end);
            if (!start)
                lo = after_line(bytes, 0);
            return lo < hi ? bytes.substr(lo, hi - lo) : Bytes{};
        }
        if (!size)
            return {};
        size_t offset = start ? start - 1 : 0;
        size_t filled = std::min(size, end + 256) - offset;
        buffer.reserve(filled);
        read(buffer.data(), offset, filled);
        size_t search_hi = end - offset;
        if (end < size && buffer[search_hi - 1] != '\n') {
            while (true) {
                const void* newline = std::memchr(buffer.data() + search_hi, '\n', filled - search_hi);
                if (newline || offset + filled == size)
                    break;
                search_hi = filled;
                size_t extra = std::min<size_t>(64 * 1024, size - offset - filled);
                buffer.reserve(filled + extra, filled);
                read(buffer.data() + filled, offset + filled, extra);
                filled += extra;
            }
        }
        Bytes bytes(buffer.data(), filled);
        size_t lo = start ? line_boundary(bytes, 1) : after_line(bytes, 0);
        size_t hi = line_boundary(bytes, end - offset);
        return lo < hi ? bytes.substr(lo, hi - lo) : Bytes{};
    }

private:
    static size_t after_line(Bytes bytes, size_t from) {
        if (from == bytes.size())
            return from;
        auto at = bytes.find('\n', from);
        return at == Bytes::npos ? bytes.size() : at + 1;
    }

    static size_t line_boundary(Bytes bytes, size_t at) {
        return at == 0 || at == bytes.size() || bytes[at - 1] == '\n' ? at : after_line(bytes, at);
    }
};

struct Part {
    const Input* input;
    size_t first, end;
};

struct Inputs {
    std::vector<Input> files;
    std::vector<Part> parts;
    size_t bytes = 0;

    Inputs(const fs::path& data, const char* subdir, ReadMode io, size_t chunk_size) {
        for (const auto& f : fs::directory_iterator(data / subdir))
            if (f.path().filename().string().ends_with(".csv") && f.is_regular_file())
                files.emplace_back(f.path(), io);
        std::sort(files.begin(), files.end(), [](const Input& a, const Input& b) {
            return a.size > b.size;
        });
        for (const auto& file : files) {
            bytes += file.size;
            for (size_t first = 0; first < file.size; first += chunk_size)
                parts.push_back({&file, first, std::min(first + chunk_size, file.size)});
        }
    }
};

struct Masks { u64 commas, lines, quotes; };

inline Masks scalar_masks(const char* p) {
    Masks m{};
    for (unsigned i = 0; i < 64; ++i) {
        u64 bit = u64{1} << i;
        m.commas |= bit * (p[i] == ',');
        m.lines |= bit * (p[i] == '\n');
        m.quotes |= bit * (p[i] == '"');
    }
    return m;
}

inline void classify_scalar(const char* p, Masks* output, size_t blocks) {
    for (size_t i = 0; i < blocks; ++i)
        output[i] = scalar_masks(p + 64 * i);
}

#if defined(__aarch64__)
inline void classify_neon(const char* p, Masks* output, size_t blocks) {
    const uint8x16_t weights = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
    for (size_t i = 0; i < blocks; ++i, p += 64) {
        const auto x = vld1q_u8(reinterpret_cast<const u8*>(p));
        const auto y = vld1q_u8(reinterpret_cast<const u8*>(p + 16));
        const auto z = vld1q_u8(reinterpret_cast<const u8*>(p + 32));
        const auto w = vld1q_u8(reinterpret_cast<const u8*>(p + 48));
        const auto mask = [&](uint8x16_t c) {
            auto a = vpaddq_u8(vandq_u8(vceqq_u8(x, c), weights), vandq_u8(vceqq_u8(y, c), weights));
            auto b = vpaddq_u8(vandq_u8(vceqq_u8(z, c), weights), vandq_u8(vceqq_u8(w, c), weights));
            auto c4 = vpaddq_u8(a, b);
            // The eight mask bytes occupy the low 64 bits after three pair sums.
            return vgetq_lane_u64(vreinterpretq_u64_u8(vpaddq_u8(c4, c4)), 0);
        };
        output[i] = {mask(vdupq_n_u8(',')), mask(vdupq_n_u8('\n')), mask(vdupq_n_u8('"'))};
    }
}
#elif defined(__x86_64__)
[[gnu::target("avx2")]]
inline u64 avx_mask(__m256i x, __m256i y, __m256i c) {
    return u32(_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, c))) |
        (u64(u32(_mm256_movemask_epi8(_mm256_cmpeq_epi8(y, c)))) << 32);
}

[[gnu::target("avx2")]]
inline void classify_avx2(const char* p, Masks* output, size_t blocks) {
    for (size_t i = 0; i < blocks; ++i, p += 64) {
        const auto x = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
        const auto y = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32));
        output[i] = {avx_mask(x, y, _mm256_set1_epi8(',')),
                     avx_mask(x, y, _mm256_set1_epi8('\n')),
                     avx_mask(x, y, _mm256_set1_epi8('"'))};
    }
}

[[gnu::target("avx512f,avx512bw")]]
inline void classify_avx512(const char* p, Masks* output, size_t blocks) {
    const auto comma = _mm512_set1_epi8(',');
    const auto line = _mm512_set1_epi8('\n');
    const auto quote = _mm512_set1_epi8('"');
    for (size_t i = 0; i < blocks; ++i, p += 64) {
        auto bytes = _mm512_loadu_si512(p);
        output[i] = {_mm512_cmpeq_epi8_mask(bytes, comma), _mm512_cmpeq_epi8_mask(bytes, line),
                     _mm512_cmpeq_epi8_mask(bytes, quote)};
    }
}


#endif

inline u64 parity_scalar(u64 bits) {
    bits ^= bits << 1;
    bits ^= bits << 2;
    bits ^= bits << 4;
    bits ^= bits << 8;
    bits ^= bits << 16;
    return bits ^ (bits << 32);
}

#if defined(__ARM_FEATURE_AES)
inline u64 parity_neon(u64 bits) {
    return u64(vmull_p64(poly64_t(bits), poly64_t(~u64{0})));
}
#elif defined(__x86_64__)
[[gnu::target("pclmul")]]
inline u64 parity_x86(u64 bits) {
    auto prefix = _mm_clmulepi64_si128(_mm_cvtsi64_si128(bits), _mm_set1_epi32(-1), 0);
    return u64(_mm_cvtsi128_si64(prefix));
}
#endif

// All quotes must be next to a field boundary and each field must end
// with even quote parity. A suspect batch uses the checked row parser.
template <u64 (*Parity)(u64)>
[[gnu::always_inline]]
inline bool quote_boundaries(const Masks* masks, size_t blocks) {
    u64 previous = 1, carry = 0, bad = 0;
    u64 delimiters = masks[0].commas | masks[0].lines;
    for (size_t i = 0; i < blocks; ++i) {
        const u64 next = masks[i + 1].commas | masks[i + 1].lines;
        const u64 quotes = masks[i].quotes;
        const u64 neighbors = previous | (delimiters << 1) | (delimiters >> 1) | (next << 63);
        const u64 inside = Parity(quotes) ^ carry;
        bad |= (quotes & ~neighbors) | (inside & delimiters);
        previous = delimiters >> 63;
        carry = u64{0} - (inside >> 63);
        delimiters = next;
    }
    return !bad;
}

#if defined(__x86_64__)
[[gnu::target("pclmul")]]
inline bool quote_boundaries_x86(const Masks* masks, size_t blocks) {
    return quote_boundaries<parity_x86>(masks, blocks);
}
#endif

using Classify = void(*)(const char*, Masks*, size_t);
using QuoteCheck = bool(*)(const Masks*, size_t);
struct Scanner {
    Classify classify;
    QuoteCheck quotes;
    const char* name;
};

inline Scanner choose_scanner() {
    if (option("SHOPPING_SCALAR") || option("W4_SCALAR") || option("W3_SCALAR"))
        return {classify_scalar, quote_boundaries<parity_scalar>, "scalar"};
#if defined(__aarch64__)
#if defined(__ARM_FEATURE_AES)
    return {classify_neon, quote_boundaries<parity_neon>, "neon"};
#else
    return {classify_neon, quote_boundaries<parity_scalar>, "neon"};
#endif
#elif defined(__x86_64__)
    const auto features = shopping_cpu::x86_features();
    const auto quotes = features.pclmul ? quote_boundaries_x86 : quote_boundaries<parity_scalar>;
    if (features.avx512 && !option("SHOPPING_AVX2") && !option("W4_AVX2"))
        return {classify_avx512, quotes, "avx512bw"};
    if (features.avx2)
        return {classify_avx2, quotes, "avx2"};
#endif
    return {classify_scalar, quote_boundaries<parity_scalar>, "scalar"};
}

template <size_t N>
bool scalar_fields(Bytes line, std::array<Bytes, N>& fields) {
    for (size_t i = 0; i < N; ++i) {
        size_t comma = line.find(',');
        if ((i + 1 == N) != (comma == Bytes::npos))
            return false;
        auto f = line.substr(0, comma);
        if (auto quote = f.find('"'); quote != Bytes::npos) {
            if (quote != 0 || f.size() < 2 || f.back() != '"' || f.find('"', 1) != f.size() - 1)
                return false;
            f = f.substr(1, f.size() - 2);
        }
        fields[i] = f;
        if (i + 1 != N)
            line.remove_prefix(comma + 1);
    }
    return true;
}

using Bits = __uint128_t;

template <u64 Masks::*Member>
inline Bits row_mask(const Masks* blocks, size_t from, size_t length) {
    const Masks* p = blocks + from / 64;
    unsigned skip = from % 64;
    Bits low = Bits(p[0].*Member) | (Bits(p[1].*Member) << 64);
    Bits bits = low >> skip;
    if (skip + length > 128)
        bits |= (Bits(p[2].*Member) << (128 - skip));
    return bits & ((Bits{1} << length) - 1);
}

template <bool CheckedBatch, bool Last = false>
[[gnu::always_inline]]
inline Bytes field(const char* line, unsigned start, unsigned end, unsigned length, bool& bad) {
    unsigned width = end - start;
    if constexpr (CheckedBatch) {
        // A checked batch ends in a newline, so even the start of its
        // empty final field is readable.
        unsigned strip = line[start] == '"';
        return {line + start + strip, width - 2 * strip};
    }
    unsigned q1 = (line[start - (start == length)] == '"') & (width != 0);
    unsigned q2 = (line[end - (end != 0)] == '"') & (width != 0);
    bad |= (q1 != q2) | ((width == 1) & q1);
    unsigned strip = q1 & (width >= 2);
    return {line + start + strip, width - 2 * strip};
}

template <bool CheckedBatch, size_t I, size_t N, class BitsType>
[[gnu::always_inline]]
inline void field_ranges(const char* line, BitsType commas, unsigned start, unsigned length,
                         std::array<Bytes, N>& fields, bool& bad) {
    unsigned end = length;
    if constexpr (I + 1 < N) {
        if constexpr (sizeof(BitsType) == 8) {
            end = std::countr_zero(commas);
        } else {
            u64 low = u64(commas);
            end = low ? std::countr_zero(low) : (64 + std::countr_zero(u64(commas >> 64)));
        }
    }
    fields[I] = field<CheckedBatch, I + 1 == N>(line, start, end, length, bad);
    if constexpr (I + 1 < N)
        field_ranges<CheckedBatch, I + 1>(line, commas & (commas - 1), end + 1, length, fields, bad);
}

template <bool CheckedBatch, size_t N>
[[gnu::always_inline]]
inline bool split_fields(const char* line, Bits commas, unsigned width, std::array<Bytes, N>& out, bool bad) {
    if constexpr (N == 7) {
        // The leading ID/time trio and the trailing ingest/type/product trio
        // normally each fit a single machine word. Verify that geometry first.
        u64 leading = u64(commas);
        unsigned a = std::countr_zero(leading);
        leading &= leading - 1;
        unsigned b = std::countr_zero(leading);
        leading &= leading - 1;
        unsigned c = std::countr_zero(leading);
        if (c < 64 && width - c <= 65) {
            u64 tail = u64(commas >> (c + 1));
            unsigned d = c + 1 + std::countr_zero(tail);
            tail &= tail - 1;
            unsigned e = c + 1 + std::countr_zero(tail);
            tail &= tail - 1;
            if (!std::has_single_bit(tail))
                return false;
            unsigned f = c + 1 + std::countr_zero(tail);
            out[0] = field<CheckedBatch>(line, 0, a, width, bad);
            out[1] = field<CheckedBatch>(line, a + 1, b, width, bad);
            out[2] = field<CheckedBatch>(line, b + 1, c, width, bad);
            out[3] = field<CheckedBatch>(line, c + 1, d, width, bad);
            out[4] = field<CheckedBatch>(line, d + 1, e, width, bad);
            out[5] = field<CheckedBatch>(line, e + 1, f, width, bad);
            out[6] = field<CheckedBatch, true>(line, f + 1, width, width, bad);
            return !bad;
        }
    }
    if (std::popcount(u64(commas)) + std::popcount(u64(commas >> 64)) != N - 1)
        return false;
    if (width <= 64) {
        field_ranges<CheckedBatch, 0>(line, u64(commas), 0, width, out, bad);
    } else {
        field_ranges<CheckedBatch, 0>(line, commas, 0, width, out, bad);
    }
    return !bad;
}

[[gnu::always_inline]] inline u64 commas_at(const Masks* masks, size_t start) {
    const Masks* p = masks + start / 64;
    return u64((Bits(p[0].commas) | (Bits(p[1].commas) << 64)) >> (start % 64));
}

[[gnu::always_inline]] inline bool split_checked_event(const char* line, const Masks* masks,
                                                       size_t start, unsigned width,
                                                       std::array<Bytes, 7>& fields) {
    u64 head = commas_at(masks, start);
    const unsigned a = std::countr_zero(head);
    head &= head - 1;
    const unsigned b = std::countr_zero(head);
    head &= head - 1;
    const unsigned c = std::countr_zero(head);
    if (c >= 64 || c >= width || width - c > 64) [[unlikely]]
        return split_fields<true>(line, row_mask<&Masks::commas>(masks, start, width), width, fields, false);

    u64 tail = commas_at(masks, start + c + 1) & ((u64{1} << (width - c - 1)) - 1);
    const unsigned d = c + 1 + std::countr_zero(tail);
    tail &= tail - 1;
    const unsigned e = c + 1 + std::countr_zero(tail);
    tail &= tail - 1;
    if (!std::has_single_bit(tail)) [[unlikely]]
        return false;
    const unsigned f = c + 1 + std::countr_zero(tail);
    bool bad = false;
    fields[0] = field<true>(line, 0, a, width, bad);
    fields[1] = field<true>(line, a + 1, b, width, bad);
    fields[2] = field<true>(line, b + 1, c, width, bad);
    fields[3] = field<true>(line, c + 1, d, width, bad);
    fields[4] = field<true>(line, d + 1, e, width, bad);
    fields[5] = field<true>(line, e + 1, f, width, bad);
    fields[6] = field<true, true>(line, f + 1, width, width, bad);
    return true;
}

template <bool CheckedBatch, size_t N, class Emit>
[[gnu::always_inline]] inline void scan_batch(const char* data, size_t length, const Masks* masks,
                                            size_t blocks, Emit& emit) {
    size_t start = 0;
    auto row = [&](size_t end) {
        size_t width = end - start;
        std::array<Bytes, N> fields;
        if (width && width < 128) {
            if constexpr (CheckedBatch && N == 7) {
                if (split_checked_event(data + start, masks, start, unsigned(width), fields))
                    emit(fields);
            } else {
                Bits commas = row_mask<&Masks::commas>(masks, start, width);
                bool bad = false;
                if constexpr (!CheckedBatch) {
                    Bits quotes = row_mask<&Masks::quotes>(masks, start, width);
                    Bits allowed = (commas << 1) | (commas >> 1) | 1 | (Bits{1} << (width - 1));
                    bad = (quotes & ~allowed) != 0;
                }
                if (split_fields<CheckedBatch>(data + start, commas, unsigned(width), fields, bad))
                    emit(fields);
            }
        } else if (width && scalar_fields<N>(Bytes(data + start, width), fields)) {
            emit(fields);
        }
        start = end + 1;
    };
    for (size_t block = 0; block < blocks; ++block)
        for (u64 lines = masks[block].lines; lines; lines &= lines - 1)
            row(block * 64 + std::countr_zero(lines));
    if (start < length)
        row(length);
}

template <size_t N, class Emit>
void scan_rows(Bytes bytes, Scanner scanner, Emit emit) {
    constexpr size_t batch = 8 * 1024;
    std::array<Masks, batch / 64 + 3> masks;
    while (!bytes.empty()) {
        size_t length = bytes.size();
        if (length > batch) {
            auto boundary = bytes.rfind('\n', batch - 1);
            if (boundary == Bytes::npos) {
                auto finish = bytes.find('\n', batch);
                auto n = finish == Bytes::npos ? length : finish;
                std::array<Bytes, N> fields;
                if (scalar_fields<N>(bytes.substr(0, n), fields))
                    emit(fields);
                bytes.remove_prefix(n + (n != length));
                continue;
            }
            length = boundary + 1;
        }
        const char* data = bytes.data();
        size_t blocks = length / 64;
        scanner.classify(data, masks.data(), blocks);
        alignas(64) char last[64]{};
        std::memcpy(last, data + blocks * 64, length % 64);
        scanner.classify(last, &masks[blocks], 1);
        masks[blocks + 1] = {};
        masks[blocks + 2] = {};
        blocks += length % 64 != 0;
        if (scanner.quotes && data[length - 1] == '\n' && scanner.quotes(masks.data(), blocks)) {
            scan_batch<true, N>(data, length, masks.data(), blocks, emit);
        } else {
            scan_batch<false, N>(data, length, masks.data(), blocks, emit);
        }
        bytes.remove_prefix(length);
    }
}

inline constexpr u64 byte_repeat(u8 byte) { return 0x0101010101010101ull * byte; }

inline bool valid_digits(u64 bytes) {
    const u64 wrong_zone = (bytes & byte_repeat(0xf0)) ^ byte_repeat('0');
    const u64 out_of_range = ((bytes & byte_repeat(0x0f)) + byte_repeat(6)) & byte_repeat(0x10);
    return (wrong_zone | out_of_range) == 0;
}

inline u32 four_digits(u32 bytes) {
    const u32 digits = bytes & 0x0f0f0f0f;
    const u32 pairs = (digits & 0x00ff00ff) * 10 + ((digits >> 8) & 0x00ff00ff);
    return (pairs & 255) * 100 + (pairs >> 16);
}

inline u64 digit_pairs(u64 bytes, u64 mask) {
    return (bytes & mask) * 10 + ((bytes >> 8) & mask);
}

inline bool ascii_shape(u64 data, u64 digits, u64 shape) {
    const u64 diff = data ^ shape;
    const u64 nondigits = diff & ~(digits & byte_repeat(15));
    const u64 out_of_range = (diff + (digits & byte_repeat(6))) & (digits & byte_repeat(16));
    return (nondigits | out_of_range) == 0;
}

inline u32 eight_digits(u64 bytes) {
    u64 d = bytes & byte_repeat(0x0f);
    u64 pair = (d & 0x00ff00ff00ff00ffull) * 10 + ((d >> 8) & 0x00ff00ff00ff00ffull);
    u64 quad = (pair & 0x0000ffff0000ffffull) * 100 + ((pair >> 16) & 0x0000ffff0000ffffull);
    return u32(quad) * 10'000 + u32(quad >> 32);
}

inline u64 decimal(Bytes bytes, u64 maximum) {
    if (bytes.empty())
        return maximum + 1;
    u64 value = 0;
    for (unsigned char c : bytes) {
        unsigned digit = c - unsigned('0');
        if (digit > 9 || value > maximum / 10 || (value == maximum / 10 && digit > maximum % 10))
            return maximum + 1;
        value = value * 10 + digit;
    }
    return value;
}

inline u64 short_decimal(Bytes bytes, u64 maximum) {
    const size_t n = bytes.size();
    if (n < 4 || n > 8)
        return decimal(bytes, maximum);
    if (n == 4) {
        const u32 word = load<u32>(bytes.data());
        return valid_digits(u64(word) | 0x3030303000000000ull) ? four_digits(word) : maximum + 1;
    }
    const u64 word = load<u32>(bytes.data()) | (u64(load<u32>(bytes.data() + n - 4)) << (8 * (n - 4)));
    const unsigned shift = unsigned(8 * (8 - n));
    const u64 padded = (word << shift) | (byte_repeat('0') & ((u64{1} << shift) - 1));
    return valid_digits(padded) ? eight_digits(padded) : maximum + 1;
}

inline i64 civil_day(int year, unsigned month, unsigned day) {
    year -= month <= 2;
    int era = int(floor_div(year, 400));
    unsigned y = unsigned(year - era * 400);
    unsigned m = month > 2 ? month - 3 : month + 9;
    return era * 146097 + int(y * 365 + y / 4 - y / 100 + (153 * m + 2) / 5 + day - 1) - 719468;
}

inline unsigned month_days(int year, unsigned month) {
    constexpr u8 days[]{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return days[month] + (month == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
}

inline int two_digits(const char* p) {
    unsigned a = u8(p[0]) - unsigned('0'), b = u8(p[1]) - unsigned('0');
    return a <= 9 && b <= 9 ? int(10 * a + b) : -1;
}

inline i64 timestamp(Bytes ts) {
    size_t n = ts.size();
    if (n == 13) {
        u64 a = load<u64>(ts.data());
        u64 b = load<u64>(ts.data() + 5);
        if (valid_digits(a) && valid_digits(b))
            return i64(eight_digits(a)) * 100'000 + (four_digits(u32(b >> 24)) * 10 + u32(b >> 56 & 15));
    }
    if (n == 20 || n == 25) {
        const char* p = ts.data();
        int century = two_digits(p), year_part = two_digits(p + 2);
        int month = two_digits(p + 5), day = two_digits(p + 8);
        int hour = two_digits(p + 11), minute = two_digits(p + 14), second = two_digits(p + 17);
        int year = century * 100 + year_part;
        bool valid = p[4] == '-' && p[7] == '-' && p[10] == 'T' && p[13] == ':' && p[16] == ':'
            && century >= 0 && year_part >= 0 && month >= 1 && month <= 12 && day >= 1
            && unsigned(day) <= month_days(year, unsigned(month))
            && unsigned(hour) <= 23 && unsigned(minute) <= 59 && unsigned(second) <= 59;
        if (valid) {
            int offset = 0;
            if (n == 20 && p[19] == 'Z')
                return civil_day(year, month, day) * day_ms + (3600 * hour + 60 * minute + second) * 1000;
            if (n == 25 && (p[19] == '+' || p[19] == '-') && p[22] == ':') {
                int oh = two_digits(p + 20), om = two_digits(p + 23);
                if (unsigned(oh) <= 23 && unsigned(om) <= 59) {
                    offset = (60 * oh + om) * (p[19] == '+' ? 60 : -60);
                    return civil_day(year, month, day) * day_ms + (3600 * hour + 60 * minute + second - offset) * 1000;
                }
            }
        }
    }
    constexpr auto max = u64(std::numeric_limits<i64>::max());
    u64 value = decimal(ts, max);
    return value > max ? no_time : i64(value);
}

// Cache the manifest year and its neighbors to cover lookback and offsets.
// Each cell verifies the complete ten-byte date before yielding its epoch.
struct Dates {
    struct Day {
        u64 suffix = 0;
        u16 prefix = 0;
        int32_t seconds = 0;
    };
    std::array<Day, 4 * 16 * 32> days{};
    i64 origin;

    static unsigned day_index(u64 date) {
        const u64 md = digit_pairs(date, 0x000f00000f000000ull);
        const unsigned year = ((u8(date) * 2 + u8(date >> 8)) & 3);
        return (year << 9) | (unsigned((md >> 24) & 15) << 5) | unsigned((md >> 48) & 31);
    }

    explicit Dates(i64 start) : origin(floor_div(start, 1000) * 1000 - week_ms) {
        i64 d = floor_div(start, day_ms);
        if (d < civil_day(0, 1, 1) || d >= civil_day(10000, 1, 1))
            return;
        int low = 0, high = 10000;
        while (low + 1 < high) {
            int m = (low + high) / 2;
            (d < civil_day(m, 1, 1) ? high : low) = m;
        }
        for (int year = std::max(0, low - 1); year <= std::min(9999, low + 1); ++year) {
            char date[10] = {'0', '0', '0', '0', '-', '0', '0', '-', '0', '0'};
            int value = year;
            for (int i = 3; i >= 0; --i) {
                date[i] = char('0' + value % 10);
                value /= 10;
            }
            const u16 prefix = load<u16>(date);
            for (unsigned m = 1; m <= 12; ++m) {
                date[5] = char('0' + m / 10);
                date[6] = char('0' + m % 10);
                for (unsigned day = 1; day <= month_days(year, m); ++day) {
                    date[8] = char('0' + day / 10);
                    date[9] = char('0' + day % 10);
                    const u64 suffix = load<u64>(date + 2);
                    days[day_index(suffix)] = {suffix, prefix,
                        int32_t(civil_day(year, m, day) * 86400 - origin / 1000)};
                }
            }
        }
    }

    struct IsoClock { int32_t seconds; bool valid; };

    static u64 clock_pairs(u64 clock) {
        return digit_pairs(clock, 0x000f00000f00000full) & 0x00ff0000ff0000ffull;
    }

    static int clock_seconds(u64 pairs) {
        return u8(pairs) * 3600 + u8(pairs >> 24) * 60 + u8(pairs >> 48);
    }

    [[gnu::always_inline]] inline IsoClock iso_clock(const char* p) const {
        const u64 date = load<u64>(p + 2);
        const auto& day = days[day_index(date)];
        const u64 clock = load<u64>(p + 11);
        constexpr u64 clock_digits = 0xffff00ffff00ffffull;
        constexpr u64 clock_shape = (clock_digits & byte_repeat('0')) | 0x00003a00003a0000ull;
        const u64 hms = clock_pairs(clock);
        // In each byte, adding 128 minus the exclusive limit sets bit 7
        // for hours >= 24 or minutes/seconds >= 60.
        const bool limits = !((hms + 0x0044000044000068ull) & 0x0080000080000080ull);
        const bool valid = (date == day.suffix) & (load<u16>(p) == day.prefix) & (day.prefix != 0) &
            (p[10] == 'T') & ascii_shape(clock, clock_digits, clock_shape) & limits;
        const int32_t seconds = day.seconds + clock_seconds(hms);
        return {seconds, valid};
    }

    [[gnu::always_inline]] inline i64 iso_seconds(const char* p, size_t length) const {
        const auto [seconds, valid] = iso_clock(p);
        if (!valid) [[unlikely]]
            return no_time;
        if (length == 20)
            return p[19] == 'Z' ? seconds : no_time;
        if ((p[19] != '+' && p[19] != '-') || p[22] != ':')
            return no_time;
        int oh = two_digits(p + 20), om = two_digits(p + 23);
        if (unsigned(oh) > 23 || unsigned(om) > 59)
            return no_time;
        int offset = (oh * 60 + om) * 60;
        return p[19] == '+' ? seconds - offset : seconds + offset;
    }

    i64 ingest_seconds(Bytes value) const {
        if (value.size() == 20) {
            // The kit guarantees valid UTC F1 ingests. A checked day lookup
            // lets the common path skip timestamp-format validation.
            const char* p = value.data();
            const u64 date = load<u64>(p + 2);
            const auto& day = days[day_index(date)];
            if ((date == day.suffix) & (load<u16>(p) == day.prefix) & (p[19] == 'Z')) [[likely]]
                return day.seconds + clock_seconds(clock_pairs(load<u64>(p + 11)));
        }
        i64 t = timestamp(value);
        return t == no_time ? t : t / 1000 - origin / 1000;
    }

    i64 millis(Bytes value) const {
        if (value.size() == 13) {
            u64 a = load<u64>(value.data());
            u64 b = load<u64>(value.data() + 5);
            if (valid_digits(a) & valid_digits(b))
                return i64(eight_digits(a)) * 100'000 + (four_digits(u32(b >> 24)) * 10 + u32(b >> 56 & 15)) - origin;
        } else if ((value.size() == 20) | (value.size() == 25)) {
            const i64 hit = iso_seconds(value.data(), value.size());
            if (hit != no_time) [[likely]]
                return hit * 1000;
        }
        i64 t = timestamp(value);
        if (t == no_time || (origin < 0 && t > std::numeric_limits<i64>::max() + origin) ||
            (origin > 0 && t < std::numeric_limits<i64>::min() + origin))
            return no_time;
        return t - origin;
    }
};

} // namespace SHOPPING_NAMESPACE
