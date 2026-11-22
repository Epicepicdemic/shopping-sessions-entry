#pragma once
#include "input.hpp"

#include <fstream>
#include <memory>
#include <numeric>

namespace SHOPPING_NAMESPACE {

inline constexpr u32 no_token = std::numeric_limits<u32>::max();
template <char Prefix, bool Underscore>
struct Index {
    u32 missing;
    std::vector<u32> dense;
    ByteMap<u32> other;

    explicit Index(u32 absent) : missing(absent) {}

    u32 position(Bytes name) const {
        constexpr unsigned prefix = Underscore ? 2 : 1;
        if (name.size() <= prefix || name[0] != Prefix)
            return no_token;
        if constexpr (Underscore)
            if (name[1] != '_')
                return no_token;
        name.remove_prefix(prefix);
        if ((name.size() > 1 && name[0] == '0') || name.size() > 6)
            return no_token;
        u64 value = short_decimal(name, 131071);
        return value <= 131071 ? u32(value) : no_token;
    }

    void add(Bytes key, u32 index) {
        u32 n = position(key);
        if (n == no_token) {
            other.emplace(key, index);
            return;
        }
        if (dense.size() <= n)
            dense.resize(n + 1, missing);
        dense[n] = index;
    }

    inline u32 lookup(Bytes key) const {
        u32 n = position(key);
        if (n != no_token)
            return n < dense.size() ? dense[n] : missing;
        if (other.empty())
            return missing;
        auto at = other.find(key);
        return at != other.end() ? at->second : missing;
    }
};

struct Catalog {
    i64 begin, end;
    std::vector<std::string> categories;
    std::vector<std::pair<std::string, std::string>> campaigns;
    Index<'p', false> products{0};
    Index<'c', true> ads{no_token};

    static std::string json_string(Bytes json, size_t& at) {
        std::string result;
        for (++at; at < json.size(); ++at) {
            char c = json[at];
            if (c == '"') {
                ++at;
                return result;
            }
            if (c == '\\' && ++at < json.size()) {
                c = json[at];
                if (c == 'u' && at + 4 < json.size()) {
                    unsigned value = 0;
                    for (unsigned stride = 1; stride <= 4; ++stride) {
                        u8 hex = json[at + stride];
                        unsigned digit = hex >= '0' && hex <= '9' ? hex - '0' : (hex | 32) - 'a' + 10;
                        if (digit > 15)
                            throw std::runtime_error("invalid JSON escape");
                        value = (value << 4) | digit;
                    }
                    at += 4;
                    // Manifest keys and time forms are ASCII. Other codepoints cannot match them.
                    c = value < 128 ? char(value) : '\x7f';
                } else if (c != '"' && c != '/' && c != '\\') {
                    c = '\x7f';
                }
            }
            result.push_back(c);
        }
        throw std::runtime_error("unterminated manifest string");
    }

    Catalog(const fs::path& data, Scanner scanner) {
        std::ifstream manifest(data / "manifest.json");
        if (!manifest)
            throw std::runtime_error("cannot open manifest.json");
        std::string json((std::istreambuf_iterator<char>(manifest)), {});
        begin = end = no_time;
        for (size_t at = json.find('"'); at != Bytes::npos; at = json.find('"', at)) {
            std::string key = json_string(json, at);
            at = json.find_first_not_of(" \t\r\n", at);
            if (at == Bytes::npos || json[at] != ':')
                continue;
            at = json.find_first_not_of(" \t\r\n", at + 1);
            if (at == Bytes::npos || json[at] != '"')
                continue;
            auto value = json_string(json, at);
            if (key == "window_start")
                begin = timestamp(value);
            if (key == "window_end")
                end = timestamp(value);
        }
        if (begin == no_time || end == no_time)
            throw std::runtime_error("manifest requires valid window_start and window_end");

        Input product_file(data / "products.csv", ReadMode::Mapped);
        Input campaign_file(data / "campaigns.csv", ReadMode::Mapped);
        ReadBuffer ignored;
        std::vector<Bytes> discovered{"unknown"};
        products.dense.reserve(50'001);
        std::unordered_map<Bytes, u32, ByteHash> observed{{"unknown", 0}};
        scan_rows<2>(product_file.chunk(0, product_file.size, ignored), scanner, [&](const auto& row) {
            const auto [category, added] = observed.try_emplace(row[1], u32(discovered.size()));
            if (added)
                discovered.push_back(row[1]);
            products.add(row[0], category->second);
        });
        std::vector<u32> order(discovered.size()), remap(discovered.size());
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](u32 a, u32 b) { return discovered[a] < discovered[b]; });
        categories.reserve(order.size());
        for (u32 from : order) {
            remap[from] = u32(categories.size());
            categories.emplace_back(discovered[from]);
        }
        products.missing = remap[0];
        for (u32& category : products.dense)
            category = remap[category];
        for (auto& product : products.other)
            product.second = remap[product.second];

        campaigns.emplace_back("organic", "organic");
        ads.dense.reserve(2001);
        scan_rows<2>(campaign_file.chunk(0, campaign_file.size, ignored), scanner, [&](const auto& row) {
            u32 id = u32(campaigns.size());
            ads.add(row[0], id);
            campaigns.emplace_back(row[0], row[1]);
        });
    }
};

struct Buy {
    u32 cents = 0, category = no_token;
};

inline Buy read_purchase(const Catalog& catalog, Bytes product, Bytes price) {
    u64 value = short_decimal(price, 10'000'000);
    if (!value || value > 10'000'000)
        return {};
    return {u32(value), catalog.products.lookup(product)};
}

inline bool view_or_cart(Bytes kind) {
    constexpr u32 view = 0x77656976, cart = 0x74726163;
    const u32 word = load<u32>(kind.data());
    return (word == view) | (word == cart);
}

} // namespace SHOPPING_NAMESPACE
