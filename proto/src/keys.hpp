#pragma once
#include "input.hpp"

#include <memory>

namespace SHOPPING_NAMESPACE {

inline u64 fold_multiply(u64 a, u64 b) {
    __uint128_t p = __uint128_t(a) * b;
    return u64(p) ^ u64(p >> 64);
}

inline constexpr u64 mix_a = 0x9e3779b97f4a7c15ULL;
inline constexpr u64 mix_b = 0xd6e8feb86659fd93ULL;

inline u64 literal_hash(u64 bytes) {
    return fold_multiply(bytes ^ mix_a, mix_b);
}

inline u64 subword(const char* p, size_t n) {
    if (n >= 4)
        return u64(load<u32>(p)) | (u64(load<u32>(p + n - 4)) << (8 * (n - 4)));
    if (n >= 2)
        return u64(load<u16>(p)) | (u64(u8(p[n - 1])) << (8 * (n - 1)));
    return n ? u8(*p) : 0;
}

inline u64 opaque_hash(Bytes key) {
    const char* p = key.data();
    size_t n = key.size();
    u64 state = mix_a ^ (n * mix_b);
    while (n >= 16) {
        state = fold_multiply(load<u64>(p) ^ state, load<u64>(p + 8) ^ mix_b);
        p += 16;
        n -= 16;
    }
    if (n >= 8)
        return fold_multiply(load<u64>(p) ^ state, load<u64>(p + n - 8) ^ mix_a);
    return fold_multiply(subword(p, n) ^ state, mix_a);
}

// Equality and ordering remain byte-exact. Only the canonical lowercase
// spelling with the expected prefix is encoded, so decoding is bijective.
inline bool pack_hex(const char* ascii, char (&out)[16]) {
#if defined(__AVX2__)
    const auto chars = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ascii));
    const auto digits = _mm256_sub_epi8(chars, _mm256_set1_epi8('0'));
    const auto letters = _mm256_sub_epi8(chars, _mm256_set1_epi8('a'));
    const auto d_ok = _mm256_cmpeq_epi8(digits, _mm256_min_epu8(digits, _mm256_set1_epi8(9)));
    const auto l_ok = _mm256_cmpeq_epi8(letters, _mm256_min_epu8(letters, _mm256_set1_epi8(5)));
    if (u32(_mm256_movemask_epi8(_mm256_or_si256(d_ok, l_ok))) != 0xffffffffu)
        return false;
    const auto nibbles = _mm256_blendv_epi8(_mm256_add_epi8(letters, _mm256_set1_epi8(10)), digits, d_ok);
    const auto pairs = _mm256_maddubs_epi16(nibbles, _mm256_set1_epi16(0x0110));
    const auto packed = _mm_packus_epi16(_mm256_castsi256_si128(pairs), _mm256_extracti128_si256(pairs, 1));
    _mm_storeu_si128(reinterpret_cast<__m128i*>(out), packed);
    return true;
#elif defined(__aarch64__)
    auto nibbles = [](const char* p, uint8x16_t& valid) {
        const auto chars = vld1q_u8(reinterpret_cast<const u8*>(p));
        const auto digits = vsubq_u8(chars, vdupq_n_u8('0'));
        const auto letters = vsubq_u8(chars, vdupq_n_u8('a'));
        const auto d_ok = vcleq_u8(digits, vdupq_n_u8(9));
        valid = vorrq_u8(vcleq_u8(letters, vdupq_n_u8(5)), d_ok);
        return vbslq_u8(d_ok, digits, vaddq_u8(letters, vdupq_n_u8(10)));
    };
    uint8x16_t lo_ok, hi_ok;
    const auto lo = nibbles(ascii, lo_ok), hi = nibbles(ascii + 16, hi_ok);
    if (!vminvq_u8(vandq_u8(lo_ok, hi_ok)))
        return false;
    const auto bytes = vorrq_u8(vshlq_n_u8(vuzp1q_u8(lo, hi), 4), vuzp2q_u8(lo, hi));
    vst1q_u8(reinterpret_cast<u8*>(out), bytes);
    return true;
#else
    for (size_t i = 0; i < 16; ++i) {
        auto digit = [](u8 c) {
            if (c >= '0' && c <= '9')
                return unsigned(c - '0');
            return c >= 'a' && c <= 'f' ? unsigned(c - 'a' + 10) : 16u;
        };
        const unsigned hi = digit(u8(ascii[2 * i])), lo = digit(u8(ascii[2 * i + 1]));
        if ((hi | lo) >= 16)
            return false;
        out[i] = char((hi << 4) | lo);
    }
    return true;
#endif
}

struct StoredKey {
    Bytes bytes;
    bool hex;
};

struct Id {
    u64 word;

    static constexpr u64 indirect = u64{1} << 63;
    static constexpr u64 extended = u64{1} << 62;
    static constexpr u64 long_key = u64{1} << 55;
    static constexpr unsigned owner_shift = 56;
    static constexpr unsigned arena_shift = 4;
    static constexpr u64 location_mask = long_key - 1;
    static constexpr u64 slot_mask = location_mask >> 32;

    bool literal() const { return !(word & indirect); }
    bool long_string() const { return word & long_key; }
    bool has_hash() const { return !(word & extended); }
    size_t owner() const { return (word >> owner_shift) & 63; }
    size_t offset() const {
        if (!has_hash())
            return word & location_mask;
        const size_t slot = (word >> 32) & slot_mask;
        return long_string() ? slot << arena_shift : slot;
    }
    unsigned suffix_size() const { return (64 - std::countl_zero(word) + 7) / 8; }
};

inline bool literal_id(Bytes bytes, char prefix, Id& id) {
    const size_t n = bytes.size();
    if (!n || n > 9 || bytes[0] != prefix)
        return false;
    const char* p = bytes.data() + 1;
    if (n == 9) {
        u64 suffix = load<u64>(p);
        // A zero final byte would make length inference ambiguous.
        if ((suffix >> 56) - 1 >= 127)
            return false;
        id = {suffix};
        return true;
    }
    if (n != 1 && bytes.back() == '\0')
        return false;
    id = {subword(p, n - 1)};
    return true;
}

// One writer during the scan, then immutable. Most IDs are represented by the
// record's literal bytes. Other short IDs fit in a fixed-size inline slot.
class KeyStore {
    static constexpr size_t page_shift = 19;
    static constexpr size_t page_size = size_t{1} << page_shift;
    static constexpr size_t header = 2 * sizeof(u64);
    static constexpr size_t inline_page = 4096;

    struct Inline {
        u64 hash;
        u8 size;
        char bytes[15];
    };

    struct Cached {
        u64 hash;
        Id id;
    };

    u64 owner_;
    std::vector<Cached> cache_;
    size_t used_ = page_size;
    size_t inline_count_ = 0;
    std::vector<std::unique_ptr<Inline[]>> inline_pages_;
    std::vector<std::unique_ptr<char[]>> pages_;

    const char* arena_entry(size_t offset) const {
        return pages_[offset >> page_shift].get() + (offset & (page_size - 1));
    }

    Inline& inline_entry(size_t index) const {
        return inline_pages_[index / inline_page][index % inline_page];
    }

    Id address(size_t offset, u64 hash, u64 kind) const {
        const u64 tag = Id::indirect | owner_ | kind;
        const size_t slot = kind ? offset >> Id::arena_shift : offset;
        if (slot <= Id::slot_mask)
            return {tag | (u64(slot) << 32) | u32(hash)};
        if (offset > Id::location_mask)
            throw std::runtime_error("opaque key arena exceeds address space");
        return {tag | Id::extended | offset};
    }

public:
    KeyStore(unsigned worker, bool repeated_ids)
        : owner_(u64(worker) << Id::owner_shift), cache_(repeated_ids ? 8192 : 0) {}

    Id put_indirect(Bytes bytes, u64 hash, bool hex) {
        const size_t n = bytes.size();
        Cached* cached = nullptr;
        if (!cache_.empty()) {
            cached = &cache_[hash & (cache_.size() - 1)];
            if (cached->id.word && cached->hash == hash) {
                const auto previous = view(cached->id);
                if (previous.hex == hex && previous.bytes == bytes)
                    return cached->id;
            }
        }
        if (n <= 15) {
            const size_t index = inline_count_;
            if (!(index % inline_page))
                inline_pages_.push_back(std::make_unique_for_overwrite<Inline[]>(inline_page));
            auto& item = inline_entry(index);
            item.hash = hash;
            item.size = u8(n);
            if (n)
                std::memcpy(item.bytes, bytes.data(), n);
            ++inline_count_;
            const Id id = address(index, hash, 0);
            if (cached)
                *cached = {hash, id};
            return id;
        }

        const size_t length = n + header;
        if (length > page_size - used_) {
            pages_.push_back(std::make_unique_for_overwrite<char[]>(std::max(page_size, length)));
            used_ = 0;
        }
        char* dest = pages_.back().get() + used_;
        const u64 offset = ((pages_.size() - 1) << page_shift) + used_;
        const u64 size = hex ? 0 : n;
        std::memcpy(dest, &hash, sizeof(hash));
        std::memcpy(dest + sizeof(hash), &size, sizeof(size));
        std::memcpy(dest + header, bytes.data(), n);
        constexpr size_t alignment = size_t{1} << Id::arena_shift;
        used_ = std::min(page_size, (used_ + length + alignment - 1) & ~(alignment - 1));
        const Id id = address(offset, hash, Id::long_key);
        if (cached)
            *cached = {hash, id};
        return id;
    }

    u64 hash(Id key) const {
        return key.long_string() ? load<u64>(arena_entry(key.offset())) : inline_entry(key.offset()).hash;
    }

    StoredKey view(Id key) const {
        if (key.long_string()) {
            const char* entry = arena_entry(key.offset());
            const u64 length = load<u64>(entry + sizeof(u64));
            return {{entry + header, length ? length : 16}, length == 0};
        }
        const auto& item = inline_entry(key.offset());
        return {{item.bytes, item.size}, false};
    }
};

class Keys {
    std::vector<KeyStore> stores_;
    char prefix_;

public:
    static constexpr size_t text_capacity = 33;

    Keys(unsigned workers, char prefix) : prefix_(prefix) {
        stores_.reserve(workers);
        for (unsigned i = 0; i < workers; ++i)
            stores_.emplace_back(i, prefix == 'u');
    }

    bool literal(Bytes bytes, Id& id) const { return literal_id(bytes, prefix_, id); }

    [[gnu::noinline]] Id put_indirect(unsigned worker, Bytes key, u64& hash) {
        char packed[16];
        if (key.size() == text_capacity && key[0] == prefix_ && pack_hex(key.data() + 1, packed)) {
            hash = fold_multiply(load<u64>(packed) ^ mix_a, load<u64>(packed + 8) ^ mix_b);
            return stores_[worker].put_indirect({packed, sizeof(packed)}, hash, true);
        }
        hash = opaque_hash(key);
        return stores_[worker].put_indirect(key, hash, false);
    }

    [[gnu::always_inline]] Id put(unsigned worker, Bytes key, u64& hash) {
        Id id;
        if (literal(key, id)) {
            hash = literal_hash(id.word);
            return id;
        }
        return put_indirect(worker, key, hash);
    }

    [[gnu::always_inline]] Id put(unsigned worker, Bytes key) {
        Id id;
        u64 unused;
        return literal(key, id) ? id : put_indirect(worker, key, unused);
    }
    u64 hash(Id key) const {
        if (key.literal())
            return literal_hash(key.word);
        return key.has_hash() ? u32(key.word) : stores_[key.owner()].hash(key);
    }

    Bytes view(Id key, char (&scratch)[text_capacity]) const {
        if (!key.literal()) {
            const StoredKey stored = stores_[key.owner()].view(key);
            if (!stored.hex)
                return stored.bytes;
            scratch[0] = prefix_;
            constexpr char hex[] = "0123456789abcdef";
            for (size_t i = 0; i < stored.bytes.size(); ++i) {
                const u8 byte = u8(stored.bytes[i]);
                scratch[2 * i + 1] = hex[byte >> 4];
                scratch[2 * i + 2] = hex[byte & 15];
            }
            return {scratch, text_capacity};
        }
        scratch[0] = prefix_;
        std::memcpy(scratch + 1, &key.word, sizeof(key.word));
        return {scratch, key.suffix_size() + 1};
    }

    bool equal(Id a, Id b) const {
        if (a.word == b.word)
            return true;
        // The literal encoding decision depends only on the ID's bytes.
        if (a.literal() || b.literal())
            return false;
        const auto first = stores_[a.owner()].view(a), second = stores_[b.owner()].view(b);
        return first.hex == second.hex && first.bytes == second.bytes;
    }

    bool before(Id a, Id b) const {
        if (a.literal() && b.literal())
            return __builtin_bswap64(a.word) < __builtin_bswap64(b.word);
        char sa[text_capacity], sb[text_capacity];
        return view(a, sa).compare(view(b, sb)) < 0;
    }
};

inline u32 fingerprint(u64 hash) {
    u32 low = u32(hash);
    return low ? low : 1;
}

inline size_t radix_bucket(u64 hash, unsigned bits) {
    return u32(hash) >> (32 - bits);
}

} // namespace SHOPPING_NAMESPACE
