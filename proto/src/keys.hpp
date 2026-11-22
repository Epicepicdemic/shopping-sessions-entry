#pragma once
#include "input.hpp"

#include <memory>

namespace w3 {

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
    std::vector<Inline> inline_;
    std::vector<std::unique_ptr<char[]>> pages_;

    const char* arena_entry(size_t offset) const {
        return pages_[offset >> page_shift].get() + (offset & (page_size - 1));
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

    Id put_indirect(Bytes bytes, u64 hash) {
        const size_t n = bytes.size();
        if (n <= 15) {
            const size_t index = inline_.size();
            Inline item{hash, u8(n), {}};
            if (n)
                std::memcpy(item.bytes, bytes.data(), n);
            inline_.push_back(item);
            return address(index, hash, 0);
        }

        Cached* cached = nullptr;
        if (!cache_.empty()) {
            cached = &cache_[hash & (cache_.size() - 1)];
            if (cached->id.word && cached->hash == hash && view(cached->id) == bytes)
                return cached->id;
        }
        const size_t length = n + header;
        if (length > page_size - used_) {
            pages_.push_back(std::make_unique_for_overwrite<char[]>(std::max(page_size, length)));
            used_ = 0;
        }
        char* dest = pages_.back().get() + used_;
        const u64 offset = ((pages_.size() - 1) << page_shift) + used_;
        const u64 size = n;
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
        return key.long_string() ? load<u64>(arena_entry(key.offset())) : inline_[key.offset()].hash;
    }

    Bytes view(Id key) const {
        if (key.long_string()) {
            const char* entry = arena_entry(key.offset());
            return {entry + header, load<u64>(entry + sizeof(u64))};
        }
        const auto& item = inline_[key.offset()];
        return {item.bytes, item.size};
    }
};

class Keys {
    std::vector<KeyStore> stores_;
    char prefix_;

public:
    Keys(unsigned workers, char prefix) : prefix_(prefix) {
        stores_.reserve(workers);
        for (unsigned i = 0; i < workers; ++i)
            stores_.emplace_back(i, prefix == 'u');
    }

    bool literal(Bytes bytes, Id& id) const { return literal_id(bytes, prefix_, id); }

    Id put_indirect(unsigned worker, Bytes key, u64 hash) {
        return stores_[worker].put_indirect(key, hash);
    }

    Id put(unsigned worker, Bytes key, u64& hash) {
        Id id;
        if (literal(key, id)) {
            hash = literal_hash(id.word);
            return id;
        }
        hash = opaque_hash(key);
        return put_indirect(worker, key, hash);
    }

    Id put(unsigned worker, Bytes key) {
        Id id;
        return literal(key, id) ? id : put_indirect(worker, key, opaque_hash(key));
    }
    void release(unsigned worker) { stores_[worker] = KeyStore(worker, false); }

    u64 hash(Id key) const {
        if (key.literal())
            return literal_hash(key.word);
        return key.has_hash() ? u32(key.word) : stores_[key.owner()].hash(key);
    }

    Bytes view(Id key, char (&scratch)[9]) const {
        if (!key.literal())
            return stores_[key.owner()].view(key);
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
        return stores_[a.owner()].view(a) == stores_[b.owner()].view(b);
    }

    bool before(Id a, Id b) const {
        if (a.literal() && b.literal())
            return __builtin_bswap64(a.word) < __builtin_bswap64(b.word);
        char sa[9], sb[9];
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

} // namespace w3
