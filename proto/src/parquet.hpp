#pragma once
#include "domain.hpp"

#include <span>
#include <sys/uio.h>

namespace w3 {

using Buffer = std::vector<u8>;

inline void append_bytes(Buffer& buf, const void* data, size_t bytes) {
    if (!bytes)
        return;
    auto* first = static_cast<const u8*>(data);
    buf.insert(buf.end(), first, first + bytes);
}

template <class T>
inline void append_le(Buffer& buf, T value) {
    append_bytes(buf, &value, sizeof(value));
}

inline void append_string(Buffer& buffer, Bytes value) {
    if (value.size() > std::numeric_limits<int32_t>::max())
        throw std::runtime_error("Parquet string exceeds the page format");
    append_le(buffer, u32(value.size()));
    append_bytes(buffer, value.data(), value.size());
}

inline void vint(Buffer& buf, u64 value) {
    while (value > 127) {
        buf.push_back(u8(value) | 0x80);
        value >>= 7;
    }
    buf.push_back(u8(value));
}

// Compact-Protocol field and collection headers for the flat schema used here.
struct Compact {
    enum Tag { Stop = 0, True = 1, False = 2, Int = 5, Long = 6, Binary = 8, List = 9, Object = 12 };
    Buffer& bytes;
    unsigned last = 0;

    void field(unsigned id, Tag type) {
        if (id > last && id - last <= 15) {
            bytes.push_back(u8((id - last) << 4) | type);
        } else {
            bytes.push_back(type);
            vint(bytes, id * 2);
        }
        last = id;
    }
    void integer(unsigned id, u64 n, Tag type = Int) {
        field(id, type);
        vint(bytes, n * 2);
    }
    void binary(unsigned id, Bytes s) {
        field(id, Binary);
        vint(bytes, s.size());
        append_bytes(bytes, s.data(), s.size());
    }
    void list(unsigned id, Tag element, size_t count) {
        field(id, List);
        bytes.push_back(u8(std::min<size_t>(count, 15) << 4) | element);
        if (count >= 15)
            vint(bytes, count);
    }
    void stop() { bytes.push_back(Stop); }
};

struct SchemaField {
    Bytes name;
    enum Type { Int32 = 1, Int64 = 2, ByteArray = 6 } physical;
    enum Annotation { Plain, Utf8, Date } logical = Plain;
    bool nullable = false;
};

class Parquet {
    struct Chunk {
        SchemaField schema;
        u64 first, data, size;
        bool dictionary;
    };
    using Data = std::span<const u8>;

    fs::path path_;
    int fd_ = -1;
    u64 offset_ = 0;
    u32 rows_;
    std::vector<Chunk> columns_;

    void output(std::initializer_list<Data> spans) {
        iovec vec[5];
        size_t n = 0;
        for (auto data : spans) {
            if (data.empty())
                continue;
            vec[n++] = {const_cast<u8*>(data.data()), data.size()};
        }
        for (size_t first = 0; first < n;) {
            ssize_t n_written = writev(fd_, vec + first, int(n - first));
            if (n_written < 0 && errno == EINTR)
                continue;
            if (n_written <= 0) {
                if (!n_written)
                    errno = EIO;
                io_error(path_);
            }
            offset_ += n_written;
            size_t bytes = size_t(n_written);
            while (first < n && bytes >= vec[first].iov_len)
                bytes -= vec[first++].iov_len;
            if (first < n && bytes) {
                vec[first].iov_base = static_cast<u8*>(vec[first].iov_base) + bytes;
                vec[first].iov_len -= bytes;
            }
        }
    }

    static void schema_field(Buffer& bytes, SchemaField field) {
        Compact node{bytes};
        node.integer(1, field.physical);
        node.integer(3, field.nullable ? 1 : 0);
        node.binary(4, field.name);
        if (field.logical != SchemaField::Plain) {
            const bool str = field.logical == SchemaField::Utf8;
            node.integer(6, str ? 0 : 6);
            node.field(10, Compact::Object);
            Compact logical{bytes};
            logical.field(str ? 1 : 6, Compact::Object);
            Compact{bytes}.stop();
            logical.stop();
        }
        node.stop();
    }

    static Buffer page_header(u64 bytes, u32 n, bool dict_page, bool dict_encoding) {
        if (bytes > std::numeric_limits<int32_t>::max())
            throw std::runtime_error("Parquet column exceeds the page format");
        Buffer header;
        header.reserve(40);
        Compact page{header};
        page.integer(1, dict_page ? 2 : 0);
        page.integer(2, bytes);
        page.integer(3, bytes);
        page.field(dict_page ? 7 : 5, Compact::Object);
        Compact details{header};
        details.integer(1, n);
        details.integer(2, dict_encoding ? 8 : 0);
        if (!dict_page) {
            details.integer(3, 3);
            details.integer(4, 3);
        }
        details.stop();
        page.stop();
        return header;
    }

public:
    Parquet(fs::path path, u32 rows) : path_(std::move(path)), rows_(rows) {
        fd_ = open(path_.c_str(), O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0666);
        if (fd_ < 0)
            io_error(path_);
        const std::array<u8, 4> magic{'P', 'A', 'R', '1'};
        try {
            output({magic});
        } catch (...) {
            close(fd_);
            fd_ = -1;
            throw;
        }
    }

    ~Parquet() {
        if (fd_ >= 0)
            close(fd_);
    }

    Parquet(const Parquet&) = delete;
    Parquet& operator=(const Parquet&) = delete;

    void column(SchemaField schema, Data values, Data definitions = {}, Data dict = {}, u32 entries = 0) {
        bool dictionary = entries != 0;
        const u64 first = offset_;
        if (rows_ && dictionary) {
            auto header = page_header(dict.size(), entries, true, false);
            output({header, dict});
        }
        const u64 data = offset_;
        if (rows_) {
            auto header = page_header(values.size() + definitions.size(), rows_, false, dictionary);
            output({header, definitions, values});
        }
        columns_.push_back({schema, first, data, offset_ - first, dictionary});
    }

    template <class T>
    void numbers(SchemaField schema, const std::vector<T>& values) {
        column(schema, Data(reinterpret_cast<const u8*>(values.data()), values.size() * sizeof(T)));
    }

    void finish(bool sync) {
        Buffer bytes;
        bytes.reserve(1024);
        Compact file{bytes};
        file.integer(1, 1);
        file.list(2, Compact::Object, columns_.size() + 1);
        Compact root{bytes};
        root.binary(4, "schema");
        root.integer(5, columns_.size());
        root.stop();
        for (const auto& chunk : columns_)
            schema_field(bytes, chunk.schema);
        file.integer(3, rows_, Compact::Long);
        file.list(4, Compact::Object, rows_ != 0);
        if (rows_) {
            u64 total = 0;
            Compact group{bytes};
            group.list(1, Compact::Object, columns_.size());
            for (const auto& col : columns_) {
                Compact chunk{bytes};
                chunk.integer(2, 0, Compact::Long);
                chunk.field(3, Compact::Object);
                Compact metadata{bytes};
                metadata.integer(1, col.schema.physical);
                metadata.list(2, Compact::Int, col.dictionary ? 3 : 2);
                vint(bytes, 0);  // PLAIN
                vint(bytes, 6);  // RLE
                if (col.dictionary)
                    vint(bytes, 16);  // RLE_DICTIONARY
                metadata.list(3, Compact::Binary, 1);
                vint(bytes, col.schema.name.size());
                append_bytes(bytes, col.schema.name.data(), col.schema.name.size());
                metadata.integer(4, 0);  // UNCOMPRESSED
                metadata.integer(5, rows_, Compact::Long);
                metadata.integer(6, col.size, Compact::Long);
                metadata.integer(7, col.size, Compact::Long);
                metadata.integer(9, col.data, Compact::Long);
                if (col.dictionary)
                    metadata.integer(11, col.first, Compact::Long);
                metadata.stop();
                chunk.stop();
                total += col.size;
            }
            group.integer(2, total, Compact::Long);
            group.integer(3, rows_, Compact::Long);
            group.stop();
        }
        file.binary(6, "shopping-w4 version 1");
        file.stop();
        u32 length = u32(bytes.size());
        append_le(bytes, length);
        const char end[] = "PAR1";
        append_bytes(bytes, end, 4);
        output({bytes});
        if (sync) {
            int result;
            do result = fsync(fd_); while (result < 0 && errno == EINTR);
            if (result < 0)
                io_error(path_);
        }
        if (close(std::exchange(fd_, -1)) < 0)
            io_error(path_);
    }
};

inline Buffer packed_ids(const std::vector<u32>& ids, unsigned entries) {
    unsigned width = std::max(1u, unsigned(std::bit_width(entries - 1)));
    size_t groups = (ids.size() + 7) / 8;
    Buffer out{u8(width)};
    if (!groups)
        return out;
    vint(out, (groups << 1) | 1);
    size_t offset = out.size();
    out.resize(offset + groups * width + 8);
    u8* dst = out.data() + offset;
    u64 pending = 0;
    unsigned used = 0;
    for (u32 id : ids) {
        pending |= u64(id) << used;
        used += width;
        if (used >= 64) {
            std::memcpy(dst, &pending, 8);
            dst += 8;
            used -= 64;
            pending = u64(id) >> (width - used);
        }
    }
    std::memcpy(dst, &pending, 8);
    out.resize(offset + groups * width);
    return out;
}

struct UserColumns {
    Buffer user_dict, present;
    std::vector<u32> person_starts, sessions, events, top;
    std::vector<int32_t> day;
    std::vector<u64> revenue;

    void reserve(size_t rows) {
        person_starts.reserve(rows / 3);
        sessions.reserve(rows);
        events.reserve(rows);
        day.reserve(rows);
        revenue.reserve(rows);
        top.reserve(rows / 4);
        user_dict.reserve(rows * 5);
        present.reserve((rows + 7) / 8);
    }

    void person(Bytes name) {
        append_string(user_dict, name);
        person_starts.push_back(u32(day.size()));
    }

    void row(int32_t date, u32 n_session, u32 n_events, u64 cents, u32 category) {
        size_t at = day.size();
        if ((at & 7) == 0)
            present.push_back(0);
        if (category != no_token) {
            present.back() |= 1u << (at & 7);
            top.push_back(category);
        }
        day.push_back(date);
        sessions.push_back(n_session);
        events.push_back(n_events);
        revenue.push_back(cents);
    }

    Buffer user_codes() const {
        const u32 names = u32(person_starts.size());
        if (!names)
            return {};
        const unsigned bits = std::max(1u, unsigned(std::bit_width(names - 1)));
        const unsigned bytes = (bits + 7) / 8;
        Buffer codes{u8(bits)};
        codes.reserve(1 + size_t(names) * (1 + bytes));
        // Every person's days are consecutive, so a dictionary run encodes
        // the whole person without a separate ID value for each output row.
        for (u32 id = 0; id < names; ++id) {
            const u32 end = id + 1 < names ? person_starts[id + 1] : u32(day.size());
            vint(codes, u64(end - person_starts[id]) << 1);
            for (unsigned byte = 0; byte < bytes; ++byte)
                codes.push_back(u8(id >> (8 * byte)));
        }
        return codes;
    }

    void write(const fs::path& path, const std::vector<std::string>& categories, bool sync) const {
        if (day.size() > std::numeric_limits<int32_t>::max())
            throw std::runtime_error("too many rows for a Parquet part");
        Parquet file(path, u32(day.size()));
        auto codes = user_codes();
        file.column({"user_id", SchemaField::ByteArray, SchemaField::Utf8}, codes, {}, user_dict,
                    u32(person_starts.size()));
        file.numbers({"day", SchemaField::Int32, SchemaField::Date}, day);
        file.numbers({"sessions", SchemaField::Int32}, sessions);
        file.numbers({"events", SchemaField::Int32}, events);
        file.numbers({"revenue_cents", SchemaField::Int64}, revenue);
        Buffer dict;
        for (const auto& category : categories)
            append_string(dict, category);
        Buffer def;
        if (!present.empty()) {
            append_le(def, u32{0});
            vint(def, (present.size() << 1) | 1);
            append_bytes(def, present.data(), present.size());
            u32 size = u32(def.size() - 4);
            std::memcpy(def.data(), &size, 4);
        }
        auto top_codes = packed_ids(top, unsigned(categories.size()));
        file.column({"top_category", SchemaField::ByteArray, SchemaField::Utf8, true},
                    top_codes, def, dict, u32(categories.size()));
        file.finish(sync);
    }
};

struct CampaignCount {
    u64 sessions = 0, purchasing = 0, revenue = 0, assists = 0;

    void add(const CampaignCount& other) {
        sessions += other.sessions;
        purchasing += other.purchasing;
        revenue += other.revenue;
        assists += other.assists;
    }
};

struct CampaignColumns {
    Buffer id, channel;
    std::vector<int32_t> day;
    std::vector<u64> sessions, purchasing, revenue, assists;

    void row(Bytes name, Bytes source, int32_t date, CampaignCount totals) {
        if (!totals.sessions && !totals.assists)
            return;
        append_string(id, name);
        append_string(channel, source);
        day.push_back(date);
        sessions.push_back(totals.sessions);
        purchasing.push_back(totals.purchasing);
        revenue.push_back(totals.revenue);
        assists.push_back(totals.assists);
    }

    void write(const fs::path& path, bool sync) const {
        Parquet file(path, u32(day.size()));
        file.column({"campaign_id", SchemaField::ByteArray, SchemaField::Utf8}, id);
        file.column({"channel", SchemaField::ByteArray, SchemaField::Utf8}, channel);
        file.numbers({"day", SchemaField::Int32, SchemaField::Date}, day);
        file.numbers({"sessions", SchemaField::Int64}, sessions);
        file.numbers({"purchasing_sessions", SchemaField::Int64}, purchasing);
        file.numbers({"revenue_cents", SchemaField::Int64}, revenue);
        file.numbers({"assisted_sessions", SchemaField::Int64}, assists);
        file.finish(sync);
    }
};

} // namespace w3
