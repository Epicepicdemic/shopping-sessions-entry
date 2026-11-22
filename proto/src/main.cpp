#include "aggregate.hpp"
#include "radix.hpp"

#include <cstdio>
#include <numeric>
#include <sys/resource.h>

namespace w3 {

struct Settings {
    ReadMode io = read_mode();
    unsigned workers = unsigned(number_option("NCPU", int(std::clamp(std::thread::hardware_concurrency(), 1u, 8u)), 1, 64));
    size_t chunk = size_t(number_option("W4_CHUNK_KIB", 2048, 64, 16384)) * 1024;
    size_t cell_threshold = size_t(number_option("W4_CELL_THRESHOLD", 16, 2, 100000000));
    bool sync = option("W2_SYNC");
    bool timing = option("W4_TIMING");
};

struct Location {
    u32 bucket;
    u32 slot;
};

struct Edge {
    Location endpoints[2];
};

struct Endpoint {
    Id id;
    u32 hash;
    u32 edge_side;
};

struct AliasRun {
    Location alias;
    u32 person;
    u32 next = no_token;
};

struct Range {
    u32 hit_begin = 0, hit_end = 0;
    u32 click_begin = 0, click_end = 0;
};

template <class Hit>
using TimeOf = decltype(Hit::time);

template <class Time>
struct RoutedClick {
    Id user;
    AdClick<Time> click;
};

template <class Time>
struct ScannedEvent {
    Id user;
    Time time;
    u32 stamp;
};

static_assert(sizeof(ScannedEvent<u32>) == 16);
static_assert(sizeof(RoutedClick<u32>) == 24);

// Store an opaque identity and a row reference in three 32-bit lanes. The
// user-side event stays 16 bytes and the dedup scan stays sequential.
struct EventRef {
    u32 low, high, row;

    Id id() const { return {u64(low) | (u64(high) << 32)}; }
};

static_assert(sizeof(EventRef) == 12);

class Names {
    struct Slot {
        u32 hash = 0;
        u32 index = 0;
    };

    std::vector<Slot> slots_;

    static size_t capacity(size_t estimate) {
        return std::bit_ceil(std::max<size_t>(16, estimate + (estimate + 1) / 2));
    }

    void grow(size_t size) {
        std::vector<Slot> target(size);
        const size_t mask = size - 1;
        for (Slot slot : slots_) {
            if (!slot.hash)
                continue;
            size_t at = slot.hash & mask;
            while (target[at].hash)
                at = (at + 1) & mask;
            target[at] = slot;
        }
        slots_.swap(target);
    }

public:
    std::vector<Id> ids;

    void reset(size_t estimate) {
        slots_.assign(capacity(estimate), {});
        ids.clear();
        ids.reserve(estimate);
    }

    void reserve(size_t estimate) {
        const size_t want = capacity(estimate);
        if (want > slots_.size())
            grow(want);
        ids.reserve(estimate);
    }

    u32 intern(Id id, u32 hash, const Keys& keys) {
        const size_t mask = slots_.size() - 1;
        for (size_t at = hash & mask; ; at = (at + 1) & mask) {
            Slot& slot = slots_[at];
            if (!slot.hash) {
                if (ids.size() + 1 > slots_.size() * 3 / 4) {
                    grow(slots_.size() * 2);
                    return intern(id, hash, keys);
                }
                if (ids.size() == no_token)
                    throw std::runtime_error("too many names in a partition");
                u32 index = u32(ids.size());
                ids.push_back(id);
                slot = {hash, index};
                return index;
            }
            if (slot.hash == hash && keys.equal(id, ids[slot.index]))
                return slot.index;
        }
    }
};

template <class Hit>
struct PersonPartition {
    using Click = AdClick<TimeOf<Hit>>;
    Names aliases;
    std::vector<Location> leaders;
    std::vector<Range> ranges;
    std::unique_ptr<Hit[]> hits;
    std::unique_ptr<Click[]> clicks;
    u32 global_base = 0;

    std::span<Hit> event_run(u32 alias) const {
        const auto& r = ranges[alias];
        return {hits ? hits.get() + r.hit_begin : nullptr, r.hit_end - r.hit_begin};
    }

    std::span<Click> click_run(u32 alias) const {
        const auto& r = ranges[alias];
        return {clicks ? clicks.get() + r.click_begin : nullptr, r.click_end - r.click_begin};
    }
};

struct EventExtra {
    i64 ingest;
    Buy purchase;
};

class Earliest {
    static constexpr u32 indirect = u32{1} << 31;
    static constexpr i64 bias = i64{1} << 30;
    static constexpr unsigned owner_shift = 26;
    static constexpr u32 tag_mask = (u32{1} << owner_shift) - 1;

    struct alignas(32) Block {
        u32 tags[4];
        u32 positions[4];
    };
    std::vector<Block> table_;

    struct Candidates { unsigned same, empty; };

    static Candidates candidates(const Block& block, u32 tag) {
#if defined(__aarch64__)
        const auto values = vandq_u32(vld1q_u32(block.tags), vdupq_n_u32(tag_mask));
        const uint32x4_t weights{1, 2, 4, 8};
        return {vaddvq_u32(vandq_u32(vceqq_u32(values, vdupq_n_u32(tag)), weights)),
                vaddvq_u32(vandq_u32(vceqzq_u32(values), weights))};
#elif defined(__x86_64__)
        const auto values = _mm_and_si128(_mm_load_si128(reinterpret_cast<const __m128i*>(block.tags)),
                                         _mm_set1_epi32(tag_mask));
        return {unsigned(_mm_movemask_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(values, _mm_set1_epi32(tag))))),
                unsigned(_mm_movemask_ps(_mm_castsi128_ps(_mm_cmpeq_epi32(values, _mm_setzero_si128()))))};
#else
        Candidates found{};
        for (unsigned i = 0; i < 4; ++i) {
            const u32 value = block.tags[i] & tag_mask;
            found.same |= (value == tag) << i;
            found.empty |= (!value) << i;
        }
        return found;
#endif
    }

    static i64 seconds(u32 stamp, const std::vector<EventExtra>& extra) {
        return stamp < indirect ? i64(stamp) - bias : extra[stamp & ~indirect].ingest;
    }

public:
    static u32 store(i64 ingest, Buy buy, std::vector<EventExtra>& extra) {
        const u64 compact = u64(ingest) + bias;
        if (compact < indirect && !buy.cents)
            return u32(compact);
        if (extra.size() >= indirect)
            throw std::runtime_error("too many wide ingests or purchases in a scan worker");
        u32 index = u32(extra.size());
        extra.push_back({ingest, buy});
        return indirect | index;
    }

    static Buy purchase(u32 stamp, const std::vector<EventExtra>& extra) {
        return stamp < indirect ? Buy{} : extra[stamp & ~indirect].purchase;
    }

    void reset(size_t count) {
        table_.assign(std::bit_ceil(std::max<size_t>(4, (count + (count + 1) / 2 + 3) / 4)), {});
    }

    template <class Worker, class Workers>
    void add(EventRef ref, u32 index, unsigned owner, Worker& source,
             const Workers& workers, const Keys& keys) {
        const Id id = ref.id();
        const u32 hash = u32(keys.hash(id));
        const u32 tag = fingerprint(hash & tag_mask);
        const u32 entry = (owner << owner_shift) | tag;
        const size_t mask = table_.size() - 1;
        for (size_t at = hash & mask; ; at = (at + 1) & mask) {
            Block& block = table_[at];
            const auto found = candidates(block, tag);
            for (unsigned same = found.same; same; same &= same - 1) {
                const unsigned lane = std::countr_zero(same);
                auto& previous = *workers[block.tags[lane] >> owner_shift];
                const auto kept_ref = previous.by_event_id->at(block.positions[lane]);
                if (!keys.equal(id, kept_ref.id()))
                    continue;
                auto& row = source.events->at(ref.row);
                auto& kept = previous.events->at(kept_ref.row);
                using Time = decltype(row.time);
                const u32 a = row.stamp, b = kept.stamp;
                const bool earlier = (a | b) < indirect ? a < b :
                    seconds(a, source.extra) < seconds(b, previous.extra);
                if (earlier) {
                    kept.time = std::numeric_limits<Time>::max();
                    block.tags[lane] = entry;
                    block.positions[lane] = index;
                } else {
                    row.time = std::numeric_limits<Time>::max();
                }
                return;
            }
            if (found.empty) {
                const unsigned lane = std::countr_zero(found.empty);
                block.tags[lane] = entry;
                block.positions[lane] = index;
                return;
            }
        }
    }
};

template <class Hit>
void sort_events(std::span<Hit> hits) {
    const auto before = [](const Hit& a, const Hit& b) { return a.time < b.time; };
    if (hits.size() > 24) {
        std::sort(hits.begin(), hits.end(), before);
        return;
    }
    for (size_t i = 1; i < hits.size(); ++i) {
        Hit hit = hits[i];
        size_t at = i;
        while (at && hit.time < hits[at - 1].time) {
            hits[at] = hits[at - 1];
            --at;
        }
        hits[at] = hit;
    }
}

template <class Hit>
class Engine {
    using Time = TimeOf<Hit>;
    using Record = ScannedEvent<Time>;
    using Click = AdClick<Time>;
    static constexpr bool narrow = std::is_same_v<Time, u32>;
    static constexpr Time dropped = std::numeric_limits<Time>::max();

    struct Worker {
        ReadBuffer input;
        std::unique_ptr<Bins<Endpoint>> link_endpoints;
        std::vector<Edge> edges;
        std::unique_ptr<Bins<EventRef>> by_event_id;
        std::vector<EventExtra> extra;
        Earliest earliest;
        std::unique_ptr<Bins<Record>> events;
        std::unique_ptr<Bins<RoutedClick<Time>>> clicks;
        Bins<AliasRun> link_runs;
        std::vector<u32> heads;
        std::vector<AliasRun> incoming;
        std::vector<Hit> merged_hits;
        std::vector<Click> merged_clicks;
        EventCells event_cells;
        Reduce<Time> reducer;
        Campaigns campaigns;
        UserColumns user_columns;
        u64 reduce_ns = 0, write_ns = 0;

        Worker(size_t event_bins, size_t user_bins, const Catalog& catalog, i64 origin, size_t cells)
            : link_endpoints(std::make_unique<Bins<Endpoint>>(user_bins)),
              by_event_id(std::make_unique<Bins<EventRef>>(event_bins)),
              events(std::make_unique<Bins<Record>>(user_bins + 1)),
              clicks(std::make_unique<Bins<RoutedClick<Time>>>(user_bins)),
              link_runs(user_bins), event_cells(cells), reducer(catalog, origin), campaigns(catalog) {}
    };

    const Settings& settings_;
    const Catalog& catalog_;
    const Dates& dates_;
    const Inputs& events_;
    const Inputs& clicks_;
    const Inputs& links_;
    const fs::path& output_;
    Scanner scanner_;
    unsigned event_bits_, user_bits_;
    size_t event_bins_, user_bins_, names_hint_;
    Keys event_keys_, user_keys_, click_keys_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::vector<PersonPartition<Hit>> people_;
    Team team_;
    i64 event_begin_, event_end_, click_begin_;
    size_t users_written_ = 0;

    static unsigned event_bits(size_t bytes) {
        const unsigned guess = unsigned(std::bit_width(bytes >> 21));
        return unsigned(number_option("W4_EVENT_BITS", int(std::clamp(guess, 6u, 13u)), 4, 14));
    }

    static unsigned user_bits(size_t bytes) {
        const unsigned guess = unsigned(std::bit_width(bytes >> 21));
        return unsigned(number_option("W4_USER_BITS", int(std::clamp(guess, 5u, 11u)), 4, 13));
    }

    i64 parse_time(Bytes value) const {
        if constexpr (narrow)
            return dates_.millis(value);
        return timestamp(value);
    }

    bool validate(const std::array<Bytes, 7>& row, i64 ingest, Time& time, Buy& purchase) const {
        if (row[1].empty() || row[5].empty() || !live_kind(row[4]))
            return false;
        i64 t = parse_time(row[2]);
        if (t == no_time || t < event_begin_ || t >= event_end_)
            return false;
        if constexpr (narrow)
            ingest *= 1000;
        else
            ingest = (ingest + dates_.origin / 1000) * 1000;
        if (ingest > t && elapsed(ingest, t) > day_ms)
            return false;

        if (row[4].size() == 8) {
            purchase = read_purchase(catalog_, row);
            if (!purchase.cents)
                return false;
        }
        time = Time(t);
        return true;
    }

    Bytes chunk(Worker& worker, const Part& part) {
        return part.input->chunk(part.first, part.end, worker.input);
    }

    void scan_links(unsigned owner, const Part& part) {
        auto& worker = *workers_[owner];
        scan_rows<2>(chunk(worker, part), scanner_, [&](const auto& row) {
            if (row[0].empty() || row[1].empty())
                return;
            if (worker.edges.size() >= (u32{1} << 31))
                throw std::runtime_error("too many link edges");
            const u32 index = u32(worker.edges.size());
            Edge edge;
            for (u32 side = 0; side < 2; ++side) {
                u64 hash;
                const Id key = user_keys_.put(owner, row[side], hash);
                const u32 bucket = u32(radix_bucket(hash, user_bits_));
                edge.endpoints[side] = {bucket, 0};
                worker.link_endpoints->add(bucket, {key, fingerprint(hash), index * 2 + side});
            }
            worker.edges.push_back(edge);
        });
    }

    void intern_link_ends(size_t bucket) {
        auto& part = people_[bucket];
        size_t ends = 0;
        for (const auto& source : workers_)
            ends += source->link_endpoints->size(bucket);
        part.aliases.reset(ends);
        for (auto& source : workers_) {
            source->link_endpoints->visit(bucket, [&](const Endpoint& end) {
                auto slot = part.aliases.intern(end.id, end.hash, user_keys_);
                source->edges[end.edge_side / 2].endpoints[end.edge_side % 2].slot = slot;
            });
        }
    }

    void link_users() {
        struct Node { Id user; Location alias; };

        size_t total = 0;
        for (auto& part : people_) {
            part.global_base = u32(total);
            total += part.aliases.ids.size();
        }
        if (total >= no_token)
            throw std::runtime_error("too many link IDs");
        auto nodes = std::make_unique_for_overwrite<Node[]>(total);
        auto parent = std::make_unique_for_overwrite<u32[]>(total);
        team_.run(user_bins_ + workers_.size(), [&](unsigned, size_t job) {
            if (job >= user_bins_) {
                auto& worker = *workers_[job - user_bins_];
                worker.link_endpoints.reset();
                worker.by_event_id.reset();
                event_keys_.release(unsigned(job - user_bins_));
                return;
            }
            const auto& part = people_[job];
            for (u32 slot = 0; slot < part.aliases.ids.size(); ++slot) {
                u32 index = part.global_base + slot;
                nodes[index] = {part.aliases.ids[slot], {u32(job), slot}};
                parent[index] = index;
            }
        });

        auto root = [&](u32 vertex) {
            while (true) {
                std::atomic_ref entry(parent[vertex]);
                u32 next = entry.load(std::memory_order_relaxed);
                if (next == vertex)
                    return vertex;
                u32 grand = std::atomic_ref(parent[next]).load(std::memory_order_relaxed);
                if (next != grand)
                    entry.compare_exchange_weak(next, grand, std::memory_order_relaxed);
                vertex = grand;
            }
        };
        auto index = [&](Location loc) {
            return people_[loc.bucket].global_base + loc.slot;
        };
        struct Batch { const Edge* data; size_t count; };
        constexpr size_t batch_size = 8192;
        std::vector<Batch> batches;
        for (const auto& worker : workers_) {
            const auto& edges = worker->edges;
            for (size_t i = 0; i < edges.size(); i += batch_size)
                batches.push_back({edges.data() + i, std::min(batch_size, edges.size() - i)});
        }
        team_.run(batches.size(), [&](unsigned, size_t job) {
            const auto batch = batches[job];
            for (size_t i = 0; i < batch.count; ++i) {
                const auto& edge = batch.data[i];
                u32 a = index(edge.endpoints[0]), b = index(edge.endpoints[1]);
                while ((a = root(a)) != (b = root(b))) {
                    // Only a root can acquire a parent. Each parent is
                    // bytewise smaller, including when joins race.
                    if (user_keys_.before(nodes[b].user, nodes[a].user))
                        std::swap(a, b);
                    u32 expected = b;
                    if (std::atomic_ref(parent[b]).compare_exchange_weak(expected, a, std::memory_order_relaxed))
                        break;
                }
            }
        });
        team_.run(user_bins_ + workers_.size(), [&](unsigned, size_t job) {
            if (job >= user_bins_) {
                std::vector<Edge>().swap(workers_[job - user_bins_]->edges);
                return;
            }
            auto& part = people_[job];
            part.leaders.reserve(part.aliases.ids.size());
            for (u32 i = 0; i < part.aliases.ids.size(); ++i)
                part.leaders.push_back(nodes[root(part.global_base + i)].alias);
        });
    }

    void scan_events(unsigned owner, Worker& worker, const Part& part) {
        scan_rows<7>(chunk(worker, part), scanner_, [&](const auto& row) {
            if (row[0].empty())
                return;
            const i64 ingest = dates_.ingest_seconds(row[3]);
            if (ingest == no_time)
                return;
            u64 hash;
            const Id key = event_keys_.put(owner, row[0], hash);
            Time time = dropped;
            Buy purchase;
            Id user{};
            size_t bucket = user_bins_;
            if (validate(row, ingest, time, purchase)) {
                u64 user_hash;
                user = user_keys_.put(owner, row[1], user_hash);
                bucket = radix_bucket(user_hash, user_bits_);
            }
            const u32 index = worker.events->add_index(bucket,
                {user, time, Earliest::store(ingest, purchase, worker.extra)});
            worker.by_event_id->add(radix_bucket(hash, event_bits_), {u32(key.word), u32(key.word >> 32), index});
        });
    }

    void scan_clicks(unsigned owner, Worker& worker, const Part& part) {
        scan_rows<4>(chunk(worker, part), scanner_, [&](const auto& row) {
            if (row[0].empty() || row[1].empty())
                return;
            i64 t = parse_time(row[3]);
            if (t == no_time || t < click_begin_ || t >= event_end_)
                return;
            u32 campaign = catalog_.ads.lookup(row[2]);
            if (campaign == no_token)
                return;
            u64 hash;
            const Id user = user_keys_.put(owner, row[1], hash);
            const Id click = click_keys_.put(owner, row[0]);
            // Identical copies leave the last-click minimum and the set of
            // assisting campaigns unchanged.
            worker.clicks->add(radix_bucket(hash, user_bits_), {user, {Time(t), campaign, click.word}});
        });
    }

    void scan() {
        const size_t n_events = events_.parts.size(), n_clicks = clicks_.parts.size();
        team_.run(n_events + n_clicks + links_.parts.size(), [&](unsigned owner, size_t job) {
            auto& worker = *workers_[owner];
            if (job < n_events)
                scan_events(owner, worker, events_.parts[job]);
            else if (job < n_events + n_clicks)
                scan_clicks(owner, worker, clicks_.parts[job - n_events]);
            else
                scan_links(owner, links_.parts[job - n_events - n_clicks]);
        });
    }

    void dedup(Worker& worker, size_t bucket) {
        size_t count = 0;
        for (const auto& source : workers_)
            count += source->by_event_id->size(bucket);
        if (!count)
            return;
        worker.earliest.reset(count);
        for (unsigned owner = 0; owner < workers_.size(); ++owner) {
            auto& source = *workers_[owner];
            source.by_event_id->visit_indexed(bucket, [&](const EventRef& ref, u32 index) {
                worker.earliest.add(ref, index, owner, source, workers_, event_keys_);
            });
        }
    }

    size_t user_load(size_t bucket) const {
        size_t count = 0;
        for (const auto& source : workers_)
            count += source->events->size(bucket) + source->clicks->size(bucket);
        return count;
    }

    void group(Worker& worker, size_t bucket) {
        auto& part = people_[bucket];
        auto& names = part.aliases;
        auto& ranges = part.ranges;
        names.reserve(names.ids.size() + names_hint_);
        ranges.reserve(names.ids.size() + names_hint_);
        ranges.resize(names.ids.size());
        struct Recent {
            u64 word = 0;
            u32 index = no_token;
        };
        std::array<Recent, 128> recent{};
        auto locate = [&](Id user) {
            const u32 hash = fingerprint(user_keys_.hash(user));
            auto& cached = recent[(hash >> 8) & (recent.size() - 1)];
            if (cached.word == user.word && cached.index != no_token)
                return cached.index;
            const u32 i = names.intern(user, hash, user_keys_);
            if (i == ranges.size())
                ranges.emplace_back();
            cached = {user.word, i};
            return i;
        };
        size_t hits = 0, clicks = 0;
        for (const auto& source : workers_) {
            hits += source->events->size(bucket);
            clicks += source->clicks->size(bucket);
        }
        if (hits >= no_token || clicks >= no_token)
            throw std::runtime_error("too many records in a user partition");

        for (const auto& source : workers_) {
            source->events->visit(bucket, [&](Record& row) {
                if (row.time != dropped) {
                    u32 i = locate(row.user);
                    row.user.word = i;
                    ++ranges[i].hit_end;
                }
            });
            source->clicks->visit(bucket, [&](RoutedClick<Time>& row) {
                u32 i = locate(row.user);
                row.user.word = i;
                ++ranges[i].click_end;
            });
        }

        u32 next_hit = 0, next_click = 0;
        for (auto& r : ranges) {
            u32 n_hit = r.hit_end, n_click = r.click_end;
            r = {next_hit, next_hit, next_click, next_click};
            next_hit += n_hit;
            next_click += n_click;
        }
        part.hits = std::make_unique_for_overwrite<Hit[]>(next_hit);
        part.clicks = std::make_unique_for_overwrite<Click[]>(next_click);
        for (const auto& source : workers_) {
            source->events->visit(bucket, [&](const Record& row) {
                if (row.time == dropped)
                    return;
                const Buy buy = Earliest::purchase(row.stamp, source->extra);
                Hit& dest = part.hits[ranges[row.user.word].hit_end++];
                if constexpr (narrow)
                    dest = {row.time, buy.cents ? u32((buy.category << 24) | buy.cents) : 0};
                else
                    dest = {row.time, buy};
            });
            source->clicks->visit(bucket, [&](const RoutedClick<Time>& row) {
                part.clicks[ranges[row.user.word].click_end++] = row.click;
            });
        }
        // Unlinked identities are already in their final partition. Only
        // active link endpoints need cross-partition descriptors.
        for (u32 i = 0; i < part.leaders.size(); ++i) {
            const auto& r = ranges[i];
            if (r.hit_begin != r.hit_end || r.click_begin != r.click_end) {
                const Location person = part.leaders[i];
                worker.link_runs.add(person.bucket, {{u32(bucket), i}, person.slot});
            }
        }
    }

    void reduce_cells(Worker& worker, const EventCells& cells, std::span<Click> clicks) requires narrow {
        // The busiest fully connected histories can scan their clicks once.
        if (clicks.size() > 32 && cells.connected()) {
            worker.reducer.one(cells, clicks, click_keys_, worker.user_columns, worker.campaigns);
        } else {
            sort_clicks(clicks, click_keys_);
            worker.reducer.person(cells, std::span<const Click>(clicks),
                                  worker.user_columns, worker.campaigns);
        }
    }

    void reduce_person(Worker& worker, Id user, std::span<Hit> hits, std::span<Click> clicks) {
        char scratch[9];
        worker.user_columns.person(user_keys_.view(user, scratch));
        if constexpr (narrow) {
            if (hits.size() >= settings_.cell_threshold) {
                auto& cells = worker.event_cells;
                cells.reset();
                cells.add(hits);
                reduce_cells(worker, cells, clicks);
                return;
            }
        }
        sort_events(hits);
        if (hits.size() <= 2 && elapsed(hits.back().time, hits.front().time) <= gap_ms) {
            worker.reducer.one(hits.front().time, std::span<const Hit>(hits), std::span<const Click>(clicks),
                               click_keys_, worker.user_columns, worker.campaigns);
            return;
        }
        sort_clicks(clicks, click_keys_);
        worker.reducer.person(std::span<const Hit>(hits), std::span<const Click>(clicks),
                              worker.user_columns, worker.campaigns);
    }

    void reduce(Worker& worker, size_t bucket) {
        const auto& part = people_[bucket];
        for (size_t i = part.leaders.size(); i < part.ranges.size(); ++i) {
            auto hits = part.event_run(u32(i));
            if (!hits.empty())
                reduce_person(worker, part.aliases.ids[i], hits, part.click_run(u32(i)));
        }

        auto& heads = worker.heads;
        auto& incoming = worker.incoming;
        heads.assign(part.leaders.size(), no_token);
        incoming.clear();
        for (const auto& source : workers_) {
            source->link_runs.visit(bucket, [&](AliasRun run) {
                run.next = std::exchange(heads[run.person], u32(incoming.size()));
                incoming.push_back(run);
            });
        }
        for (size_t i = 0; i < heads.size(); ++i) {
            const u32 first = heads[i];
            if (first == no_token)
                continue;
            const auto linked_events = [&](u32 at) {
                const Location alias = incoming[at].alias;
                return people_[alias.bucket].event_run(alias.slot);
            };
            const auto linked_clicks = [&](u32 at) {
                const Location alias = incoming[at].alias;
                return people_[alias.bucket].click_run(alias.slot);
            };
            auto hits = linked_events(first);
            auto clicks = linked_clicks(first);
            if (incoming[first].next == no_token) {
                if (!hits.empty())
                    reduce_person(worker, part.aliases.ids[i], hits, clicks);
                continue;
            }
            size_t count = hits.size();
            for (u32 at = incoming[first].next; at != no_token; at = incoming[at].next)
                count += linked_events(at).size();
            if (!count)
                continue;

            auto& merged_clicks = worker.merged_clicks;
            merged_clicks.clear();
            for (u32 at = first; at != no_token; at = incoming[at].next) {
                const auto slice = linked_clicks(at);
                if (!slice.empty())
                    merged_clicks.insert(merged_clicks.end(), slice.begin(), slice.end());
            }
            if constexpr (narrow) {
                if (count >= settings_.cell_threshold) {
                    auto& cells = worker.event_cells;
                    cells.reset();
                    for (u32 at = first; at != no_token; at = incoming[at].next)
                        cells.add(linked_events(at));
                    char scratch[9];
                    worker.user_columns.person(user_keys_.view(part.aliases.ids[i], scratch));
                    reduce_cells(worker, cells, merged_clicks);
                    continue;
                }
            }
            auto& merged_hits = worker.merged_hits;
            merged_hits.clear();
            merged_hits.reserve(count);
            for (u32 at = first; at != no_token; at = incoming[at].next) {
                const auto slice = linked_events(at);
                if (!slice.empty())
                    merged_hits.insert(merged_hits.end(), slice.begin(), slice.end());
            }
            reduce_person(worker, part.aliases.ids[i], merged_hits, merged_clicks);
        }
    }

    template <class Load>
    std::vector<size_t> work_order(Load load) const {
        std::vector<size_t> order(user_bins_), counts(user_bins_);
        std::iota(order.begin(), order.end(), size_t{0});
        for (size_t bucket = 0; bucket < user_bins_; ++bucket)
            counts[bucket] = load(bucket);
        std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return counts[a] > counts[b]; });
        return order;
    }

    void aggregate() {
        const auto order = work_order([&](size_t bucket) {
            // An active transition carries the source alias's full history.
            size_t count = people_[bucket].ranges.size();
            for (const auto& source : workers_)
                source->link_runs.visit(bucket, [&](const AliasRun& run) {
                    const auto& range = people_[run.alias.bucket].ranges[run.alias.slot];
                    count += range.hit_end - range.hit_begin + range.click_end - range.click_begin;
                });
            return count;
        });
        team_.run(order.size() + workers_.size(), [&](unsigned owner, size_t job) {
            if (job >= order.size()) {
                auto& source = *workers_[job - order.size()];
                source.events.reset();
                source.clicks.reset();
                std::vector<EventExtra>().swap(source.extra);
                return;
            }
            auto& worker = *workers_[owner];
            const u64 start = settings_.timing ? clock_ns() : 0;
            reduce(worker, order[job]);
            if (settings_.timing)
                worker.reduce_ns += clock_ns() - start;
        }, [&](unsigned owner) {
            auto& worker = *workers_[owner];
            auto& columns = worker.user_columns;
            if (owner && columns.day.empty())
                return;
            const u64 start = settings_.timing ? clock_ns() : 0;
            columns.write(output_ / "user_daily" / ("part-" + std::to_string(owner) + ".parquet"),
                          catalog_.categories, settings_.sync);
            if (settings_.timing)
                worker.write_ns = clock_ns() - start;
        });
        for (const auto& worker : workers_)
            users_written_ += worker->user_columns.day.size();
    }

public:
    Engine(const Settings& settings, const Catalog& catalog, const Dates& dates,
           const Inputs& events, const Inputs& clicks, const Inputs& links,
           const fs::path& output, Scanner scanner)
        : settings_(settings), catalog_(catalog), dates_(dates), events_(events), clicks_(clicks),
          links_(links), output_(output), scanner_(scanner), event_bits_(event_bits(events.bytes)),
          user_bits_(user_bits(events.bytes)), event_bins_(size_t{1} << event_bits_),
          user_bins_(size_t{1} << user_bits_), names_hint_(events.bytes / (1800 * user_bins_) + 16),
          event_keys_(settings.workers, 'e'), user_keys_(settings.workers, 'u'),
          click_keys_(settings.workers, 'k'), people_(user_bins_), team_(settings.workers) {
        i64 origin = narrow ? dates.origin : 0;
        event_begin_ = catalog.begin - origin;
        event_end_ = catalog.end - origin;
        click_begin_ = event_begin_ - week_ms;
        const size_t rows_hint = events.bytes / (600 * settings.workers);
        const size_t cells = narrow ? size_t(event_end_ / gap_ms + 1) : 0;
        for (unsigned i = 0; i < settings.workers; ++i) {
            auto worker = std::make_unique<Worker>(event_bins_, user_bins_, catalog, origin, cells);
            worker->user_columns.reserve(rows_hint);
            workers_.push_back(std::move(worker));
        }
    }

    void run(u64 start) {
        const u64 ready = clock_ns();
        scan();
        const u64 scanned = clock_ns();
        team_.run(event_bins_ + user_bins_, [&](unsigned owner, size_t job) {
            if (job < event_bins_)
                dedup(*workers_[owner], job);
            else
                intern_link_ends(job - event_bins_);
        });
        const u64 deduped = clock_ns();
        link_users();
        const u64 linked = clock_ns();
        const auto order = work_order([&](size_t bucket) { return user_load(bucket); });
        team_.run(order.size(), [&](unsigned owner, size_t job) { group(*workers_[owner], order[job]); });
        const u64 grouped = clock_ns();
        aggregate();
        const u64 users_done = clock_ns();
        auto& total = workers_[0]->campaigns;
        for (size_t owner = 1; owner < workers_.size(); ++owner)
            total.merge(workers_[owner]->campaigns);
        auto campaigns = total.columns(catalog_);
        campaigns.write(output_ / "campaign_daily" / "part-0.parquet", settings_.sync);
        const u64 done = clock_ns();
        if (settings_.timing) {
            auto ms = [](u64 ns) { return double(ns) * 1e-6; };
            rusage stats{};
            getrusage(RUSAGE_SELF, &stats);
#ifdef __APPLE__
            u64 rss = stats.ru_maxrss;
#else
            u64 rss = u64(stats.ru_maxrss) * 1024;
#endif
            std::fprintf(stderr,
                "w4 scanner=%s workers=%u event_bits=%u user_bits=%u time=%s sync=%d input_bytes=%zu "
                "user_rows=%zu campaign_rows=%zu peak_rss_bytes=%llu minor_faults=%ld major_faults=%ld\n"
                "w4_ms setup=%.3f scan=%.3f dedup=%.3f links=%.3f group=%.3f reduce_write=%.3f "
                "campaign_write=%.3f total=%.3f\n",
                scanner_.name, settings_.workers, event_bits_, user_bits_, narrow ? "compact" : "wide",
                int(settings_.sync), events_.bytes + clicks_.bytes + links_.bytes,
                users_written_, campaigns.day.size(), static_cast<unsigned long long>(rss),
                stats.ru_minflt, stats.ru_majflt, ms(ready - start), ms(scanned - ready),
                ms(deduped - scanned), ms(linked - deduped), ms(grouped - linked),
                ms(users_done - grouped), ms(done - users_done), ms(done - start));
            const auto& longest = **std::max_element(workers_.begin(), workers_.end(), [](const auto& a, const auto& b) {
                return a->reduce_ns + a->write_ns < b->reduce_ns + b->write_ns;
            });
            std::fprintf(stderr, "w4_worker_ms reduce=%.3f write=%.3f\n",
                         ms(longest.reduce_ns), ms(longest.write_ns));
#ifdef __linux__
            for (const char* name : {"memory.max", "memory.peak", "memory.swap.max", "cpu.max"}) {
                std::ifstream counter(std::string("/sys/fs/cgroup/") + name);
                std::string value;
                if (std::getline(counter, value))
                    std::fprintf(stderr, "w4_cgroup %s=%s\n", name, value.c_str());
            }
#endif
        }
        // All files were closed, and the worker team has finished.
        _exit(0);
    }
};

inline fs::path path_option(const char* env, const char* fallback) {
    const char* value = std::getenv(env);
    return value ? value : fallback;
}

} // namespace w3

int main(int argc, char** argv) {
    using namespace w3;
    const u64 start = clock_ns();
    try {
        if (argc != 1 && argc != 3)
            throw std::runtime_error("usage: shopping-w4 [DATA_DIR OUTPUT_DIR]");
        const fs::path data = argc == 3 ? fs::path(argv[1]) : path_option("DATA_DIR", "/data");
        const fs::path output = argc == 3 ? fs::path(argv[2]) : path_option("OUTPUT_DIR", "/output");
        const Settings settings;
        const Scanner scanner = choose_scanner();
        const Catalog catalog(data, scanner);
        if (catalog.begin >= catalog.end)
            throw std::runtime_error("window_start must precede window_end");
        const Dates dates(catalog.begin);
        const Inputs events(data, "events", settings.io, settings.chunk);
        const Inputs clicks(data, "clicks", settings.io, settings.chunk);
        const Inputs links(data, "links", settings.io, settings.chunk);
        fs::create_directories(output / "user_daily");
        fs::create_directories(output / "campaign_daily");
        if (catalog.categories.size() <= 256 &&
            elapsed(catalog.end, dates.origin) <= std::numeric_limits<u32>::max()) {
            Engine<Atom>(settings, catalog, dates, events, clicks, links, output, scanner).run(start);
        } else {
            Engine<WideAtom>(settings, catalog, dates, events, clicks, links, output, scanner).run(start);
        }
    } catch (const std::exception& error) {
        std::fprintf(stderr, "shopping-w4: %s\n", error.what());
        return 1;
    }
}
