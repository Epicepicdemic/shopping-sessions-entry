#pragma once
#include "keys.hpp"
#include "parquet.hpp"

#include <span>

namespace w3 {

template <class Time> struct AdClick {
    Time time;
    u32 campaign;
    u64 key;
};

struct Atom {
    u32 time;
    u32 purchase; // high byte is category, low 24 bits are cents
    Buy bought() const { return {purchase & 0xffffff, purchase >> 24}; }
};

struct WideAtom {
    i64 time;
    Buy buy;
    Buy bought() const { return buy; }
};

inline u64 elapsed(i64 later, i64 earlier) { return u64(later) - u64(earlier); }

class Campaigns {
    i64 first_;
    size_t days_, count_;

public:
    std::vector<CampaignCount> dense;
    std::unordered_map<u64, CampaignCount> sparse;

    Campaigns(const Catalog& catalog)
        : first_(floor_div(catalog.begin, day_ms)),
          days_(size_t(std::max<i64>(1, floor_div(catalog.end, day_ms) - first_ + 1))),
          count_(catalog.campaigns.size()) {
        if (days_ <= 366 && count_ <= 262'144 / days_)
            dense.resize(count_ * days_);
    }

    CampaignCount& at(int32_t date, u32 campaign) {
        if (!dense.empty())
            return dense[size_t(date - first_) * count_ + campaign];
        return sparse[(u64(u32(date)) << 32) | campaign];
    }

    void merge(const Campaigns& other) {
        for (size_t i = 0; i < dense.size(); ++i)
            dense[i].add(other.dense[i]);
        for (const auto& [key, value] : other.sparse)
            sparse[key].add(value);
    }

    CampaignColumns columns(const Catalog& catalog) const {
        CampaignColumns cols;
        auto make_row = [&](int32_t date, u32 c, CampaignCount count) {
            const auto& [name, channel] = catalog.campaigns[c];
            cols.row(name, channel, date, count);
        };
        for (size_t i = 0; i < dense.size(); ++i)
            make_row(int32_t(first_ + i / count_), u32(i % count_), dense[i]);
        for (const auto& [key, count] : sparse)
            make_row(int32_t(key >> 32), u32(key), count);
        return cols;
    }
};

// All events in a fixed 30-minute cell are connected. The first and last
// timestamps determine how a cell joins its neighbors; individual purchases
// still contribute at the session's day.
class EventCells {
    struct Cell {
        u32 first, last, events, purchase;
    };

    struct Purchase {
        u32 value, next;
    };

    std::vector<Cell> cells_;
    std::vector<u64> occupied_;
    std::vector<Purchase> purchases_;
    u32 first_ = no_token;

public:
    explicit EventCells(size_t count) : cells_(count), occupied_((count + 63) / 64) {}

    void reset() {
        std::fill(occupied_.begin(), occupied_.end(), 0);
        purchases_.clear();
        first_ = no_token;
    }

    void add(std::span<const Atom> events) {
        for (const Atom& event : events) {
            const u32 index = event.time / u32(gap_ms);
            const u64 mask = u64{1} << (index % 64);
            u64& present = occupied_[index / 64];
            Cell& cell = cells_[index];
            if (present & mask) {
                cell.first = std::min(cell.first, event.time);
                cell.last = std::max(cell.last, event.time);
                ++cell.events;
            } else {
                present |= mask;
                cell = {event.time, event.time, 1, no_token};
                first_ = std::min(first_, index);
            }
            if (event.purchase) {
                u32 next = std::exchange(cell.purchase, u32(purchases_.size()));
                purchases_.push_back({event.purchase, next});
            }
        }
    }

    u32 begin() const { return cells_[first_].first; }

    bool connected() const {
        u32 previous = begin();
        for (size_t word = first_ / 64; word < occupied_.size(); ++word) {
            for (u64 bits = occupied_[word]; bits; bits &= bits - 1) {
                const auto& cell = cells_[word * 64 + std::countr_zero(bits)];
                if (cell.first - previous > gap_ms)
                    return false;
                previous = cell.last;
            }
        }
        return true;
    }

    template <class Emit>
    void visit(Emit emit) const {
        for (size_t word = first_ / 64; word < occupied_.size(); ++word) {
            for (u64 bits = occupied_[word]; bits; bits &= bits - 1) {
                const auto& cell = cells_[word * 64 + std::countr_zero(bits)];
                emit(cell.first, cell.last, cell.events, [&](auto buyer) {
                    for (u32 i = cell.purchase; i != no_token; i = purchases_[i].next) {
                        const u32 value = purchases_[i].value;
                        buyer(Buy{value & 0xffffff, value >> 24});
                    }
                });
            }
        }
    }
};

template <class Time>
class Reduce {
    struct Day {
        int32_t date;
        u32 sessions = 1, events = 0;
        u64 revenue = 0, top_total = 0;
        u32 top = no_token;
    };

    struct Start { Time time; int32_t day; };

    std::vector<u64> totals_;
    std::vector<u32> seen_category_, seen_campaign_, cover_;
    std::vector<Start> starts_;
    u32 day_stamp_ = 0, click_stamp_ = 0;
    i64 origin_;

    static u32 next_stamp(u32& stamp, std::vector<u32>& stamps) {
        if (++stamp == 0) {
            std::fill(stamps.begin(), stamps.end(), 0);
            ++stamp;
        }
        return stamp;
    }

    int32_t day_of(Time t) const {
        return int32_t(floor_div(origin_ + i64(t), day_ms));
    }

    void add_category(Day& day, Buy buy) {
        if (seen_category_[buy.category] != day_stamp_) {
            seen_category_[buy.category] = day_stamp_;
            totals_[buy.category] = 0;
        }
        u64 sum = (totals_[buy.category] += buy.cents);
        if (sum > day.top_total || (sum == day.top_total && buy.category < day.top)) {
            day.top = buy.category;
            day.top_total = sum;
        }
    }

    template <class Visit>
    void single_session(Time start, Visit visit, std::span<const AdClick<Time>> clicks,
                        const Keys& keys, UserColumns& output, Campaigns& campaigns) {
        Day day{day_of(start)};
        next_stamp(day_stamp_, seen_category_);
        visit([&](u32 count, auto purchases) {
            day.events += count;
            purchases([&](Buy buy) {
                day.revenue += buy.cents;
                add_category(day, buy);
            });
        });

        const AdClick<Time>* latest = nullptr;
        if (!clicks.empty()) {
            next_stamp(click_stamp_, seen_campaign_);
            for (const auto& click : clicks) {
                if (click.time > start || elapsed(start, click.time) > week_ms)
                    continue;
                const u32 campaign = click.campaign;
                if (seen_campaign_[campaign] != click_stamp_) {
                    seen_campaign_[campaign] = click_stamp_;
                    ++campaigns.at(day.date, campaign).assists;
                }
                if (!latest || click.time > latest->time ||
                    (click.time == latest->time && keys.before(Id{click.key}, Id{latest->key})))
                    latest = &click;
            }
        }
        auto& tally = campaigns.at(day.date, latest ? latest->campaign : 0);
        ++tally.sessions;
        tally.purchasing += day.revenue != 0;
        tally.revenue += day.revenue;
        output.row(day.date, 1, day.events, day.revenue, day.top);
    }

    void credit_many(std::span<const AdClick<Time>> clicks, Campaigns& campaigns) {
        std::fill(cover_.begin(), cover_.end(), 0);
        size_t first = 0, past = 0;
        for (auto click : clicks) {
            while (first < starts_.size() && starts_[first].time < click.time)
                ++first;
            if (first == starts_.size())
                break;
            past = std::max(past, first);
            while (past < starts_.size() && elapsed(starts_[past].time, click.time) <= week_ms)
                ++past;
            size_t at = std::max(first, size_t(cover_[click.campaign]));
            if (at >= past)
                continue;
            cover_[click.campaign] = u32(past);
            do {
                int32_t date = starts_[at].day;
                size_t lo = at++;
                while (at < past && starts_[at].day == date)
                    ++at;
                campaigns.at(date, click.campaign).assists += at - lo;
            } while (at < past);
        }
    }

public:
    Reduce(const Catalog& catalog, i64 origin)
        : totals_(catalog.categories.size()), seen_category_(catalog.categories.size()),
          seen_campaign_(catalog.campaigns.size()), cover_(catalog.campaigns.size()), origin_(origin) {}

    template <class Event>
    void one(Time start, std::span<const Event> hits, std::span<const AdClick<Time>> clicks,
             const Keys& keys, UserColumns& output, Campaigns& campaigns) {
        single_session(start, [&](auto emit) {
            emit(u32(hits.size()), [&](auto buyer) {
                for (const auto& hit : hits) {
                    const Buy buy = hit.bought();
                    if (buy.cents)
                        buyer(buy);
                }
            });
        }, clicks, keys, output, campaigns);
    }

    void one(const EventCells& cells, std::span<const AdClick<Time>> clicks,
             const Keys& keys, UserColumns& output, Campaigns& campaigns) requires std::is_same_v<Time, u32> {
        single_session(cells.begin(), [&](auto emit) {
            cells.visit([&](u32, u32, u32 count, auto purchases) { emit(count, purchases); });
        }, clicks, keys, output, campaigns);
    }

    template <class Visit>
    void timeline(Time session, Visit visit, std::span<const AdClick<Time>> clicks,
                  UserColumns& output, Campaigns& campaigns) {
        const bool many_clicks = clicks.size() > 96;
        starts_.clear();
        Time previous = session;
        Day day{day_of(session)};
        next_stamp(day_stamp_, seen_category_);
        size_t first = 0, past = 0;
        u64 session_revenue = 0;
        auto close_session = [&] {
            while (past < clicks.size() && clicks[past].time <= session)
                ++past;
            while (first < past && elapsed(session, clicks[first].time) > week_ms)
                ++first;
            u32 campaign = first == past ? 0 : clicks[past - 1].campaign;
            auto& tally = campaigns.at(day.date, campaign);
            ++tally.sessions;
            tally.purchasing += session_revenue != 0;
            tally.revenue += session_revenue;
            session_revenue = 0;

            if (many_clicks) {
                starts_.push_back({session, day.date});
            } else if (first != past) {
                next_stamp(click_stamp_, seen_campaign_);
                for (size_t i = first; i < past; ++i) {
                    u32 c = clicks[i].campaign;
                    if (seen_campaign_[c] != click_stamp_) {
                        seen_campaign_[c] = click_stamp_;
                        ++campaigns.at(day.date, c).assists;
                    }
                }
            }
        };
        auto close_day = [&] {
            output.row(day.date, day.sessions, day.events, day.revenue, day.top);
        };
        visit([&](Time first_time, Time last_time, u32 events, auto purchases) {
            if (elapsed(first_time, previous) > gap_ms) {
                close_session();
                session = first_time;
                int32_t date = day_of(session);
                if (date != day.date) {
                    close_day();
                    day = Day{date};
                    next_stamp(day_stamp_, seen_category_);
                } else {
                    ++day.sessions;
                }
            }
            previous = last_time;
            day.events += events;
            purchases([&](Buy buy) {
                day.revenue += buy.cents;
                session_revenue += buy.cents;
                add_category(day, buy);
            });
        });
        close_session();
        close_day();
        if (many_clicks)
            credit_many(clicks, campaigns);
    }

    template <class Event>
    void person(std::span<const Event> hits, std::span<const AdClick<Time>> clicks,
                UserColumns& output, Campaigns& campaigns) {
        timeline(hits[0].time, [&](auto emit) {
            for (const Event& hit : hits) {
                emit(hit.time, hit.time, 1, [&](auto buyer) {
                    Buy buy = hit.bought();
                    if (buy.cents)
                        buyer(buy);
                });
            }
        }, clicks, output, campaigns);
    }

    void person(const EventCells& cells, std::span<const AdClick<Time>> clicks,
                UserColumns& output, Campaigns& campaigns) requires std::is_same_v<Time, u32> {
        timeline(cells.begin(), [&](auto emit) { cells.visit(emit); }, clicks, output, campaigns);
    }
};

template <class Time>
void sort_clicks(std::span<AdClick<Time>> clicks, const Keys& names) {
    auto before = [&](const auto& a, const auto& b) {
        if (a.time != b.time)
            return a.time < b.time;
        // For equal timestamps the selected click is the last in the sweep.
        return names.before(Id{b.key}, Id{a.key});
    };
    if (clicks.size() > 12) {
        std::sort(clicks.begin(), clicks.end(), before);
        return;
    }
    for (size_t i = 1; i < clicks.size(); ++i) {
        auto value = clicks[i];
        size_t j = i;
        while (j && before(value, clicks[j - 1])) {
            clicks[j] = clicks[j - 1];
            --j;
        }
        clicks[j] = value;
    }
}

} // namespace w3
