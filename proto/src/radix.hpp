#pragma once
#include "input.hpp"

#include <memory>

namespace SHOPPING_NAMESPACE {

class Slab {
    static constexpr size_t huge_page = 2 * 1024 * 1024;

    struct Free {
        size_t length = 0;
        void operator()(void* p) const noexcept { munmap(p, length); }
    };

    std::unique_ptr<void, Free> mapping_{nullptr, Free{}};
    std::byte* data_ = nullptr;

public:
#if defined(__linux__) && defined(MADV_HUGEPAGE)
    static constexpr size_t extent_bytes = 4 * huge_page;
#else
    static constexpr size_t extent_bytes = huge_page;
#endif

    explicit Slab(size_t bytes, [[maybe_unused]] bool huge) {
#if defined(__linux__) && defined(MADV_HUGEPAGE)
        const size_t length = huge ? ((bytes + huge_page - 1) & ~(huge_page - 1)) + huge_page : bytes;
#else
        const size_t length = bytes;
#endif
        int flags = MAP_PRIVATE | MAP_ANONYMOUS;
#ifdef MAP_NORESERVE
        flags |= MAP_NORESERVE;
#endif
        void* raw = mmap(nullptr, length, PROT_READ | PROT_WRITE, flags, -1, 0);
        if (raw == MAP_FAILED)
            throw std::bad_alloc();
        mapping_ = std::unique_ptr<void, Free>(raw, Free{length});
        data_ = static_cast<std::byte*>(raw);
#if defined(__linux__) && defined(MADV_HUGEPAGE)
        if (huge) {
            const size_t skip = -reinterpret_cast<uintptr_t>(raw) & (huge_page - 1);
            data_ += skip;
            (void)madvise(data_, length - huge_page, MADV_HUGEPAGE);
        } else {
            (void)madvise(raw, length, MADV_NOHUGEPAGE);
        }
#endif
    }

    std::byte* data() const { return data_; }
};

// Reserve virtual memory up front and charge pages only as workers touch
// them. The locked growth path handles unusually short or wide rows.
class SlabPool {
    const size_t capacity_;
    const bool huge_;
    Slab backing_;
    std::atomic<size_t> used_{0};
    std::mutex spill_mutex_;
    std::vector<Slab> spills_;
    std::byte* spill_next_ = nullptr;
    size_t spill_left_ = 0;

public:
    SlabPool(size_t capacity, bool huge) : capacity_(capacity), huge_(huge), backing_(capacity, huge) {}

    std::byte* take(size_t bytes) {
        const size_t aligned = (bytes + 63) & ~size_t{63};
        const size_t offset = used_.fetch_add(aligned, std::memory_order_relaxed);
        if (offset <= capacity_ && aligned <= capacity_ - offset) [[likely]]
            return backing_.data() + offset;
        std::lock_guard lock(spill_mutex_);
        if (aligned > spill_left_) {
            spill_left_ = std::max(aligned, 8 * Slab::extent_bytes);
            spill_next_ = spills_.emplace_back(spill_left_, huge_).data();
        }
        std::byte* out = spill_next_;
        spill_next_ += aligned;
        spill_left_ -= aligned;
        return out;
    }

    // Called between worker phases, once the event index and link endpoints
    // are dead. Their pages can hold the grouped histories without unmapping.
    void rewind() { used_.store(0, std::memory_order_relaxed); }
};

// Each worker packs its partition slices into a few mappings. The slices
// outlive the grouping phase and may be consumed by other workers.
template <class T>
class PartitionArena {
    static_assert(std::is_trivially_destructible_v<T>);
    SlabPool& pool_;
    std::byte* next_ = nullptr;
    size_t left_ = 0;

public:
    explicit PartitionArena(SlabPool& pool) : pool_(pool) {}

    T* allocate(size_t count) {
        if (!count)
            return nullptr;
        const size_t bytes = count * sizeof(T);
        if (bytes > left_) {
            left_ = std::max(bytes, Slab::extent_bytes);
            next_ = pool_.take(left_);
        }
        T* result = ::new (next_) T[count];
        next_ += bytes;
        left_ -= bytes;
        return result;
    }
};

// Worker-local head/tail pairs avoid shared writes during radix scatter.
// An extent supplies pages to every partition touched by its worker.
template <class T, size_t page_bytes = 4096>
class Bins {
    static_assert(std::is_trivially_copyable_v<T> && sizeof(T) <= page_bytes);
    static constexpr size_t per_page = std::bit_floor(page_bytes / sizeof(T));

    struct alignas(64) Page {
        Page* next;
        u32 first;
        alignas(64) T records[per_page];
    };
    struct Queue {
        Page* head = nullptr;
        Page* tail = nullptr;
        size_t size = 0;
    };
    static constexpr size_t pages_per_extent = Slab::extent_bytes / sizeof(Page);

    SlabPool& pool_;
    std::vector<std::byte*> extents_;
    std::byte* extent_ = nullptr;
    size_t left_ = 0;
    std::vector<Queue> queues_;

    Page* allocate() {
        if (!left_) {
            extent_ = pool_.take(Slab::extent_bytes);
            extents_.push_back(extent_);
            left_ = pages_per_extent;
        }
        auto* page = ::new(extent_ + (--left_) * sizeof(Page)) Page;
        page->next = nullptr;
        const size_t first = ((extents_.size() - 1) * pages_per_extent + left_) * per_page;
        if (first > std::numeric_limits<u32>::max()) [[unlikely]]
            throw std::runtime_error("too many records in a worker's radix pages");
        page->first = u32(first);
        return page;
    }

    Page& tail(size_t bucket, size_t& pos) {
        Queue& q = queues_[bucket];
        pos = q.size++ & (per_page - 1);
        if (!pos) {
            Page* page = allocate();
            if (q.tail)
                q.tail->next = page;
            else
                q.head = page;
            q.tail = page;
        }
        return *q.tail;
    }

public:
    Bins(size_t count, SlabPool& pool) : pool_(pool), queues_(count) {}

    void add(size_t bucket, const T& record) {
        size_t pos;
        auto& page = tail(bucket, pos);
        page.records[pos] = record;
    }

    u32 add_index(size_t bucket, const T& record) {
        size_t pos;
        auto& page = tail(bucket, pos);
        page.records[pos] = record;
        return page.first + u32(pos);
    }

    T& at(u32 index) {
        size_t page = index / per_page;
        auto* base = extents_[page / pages_per_extent];
        auto* data = reinterpret_cast<Page*>(base + (page % pages_per_extent) * sizeof(Page));
        return data->records[index & (per_page - 1)];
    }

    size_t size(size_t bucket) const { return queues_[bucket].size; }

    template <class F>
    void visit_pages(size_t bucket, F f) {
        const auto& q = queues_[bucket];
        size_t left = q.size;
        for (Page* page = q.head; page; page = page->next) {
            size_t n = std::min(left, per_page);
            f(page->records, n);
            left -= n;
        }
    }

    template <class F>
    void visit(size_t bucket, F f) {
        visit_pages(bucket, [&](T* records, size_t n) {
            for (size_t i = 0; i < n; ++i)
                f(records[i]);
        });
    }


    template <class F>
    void visit_indexed(size_t bucket, F f) {
        const auto& q = queues_[bucket];
        size_t left = q.size;
        for (Page* page = q.head; page; page = page->next) {
            size_t n = std::min(left, per_page);
            for (size_t i = 0; i < n; ++i)
                f(page->records[i], page->first + u32(i));
            left -= n;
        }
    }
};

} // namespace SHOPPING_NAMESPACE
