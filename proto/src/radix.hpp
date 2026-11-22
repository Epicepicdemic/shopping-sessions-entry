#pragma once
#include "input.hpp"

#include <memory>

namespace w3 {

// Worker-local head/tail pairs avoid shared writes during radix scatter.
// An extent supplies pages to every partition touched by its worker.
template <class T, size_t page_bytes = 4096>
class Bins {
    static_assert(std::is_trivially_copyable_v<T> && sizeof(T) <= page_bytes);
    static constexpr size_t per_page = std::bit_floor(page_bytes / sizeof(T));

    struct alignas(64) Page {
        Page* next;
        size_t ordinal;
        alignas(64) T records[per_page];
    };
    struct Queue {
        Page* head = nullptr;
        Page* tail = nullptr;
        size_t size = 0;
    };
    static constexpr size_t huge_page = 2 * 1024 * 1024;
#if defined(__linux__) && defined(MADV_HUGEPAGE)
    static constexpr size_t extent_bytes = 4 * huge_page;
    struct Free {
        void operator()(void* p) const { munmap(p, extent_bytes); }
    };

    static void* new_extent() {
        void* raw = mmap(nullptr, extent_bytes + huge_page, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (raw == MAP_FAILED)
            throw std::bad_alloc();
        size_t skip = (huge_page - (reinterpret_cast<uintptr_t>(raw) & (huge_page - 1)))
                      & (huge_page - 1);
        auto* base = static_cast<std::byte*>(raw) + skip;
        if (skip)
            munmap(raw, skip);
        munmap(base + extent_bytes, huge_page - skip);
        madvise(base, extent_bytes, MADV_HUGEPAGE);
        return base;
    }
#else
    static constexpr size_t extent_bytes = huge_page;
    struct Free {
        void operator()(void* p) const { std::free(p); }
    };

    static void* new_extent() {
        void* raw = std::aligned_alloc(64, extent_bytes);
        if (!raw)
            throw std::bad_alloc();
        return raw;
    }
#endif
    static constexpr size_t pages_per_extent = extent_bytes / sizeof(Page);

    std::vector<std::unique_ptr<void, Free>> extents_;
    std::byte* extent_ = nullptr;
    size_t left_ = 0;
    std::vector<Queue> queues_;

    Page* allocate() {
        if (!left_) {
            std::unique_ptr<void, Free> region(new_extent());
            extent_ = static_cast<std::byte*>(region.get());
            extents_.push_back(std::move(region));
            left_ = pages_per_extent;
        }
        auto* page = ::new(extent_ + (--left_) * sizeof(Page)) Page;
        page->next = nullptr;
        return page;
    }

    Page& tail(size_t bucket, size_t& pos) {
        Queue& q = queues_[bucket];
        pos = q.size++ & (per_page - 1);
        if (!pos) {
            Page* page = allocate();
            page->ordinal = (extents_.size() - 1) * pages_per_extent + left_;
            if (q.tail)
                q.tail->next = page;
            else
                q.head = page;
            q.tail = page;
        }
        return *q.tail;
    }

public:
    explicit Bins(size_t count) : queues_(count) {}

    void add(size_t bucket, const T& record) {
        size_t pos;
        auto& page = tail(bucket, pos);
        page.records[pos] = record;
    }

    u32 add_index(size_t bucket, const T& record) {
        size_t pos;
        auto& page = tail(bucket, pos);
        size_t index = page.ordinal * per_page + pos;
        if (index >= std::numeric_limits<u32>::max())
            throw std::runtime_error("too many records in a worker's radix pages");
        page.records[pos] = record;
        return u32(index);
    }

    T& at(u32 index) {
        size_t page = index / per_page;
        auto* base = static_cast<std::byte*>(extents_[page / pages_per_extent].get());
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
            size_t base = page->ordinal * per_page;
            if (base + n > std::numeric_limits<u32>::max())
                throw std::runtime_error("too many records in a worker's radix pages");
            for (size_t i = 0; i < n; ++i)
                f(page->records[i], u32(base + i));
            left -= n;
        }
    }
};

} // namespace w3
