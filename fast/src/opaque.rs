//! The opaque-ID engine (on unless `SHOPPING_OPAQUE=0`). IDs are values, never
//! numbers: short IDs use injective inline keys. Indirect IDs use a tagged
//! hash whose original bytes are compared whenever two keys match.
//!
//! 1. One parallel pass reads links, events and clicks. Every user ID is
//!    interned into a dense index `u` (a lock-free table); each link line joins
//!    its two indices in a concurrent union-find. Each event copy is filed under
//!    a bucket of its event key with its ingest time, user index and payload;
//!    each valid click is kept with its user index.
//! 2. Each link component becomes a person named by its byte-wise smallest ID;
//!    every other index is a person of its own. Persons get dense numbers `p`.
//! 3. Each event bucket keeps, per event key, the copy with the smallest ingest
//!    time (dedup before validation), and files the surviving valid events by
//!    person. Matching long keys have their original bytes compared in place.
//! 4. Each person bucket is sorted by person and handed to the shared `Sink`.
//!
//! Anything outside the engine's limits (table capacity, record widths, a long
//! key shared by different bytes) returns `None` before any output is written,
//! and the exact solver runs instead.

use std::env;
use std::io;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use crate::id::{self, FallbackArena, PackedId, fxhash_bytes};
use crate::parquet::{CampaignDailyRow, UserCols, write_campaign_daily_file, write_user_daily_cols};
use crate::parse::{
    CLICK_LOOKBACK_MS, DateCache, LATE_LIMIT_MS, MS_PER_DAY, MS_PER_SECOND, parse_f1_seconds_cached, parse_purchase_price,
    parse_ts_cached,
};
use crate::scan;
use crate::sink::{CampAcc, Sink};
use crate::{Catalog, SCAN_CHUNK};

const LONG: u64 = 1 << 63;
/// PackedId uses bits 0..60. Inline suffixes use bits 60..63 as a tag.
const RAW_SUFFIX: u64 = 1 << 60;
const FALLBACK: u64 = 1 << 63;
const INVALID: u32 = u32::MAX;
const FAILED_CLAIM: u64 = u64::MAX;
/// Offsets in one long-ID arena (64 MiB each).
const ARENA_BITS: u32 = 26;
/// Records per event bucket the dedup table is sized for
/// (`SHOPPING_OPQ_EB` overrides, for measurement).
const BUCKET_TARGET: usize = 32 * 1024;
/// Events per person bucket (`SHOPPING_OPQ_PB` overrides, for measurement).
const PERSON_TARGET: usize = 32 * 1024;

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name).ok().and_then(|v| v.parse().ok()).filter(|&v| v > 0).unwrap_or(default)
}

pub fn enabled() -> bool {
    env::var("SHOPPING_OPAQUE").ok().as_deref() != Some("0")
}

#[inline(always)]
fn mix64(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

/// Key of a non-empty ID inside a scan buffer (16 readable bytes). The
/// common short IDs with a known prefix have an exact, bytewise suffix key.
/// The other forms are an exact PackedId or a tagged 63-bit hash.
#[inline(always)]
fn id_key<const PREFIX: u8>(bytes: &[u8]) -> u64 {
    if (bytes.len() == 9 || bytes.len() == 8) && bytes[0] == PREFIX {
        let suffix = if bytes.len() == 9 {
            u64::from_be_bytes(bytes[1..9].try_into().unwrap())
        } else {
            u64::from_be_bytes(bytes[..8].try_into().unwrap()) << 8
        };
        // The leading suffix byte leaves the PackedId and LONG tags disjoint.
        // A zero last byte denotes the seven-byte suffix, so a nine-byte ID
        // ending in NUL uses the general codec.
        if (RAW_SUFFIX..LONG).contains(&suffix) && (suffix as u8 != 0 || bytes.len() == 8) {
            return suffix;
        }
    }
    match id::try_pack_padded(bytes) {
        Some(p) => p.0,
        None => fxhash_bytes(bytes) | LONG,
    }
}

/// `id_key` for event IDs, with a test hook (`SHOPPING_OPQ_COLLIDE=1`): every
/// long event ID gets one key, so different IDs share a key and the bucket
/// check must send the run to the exact solver.
#[inline(always)]
fn event_key(bytes: &[u8]) -> u64 {
    let k = id_key::<b'e'>(bytes);
    if k & LONG != 0 && COLLIDE.load(Ordering::Relaxed) { LONG | 1 } else { k }
}

static COLLIDE: AtomicBool = AtomicBool::new(false);

unsafe extern "C" {
    fn madvise(addr: *mut core::ffi::c_void, len: usize, advice: i32) -> i32;
}

/// Ask for transparent huge pages over a fresh (untouched) allocation.
fn huge_pages(p: *const u8, bytes: usize) {
    const H: usize = 2 << 20;
    if env::var("SHOPPING_HUGE").ok().as_deref() == Some("0") {
        return;
    }
    let start = (p as usize).div_ceil(H) * H;
    let end = (p as usize + bytes) / H * H;
    if end > start {
        // SAFETY: advice only, inside our own allocation.
        unsafe {
            madvise(start as *mut core::ffi::c_void, end - start, 14); // MADV_HUGEPAGE
        }
    }
}

/// Map a chunk's pages in one call before scanning it (MADV_POPULATE_READ;
/// a kernel without it just returns an error).
fn populate_read(bytes: &[u8], first: usize, stop: usize) {
    let base = bytes.as_ptr() as usize;
    let start = (base + first) / 4096 * 4096;
    let end = (base + stop).div_ceil(4096) * 4096;
    if end > start {
        // SAFETY: advice only, inside the mapping.
        unsafe {
            madvise(start as *mut core::ffi::c_void, end - start, 22);
        }
    }
}

/// Give a consumed buffer's whole pages back to the kernel now, from the
/// thread that consumed it, instead of all at exit (`SHOPPING_DISCARD=1`;
/// off by default: the TLB shootdowns cost the run what exit saves). Nothing
/// reads the buffer afterwards, and it is never dropped.
fn discard<T>(v: &[T], cap: usize) {
    if !DISCARD.load(Ordering::Relaxed) {
        return;
    }
    let bytes = cap * std::mem::size_of::<T>();
    let start = (v.as_ptr() as usize).div_ceil(4096) * 4096;
    let end = (v.as_ptr() as usize + bytes) / 4096 * 4096;
    if end > start {
        // SAFETY: whole pages inside the buffer's own allocation, unused from here on.
        unsafe {
            madvise(start as *mut core::ffi::c_void, end - start, 4); // MADV_DONTNEED
        }
    }
}

static DISCARD: AtomicBool = AtomicBool::new(false);

fn zeroed_u64(n: usize) -> Vec<AtomicU64> {
    let mut v: Vec<u64> = vec![0; n];
    huge_pages(v.as_ptr() as *const u8, n * 8);
    let (p, len, cap) = (v.as_mut_ptr(), v.len(), v.capacity());
    std::mem::forget(v);
    // SAFETY: AtomicU64 has the same size, alignment and bit validity as u64.
    unsafe { Vec::from_raw_parts(p as *mut AtomicU64, len, cap) }
}

fn zeroed_u32(n: usize) -> Vec<AtomicU32> {
    let mut v: Vec<u32> = vec![0; n];
    huge_pages(v.as_ptr() as *const u8, n * 4);
    let (p, len, cap) = (v.as_mut_ptr(), v.len(), v.capacity());
    std::mem::forget(v);
    // SAFETY: as above, for u32.
    unsafe { Vec::from_raw_parts(p as *mut AtomicU32, len, cap) }
}

/// Append-only byte arenas, one per scan worker, with fixed capacity so that a
/// published reference stays valid while the owner keeps appending.
struct Arenas {
    bufs: Vec<(*mut u8, usize)>,
    _keep: Vec<Vec<u8>>,
}

// SAFETY: a worker writes only past every reference it has published; other
// threads read only published ranges (Release/Acquire through the table).
unsafe impl Sync for Arenas {}
unsafe impl Send for Arenas {}

impl Arenas {
    fn new(n: usize, cap: usize) -> Self {
        let mut keep: Vec<Vec<u8>> = (0..n).map(|_| Vec::with_capacity(cap.min(1 << ARENA_BITS))).collect();
        let bufs = keep.iter_mut().map(|v| (v.as_mut_ptr(), v.capacity().min(1 << ARENA_BITS))).collect();
        Self { bufs, _keep: keep }
    }

    /// Copy `bytes` into arena `a` at `*len`; `None` when it does not fit.
    #[inline]
    fn store(&self, a: usize, len: &mut usize, bytes: &[u8]) -> Option<u32> {
        let (ptr, cap) = self.bufs[a];
        let need = 2 + bytes.len();
        if bytes.len() > 0xFFFF || *len + need > cap {
            return None;
        }
        let off = *len;
        // SAFETY: `off + need <= cap` inside this worker's own allocation.
        unsafe {
            let p = ptr.add(off);
            let l = (bytes.len() as u16).to_le_bytes();
            *p = l[0];
            *p.add(1) = l[1];
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), p.add(2), bytes.len());
        }
        *len += need;
        Some(((a as u32) << ARENA_BITS) | off as u32)
    }

    #[inline]
    fn bytes(&self, r: u32) -> &[u8] {
        let (ptr, _) = self.bufs[(r >> ARENA_BITS) as usize];
        let off = (r & ((1 << ARENA_BITS) - 1)) as usize;
        // SAFETY: `r` was published by `store` after the bytes were written.
        unsafe {
            let p = ptr.add(off);
            let len = u16::from_le_bytes([*p, *p.add(1)]) as usize;
            std::slice::from_raw_parts(p.add(2), len)
        }
    }
}

/// Lock-free user-ID table: slot `2i` holds the key (0 = empty), slot `2i+1`
/// holds `(u + 1) | ref << 32` once published (0 = not yet).
struct Users {
    slots: Vec<AtomicU64>,
    mask: usize,
    /// Union-find parent of each index (only ever points to a smaller index).
    parent: Vec<AtomicU32>,
    /// Each index's ID: its inline key, or `LONG` | arena reference.
    ukey: Vec<AtomicU64>,
    next_block: AtomicU32,
    block: u32,
    limit_u: u32,
    used: AtomicUsize,
    max_used: usize,
    arenas: Arenas,
    overflow: AtomicBool,
    /// The table held more IDs than its size allows (a larger table succeeds).
    table_full: AtomicBool,
}

struct Ctx {
    arena: usize,
    arena_len: usize,
    u_next: u32,
    u_end: u32,
    inserted: usize,
}

/// A user ID whose table slot was prefetched for the pending batch.
#[derive(Clone, Copy)]
struct Pend {
    ptr: *const u8,
    len: u32,
    /// Event bucket or the side of a link.
    dest: u32,
    key: u64,
    h: u64,
}

impl Pend {
    #[inline(always)]
    fn bytes<'a>(&self) -> &'a [u8] {
        // SAFETY: the ID lies in the chunk being scanned, and every pending
        // lookup is resolved before the chunk's buffer is reused or released.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len as usize) }
    }
}

const MAX_BATCH: usize = 64;

/// Lookups per batch (`SHOPPING_OPQ_DEPTH`, default 16, at most 64).
fn depth() -> usize {
    env::var("SHOPPING_OPQ_DEPTH").ok().and_then(|v| v.parse().ok()).unwrap_or(16).clamp(1, MAX_BATCH)
}

/// Parse and prefetch a batch; resolve its lookups in input order.
struct Batch<T: Copy> {
    buf: [(Pend, T); MAX_BATCH],
    n: usize,
    depth: usize,
}

impl<T: Copy> Batch<T> {
    fn new(zero: T, depth: usize) -> Self {
        let z = Pend { ptr: std::ptr::null(), len: 0, dest: 0, key: 0, h: 0 };
        Self { buf: [(z, zero); MAX_BATCH], n: 0, depth }
    }

    #[inline(always)]
    fn push(&mut self, p: (Pend, T)) -> bool {
        self.buf[self.n] = p;
        self.n += 1;
        self.n == self.depth
    }

    #[inline(always)]
    fn take(&mut self) -> &[(Pend, T)] {
        let n = std::mem::take(&mut self.n);
        &self.buf[..n]
    }
}

impl Users {
    fn new(slots_pow2: usize, workers: usize, arena_cap: usize) -> Self {
        let block = (slots_pow2 / (16 * workers.max(1))).clamp(64, 4096) as u32;
        let max_used = slots_pow2 / 4 * 3;
        let limit_u = (max_used + 2 * block as usize * workers.max(1)).min(u32::MAX as usize - 1) as u32;
        Self {
            slots: zeroed_u64(2 * slots_pow2),
            mask: slots_pow2 - 1,
            parent: zeroed_u32(limit_u as usize),
            ukey: zeroed_u64(limit_u as usize),
            next_block: AtomicU32::new(0),
            block,
            limit_u,
            used: AtomicUsize::new(0),
            max_used,
            arenas: Arenas::new(workers, arena_cap),
            overflow: AtomicBool::new(false),
            table_full: AtomicBool::new(false),
        }
    }

    fn ctx(&self, worker: usize) -> Ctx {
        Ctx { arena: worker, arena_len: 0, u_next: 0, u_end: 0, inserted: 0 }
    }

    #[inline]
    fn wait(&self, i: usize) -> u64 {
        loop {
            let v = self.slots[2 * i + 1].load(Ordering::Acquire);
            if v != 0 {
                return v;
            }
            std::hint::spin_loop();
        }
    }

    /// The index of `bytes` (non-empty, inside a scan buffer), inserting it if
    /// new; `INVALID` once the table is over capacity.
    /// Start loading the slot a lookup of hash `h` begins at.
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        // SAFETY: a prefetch of an in-bounds address.
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            _mm_prefetch(self.slots.as_ptr().add(2 * ((h as usize) & self.mask)) as *const i8, _MM_HINT_T0);
        }
        #[cfg(target_arch = "aarch64")]
        // SAFETY: the pointer is a slot in the table and `prfm` only hints.
        unsafe {
            let slot = self.slots.as_ptr().add(2 * ((h as usize) & self.mask));
            core::arch::asm!("prfm pldl1keep, [{slot}]", slot = in(reg) slot,
                options(readonly, nostack, preserves_flags));
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let _ = h;
    }

    /// A pending lookup for `bytes` (non-empty, inside a scan buffer), with its
    /// slot prefetched.
    #[inline(always)]
    fn pend(&self, bytes: &[u8], dest: u32) -> Pend {
        let key = id_key::<b'u'>(bytes);
        let h = mix64(key);
        self.prefetch(h);
        Pend { ptr: bytes.as_ptr(), len: bytes.len() as u32, dest, key, h }
    }

    /// `intern_slow` with the key computed here (tests).
    #[cfg(test)]
    fn intern(&self, bytes: &[u8], ctx: &mut Ctx) -> u32 {
        let key = id_key::<b'u'>(bytes);
        self.intern_slow(bytes, key, mix64(key), ctx)
    }

    /// The index of an ID with key `key` and hash `h`, inserting it if new;
    /// `INVALID` once the table is over capacity.
    #[inline]
    fn intern_slow(&self, bytes: &[u8], key: u64, h: u64, ctx: &mut Ctx) -> u32 {
        let mut i = (h as usize) & self.mask;
        let mut probes = 0u32;
        loop {
            let ks = &self.slots[2 * i];
            // The key reserves a slot. The separate value's Release/Acquire
            // publishes the user record and any bytes in its arena.
            let mut k = ks.load(Ordering::Relaxed);
            if k == 0 {
                match ks.compare_exchange(0, key, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => return self.claim(i, key, bytes, ctx),
                    Err(actual) => k = actual,
                }
            }
            if k == key {
                let v = self.wait(i);
                if v == FAILED_CLAIM {
                    return INVALID;
                }
                if key & LONG == 0 || self.arenas.bytes((v >> 32) as u32) == bytes {
                    return (v as u32).wrapping_sub(1);
                }
            }
            i = (i + 1) & self.mask;
            probes += 1;
            if probes > 1024 || (probes & 15 == 0 && self.overflow.load(Ordering::Relaxed)) {
                // A chain this long at 3/4 load means colliding hashes, which
                // a larger table does not fix: the exact solver runs.
                self.overflow.store(true, Ordering::Relaxed);
                return INVALID;
            }
        }
    }

    #[inline]
    fn claim(&self, i: usize, key: u64, bytes: &[u8], ctx: &mut Ctx) -> u32 {
        if ctx.u_next == ctx.u_end {
            let b = self.next_block.fetch_add(1, Ordering::Relaxed) as u64;
            let start = b * self.block as u64;
            if start + self.block as u64 > self.limit_u as u64 {
                // Publish something so no reader waits forever; the run is void.
                self.table_full.store(true, Ordering::Relaxed);
                self.overflow.store(true, Ordering::Relaxed);
                self.slots[2 * i + 1].store(FAILED_CLAIM, Ordering::Release);
                return INVALID;
            }
            ctx.u_next = start as u32;
            ctx.u_end = (start + self.block as u64) as u32;
        }
        let u = ctx.u_next;
        ctx.u_next += 1;
        let mut val = u as u64 + 1;
        let ukey = if key & LONG != 0 {
            match self.arenas.store(ctx.arena, &mut ctx.arena_len, bytes) {
                Some(r) => {
                    val |= (r as u64) << 32;
                    LONG | r as u64
                }
                None => {
                    self.overflow.store(true, Ordering::Relaxed);
                    self.slots[2 * i + 1].store(FAILED_CLAIM, Ordering::Release);
                    return INVALID;
                }
            }
        } else {
            key
        };
        self.parent[u as usize].store(u, Ordering::Relaxed);
        self.ukey[u as usize].store(ukey, Ordering::Relaxed);
        self.slots[2 * i + 1].store(val, Ordering::Release);
        ctx.inserted += 1;
        if ctx.inserted & 1023 == 0 && self.used.fetch_add(1024, Ordering::Relaxed) + 1024 > self.max_used {
            self.table_full.store(true, Ordering::Relaxed);
            self.overflow.store(true, Ordering::Relaxed);
        }
        u
    }

    #[inline]
    fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent[x as usize].load(Ordering::Relaxed);
            if p == x {
                return x;
            }
            let g = self.parent[p as usize].load(Ordering::Relaxed);
            if g != p {
                let _ = self.parent[x as usize].compare_exchange_weak(p, g, Ordering::Relaxed, Ordering::Relaxed);
            }
            x = g;
        }
    }

    /// Join two indices: the root with the larger index points to the smaller.
    #[inline]
    fn union(&self, a: u32, b: u32) {
        loop {
            let ra = self.find(a);
            let rb = self.find(b);
            if ra == rb {
                return;
            }
            let (hi, lo) = if ra > rb { (ra, rb) } else { (rb, ra) };
            if self.parent[hi as usize]
                .compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// The ID bytes of index `u` (`buf` holds an unpacked short ID).
    #[inline]
    fn id_bytes<'a>(&'a self, u: u32, buf: &'a mut [u8; 10]) -> &'a [u8] {
        let k = self.ukey[u as usize].load(Ordering::Relaxed);
        if k & LONG != 0 {
            self.arenas.bytes(k as u32)
        } else if k >= RAW_SUFFIX {
            buf[0] = b'u';
            buf[1..9].copy_from_slice(&k.to_be_bytes());
            &buf[..if k as u8 == 0 { 8 } else { 9 }]
        } else {
            id::unpack(PackedId(k), buf)
        }
    }

    /// Byte-wise `id(a) < id(b)`.
    #[inline]
    fn id_less(&self, a: u32, b: u32) -> bool {
        let ka = self.ukey[a as usize].load(Ordering::Relaxed);
        let kb = self.ukey[b as usize].load(Ordering::Relaxed);
        if (ka | kb) < LONG && (ka < RAW_SUFFIX) == (kb < RAW_SUFFIX) {
            return ka < kb; // Within either inline encoding, key order is byte order.
        }
        let (mut x, mut y) = ([0u8; 10], [0u8; 10]);
        self.id_bytes(a, &mut x) < self.id_bytes(b, &mut y)
    }
}

/// `N` records filling whole cache lines.
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Group<T: Copy, const N: usize>([T; N]);

/// Per-bucket staging: records gather in one group per bucket (cache
/// resident) and reach the bucket's span a whole group at a time through
/// non-temporal stores, so the scattered writes never read memory first.
/// All spans are carved from one region (one allocation instead of one per
/// bucket); a span that outgrows its share moves to a block of its own.
/// (Huge pages for these regions were measured: kernel compaction made them
/// slower and erratic, so none are requested.)
struct Stager<T: Copy, const N: usize> {
    stage: Vec<Group<T, N>>,
    fill: Vec<u8>,
    ptr: Vec<*mut Group<T, N>>,
    len: Vec<u32>,
    cap: Vec<u32>,
    owned: Vec<(*mut u8, std::alloc::Layout)>,
}

impl<T: Copy, const N: usize> Stager<T, N> {
    const WHOLE_LINES: () = assert!(std::mem::size_of::<Group<T, N>>() == N * std::mem::size_of::<T>()
        && std::mem::size_of::<Group<T, N>>() % 64 == 0 && N < 256);

    fn new(buckets: usize, reserve_records: usize, zero: T) -> Self {
        let () = Self::WHOLE_LINES;
        let per = reserve_records.div_ceil(N).clamp(1, u32::MAX as usize / 2);
        let gsize = std::mem::size_of::<Group<T, N>>();
        let bytes = buckets.max(1) * per * gsize;
        let align = 64;
        let layout = std::alloc::Layout::from_size_align(bytes, align).expect("stager layout");
        // SAFETY: non-zero size; checked for null below.
        let region = unsafe { std::alloc::alloc(layout) };
        if region.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        let base = region as *mut Group<T, N>;
        Self {
            stage: vec![Group([zero; N]); buckets],
            fill: vec![0; buckets],
            // SAFETY: each bucket's share lies inside the region.
            ptr: (0..buckets).map(|b| unsafe { base.add(b * per) }).collect(),
            len: vec![0; buckets],
            cap: vec![per as u32; buckets],
            owned: vec![(region, layout)],
        }
    }

    #[inline(always)]
    fn push(&mut self, b: usize, r: T) {
        let f = self.fill[b] as usize;
        self.stage[b].0[f] = r;
        if f + 1 == N {
            self.flush(b);
        } else {
            self.fill[b] = (f + 1) as u8;
        }
    }

    #[inline]
    fn flush(&mut self, b: usize) {
        if self.len[b] == self.cap[b] {
            self.grow(b);
        }
        // SAFETY: an unused whole group of a 64-byte aligned span.
        unsafe {
            store_group(self.ptr[b].add(self.len[b] as usize) as *mut u8, &self.stage[b] as *const _ as *const u8,
                std::mem::size_of::<Group<T, N>>());
        }
        self.len[b] += 1;
        self.fill[b] = 0;
    }

    /// Move bucket `b` to a block of twice its capacity.
    #[cold]
    fn grow(&mut self, b: usize) {
        let cap = (self.cap[b] as usize * 2).max(16);
        assert!(cap < u32::MAX as usize, "stager span too large");
        let layout =
            std::alloc::Layout::from_size_align(cap * std::mem::size_of::<Group<T, N>>(), 64).expect("stager layout");
        // SAFETY: non-zero size; checked for null.
        let block = unsafe { std::alloc::alloc(layout) } as *mut Group<T, N>;
        if block.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        store_fence(); // the streamed groups are in place before they are copied
        // SAFETY: `len` groups are initialized in the old span; the new block is larger.
        unsafe { std::ptr::copy_nonoverlapping(self.ptr[b], block, self.len[b] as usize) };
        self.owned.push((block as *mut u8, layout));
        self.ptr[b] = block;
        self.cap[b] = cap as u32;
    }

    /// Every bucket's records in push order.
    fn finish(mut self) -> Staged<T> {
        store_fence();
        let mut spans = Vec::with_capacity(self.ptr.len());
        for b in 0..self.ptr.len() {
            let f = self.fill[b] as usize;
            if f > 0 && self.len[b] == self.cap[b] {
                self.grow(b);
            }
            let p = self.ptr[b] as *mut T;
            // SAFETY: the partial group goes to the next unused group.
            unsafe { std::ptr::copy_nonoverlapping(self.stage[b].0.as_ptr(), p.add(self.len[b] as usize * N), f) };
            spans.push((p as *const T, self.len[b] as usize * N + f));
        }
        Staged { spans, owned: std::mem::take(&mut self.owned) }
    }
}

/// The buckets a `Stager` filled: one slice per bucket, over memory it owns.
struct Staged<T> {
    spans: Vec<(*const T, usize)>,
    owned: Vec<(*mut u8, std::alloc::Layout)>,
}

// SAFETY: read-only after `finish`; the memory is owned and freed only on drop.
unsafe impl<T: Sync> Send for Staged<T> {}
unsafe impl<T: Sync> Sync for Staged<T> {}

impl<T> Staged<T> {
    fn empty() -> Self {
        Self { spans: Vec::new(), owned: Vec::new() }
    }

    fn iter(&self) -> impl Iterator<Item = &[T]> + '_ {
        (0..self.spans.len()).map(move |b| &self[b])
    }
}

impl<T> std::ops::Index<usize> for Staged<T> {
    type Output = [T];
    #[inline(always)]
    fn index(&self, b: usize) -> &[T] {
        let (p, n) = self.spans[b];
        // SAFETY: `n` initialized records at `p`, inside an owned block.
        unsafe { std::slice::from_raw_parts(p, n) }
    }
}

impl<T> Drop for Staged<T> {
    fn drop(&mut self) {
        for &(p, layout) in &self.owned {
            // SAFETY: allocated by `Stager` with this layout.
            unsafe { std::alloc::dealloc(p, layout) };
        }
    }
}

#[inline(always)]
unsafe fn store_group(dst: *mut u8, src: *const u8, bytes: usize) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
    // SAFETY: both 32-byte aligned (groups are 64-byte aligned), `bytes` % 32 == 0.
    unsafe {
        use std::arch::x86_64::{__m256i, _mm256_load_si256, _mm256_stream_si256};
        for k in 0..bytes / 32 {
            _mm256_stream_si256((dst as *mut __m256i).add(k), _mm256_load_si256((src as *const __m256i).add(k)));
        }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx")))]
    // SAFETY: as above.
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst, bytes);
    }
}

#[inline(always)]
fn store_fence() {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: a fence.
    unsafe {
        std::arch::x86_64::_mm_sfence();
    }
}

#[inline(always)]
fn prefetch_ptr<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: a prefetch.
    unsafe {
        use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
        _mm_prefetch(p as *const i8, _MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = p;
}

/// One event copy in its event bucket (24 bytes).
#[repr(C)]
#[derive(Clone, Copy)]
struct ERec {
    key: u64,
    /// Ingest seconds minus `ingest_base`.
    ingest: u32,
    /// User index, or `INVALID` for a copy that fails validation.
    u: u32,
    /// `ts - window_start` in ms (valid copies).
    ts: u32,
    /// Price (bits 8..32) and category (bits 0..8).
    pc: u32,
}

/// One valid click (24 bytes); `cid` is a packed ID or `FALLBACK | worker << 40 | offset`.
#[repr(C)]
#[derive(Clone, Copy)]
struct CRec {
    ts: i64,
    cid: u64,
    u: u32,
    camp: u32,
}

/// One surviving event in its person bucket (12 bytes).
#[repr(C)]
#[derive(Clone, Copy)]
struct PRec {
    p: u32,
    ts: u32,
    pc: u32,
}

/// Six high bits identify the scan worker that owns the long ID's bytes.
const EVIDENCE_BITS: u32 = 58;
const EVIDENCE_MASK: u64 = (1 << EVIDENCE_BITS) - 1;

struct ScanOut {
    /// Filled from a `Stager` (64-byte aligned buffers): never dropped.
    ebuckets: Staged<ERec>,
    /// Exact byte references, in long-event order within each bucket.
    lbuckets: Staged<u64>,
    ev_arena: FallbackArena,
    clicks: Vec<CRec>,
    click_arena: FallbackArena,
}

fn timing() -> bool {
    env::var_os("SHOPPING_TIMING").is_some()
}

/// Run the whole pipeline on opaque IDs. `None`: the input is outside the
/// engine's limits and nothing was written; the caller runs the exact solver.
#[allow(clippy::too_many_arguments)]
pub fn run(
    catalog: &Catalog,
    data_dir: &Path,
    output_dir: &Path,
    ncpu: usize,
    use_bins: bool,
    assist_sweep: bool,
    t0: Instant,
    t_cat: std::time::Duration,
) -> Option<io::Result<()>> {
    COLLIDE.store(env::var("SHOPPING_OPQ_COLLIDE").ok().as_deref() == Some("1"), Ordering::Relaxed);
    DISCARD.store(env::var("SHOPPING_DISCARD").ok().as_deref() == Some("1"), Ordering::Relaxed);

    let window_start = catalog.window_start;
    let window_end = catalog.window_end;
    if window_end.saturating_sub(window_start) > u32::MAX as i64 || catalog.categories.len() > 256 {
        return None;
    }
    let link_inputs = scan::plan(&data_dir.join("links"), SCAN_CHUNK).ok()?;
    let event_inputs = scan::plan(&data_dir.join("events"), SCAN_CHUNK).ok()?;
    let click_inputs = scan::plan(&data_dir.join("clicks"), SCAN_CHUNK).ok()?;
    let bytes_of = |i: &scan::Inputs| i.sizes.iter().sum::<u64>() as usize;
    let (lb, eb, cb) = (bytes_of(&link_inputs), bytes_of(&event_inputs), bytes_of(&click_inputs));
    let workers = ncpu.max(1).min(64);

    // Table size from the input: link lines bring up to two IDs each; events
    // and clicks repeat their users. The table stays at most about 1/4 full
    // on generated data (probes stay short); one that fills up anyway is
    // thrown away and the scan repeats with one 4x larger (up to 2^28 slots).
    let est_events = eb / 72 + 1;
    let est_users = lb / 20 * 3 / 2 + (eb / 64 + cb / 48) / 16 + 1024;
    let mut slots = (est_users * 7 / 5).next_power_of_two().max(1 << 16);
    // Measurement: SHOPPING_OPQ_SLOTS=k forces 2^k slots.
    if let Some(k) = env::var("SHOPPING_OPQ_SLOTS").ok().and_then(|v| v.parse::<u32>().ok()) {
        slots = 1usize << k.clamp(16, 30);
    }
    let ebits = (est_events / env_usize("SHOPPING_OPQ_EB", BUCKET_TARGET)).next_power_of_two().trailing_zeros().clamp(0, 14);
    let n_eb = 1usize << ebits;
    let eshift = 64 - ebits;
    let ebucket = |key: u64| -> usize { if ebits == 0 { 0 } else { (mix64(key) >> eshift) as usize } };
    let reserve = est_events / n_eb / workers * 5 / 4 + 16;

    let ingest_base = window_start.div_euclid(1000) - (1i64 << 31);
    let click_win_start = window_start - CLICK_LOOKBACK_MS;
    let click_win_end = window_end;
    let out_of_range = AtomicBool::new(false);
    let use_pread = env::var("SHOPPING_PREAD").ok().as_deref() == Some("1");
    let populate = env::var("SHOPPING_POPULATE").ok().as_deref() == Some("1");

    // Jobs: links first (their unions overlap the event scan), then events, clicks.
    let mut jobs: Vec<(u8, u32, u64, u64)> = Vec::new();
    for (kind, inputs) in [(0u8, &link_inputs), (1, &event_inputs), (2, &click_inputs)] {
        for &(f, s, e) in &inputs.chunks {
            jobs.push((kind, f, s, e));
        }
    }
    let mut retries = 0u32;
    let (users, outs) = loop {
    let users = Users::new(slots, workers, (lb + eb + cb) / workers.max(1) / 4 + (1 << 20));
    let next_job = AtomicUsize::new(0);
    // Thread-summed scan time per input kind (links, events, clicks), in ns.
    let kind_ns: [AtomicU64; 3] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
    let outs: Mutex<Vec<(usize, ScanOut)>> = Mutex::new(Vec::with_capacity(workers));
    let next_wid = AtomicUsize::new(0);
    thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let wid = next_wid.fetch_add(1, Ordering::Relaxed);
                let mut ctx = users.ctx(wid);
                let zero_e = ERec { key: 0, ingest: 0, u: 0, ts: 0, pc: 0 };
                let zero_c = CRec { ts: 0, cid: 0, u: 0, camp: 0 };
                let mut stager: Stager<ERec, 8> = Stager::new(n_eb, reserve, zero_e);
                let mut lstager: Stager<u64, 8> = Stager::new(n_eb, reserve / 6 + 4, 0);
                let mut out = ScanOut {
                    ebuckets: Staged::empty(),
                    lbuckets: Staged::empty(),
                    ev_arena: FallbackArena::new(),
                    clicks: Vec::new(),
                    click_arena: FallbackArena::new(),
                };
                let mut dates = DateCache::default();
                let mut tmp_arena = FallbackArena::new();
                let mut reader = scan::Reader::default();
                let d = depth();
                let mut batch_l: Batch<()> = Batch::new((), d);
                let mut batch_e: Batch<(ERec, u64)> = Batch::new((zero_e, 0), d);
                let mut batch_c: Batch<CRec> = Batch::new(zero_c, d);
                let mut link_a = INVALID;
                macro_rules! resolve_link {
                    ($p:expr) => {{
                        let (p, ()) = $p;
                        let u = users.intern_slow(p.bytes(), p.key, p.h, &mut ctx);
                        if p.dest == 0 {
                            link_a = u;
                        } else if link_a != INVALID && u != INVALID && link_a != u {
                            users.union(link_a, u);
                        }
                    }};
                }
                macro_rules! resolve_event {
                    ($p:expr) => {{
                        let (p, (mut r, evidence)) = $p;
                        r.u = users.intern_slow(p.bytes(), p.key, p.h, &mut ctx);
                        if r.key & LONG != 0 {
                            lstager.push(p.dest as usize, evidence);
                        }
                        stager.push(p.dest as usize, r);
                    }};
                }
                macro_rules! resolve_click {
                    ($p:expr) => {{
                        let (p, mut c) = $p;
                        c.u = users.intern_slow(p.bytes(), p.key, p.h, &mut ctx);
                        out.clicks.push(c);
                    }};
                }
                loop {
                    if users.overflow.load(Ordering::Relaxed) {
                        break; // this scan is void; stop early
                    }
                    let j = next_job.fetch_add(1, Ordering::Relaxed);
                    let Some(&(kind, f, start, end)) = jobs.get(j) else {
                        break;
                    };
                    let inputs = match kind {
                        0 => &link_inputs,
                        1 => &event_inputs,
                        _ => &click_inputs,
                    };
                    let f = f as usize;
                    let tj = Instant::now();
                    let (bytes, first, stop, mapped) = if use_pread {
                        let (b, x, y) = reader.load(&inputs.files[f], inputs.sizes[f], start, end).expect("read input");
                        (b, x, y, false)
                    } else {
                        reader
                            .load_mapped(&inputs.files[f], inputs.sizes[f], inputs.maps[f].as_slice(), start, end)
                            .expect("read input")
                    };
                    if mapped && populate {
                        populate_read(bytes, first, stop);
                    }
                    match kind {
                        0 => {
                            scan::for_each_record::<2>(bytes, first, stop, |rec| {
                                let [a, b] = rec.fields();
                                if a.is_empty() || b.is_empty() {
                                    return;
                                }
                                for (x, side) in [(a, 0), (b, 1)] {
                                    if batch_l.push((users.pend(x, side), ())) {
                                        for &p in batch_l.take() {
                                            resolve_link!(p);
                                        }
                                    }
                                }
                            });
                            for &p in batch_l.take() {
                                resolve_link!(p);
                            }
                        }
                        1 => scan::for_each_record::<7>(bytes, first, stop, |rec| {
                            let [event_id, user_id, ts_text, ingest_text, event_type, product_id, price_text] =
                                rec.fields();
                            if event_id.is_empty() {
                                return;
                            }
                            let Some(ingest_secs) = parse_f1_seconds_cached(ingest_text, &mut dates) else {
                                return;
                            };
                            let mut valid = false;
                            let mut ts_val = 0i64;
                            let mut price_val = 0u32;
                            let mut cat_id = catalog.unknown_cat_id;
                            if !user_id.is_empty() && !product_id.is_empty() {
                                let type_code = match event_type.len() {
                                    4 => {
                                        let w = u32::from_le_bytes(event_type.try_into().unwrap());
                                        (w == u32::from_le_bytes(*b"view") || w == u32::from_le_bytes(*b"cart")) as u8
                                    }
                                    8 => {
                                        let w = u64::from_le_bytes(event_type.try_into().unwrap());
                                        2 * (w == u64::from_le_bytes(*b"purchase")) as u8
                                    }
                                    _ => 0u8,
                                };
                                if type_code != 0 {
                                    let price_ok = if type_code == 2 {
                                        if let Some(p) = parse_purchase_price(price_text) {
                                            price_val = p;
                                            true
                                        } else {
                                            false
                                        }
                                    } else {
                                        true
                                    };
                                    if price_ok {
                                        if let Some(ts) = parse_ts_cached(ts_text, &mut dates) {
                                            if ts >= window_start
                                                && ts < window_end
                                                && (ingest_secs * MS_PER_SECOND).saturating_sub(ts) <= LATE_LIMIT_MS
                                            {
                                                valid = true;
                                                ts_val = ts;
                                                let prod = if type_code != 2 {
                                                    None
                                                } else {
                                                    match id::try_pack_padded(product_id) {
                                                        Some(p) => catalog.packed_products.get(p.0),
                                                        None => {
                                                            tmp_arena.bytes.clear();
                                                            let p = tmp_arena.push_fallback(product_id);
                                                            catalog.products.get(
                                                                p,
                                                                &tmp_arena.bytes,
                                                                &catalog.product_arena.bytes,
                                                            )
                                                        }
                                                    }
                                                };
                                                if let Some(cid) = prod {
                                                    cat_id = cid;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            let key = event_key(event_id);
                            let b = ebucket(key);
                            let evidence = if key & LONG != 0 {
                                let off = out.ev_arena.push_fallback(event_id).0 & !FALLBACK;
                                if off > EVIDENCE_MASK {
                                    out_of_range.store(true, Ordering::Relaxed);
                                    return;
                                }
                                (wid as u64) << EVIDENCE_BITS | off
                            } else {
                                0
                            };
                            let ing = ingest_secs - ingest_base;
                            if !(0..=u32::MAX as i64).contains(&ing) {
                                out_of_range.store(true, Ordering::Relaxed);
                                return;
                            }
                            let (u, ts_off) = if valid { (0, (ts_val - window_start) as u32) } else { (INVALID, 0) };
                            let r = ERec { key, ingest: ing as u32, u, ts: ts_off, pc: (price_val << 8) | cat_id };
                            if valid {
                                if batch_e.push((users.pend(user_id, b as u32), (r, evidence))) {
                                    for &p in batch_e.take() {
                                        resolve_event!(p);
                                    }
                                }
                            } else {
                                if key & LONG != 0 {
                                    lstager.push(b, evidence);
                                }
                                stager.push(b, r);
                            }
                        }),
                        _ => scan::for_each_record::<4>(bytes, first, stop, |rec| {
                            let [click_id, user_id, campaign_id, ts_text] = rec.fields();
                            if click_id.is_empty() || user_id.is_empty() || campaign_id.is_empty() {
                                return;
                            }
                            let Some(ts) = parse_ts_cached(ts_text, &mut dates) else {
                                return;
                            };
                            if ts < click_win_start || ts >= click_win_end {
                                return;
                            }
                            tmp_arena.bytes.clear();
                            let camp_pid = tmp_arena.pack_or_push_padded(campaign_id);
                            let camp = if camp_pid.is_packed() {
                                catalog.packed_campaigns.get(camp_pid.0)
                            } else {
                                catalog.campaigns.get(camp_pid, &tmp_arena.bytes, &catalog.campaign_arena.bytes)
                            };
                            let Some(camp_idx) = camp else {
                                return;
                            };
                            let cid = match id::try_pack_padded(click_id) {
                                Some(p) => p.0,
                                None => {
                                    let off = out.click_arena.push_fallback(click_id).0 & !FALLBACK;
                                    if off >= 1 << 40 {
                                        out_of_range.store(true, Ordering::Relaxed);
                                        return;
                                    }
                                    FALLBACK | ((wid as u64) << 40) | off
                                }
                            };
                            let c = CRec { ts, cid, u: 0, camp: camp_idx };
                            if batch_c.push((users.pend(user_id, 0), c)) {
                                for &p in batch_c.take() {
                                    resolve_click!(p);
                                }
                            }
                        }),
                    }
                    for &p in batch_e.take() {
                        resolve_event!(p);
                    }
                    for &p in batch_c.take() {
                        resolve_click!(p);
                    }
                    if mapped {
                        scan::unmap_pages(bytes, first, stop);
                    }
                    kind_ns[kind as usize].fetch_add(tj.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                let tf = Instant::now();
                out.ebuckets = stager.finish();
                out.lbuckets = lstager.finish();
                kind_ns[1].fetch_add(tf.elapsed().as_nanos() as u64, Ordering::Relaxed);
                outs.lock().unwrap().push((wid, out));
            });
        }
    });
    if timing() {
        let k = |i: usize| kind_ns[i].load(Ordering::Relaxed) as f64 / 1e6;
        eprintln!("opaque_scan_thread_ms links={:.0} events={:.0} clicks={:.0} slots={} retries={}", k(0), k(1), k(2), slots, retries);
    }
    let mut outs = outs.into_inner().unwrap();
    outs.sort_by_key(|(w, _)| *w);
    let outs: Vec<ScanOut> = outs.into_iter().map(|(_, o)| o).collect();
    if users.table_full.load(Ordering::Relaxed) && !out_of_range.load(Ordering::Relaxed) && slots < 1 << 28 {
        drop(outs);
        drop(users);
        slots <<= 2;
        retries += 1;
        if timing() {
            eprintln!("opaque: user table full, scanning again with {slots} slots");
        }
        continue;
    }
    if users.overflow.load(Ordering::Relaxed) || out_of_range.load(Ordering::Relaxed) {
        if timing() {
            eprintln!("opaque: limits exceeded, exact solver");
        }
        drop(outs);
        return None;
    }
    break (users, outs);
    };
    let t_scan = t0.elapsed();

    // 2. Persons: the smallest ID of each component, then dense numbers.
    let u_max = (users.next_block.load(Ordering::Relaxed) as usize * users.block as usize).min(users.limit_u as usize);
    let best = zeroed_u32(u_max); // best member + 1 (0 = none yet)
    let roots = zeroed_u32(u_max);
    let span = u_max.div_ceil(workers).max(1);
    let root_counts: Vec<AtomicUsize> = (0..workers).map(|_| AtomicUsize::new(0)).collect();
    thread::scope(|scope| {
        for t in 0..workers {
            let (users, best, roots, root_counts) = (&users, &best, &roots, &root_counts);
            scope.spawn(move || {
                let mut n = 0usize;
                for u in (t * span)..((t + 1) * span).min(u_max) {
                    if users.ukey[u].load(Ordering::Relaxed) == 0 {
                        roots[u].store(INVALID, Ordering::Relaxed);
                        continue;
                    }
                    let u32_ = u as u32;
                    let r = users.find(u32_);
                    roots[u].store(r, Ordering::Relaxed);
                    if r == u32_ {
                        n += 1;
                    }
                    let slot = &best[r as usize];
                    let mut cur = slot.load(Ordering::Relaxed);
                    loop {
                        if cur != 0 && !users.id_less(u32_, cur - 1) {
                            break;
                        }
                        match slot.compare_exchange_weak(cur, u32_ + 1, Ordering::Relaxed, Ordering::Relaxed) {
                            Ok(_) => break,
                            Err(x) => cur = x,
                        }
                    }
                }
                root_counts[t].store(n, Ordering::Relaxed);
            });
        }
    });
    let mut bases = vec![0usize; workers + 1];
    for t in 0..workers {
        bases[t + 1] = bases[t] + root_counts[t].load(Ordering::Relaxed);
    }
    let n_persons = bases[workers];
    if n_persons >= INVALID as usize {
        return None;
    }
    let p_of = zeroed_u32(u_max);
    let names = zeroed_u32(n_persons.max(1));
    thread::scope(|scope| {
        for t in 0..workers {
            let (users, best, roots, p_of, names, bases) = (&users, &best, &roots, &p_of, &names, &bases);
            scope.spawn(move || {
                let mut next = bases[t] as u32;
                for u in (t * span)..((t + 1) * span).min(u_max) {
                    if roots[u].load(Ordering::Relaxed) == u as u32 {
                        p_of[u].store(next, Ordering::Relaxed);
                        names[next as usize].store(best[u].load(Ordering::Relaxed) - 1, Ordering::Relaxed);
                        next += 1;
                    }
                }
                let _ = users;
            });
        }
    });
    thread::scope(|scope| {
        for t in 0..workers {
            let (roots, p_of) = (&roots, &p_of);
            scope.spawn(move || {
                for u in (t * span)..((t + 1) * span).min(u_max) {
                    let r = roots[u].load(Ordering::Relaxed);
                    if r != INVALID && r != u as u32 {
                        p_of[u].store(p_of[r as usize].load(Ordering::Relaxed), Ordering::Relaxed);
                    }
                }
            });
        }
    });
    drop(best);
    drop(roots);
    let t_persons = t0.elapsed();

    // 3. Dedup each event bucket; file surviving events by person.
    if outs.iter().any(|o| o.ebuckets.iter().any(|v| v.len() >= 1 << 26)) || outs.len() > 64 {
        return None;
    }
    let n_records: usize = outs.iter().map(|o| o.ebuckets.iter().map(|v| v.len()).sum::<usize>()).sum();
    let per_person = (n_records / n_persons.max(1)).max(1);
    let pbits = ((env_usize("SHOPPING_OPQ_PB", PERSON_TARGET) / per_person).max(1).next_power_of_two().trailing_zeros()).min(20);
    let n_pb = (n_persons >> pbits) + 1;
    let next_eb = AtomicUsize::new(0);
    let dedup_out: Mutex<Vec<Staged<PRec>>> = Mutex::new(Vec::with_capacity(workers));
    let long_mismatch = AtomicBool::new(false);
    let max_bucket = (0..n_eb).map(|b| outs.iter().map(|o| o.ebuckets[b].len()).sum::<usize>()).max().unwrap_or(0);
    thread::scope(|scope| {
        for _ in 0..workers {
            let (outs, p_of, next_eb, dedup_out, long_mismatch) = (&outs, &p_of, &next_eb, &dedup_out, &long_mismatch);
            scope.spawn(move || {
                // At most 2/3 full: the bucket's records over a power of two >= 1.5x.
                let tsize = (max_bucket * 3 / 2 + 1).next_power_of_two().max(16);
                let tmask = tsize - 1;
                // The winning copy of each key, in place; key 0 = empty (keys are
                // never 0). The output pass walks the table in order.
                let mut table: Vec<ERec> = vec![ERec { key: 0, ingest: 0, u: 0, ts: 0, pc: 0 }; tsize];
                let per_pb = n_records / n_pb.max(1) / workers.max(1) * 5 / 4 + 16;
                let mut pstage: Stager<PRec, 16> = Stager::new(n_pb, per_pb, PRec { p: 0, ts: 0, pc: 0 });
                let mut first_long = vec![0u64; tsize];
                let bytes = |evidence: u64| -> &[u8] {
                    let a = &outs[(evidence >> EVIDENCE_BITS) as usize].ev_arena.bytes;
                    let (off, len) = PackedId(FALLBACK | (evidence & EVIDENCE_MASK)).fallback_span(a);
                    &a[off..off + len]
                };
                'buckets: loop {
                    let b = next_eb.fetch_add(1, Ordering::Relaxed);
                    if b >= n_eb || long_mismatch.load(Ordering::Relaxed) {
                        break;
                    }
                    for o in outs.iter() {
                        // Each worker emits its long ID reference alongside the
                        // event, so this side span follows the same bucket order.
                        let mut ids = o.lbuckets[b].iter();
                        for r in o.ebuckets[b].iter() {
                            let long = r.key & LONG != 0;
                            let evidence = if long { *ids.next().expect("long ID reference") } else { 0 };
                            let mut s = (mix64(r.key) as usize) & tmask;
                            loop {
                                let e = &mut table[s];
                                if e.key == 0 {
                                    *e = *r;
                                    if long {
                                        first_long[s] = evidence;
                                    }
                                    break;
                                }
                                if e.key == r.key {
                                    if long && bytes(first_long[s]) != bytes(evidence) {
                                        long_mismatch.store(true, Ordering::Relaxed);
                                        break 'buckets;
                                    }
                                    if r.ingest < e.ingest {
                                        *e = *r; // equal ingest: copies are identical, keep the first
                                    }
                                    break;
                                }
                                s = (s + 1) & tmask;
                            }
                        }
                    }
                    const AHEAD: usize = 16;
                    for s in 0..tsize {
                        if let Some(e2) = table.get(s + AHEAD) {
                            if e2.key != 0 && e2.u != INVALID {
                                prefetch_ptr(&p_of[e2.u as usize]);
                            }
                        }
                        let e = &mut table[s];
                        if e.key == 0 {
                            continue;
                        }
                        if e.u != INVALID {
                            let p = p_of[e.u as usize].load(Ordering::Relaxed);
                            pstage.push((p >> pbits) as usize, PRec { p, ts: e.ts, pc: e.pc });
                        }
                        e.key = 0;
                    }
                    for o in outs.iter() {
                        discard(&o.ebuckets[b], o.ebuckets[b].len());
                        discard(&o.lbuckets[b], o.lbuckets[b].len());
                    }
                }
                dedup_out.lock().unwrap().push(pstage.finish());
            });
        }
    });
    let dedup_out = dedup_out.into_inner().unwrap();
    let t_dedup = t0.elapsed();

    if long_mismatch.load(Ordering::Relaxed) {
        if timing() {
            eprintln!("opaque: long event key shared by different IDs, exact solver");
        }
        return None;
    }
    let t_verify = t0.elapsed();

    // Clicks by person bucket.
    // Staged like the event records.
    let click_parts: Vec<Staged<CRec>> = thread::scope(|scope| {
        let handles: Vec<_> = outs
            .iter()
            .map(|o| {
                let p_of = &p_of;
                scope.spawn(move || {
                    let zero = CRec { ts: 0, cid: 0, u: 0, camp: 0 };
                    let mut cb: Stager<CRec, 8> = Stager::new(n_pb, o.clicks.len() / n_pb.max(1) * 5 / 4 + 8, zero);
                    for (k, c) in o.clicks.iter().enumerate() {
                        if let Some(c2) = o.clicks.get(k + 16) {
                            prefetch_ptr(&p_of[c2.u as usize]);
                        }
                        let p = p_of[c.u as usize].load(Ordering::Relaxed);
                        cb.push((p >> pbits) as usize, CRec { u: p, ..*c });
                    }
                    discard(&o.clicks, o.clicks.capacity());
                    cb.finish()
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let t_clicks = t0.elapsed();

    // 4. Sessions per person bucket, largest buckets first.
    let num_campaigns = catalog.campaign_names.len();
    let num_categories = catalog.categories.len();
    let full_window_days = (window_end.saturating_sub(window_start) / MS_PER_DAY + 2).max(1) as usize;
    let window_days = if num_campaigns.checked_mul(full_window_days).is_some_and(|n| n <= 1_000_000) {
        full_window_days
    } else {
        0
    };
    let base_day = window_start.div_euclid(MS_PER_DAY) as i32;
    let mut order: Vec<(usize, usize)> = (0..n_pb)
        .map(|pb| (dedup_out.iter().map(|v| v[pb].len()).sum::<usize>(), pb))
        .collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let global_camp_grid: Mutex<Vec<CampAcc>> = Mutex::new(vec![CampAcc::default(); num_campaigns * window_days]);
    let global_camp_extra: Mutex<std::collections::HashMap<(u32, i32), CampAcc>> =
        Mutex::new(std::collections::HashMap::new());
    let io_error: Mutex<Option<io::Error>> = Mutex::new(None);
    let total_user_rows = AtomicUsize::new(0);
    let next_pb = AtomicUsize::new(0);
    let span_p = 1usize << pbits;
    thread::scope(|scope| {
        for worker_id in 0..workers {
            let (dedup_out, click_parts, outs, users, names, order) =
                (&dedup_out, &click_parts, &outs, &users, &names, &order);
            let (global_camp_grid, global_camp_extra, io_error, total_user_rows, next_pb) =
                (&global_camp_grid, &global_camp_extra, &io_error, &total_user_rows, &next_pb);
            scope.spawn(move || {
                let mut sink = Sink::new(
                    num_campaigns,
                    num_categories,
                    window_days,
                    base_day,
                    window_start,
                    window_end,
                    use_bins,
                    assist_sweep,
                );
                let mut ecount = vec![0u32; span_p + 1];
                let mut ccount = vec![0u32; span_p + 1];
                let mut evs: Vec<(i64, u32, u32)> = Vec::new();
                let mut cls: Vec<(i64, PackedId, u32)> = Vec::new();
                let mut local = FallbackArena::new();
                let (mut t_fill, mut t_sink) = (0f64, 0f64);
                loop {
                    let k = next_pb.fetch_add(1, Ordering::Relaxed);
                    let Some(&(n_ev, pb)) = order.get(k) else {
                        break;
                    };
                    if n_ev == 0 {
                        continue;
                    }
                    let tb = Instant::now();
                    let base = (pb << pbits) as u32;
                    ecount.iter_mut().for_each(|c| *c = 0);
                    ccount.iter_mut().for_each(|c| *c = 0);
                    for v in dedup_out {
                        for r in &v[pb] {
                            ecount[(r.p - base) as usize + 1] += 1;
                        }
                    }
                    for part in click_parts {
                        for c in &part[pb] {
                            ccount[(c.u - base) as usize + 1] += 1;
                        }
                    }
                    for i in 0..span_p {
                        ecount[i + 1] += ecount[i];
                        ccount[i + 1] += ccount[i];
                    }
                    evs.clear();
                    evs.resize(n_ev, (0, 0, 0));
                    let mut epos: Vec<u32> = ecount[..span_p].to_vec();
                    for v in dedup_out {
                        for r in &v[pb] {
                            let lp = (r.p - base) as usize;
                            evs[epos[lp] as usize] = (window_start + r.ts as i64, r.pc >> 8, r.pc & 255);
                            epos[lp] += 1;
                        }
                    }
                    let n_cl = ccount[span_p] as usize;
                    cls.clear();
                    cls.resize(n_cl, (0, PackedId(0), 0));
                    local.bytes.clear();
                    let mut cpos: Vec<u32> = ccount[..span_p].to_vec();
                    for part in click_parts {
                        for c in &part[pb] {
                            let lp = (c.u - base) as usize;
                            let cid = if c.cid & FALLBACK == 0 {
                                PackedId(c.cid)
                            } else {
                                let w = ((c.cid >> 40) & 63) as usize;
                                let arena = &outs[w].click_arena.bytes;
                                let (off, len) = PackedId(FALLBACK | (c.cid & ((1 << 40) - 1))).fallback_span(arena);
                                local.push_fallback(&arena[off..off + len])
                            };
                            cls[cpos[lp] as usize] = (c.ts, cid, c.camp);
                            cpos[lp] += 1;
                        }
                    }
                    let tm = Instant::now();
                    t_fill += (tm - tb).as_secs_f64();
                    for lp in 0..span_p {
                        let (es, ee) = (ecount[lp] as usize, ecount[lp + 1] as usize);
                        if es == ee {
                            continue;
                        }
                        let (cs, ce) = (ccount[lp] as usize, ccount[lp + 1] as usize);
                        let p = base as usize + lp;
                        let name_u = names[p].load(Ordering::Relaxed);
                        let mut buf = [0u8; 10];
                        let name = users.id_bytes(name_u, &mut buf);
                        sink.person(name, &mut evs[es..ee], &mut cls[cs..ce], &local.bytes, false);
                    }
                    t_sink += tm.elapsed().as_secs_f64();
                    for v in dedup_out {
                        discard(&v[pb], v[pb].len());
                    }
                    for part in click_parts {
                        discard(&part[pb], part[pb].len());
                    }
                }
                let tw = Instant::now();
                let ucols = &sink.ucols;
                if ucols.len() > 0 {
                    total_user_rows.fetch_add(ucols.len(), Ordering::Relaxed);
                    let path = output_dir.join("user_daily").join(format!("part-{worker_id:02}.parquet"));
                    if let Err(e) = write_user_daily_cols(&path, ucols, &catalog.categories) {
                        *io_error.lock().unwrap() = Some(e);
                    }
                }
                {
                    let mut g = global_camp_grid.lock().unwrap();
                    for (g, l) in g.iter_mut().zip(sink.camp_grid.iter()) {
                        g.sessions += l.sessions;
                        g.purchasing_sessions += l.purchasing_sessions;
                        g.revenue_cents += l.revenue_cents;
                        g.assisted_sessions += l.assisted_sessions;
                    }
                }
                if timing() {
                    eprintln!("opaque_sessions worker={worker_id} fill_ms={:.1} sink_ms={:.1} write_ms={:.1}", t_fill * 1e3, t_sink * 1e3, tw.elapsed().as_secs_f64() * 1e3);
                }
                if !sink.camp_extra.is_empty() {
                    let mut g = global_camp_extra.lock().unwrap();
                    for (k, l) in sink.camp_extra.drain() {
                        let e = g.entry(k).or_default();
                        e.sessions += l.sessions;
                        e.purchasing_sessions += l.purchasing_sessions;
                        e.revenue_cents += l.revenue_cents;
                        e.assisted_sessions += l.assisted_sessions;
                    }
                }
            });
        }
    });
    let t_sessions = t0.elapsed();
    if let Some(e) = io_error.into_inner().unwrap() {
        return Some(Err(e));
    }
    let result = (|| -> io::Result<()> {
        if total_user_rows.load(Ordering::Relaxed) == 0 {
            write_user_daily_cols(
                &output_dir.join("user_daily").join("part-00.parquet"),
                &UserCols::default(),
                &catalog.categories,
            )?;
        }
        let g_grid = global_camp_grid.into_inner().unwrap();
        let g_extra = global_camp_extra.into_inner().unwrap();
        let mut camp_rows: Vec<CampaignDailyRow<'_>> = Vec::new();
        for c in 0..num_campaigns {
            for d in 0..window_days {
                let acc = g_grid[c * window_days + d];
                if acc.sessions > 0 || acc.assisted_sessions > 0 {
                    camp_rows.push(CampaignDailyRow {
                        campaign_id: &catalog.campaign_names[c],
                        channel: &catalog.campaign_channels[c],
                        day: base_day + d as i32,
                        sessions: acc.sessions,
                        purchasing_sessions: acc.purchasing_sessions,
                        revenue_cents: acc.revenue_cents,
                        assisted_sessions: acc.assisted_sessions,
                    });
                }
            }
        }
        for (&(c, day), acc) in &g_extra {
            if acc.sessions > 0 || acc.assisted_sessions > 0 {
                camp_rows.push(CampaignDailyRow {
                    campaign_id: &catalog.campaign_names[c as usize],
                    channel: &catalog.campaign_channels[c as usize],
                    day,
                    sessions: acc.sessions,
                    purchasing_sessions: acc.purchasing_sessions,
                    revenue_cents: acc.revenue_cents,
                    assisted_sessions: acc.assisted_sessions,
                });
            }
        }
        write_campaign_daily_file(&output_dir.join("campaign_daily").join("part-00.parquet"), &camp_rows)
    })();
    let t_total = t0.elapsed();
    if timing() {
        let ms = |a: std::time::Duration, b: std::time::Duration| (b - a).as_secs_f64() * 1000.0;
        eprintln!(
            "timing_ms: catalog={:.1} opaque_scan={:.1} persons_ms={:.1} ev_dedup={:.1} verify={:.1} click_route={:.1} sessions_and_user_write={:.1} campaign_write={:.1} total={:.1} opaque=1 users={} people={} buckets={}x{} pbits={} slots={}",
            t_cat.as_secs_f64() * 1000.0,
            ms(t_cat, t_scan),
            ms(t_scan, t_persons),
            ms(t_persons, t_dedup),
            ms(t_dedup, t_verify),
            ms(t_verify, t_clicks),
            ms(t_clicks, t_sessions),
            ms(t_sessions, t_total),
            t_total.as_secs_f64() * 1000.0,
            u_max,
            n_persons,
            n_eb,
            n_pb,
            pbits,
            slots,
        );
    }
    if timing() {
        let st = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        let hwm = st.lines().find(|l| l.starts_with("VmHWM")).unwrap_or("VmHWM: ?");
        eprintln!("peak_rss: {}", hwm.trim_start_matches("VmHWM:").trim());
    }
    std::mem::forget(dedup_out);
    std::mem::forget(click_parts);
    std::mem::forget(outs);
    std::mem::forget(users);
    std::mem::forget(p_of);
    std::mem::forget(names);
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// IDs inside a buffer with the scanner's 16-byte slack.
    fn padded(ids: &[&[u8]]) -> (Vec<u8>, Vec<(usize, usize)>) {
        let mut buf = Vec::new();
        let mut spans = Vec::new();
        for id in ids {
            spans.push((buf.len(), id.len()));
            buf.extend_from_slice(id);
            buf.push(b',');
        }
        buf.extend_from_slice(&[0u8; 64]);
        (buf, spans)
    }

    #[test]
    fn batch_resolves_in_push_order_at_every_depth() {
        for depth in [1, 2, 7, 16, 64] {
            let mut r: Batch<u32> = Batch::new(0, depth);
            let z = Pend { ptr: std::ptr::null(), len: 0, dest: 0, key: 0, h: 0 };
            let mut out = Vec::new();
            for i in 0..500u32 {
                if r.push((z, i)) {
                    out.extend(r.take().iter().map(|&(_, v)| v));
                }
            }
            out.extend(r.take().iter().map(|&(_, v)| v));
            assert_eq!(out, (0..500).collect::<Vec<_>>(), "depth {depth}");
        }
    }

    #[test]
    fn stager_keeps_every_record_in_bucket_order() {
        let mut st: Stager<ERec, 8> = Stager::new(5, 3, ERec { key: 0, ingest: 0, u: 0, ts: 0, pc: 0 });
        let mut want: Vec<Vec<u64>> = vec![Vec::new(); 5];
        let mut x = 88172645463325252u64;
        for i in 0..10_000u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let b = (x % 5) as usize;
            st.push(b, ERec { key: i + 1, ingest: 0, u: 0, ts: 0, pc: 0 });
            want[b].push(i + 1);
        }
        let got = st.finish();
        for b in 0..5 {
            assert_eq!(got[b].iter().map(|r| r.key).collect::<Vec<_>>(), want[b]);
        }
        drop(got);
    }

    #[test]
    fn stager_spans_that_outgrow_their_share_keep_every_record() {
        // A reserve of one group per bucket, then thousands of records in a few
        // buckets: spans move to blocks of their own, repeatedly.
        let zero = PRec { p: 0, ts: 0, pc: 0 };
        let mut st: Stager<PRec, 16> = Stager::new(7, 1, zero);
        let mut want: Vec<Vec<u32>> = vec![Vec::new(); 7];
        for i in 0..20_000u32 {
            let b = if i % 3 == 0 { 1 } else if i % 5 == 0 { 6 } else { (i % 7) as usize };
            st.push(b, PRec { p: i, ts: i, pc: 0 });
            want[b].push(i);
        }
        let got = st.finish();
        for b in 0..7 {
            assert_eq!(got[b].iter().map(|r| r.p).collect::<Vec<_>>(), want[b], "bucket {b}");
        }
        assert_eq!(got.iter().map(|v| v.len()).sum::<usize>(), 20_000);
    }

    #[test]
    fn interning_is_exact_for_short_long_and_colliding_ids() {
        let users = Users::new(1 << 16, 2, 1 << 20);
        let mut ctx = users.ctx(0);
        let ids: Vec<&[u8]> = vec![
            b"u1", b"U1", b"u1 ", b" u1", b"u02423c3d84ab053af78764ce49f23b5c", b"u02423c3d84ab053af78764ce49f23b5d",
            "é".as_bytes(), b"0123456789", b"01234567890", b"a_b", b"a-b",
            b"u1234567", b"u12345678", b"u1234567\0", b"u12345\x807",
            b"u\x0f2345678", b"u\x102345678", b"u\x7f2345678", b"u\x802345678",
        ];
        let (buf, spans) = padded(&ids);
        let at = |i: usize| &buf[spans[i].0..spans[i].0 + spans[i].1];
        let first: Vec<u32> = (0..ids.len()).map(|i| users.intern(at(i), &mut ctx)).collect();
        let mut sorted = first.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "distinct IDs get distinct indices");
        for i in 0..ids.len() {
            assert_eq!(users.intern(at(i), &mut ctx), first[i], "the same ID maps to the same index");
            let mut b = [0u8; 10];
            assert_eq!(users.id_bytes(first[i], &mut b), ids[i]);
        }
        // Two different long IDs forced onto one key and hash: bytes decide.
        let (a, b) = (at(4), at(5));
        let k = LONG | 12345;
        let ua = users.intern_slow(a, k, 99, &mut ctx);
        let ub = users.intern_slow(b, k, 99, &mut ctx);
        assert_ne!(ua, ub);
        assert_eq!(users.intern_slow(a, k, 99, &mut ctx), ua);
        assert_eq!(users.intern_slow(b, k, 99, &mut ctx), ub);
        // Byte order across packed and long forms.
        for i in 0..ids.len() {
            for j in 0..ids.len() {
                assert_eq!(users.id_less(first[i], first[j]), ids[i] < ids[j], "{:?} {:?}", ids[i], ids[j]);
            }
        }
    }

    #[test]
    fn failed_intern_never_publishes_unwritten_bytes() {
        let users = Users::new(1 << 16, 1, 0);
        let mut ctx = users.ctx(0);
        let (buf, spans) = padded(&[b"a user ID requiring an arena"]);
        let bytes = &buf[..spans[0].1];
        assert_eq!(users.intern(bytes, &mut ctx), INVALID);
        assert_eq!(users.intern(bytes, &mut ctx), INVALID);
        assert!(users.overflow.load(Ordering::Relaxed));
    }

    #[test]
    fn union_find_joins_transitively_from_any_thread() {
        let users = Users::new(1 << 16, 4, 1 << 20);
        let names: Vec<Vec<u8>> = (0..2000).map(|i| format!("id{i}").into_bytes()).collect();
        let refs: Vec<&[u8]> = names.iter().map(|v| v.as_slice()).collect();
        let (buf, spans) = padded(&refs);
        let ix: Vec<u32> = {
            let mut ctx = users.ctx(0);
            (0..names.len()).map(|i| users.intern(&buf[spans[i].0..spans[i].0 + spans[i].1], &mut ctx)).collect()
        };
        // Chains i -> i+10 within each residue class mod 10, joined in parallel.
        thread::scope(|s| {
            for t in 0..4 {
                let (users, ix) = (&users, &ix);
                s.spawn(move || {
                    for i in (t..1990).step_by(4) {
                        users.union(ix[i], ix[i + 10]);
                    }
                });
            }
        });
        for i in 0..2000 {
            for j in [i % 10, (i + 1) % 10] {
                assert_eq!(users.find(ix[i]) == users.find(ix[j]), i % 10 == j % 10);
            }
        }
    }

}
