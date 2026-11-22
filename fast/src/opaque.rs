//! The opaque-ID engine (on unless `SHOPPING_OPAQUE=0`). IDs are values, never
//! numbers: an ID of at most 10 characters from `[0-9A-Z_a-z]` is keyed by its
//! exact packed form, and any other ID by a 64-bit hash whose bytes are compared
//! whenever two keys match.
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
//!    person. Matching long keys have their bytes compared afterwards.
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
    CLICK_LOOKBACK_MS, DateCache, LATE_LIMIT_MS, MS_PER_DAY, parse_f1_cached, parse_purchase_price,
    parse_ts_cached,
};
use crate::scan;
use crate::sink::{CampAcc, Sink};
use crate::{Catalog, SCAN_CHUNK};

const LONG: u64 = 1 << 63;
const FALLBACK: u64 = 1 << 63;
const INVALID: u32 = u32::MAX;
/// Offsets in one long-ID arena (64 MiB each).
const ARENA_BITS: u32 = 26;
/// Records per event bucket the dedup table is sized for.
const BUCKET_TARGET: usize = 32 * 1024;
/// Events per person bucket.
const PERSON_TARGET: usize = 32 * 1024;

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

/// Key of a non-empty ID inside a scan buffer (16 readable bytes): the exact
/// packed form, or `LONG` | a 63-bit hash of the bytes.
#[inline(always)]
fn id_key(bytes: &[u8]) -> u64 {
    match id::try_pack_padded(bytes) {
        Some(p) => p.0,
        None => fxhash_bytes(bytes) | LONG,
    }
}

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
    /// Each index's ID: its packed key, or `LONG` | arena reference.
    ukey: Vec<AtomicU64>,
    next_block: AtomicU32,
    block: u32,
    limit_u: u32,
    used: AtomicUsize,
    max_used: usize,
    arenas: Arenas,
    overflow: AtomicBool,
}

struct Ctx {
    arena: usize,
    arena_len: usize,
    u_next: u32,
    u_end: u32,
    inserted: usize,
}

/// A user ID whose table slot was prefetched, resolved `DEPTH` rows later.
#[derive(Clone, Copy)]
struct Pend {
    ptr: *const u8,
    len: u32,
    /// Where the index goes: a record position, or which side of a link.
    dst: u32,
    b: u32,
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

const DEPTH: usize = 16;

/// FIFO of pending lookups.
struct Ring {
    buf: [Pend; DEPTH],
    head: usize,
    n: usize,
}

impl Ring {
    fn new() -> Self {
        let z = Pend { ptr: std::ptr::null(), len: 0, dst: 0, b: 0, key: 0, h: 0 };
        Self { buf: [z; DEPTH], head: 0, n: 0 }
    }

    /// Queue `p`; when full, return the oldest entry for resolution.
    #[inline(always)]
    fn push(&mut self, p: Pend) -> Option<Pend> {
        if self.n == DEPTH {
            let old = self.buf[self.head];
            self.buf[self.head] = p;
            self.head = (self.head + 1) % DEPTH;
            Some(old)
        } else {
            self.buf[(self.head + self.n) % DEPTH] = p;
            self.n += 1;
            None
        }
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Pend> {
        if self.n == 0 {
            return None;
        }
        let p = self.buf[self.head];
        self.head = (self.head + 1) % DEPTH;
        self.n -= 1;
        Some(p)
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
        #[cfg(not(target_arch = "x86_64"))]
        let _ = h;
    }

    /// A pending lookup for `bytes` (non-empty, inside a scan buffer), with its
    /// slot prefetched.
    #[inline(always)]
    fn pend(&self, bytes: &[u8], dst: u32, b: u32) -> Pend {
        let key = id_key(bytes);
        let h = mix64(key);
        self.prefetch(h);
        Pend { ptr: bytes.as_ptr(), len: bytes.len() as u32, dst, b, key, h }
    }

    /// The index of an ID with key `key` and hash `h`, inserting it if new;
    /// `INVALID` once the table is over capacity.
    #[inline]
    fn intern_slow(&self, bytes: &[u8], key: u64, h: u64, ctx: &mut Ctx) -> u32 {
        let mut i = (h as usize) & self.mask;
        let mut probes = 0u32;
        loop {
            let ks = &self.slots[2 * i];
            let mut k = ks.load(Ordering::Acquire);
            if k == 0 {
                match ks.compare_exchange(0, key, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => return self.claim(i, key, bytes, ctx),
                    Err(actual) => k = actual,
                }
            }
            if k == key {
                let v = self.wait(i);
                if key & LONG == 0 || self.arenas.bytes((v >> 32) as u32) == bytes {
                    return (v as u32).wrapping_sub(1);
                }
            }
            i = (i + 1) & self.mask;
            probes += 1;
            if probes > 1 << 16 {
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
                self.overflow.store(true, Ordering::Relaxed);
                self.slots[2 * i + 1].store(1, Ordering::Release);
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
                    self.slots[2 * i + 1].store(val, Ordering::Release);
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
        if k & LONG == 0 {
            id::unpack(PackedId(k), buf)
        } else {
            self.arenas.bytes(k as u32)
        }
    }

    /// Byte-wise `id(a) < id(b)`.
    #[inline]
    fn id_less(&self, a: u32, b: u32) -> bool {
        let ka = self.ukey[a as usize].load(Ordering::Relaxed);
        let kb = self.ukey[b as usize].load(Ordering::Relaxed);
        if ka & LONG == 0 && kb & LONG == 0 {
            return ka < kb; // packed order is byte order
        }
        let (mut x, mut y) = ([0u8; 10], [0u8; 10]);
        self.id_bytes(a, &mut x) < self.id_bytes(b, &mut y)
    }
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

/// A small set of u64 keys (open addressing; 0 is never a key here).
struct KeySet {
    keys: Vec<u64>,
    mask: usize,
}

impl KeySet {
    fn new(keys: &[u64]) -> Self {
        let size = (keys.len() * 2).next_power_of_two().max(16);
        let mut s = Self { keys: vec![0; size], mask: size - 1 };
        for &k in keys {
            let mut i = (mix64(k) as usize) & s.mask;
            while s.keys[i] != 0 && s.keys[i] != k {
                i = (i + 1) & s.mask;
            }
            s.keys[i] = k;
        }
        s
    }

    #[inline]
    fn contains(&self, k: u64) -> bool {
        let mut i = (mix64(k) as usize) & self.mask;
        loop {
            let x = self.keys[i];
            if x == k {
                return true;
            }
            if x == 0 {
                return false;
            }
            i = (i + 1) & self.mask;
        }
    }
}

struct ScanOut {
    ebuckets: Vec<Vec<ERec>>,
    long_events: Vec<(u64, u64)>,
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
    // and clicks repeat their users. Past 3/4 full the run falls back.
    let est_events = eb / 72 + 1;
    let est_users = lb / 20 * 3 / 2 + (eb / 64 + cb / 48) / 16 + 1024;
    let slots = (est_users * 7 / 5).next_power_of_two().max(1 << 16);
    let users = Users::new(slots, workers, (lb + eb + cb) / workers.max(1) / 4 + (1 << 20));
    let ebits = (est_events / BUCKET_TARGET).next_power_of_two().trailing_zeros().clamp(0, 14);
    let n_eb = 1usize << ebits;
    let eshift = 64 - ebits;
    let ebucket = |key: u64| -> usize { if ebits == 0 { 0 } else { (mix64(key) >> eshift) as usize } };
    let reserve = est_events / n_eb / workers * 5 / 4 + 16;

    let ingest_base = window_start.div_euclid(1000) - (1i64 << 31);
    let click_win_start = window_start - CLICK_LOOKBACK_MS;
    let click_win_end = window_end;
    let out_of_range = AtomicBool::new(false);

    // Jobs: links first (their unions overlap the event scan), then events, clicks.
    let mut jobs: Vec<(u8, u32, u64, u64)> = Vec::new();
    for (kind, inputs) in [(0u8, &link_inputs), (1, &event_inputs), (2, &click_inputs)] {
        for &(f, s, e) in &inputs.chunks {
            jobs.push((kind, f, s, e));
        }
    }
    let next_job = AtomicUsize::new(0);
    let outs: Mutex<Vec<(usize, ScanOut)>> = Mutex::new(Vec::with_capacity(workers));
    let next_wid = AtomicUsize::new(0);
    thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                let wid = next_wid.fetch_add(1, Ordering::Relaxed);
                let mut ctx = users.ctx(wid);
                let mut out = ScanOut {
                    ebuckets: (0..n_eb).map(|_| Vec::with_capacity(reserve)).collect(),
                    long_events: Vec::new(),
                    ev_arena: FallbackArena::new(),
                    clicks: Vec::new(),
                    click_arena: FallbackArena::new(),
                };
                let mut dates = DateCache::default();
                let mut tmp_arena = FallbackArena::new();
                let mut reader = scan::Reader::default();
                let mut ring = Ring::new();
                let mut link_a = INVALID;
                macro_rules! resolve_link {
                    ($p:expr) => {{
                        let p = $p;
                        let u = users.intern_slow(p.bytes(), p.key, p.h, &mut ctx);
                        if p.dst == 0 {
                            link_a = u;
                        } else if link_a != INVALID && u != INVALID && link_a != u {
                            users.union(link_a, u);
                        }
                    }};
                }
                macro_rules! resolve_event {
                    ($p:expr) => {{
                        let p = $p;
                        let u = users.intern_slow(p.bytes(), p.key, p.h, &mut ctx);
                        out.ebuckets[p.b as usize][p.dst as usize].u = u;
                    }};
                }
                macro_rules! resolve_click {
                    ($p:expr) => {{
                        let p = $p;
                        let u = users.intern_slow(p.bytes(), p.key, p.h, &mut ctx);
                        out.clicks[p.dst as usize].u = u;
                    }};
                }
                loop {
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
                    let (bytes, first, stop, mapped) = reader
                        .load_mapped(&inputs.files[f], inputs.sizes[f], inputs.maps[f].as_slice(), start, end)
                        .expect("read input");
                    match kind {
                        0 => {
                            scan::for_each_record::<2>(bytes, first, stop, |rec| {
                                let [a, b] = rec.fields();
                                if a.is_empty() || b.is_empty() {
                                    return;
                                }
                                for (x, side) in [(a, 0), (b, 1)] {
                                    if let Some(p) = ring.push(users.pend(x, side, 0)) {
                                        resolve_link!(p);
                                    }
                                }
                            });
                            while let Some(p) = ring.pop() {
                                resolve_link!(p);
                            }
                        }
                        1 => scan::for_each_record::<7>(bytes, first, stop, |rec| {
                            let [event_id, user_id, ts_text, ingest_text, event_type, product_id, price_text] =
                                rec.fields();
                            if event_id.is_empty() {
                                return;
                            }
                            let Some(ingest_ts) = parse_f1_cached(ingest_text, &mut dates) else {
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
                                                && ingest_ts.saturating_sub(ts) <= LATE_LIMIT_MS
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
                            let key = id_key(event_id);
                            if key & LONG != 0 {
                                let r = out.ev_arena.push_fallback(event_id);
                                out.long_events.push((key, r.0));
                            }
                            let ing = ingest_ts.div_euclid(1000) - ingest_base;
                            if !(0..=u32::MAX as i64).contains(&ing) {
                                out_of_range.store(true, Ordering::Relaxed);
                                return;
                            }
                            let b = ebucket(key);
                            let (u, ts_off) = if valid { (0, (ts_val - window_start) as u32) } else { (INVALID, 0) };
                            let i = out.ebuckets[b].len() as u32;
                            out.ebuckets[b].push(ERec {
                                key,
                                ingest: ing as u32,
                                u,
                                ts: ts_off,
                                pc: (price_val << 8) | cat_id,
                            });
                            if valid {
                                if let Some(p) = ring.push(users.pend(user_id, i, b as u32)) {
                                    resolve_event!(p);
                                }
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
                            let i = out.clicks.len() as u32;
                            out.clicks.push(CRec { ts, cid, u: 0, camp: camp_idx });
                            if let Some(p) = ring.push(users.pend(user_id, i, 0)) {
                                resolve_click!(p);
                            }
                        }),
                    }
                    while let Some(p) = ring.pop() {
                        if kind == 1 {
                            resolve_event!(p);
                        } else {
                            resolve_click!(p);
                        }
                    }
                    if mapped {
                        scan::unmap_pages(bytes, first, stop);
                    }
                }
                outs.lock().unwrap().push((wid, out));
            });
        }
    });
    let t_scan = t0.elapsed();
    if users.overflow.load(Ordering::Relaxed) || out_of_range.load(Ordering::Relaxed) {
        if timing() {
            eprintln!("opaque: limits exceeded, exact solver");
        }
        return None;
    }
    let mut outs = outs.into_inner().unwrap();
    outs.sort_by_key(|(w, _)| *w);
    let outs: Vec<ScanOut> = outs.into_iter().map(|(_, o)| o).collect();

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
    let n_records: usize = outs.iter().map(|o| o.ebuckets.iter().map(Vec::len).sum::<usize>()).sum();
    let per_person = (n_records / n_persons.max(1)).max(1);
    let pbits = ((PERSON_TARGET / per_person).max(1).next_power_of_two().trailing_zeros()).min(20);
    let n_pb = (n_persons >> pbits) + 1;
    let next_eb = AtomicUsize::new(0);
    let dedup_out: Mutex<Vec<(Vec<Vec<PRec>>, Vec<u64>)>> = Mutex::new(Vec::with_capacity(workers));
    let max_bucket = (0..n_eb).map(|b| outs.iter().map(|o| o.ebuckets[b].len()).sum::<usize>()).max().unwrap_or(0);
    thread::scope(|scope| {
        for _ in 0..workers {
            let (outs, p_of, next_eb, dedup_out) = (&outs, &p_of, &next_eb, &dedup_out);
            scope.spawn(move || {
                let tsize = (max_bucket * 2).next_power_of_two().max(16);
                let tmask = tsize - 1;
                // (key, record index, stamp)
                let mut table: Vec<(u64, u32, u32)> = vec![(0, 0, 0); tsize];
                let mut occupied: Vec<u32> = Vec::with_capacity(max_bucket);
                let mut stamp = 0u32;
                let per_pb = n_records / n_pb.max(1) / workers.max(1) * 5 / 4 + 16;
                let mut pbuckets: Vec<Vec<PRec>> = (0..n_pb).map(|_| Vec::with_capacity(per_pb)).collect();
                let mut verify: Vec<u64> = Vec::new();
                loop {
                    let b = next_eb.fetch_add(1, Ordering::Relaxed);
                    if b >= n_eb {
                        break;
                    }
                    stamp += 1;
                    occupied.clear();
                    for (w, o) in outs.iter().enumerate() {
                        for (i, r) in o.ebuckets[b].iter().enumerate() {
                            let idx = ((w as u32) << 26) | i as u32;
                            let mut s = (mix64(r.key) as usize) & tmask;
                            loop {
                                let e = &mut table[s];
                                if e.2 != stamp {
                                    *e = (r.key, idx, stamp);
                                    occupied.push(s as u32);
                                    break;
                                }
                                if e.0 == r.key {
                                    if r.key & LONG != 0 {
                                        verify.push(r.key);
                                    }
                                    let cur = &outs[(e.1 >> 26) as usize].ebuckets[b][(e.1 & ((1 << 26) - 1)) as usize];
                                    if r.ingest < cur.ingest {
                                        e.1 = idx;
                                    }
                                    break;
                                }
                                s = (s + 1) & tmask;
                            }
                        }
                    }
                    for &s in &occupied {
                        let idx = table[s as usize].1;
                        let r = &outs[(idx >> 26) as usize].ebuckets[b][(idx & ((1 << 26) - 1)) as usize];
                        if r.u != INVALID {
                            let p = p_of[r.u as usize].load(Ordering::Relaxed);
                            pbuckets[(p >> pbits) as usize].push(PRec { p, ts: r.ts, pc: r.pc });
                        }
                    }
                }
                dedup_out.lock().unwrap().push((pbuckets, verify));
            });
        }
    });
    let dedup_out = dedup_out.into_inner().unwrap();
    let t_dedup = t0.elapsed();

    // Long event keys that matched: every copy must have the same bytes.
    let verify: Vec<u64> = dedup_out.iter().flat_map(|(_, v)| v.iter().copied()).collect();
    if !verify.is_empty() {
        let set = KeySet::new(&verify);
        let found: Mutex<Vec<(u64, usize, u64)>> = Mutex::new(Vec::new());
        thread::scope(|scope| {
            for (w, o) in outs.iter().enumerate() {
                let (set, found) = (&set, &found);
                scope.spawn(move || {
                    let mine: Vec<(u64, usize, u64)> = o
                        .long_events
                        .iter()
                        .filter(|(k, _)| set.contains(*k))
                        .map(|&(k, r)| (k, w, r))
                        .collect();
                    found.lock().unwrap().extend(mine);
                });
            }
        });
        let mut found = found.into_inner().unwrap();
        found.sort_unstable_by_key(|x| x.0);
        let bytes_of = |&(_, w, r): &(u64, usize, u64)| -> &[u8] {
            let (off, len) = PackedId(r).fallback_span(&outs[w].ev_arena.bytes);
            &outs[w].ev_arena.bytes[off..off + len]
        };
        for run in found.chunk_by(|a, b| a.0 == b.0) {
            let first = bytes_of(&run[0]);
            if run[1..].iter().any(|x| bytes_of(x) != first) {
                if timing() {
                    eprintln!("opaque: long event key shared by different IDs, exact solver");
                }
                return None;
            }
        }
    }
    let t_verify = t0.elapsed();

    // Clicks by person bucket.
    let click_parts: Vec<Vec<Vec<CRec>>> = thread::scope(|scope| {
        let handles: Vec<_> = outs
            .iter()
            .map(|o| {
                let p_of = &p_of;
                scope.spawn(move || {
                    let mut cb: Vec<Vec<CRec>> = (0..n_pb).map(|_| Vec::new()).collect();
                    for c in &o.clicks {
                        let p = p_of[c.u as usize].load(Ordering::Relaxed);
                        cb[(p >> pbits) as usize].push(CRec { u: p, ..*c });
                    }
                    cb
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
        .map(|pb| (dedup_out.iter().map(|(v, _)| v[pb].len()).sum::<usize>(), pb))
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
                    for (v, _) in dedup_out {
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
                    for (v, _) in dedup_out {
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
            "timing_ms: catalog={:.1} opaque_scan={:.1} persons={:.1} ev_dedup={:.1} verify={:.1} click_route={:.1} sessions_and_user_write={:.1} campaign_write={:.1} total={:.1} opaque=1 users={} persons={} buckets={}x{} pbits={} slots={}",
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
    std::mem::forget(dedup_out);
    std::mem::forget(click_parts);
    std::mem::forget(outs);
    std::mem::forget(users);
    std::mem::forget(p_of);
    std::mem::forget(names);
    Some(result)
}
