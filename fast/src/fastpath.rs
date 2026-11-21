//! Verified fast path for generator-shaped IDs.
//!
//! The kit generator writes IDs as a prefix and the lowercase base-36 form of a
//! 40-bit bijection of a counter: event counters are dense and each person's
//! events take a contiguous run; a user counter is `person * 512 + alias`. Two
//! secret keys (events, users) are recovered from a startup sample. Nothing
//! about correctness depends on the keys: decoding is a bijection between
//! canonical ID strings and counters under any key, so equal counters mean
//! equal IDs, and every rules-level decision (which IDs form a person, which
//! copies are duplicates, where a person's events lie) is checked on the actual
//! data. A row, person or bucket that fails a check goes to the exact solver.
//!
//! - Event rows whose ID is canonical (`e` + base 36, no leading zero, below
//!   2^40) and whose counter is below a bound derived from the input size go
//!   to a counter bucket of `2^bucket_bits()` counters. All copies of an event share a
//!   counter, so a bucket deduplicates its rows exactly with a direct array.
//! - A person slot `p` is fast only if (a) every link touching an ID of `p`
//!   joins IDs of `p` with alias < 32, all in one component (`LINKED`), or no
//!   link touches `p` and every row uses alias 0 (`UNLINKED`); (b) every row of
//!   `p` (event winner or click) uses an alias in that component; (c) every
//!   surviving event of `p` lies in one bucket; (d) no exact-path event row
//!   names an ID of `p`; and (e) every click ID of `p` packs. Anything else
//!   marks `p` complex, and all of `p`'s events and clicks go to the exact path.

use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};

pub const M40: u64 = (1 << 40) - 1;
const A: u64 = 0x9E37_79B9_7F4B & M40;
const B: u64 = 0xD6E8_FEB8_6659 & M40;
const AINV: u64 = inverse(A) & M40;
const BINV: u64 = inverse(B) & M40;

/// Most counters a bucket can hold. Records keep the counter's low `QB_MAX`
/// bits, so any width up to this works; `bucket_bits` picks one at startup.
pub const QB_MAX: u32 = 16;
pub const Q_MAX: usize = 1 << QB_MAX;

/// L2 size per core in bytes: CPUID leaf 4 (Intel) or 0x8000001D (AMD), else
/// sysfs.
pub fn l2_bytes() -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::{__cpuid, __cpuid_count};
        let walk = |leaf: u32| -> Option<usize> {
            for i in 0..16 {
                let r = __cpuid_count(leaf, i);
                let ty = r.eax & 0x1F;
                if ty == 0 {
                    break;
                }
                if (r.eax >> 5) & 7 == 2 && (ty == 1 || ty == 3) {
                    let ways = ((r.ebx >> 22) & 0x3FF) as usize + 1;
                    let parts = ((r.ebx >> 12) & 0x3FF) as usize + 1;
                    let line = (r.ebx & 0xFFF) as usize + 1;
                    let sets = r.ecx as usize + 1;
                    return Some(ways * parts * line * sets);
                }
            }
            None
        };
        let (max, max_ext) = (__cpuid(0).eax, __cpuid(0x8000_0000).eax);
        if let Some(v) = (max >= 4).then(|| walk(4)).flatten() {
            return Some(v);
        }
        if let Some(v) = (max_ext >= 0x8000_001D).then(|| walk(0x8000_001D)).flatten() {
            return Some(v);
        }
    }
    for i in 0..8 {
        let dir = format!("/sys/devices/system/cpu/cpu0/cache/index{i}");
        let read = |f: &str| std::fs::read_to_string(format!("{dir}/{f}")).ok();
        let Some(level) = read("level") else { break };
        let ty = read("type").unwrap_or_default();
        if level.trim() == "2" && matches!(ty.trim(), "Data" | "Unified") {
            let size = read("size")?;
            let size = size.trim();
            let (num, mul) = match size.strip_suffix('K') {
                Some(n) => (n, 1024),
                None => match size.strip_suffix('M') {
                    Some(n) => (n, 1 << 20),
                    None => (size, 1),
                },
            };
            return num.parse::<usize>().ok().map(|n| n * mul);
        }
    }
    None
}

fn cpu_list(text: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for part in text.trim().split(',').filter(|p| !p.is_empty()) {
        let mut ends = part.splitn(2, '-').map(|x| x.trim().parse::<usize>());
        match (ends.next(), ends.next()) {
            (Some(Ok(a)), None) => out.push(a),
            (Some(Ok(a)), Some(Ok(b))) if b >= a && b - a < 4096 => out.extend(a..=b),
            _ => return Vec::new(),
        }
    }
    out
}

/// Whether two of this process's CPUs are hyperthreads of one core (assumed
/// when the topology cannot be read).
pub fn siblings_shared() -> bool {
    let allowed = std::fs::read_to_string("/proc/self/status").ok().and_then(|s| {
        s.lines()
            .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
            .map(cpu_list)
    });
    let Some(allowed) = allowed.filter(|a| !a.is_empty()) else { return true };
    for &c in &allowed {
        let path = format!("/sys/devices/system/cpu/cpu{c}/topology/thread_siblings_list");
        let Ok(text) = std::fs::read_to_string(path) else { return true };
        if cpu_list(&text).iter().any(|&o| o != c && allowed.contains(&o)) {
            return true;
        }
    }
    false
}

/// Bucket width (log2 counters) for this machine. What dedup hits at random
/// is a bucket's slots and stamps, 20 bytes per counter and thread. On the
/// worker (2 MB L2, siblings) that set overflowing the core's L2 by 1.25x
/// (64K buckets) cost dedup nothing measurable, while smaller buckets cost the
/// scan 36-64 ms (more scatter tails). So the width is the widest, up to 2^15
/// (the largest with a scorer result), whose per-core set stays within 1.25x
/// of L2: 2^15 on 1 MB+ L2 (Cascade Lake, Ice Lake, the worker), smaller only
/// on small-L2 CPUs. `SHOPPING_QB` (13-16) overrides.
pub fn bucket_bits() -> (u32, Option<usize>, bool) {
    let l2 = l2_bytes();
    let shared = siblings_shared();
    if let Some(v) = std::env::var("SHOPPING_QB").ok().and_then(|v| v.parse::<u32>().ok()) {
        if (13..=QB_MAX).contains(&v) {
            return (v, l2, shared);
        }
    }
    let budget = l2.unwrap_or(1 << 20) * 5 / 4;
    let per_core = if shared { 2 } else { 1 };
    let mut qb = 15;
    while qb > 13 && (20usize << qb) * per_core > budget {
        qb -= 1;
    }
    (qb, l2, shared)
}

/// Person slots per status word group.
pub const UNLINKED: u8 = 0;
pub const LINKED: u8 = 1;
pub const COMPLEX: u8 = 2;

/// `FastRec::user` flags.
pub const INVALID: u64 = 1 << 63;
pub const OPAQUE: u64 = 1 << 62;

const fn inverse(x: u64) -> u64 {
    // Newton iteration for the inverse of an odd number modulo 2^64.
    let mut inv = x;
    let mut i = 0;
    while i < 6 {
        inv = inv.wrapping_mul(2u64.wrapping_sub(x.wrapping_mul(inv)));
        i += 1;
    }
    inv
}

#[inline(always)]
pub fn scramble(counter: u64, key: u64) -> u64 {
    let mut m = (counter.wrapping_mul(A) ^ key) & M40;
    m ^= m >> 17;
    m.wrapping_mul(B) & M40
}

/// `(counter * A) XOR key`, recovered from a scrambled value without the key.
#[inline(always)]
fn unmix(value: u64) -> u64 {
    let y = value.wrapping_mul(BINV) & M40;
    (y ^ (y >> 17) ^ (y >> 34)) & M40
}

#[inline(always)]
fn counter_of(mixed: u64, key: u64) -> u64 {
    ((mixed ^ key).wrapping_mul(AINV)) & M40
}

/// The canonical lowercase base-36 value after a one-byte prefix, if `id` is
/// exactly what the generator would write for some value below 2^40.
#[inline(always)]
pub fn decode(id: &[u8], prefix: u8) -> Option<u64> {
    let n = id.len();
    if !(2..=9).contains(&n) || id[0] != prefix || (n > 2 && id[1] == b'0') {
        return None;
    }
    let mut v = 0u64;
    for &c in &id[1..] {
        let d = if c.is_ascii_digit() {
            c - b'0'
        } else if c.is_ascii_lowercase() {
            c - b'a' + 10
        } else {
            return None;
        };
        v = v * 36 + d as u64;
    }
    if v > M40 { None } else { Some(v) }
}

/// `decode` for an ID inside a scan buffer: the caller guarantees 16 readable
/// bytes from `id.as_ptr()`. Same result as `decode` for every input.
#[inline(always)]
pub fn decode_padded(id: &[u8], prefix: u8) -> Option<u64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1", not(feature = "scalar")))]
    {
        use std::arch::x86_64::*;
        let n = id.len();
        if !(2..=9).contains(&n) || id[0] != prefix || (n > 2 && id[1] == b'0') {
            return None;
        }
        let digits = n - 1;
        // Right-align the digits in lanes 0..8 (most significant first).
        const fn ctrl(digits: usize) -> [i8; 16] {
            let mut c = [-1i8; 16];
            let mut j = 8 - digits;
            while j < 8 {
                c[j] = (1 + j - (8 - digits)) as i8;
                j += 1;
            }
            c
        }
        const CTRL: [[i8; 16]; 9] = [
            ctrl(0), ctrl(1), ctrl(2), ctrl(3), ctrl(4), ctrl(5), ctrl(6), ctrl(7), ctrl(8),
        ];
        unsafe {
            let x = _mm_loadu_si128(id.as_ptr() as *const __m128i);
            let c = _mm_loadu_si128(CTRL[digits].as_ptr() as *const __m128i);
            let v = _mm_shuffle_epi8(x, c);
            let used = _mm_cmpgt_epi8(c, _mm_set1_epi8(-1));
            let dig = _mm_and_si128(
                _mm_cmpgt_epi8(v, _mm_set1_epi8(b'0' as i8 - 1)),
                _mm_cmpgt_epi8(_mm_set1_epi8(b'9' as i8 + 1), v),
            );
            let low = _mm_and_si128(
                _mm_cmpgt_epi8(v, _mm_set1_epi8(b'a' as i8 - 1)),
                _mm_cmpgt_epi8(_mm_set1_epi8(b'z' as i8 + 1), v),
            );
            let ok = _mm_or_si128(dig, low);
            if _mm_movemask_epi8(_mm_andnot_si128(ok, used)) != 0 {
                return None;
            }
            let val = _mm_and_si128(
                used,
                _mm_or_si128(
                    _mm_and_si128(dig, _mm_sub_epi8(v, _mm_set1_epi8(48))),
                    _mm_and_si128(low, _mm_sub_epi8(v, _mm_set1_epi8(87))),
                ),
            );
            let pairs = _mm_maddubs_epi16(val, _mm_set1_epi16(0x0124)); // d0*36 + d1
            let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_0510)); // w0*1296 + w1
            let hi = _mm_cvtsi128_si32(quads) as u32 as u64;
            let lo = _mm_extract_epi32::<1>(quads) as u32 as u64;
            let value = hi * 1_679_616 + lo;
            return if value > M40 { None } else { Some(value) };
        }
    }
    #[allow(unreachable_code)]
    decode(id, prefix)
}

/// `decode` of an `e` ID and a `u` ID in one vector pass (Argon's dual
/// decode). Both slices must have 16 readable bytes from their start (scan
/// buffer fields). Same results as two `decode` calls.
#[inline(always)]
pub fn decode_pair_padded(e: &[u8], u: &[u8]) -> (Option<u64>, Option<u64>) {
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1", not(feature = "scalar")))]
    {
        use std::arch::x86_64::*;
        let shape = |id: &[u8], prefix: u8| {
            let n = id.len();
            (2..=9).contains(&n) && id[0] == prefix && (n == 2 || id[1] != b'0')
        };
        let (se, su) = (shape(e, b'e'), shape(u, b'u'));
        if !se && !su {
            return (None, None);
        }
        // Right-align `d` digits in an 8-lane half (most significant first).
        const fn ctrl8(d: usize, base: u8) -> u64 {
            let mut c = 0u64;
            let mut j = 0;
            while j < 8 {
                let b = if j >= 8 - d { base + (j - (8 - d)) as u8 } else { 0x80 };
                c |= (b as u64) << (8 * j);
                j += 1;
            }
            c
        }
        const LO: [u64; 9] = [ctrl8(0, 0), ctrl8(1, 0), ctrl8(2, 0), ctrl8(3, 0), ctrl8(4, 0), ctrl8(5, 0), ctrl8(6, 0), ctrl8(7, 0), ctrl8(8, 0)];
        const HI: [u64; 9] = [ctrl8(0, 8), ctrl8(1, 8), ctrl8(2, 8), ctrl8(3, 8), ctrl8(4, 8), ctrl8(5, 8), ctrl8(6, 8), ctrl8(7, 8), ctrl8(8, 8)];
        let de = if se { e.len() - 1 } else { 0 };
        let du = if su { u.len() - 1 } else { 0 };
        // SAFETY: each slice has 16 readable bytes, so bytes 1..9 are readable.
        unsafe {
            let lo = std::ptr::read_unaligned(e.as_ptr().add(1) as *const u64);
            let hi = std::ptr::read_unaligned(u.as_ptr().add(1) as *const u64);
            let x = _mm_set_epi64x(hi as i64, lo as i64);
            let c = _mm_set_epi64x(HI[du] as i64, LO[de] as i64);
            let v = _mm_shuffle_epi8(x, c);
            let (bad, val) = {
                let used = _mm_cmpgt_epi8(c, _mm_set1_epi8(-1));
                let dig = _mm_and_si128(
                    _mm_cmpgt_epi8(v, _mm_set1_epi8(b'0' as i8 - 1)),
                    _mm_cmpgt_epi8(_mm_set1_epi8(b'9' as i8 + 1), v),
                );
                let low = _mm_and_si128(
                    _mm_cmpgt_epi8(v, _mm_set1_epi8(b'a' as i8 - 1)),
                    _mm_cmpgt_epi8(_mm_set1_epi8(b'z' as i8 + 1), v),
                );
                let bad = _mm_movemask_epi8(_mm_andnot_si128(_mm_or_si128(dig, low), used)) as u32;
                let val = _mm_and_si128(
                    used,
                    _mm_or_si128(
                        _mm_and_si128(dig, _mm_sub_epi8(v, _mm_set1_epi8(48))),
                        _mm_and_si128(low, _mm_sub_epi8(v, _mm_set1_epi8(87))),
                    ),
                );
                (bad, val)
            };
            let pairs = _mm_maddubs_epi16(val, _mm_set1_epi16(0x0124));
            let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_0510));
            let q0 = _mm_cvtsi128_si32(quads) as u32 as u64;
            let q1 = _mm_extract_epi32::<1>(quads) as u32 as u64;
            let q2 = _mm_extract_epi32::<2>(quads) as u32 as u64;
            let q3 = _mm_extract_epi32::<3>(quads) as u32 as u64;
            let ve = q0 * 1_679_616 + q1;
            let vu = q2 * 1_679_616 + q3;
            let re = (se && bad & 0xFF == 0 && ve <= M40).then_some(ve);
            let ru = (su && bad >> 8 == 0 && vu <= M40).then_some(vu);
            return (re, ru);
        }
    }
    #[allow(unreachable_code)]
    (decode(e, b'e'), decode(u, b'u'))
}

/// The ID string the generator writes for `value` (already scrambled).
pub fn encode(prefix: u8, value: u64, out: &mut Vec<u8>) {
    if value > M40 {
        return encode_slow(prefix, value, out);
    }
    // Two base-36 digits per table entry: 36^2 = 1296 pairs.
    const PAIRS: [[u8; 2]; 1296] = {
        const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
        let mut t = [[0u8; 2]; 1296];
        let mut i = 0;
        while i < 1296 {
            t[i] = [DIGITS[i / 36], DIGITS[i % 36]];
            i += 1;
        }
        t
    };
    // value < 2^40 < 36^8: eight digits, most significant first.
    let hi = (value / 1_679_616) as usize;
    let lo = (value % 1_679_616) as usize;
    let mut buf = [0u8; 8];
    buf[0..2].copy_from_slice(&PAIRS[hi / 1296]);
    buf[2..4].copy_from_slice(&PAIRS[hi % 1296]);
    buf[4..6].copy_from_slice(&PAIRS[lo / 1296]);
    buf[6..8].copy_from_slice(&PAIRS[lo % 1296]);
    let zeros = (u64::from_be_bytes(buf) ^ 0x3030_3030_3030_3030).leading_zeros() as usize / 8;
    let skip = zeros.min(7);
    out.clear();
    out.push(prefix);
    out.extend_from_slice(&buf[skip..]);
}

fn encode_slow(prefix: u8, value: u64, out: &mut Vec<u8>) {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = [0u8; 13];
    let mut at = buf.len();
    let mut rest = value;
    loop {
        at -= 1;
        buf[at] = DIGITS[(rest % 36) as usize];
        rest /= 36;
        if rest == 0 {
            break;
        }
    }
    out.clear();
    out.push(prefix);
    out.extend_from_slice(&buf[at..]);
}

#[derive(Clone, Copy, Debug)]
pub struct Keys {
    pub event_key: u64,
    pub user_key: u64,
    /// Exclusive bound on fast event counters (a multiple of `Q_MAX`).
    pub event_cap: u64,
    /// Exclusive bound on fast person slots.
    pub person_cap: u64,
    /// Estimated event lines in this input.
    pub est_lines: u64,
}

impl Keys {
    /// The user counter (`person * 512 + alias`) of a canonical `u` ID whose
    /// person slot is below the bound. `id` must have 16 readable bytes.
    #[inline(always)]
    pub fn user_counter(&self, id: &[u8]) -> Option<u64> {
        let v = counter_of(unmix(decode_padded(id, b'u')?), self.user_key);
        ((v >> 9) < self.person_cap).then_some(v)
    }

    /// `event_counter` and `user_counter` together (both scan-buffer fields).
    #[inline(always)]
    pub fn pair_counters(&self, event_id: &[u8], user_id: &[u8]) -> (Option<u64>, Option<u64>) {
        let (e, u) = decode_pair_padded(event_id, user_id);
        let e = e.map(|v| counter_of(unmix(v), self.event_key)).filter(|&c| c < self.event_cap);
        let u = u
            .map(|v| counter_of(unmix(v), self.user_key))
            .filter(|&c| (c >> 9) < self.person_cap);
        (e, u)
    }

    /// `user_counter` for an ID anywhere (no padding guarantee).
    pub fn user_counter_unpadded(&self, id: &[u8]) -> Option<u64> {
        let v = counter_of(unmix(decode(id, b'u')?), self.user_key);
        ((v >> 9) < self.person_cap).then_some(v)
    }

    pub fn user_id(&self, counter: u64, out: &mut Vec<u8>) {
        encode(b'u', scramble(counter, self.user_key), out);
    }
}

/// Search `[0, bound)` for a counter `g` of `first` whose implied key puts the
/// `checks` and then 90% of `all` under the bound. Threads interleave from 0
/// upward, so a small true counter is found at once; stops at the first hit.
fn search(first: u64, checks: &[u64], all: &[u64], bound: u64, step: u64, threads: usize) -> Option<u64> {
    use std::sync::atomic::AtomicU64;
    let found = AtomicU64::new(u64::MAX);
    let limit = bound * step;
    let stride = threads as u64;
    std::thread::scope(|scope| {
        for t in 0..threads as u64 {
            let found = &found;
            scope.spawn(move || {
                let mut g = t;
                let mut n = 0u32;
                while g < bound {
                    n = n.wrapping_add(1);
                    if n & 0xFFFF == 0 && found.load(Ordering::Relaxed) != u64::MAX {
                        return;
                    }
                    let key = first ^ ((g * step).wrapping_mul(A) & M40);
                    if checks.iter().all(|&m| counter_of(m, key) < limit)
                        && all.iter().filter(|&&m| counter_of(m, key) < limit).count() * 10
                            >= all.len() * 9
                    {
                        found.store(key, Ordering::Relaxed);
                        return;
                    }
                    g += stride;
                }
            });
        }
    });
    let key = found.load(Ordering::Relaxed);
    (key != u64::MAX).then_some(key)
}

/// Propose keys from sampled event and user IDs (already `unmix`ed). Each
/// proposal must put at least 90% of the sample in bounds; the per-row checks
/// that follow are what correctness rests on.
pub fn recover(
    event_mixed: &[u64],
    user_mixed: &[u64],
    pairs: &[(u64, u64)],
    event_cap: u64,
    person_cap: u64,
    est_lines: u64,
    threads: usize,
) -> Option<Keys> {
    if event_mixed.len() < 64 || user_mixed.len() < 64 {
        return None;
    }
    // Users: alias 0 is the commonest alias, and its counter is a multiple of
    // 512, so the key's low 9 bits are the commonest low 9 bits.
    let mut counts = vec![0u32; 512];
    for &m in user_mixed {
        counts[(m & 511) as usize] += 1;
    }
    let low = (0..512).max_by_key(|&i| counts[i]).unwrap() as u64;
    let zero: Vec<u64> = user_mixed.iter().copied().filter(|&m| m & 511 == low).collect();
    let mut user_key = None;
    for first in 0..4.min(zero.len()) {
        let checks: Vec<u64> = zero.iter().skip(first + 1).take(4).copied().collect();
        user_key = search(zero[first], &checks, user_mixed, person_cap, 512, threads);
        if user_key.is_some() {
            break;
        }
    }
    let user_key = user_key?;
    // Events: a generator person's events take the next run of counters, so
    // the sampled row of the smallest person has a small counter. Try those
    // rows first; the search is exhaustive either way.
    let mut by_person: Vec<(u64, u64)> = pairs
        .iter()
        .map(|&(e, u)| (counter_of(u, user_key) >> 9, e))
        .collect();
    by_person.sort_unstable();
    let mut firsts: Vec<u64> = by_person.iter().take(3).map(|&(_, e)| e).collect();
    firsts.extend(event_mixed.iter().take(2));
    let mut event_key = None;
    for (i, &first) in firsts.iter().enumerate() {
        let checks: Vec<u64> = event_mixed.iter().filter(|&&m| m != first).skip(i).take(4).copied().collect();
        event_key = search(first, &checks, event_mixed, event_cap, 1, threads);
        if event_key.is_some() {
            break;
        }
    }
    Some(Keys {
        event_key: event_key?,
        user_key,
        event_cap,
        person_cap,
        est_lines,
    })
}

pub fn unmixed(id: &[u8], prefix: u8) -> Option<u64> {
    decode(id, prefix).map(unmix)
}

/// One event row on the fast path, 24 bytes. The category (16 bits) is split
/// over the spare top bytes of `key` and `user`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FastRec {
    /// Ingest seconds + 2^39 (bits 0..40; F1 ingest is whole seconds, years
    /// 0000-9999), the counter modulo `Q` (bits 40..56), category high byte
    /// (bits 56..64).
    pub key: u64,
    /// Bits 0..40: the user counter, or for `OPAQUE` an offset into the
    /// scanning thread's arena; bits 40..48: that thread's index (`OPAQUE`);
    /// bits 48..56: category low byte; bit 62 `OPAQUE`, bit 63 `INVALID`.
    pub user: u64,
    /// `ts - window_start` in ms (bits 0..40, valid rows only; windows up to
    /// 34.8 years) and the price (bits 40..64, at most 10,000,000).
    pub time_price: u64,
}

const _: () = assert!(std::mem::size_of::<FastRec>() == 24 && QB_MAX <= 16);

/// Largest scanning-thread index an `OPAQUE` record can name.
pub const MAX_WORKERS: usize = 256;

impl FastRec {
    /// `user` holds a counter (or `OPAQUE | wid << 40 | offset`) with bits
    /// 48..56 clear; `cat` < 65,536; `ts_off` < 2^40.
    #[inline(always)]
    pub fn new(ingest_ms: i64, counter: u64, user: u64, ts_off: u64, price: u32, cat: u32) -> Self {
        let sec = (ingest_ms.div_euclid(1000) + (1i64 << 39)) as u64 & M40;
        Self {
            key: sec | (((counter as usize & (Q_MAX - 1)) as u64) << 40) | (((cat >> 8) as u64) << 56),
            user: user | (((cat & 0xFF) as u64) << 48),
            time_price: (ts_off & M40) | ((price as u64) << 40),
        }
    }
    #[inline(always)]
    pub fn ingest(&self) -> u64 {
        self.key & M40
    }
    #[allow(dead_code)] // the Rec16 tests compare against it
    #[inline(always)]
    pub fn low(&self) -> usize {
        (self.key >> 40) as usize & (Q_MAX - 1)
    }
    #[allow(dead_code)] // the Rec16 tests compare against it
    #[inline(always)]
    pub fn is_invalid(&self) -> bool {
        self.user & INVALID != 0
    }
    #[inline(always)]
    pub fn is_opaque(&self) -> bool {
        self.user & OPAQUE != 0
    }
    /// The user counter (`person * 512 + alias`) of a non-opaque record.
    #[inline(always)]
    pub fn counter(&self) -> u64 {
        self.user & M40
    }
    /// Scanning thread and arena offset of an `OPAQUE` record's user bytes.
    #[inline(always)]
    pub fn opaque_ref(&self) -> (usize, usize) {
        (((self.user >> 40) & 0xFF) as usize, (self.user & M40) as usize)
    }
    #[inline(always)]
    pub fn cat(&self) -> u32 {
        (((self.key >> 56) as u32) << 8) | ((self.user >> 48) & 0xFF) as u32
    }
    #[inline(always)]
    pub fn ts_off(&self) -> i64 {
        (self.time_price & M40) as i64
    }
    #[inline(always)]
    pub fn price(&self) -> u32 {
        (self.time_price >> 40) as u32
    }
}

/// One event row on the fast path in 16 bytes. A row whose fields do not all
/// fit (a purchase's price and category, an opaque user, an ingest time more
/// than 68 years from the window, a counter of 2^32 or more) also keeps its
/// full 24-byte `FastRec` in its scanning thread's side list, named by `SIDE`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Rec16 {
    /// Bits 0..32: ingest seconds minus the ingest base, clamped (then `WIDE`);
    /// bits 32..48: the counter modulo `Q_MAX`; bits 48..56: `ts_off` bits 32..40,
    /// or for `SIDE` the scanning thread's index; bit 61 `WIDE` (ingest clamped,
    /// or no usable inline counter), bit 62 `SIDE`, bit 63 `INVALID`.
    pub key: u64,
    /// Bits 0..32: `ts_off` bits 0..32, or for `SIDE` the side-list index;
    /// bits 32..64: the user counter (zero for `WIDE` or `INVALID`).
    pub data: u64,
}

const _: () = assert!(std::mem::size_of::<Rec16>() == 16 && QB_MAX <= 16);

pub const R_WIDE: u64 = 1 << 61;
pub const R_SIDE: u64 = 1 << 62;
pub const R_INVALID: u64 = 1 << 63;
const M32: u64 = (1 << 32) - 1;

/// The ingest base for a window: inline ingest offsets cover +-68 years.
pub fn ingest_base(window_start_ms: i64) -> i64 {
    window_start_ms.div_euclid(1000) - (1i64 << 31)
}

impl Rec16 {
    pub const BLANK: Rec16 = Rec16 { key: 0, data: 0 };

    /// Encodes one scanned row (arguments as for `FastRec::new`); pushes the
    /// full record to `side` when the row does not fit. `plain_cat` is the
    /// category every non-purchase row carries.
    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    pub fn encode(
        ingest_ms: i64,
        base: i64,
        counter: u64,
        user: u64,
        ts_off: u64,
        price: u32,
        cat: u32,
        plain_cat: u32,
        wid: u64,
        side: &mut Vec<FastRec>,
    ) -> Rec16 {
        let rel = ingest_ms.div_euclid(1000) - base;
        let fits = (0..=M32 as i64).contains(&rel);
        let ing = rel.clamp(0, M32 as i64) as u64;
        let low = ((counter as usize & (Q_MAX - 1)) as u64) << 32;
        let invalid = user & INVALID != 0;
        let plain_user = user & (INVALID | OPAQUE) == 0 && user <= M32;
        if fits && invalid {
            return Rec16 { key: ing | low | R_INVALID, data: 0 };
        }
        if fits && plain_user && price == 0 && cat == plain_cat && ts_off <= M40 {
            return Rec16 {
                key: ing | low | ((ts_off >> 32) << 48),
                data: (ts_off & M32) | (user << 32),
            };
        }
        let idx = side.len() as u64;
        assert!(idx <= M32, "side list over 2^32 rows");
        side.push(FastRec::new(ingest_ms, counter, user, ts_off, price, cat));
        let wide = !fits || (!invalid && !plain_user);
        Rec16 {
            key: ing
                | low
                | ((wid & 0xFF) << 48)
                | R_SIDE
                | (if wide { R_WIDE } else { 0 })
                | (if invalid { R_INVALID } else { 0 }),
            data: idx | (if plain_user { user << 32 } else { 0 }),
        }
    }
    #[inline(always)]
    pub fn ing(&self) -> u32 {
        self.key as u32
    }
    #[inline(always)]
    pub fn low(&self) -> usize {
        (self.key >> 32) as usize & (Q_MAX - 1)
    }
    #[inline(always)]
    pub fn is_invalid(&self) -> bool {
        self.key & R_INVALID != 0
    }
    #[inline(always)]
    pub fn is_side(&self) -> bool {
        self.key & R_SIDE != 0
    }
    #[inline(always)]
    pub fn is_wide(&self) -> bool {
        self.key & R_WIDE != 0
    }
    /// Scanning thread and side-list index of a `SIDE` record.
    #[inline(always)]
    pub fn side_ref(&self) -> (usize, usize) {
        (((self.key >> 48) & 0xFF) as usize, (self.data & M32) as usize)
    }
    /// The full record of a `SIDE` row.
    #[inline(always)]
    pub fn full<'a>(&self, sides: &'a [Vec<FastRec>]) -> &'a FastRec {
        let (w, i) = self.side_ref();
        &sides[w][i]
    }
    /// The user counter of a valid row that is not `WIDE`.
    #[inline(always)]
    pub fn counter(&self) -> u64 {
        self.data >> 32
    }
    /// `ts - window_start` of a valid inline (not `SIDE`) row.
    #[inline(always)]
    pub fn ts_off(&self) -> i64 {
        ((self.data & M32) | (((self.key >> 48) & 0xFF) << 32)) as i64
    }
    /// Ingest in `FastRec::ingest` form, exact for every row.
    #[inline(always)]
    pub fn full_ingest(&self, base: i64, sides: &[Vec<FastRec>]) -> u64 {
        if self.is_wide() {
            self.full(sides).ingest()
        } else {
            ((self.ing() as i64 + base + (1i64 << 39)) as u64) & M40
        }
    }
    /// Whether `self` replaces `cur` as an event's winner: strictly earlier
    /// ingest, exactly as `FastRec::ingest` compares (equal keeps `cur`).
    #[inline(always)]
    pub fn earlier(&self, cur: &Rec16, base: i64, sides: &[Vec<FastRec>]) -> bool {
        let (a, b) = (self.ing(), cur.ing());
        a < b
            || (a == b
                && (self.key | cur.key) & R_WIDE != 0
                && self.full_ingest(base, sides) < cur.full_ingest(base, sides))
    }
}

/// One click on the fast path.
#[derive(Clone, Copy)]
pub struct FastClick {
    pub ts: i64,
    pub click_id: u64,
    pub user: u64,
    pub campaign: u32,
}

/// Per person slot, one 16-byte word so a check touches one cache line.
#[repr(C, align(16))]
pub struct Slot {
    pub status: AtomicU8,
    pub mask: AtomicU32,
    /// `root + 1` for a link node of the person's component (for its name), or 0.
    pub node: AtomicU32,
    /// `bucket + 1` of the first bucket holding one of the person's events.
    pub claim: AtomicU32,
}

const _: () = assert!(std::mem::size_of::<Slot>() == 16 && std::mem::align_of::<Slot>() == 16);

pub struct People {
    pub slots: Vec<Slot>,
    /// A `LINKED` person's name: its component's byte-wise smallest ID, packed.
    pub name: Vec<std::sync::atomic::AtomicU64>,
}

impl People {
    /// All-zero slots (`UNLINKED`, no mask, no node, no claim), from zeroed
    /// pages that the kernel supplies lazily: no initialisation pass.
    pub fn new(cap: usize) -> Self {
        let mut zeroed = std::mem::ManuallyDrop::new(vec![0u128; cap]);
        // SAFETY: `Slot` is 16 bytes with 16-byte alignment, like `u128`, and
        // all-zero bytes are a valid `Slot`.
        let slots = unsafe {
            Vec::from_raw_parts(zeroed.as_mut_ptr() as *mut Slot, zeroed.len(), zeroed.capacity())
        };
        let mut zeroed = std::mem::ManuallyDrop::new(vec![0u64; cap]);
        // SAFETY: AtomicU64 has the layout of u64, and zero is a valid value.
        let name = unsafe {
            Vec::from_raw_parts(zeroed.as_mut_ptr() as *mut std::sync::atomic::AtomicU64, zeroed.len(), zeroed.capacity())
        };
        Self { slots, name }
    }

    #[inline(always)]
    pub fn complex(&self, p: usize) {
        self.slots[p].status.store(COMPLEX, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn is_complex(&self, p: usize) -> bool {
        self.slots[p].status.load(Ordering::Relaxed) == COMPLEX
    }

    /// Check that a row's alias belongs to the person's component; mark the
    /// person complex otherwise. Returns whether the person is still fast.
    #[inline(always)]
    pub fn check_alias(&self, user: u64) -> bool {
        let slot = &self.slots[(user >> 9) as usize];
        let a = (user & 511) as u32;
        let ok = match slot.status.load(Ordering::Relaxed) {
            UNLINKED => a == 0,
            LINKED => a < 32 && slot.mask.load(Ordering::Relaxed) & (1 << a) != 0,
            _ => false,
        };
        if !ok {
            slot.status.store(COMPLEX, Ordering::Relaxed);
        }
        ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every field a consumer reads from a `Rec16` (inline or through the side
    /// list) equals the `FastRec` of the same row, and winner choice between
    /// copies of one event matches `FastRec::ingest` order, ties included.
    #[test]
    fn rec16_matches_fastrec() {
        let window_start: i64 = 1_767_225_600_000; // 2026-01-01
        let base = ingest_base(window_start);
        let plain_cat = 7u32;
        let mut rng = 0x9E3779B97F4A7C15u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut sides: Vec<Vec<FastRec>> = vec![Vec::new(); 3];
        let mut rows: Vec<(FastRec, Rec16)> = Vec::new();
        for i in 0..400_000u64 {
            let wid = i % 3;
            let counter = next() & M40;
            // Ingest: near the window, far outside it (clamped), or at the edges.
            let ingest_ms = match next() % 8 {
                0 => window_start - (next() % (1 << 46)) as i64,
                1 => window_start + (next() % (1 << 46)) as i64,
                2 => (base + (next() % 3) as i64 - 1) * 1000 + (next() % 1000) as i64,
                3 => (base + M32 as i64 + (next() % 3) as i64 - 1) * 1000,
                _ => window_start + (next() % (90 * 86_400_000)) as i64,
            };
            let user = match next() % 10 {
                0 => INVALID,
                1 => OPAQUE | (wid << 40) | (next() & M40),
                2 => (1u64 << 32) + (next() % (1 << 39)),
                3 => M32,
                _ => next() & M32,
            };
            let ts_off = if next() % 9 == 0 { M40 - (next() % 2) } else { next() % (1 << 40) };
            let (price, cat) = match next() % 12 {
                0 => ((next() % 10_000_001) as u32, (next() % 65_536) as u32),
                1 => (0, (next() % 65_536) as u32),
                2 => ((next() % 10_000_001) as u32, plain_cat),
                _ => (0, plain_cat),
            };
            let (price, cat) = if user & INVALID != 0 { (0, plain_cat) } else { (price, cat) };
            let f = FastRec::new(ingest_ms, counter, user, ts_off, price, cat);
            let r = Rec16::encode(ingest_ms, base, counter, user, ts_off, price, cat, plain_cat, wid, &mut sides[wid as usize]);
            rows.push((f, r));
        }
        for (f, r) in &rows {
            assert_eq!(r.low(), f.low());
            assert_eq!(r.is_invalid(), f.is_invalid());
            assert_eq!(r.full_ingest(base, &sides), f.ingest());
            if f.is_invalid() {
                continue;
            }
            if r.is_side() {
                let g = r.full(&sides);
                assert_eq!((g.key, g.user, g.time_price), (f.key, f.user, f.time_price));
            } else {
                assert!(!f.is_opaque());
                assert_eq!(r.counter(), f.counter());
                assert_eq!(r.ts_off(), f.ts_off());
                assert_eq!((f.price(), f.cat()), (0, plain_cat));
            }
            if !r.is_wide() {
                assert!(!f.is_opaque());
                assert_eq!(r.counter(), f.counter());
            }
        }
        for k in 0..rows.len() - 1 {
            let ((f1, r1), (f2, r2)) = (&rows[k], &rows[k + 1]);
            assert_eq!(r1.earlier(r2, base, &sides), f1.ingest() < f2.ingest());
            assert_eq!(r2.earlier(r1, base, &sides), f2.ingest() < f1.ingest());
            assert!(!r1.earlier(r1, base, &sides));
        }
    }
    #[test]
    fn padded_decode_matches_scalar_and_roundtrips() {
        let mut rng = 0x2545F4914F6CDD1Du64;
        let alphabet = b"0123456789abcdefghijklmnopqrstuvwxyzAZ_/:`{e u";
        for _ in 0..300_000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let len = (rng % 12) as usize;
            let mut buf = [0u8; 32];
            buf[0] = if rng % 7 == 0 { b'x' } else { b'e' };
            for (i, b) in buf.iter_mut().enumerate().skip(1) {
                let r = rng.rotate_left(i as u32 * 7) >> 3;
                *b = if r % 5 == 0 { alphabet[(r >> 5) as usize % alphabet.len()] } else { alphabet[(r >> 5) as usize % 36] };
            }
            assert_eq!(decode_padded(&buf[..len], b'e'), decode(&buf[..len], b'e'), "{:?}", &buf[..len]);
        }
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..300_000 {
            let mut bufs = [[0u8; 32]; 2];
            let mut lens = [0usize; 2];
            for (k, (buf, len)) in bufs.iter_mut().zip(lens.iter_mut()).enumerate() {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                *len = (rng % 12) as usize;
                buf[0] = if rng % 9 == 0 { b'x' } else if k == 0 { b'e' } else { b'u' };
                for (i, b) in buf.iter_mut().enumerate().skip(1) {
                    let r = rng.rotate_left(i as u32 * 5) >> 3;
                    *b = if r % 6 == 0 { alphabet[(r >> 5) as usize % alphabet.len()] } else { alphabet[(r >> 5) as usize % 36] };
                }
            }
            let (e, u) = (&bufs[0][..lens[0]], &bufs[1][..lens[1]]);
            assert_eq!(decode_pair_padded(e, u), (decode(e, b'e'), decode(u, b'u')), "{:?} {:?}", e, u);
        }
        let mut a = Vec::new();
        let mut b = Vec::new();
        let mut rng = 0xC2B2_AE3D_27D4_EB4Fu64;
        for i in 0..400_000u64 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let v = match i % 4 {
                0 => rng & M40,
                1 => rng % 1_679_616,
                2 => rng % 1296,
                _ => rng % 50,
            };
            encode(b'u', v, &mut a);
            encode_slow(b'u', v, &mut b);
            assert_eq!(a, b, "{v}");
        }
        let mut out = Vec::new();
        for v in [0u64, 1, 35, 36, 1295, 1 << 20, M40 - 1, M40] {
            encode(b'e', v, &mut out);
            let mut buf = out.clone();
            buf.resize(32, 0);
            assert_eq!(decode_padded(&buf[..out.len()], b'e'), Some(v));
            assert_eq!(decode(&out, b'e'), Some(v));
        }
        encode(b'e', M40 + 1, &mut out);
        assert_eq!(decode(&out, b'e'), None);
        let key = 0x1234_5678_9a;
        for c in [0u64, 7, 1 << 25, M40] {
            assert_eq!(counter_of(unmix(scramble(c, key)), key), c);
        }
    }
}
