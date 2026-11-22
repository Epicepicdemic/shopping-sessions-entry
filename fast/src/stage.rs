//! Per-bucket cache-line staging for the event scatter (`SHOPPING_STAGE=1`).
//!
//! Each scan worker keeps one 64-byte line (four 16-byte records) per bucket.
//! A full line is written to the bucket's span, a 64-byte-aligned array of
//! lines, as one whole cache line: on x86 with non-temporal stores, so the
//! destination line is not read first (no read-for-ownership) and the scatter
//! does not evict the scan's working set. At the end the partial line goes
//! into the span's spare capacity and each span becomes the bucket's
//! `Vec<Rec16>` in exactly the order the records were pushed.
//!
//! The spans are never reallocated after `finish` and are never dropped
//! through `Vec`: consumers hand them to `io_util::release` (madvise and
//! forget).

use crate::fastpath::Rec16;

static STAGE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// `SHOPPING_STAGE=1`: stage the scatter in cache lines. Read once per process.
pub fn enabled() -> bool {
    use std::sync::atomic::Ordering::Relaxed;
    match STAGE.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var("SHOPPING_STAGE").is_ok_and(|v| v == "1");
            STAGE.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct Line(pub [Rec16; 4]);

const _: () = assert!(std::mem::size_of::<Line>() == 64 && std::mem::size_of::<Rec16>() == 16);

pub struct Stager {
    stage: Vec<Line>,
    fill: Vec<u8>,
    spans: Vec<Vec<Line>>,
}

impl Stager {
    pub fn new(buckets: usize, reserve_records: usize) -> Self {
        Self {
            stage: vec![Line([Rec16::BLANK; 4]); buckets],
            fill: vec![0; buckets],
            spans: (0..buckets).map(|_| Vec::with_capacity(reserve_records / 4 + 2)).collect(),
        }
    }

    #[inline(always)]
    pub fn push(&mut self, b: usize, r: Rec16) {
        let f = self.fill[b] as usize;
        self.stage[b].0[f] = r;
        if f == 3 {
            self.fill[b] = 0;
            let line = self.stage[b];
            flush(&mut self.spans[b], &line);
        } else {
            self.fill[b] = f as u8 + 1;
        }
    }

    /// The buckets' records, in push order.
    pub fn finish(self) -> Vec<Vec<Rec16>> {
        fence();
        let Stager { stage, fill, spans } = self;
        spans
            .into_iter()
            .enumerate()
            .map(|(b, mut v)| {
                let f = fill[b] as usize;
                if f > 0 && v.len() == v.capacity() {
                    v.reserve(1);
                }
                let lines = v.len();
                let ptr = v.as_mut_ptr() as *mut Rec16;
                let cap = v.capacity() * 4;
                // SAFETY: the span has room for one more line (reserved above);
                // the records are Copy and written into that line's slots.
                unsafe {
                    for i in 0..f {
                        ptr.add(lines * 4 + i).write(stage[b].0[i]);
                    }
                }
                std::mem::forget(v);
                // SAFETY: ptr holds lines * 4 + f initialised records of a
                // buffer of `cap` records; it is released, never reallocated.
                unsafe { Vec::from_raw_parts(ptr, lines * 4 + f, cap) }
            })
            .collect()
    }
}

#[inline(always)]
fn flush(v: &mut Vec<Line>, line: &Line) {
    if v.len() == v.capacity() {
        v.reserve(v.capacity().max(16));
    }
    // SAFETY: capacity > len (reserved above); Line is 64-byte aligned.
    unsafe {
        let dst = v.as_mut_ptr().add(v.len());
        store_line(dst, line);
        v.set_len(v.len() + 1);
    }
}

/// SAFETY: `dst` is a valid, 64-byte-aligned line slot.
#[inline(always)]
unsafe fn store_line(dst: *mut Line, src: &Line) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx"))]
    unsafe {
        use std::arch::x86_64::*;
        let s = src as *const Line as *const __m256i;
        let d = dst as *mut __m256i;
        _mm256_stream_si256(d, _mm256_load_si256(s));
        _mm256_stream_si256(d.add(1), _mm256_load_si256(s.add(1)));
        return;
    }
    #[allow(unreachable_code)]
    unsafe {
        dst.write(*src)
    }
}

/// Non-temporal stores become visible to other threads after a store fence.
#[inline(always)]
fn fence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_sfence()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_buckets_keep_push_order() {
        let n = 37;
        let mut st = Stager::new(n, 3);
        let mut want: Vec<Vec<(u64, u64)>> = vec![Vec::new(); n];
        let mut rng = 0x9E3779B97F4A7C15u64;
        for i in 0..20000u64 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let b = (rng % n as u64) as usize;
            let r = Rec16 { key: i, data: rng };
            st.push(b, r);
            want[b].push((i, rng));
        }
        let got = st.finish();
        for b in 0..n {
            assert_eq!(got[b].iter().map(|r| (r.key, r.data)).collect::<Vec<_>>(), want[b], "bucket {b}");
        }
    }
}
