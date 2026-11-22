//! Positioned reads of CSV byte ranges into a reused buffer, and a single-pass
//! line and field splitter.
//!
//! A chunk is a raw byte range `[start, end)` of one file. It owns every line
//! whose first byte lies in that range, except the file's first line (the
//! header). Reading into a private buffer instead of mapping the file avoids
//! page faults during the scan and page-table teardown at exit, and it leaves
//! `SLACK` zero bytes after the last line so SIMD loads never need bounds checks.

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::io_util::{MmapFile, csv_files};
use crate::parse::{find_newline, parse_csv_line};

/// Zero bytes kept after the last owned line; at least one 128-byte window.
pub const SLACK: usize = 192;
/// Prefetch distance in bytes ahead of the current line.
#[cfg(target_arch = "x86_64")]
const PREFETCH: usize = 4096;
const GROW: usize = 64 * 1024;

pub struct Inputs {
    pub files: Vec<File>,
    pub sizes: Vec<u64>,
    pub chunks: Vec<(u32, u64, u64)>,
    /// Read-only mappings of the same files.
    pub maps: Vec<MmapFile>,
}

/// Open every `*.csv` in `folder` and cut each file into `chunk`-byte ranges.
pub fn plan(folder: &Path, chunk: u64) -> io::Result<Inputs> {
    let mut files = Vec::new();
    let mut sizes = Vec::new();
    let mut chunks = Vec::new();
    let mut maps = Vec::new();
    for path in csv_files(folder) {
        let file = File::open(&path)?;
        let size = file.metadata()?.len();
        let index = files.len() as u32;
        let mut start = 0;
        while start < size {
            let end = (start + chunk).min(size);
            chunks.push((index, start, end));
            start = end;
        }
        files.push(file);
        sizes.push(size);
        maps.push(MmapFile::open(&path)?);
    }
    Ok(Inputs {
        files,
        sizes,
        chunks,
        maps,
    })
}

unsafe extern "C" {
    fn madvise(addr: *mut core::ffi::c_void, len: usize, advice: i32) -> i32;
}

/// After a chunk was scanned from a mapping, drop its page-table entries from
/// this thread (they re-fault from the page cache if a neighbour needs them),
/// so exit has no input mappings to tear down.
pub fn unmap_pages(bytes: &[u8], first: usize, stop: usize) {
    let base = bytes.as_ptr() as usize;
    let start = (base + first).div_ceil(4096) * 4096;
    let end = (base + stop) / 4096 * 4096;
    if end > start {
        unsafe {
            madvise(start as *mut core::ffi::c_void, end - start, 4); // MADV_DONTNEED
        }
    }
}

/// First owned line and the end of the last owned line of chunk
/// `[start, end)` of a mapped file, by the same rules as `Reader::load`.
fn owned_range(map: &[u8], start: usize, end: usize) -> Option<(usize, usize)> {
    let after_nl = |from: usize| find_newline(&map[from..]).map(|i| from + i + 1);
    let first = if start == 0 {
        after_nl(0)?
    } else if map[start - 1] == b'\n' {
        start
    } else {
        after_nl(start)?
    };
    if first >= end {
        return None;
    }
    let stop = after_nl(end - 1).unwrap_or(map.len());
    Some((first, stop))
}

#[derive(Default)]
pub struct Reader {
    buf: Vec<u8>,
    /// File offset of `buf[0]`.
    base: u64,
    /// Valid bytes in `buf`.
    valid: usize,
}

impl Reader {
    fn fill(&mut self, file: &File, size: u64, want: usize) -> io::Result<()> {
        let want = want.min((size - self.base) as usize);
        if self.buf.len() < want + SLACK {
            self.buf.resize(want + SLACK, 0);
        }
        while self.valid < want {
            let n = file.read_at(
                &mut self.buf[self.valid..want],
                self.base + self.valid as u64,
            )?;
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "input file shrank"));
            }
            self.valid += n;
        }
        Ok(())
    }

    /// Index just past the first `\n` at or after `from`, reading more as
    /// needed; `None` at end of file.
    fn line_end(&mut self, file: &File, size: u64, from: usize) -> io::Result<Option<usize>> {
        let mut at = from;
        loop {
            if at < self.valid {
                if let Some(nl) = find_newline(&self.buf[at..self.valid]) {
                    return Ok(Some(at + nl + 1));
                }
                at = self.valid;
            }
            if self.base + self.valid as u64 >= size {
                return Ok(None);
            }
            let want = self.valid + GROW;
            self.fill(file, size, want)?;
        }
    }

    /// Chunk `[start, end)` straight from the file's mapping when at least
    /// `SLACK` bytes follow its last line there; otherwise (the file's tail,
    /// or no mapping) a padded copy. Returns the bytes, the owned line range,
    /// and whether the bytes are the mapping.
    pub fn load_mapped<'a>(
        &'a mut self,
        file: &File,
        size: u64,
        map: &'a [u8],
        start: u64,
        end: u64,
    ) -> io::Result<(&'a [u8], usize, usize, bool)> {
        if map.len() as u64 == size && size > 0 {
            let Some((first, stop)) = owned_range(map, start as usize, end as usize) else {
                return Ok((&map[..0], 0, 0, true));
            };
            if stop + SLACK <= map.len() {
                return Ok((map, first, stop, true));
            }
            let len = stop - first;
            self.buf.clear();
            self.buf.extend_from_slice(&map[first..stop]);
            self.buf.resize(len + SLACK, 0);
            return Ok((&self.buf[..], 0, len, false));
        }
        let (bytes, first, stop) = self.load(file, size, start, end)?;
        Ok((bytes, first, stop, false))
    }

    /// Load chunk `[start, end)` of `file`. Returns the buffer and the range of
    /// buffer indices holding its owned lines; `SLACK` zero bytes follow it.
    pub fn load(
        &mut self,
        file: &File,
        size: u64,
        start: u64,
        end: u64,
    ) -> io::Result<(&[u8], usize, usize)> {
        self.base = start.saturating_sub(1);
        self.valid = 0;
        self.fill(file, size, (end - self.base) as usize + 4096)?;
        // First owned line: after the header in the first chunk, otherwise the
        // first line that starts at or after `start`.
        let first = if start == 0 {
            match self.line_end(file, size, 0)? {
                Some(i) => i,
                None => return Ok((&self.buf[..0], 0, 0)),
            }
        } else if self.buf[0] == b'\n' {
            1
        } else {
            match self.line_end(file, size, 1)? {
                Some(i) => i,
                None => return Ok((&self.buf[..0], 0, 0)),
            }
        };
        let end_index = (end - self.base) as usize;
        if first >= end_index {
            return Ok((&self.buf[..0], 0, 0));
        }
        // The last owned line is the one holding byte `end - 1`.
        let stop = match self.line_end(file, size, end_index - 1)? {
            Some(i) => i,
            None => self.valid,
        };
        if self.buf.len() < stop + SLACK {
            self.buf.resize(stop + SLACK, 0);
        }
        self.buf[stop..stop + SLACK].fill(0);
        Ok((&self.buf[..stop + SLACK], first, stop))
    }
}

/// A well-formed line and the position of the delimiter after each field.
/// Fields are cut on demand: once the line passed the quote rules, a field is
/// quoted exactly when its first byte is a quote, so no flags are stored.
#[derive(Clone, Copy)]
pub struct Rec<'a, const N: usize> {
    line: &'a [u8],
    ends: [usize; N],
}

impl<'a, const N: usize> Rec<'a, N> {
    /// Field `i`, without its quotes.
    #[inline(always)]
    pub fn get(&self, i: usize) -> &'a [u8] {
        let s = if i == 0 { 0 } else { self.ends[i - 1] + 1 };
        let e = self.ends[i];
        // SAFETY: a line is always followed by a readable byte (its newline or
        // the scan buffer's zero slack), so byte `s <= len` is readable; a field
        // opening with a quote was checked to close with one, so e - s >= 2.
        unsafe {
            let q = (*self.line.as_ptr().add(s) == b'"') as usize;
            self.line.get_unchecked(s + q..e - q)
        }
    }

    #[inline(always)]
    pub fn fields(&self) -> [&'a [u8]; N] {
        std::array::from_fn(|i| self.get(i))
    }

    /// From fields that `parse_csv_line` cut out of `line`.
    #[inline]
    fn from_fields(line: &'a [u8], fields: [&'a [u8]; N]) -> Self {
        let base = line.as_ptr() as usize;
        let mut ends = [0usize; N];
        let mut raw = 0usize;
        for i in 0..N {
            let o = fields[i].as_ptr() as usize - base;
            let quoted = (o != raw) as usize;
            ends[i] = o + fields[i].len() + quoted;
            raw = ends[i] + 1;
        }
        Self { line, ends }
    }
}

/// Call `f` with the fields of every well-formed line in `bytes[first..stop]`
/// (malformed lines are skipped, exactly as `parse_csv_line` decides).
/// `bytes` must have `SLACK` readable bytes after `stop`.
#[inline(always)]
pub fn for_each_record<'a, const N: usize>(
    bytes: &'a [u8],
    first: usize,
    stop: usize,
    mut f: impl FnMut(&Rec<'a, N>),
) {
    if first >= stop {
        return;
    }
    debug_assert!(bytes.len() >= stop + SLACK);
    if block_scan() {
        return for_each_record_blocks::<N>(bytes, first, stop, f);
    }
    let mut pos = first;
    while pos < stop {
        // The hardware streamer stops at 4 KiB page boundaries; fetch ahead so
        // a mapped chunk's next page is in cache when the scan reaches it.
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch::<_MM_HINT_T0>(bytes.as_ptr().wrapping_add(pos + PREFETCH) as *const i8);
        }
        #[cfg(all(
            not(feature = "scalar"),
            any(target_arch = "x86_64", all(target_arch = "aarch64", target_endian = "little"))
        ))]
        {
            // SAFETY: `pos < stop` and at least 128 bytes follow `stop`.
            if let Some(len) = unsafe { split_call::<N>(bytes, pos, stop, &mut f) } {
                pos += len + 1;
                continue;
            }
        }
        let rest = &bytes[pos..stop];
        let (line, next) = match find_newline(rest) {
            Some(nl) => (&rest[..nl], pos + nl + 1),
            None => (rest, stop),
        };
        if let Some(fields) = parse_csv_line::<N>(line) {
            f(&Rec::from_fields(line, fields));
        }
        pos = next;
    }
}

// ------------------------------------------------------------------ block scanner
static BLOCKS: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// `SHOPPING_BLOCKSCAN=1`: scan each chunk once in 64-byte blocks (masks are
/// computed once per block and carried across rows) instead of a 128-byte
/// window per line. Read once per process.
pub fn block_scan() -> bool {
    use std::sync::atomic::Ordering::Relaxed;
    match BLOCKS.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = std::env::var("SHOPPING_BLOCKSCAN").is_ok_and(|v| v == "1");
            BLOCKS.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

thread_local! {
    /// Per block k (slot k + 1; slots 0 and nb + 1 stay zero): newline,
    /// comma and quote masks and the running quote parity, reused across chunks.
    static BLOCK_MASKS: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Newline, comma and quote masks of the 64 bytes at `p` (bit i = byte i).
/// SAFETY: 64 bytes at `p` must be readable.
#[cfg_attr(
    all(target_arch = "x86_64", target_feature = "avx512bw", not(feature = "scalar")),
    target_feature(enable = "avx512bw")
)]
#[cfg_attr(
    all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512bw"), not(feature = "scalar")),
    target_feature(enable = "avx2")
)]
#[inline]
unsafe fn block_masks(p: *const u8) -> (u64, u64, u64) {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", not(feature = "scalar")))]
    unsafe {
        use std::arch::x86_64::*;
        let v = _mm512_loadu_si512(p.cast());
        return (
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)),
        );
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512bw"), not(feature = "scalar")))]
    unsafe {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p.cast());
        let b = _mm256_loadu_si256(p.add(32).cast());
        let mask = |byte: u8| {
            let v = _mm256_set1_epi8(byte as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)) as u32 as u64;
            lo | (hi << 32)
        };
        return (mask(b'\n'), mask(b','), mask(b'"'));
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2"), not(feature = "scalar")))]
    unsafe {
        use std::arch::x86_64::*;
        let a = _mm_loadu_si128(p.cast());
        let b = _mm_loadu_si128(p.add(16).cast());
        let c = _mm_loadu_si128(p.add(32).cast());
        let d = _mm_loadu_si128(p.add(48).cast());
        let mask = |byte: u8| {
            let v = _mm_set1_epi8(byte as i8);
            let m = |x| _mm_movemask_epi8(_mm_cmpeq_epi8(x, v)) as u64;
            m(a) | (m(b) << 16) | (m(c) << 32) | (m(d) << 48)
        };
        return (mask(b'\n'), mask(b','), mask(b'"'));
    }
    #[cfg(all(target_arch = "aarch64", target_endian = "little", not(feature = "scalar")))]
    unsafe {
        use std::arch::aarch64::*;
        let a = vld1q_u8(p);
        let b = vld1q_u8(p.add(16));
        let c = vld1q_u8(p.add(32));
        let d = vld1q_u8(p.add(48));
        let bits = vcreate_u8(0x8040_2010_0804_0201);
        let weights = vcombine_u8(bits, bits);
        let mask = |byte| {
            let v = vdupq_n_u8(byte);
            let a = vandq_u8(vceqq_u8(a, v), weights);
            let b = vandq_u8(vceqq_u8(b, v), weights);
            let c = vandq_u8(vceqq_u8(c, v), weights);
            let d = vandq_u8(vceqq_u8(d, v), weights);
            let pairs = vpaddq_u8(a, b);
            let pairs_hi = vpaddq_u8(c, d);
            let quads = vpaddq_u8(pairs, pairs_hi);
            let octets = vpaddq_u8(quads, quads);
            vgetq_lane_u64::<0>(vreinterpretq_u64_u8(octets))
        };
        return (mask(b'\n'), mask(b','), mask(b'"'));
    }
    #[allow(unreachable_code)]
    {
        let (mut nl, mut c, mut q) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            // SAFETY: the caller guarantees 64 readable bytes.
            let x = unsafe { *p.add(i) };
            nl |= ((x == b'\n') as u64) << i;
            c |= ((x == b',') as u64) << i;
            q |= ((x == b'"') as u64) << i;
        }
        (nl, c, q)
    }
}

/// Inclusive prefix XOR of a 64-bit mask (bit i = XOR of bits 0..=i).
#[inline(always)]
fn prefix_xor64(q: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "pclmulqdq", not(feature = "scalar")))]
    unsafe {
        use std::arch::x86_64::*;
        return _mm_cvtsi128_si64(_mm_clmulepi64_si128::<0x00>(_mm_set_epi64x(0, q as i64), _mm_set1_epi64x(-1)))
            as u64;
    }
    #[allow(unreachable_code)]
    {
        let mut x = q;
        x ^= x << 1;
        x ^= x << 2;
        x ^= x << 4;
        x ^= x << 8;
        x ^= x << 16;
        x ^= x << 32;
        x
    }
}

/// Bits lo..=hi (lo <= hi < 64).
#[inline(always)]
fn bit_range(lo: usize, hi: usize) -> u64 {
    (u64::MAX >> (63 - hi)) & (u64::MAX << lo)
}

/// `for_each_record`, scanning `bytes[first..stop]` once in 64-byte blocks.
/// Pass 1 computes each block's newline, comma and quote masks and the
/// running quote parity (one carry-less multiply per block). Pass 2 walks the
/// newlines; a row's quote state is the running parity XOR the parity just
/// before the row, and the row is accepted by exactly the rules of
/// `split_call` / `parse_csv_line`: N - 1 commas, no comma inside a quoted
/// pair, an opening quote only at a field start, a closing quote only at a
/// field end, an even number of quotes. Rows of any length are handled; the
/// last line without a newline goes to `parse_csv_line`.
pub fn for_each_record_blocks<'a, const N: usize>(
    bytes: &'a [u8],
    first: usize,
    stop: usize,
    mut f: impl FnMut(&Rec<'a, N>),
) {
    if stop <= first {
        return;
    }
    let n = stop - first;
    let nb = n.div_ceil(64);
    assert!(bytes.len() >= first + 64 * nb, "the scan buffer must extend 64 bytes past stop");
    BLOCK_MASKS.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();
        buf.resize(4 * (nb + 2), 0);
        let base = bytes.as_ptr().wrapping_add(first);
        let mut carry = 0u64;
        for k in 0..nb {
            #[cfg(target_arch = "x86_64")]
            unsafe {
                use std::arch::x86_64::*;
                _mm_prefetch::<_MM_HINT_T0>(base.wrapping_add(64 * k + PREFETCH) as *const i8);
            }
            // SAFETY: first + 64 * nb <= bytes.len() (asserted above).
            let (mut nl, mut c, mut q) = unsafe { block_masks(base.add(64 * k)) };
            if 64 * (k + 1) > n {
                let keep = bit_range(0, n - 64 * k - 1);
                nl &= keep;
                c &= keep;
                q &= keep;
            }
            let p = prefix_xor64(q) ^ carry;
            carry = 0u64.wrapping_sub(p >> 63);
            let i = 4 * (k + 1);
            buf[i] = nl;
            buf[i + 1] = c;
            buf[i + 2] = q;
            buf[i + 3] = p;
        }
        let mut rs = 0usize;
        for kb in 0..nb {
            let mut m = buf[4 * (kb + 1)];
            while m != 0 {
                let nlpos = 64 * kb + m.trailing_zeros() as usize;
                m &= m - 1;
                block_row::<N>(&buf, bytes, first, rs, nlpos, &mut f);
                rs = nlpos + 1;
            }
        }
        if rs < n {
            let line = &bytes[first + rs..stop];
            if let Some(fields) = parse_csv_line::<N>(line) {
                f(&Rec::from_fields(line, fields));
            }
        }
    });
}

/// Check and emit the row bytes[first + rs .. first + nlpos] (nlpos: its newline).
#[inline(always)]
fn block_row<'a, const N: usize>(
    buf: &[u64],
    bytes: &'a [u8],
    first: usize,
    rs: usize,
    nlpos: usize,
    f: &mut impl FnMut(&Rec<'a, N>),
) {
    let (k0, k1) = (rs / 64, nlpos / 64);
    let before = if rs == 0 {
        0
    } else {
        (buf[4 * ((rs - 1) / 64 + 1) + 3] >> ((rs - 1) % 64)) & 1
    };
    let fill = 0u64.wrapping_sub(before);
    let mut bad = 0u64;
    let mut commas = 0u32;
    for k in k0..=k1 {
        let i = 4 * (k + 1);
        let (nl, c, q, p) = (buf[i], buf[i + 1], buf[i + 2], buf[i + 3]);
        let lo = if k == k0 { rs % 64 } else { 0 };
        let hi = if k == k1 { nlpos % 64 } else { 63 };
        let mut rm = bit_range(lo, hi);
        if k == k1 {
            rm &= !(1u64 << hi);                  // the newline itself
        }
        let inside = p ^ fill;
        let cm = c & rm;
        let qm = q & rm;
        let starts = (c << 1) | (buf[i - 3] >> 63) | (nl << 1) | (buf[i - 4] >> 63) | (k == 0) as u64;
        let ends = (c >> 1) | (buf[i + 5] << 63) | (nl >> 1) | (buf[i + 4] << 63);
        bad |= (inside & cm) | (qm & inside & !starts) | (qm & !inside & !ends);
        if k == k1 {
            bad |= inside & (1u64 << hi);         // an odd number of quotes
        }
        commas += cm.count_ones();
    }
    if bad != 0 || commas as usize != N - 1 {
        return;
    }
    let len = nlpos - rs;
    let mut ends = [len; N];
    let mut j = 0usize;
    for k in k0..=k1 {
        let lo = if k == k0 { rs % 64 } else { 0 };
        let hi = if k == k1 { nlpos % 64 } else { 63 };
        let mut m = buf[4 * (k + 1) + 1] & bit_range(lo, hi);
        while m != 0 {
            ends[j] = 64 * k + m.trailing_zeros() as usize - rs;
            m &= m - 1;
            j += 1;
        }
    }
    // SAFETY: rs <= nlpos < n and first + n <= bytes.len().
    let line = unsafe { bytes.get_unchecked(first + rs..first + nlpos) };
    f(&Rec { line, ends });
}

/// Inclusive quote parity in the two halves of a row.
#[cfg(all(
    not(feature = "scalar"),
    any(target_arch = "x86_64", all(target_arch = "aarch64", target_endian = "little"))
))]
#[inline(always)]
fn prefix_xor(lo: u64, hi: u64) -> u128 {
    let plo = prefix_xor64(lo);
    let phi = prefix_xor64(hi) ^ 0u64.wrapping_sub(plo >> 63);
    plo as u128 | ((phi as u128) << 64)
}

/// Split and validate a row in one or two blocks. The general path handles
/// longer rows and lines with no final newline. Bytes beyond the owned range
/// may come from the next chunk, and are never interpreted as part of a row.
#[cfg(all(
    not(feature = "scalar"),
    any(target_arch = "x86_64", all(target_arch = "aarch64", target_endian = "little"))
))]
#[inline(always)]
unsafe fn split_call<'a, const N: usize>(
    bytes: &'a [u8],
    pos: usize,
    stop: usize,
    f: &mut impl FnMut(&Rec<'a, N>),
) -> Option<usize> {
    let p = unsafe { bytes.as_ptr().add(pos) };
    let (nl0, mut c0, mut q0) = unsafe { block_masks(p) };
    let (len, c1, q1) = if nl0 != 0 {
        let len = nl0.trailing_zeros() as usize;
        let keep = (1u64 << len) - 1;
        c0 &= keep;
        q0 &= keep;
        (len, 0, 0)
    } else {
        let (nl1, c1, q1) = unsafe { block_masks(p.add(64)) };
        if nl1 == 0 {
            return None;
        }
        let extra = nl1.trailing_zeros() as usize;
        let keep = (1u64 << extra) - 1;
        (64 + extra, c1 & keep, q1 & keep)
    };
    if pos + len >= stop {
        return None;
    }
    if len == 0 || (c0.count_ones() + c1.count_ones()) as usize != N - 1 {
        return Some(len);
    }
    if (q0 | q1) != 0 {
        let commas = c0 as u128 | ((c1 as u128) << 64);
        let quotes = q0 as u128 | ((q1 as u128) << 64);
        let starts = (commas << 1) | 1;
        let ends = (commas >> 1) | (1u128 << (len - 1));
        let inside = prefix_xor(q0, q1);
        let bad = (inside & commas) | (quotes & inside & !starts) | (quotes & !inside & !ends);
        if bad != 0 || (inside >> len) & 1 != 0 {
            return Some(len);
        }
    }
    let line = unsafe { std::slice::from_raw_parts(p, len) };
    let mut ends = [len; N];
    let mut k = 0usize;
    for (mut m, base) in [(c0, 0), (c1, 64)] {
        while m != 0 {
            ends[k] = base + m.trailing_zeros() as usize;
            m &= m - 1;
            k += 1;
        }
    }
    f(&Rec { line, ends });
    Some(len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_csv_line_scalar;
    use std::io::Write;

    fn records<const N: usize>(text: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut bytes = text.to_vec();
        bytes.resize(text.len() + SLACK, 0);
        let mut out = Vec::new();
        for_each_record::<N>(&bytes, 0, text.len(), |r| {
            out.push(r.fields().iter().map(|x| x.to_vec()).collect())
        });
        out
    }

    fn oracle<const N: usize>(text: &[u8]) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut lines: Vec<&[u8]> = text.split(|&b| b == b'\n').collect();
        if text.last() == Some(&b'\n') {
            lines.pop();
        }
        for line in lines {
            if let Some(f) = parse_csv_line_scalar::<N>(line) {
                out.push(f.iter().map(|x| x.to_vec()).collect());
            }
        }
        out
    }

    #[cfg(all(
        not(feature = "scalar"),
        any(target_arch = "x86_64", all(target_arch = "aarch64", target_endian = "little"))
    ))]
    #[test]
    fn prefix_xor_matches_shifts() {
        let mut rng = 0x1234_5678_9ABC_DEF1u64;
        for _ in 0..200000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let lo = rng & rng.rotate_left(17);
            let hi = rng.rotate_left(29) & rng.rotate_left(41);
            let mut want = (lo as u128) | ((hi as u128) << 64);
            let mut acc = 0u128;
            let mut out = 0u128;
            for i in 0..128 {
                acc ^= (want >> i) & 1;
                out |= acc << i;
            }
            want = out;
            assert_eq!(prefix_xor(lo, hi), want);
        }
    }
    #[test]
    fn splitter_matches_scalar_oracle() {
        let mut rng = 0x9E3779B97F4A7C15u64;
        let alphabet = b"ab,\"\n\r 1";
        for case in 0..200000 {
            let len = case % 300;
            let mut text = Vec::with_capacity(len);
            for _ in 0..len {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                text.push(alphabet[(rng % alphabet.len() as u64) as usize]);
            }
            assert_eq!(records::<2>(&text), oracle::<2>(&text), "{:?}", text);
            assert_eq!(records::<4>(&text), oracle::<4>(&text), "{:?}", text);
            assert_eq!(records::<7>(&text), oracle::<7>(&text), "{:?}", text);
        }
        let long = [b"x".repeat(200), b",\"y\"\n\"a\",\"\"\n".to_vec()].concat();
        assert_eq!(records::<2>(&long), oracle::<2>(&long));
    }

    #[test]
    fn splitter_respects_owned_end_with_nonzero_slack() {
        for count in [0, 1, 60, 61, 62, 63, 64, 125, 126, 127, 128, 192] {
            let row = format!("\"{}\",tail", "x".repeat(count));
            for prefix in [0, 1, 13, 63] {
                let mut storage = vec![b'x'; prefix];
                storage.extend_from_slice(row.as_bytes());
                let stop = storage.len();
                storage.extend_from_slice(b"\nextra,row\n");
                storage.resize(stop + SLACK, b',');
                let mut got = Vec::new();
                for_each_record::<2>(&storage, prefix, stop, |rec| {
                    got.push(rec.fields().map(|field| field.to_vec()));
                });
                assert_eq!(got, [[vec![b'x'; count], b"tail".to_vec()]]);
            }
        }
    }

    fn block_records<const N: usize>(text: &[u8], tail: u8) -> Vec<Vec<Vec<u8>>> {
        let mut bytes = text.to_vec();
        bytes.resize(text.len() + SLACK, tail);       // bytes after stop need not be zero (mapped input)
        let mut out = Vec::new();
        for_each_record_blocks::<N>(&bytes, 0, text.len(), |r| {
            out.push(r.fields().iter().map(|x| x.to_vec()).collect())
        });
        out
    }

    #[test]
    fn block_scanner_matches_scalar_oracle() {
        let mut rng = 0x2545F4914F6CDD1Du64;
        let alphabets: [&[u8]; 3] = [b"ab,\"\n\r 1", b"aaaaaaaaaaaaaaaa,\"\n", b"x,\"\"\n\nabc,,"];
        for case in 0..150000 {
            let alphabet = alphabets[case % 3];
            let len = case % 400;
            let mut text = Vec::with_capacity(len);
            for _ in 0..len {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                text.push(alphabet[(rng % alphabet.len() as u64) as usize]);
            }
            // Bytes after stop: zero slack, or (mapped input) the next chunk's data. A line
            // without its newline is only ever a file's last line, followed by zero
            // slack; every other line is followed by its newline (Rec::get relies on it).
            let ends_nl = text.last() == Some(&b'\n');
            let tail = if case % 2 == 0 || !ends_nl { 0 } else { [b',', b'"', b'\n'][case % 3] };
            assert_eq!(block_records::<2>(&text, tail), oracle::<2>(&text), "{:?}", text);
            assert_eq!(block_records::<4>(&text, tail), oracle::<4>(&text), "{:?}", text);
            assert_eq!(block_records::<7>(&text, tail), oracle::<7>(&text), "{:?}", text);
        }
        // realistic rows: quoted fields, rows across block edges, long rows
        let mut text = Vec::new();
        for i in 0..3000 {
            let q = |s: &str, on: bool| if on { format!("\"{s}\"") } else { s.to_string() };
            let pad = "y".repeat(i % 150);
            let row = format!("{},{},2026-10-0{}T12:00:00Z,{},{},p{},{}\n", q(&format!("e{i}"), i % 3 == 0),
                              q(&format!("u{i}{pad}"), i % 5 == 0), i % 9 + 1, q("1790812800000", i % 7 == 0),
                              q("view", i % 2 == 0), i % 50, if i % 11 == 0 { "\"bad" } else { "" });
            text.extend_from_slice(row.as_bytes());
        }
        assert_eq!(block_records::<7>(&text, 0), oracle::<7>(&text));
        assert_eq!(block_records::<7>(&text, b'"'), oracle::<7>(&text));
    }

    #[test]
    fn block_scanner_survives_mutations_at_block_edges() {
        // Realistic rows, then one byte changed, inserted or deleted at every
        // offset within 3 bytes of each 64-byte block edge, for start offsets 0..64.
        let mut base = Vec::new();
        for i in 0..12 {
            let row = format!("\"e{i}9zz\",u{i}qq{},\"2026-10-0{}T08:15:00Z\",1790812800{:03},\"purchase\",p{},\"12{}.50\"\n",
                              "x".repeat(i * 7 % 40), i % 9 + 1, i, i % 30, i);
            base.extend_from_slice(row.as_bytes());
        }
        let muts: [&[u8]; 9] = [b"\"", b",", b"\n", b"\r", b"\t", b" ", "\u{e9}".as_bytes(), b"", b"\"\""];
        for start in [0usize, 1, 17, 31, 63] {
            let text = &base[start..];
            let edges: Vec<usize> = (64..text.len()).step_by(64).collect();
            for &e in &edges {
                for off in e.saturating_sub(3)..(e + 3).min(text.len()) {
                    for m in &muts {
                        for insert in [false, true] {
                            let mut t = text[..off].to_vec();
                            t.extend_from_slice(m);
                            t.extend_from_slice(&text[off + (!insert) as usize..]);
                            assert_eq!(block_records::<7>(&t, 0), oracle::<7>(&t), "start {start} off {off} mut {m:?}");
                            assert_eq!(block_records::<4>(&t, 0), oracle::<4>(&t));
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn chunks_own_each_line_once() {
        let dir = std::env::temp_dir().join(format!("scan-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut text = b"h1,h2\n".to_vec();
        for i in 0..5000 {
            let pad = "z".repeat(i % 97);
            text.extend_from_slice(format!("a{i}{pad},b\n").as_bytes());
            if i % 13 == 0 {
                text.extend_from_slice(b"\n");
            }
        }
        text.extend_from_slice(b"last,line");
        let path = dir.join("t.csv");
        std::fs::File::create(&path).unwrap().write_all(&text).unwrap();
        let expected = oracle::<2>(&text[6..]);
        for chunk in [1u64, 2, 7, 64, 100, 4096, 1 << 20] {
            let inputs = plan(&dir, chunk).unwrap();
            let mut reader = Reader::default();
            let mut got = Vec::new();
            for &(f, s, e) in &inputs.chunks {
                let (bytes, first, stop) = reader
                    .load(&inputs.files[f as usize], inputs.sizes[f as usize], s, e)
                    .unwrap();
                for_each_record::<2>(bytes, first, stop, |r| {
                    got.push(r.fields().iter().map(|y| y.to_vec()).collect::<Vec<_>>())
                });
            }
            assert_eq!(got, expected, "chunk {chunk}");
            let mut got = Vec::new();
            for &(f, s, e) in &inputs.chunks {
                let (bytes, first, stop, _) = reader
                    .load_mapped(
                        &inputs.files[f as usize],
                        inputs.sizes[f as usize],
                        inputs.maps[f as usize].as_slice(),
                        s,
                        e,
                    )
                    .unwrap();
                for_each_record::<2>(bytes, first, stop, |r| {
                    got.push(r.fields().iter().map(|y| y.to_vec()).collect::<Vec<_>>())
                });
            }
            assert_eq!(got, expected, "mapped chunk {chunk}");
            for mapped in [false, true] {
                let mut got = Vec::new();
                for &(f, s, e) in &inputs.chunks {
                    let (bytes, first, stop) = if mapped {
                        let (b, x, y, _) = reader
                            .load_mapped(&inputs.files[f as usize], inputs.sizes[f as usize],
                                         inputs.maps[f as usize].as_slice(), s, e)
                            .unwrap();
                        (b, x, y)
                    } else {
                        reader.load(&inputs.files[f as usize], inputs.sizes[f as usize], s, e).unwrap()
                    };
                    for_each_record_blocks::<2>(bytes, first, stop, |r| {
                        got.push(r.fields().iter().map(|y| y.to_vec()).collect::<Vec<_>>())
                    });
                }
                assert_eq!(got, expected, "block scanner, chunk {chunk}, mapped {mapped}");
            }
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
