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
/// `bytes` must extend `SLACK` zero bytes past `stop`.
#[inline(always)]
pub fn for_each_record<'a, const N: usize>(
    bytes: &'a [u8],
    first: usize,
    stop: usize,
    mut f: impl FnMut(&Rec<'a, N>),
) {
    debug_assert!(bytes.len() >= stop + SLACK);
    let mut pos = first;
    while pos < stop {
        // The hardware streamer stops at 4 KiB page boundaries; fetch ahead so
        // a mapped chunk's next page is in cache when the scan reaches it.
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch::<_MM_HINT_T0>(bytes.as_ptr().wrapping_add(pos + PREFETCH) as *const i8);
        }
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2", not(feature = "scalar")))]
        {
            // SAFETY: `pos < stop` and `SLACK >= 128` zero bytes follow `stop`.
            if let Some(len) = unsafe { split_call::<N>(bytes, pos, &mut f) } {
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

/// Prefix XOR of a 128-bit mask given as halves: bit i is the XOR of bits
/// 0..=i. With PCLMULQDQ, one carry-less multiply by all-ones per half.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2", not(feature = "scalar")))]
#[inline(always)]
fn prefix_xor(lo: u64, hi: u64) -> u128 {
    #[cfg(target_feature = "pclmulqdq")]
    {
        use std::arch::x86_64::*;
        // SAFETY: pclmulqdq is enabled for this build.
        unsafe {
            let ones = _mm_set1_epi64x(-1);
            let a = _mm_clmulepi64_si128::<0x00>(_mm_set_epi64x(0, lo as i64), ones);
            let b = _mm_clmulepi64_si128::<0x00>(_mm_set_epi64x(0, hi as i64), ones);
            let plo = _mm_cvtsi128_si64(a) as u64;
            // The high half continues from the parity of the low half.
            let phi = (_mm_cvtsi128_si64(b) as u64) ^ (0u64.wrapping_sub((lo.count_ones() & 1) as u64));
            return (plo as u128) | ((phi as u128) << 64);
        }
    }
    #[allow(unreachable_code)]
    {
        let mut inside = (lo as u128) | ((hi as u128) << 64);
        inside ^= inside << 1;
        inside ^= inside << 2;
        inside ^= inside << 4;
        inside ^= inside << 8;
        inside ^= inside << 16;
        inside ^= inside << 32;
        inside ^= inside << 64;
        inside
    }
}

/// Split the line at `pos` from one 128-byte window and call `f` with its
/// fields if it is well formed (exactly as `parse_csv_line` decides). Returns
/// the line length, or `None` when no newline lies in the window (then the
/// general path handles the line). Masks stay in 64-bit halves; the 128-bit
/// quote check runs only for lines that contain a quote.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2", not(feature = "scalar")))]
#[inline(always)]
unsafe fn split_call<'a, const N: usize>(
    bytes: &'a [u8],
    pos: usize,
    f: &mut impl FnMut(&Rec<'a, N>),
) -> Option<usize> {
    use std::arch::x86_64::*;
    let p = unsafe { bytes.as_ptr().add(pos) };
    #[cfg(target_feature = "avx512bw")]
    let (nl0, nl1, c0, c1, q0, q1) = unsafe {
        let a = _mm512_loadu_si512(p as *const _);
        let b = _mm512_loadu_si512(p.add(64) as *const _);
        let nlv = _mm512_set1_epi8(b'\n' as i8);
        let cv = _mm512_set1_epi8(b',' as i8);
        let qv = _mm512_set1_epi8(b'"' as i8);
        (
            _mm512_cmpeq_epi8_mask(a, nlv),
            _mm512_cmpeq_epi8_mask(b, nlv),
            _mm512_cmpeq_epi8_mask(a, cv),
            _mm512_cmpeq_epi8_mask(b, cv),
            _mm512_cmpeq_epi8_mask(a, qv),
            _mm512_cmpeq_epi8_mask(b, qv),
        )
    };
    #[cfg(not(target_feature = "avx512bw"))]
    let (nl0, nl1, c0, c1, q0, q1) = unsafe {
        let nlv = _mm256_set1_epi8(b'\n' as i8);
        let cv = _mm256_set1_epi8(b',' as i8);
        let qv = _mm256_set1_epi8(b'"' as i8);
        let v: [__m256i; 4] = [
            _mm256_loadu_si256(p as *const __m256i),
            _mm256_loadu_si256(p.add(32) as *const __m256i),
            _mm256_loadu_si256(p.add(64) as *const __m256i),
            _mm256_loadu_si256(p.add(96) as *const __m256i),
        ];
        let m = |x: __m256i, c: __m256i| _mm256_movemask_epi8(_mm256_cmpeq_epi8(x, c)) as u32 as u64;
        (
            m(v[0], nlv) | (m(v[1], nlv) << 32),
            m(v[2], nlv) | (m(v[3], nlv) << 32),
            m(v[0], cv) | (m(v[1], cv) << 32),
            m(v[2], cv) | (m(v[3], cv) << 32),
            m(v[0], qv) | (m(v[1], qv) << 32),
            m(v[2], qv) | (m(v[3], qv) << 32),
        )
    };
    let len = if nl0 != 0 {
        nl0.trailing_zeros() as usize
    } else if nl1 != 0 {
        64 + nl1.trailing_zeros() as usize
    } else {
        return None;
    };
    // Keep only bits before the newline (bzhi with an index of 64 keeps all).
    let (lo, hi) = (len.min(64) as u32, len.saturating_sub(64) as u32);
    // SAFETY: bmi2 is enabled for this build (cfg above).
    let (c0, c1) = unsafe { (_bzhi_u64(c0, lo), _bzhi_u64(c1, hi)) };
    if (c0.count_ones() + c1.count_ones()) as usize != N - 1 {
        return Some(len);
    }
    let (q0, q1) = unsafe { (_bzhi_u64(q0, lo), _bzhi_u64(q1, hi)) };
    if q0 | q1 != 0 {
        // Quotes are legal only as pairs around whole fields. With P the
        // prefix XOR of the quote mask (set from an opening quote up to its
        // closing quote): no comma may lie inside a pair, every opening quote
        // must sit at a field start, every closing quote at a field end, and
        // the count must be even.
        let commas = (c0 as u128) | ((c1 as u128) << 64);
        let quotes = (q0 as u128) | ((q1 as u128) << 64);
        let starts = (commas << 1) | 1;
        let ends = (commas >> 1) | (1u128 << (len - 1));
        let inside = prefix_xor(q0, q1);
        let bad = (inside & commas) | (quotes & inside & !starts) | (quotes & !inside & !ends);
        if bad != 0 || (q0.count_ones() + q1.count_ones()) & 1 != 0 {
            return Some(len);
        }
    }
    let line = unsafe { std::slice::from_raw_parts(p, len) };
    let mut ends = [len; N];
    let mut k = 0usize;
    let mut m = c0;
    while m != 0 {
        ends[k] = m.trailing_zeros() as usize;
        m &= m - 1;
        k += 1;
    }
    let mut m = c1;
    while m != 0 {
        ends[k] = 64 + m.trailing_zeros() as usize;
        m &= m - 1;
        k += 1;
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

    #[cfg(all(target_arch = "x86_64", target_feature = "avx2", target_feature = "bmi2", not(feature = "scalar")))]
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
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
