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

use crate::io_util::csv_files;
use crate::parse::{find_newline, parse_csv_line};

/// Zero bytes kept after the last owned line; at least one 128-byte window.
pub const SLACK: usize = 192;
const GROW: usize = 64 * 1024;

pub struct Inputs {
    pub files: Vec<File>,
    pub sizes: Vec<u64>,
    pub chunks: Vec<(u32, u64, u64)>,
}

/// Open every `*.csv` in `folder` and cut each file into `chunk`-byte ranges.
pub fn plan(folder: &Path, chunk: u64) -> io::Result<Inputs> {
    let mut files = Vec::new();
    let mut sizes = Vec::new();
    let mut chunks = Vec::new();
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
    }
    Ok(Inputs {
        files,
        sizes,
        chunks,
    })
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

/// Call `f` with the fields of every well-formed line in `bytes[first..stop]`
/// (malformed lines are skipped, exactly as `parse_csv_line` decides).
/// `bytes` must extend `SLACK` zero bytes past `stop`.
#[inline(always)]
pub fn for_each_record<'a, const N: usize>(
    bytes: &'a [u8],
    first: usize,
    stop: usize,
    mut f: impl FnMut([&'a [u8]; N]),
) {
    debug_assert!(bytes.len() >= stop + SLACK);
    let mut pos = first;
    while pos < stop {
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(feature = "scalar")))]
        {
            // SAFETY: `pos < stop` and `SLACK >= 128` zero bytes follow `stop`.
            match unsafe { split_avx2::<N>(bytes, pos) } {
                Split::Line(len, fields) => {
                    if let Some(fields) = fields {
                        f(fields);
                    }
                    pos += len + 1;
                    continue;
                }
                Split::Long => {}
            }
        }
        let rest = &bytes[pos..stop];
        let (line, next) = match find_newline(rest) {
            Some(nl) => (&rest[..nl], pos + nl + 1),
            None => (rest, stop),
        };
        if let Some(fields) = parse_csv_line::<N>(line) {
            f(fields);
        }
        pos = next;
    }
}

#[allow(dead_code)]
enum Split<'a, const N: usize> {
    /// A line of this length (newline excluded), and its fields if well formed.
    Line(usize, Option<[&'a [u8]; N]>),
    /// No newline in the 128-byte window: use the general path.
    Long,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(feature = "scalar")))]
#[inline(always)]
unsafe fn split_avx2<'a, const N: usize>(bytes: &'a [u8], pos: usize) -> Split<'a, N> {
    use std::arch::x86_64::*;
    let p = unsafe { bytes.as_ptr().add(pos) };
    let (nl, comma, quote) = unsafe {
        let nlv = _mm256_set1_epi8(b'\n' as i8);
        let cv = _mm256_set1_epi8(b',' as i8);
        let qv = _mm256_set1_epi8(b'"' as i8);
        let mut nl = 0u128;
        let mut comma = 0u128;
        let mut quote = 0u128;
        for k in 0..4 {
            let v = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
            nl |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nlv)) as u32 as u128) << (32 * k);
            comma |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, cv)) as u32 as u128) << (32 * k);
            quote |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, qv)) as u32 as u128) << (32 * k);
        }
        (nl, comma, quote)
    };
    if nl == 0 {
        return Split::Long;
    }
    let len = nl.trailing_zeros() as usize;
    let below = (1u128 << len) - 1;
    let commas = comma & below;
    if commas.count_ones() as usize != N - 1 {
        return Split::Line(len, None);
    }
    let quotes = quote & below;
    if quotes != 0 {
        // Quotes are legal only as pairs around whole fields. With P the
        // prefix XOR of the quote mask (set from an opening quote up to its
        // closing quote): no comma may lie inside a pair, every opening quote
        // must sit at a field start, every closing quote at a field end, and
        // the count must be even.
        let starts = (commas << 1) | 1;
        let ends = (commas >> 1) | (1u128 << (len - 1));
        let mut inside = quotes;
        inside ^= inside << 1;
        inside ^= inside << 2;
        inside ^= inside << 4;
        inside ^= inside << 8;
        inside ^= inside << 16;
        inside ^= inside << 32;
        inside ^= inside << 64;
        let bad = (inside & commas) | (quotes & inside & !starts) | (quotes & !inside & !ends);
        if bad != 0 || quotes.count_ones() & 1 != 0 {
            return Split::Line(len, None);
        }
    }
    let line = unsafe { std::slice::from_raw_parts(p, len) };
    let mut fields = [&line[..0]; N];
    // Comma positions from the two 64-bit halves (cheaper than u128 shifts).
    let mut ends = [len as u32; 8];
    let mut k = 0usize;
    let mut m = commas as u64;
    while m != 0 {
        ends[k] = m.trailing_zeros();
        m &= m - 1;
        k += 1;
    }
    let mut m = (commas >> 64) as u64;
    while m != 0 {
        ends[k] = 64 + m.trailing_zeros();
        m &= m - 1;
        k += 1;
    }
    let q_lo = quotes as u64;
    let q_hi = (quotes >> 64) as u64;
    let mut start = 0usize;
    for (i, field) in fields.iter_mut().enumerate() {
        let end = if i + 1 < N { ends[i] as usize } else { len };
        let quoted = quotes != 0
            && if start < 64 { (q_lo >> start) & 1 != 0 } else { (q_hi >> (start - 64)) & 1 != 0 };
        // SAFETY: start <= end <= len by the comma mask, and a field that opens
        // with a quote was checked above to close with one, so end - start >= 2.
        *field = unsafe {
            if quoted {
                line.get_unchecked(start + 1..end - 1)
            } else {
                line.get_unchecked(start..end)
            }
        };
        start = end + 1;
    }
    Split::Line(len, Some(fields))
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
        for_each_record::<N>(&bytes, 0, text.len(), |f| {
            out.push(f.iter().map(|x| x.to_vec()).collect())
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
                for_each_record::<2>(bytes, first, stop, |x| {
                    got.push(x.iter().map(|y| y.to_vec()).collect::<Vec<_>>())
                });
            }
            assert_eq!(got, expected, "chunk {chunk}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
