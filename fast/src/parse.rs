//! Exact CSV line scanning and timestamp/price parsers according to `rules.md`.
//! Includes runtime CPU feature detection (`avx2` on x86_64, `neon` on aarch64) with a scalar fallback.

pub const MS_PER_SECOND: i64 = 1_000;
pub const MS_PER_MINUTE: i64 = 60_000;
pub const MS_PER_DAY: i64 = 86_400_000;
pub const SESSION_GAP_MS: i64 = 1_800_000;
pub const LATE_LIMIT_MS: i64 = 86_400_000;
pub const CLICK_LOOKBACK_MS: i64 = 604_800_000;

pub fn scanner_name() -> &'static str {
    #[cfg(feature = "scalar")]
    return "scalar";
    #[cfg(all(target_arch = "aarch64", not(feature = "scalar")))]
    return "neon";
    #[cfg(all(target_arch = "x86_64", not(feature = "scalar")))]
    if is_x86_feature_detected!("avx2") {
        return "avx2";
    }
    #[allow(unreachable_code)]
    "scalar"
}

/// Find the next `\n` in `buf`, using runtime-dispatched SIMD (AVX2 on x86_64, NEON on aarch64)
/// or a fast 64-bit word scan fallback.
#[inline(always)]
pub fn find_newline(buf: &[u8]) -> Option<usize> {
    #[cfg(all(target_arch = "x86_64", not(feature = "scalar")))]
    {
        if is_x86_feature_detected!("avx2") {
            // SAFETY: avx2 is verified present at runtime.
            return unsafe { find_newline_avx2(buf) };
        }
    }
    #[cfg(all(target_arch = "aarch64", not(feature = "scalar")))]
    {
        // NEON is baseline on aarch64
        return unsafe { find_newline_neon(buf) };
    }
    #[allow(unreachable_code)]
    find_newline_scalar(buf)
}

#[inline(always)]
pub fn find_newline_scalar(buf: &[u8]) -> Option<usize> {
    let mut i = 0;
    let len = buf.len();
    while i + 8 <= len {
        let word = u64::from_le_bytes(buf[i..i + 8].try_into().unwrap());
        let x = word ^ 0x0A0A_0A0A_0A0A_0A0A;
        let has_nl = (x.wrapping_sub(0x0101_0101_0101_0101)) & !x & 0x8080_8080_8080_8080;
        if has_nl != 0 {
            return Some(i + (has_nl.trailing_zeros() as usize >> 3));
        }
        i += 8;
    }
    while i < len {
        if buf[i] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

#[cfg(all(target_arch = "x86_64", not(feature = "scalar")))]
#[target_feature(enable = "avx2")]
unsafe fn find_newline_avx2(buf: &[u8]) -> Option<usize> {
    use std::arch::x86_64::*;
    let len = buf.len();
    let mut i = 0;
    let nl = _mm256_set1_epi8(b'\n' as i8);
    while i + 32 <= len {
        let chunk = unsafe { _mm256_loadu_si256(buf.as_ptr().add(i) as *const __m256i) };
        let cmp = _mm256_cmpeq_epi8(chunk, nl);
        let mask = _mm256_movemask_epi8(cmp) as u32;
        if mask != 0 {
            return Some(i + mask.trailing_zeros() as usize);
        }
        i += 32;
    }
    find_newline_scalar(&buf[i..]).map(|off| i + off)
}

#[cfg(all(target_arch = "aarch64", not(feature = "scalar")))]
#[target_feature(enable = "neon")]
unsafe fn find_newline_neon(buf: &[u8]) -> Option<usize> {
    use std::arch::aarch64::*;
    let len = buf.len();
    let mut i = 0;
    let nl = vdupq_n_u8(b'\n');
    while i + 16 <= len {
        let chunk = unsafe { vld1q_u8(buf.as_ptr().add(i)) };
        let cmp = vceqq_u8(chunk, nl);
        // Extract two u64 lanes
        let lo = vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 0);
        if lo != 0 {
            return Some(i + (lo.trailing_zeros() as usize >> 3));
        }
        let hi = vgetq_lane_u64(vreinterpretq_u64_u8(cmp), 1);
        if hi != 0 {
            return Some(i + 8 + (hi.trailing_zeros() as usize >> 3));
        }
        i += 16;
    }
    find_newline_scalar(&buf[i..]).map(|off| i + off)
}

// Mask commas and quotes together, so ordinary field bytes need no second scan.
#[cfg(all(target_arch = "x86_64", not(feature = "scalar")))]
#[target_feature(enable = "avx2")]
unsafe fn special_mask(bytes: &[u8]) -> u64 {
    use std::arch::x86_64::*;
    let v = unsafe { _mm256_loadu_si256(bytes.as_ptr() as *const __m256i) };
    let comma = _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b',' as i8));
    let quote = _mm256_cmpeq_epi8(v, _mm256_set1_epi8(b'"' as i8));
    _mm256_movemask_epi8(_mm256_or_si256(comma, quote)) as u32 as u64
}
#[cfg(all(target_arch = "aarch64", not(feature = "scalar")))]
unsafe fn special_mask(bytes: &[u8]) -> u64 {
    use std::arch::aarch64::*;
    unsafe {
        let v = vld1q_u8(bytes.as_ptr());
        let both = vorrq_u8(vceqq_u8(v, vdupq_n_u8(b',')), vceqq_u8(v, vdupq_n_u8(b'"')));
        // Four bits per source byte, in source order on little-endian aarch64.
        vget_lane_u64(
            vreinterpret_u64_u8(vshrn_n_u16::<4>(vreinterpretq_u16_u8(both))),
            0,
        )
    }
}

#[inline(always)]
pub fn parse_csv_line<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(feature = "scalar")]
    return parse_csv_line_scalar(line);
    #[cfg(all(not(feature = "scalar"), target_arch = "x86_64"))]
    if !is_x86_feature_detected!("avx2") {
        return parse_csv_line_scalar(line);
    }
    #[cfg(all(
        not(feature = "scalar"),
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    {
        if line.is_empty() {
            return None;
        }
        #[cfg(target_arch = "aarch64")]
        const BLOCK: usize = 16;
        #[cfg(target_arch = "aarch64")]
        const SHIFT: usize = 2;
        #[cfg(target_arch = "x86_64")]
        const BLOCK: usize = 32;
        #[cfg(target_arch = "x86_64")]
        const SHIFT: usize = 0;
        let mut fields = [&[][..]; N];
        let (mut start, mut col, mut quotes) = (0, 0, 0);
        let mut pos = 0;
        while pos < line.len() {
            let width = BLOCK.min(line.len() - pos);
            let mut mask = if width == BLOCK {
                unsafe { special_mask(&line[pos..]) }
            } else {
                let mut m = 0u64;
                for i in 0..width {
                    if line[pos + i] == b',' || line[pos + i] == b'"' {
                        m |= 1u64 << (i << SHIFT);
                    }
                }
                m
            };
            while mask != 0 {
                let index = mask.trailing_zeros() as usize >> SHIFT;
                mask &= !(((1u64 << (1 << SHIFT)) - 1) << (index << SHIFT));
                let i = pos + index;
                if line[i] == b'"' {
                    if (quotes == 0 && i == start)
                        || (quotes == 1 && (i + 1 == line.len() || line[i + 1] == b','))
                    {
                        quotes += 1;
                    } else {
                        return None;
                    }
                } else {
                    if col + 1 >= N || quotes == 1 {
                        return None;
                    }
                    fields[col] = if quotes == 2 {
                        &line[start + 1..i - 1]
                    } else {
                        &line[start..i]
                    };
                    col += 1;
                    start = i + 1;
                    quotes = 0;
                }
            }
            pos += width;
        }
        if col + 1 != N || quotes == 1 {
            return None;
        }
        fields[col] = if quotes == 2 {
            &line[start + 1..line.len() - 1]
        } else {
            &line[start..]
        };
        Some(fields)
    }
    #[cfg(all(
        not(feature = "scalar"),
        not(any(target_arch = "x86_64", target_arch = "aarch64"))
    ))]
    parse_csv_line_scalar(line)
}

/// Parse a CSV line into exact unquoted field slices according to `rules.md`:
/// - `line` must not be empty (`b""` is malformed).
/// - Must have exactly `N` comma-separated fields.
/// - Any field may be wrapped in double quotes `"..."`, and the quotes are stripped.
/// - A quote anywhere except around a whole field makes the entire line malformed (`None`).
#[inline(always)]
#[allow(dead_code)] // Also retained as the differential oracle in tests.
pub fn parse_csv_line_scalar<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut fields = [&[][..]; N];
    let mut col = 0usize;
    let mut start = 0usize;
    let len = line.len();
    let mut i = 0usize;

    // Fast scan for commas and quotes
    while i < len {
        let b = line[i];
        if b == b',' {
            if col + 1 >= N {
                return None; // too many fields
            }
            let raw = &line[start..i];
            fields[col] = unquote_field(raw)?;
            col += 1;
            start = i + 1;
        }
        i += 1;
    }
    if col + 1 != N {
        return None; // wrong number of fields
    }
    fields[col] = unquote_field(&line[start..len])?;
    Some(fields)
}

#[inline(always)]
#[allow(dead_code)]
fn unquote_field(raw: &[u8]) -> Option<&[u8]> {
    let len = raw.len();
    if len == 0 {
        return Some(raw);
    }
    let first_q = raw[0] == b'"';
    let last_q = raw[len - 1] == b'"';
    if !first_q && !last_q {
        // Ensure no interior quote
        for &b in raw {
            if b == b'"' {
                return None;
            }
        }
        Some(raw)
    } else if first_q && last_q && len >= 2 {
        let inner = &raw[1..len - 1];
        for &b in inner {
            if b == b'"' {
                return None;
            }
        }
        Some(inner)
    } else {
        None
    }
}

#[inline(always)]
pub fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

#[inline(always)]
pub fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Howard Hinnant's `days_from_civil` (days from 1970-01-01).
#[inline(always)]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline(always)]
fn parse_2digits(b0: u8, b1: u8) -> Option<i64> {
    let d0 = b0.wrapping_sub(b'0');
    let d1 = b1.wrapping_sub(b'0');
    if d0 <= 9 && d1 <= 9 {
        Some((d0 as i64) * 10 + (d1 as i64))
    } else {
        None
    }
}

#[inline(always)]
fn parse_4digits(b: &[u8]) -> Option<i64> {
    let d0 = b[0].wrapping_sub(b'0');
    let d1 = b[1].wrapping_sub(b'0');
    let d2 = b[2].wrapping_sub(b'0');
    let d3 = b[3].wrapping_sub(b'0');
    if d0 <= 9 && d1 <= 9 && d2 <= 9 && d3 <= 9 {
        Some((d0 as i64) * 1000 + (d1 as i64) * 100 + (d2 as i64) * 10 + (d3 as i64))
    } else {
        None
    }
}

/// Parse `YYYY-MM-DDTHH:MM:SS` (19 bytes) into milliseconds since Unix epoch.
#[inline(always)]
pub fn parse_calendar_part(text: &[u8]) -> Option<i64> {
    if text.len() != 19 {
        return None;
    }
    if text[4] != b'-'
        || text[7] != b'-'
        || text[10] != b'T'
        || text[13] != b':'
        || text[16] != b':'
    {
        return None;
    }
    let year = parse_4digits(&text[0..4])?;
    let month = parse_2digits(text[5], text[6])?;
    let day = parse_2digits(text[8], text[9])?;
    let hour = parse_2digits(text[11], text[12])?;
    let minute = parse_2digits(text[14], text[15])?;
    let second = parse_2digits(text[17], text[18])?;

    if !(1..=12).contains(&month) {
        return None;
    }
    if day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(((days * 24 + hour) * 60 + minute) * MS_PER_MINUTE + second * MS_PER_SECOND)
}

/// Validated `YYYY-MM-DD` dates seen by one scanning thread, direct-mapped by
/// day of month (any window of up to 31 days fits without conflicts).
pub struct DateCache {
    key: [u64; 32],
    key2: [u16; 32],
    days: [i64; 32],
}

impl Default for DateCache {
    fn default() -> Self {
        // Key 0 never matches: a real date's first 8 bytes are digits and dashes.
        Self { key: [0; 32], key2: [0; 32], days: [0; 32] }
    }
}

/// `parse_calendar_part` with a cache of validated dates.
#[inline(always)]
pub fn parse_calendar_cached(text: &[u8], cache: &mut DateCache) -> Option<i64> {
    if text.len() != 19 || text[10] != b'T' || text[13] != b':' || text[16] != b':' {
        return None;
    }
    let key = u64::from_le_bytes(text[0..8].try_into().unwrap());
    let key2 = u16::from_le_bytes([text[8], text[9]]);
    let slot = (text[8].wrapping_mul(10).wrapping_add(text[9]) & 31) as usize;
    let days = if cache.key[slot] == key && cache.key2[slot] == key2 {
        cache.days[slot]
    } else {
        if text[4] != b'-' || text[7] != b'-' {
            return None;
        }
        let year = parse_4digits(&text[0..4])?;
        let month = parse_2digits(text[5], text[6])?;
        let day = parse_2digits(text[8], text[9])?;
        if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
            return None;
        }
        let days = days_from_civil(year, month, day);
        cache.key[slot] = key;
        cache.key2[slot] = key2;
        cache.days[slot] = days;
        days
    };
    let hour = parse_2digits(text[11], text[12])?;
    let minute = parse_2digits(text[14], text[15])?;
    let second = parse_2digits(text[17], text[18])?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(((days * 24 + hour) * 60 + minute) * MS_PER_MINUTE + second * MS_PER_SECOND)
}

/// `parse_f1` with a date cache.
#[inline(always)]
pub fn parse_f1_cached(text: &[u8], cache: &mut DateCache) -> Option<i64> {
    if text.len() != 20 || text[19] != b'Z' {
        return None;
    }
    parse_calendar_cached(&text[..19], cache)
}

/// F3 for a scan-buffer field (16 readable bytes from `text.as_ptr()`): up to
/// 16 digits in SIMD, anything longer by `parse_f3`. Same result as `parse_f3`.
#[inline(always)]
pub fn parse_f3_padded(text: &[u8]) -> Option<i64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1", not(feature = "scalar")))]
    {
        use std::arch::x86_64::*;
        let n = text.len();
        if (1..=16).contains(&n) {
            const fn ctrl(n: usize) -> [i8; 16] {
                let mut c = [-1i8; 16];
                let mut j = 16 - n;
                while j < 16 {
                    c[j] = (j - (16 - n)) as i8;
                    j += 1;
                }
                c
            }
            const CTRL: [[i8; 16]; 17] = [
                ctrl(0), ctrl(1), ctrl(2), ctrl(3), ctrl(4), ctrl(5), ctrl(6), ctrl(7), ctrl(8),
                ctrl(9), ctrl(10), ctrl(11), ctrl(12), ctrl(13), ctrl(14), ctrl(15), ctrl(16),
            ];
            // SAFETY: the caller guarantees 16 readable bytes.
            unsafe {
                let x = _mm_loadu_si128(text.as_ptr() as *const __m128i);
                let c = _mm_loadu_si128(CTRL[n].as_ptr() as *const __m128i);
                let v = _mm_shuffle_epi8(x, c);
                let used = _mm_cmpgt_epi8(c, _mm_set1_epi8(-1));
                let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
                // A digit is 0..=9 after subtracting '0' (unsigned compare via min).
                let is_digit = _mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d);
                if _mm_movemask_epi8(_mm_andnot_si128(is_digit, used)) != 0 {
                    return None;
                }
                let d = _mm_and_si128(d, used);
                let pairs = _mm_maddubs_epi16(d, _mm_set1_epi16(0x010A)); // d0*10 + d1
                let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_0064)); // p0*100 + p1
                let packed = _mm_packus_epi32(quads, quads);
                let octs = _mm_madd_epi16(packed, _mm_set1_epi32(0x0001_2710)); // q0*10000 + q1
                let hi = _mm_cvtsi128_si32(octs) as u32 as i64;
                let lo = _mm_extract_epi32::<1>(octs) as u32 as i64;
                return Some(hi * 100_000_000 + lo);
            }
        }
    }
    #[allow(unreachable_code)]
    parse_f3(text)
}

/// `parse_ts` with a date cache, for a scan-buffer field (16 readable bytes
/// from `text.as_ptr()`).
#[inline(always)]
pub fn parse_ts_cached(text: &[u8], cache: &mut DateCache) -> Option<i64> {
    match text.len() {
        20 if text[19] == b'Z' => parse_calendar_cached(&text[..19], cache),
        25 if (text[19] == b'+' || text[19] == b'-') => {
            if text[22] != b':' {
                return None;
            }
            let local = parse_calendar_cached(&text[..19], cache)?;
            let off_h = parse_2digits(text[20], text[21])?;
            let off_m = parse_2digits(text[23], text[24])?;
            if off_h > 23 || off_m > 59 {
                return None;
            }
            let offset_ms = (off_h * 60 + off_m) * MS_PER_MINUTE;
            Some(if text[19] == b'+' { local - offset_ms } else { local + offset_ms })
        }
        _ => parse_f3_padded(text),
    }
}

#[inline(always)]
pub fn parse_f1(text: &[u8]) -> Option<i64> {
    if text.len() != 20 || text[19] != b'Z' {
        return None;
    }
    parse_calendar_part(&text[..19])
}

#[inline(always)]
pub fn parse_f2(text: &[u8]) -> Option<i64> {
    if text.len() != 25 || text[22] != b':' {
        return None;
    }
    let sign = text[19];
    if sign != b'+' && sign != b'-' {
        return None;
    }
    let local = parse_calendar_part(&text[..19])?;
    let off_h = parse_2digits(text[20], text[21])?;
    let off_m = parse_2digits(text[23], text[24])?;
    if off_h > 23 || off_m > 59 {
        return None;
    }
    let offset_ms = (off_h * 60 + off_m) * MS_PER_MINUTE;
    if sign == b'+' {
        Some(local - offset_ms)
    } else {
        Some(local + offset_ms)
    }
}

/// F3: Digits only, epoch milliseconds. Leading zeros allowed.
/// "An F3 value is just a number, so one that is very short (`86400000` is 1970-01-02)
/// or too long for a 64-bit integer is not malformed, only outside the window."
/// We saturate at `i64::MAX` on overflow so it cleanly fails the window check!
#[inline(always)]
pub fn parse_f3(text: &[u8]) -> Option<i64> {
    if text.is_empty() {
        return None;
    }
    let mut acc: u64 = 0;
    let mut overflow = false;
    for &b in text {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if !overflow {
            match acc.checked_mul(10).and_then(|v| v.checked_add(d as u64)) {
                Some(v) if v <= i64::MAX as u64 => acc = v,
                _ => overflow = true,
            }
        }
    }
    if overflow {
        Some(i64::MAX)
    } else {
        Some(acc as i64)
    }
}

#[inline(always)]
pub fn parse_ts(text: &[u8]) -> Option<i64> {
    match text.len() {
        20 if text[19] == b'Z' => parse_f1(text),
        25 if (text[19] == b'+' || text[19] == b'-') => parse_f2(text),
        _ => parse_f3(text),
    }
}

/// Purchase price: digits only, value in `1..=10_000_000` (`0250` is 250).
#[inline(always)]
pub fn parse_purchase_price(text: &[u8]) -> Option<u32> {
    if text.is_empty() {
        return None;
    }
    let mut acc: u32 = 0;
    for &b in text {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        acc = acc.saturating_mul(10).saturating_add(d as u32);
    }
    if (1..=10_000_000).contains(&acc) {
        Some(acc)
    } else {
        None
    }
}

/// Decode JSON strings in the small two-string manifest, including escaped keys
/// and timestamps. The manifest shape is promised by the input specification.
pub fn json_strings(text: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut strings = Vec::new();
    let mut i = 0;
    while i < text.len() {
        if text[i] != b'"' {
            i += 1;
            continue;
        }
        i += 1;
        let mut value = Vec::new();
        loop {
            let byte = *text.get(i)?;
            i += 1;
            if byte == b'"' {
                break;
            }
            if byte == b'\\' {
                let escape = *text.get(i)?;
                i += 1;
                match escape {
                    b'"' | b'\\' | b'/' => value.push(escape),
                    b'b' => value.push(8),
                    b'f' => value.push(12),
                    b'n' => value.push(10),
                    b'r' => value.push(13),
                    b't' => value.push(9),
                    b'u' => {
                        let hex = std::str::from_utf8(text.get(i..i + 4)?).ok()?;
                        let n = u32::from_str_radix(hex, 16).ok()?;
                        let c = char::from_u32(n)?;
                        let mut buf = [0; 4];
                        value.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                        i += 4;
                    }
                    _ => return None,
                }
            } else if byte < 32 {
                return None;
            } else {
                value.push(byte);
            }
        }
        strings.push(value);
    }
    Some(strings)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn masked_csv_equals_scalar_for_malformed_and_long_lines() {
        let alphabet = b"a,\"\r ";
        for n in 0..=7u32 {
            for mut code in 0..5usize.pow(n) {
                let mut line = vec![0; n as usize];
                for c in &mut line {
                    *c = alphabet[code % 5];
                    code /= 5;
                }
                assert_eq!(
                    parse_csv_line::<2>(&line),
                    parse_csv_line_scalar::<2>(&line),
                    "{:?}",
                    line
                );
                assert_eq!(
                    parse_csv_line::<4>(&line),
                    parse_csv_line_scalar::<4>(&line),
                    "{:?}",
                    line
                );
            }
        }
        for len in 0..100 {
            let base = vec![b'a'; len];
            for bad in [b"".as_slice(), b"\"", b",", b"\"\"", b"\r"] {
                let mut line = base.clone();
                line.extend_from_slice(bad);
                line.extend_from_slice(b",\"abc\",u,\"\"");
                assert_eq!(
                    parse_csv_line::<4>(&line),
                    parse_csv_line_scalar::<4>(&line)
                );
            }
        }
    }
    #[test]
    fn padded_f3_matches_scalar() {
        let mut rng = 0xD1B54A32D192ED03u64;
        let alphabet = b"0123456789/:.-+e ";
        for _ in 0..300000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let len = (rng % 22) as usize;
            let mut buf = [b'5'; 48];
            for (i, b) in buf.iter_mut().enumerate().take(len) {
                let r = rng.rotate_left(i as u32 * 3) >> 4;
                *b = if r % 11 == 0 { alphabet[(r >> 4) as usize % alphabet.len()] } else { b'0' + (r % 10) as u8 };
            }
            assert_eq!(parse_f3_padded(&buf[..len]), parse_f3(&buf[..len]), "{:?}", &buf[..len]);
        }
        let mut buf = *b"9999999999999999xxxxxxxxxxxxxxxx";
        assert_eq!(parse_f3_padded(&buf[..16]), Some(9_999_999_999_999_999));
        buf[0] = b'0';
        assert_eq!(parse_f3_padded(&buf[..1]), Some(0));
    }
    #[test]
    fn cached_timestamps_match_uncached() {
        let mut cache = DateCache::default();
        let mut rng = 0x9E3779B97F4A7C15u64;
        let pieces: [&[u8]; 12] = [b"2026", b"2028", b"0000", b"-02-", b"-13-", b"-12-", b"29", b"30", b"31", b"T23:59:59", b"T24:00:00", b"T00:00:60"];
        for _ in 0..200000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let mut t = Vec::new();
            t.extend_from_slice(pieces[(rng % 3) as usize]);
            t.extend_from_slice(pieces[3 + ((rng >> 8) % 3) as usize]);
            t.extend_from_slice(pieces[6 + ((rng >> 16) % 3) as usize]);
            t.extend_from_slice(pieces[9 + ((rng >> 24) % 3) as usize]);
            match (rng >> 32) % 4 {
                0 => t.push(b'Z'),
                1 => t.extend_from_slice(b"+05:30"),
                2 => t.extend_from_slice(b"-23:59"),
                _ => t.extend_from_slice(b"+24:00"),
            }
            if (rng >> 40) % 9 == 0 {
                t = (rng % 100000000000000).to_string().into_bytes();
            }
            let mut padded = t.clone();
            padded.resize(t.len() + 16, b'7');
            assert_eq!(parse_ts_cached(&padded[..t.len()], &mut cache), parse_ts(&t), "{:?}", String::from_utf8_lossy(&t));
            if t.len() == 20 {
                assert_eq!(parse_f1_cached(&t, &mut cache), parse_f1(&t));
            }
        }
    }
    #[test]
    fn exact_timestamp_and_price_edges() {
        assert_eq!(parse_ts(b"0000-02-29T00:00:00Z"), Some(-62162121600000));
        assert_eq!(
            parse_ts(b"2028-02-29T00:00:00Z"),
            parse_ts(b"2028-03-01T01:00:00+01:00").map(|t| t - MS_PER_DAY)
        );
        for bad in [
            b"2027-02-29T00:00:00Z".as_slice(),
            b"2028-02-30T00:00:00Z",
            b"2026-01-01T24:00:00Z",
            b"2026-01-01T00:00:60Z",
            b"2026-01-01T00:00:00+24:00",
            b"2026-01-01T00:00:00+05:60",
            b"1.0",
            b"-5",
            b"\xef\xbc\x91",
        ] {
            assert!(parse_ts(bad).is_none(), "{:?}", bad);
        }
        assert_eq!(
            parse_ts(b"000000000000000000000086400000"),
            Some(MS_PER_DAY)
        );
        assert!(parse_ts(b"1844674407370955161618446744073709551616").unwrap() > 253402300799000);
        assert_eq!(parse_purchase_price(b"00010000000"), Some(10_000_000));
        for bad in [
            b"0".as_slice(),
            b"10000001",
            b"1e3",
            b"+1",
            b"250\r",
            b"999999999999999999999999",
        ] {
            assert_eq!(parse_purchase_price(bad), None);
        }
    }
    #[test]
    fn ignored_fields_still_require_valid_quotes() {
        assert!(parse_csv_line::<7>(b"e,u,1,2,view,p,ignored\"").is_none());
        assert!(parse_csv_line::<2>(b"a,\"b\"\"").is_none());
        assert_eq!(
            parse_csv_line::<2>(b"\" u\",\"\""),
            Some([b" u".as_slice(), b"".as_slice()])
        );
        assert_eq!(
            parse_csv_line::<2>(b"a,b\r"),
            Some([b"a".as_slice(), b"b\r".as_slice()])
        );
    }
    #[test]
    fn newline_dispatch_matches_scalar_at_every_alignment() {
        for n in 0..130 {
            for pos in 0..n {
                let mut s = vec![b'x'; n];
                s[pos] = b'\n';
                assert_eq!(find_newline(&s), Some(pos));
                assert_eq!(find_newline_scalar(&s), Some(pos));
            }
            assert_eq!(find_newline(&vec![b'x'; n]), None);
        }
    }
}
