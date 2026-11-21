//! Packed ID codec (6 bits per character for up to 10 characters in `[0-9A-Z_a-z]`),
//! with an exact byte-slice fallback into a global or thread-local intern arena.

use std::cmp::Ordering;

/// Packed representation of an ID.
/// - If bit 63 is 0 (`raw & (1 << 63) == 0`), it is a 6-bit packed string of 1..=10 characters
///   from `[0-9A-Z_a-z]`. The 10 6-bit slots occupy bits 59..0 (MSB first, zero-padded on the right).
///   Because codes are in 1..=63 in strict ASCII order and 0 is the terminator/padding,
///   integer comparison `a.0.cmp(&b.0)` is identical to byte-wise lexicographical comparison!
/// - If bit 63 is 1 (`raw & (1 << 63) != 0`), the lower 63 bits hold an index or offset
///   into an exact byte store.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct PackedId(pub u64);

const FALLBACK_BIT: u64 = 1 << 63;

const ENCODE_TABLE: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut c = b'0';
    while c <= b'9' {
        t[c as usize] = (c - b'0') + 1; // 1..=10
        c += 1;
    }
    let mut c = b'A';
    while c <= b'Z' {
        t[c as usize] = (c - b'A') + 11; // 11..=36
        c += 1;
    }
    t[b'_' as usize] = 37;
    let mut c = b'a';
    while c <= b'z' {
        t[c as usize] = (c - b'a') + 38; // 38..=63
        c += 1;
    }
    t
};

const DECODE_TABLE: [u8; 64] = {
    let mut t = [0u8; 64];
    let mut i = 0;
    while i < 10 {
        t[1 + i] = b'0' + (i as u8);
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[11 + i] = b'A' + (i as u8);
        i += 1;
    }
    t[37] = b'_';
    let mut i = 0;
    while i < 26 {
        t[38 + i] = b'a' + (i as u8);
        i += 1;
    }
    t
};

/// Try to pack a non-empty byte slice of length 1..=10 into a 64-bit integer.
#[inline(always)]
pub fn try_pack(bytes: &[u8]) -> Option<PackedId> {
    let len = bytes.len();
    if len == 0 || len > 10 {
        return None;
    }
    let mut acc = 0u64;
    for (i, &b) in bytes.iter().enumerate() {
        let code = ENCODE_TABLE[b as usize];
        if code == 0 {
            return None;
        }
        acc |= (code as u64) << (54 - 6 * i);
    }
    Some(PackedId(acc))
}

/// `try_pack` for a field inside a scan buffer: the caller guarantees that 16
/// bytes are readable from `bytes.as_ptr()` (the scan buffers keep 192 zero
/// bytes after their last line). Same result as `try_pack` for every input.
#[inline(always)]
pub fn try_pack_padded(bytes: &[u8]) -> Option<PackedId> {
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.1", target_feature = "ssse3", not(feature = "scalar")))]
    {
        use std::arch::x86_64::*;
        let len = bytes.len();
        if len == 0 || len > 10 {
            return None;
        }
        // SAFETY: the caller guarantees 16 readable bytes.
        unsafe {
            let raw = _mm_loadu_si128(bytes.as_ptr() as *const __m128i);
            let iota = _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
            let keep = _mm_cmpgt_epi8(_mm_set1_epi8(len as i8), iota);
            let x = _mm_and_si128(raw, keep);
            let ge = |v: i8| _mm_cmpgt_epi8(x, _mm_set1_epi8(v - 1));
            let le = |v: i8| _mm_cmpgt_epi8(_mm_set1_epi8(v + 1), x);
            let dig = _mm_and_si128(ge(b'0' as i8), le(b'9' as i8));
            let up = _mm_and_si128(ge(b'A' as i8), le(b'Z' as i8));
            let low = _mm_and_si128(ge(b'a' as i8), le(b'z' as i8));
            let us = _mm_cmpeq_epi8(x, _mm_set1_epi8(b'_' as i8));
            let any = _mm_or_si128(_mm_or_si128(dig, up), _mm_or_si128(low, us));
            let need = (1u32 << len) - 1;
            if (_mm_movemask_epi8(any) as u32) & need != need {
                return None;
            }
            let code = _mm_or_si128(
                _mm_or_si128(
                    _mm_and_si128(dig, _mm_sub_epi8(x, _mm_set1_epi8(47))),
                    _mm_and_si128(up, _mm_sub_epi8(x, _mm_set1_epi8(54))),
                ),
                _mm_or_si128(
                    _mm_and_si128(low, _mm_sub_epi8(x, _mm_set1_epi8(59))),
                    _mm_and_si128(us, _mm_set1_epi8(37)),
                ),
            );
            // Pairs -> 12 bits, quads -> 24 bits, first character most significant.
            let pairs = _mm_maddubs_epi16(code, _mm_set1_epi16(0x0140));
            let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_1000));
            let q0 = _mm_cvtsi128_si32(quads) as u32 as u64;
            let q1 = _mm_extract_epi32::<1>(quads) as u32 as u64;
            let q2 = _mm_extract_epi32::<2>(quads) as u32 as u64;
            return Some(PackedId((q0 << 36) | (q1 << 12) | (q2 >> 12)));
        }
    }
    #[allow(unreachable_code)]
    try_pack(bytes)
}

/// Unpack a packed ID (where `is_packed()` is true) into a 10-byte stack buffer, returning the valid slice.
#[inline(always)]
pub fn unpack(id: PackedId, buf: &mut [u8; 10]) -> &[u8] {
    debug_assert!(id.is_packed());
    let mut val = id.0;
    let mut len = 0;
    while len < 10 {
        let code = ((val >> 54) & 0x3F) as usize;
        if code == 0 {
            break;
        }
        buf[len] = DECODE_TABLE[code];
        val <<= 6;
        len += 1;
    }
    &buf[..len]
}

impl PackedId {
    #[inline(always)]
    pub fn is_packed(self) -> bool {
        (self.0 & FALLBACK_BIT) == 0
    }

    #[inline(always)]
    pub fn from_fallback(offset: usize) -> Self {
        assert!(
            offset < FALLBACK_BIT as usize,
            "fallback arena exceeds address space"
        );
        PackedId(FALLBACK_BIT | offset as u64)
    }

    #[inline(always)]
    pub fn fallback_span(self, arena: &[u8]) -> (usize, usize) {
        debug_assert!(!self.is_packed());
        let offset = (self.0 & !FALLBACK_BIT) as usize;
        let len = u64::from_le_bytes(arena[offset..offset + 8].try_into().unwrap()) as usize;
        (offset + 8, len)
    }

    #[inline(always)]
    pub fn hash64(self, arena: &[u8]) -> u64 {
        if self.is_packed() {
            let mut h = self.0;
            h ^= h >> 33;
            h = h.wrapping_mul(0xff51afd7ed558ccd);
            h ^= h >> 33;
            h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
            h ^= h >> 33;
            h
        } else {
            let (off, len) = self.fallback_span(arena);
            fxhash_bytes(&arena[off..off + len])
        }
    }

    #[inline(always)]
    pub fn eq_exact(self, other: Self, arena_self: &[u8], arena_other: &[u8]) -> bool {
        if self.is_packed() && other.is_packed() {
            self.0 == other.0
        } else if !self.is_packed() && !other.is_packed() {
            let (o1, l1) = self.fallback_span(arena_self);
            let (o2, l2) = other.fallback_span(arena_other);
            l1 == l2 && arena_self[o1..o1 + l1] == arena_other[o2..o2 + l2]
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn cmp_exact(self, other: Self, arena_self: &[u8], arena_other: &[u8]) -> Ordering {
        if self.is_packed() && other.is_packed() {
            self.0.cmp(&other.0)
        } else {
            let mut b1 = [0u8; 10];
            let mut b2 = [0u8; 10];
            let s1 = if self.is_packed() {
                unpack(self, &mut b1)
            } else {
                let (o, l) = self.fallback_span(arena_self);
                &arena_self[o..o + l]
            };
            let s2 = if other.is_packed() {
                unpack(other, &mut b2)
            } else {
                let (o, l) = other.fallback_span(arena_other);
                &arena_other[o..o + l]
            };
            s1.cmp(s2)
        }
    }

    #[inline(always)]
    pub fn as_bytes<'a>(self, arena: &'a [u8], buf: &'a mut [u8; 10]) -> &'a [u8] {
        if self.is_packed() {
            unpack(self, buf)
        } else {
            let (o, l) = self.fallback_span(arena);
            &arena[o..o + l]
        }
    }
}

#[inline(always)]
pub fn fxhash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    let mut i = 0;
    while i + 8 <= bytes.len() {
        let word = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        hash = (hash ^ word).wrapping_mul(0x100000001b3);
        i += 8;
    }
    while i < bytes.len() {
        hash = (hash ^ (bytes[i] as u64)).wrapping_mul(0x100000001b3);
        i += 1;
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash
}

/// Arena for fallback IDs (> 10 chars or containing characters outside `[0-9A-Z_a-z]`).
#[derive(Default)]
pub struct FallbackArena {
    pub bytes: Vec<u8>,
}

impl FallbackArena {
    pub fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    #[inline(always)]
    pub fn pack_or_push(&mut self, slice: &[u8]) -> PackedId {
        if let Some(id) = try_pack(slice) {
            id
        } else {
            self.push_fallback(slice)
        }
    }

    /// `pack_or_push` for a field inside a scan buffer (see `try_pack_padded`).
    #[inline(always)]
    pub fn pack_or_push_padded(&mut self, slice: &[u8]) -> PackedId {
        if let Some(id) = try_pack_padded(slice) {
            id
        } else {
            self.push_fallback(slice)
        }
    }

    #[inline(always)]
    pub fn push_fallback(&mut self, slice: &[u8]) -> PackedId {
        let offset = self.bytes.len();
        self.bytes
            .extend_from_slice(&(slice.len() as u64).to_le_bytes());
        self.bytes.extend_from_slice(slice);
        PackedId::from_fallback(offset)
    }

    /// Re-home a `PackedId` from `other_arena` into `self`.
    #[inline(always)]
    pub fn import(&mut self, id: PackedId, other_arena: &[u8]) -> PackedId {
        if id.is_packed() {
            id
        } else {
            let (off, len) = id.fallback_span(other_arena);
            self.push_fallback(&other_arena[off..off + len])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn padded_pack_matches_scalar() {
        let alphabet = b"09AZ_az/:@[`{\x7f\x80\xff ";
        let mut rng = 88172645463325252u64;
        for _ in 0..200000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let len = (rng % 13) as usize;
            let mut buf = [0u8; 32];
            for (i, b) in buf.iter_mut().enumerate() {
                let r = rng.rotate_left(i as u32 * 5);
                *b = if r % 3 == 0 { alphabet[(r >> 8) as usize % alphabet.len()] } else { b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"[(r >> 8) as usize % 63] };
            }
            assert_eq!(try_pack_padded(&buf[..len]), try_pack(&buf[..len]), "{:?}", &buf[..len]);
        }
    }
    #[test]
    fn exact_equality_order_and_roundtrip_across_arenas() {
        let values: &[&[u8]] = &[
            b"",
            b"0",
            b"00",
            b"A",
            b"Z",
            b"_",
            b"a",
            b"z",
            b"0123456789",
            b"01234567890",
            b" u1",
            b"u1 ",
            b"u1\r",
            "é".as_bytes(),
        ];
        let mut a = FallbackArena::new();
        let mut b = FallbackArena::new();
        b.pack_or_push(b"offset padding");
        let aa: Vec<_> = values.iter().map(|v| a.pack_or_push(v)).collect();
        let bb: Vec<_> = values.iter().map(|v| b.pack_or_push(v)).collect();
        for (i, &x) in aa.iter().enumerate() {
            let mut scratch = [0; 10];
            assert_eq!(x.as_bytes(&a.bytes, &mut scratch), values[i]);
            let copied = b.import(x, &a.bytes);
            assert!(x.eq_exact(copied, &a.bytes, &b.bytes));
            assert_eq!(x.hash64(&a.bytes), copied.hash64(&b.bytes));
            for (j, &y) in bb.iter().enumerate() {
                assert_eq!(x.eq_exact(y, &a.bytes, &b.bytes), i == j);
                assert_eq!(x.cmp_exact(y, &a.bytes, &b.bytes), values[i].cmp(values[j]));
            }
        }
    }
}
