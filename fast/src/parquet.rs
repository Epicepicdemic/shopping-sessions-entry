//! Hand-written zero-dependency Parquet writer for `user_daily` and `campaign_daily`.
//! Writes plain uncompressed Parquet v1 with `DATE` logical type for `day` and `STRING` (UTF-8)
//! for string columns.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

fn write_varint(out: &mut Vec<u8>, mut n: u64) {
    while (n & !0x7F) != 0 {
        out.push(((n & 0x7F) as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

fn write_zigzag_i32(out: &mut Vec<u8>, n: i32) {
    let zz = ((n << 1) ^ (n >> 31)) as u32 as u64;
    write_varint(out, zz);
}

fn write_zigzag_i64(out: &mut Vec<u8>, n: i64) {
    let zz = ((n << 1) ^ (n >> 63)) as u64;
    write_varint(out, zz);
}

fn write_field_hdr(out: &mut Vec<u8>, prev: &mut i16, fid: i16, ftype: u8) {
    let delta = fid - *prev;
    if delta > 0 && delta <= 15 {
        out.push(((delta as u8) << 4) | ftype);
    } else {
        out.push(ftype);
        write_zigzag_i32(out, fid as i32);
    }
    *prev = fid;
}

fn write_i32(out: &mut Vec<u8>, prev: &mut i16, fid: i16, val: i32) {
    write_field_hdr(out, prev, fid, 5);
    write_zigzag_i32(out, val);
}

fn write_i64(out: &mut Vec<u8>, prev: &mut i16, fid: i16, val: i64) {
    write_field_hdr(out, prev, fid, 6);
    write_zigzag_i64(out, val);
}

fn write_bytes(out: &mut Vec<u8>, prev: &mut i16, fid: i16, s: &[u8]) {
    write_field_hdr(out, prev, fid, 8);
    write_varint(out, s.len() as u64);
    out.extend_from_slice(s);
}

fn write_list_hdr(out: &mut Vec<u8>, prev: &mut i16, fid: i16, etype: u8, count: usize) {
    write_field_hdr(out, prev, fid, 9);
    if count < 15 {
        out.push(((count as u8) << 4) | etype);
    } else {
        out.push(0xF0 | etype);
        write_varint(out, count as u64);
    }
}

fn write_data_page(out: &mut Vec<u8>, num_values: i32, raw_bytes: &[u8]) {
    let len = raw_bytes.len() as i32;
    let mut prev = 0i16;
    write_i32(out, &mut prev, 1, 0); // PageType::DATA_PAGE = 0
    write_i32(out, &mut prev, 2, len);
    write_i32(out, &mut prev, 3, len);
    write_field_hdr(out, &mut prev, 5, 12); // DataPageHeader struct
    let mut dp = 0i16;
    write_i32(out, &mut dp, 1, num_values);
    write_i32(out, &mut dp, 2, 0); // Encoding::PLAIN = 0
    write_i32(out, &mut dp, 3, 3); // def_level_encoding: RLE = 3
    write_i32(out, &mut dp, 4, 3); // rep_level_encoding: RLE = 3
    out.push(0); // end DataPageHeader
    out.push(0); // end PageHeader
    out.extend_from_slice(raw_bytes);
}

fn write_schema_elem(
    out: &mut Vec<u8>,
    name: &[u8],
    ptype: Option<i32>,
    rep: Option<i32>,
    ctype: Option<i32>,
    ltype_kind: Option<i16>,
    num_children: Option<i32>,
) {
    let mut prev = 0i16;
    if let Some(t) = ptype {
        write_i32(out, &mut prev, 1, t);
    }
    if let Some(r) = rep {
        write_i32(out, &mut prev, 3, r);
    }
    write_bytes(out, &mut prev, 4, name);
    if let Some(nc) = num_children {
        write_i32(out, &mut prev, 5, nc);
    }
    if let Some(c) = ctype {
        write_i32(out, &mut prev, 6, c);
    }
    if let Some(lk) = ltype_kind {
        write_field_hdr(out, &mut prev, 10, 12); // logicalType union
        let mut lp = 0i16;
        write_field_hdr(out, &mut lp, lk, 12); // empty struct inside union
        out.push(0);
        out.push(0);
    }
    out.push(0); // end SchemaElement
}

fn write_col_chunk(
    out: &mut Vec<u8>,
    ptype: i32,
    col_name: &[u8],
    num_values: i64,
    total_size: i64,
    data_offset: i64,
) {
    let mut prev = 0i16;
    write_i64(out, &mut prev, 2, 0); // file_offset = 0
    write_field_hdr(out, &mut prev, 3, 12); // meta_data: ColumnMetaData
    let mut mp = 0i16;
    write_i32(out, &mut mp, 1, ptype);
    write_list_hdr(out, &mut mp, 2, 5, 2); // encodings: [PLAIN=0, RLE=3]
    write_zigzag_i32(out, 0);
    write_zigzag_i32(out, 3);
    write_list_hdr(out, &mut mp, 3, 8, 1); // path_in_schema: [col_name]
    write_varint(out, col_name.len() as u64);
    out.extend_from_slice(col_name);
    write_i32(out, &mut mp, 4, 0); // codec = UNCOMPRESSED (0)
    write_i64(out, &mut mp, 5, num_values);
    write_i64(out, &mut mp, 6, total_size);
    write_i64(out, &mut mp, 7, total_size);
    write_i64(out, &mut mp, 9, data_offset);
    out.push(0); // end ColumnMetaData
    out.push(0); // end ColumnChunk
}

pub struct UserDailyRow<'a> {
    pub user_id: &'a [u8],
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue_cents: i64,
    pub top_category: Option<&'a [u8]>,
}

pub struct CampaignDailyRow<'a> {
    pub campaign_id: &'a [u8],
    pub channel: &'a [u8],
    pub day: i32,
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue_cents: i64,
    pub assisted_sessions: i64,
}

pub fn write_user_daily_file(path: &Path, rows: &[UserDailyRow<'_>]) -> io::Result<()> {
    let n = rows.len();
    let mut buf = Vec::with_capacity(n * 48 + 1024);
    buf.extend_from_slice(b"PAR1");

    let mut chunks = Vec::new();

    if n > 0 {
        let mut raw = Vec::with_capacity(n * 16);
        let mut page = Vec::new();

        // 1. user_id (BYTE_ARRAY = 6)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&(r.user_id.len() as u32).to_le_bytes());
            raw.extend_from_slice(r.user_id);
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off1 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c1 = Vec::new();
        write_col_chunk(&mut c1, 6, b"user_id", n as i64, page.len() as i64, off1);
        chunks.push(c1);

        // 2. day (INT32 = 1)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&r.day.to_le_bytes());
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off2 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c2 = Vec::new();
        write_col_chunk(&mut c2, 1, b"day", n as i64, page.len() as i64, off2);
        chunks.push(c2);

        // 3. sessions (INT64 = 2)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&r.sessions.to_le_bytes());
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off3 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c3 = Vec::new();
        write_col_chunk(&mut c3, 2, b"sessions", n as i64, page.len() as i64, off3);
        chunks.push(c3);

        // 4. events (INT64 = 2)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&r.events.to_le_bytes());
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off4 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c4 = Vec::new();
        write_col_chunk(&mut c4, 2, b"events", n as i64, page.len() as i64, off4);
        chunks.push(c4);

        // 5. revenue_cents (INT64 = 2)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&r.revenue_cents.to_le_bytes());
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off5 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c5 = Vec::new();
        write_col_chunk(
            &mut c5,
            2,
            b"revenue_cents",
            n as i64,
            page.len() as i64,
            off5,
        );
        chunks.push(c5);

        // 6. top_category (OPTIONAL BYTE_ARRAY = 6)
        raw.clear();
        // RLE bit-packed definition levels (bit-width 1):
        let num_groups = (n + 7) / 8;
        let mut rle_payload = Vec::with_capacity(num_groups + 8);
        write_varint(&mut rle_payload, ((num_groups as u64) << 1) | 1);
        for g in 0..num_groups {
            let mut b = 0u8;
            for bit in 0..8 {
                let idx = g * 8 + bit;
                if idx < n && rows[idx].top_category.is_some() {
                    b |= 1 << bit;
                }
            }
            rle_payload.push(b);
        }
        raw.extend_from_slice(&(rle_payload.len() as u32).to_le_bytes());
        raw.extend_from_slice(&rle_payload);
        for r in rows {
            if let Some(cat) = r.top_category {
                raw.extend_from_slice(&(cat.len() as u32).to_le_bytes());
                raw.extend_from_slice(cat);
            }
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off6 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c6 = Vec::new();
        write_col_chunk(
            &mut c6,
            6,
            b"top_category",
            n as i64,
            page.len() as i64,
            off6,
        );
        chunks.push(c6);
    }

    let total_byte_size = (buf.len() - 4) as i64;

    let mut fmd = Vec::new();
    let mut fp = 0i16;
    write_i32(&mut fmd, &mut fp, 1, 1); // version = 1
    write_list_hdr(&mut fmd, &mut fp, 2, 12, 7); // schema: 7 elements
    write_schema_elem(&mut fmd, b"schema", None, None, None, None, Some(6));
    write_schema_elem(
        &mut fmd,
        b"user_id",
        Some(6),
        Some(0),
        Some(0),
        Some(1),
        None,
    );
    write_schema_elem(&mut fmd, b"day", Some(1), Some(0), Some(6), Some(6), None);
    write_schema_elem(&mut fmd, b"sessions", Some(2), Some(0), None, None, None);
    write_schema_elem(&mut fmd, b"events", Some(2), Some(0), None, None, None);
    write_schema_elem(
        &mut fmd,
        b"revenue_cents",
        Some(2),
        Some(0),
        None,
        None,
        None,
    );
    write_schema_elem(
        &mut fmd,
        b"top_category",
        Some(6),
        Some(1),
        Some(0),
        Some(1),
        None,
    );
    write_i64(&mut fmd, &mut fp, 3, n as i64);
    if n > 0 {
        write_list_hdr(&mut fmd, &mut fp, 4, 12, 1); // 1 RowGroup
        let mut rp = 0i16;
        write_list_hdr(&mut fmd, &mut rp, 1, 12, 6);
        for c in &chunks {
            fmd.extend_from_slice(c);
        }
        write_i64(&mut fmd, &mut rp, 2, total_byte_size);
        write_i64(&mut fmd, &mut rp, 3, n as i64);
        fmd.push(0); // end RowGroup
    } else {
        write_list_hdr(&mut fmd, &mut fp, 4, 12, 0); // 0 RowGroups
    }
    fmd.push(0); // end FileMetaData

    let fmd_len = fmd.len() as u32;
    buf.extend_from_slice(&fmd);
    buf.extend_from_slice(&fmd_len.to_le_bytes());
    buf.extend_from_slice(b"PAR1");

    let mut file = BufWriter::with_capacity(1 << 20, File::create(path)?);
    file.write_all(&buf)?;
    file.flush()
}

pub fn write_campaign_daily_file(path: &Path, rows: &[CampaignDailyRow<'_>]) -> io::Result<()> {
    let n = rows.len();
    let mut buf = Vec::with_capacity(n * 48 + 1024);
    buf.extend_from_slice(b"PAR1");

    let mut chunks = Vec::new();

    if n > 0 {
        let mut raw = Vec::with_capacity(n * 16);
        let mut page = Vec::new();

        // 1. campaign_id (BYTE_ARRAY = 6)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&(r.campaign_id.len() as u32).to_le_bytes());
            raw.extend_from_slice(r.campaign_id);
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off1 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c1 = Vec::new();
        write_col_chunk(
            &mut c1,
            6,
            b"campaign_id",
            n as i64,
            page.len() as i64,
            off1,
        );
        chunks.push(c1);

        // 2. channel (BYTE_ARRAY = 6)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&(r.channel.len() as u32).to_le_bytes());
            raw.extend_from_slice(r.channel);
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off2 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c2 = Vec::new();
        write_col_chunk(&mut c2, 6, b"channel", n as i64, page.len() as i64, off2);
        chunks.push(c2);

        // 3. day (INT32 = 1)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&r.day.to_le_bytes());
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off3 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c3 = Vec::new();
        write_col_chunk(&mut c3, 1, b"day", n as i64, page.len() as i64, off3);
        chunks.push(c3);

        // 4. sessions (INT64 = 2)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&r.sessions.to_le_bytes());
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off4 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c4 = Vec::new();
        write_col_chunk(&mut c4, 2, b"sessions", n as i64, page.len() as i64, off4);
        chunks.push(c4);

        // 5. purchasing_sessions (INT64 = 2)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&r.purchasing_sessions.to_le_bytes());
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off5 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c5 = Vec::new();
        write_col_chunk(
            &mut c5,
            2,
            b"purchasing_sessions",
            n as i64,
            page.len() as i64,
            off5,
        );
        chunks.push(c5);

        // 6. revenue_cents (INT64 = 2)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&r.revenue_cents.to_le_bytes());
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off6 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c6 = Vec::new();
        write_col_chunk(
            &mut c6,
            2,
            b"revenue_cents",
            n as i64,
            page.len() as i64,
            off6,
        );
        chunks.push(c6);

        // 7. assisted_sessions (INT64 = 2)
        raw.clear();
        for r in rows {
            raw.extend_from_slice(&r.assisted_sessions.to_le_bytes());
        }
        page.clear();
        write_data_page(&mut page, n as i32, &raw);
        let off7 = buf.len() as i64;
        buf.extend_from_slice(&page);
        let mut c7 = Vec::new();
        write_col_chunk(
            &mut c7,
            2,
            b"assisted_sessions",
            n as i64,
            page.len() as i64,
            off7,
        );
        chunks.push(c7);
    }

    let total_byte_size = (buf.len() - 4) as i64;

    let mut fmd = Vec::new();
    let mut fp = 0i16;
    write_i32(&mut fmd, &mut fp, 1, 1); // version = 1
    write_list_hdr(&mut fmd, &mut fp, 2, 12, 8); // schema: 8 elements
    write_schema_elem(&mut fmd, b"schema", None, None, None, None, Some(7));
    write_schema_elem(
        &mut fmd,
        b"campaign_id",
        Some(6),
        Some(0),
        Some(0),
        Some(1),
        None,
    );
    write_schema_elem(
        &mut fmd,
        b"channel",
        Some(6),
        Some(0),
        Some(0),
        Some(1),
        None,
    );
    write_schema_elem(&mut fmd, b"day", Some(1), Some(0), Some(6), Some(6), None);
    write_schema_elem(&mut fmd, b"sessions", Some(2), Some(0), None, None, None);
    write_schema_elem(
        &mut fmd,
        b"purchasing_sessions",
        Some(2),
        Some(0),
        None,
        None,
        None,
    );
    write_schema_elem(
        &mut fmd,
        b"revenue_cents",
        Some(2),
        Some(0),
        None,
        None,
        None,
    );
    write_schema_elem(
        &mut fmd,
        b"assisted_sessions",
        Some(2),
        Some(0),
        None,
        None,
        None,
    );
    write_i64(&mut fmd, &mut fp, 3, n as i64);
    if n > 0 {
        write_list_hdr(&mut fmd, &mut fp, 4, 12, 1); // 1 RowGroup
        let mut rp = 0i16;
        write_list_hdr(&mut fmd, &mut rp, 1, 12, 7);
        for c in &chunks {
            fmd.extend_from_slice(c);
        }
        write_i64(&mut fmd, &mut rp, 2, total_byte_size);
        write_i64(&mut fmd, &mut rp, 3, n as i64);
        fmd.push(0); // end RowGroup
    } else {
        write_list_hdr(&mut fmd, &mut fp, 4, 12, 0); // 0 RowGroups
    }
    fmd.push(0); // end FileMetaData

    let fmd_len = fmd.len() as u32;
    buf.extend_from_slice(&fmd);
    buf.extend_from_slice(&fmd_len.to_le_bytes());
    buf.extend_from_slice(b"PAR1");

    let mut file = BufWriter::with_capacity(1 << 20, File::create(path)?);
    file.write_all(&buf)?;
    file.flush()
}

pub fn ensure_output_dirs(output_dir: &Path) -> io::Result<()> {
    fs::create_dir_all(output_dir.join("user_daily"))?;
    fs::create_dir_all(output_dir.join("campaign_daily"))?;
    Ok(())
}
