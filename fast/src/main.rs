//! High-performance Shopping Sessions pipeline (`fast`).
//! Zero third-party dependencies. Implements every rule of `rules.md` with exact fallbacks.

mod id;
mod io_util;
mod parquet;
mod parse;
mod session;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::thread;
use std::time::Instant;

use id::{FallbackArena, PackedId, fxhash_bytes};
use io_util::{MmapFile, for_each_line, map_and_split_csvs};
use parquet::{
    CampaignDailyRow, UserDailyRow, ensure_output_dirs, write_campaign_daily_file,
    write_user_daily_file,
};
use parse::{
    CLICK_LOOKBACK_MS, LATE_LIMIT_MS, MS_PER_DAY, SESSION_GAP_MS, find_newline, parse_csv_line,
    parse_f1, parse_purchase_price, parse_ts,
};

const NUM_SHARDS: usize = 2048;
const CHUNK_SIZE: usize = 2 * 1024 * 1024; // 2 MiB chunks

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let (data_dir, output_dir) = if args.len() >= 3 {
        (
            PathBuf::from(&args[args.len() - 2]),
            PathBuf::from(&args[args.len() - 1]),
        )
    } else {
        (
            PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string())),
            PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".to_string())),
        )
    };
    let ncpu = env::var("NCPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| thread::available_parallelism().map_or(8, usize::from));
    let use_bins = env::var("SHOPPING_USE_BINS").ok().as_deref() == Some("1");

    let assist_sweep = env::var("SHOPPING_ASSISTS").ok().as_deref() != Some("rescan");
    match run_pipeline(&data_dir, &output_dir, ncpu, use_bins, assist_sweep) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("shopping-fast error: {err}");
            ExitCode::from(1)
        }
    }
}

/// Fast open-addressing hash map from `PackedId` to `u32`, backed by a `FallbackArena`.
struct IdTable {
    slots: Vec<(u64, PackedId, u32)>, // (hash, id, val), hash == 0 means empty
    mask: usize,
    len: usize,
}

impl IdTable {
    fn with_capacity(cap: usize) -> Self {
        let size = (cap * 2).next_power_of_two().max(16);
        Self {
            slots: vec![(0, PackedId(0), 0); size],
            mask: size - 1,
            len: 0,
        }
    }

    #[inline(always)]
    fn non_zero_hash(h: u64) -> u64 {
        if h == 0 { 1 } else { h }
    }

    #[inline(always)]
    fn get(&self, id: PackedId, query_arena: &[u8], table_arena: &[u8]) -> Option<u32> {
        if self.len == 0 {
            return None;
        }
        let h = Self::non_zero_hash(id.hash64(query_arena));
        let mut idx = ((h >> 11) as usize) & self.mask;
        loop {
            let (sh, sid, sval) = self.slots[idx];
            if sh == 0 {
                return None;
            }
            if sh == h && sid.eq_exact(id, table_arena, query_arena) {
                return Some(sval);
            }
            idx = (idx + 1) & self.mask;
        }
    }

    fn grow(&mut self, table_arena: &[u8]) {
        let new_size = (self.slots.len() * 2).max(16);
        let new_mask = new_size - 1;
        let mut new_slots = vec![(0u64, PackedId(0), 0u32); new_size];
        for &(sh, sid, sval) in &self.slots {
            if sh != 0 {
                let mut idx = ((sh >> 11) as usize) & new_mask;
                while new_slots[idx].0 != 0 {
                    idx = (idx + 1) & new_mask;
                }
                new_slots[idx] = (sh, sid, sval);
            }
        }
        self.slots = new_slots;
        self.mask = new_mask;
        let _ = table_arena;
    }

    #[inline(always)]
    fn insert_or_get(
        &mut self,
        id: PackedId,
        query_arena: &[u8],
        table_arena: &mut FallbackArena,
        next_val: impl FnOnce(PackedId) -> u32,
    ) -> u32 {
        if self.len * 2 >= self.slots.len() {
            self.grow(&table_arena.bytes);
        }
        let h = Self::non_zero_hash(id.hash64(query_arena));
        let mut idx = ((h >> 11) as usize) & self.mask;
        loop {
            let (sh, sid, sval) = self.slots[idx];
            if sh == 0 {
                let stored_id = table_arena.import(id, query_arena);
                let val = next_val(stored_id);
                self.slots[idx] = (h, stored_id, val);
                self.len += 1;
                return val;
            }
            if sh == h && sid.eq_exact(id, &table_arena.bytes, query_arena) {
                return sval;
            }
            idx = (idx + 1) & self.mask;
        }
    }
}

struct Catalog {
    window_start: i64,
    window_end: i64,
    categories: Vec<Vec<u8>>, // sorted lexicographically! index 0..num_categories
    unknown_cat_id: u32,
    product_arena: FallbackArena,
    products: IdTable,            // product_id -> category_id (as u32)
    campaign_names: Vec<Vec<u8>>, // index 0 is "organic", 1..=C are sorted by campaign_id
    campaign_channels: Vec<Vec<u8>>,
    campaign_arena: FallbackArena,
    campaigns: IdTable, // campaign_id -> campaign_idx (1..=C)
    link_arena: FallbackArena,
    links: IdTable, // linked user_id -> canonical index in `canonical_links`
    canonical_links: Vec<PackedId>, // index -> canonical PackedId in `link_arena`
}

fn read_manifest(data_dir: &Path) -> io::Result<(i64, i64)> {
    let text = fs::read(data_dir.join("manifest.json"))?;
    let strings = parse::json_strings(&text)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid manifest strings"))?;
    let extract = |key: &[u8]| -> Option<i64> {
        strings
            .chunks_exact(2)
            .rev()
            .find(|pair| pair[0] == key)
            .and_then(|pair| parse_ts(&pair[1]))
    };
    let ws = extract(b"window_start")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad window_start"))?;
    let we = extract(b"window_end")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad window_end"))?;
    Ok((ws, we))
}

fn build_catalog(data_dir: &Path, ncpu: usize) -> io::Result<Catalog> {
    let (window_start, window_end) = read_manifest(data_dir)?;

    // 1. Read products.csv
    let prod_mmap = MmapFile::open(&data_dir.join("products.csv"))?;
    let prod_bytes = prod_mmap.as_slice();
    let mut raw_products: Vec<(&[u8], &[u8])> = Vec::with_capacity(50_000);
    let mut cat_set: Vec<&[u8]> = Vec::new();
    let mut seen_categories = HashSet::new();
    if let Some(nl) = find_newline(prod_bytes) {
        for_each_line(&prod_bytes[nl + 1..], |line| {
            if let Some([pid, cat]) = parse_csv_line::<2>(line) {
                raw_products.push((pid, cat));
                if seen_categories.insert(cat) {
                    cat_set.push(cat);
                }
            }
        });
    }
    let unknown_slice: &[u8] = b"unknown";
    if seen_categories.insert(unknown_slice) {
        cat_set.push(unknown_slice);
    }
    cat_set.sort_unstable();
    let categories: Vec<Vec<u8>> = cat_set.iter().map(|s| s.to_vec()).collect();
    let unknown_cat_id = cat_set
        .binary_search(&unknown_slice)
        .expect("unknown present") as u32;

    let mut product_arena = FallbackArena::new();
    let mut products = IdTable::with_capacity(raw_products.len().max(16));
    let mut tmp_prod_arena = FallbackArena::new();
    for (pid, cat) in raw_products {
        let cat_id = cat_set.binary_search(&cat).unwrap() as u32;
        tmp_prod_arena.bytes.clear();
        let packed = tmp_prod_arena.pack_or_push(pid);
        products.insert_or_get(packed, &tmp_prod_arena.bytes, &mut product_arena, |_| {
            cat_id
        });
    }

    // 2. Read campaigns.csv
    let camp_mmap = MmapFile::open(&data_dir.join("campaigns.csv"))?;
    let camp_bytes = camp_mmap.as_slice();
    let mut raw_campaigns: Vec<(&[u8], &[u8])> = Vec::with_capacity(2_048);
    if let Some(nl) = find_newline(camp_bytes) {
        for_each_line(&camp_bytes[nl + 1..], |line| {
            if let Some([cid, chan]) = parse_csv_line::<2>(line) {
                raw_campaigns.push((cid, chan));
            }
        });
    }
    raw_campaigns.sort_unstable_by(|a, b| a.0.cmp(b.0));
    let mut campaign_names = Vec::with_capacity(raw_campaigns.len() + 1);
    let mut campaign_channels = Vec::with_capacity(raw_campaigns.len() + 1);
    campaign_names.push(b"organic".to_vec());
    campaign_channels.push(b"organic".to_vec());

    let mut campaign_arena = FallbackArena::new();
    let mut campaigns = IdTable::with_capacity(raw_campaigns.len().max(16));
    for (i, (cid, chan)) in raw_campaigns.into_iter().enumerate() {
        let idx = (i + 1) as u32;
        campaign_names.push(cid.to_vec());
        campaign_channels.push(chan.to_vec());
        let mut tmp_arena = FallbackArena::new();
        let packed = tmp_arena.pack_or_push(cid);
        campaigns.insert_or_get(packed, &tmp_arena.bytes, &mut campaign_arena, |_| idx);
    }

    // 3. Read links/*.csv in parallel chunks and build connected components
    let (link_mmaps, link_chunks) = map_and_split_csvs(&data_dir.join("links"), CHUNK_SIZE)?;
    let next_chunk = AtomicUsize::new(0);
    let thread_count = ncpu.min(link_chunks.len().max(1));
    let partial_pairs: Mutex<Vec<(FallbackArena, Vec<(PackedId, PackedId)>)>> =
        Mutex::new(Vec::new());

    thread::scope(|scope| {
        for _ in 0..thread_count {
            scope.spawn(|| {
                let mut arena = FallbackArena::new();
                let mut pairs = Vec::new();
                loop {
                    let c_idx = next_chunk.fetch_add(1, AtomicOrdering::Relaxed);
                    let Some(&(file_idx, start, end)) = link_chunks.get(c_idx) else {
                        break;
                    };
                    let slice = &link_mmaps[file_idx].as_slice()[start..end];
                    for_each_line(slice, |line| {
                        if let Some([id_a, id_b]) = parse_csv_line::<2>(line) {
                            if !id_a.is_empty() && !id_b.is_empty() {
                                let pa = arena.pack_or_push(id_a);
                                let pb = arena.pack_or_push(id_b);
                                pairs.push((pa, pb));
                            }
                        }
                    });
                }
                if !pairs.is_empty() {
                    partial_pairs.lock().unwrap().push((arena, pairs));
                }
            });
        }
    });

    let partials = partial_pairs.into_inner().unwrap();
    let total_pairs: usize = partials.iter().map(|(_, p)| p.len()).sum();
    let mut link_arena = FallbackArena::new();
    let mut links = IdTable::with_capacity(total_pairs + 16);
    let mut nodes: Vec<PackedId> = Vec::with_capacity(total_pairs * 2);
    let mut parent: Vec<u32> = Vec::with_capacity(total_pairs * 2);

    let find = |mut x: u32, parent: &mut [u32]| -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    };

    for (part_arena, pairs) in partials {
        for (pa, pb) in pairs {
            let ua = links.insert_or_get(pa, &part_arena.bytes, &mut link_arena, |stored| {
                let idx = nodes.len() as u32;
                nodes.push(stored);
                parent.push(idx);
                idx
            });
            let ub = links.insert_or_get(pb, &part_arena.bytes, &mut link_arena, |stored| {
                let idx = nodes.len() as u32;
                nodes.push(stored);
                parent.push(idx);
                idx
            });
            let ra = find(ua, &mut parent);
            let rb = find(ub, &mut parent);
            if ra != rb {
                parent[rb as usize] = ra;
            }
        }
    }

    // Compute byte-wise smallest ID per connected component
    let num_nodes = nodes.len();
    let mut min_id: Vec<PackedId> = nodes.clone();
    for i in 0..num_nodes {
        let r = find(i as u32, &mut parent) as usize;
        if nodes[i].cmp_exact(min_id[r], &link_arena.bytes, &link_arena.bytes) == Ordering::Less {
            min_id[r] = nodes[i];
        }
    }
    let mut canonical_links = Vec::with_capacity(num_nodes);
    for i in 0..num_nodes {
        let r = find(i as u32, &mut parent) as usize;
        canonical_links.push(min_id[r]);
    }

    Ok(Catalog {
        window_start,
        window_end,
        categories,
        unknown_cat_id,
        product_arena,
        products,
        campaign_names,
        campaign_channels,
        campaign_arena,
        campaigns,
        link_arena,
        links,
        canonical_links,
    })
}

/// Raw parsed event record stored in a shard before dedup.
/// Notice that invalid copies must participate in dedup (`ingest_ts` comparison)!
/// An empty `user_id` is the invalid-copy tombstone; its other fields are ignored after dedup.
#[derive(Clone, Copy)]
struct RawEvent {
    event_id: PackedId,
    ingest_ts: i64,
    user_id: PackedId, // still in local shard arena or catalog link_arena (resolved after dedup!)
    ts: i64,
    price: u32,  // 0 for view/cart, 1..=10_000_000 for valid purchase
    cat_id: u32, // category index
}

/// Surviving event record after dedup and person resolution.
#[derive(Clone, Copy)]
struct PersonEvent {
    person_id: PackedId,
    ts: i64,
    price: u32,
    cat_id: u32,
}

/// Valid click record routed to a person shard.
#[derive(Clone, Copy)]
struct PersonClick {
    person_id: PackedId,
    ts: i64,
    click_id: PackedId,
    campaign_idx: u32,
}

#[derive(Default)]
struct EventBatch {
    arena: FallbackArena,
    events: Vec<RawEvent>,
}

#[derive(Default)]
struct ClickBatch {
    arena: FallbackArena,
    clicks: Vec<PersonClick>,
}

#[derive(Default)]
struct PersonEventBatch {
    arena: FallbackArena,
    events: Vec<PersonEvent>,
}

fn run_pipeline(
    data_dir: &Path,
    output_dir: &Path,
    ncpu: usize,
    use_bins: bool,
    assist_sweep: bool,
) -> io::Result<()> {
    let t0 = Instant::now();
    ensure_output_dirs(output_dir)?;

    // Stage 1: Catalog + Links
    let catalog = build_catalog(data_dir, ncpu)?;
    let t_cat = t0.elapsed();

    // Stage 2: Scan events/*.csv and partition by event_id into NUM_SHARDS
    let (event_mmaps, event_chunks) = map_and_split_csvs(&data_dir.join("events"), CHUNK_SIZE)?;
    let event_workers = ncpu.min(event_chunks.len().max(1));
    let next_ev_chunk = AtomicUsize::new(0);
    let worker_ev_shards: Mutex<Vec<Vec<EventBatch>>> =
        Mutex::new(Vec::with_capacity(event_workers));

    thread::scope(|scope| {
        for _ in 0..event_workers {
            scope.spawn(|| {
                let mut shards: Vec<EventBatch> =
                    (0..NUM_SHARDS).map(|_| EventBatch::default()).collect();
                loop {
                    let c_idx = next_ev_chunk.fetch_add(1, AtomicOrdering::Relaxed);
                    let Some(&(file_idx, start, end)) = event_chunks.get(c_idx) else {
                        break;
                    };
                    let slice = &event_mmaps[file_idx].as_slice()[start..end];
                    for_each_line(slice, |line| {
                        let Some(
                            [
                                event_id,
                                user_id,
                                ts_text,
                                ingest_text,
                                event_type,
                                product_id,
                                price_text,
                            ],
                        ) = parse_csv_line::<7>(line)
                        else {
                            return;
                        };
                        if event_id.is_empty() {
                            return;
                        }
                        let Some(ingest_ts) = parse_f1(ingest_text) else {
                            return;
                        };

                        // Hash event_id to choose shard
                        let eid_tmp = id::try_pack(event_id);
                        let h = match eid_tmp {
                            Some(pid) => pid.hash64(&[]),
                            None => fxhash_bytes(event_id),
                        };
                        let shard_idx = (h as usize) & (NUM_SHARDS - 1);
                        let batch = &mut shards[shard_idx];
                        let packed_eid = match eid_tmp {
                            Some(pid) => pid,
                            None => batch.arena.push_fallback(event_id),
                        };

                        // Validate the copy now so we only store compact fields
                        let mut packed_uid = PackedId(0);
                        let mut ts_val = 0i64;
                        let mut price_val = 0u32;
                        let mut cat_id = catalog.unknown_cat_id;

                        if !user_id.is_empty() && !product_id.is_empty() {
                            let type_code = match event_type {
                                b"view" | b"cart" => 1u8,
                                b"purchase" => 2u8,
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
                                    if let Some(ts) = parse_ts(ts_text) {
                                        if ts >= catalog.window_start
                                            && ts < catalog.window_end
                                            && ingest_ts.saturating_sub(ts) <= LATE_LIMIT_MS
                                        {
                                            ts_val = ts;
                                            packed_uid = batch.arena.pack_or_push(user_id);
                                            // Only purchases use a category; CSV structure and nonempty product
                                            // were checked for every event above.
                                            let prod_pid = if type_code != 2 {
                                                None
                                            } else {
                                                match id::try_pack(product_id) {
                                                    Some(p) => catalog.products.get(
                                                        p,
                                                        &[],
                                                        &catalog.product_arena.bytes,
                                                    ),
                                                    None => {
                                                        let mut tmp = FallbackArena::new();
                                                        let p = tmp.push_fallback(product_id);
                                                        catalog.products.get(
                                                            p,
                                                            &tmp.bytes,
                                                            &catalog.product_arena.bytes,
                                                        )
                                                    }
                                                }
                                            };
                                            if let Some(cid) = prod_pid {
                                                cat_id = cid as u32;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        batch.events.push(RawEvent {
                            event_id: packed_eid,
                            ingest_ts,
                            user_id: packed_uid,
                            ts: ts_val,
                            price: price_val,
                            cat_id,
                        });
                    });
                }
                worker_ev_shards.lock().unwrap().push(shards);
            });
        }
    });
    drop(event_mmaps);
    let mut all_ev_shards = worker_ev_shards.into_inner().unwrap();
    let t_ev_scan = t0.elapsed();

    // Stage 3: Dedup events per shard in parallel, resolve user_id -> person_id, and re-partition by person_id
    // First transpose `all_ev_shards[worker][shard]` into `ev_by_shard[shard][worker]`
    let mut ev_by_shard: Vec<Vec<EventBatch>> = (0..NUM_SHARDS)
        .map(|_| Vec::with_capacity(all_ev_shards.len()))
        .collect();
    for worker_vec in &mut all_ev_shards {
        for (s_idx, batch) in worker_vec.drain(..).enumerate() {
            if !batch.events.is_empty() {
                ev_by_shard[s_idx].push(batch);
            }
        }
    }
    drop(all_ev_shards);

    let ev_by_shard: Vec<_> = ev_by_shard
        .into_iter()
        .map(|v| Mutex::new(Some(v)))
        .collect();
    let next_dedup_shard = AtomicUsize::new(0);
    let person_ev_partials: Mutex<Vec<Vec<PersonEventBatch>>> =
        Mutex::new(Vec::with_capacity(ncpu));

    thread::scope(|scope| {
        for _ in 0..ncpu {
            scope.spawn(|| {
                let mut out_shards: Vec<PersonEventBatch> = (0..NUM_SHARDS)
                    .map(|_| PersonEventBatch::default())
                    .collect();
                let mut dedup_slots: Vec<(u64, PackedId, u32, u32)> = Vec::new(); // (hash, eid, batch_idx, ev_idx)

                loop {
                    let s_idx = next_dedup_shard.fetch_add(1, AtomicOrdering::Relaxed);
                    if s_idx >= NUM_SHARDS {
                        break;
                    }
                    let batches = ev_by_shard[s_idx].lock().unwrap().take().unwrap();
                    let total_in_shard: usize = batches.iter().map(|b| b.events.len()).sum();
                    if total_in_shard == 0 {
                        continue;
                    }

                    let table_size = (total_in_shard * 2).next_power_of_two().max(16);
                    let mask = table_size - 1;
                    dedup_slots.clear();
                    dedup_slots.resize(table_size, (0, PackedId(0), 0, 0));
                    let mut winners: Vec<usize> = Vec::with_capacity(total_in_shard);

                    for (b_idx, batch) in batches.iter().enumerate() {
                        let b_arena = &batch.arena.bytes;
                        for (e_idx, ev) in batch.events.iter().enumerate() {
                            let h = IdTable::non_zero_hash(ev.event_id.hash64(b_arena));
                            let mut slot = ((h >> 11) as usize) & mask;
                            loop {
                                let (sh, seid, wb, we) = dedup_slots[slot];
                                if sh == 0 {
                                    dedup_slots[slot] =
                                        (h, ev.event_id, b_idx as u32, e_idx as u32);
                                    winners.push(slot);
                                    break;
                                }
                                let w_arena = &batches[wb as usize].arena.bytes;
                                if sh == h && seid.eq_exact(ev.event_id, w_arena, b_arena) {
                                    let prev_ingest =
                                        batches[wb as usize].events[we as usize].ingest_ts;
                                    if ev.ingest_ts < prev_ingest {
                                        dedup_slots[slot] =
                                            (h, ev.event_id, b_idx as u32, e_idx as u32);
                                    }
                                    break;
                                }
                                slot = (slot + 1) & mask;
                            }
                        }
                    }

                    // Emit surviving winners to person shards
                    for &slot in &winners {
                        let (_, _, final_b, final_e) = dedup_slots[slot];
                        let (final_b, final_e) = (final_b as usize, final_e as usize);
                        let win_ev = &batches[final_b].events[final_e];
                        if win_ev.user_id.0 == 0 {
                            continue;
                        }
                        let win_arena = &batches[final_b].arena.bytes;
                        // Resolve user_id -> person_id
                        let (person_id, person_arena) = match catalog.links.get(
                            win_ev.user_id,
                            win_arena,
                            &catalog.link_arena.bytes,
                        ) {
                            Some(canon_idx) => (
                                catalog.canonical_links[canon_idx as usize],
                                &catalog.link_arena.bytes[..],
                            ),
                            None => (win_ev.user_id, win_arena.as_slice()),
                        };
                        let ph = person_id.hash64(person_arena);
                        let p_shard = (ph as usize) & (NUM_SHARDS - 1);
                        let out_batch = &mut out_shards[p_shard];
                        let imported_pid = out_batch.arena.import(person_id, person_arena);
                        out_batch.events.push(PersonEvent {
                            person_id: imported_pid,
                            ts: win_ev.ts,
                            price: win_ev.price,
                            cat_id: win_ev.cat_id,
                        });
                    }
                }
                person_ev_partials.lock().unwrap().push(out_shards);
            });
        }
    });
    drop(ev_by_shard);
    let mut all_person_ev = person_ev_partials.into_inner().unwrap();
    let t_ev_dedup = t0.elapsed();

    // Stage 4: Scan clicks/*.csv, validate, resolve person_id, and partition by person_id into NUM_SHARDS
    let (click_mmaps, click_chunks) = map_and_split_csvs(&data_dir.join("clicks"), CHUNK_SIZE)?;
    let click_workers = ncpu.min(click_chunks.len().max(1));
    let next_cl_chunk = AtomicUsize::new(0);
    let person_cl_partials: Mutex<Vec<Vec<ClickBatch>>> =
        Mutex::new(Vec::with_capacity(click_workers));
    let click_win_start = catalog.window_start - CLICK_LOOKBACK_MS;
    let click_win_end = catalog.window_end;

    thread::scope(|scope| {
        for _ in 0..click_workers {
            scope.spawn(|| {
                let mut shards: Vec<ClickBatch> =
                    (0..NUM_SHARDS).map(|_| ClickBatch::default()).collect();
                let mut tmp_arena = FallbackArena::new();
                loop {
                    let c_idx = next_cl_chunk.fetch_add(1, AtomicOrdering::Relaxed);
                    let Some(&(file_idx, start, end)) = click_chunks.get(c_idx) else {
                        break;
                    };
                    let slice = &click_mmaps[file_idx].as_slice()[start..end];
                    for_each_line(slice, |line| {
                        let Some([click_id, user_id, campaign_id, ts_text]) =
                            parse_csv_line::<4>(line)
                        else {
                            return;
                        };
                        if click_id.is_empty() || user_id.is_empty() || campaign_id.is_empty() {
                            return;
                        }
                        let Some(ts) = parse_ts(ts_text) else {
                            return;
                        };
                        if ts < click_win_start || ts >= click_win_end {
                            return;
                        }
                        tmp_arena.bytes.clear();
                        let camp_pid = tmp_arena.pack_or_push(campaign_id);
                        let Some(camp_idx) = catalog.campaigns.get(
                            camp_pid,
                            &tmp_arena.bytes,
                            &catalog.campaign_arena.bytes,
                        ) else {
                            return;
                        };

                        tmp_arena.bytes.clear();
                        let uid_pid = tmp_arena.pack_or_push(user_id);
                        let (person_id, person_arena) = match catalog.links.get(
                            uid_pid,
                            &tmp_arena.bytes,
                            &catalog.link_arena.bytes,
                        ) {
                            Some(canon_idx) => (
                                catalog.canonical_links[canon_idx as usize],
                                &catalog.link_arena.bytes[..],
                            ),
                            None => (uid_pid, tmp_arena.bytes.as_slice()),
                        };
                        let ph = person_id.hash64(person_arena);
                        let p_shard = (ph as usize) & (NUM_SHARDS - 1);
                        let batch = &mut shards[p_shard];
                        let stored_pid = batch.arena.import(person_id, person_arena);
                        let stored_cid = batch.arena.pack_or_push(click_id);
                        batch.clicks.push(PersonClick {
                            person_id: stored_pid,
                            ts,
                            click_id: stored_cid,
                            campaign_idx: camp_idx as u32,
                        });
                    });
                }
                person_cl_partials.lock().unwrap().push(shards);
            });
        }
    });
    drop(click_mmaps);
    let mut all_person_cl = person_cl_partials.into_inner().unwrap();
    let t_cl_scan = t0.elapsed();

    // Stage 5: Transpose person events & clicks by shard, then process each person shard in parallel
    let mut pev_by_shard: Vec<Vec<PersonEventBatch>> = (0..NUM_SHARDS)
        .map(|_| Vec::with_capacity(all_person_ev.len()))
        .collect();
    for worker_vec in &mut all_person_ev {
        for (s_idx, batch) in worker_vec.drain(..).enumerate() {
            if !batch.events.is_empty() {
                pev_by_shard[s_idx].push(batch);
            }
        }
    }
    drop(all_person_ev);

    let mut pcl_by_shard: Vec<Vec<ClickBatch>> = (0..NUM_SHARDS)
        .map(|_| Vec::with_capacity(all_person_cl.len()))
        .collect();
    for worker_vec in &mut all_person_cl {
        for (s_idx, batch) in worker_vec.drain(..).enumerate() {
            if !batch.clicks.is_empty() {
                pcl_by_shard[s_idx].push(batch);
            }
        }
    }
    drop(all_person_cl);

    let next_person_shard = AtomicUsize::new(0);
    let num_campaigns = catalog.campaign_names.len();
    let num_categories = catalog.categories.len();
    let window_start = catalog.window_start;
    let full_window_days =
        (catalog.window_end.saturating_sub(catalog.window_start) / MS_PER_DAY + 2).max(1) as usize;
    // A wide window or a large catalog must not force a campaign × day allocation.
    let window_days = if num_campaigns
        .checked_mul(full_window_days)
        .is_some_and(|n| n <= 1_000_000)
    {
        full_window_days
    } else {
        0
    };
    let base_day = catalog.window_start.div_euclid(MS_PER_DAY) as i32;

    // Campaign daily accumulators: [campaign_idx * window_days + day_offset]
    // Plus a fallback HashMap for any day outside 0..window_days
    #[derive(Clone, Copy, Default)]
    struct CampAcc {
        sessions: i64,
        purchasing_sessions: i64,
        revenue_cents: i64,
        assisted_sessions: i64,
    }

    let global_camp_grid: Mutex<Vec<CampAcc>> =
        Mutex::new(vec![CampAcc::default(); num_campaigns * window_days]);
    let global_camp_extra: Mutex<HashMap<(u32, i32), CampAcc>> = Mutex::new(HashMap::new());
    let io_error: Mutex<Option<io::Error>> = Mutex::new(None);
    let total_user_rows = AtomicUsize::new(0);

    thread::scope(|scope| {
        for worker_id in 0..ncpu {
            let global_camp_grid = &global_camp_grid;
            let global_camp_extra = &global_camp_extra;
            let io_error = &io_error;
            let total_user_rows = &total_user_rows;
            let pev_by_shard = &pev_by_shard;
            let pcl_by_shard = &pcl_by_shard;
            let catalog = &catalog;
            let next_person_shard = &next_person_shard;

            scope.spawn(move || {
                let mut local_camp_grid = vec![CampAcc::default(); num_campaigns * window_days];
                let mut local_camp_extra: HashMap<(u32, i32), CampAcc> = HashMap::new();

                // Buffer for this worker's user_daily rows
                let mut user_id_bytes: Vec<u8> = Vec::with_capacity(1 << 20);
                let mut user_records: Vec<(usize, usize, i32, i64, i64, i64, Option<u32>)> =
                    Vec::with_capacity(64_000);

                // Per-campaign assist stamp table to deduplicate assists per session in O(1)
                let mut assist_stamp = vec![0u32; num_campaigns];
                let mut cur_stamp = 0u32;
                let mut active_counts = vec![0u32; num_campaigns];
                let mut active_positions = vec![0usize; num_campaigns];
                let mut active_campaigns: Vec<u32> = Vec::new();

                loop {
                    let s_idx = next_person_shard.fetch_add(1, AtomicOrdering::Relaxed);
                    if s_idx >= NUM_SHARDS {
                        break;
                    }
                    let ev_batches = &pev_by_shard[s_idx];
                    let total_ev: usize = ev_batches.iter().map(|b| b.events.len()).sum();
                    if total_ev == 0 {
                        continue;
                    }
                    let cl_batches = &pcl_by_shard[s_idx];

                    // Group events by person_id in this shard
                    let mut shard_arena = FallbackArena::new();
                    let mut person_table = IdTable::with_capacity(total_ev / 8 + 16);
                    let mut person_ids: Vec<PackedId> = Vec::new();
                    let mut person_events: Vec<Vec<(i64, u32, u32)>> = Vec::new();

                    for batch in ev_batches {
                        let b_arena = &batch.arena.bytes;
                        for ev in &batch.events {
                            let p_idx = person_table.insert_or_get(
                                ev.person_id,
                                b_arena,
                                &mut shard_arena,
                                |stored_pid| {
                                    let idx = person_ids.len() as u32;
                                    person_ids.push(stored_pid);
                                    person_events.push(Vec::new());
                                    idx
                                },
                            ) as usize;
                            person_events[p_idx].push((ev.ts, ev.price, ev.cat_id));
                        }
                    }

                    // Group clicks for people who exist in `person_table`
                    let mut person_clicks: Vec<Vec<(i64, PackedId, u32)>> =
                        vec![Vec::new(); person_ids.len()];
                    for batch in cl_batches {
                        let b_arena = &batch.arena.bytes;
                        for cl in &batch.clicks {
                            if let Some(p_idx) =
                                person_table.get(cl.person_id, b_arena, &shard_arena.bytes)
                            {
                                let stored_cid = shard_arena.import(cl.click_id, b_arena);
                                person_clicks[p_idx as usize].push((
                                    cl.ts,
                                    stored_cid,
                                    cl.campaign_idx,
                                ));
                            }
                        }
                    }

                    // Process each person in this shard
                    let mut cat_revenue = vec![0i64; num_categories];
                    let mut sessions_buf: Vec<(i32, i64, i64, i64, bool)> = Vec::new(); // (day, start_ts, events, rev, has_purchase)

                    for (p_idx, evs) in person_events.iter_mut().enumerate() {
                        sessions_buf.clear();

                        session::order(evs, use_bins, window_start, catalog.window_end);

                        // Build sessions and per-day user totals
                        // Since `evs` is sorted by `ts`, sessions are naturally ordered by `start_ts`,
                        // and therefore `day = start_ts.div_euclid(MS_PER_DAY)` is monotonically non-decreasing!
                        let mut cur_day: i32 = i32::MIN;
                        let mut day_sessions: i64 = 0;
                        let mut day_events: i64 = 0;
                        let mut day_rev: i64 = 0;
                        let mut day_has_purchases = false;

                        let mut prev_ts: i64 = i64::MIN;
                        let mut sess_start: i64 = 0;
                        let mut sess_day: i32 = 0;
                        let mut sess_events: i64 = 0;
                        let mut sess_rev: i64 = 0;
                        let mut sess_has_purch = false;

                        let flush_day =
                            |user_id_bytes: &mut Vec<u8>,
                             user_records: &mut Vec<(
                                usize,
                                usize,
                                i32,
                                i64,
                                i64,
                                i64,
                                Option<u32>,
                            )>,
                             pid: PackedId,
                             arena: &[u8],
                             day: i32,
                             sessions: i64,
                             events: i64,
                             rev: i64,
                             has_purch: bool,
                             cat_rev: &mut [i64]| {
                                let top_cat = if has_purch {
                                    let mut best_c = 0u32;
                                    let mut best_v = -1i64;
                                    for (c_idx, &v) in cat_rev.iter().enumerate() {
                                        if v > best_v {
                                            best_v = v;
                                            best_c = c_idx as u32;
                                        }
                                    }
                                    cat_rev.fill(0);
                                    Some(best_c)
                                } else {
                                    None
                                };
                                let mut tmp = [0u8; 10];
                                let u_slice = pid.as_bytes(arena, &mut tmp);
                                let u_off = user_id_bytes.len();
                                user_id_bytes.extend_from_slice(u_slice);
                                user_records.push((
                                    u_off,
                                    u_slice.len(),
                                    day,
                                    sessions,
                                    events,
                                    rev,
                                    top_cat,
                                ));
                            };

                        for &(ts, price, cat_id) in evs.iter() {
                            if prev_ts == i64::MIN || ts - prev_ts > SESSION_GAP_MS {
                                if prev_ts != i64::MIN {
                                    sessions_buf.push((
                                        sess_day,
                                        sess_start,
                                        sess_events,
                                        sess_rev,
                                        sess_has_purch,
                                    ));
                                }
                                sess_start = ts;
                                sess_day = i32::try_from(ts.div_euclid(MS_PER_DAY))
                                    .expect("session start exceeds the Parquet DATE range");
                                sess_events = 0;
                                sess_rev = 0;
                                sess_has_purch = false;

                                if sess_day != cur_day {
                                    if cur_day != i32::MIN {
                                        flush_day(
                                            &mut user_id_bytes,
                                            &mut user_records,
                                            person_ids[p_idx],
                                            &shard_arena.bytes,
                                            cur_day,
                                            day_sessions,
                                            day_events,
                                            day_rev,
                                            day_has_purchases,
                                            &mut cat_revenue,
                                        );
                                    }
                                    cur_day = sess_day;
                                    day_sessions = 0;
                                    day_events = 0;
                                    day_rev = 0;
                                    day_has_purchases = false;
                                }
                                day_sessions += 1;
                            }
                            sess_events += 1;
                            day_events += 1;
                            if price > 0 {
                                let p64 = price as i64;
                                sess_rev += p64;
                                sess_has_purch = true;
                                day_rev += p64;
                                day_has_purchases = true;
                                cat_revenue[cat_id as usize] += p64;
                            }
                            prev_ts = ts;
                        }
                        if prev_ts != i64::MIN {
                            sessions_buf.push((
                                sess_day,
                                sess_start,
                                sess_events,
                                sess_rev,
                                sess_has_purch,
                            ));
                            flush_day(
                                &mut user_id_bytes,
                                &mut user_records,
                                person_ids[p_idx],
                                &shard_arena.bytes,
                                cur_day,
                                day_sessions,
                                day_events,
                                day_rev,
                                day_has_purchases,
                                &mut cat_revenue,
                            );
                        }

                        for c in active_campaigns.drain(..) {
                            active_counts[c as usize] = 0;
                        }
                        // Click attribution and assists for this person's sessions
                        let clicks = &mut person_clicks[p_idx];
                        if clicks.is_empty() {
                            // All sessions are organic (campaign_idx = 0)
                            for &(s_day, _s_start, _s_ev, s_rev, s_purch) in &sessions_buf {
                                let d_off = s_day as i64 - base_day as i64;
                                let acc = if d_off >= 0 && (d_off as usize) < window_days {
                                    &mut local_camp_grid[d_off as usize]
                                } else {
                                    local_camp_extra.entry((0, s_day)).or_default()
                                };
                                acc.sessions += 1;
                                if s_purch {
                                    acc.purchasing_sessions += 1;
                                }
                                acc.revenue_cents += s_rev;
                            }
                        } else {
                            // Sort by ts ascending, ID descending: the last eligible row wins the tie.
                            let arena_ref = &shard_arena.bytes;
                            clicks.sort_unstable_by(|a, b| {
                                a.0.cmp(&b.0)
                                    .then_with(|| b.1.cmp_exact(a.1, arena_ref, arena_ref))
                            });

                            let mut left = 0usize;
                            let mut right = 0usize;
                            let n_clicks = clicks.len();

                            for &(s_day, s_start, _s_ev, s_rev, s_purch) in &sessions_buf {
                                let min_click_ts = s_start - CLICK_LOOKBACK_MS;
                                while right < n_clicks && clicks[right].0 <= s_start {
                                    if assist_sweep {
                                        let c = clicks[right].2 as usize;
                                        if active_counts[c] == 0 {
                                            active_positions[c] = active_campaigns.len();
                                            active_campaigns.push(c as u32);
                                        }
                                        active_counts[c] += 1;
                                    }
                                    right += 1;
                                }
                                while left < right && clicks[left].0 < min_click_ts {
                                    if assist_sweep {
                                        let c = clicks[left].2 as usize;
                                        active_counts[c] -= 1;
                                        if active_counts[c] == 0 {
                                            let pos = active_positions[c];
                                            active_campaigns.swap_remove(pos);
                                            if pos < active_campaigns.len() {
                                                active_positions[active_campaigns[pos] as usize] =
                                                    pos;
                                            }
                                        }
                                    }
                                    left += 1;
                                }

                                if left == right {
                                    // Organic
                                    let d_off = s_day as i64 - base_day as i64;
                                    let acc = if d_off >= 0 && (d_off as usize) < window_days {
                                        &mut local_camp_grid[d_off as usize]
                                    } else {
                                        local_camp_extra.entry((0, s_day)).or_default()
                                    };
                                    acc.sessions += 1;
                                    if s_purch {
                                        acc.purchasing_sessions += 1;
                                    }
                                    acc.revenue_cents += s_rev;
                                } else {
                                    let credited_camp = clicks[right - 1].2;

                                    let d_off = s_day as i64 - base_day as i64;
                                    let in_grid = d_off >= 0 && (d_off as usize) < window_days;
                                    let acc = if in_grid {
                                        &mut local_camp_grid[(credited_camp as usize) * window_days
                                            + (d_off as usize)]
                                    } else {
                                        local_camp_extra.entry((credited_camp, s_day)).or_default()
                                    };
                                    acc.sessions += 1;
                                    if s_purch {
                                        acc.purchasing_sessions += 1;
                                    }
                                    acc.revenue_cents += s_rev;

                                    if assist_sweep {
                                        for &c in &active_campaigns {
                                            let a_acc = if in_grid {
                                                &mut local_camp_grid
                                                    [c as usize * window_days + d_off as usize]
                                            } else {
                                                local_camp_extra.entry((c, s_day)).or_default()
                                            };
                                            a_acc.assisted_sessions += 1;
                                        }
                                    } else {
                                        cur_stamp = cur_stamp.wrapping_add(1);
                                        if cur_stamp == 0 {
                                            assist_stamp.fill(0);
                                            cur_stamp = 1;
                                        }
                                        for cl in &clicks[left..right] {
                                            let c_idx = cl.2 as usize;
                                            if assist_stamp[c_idx] != cur_stamp {
                                                assist_stamp[c_idx] = cur_stamp;
                                                let a_acc = if in_grid {
                                                    &mut local_camp_grid
                                                        [c_idx * window_days + (d_off as usize)]
                                                } else {
                                                    local_camp_extra
                                                        .entry((cl.2, s_day))
                                                        .or_default()
                                                };
                                                a_acc.assisted_sessions += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Write this worker's user_daily parquet file if non-empty
                if !user_records.is_empty() {
                    total_user_rows.fetch_add(user_records.len(), AtomicOrdering::Relaxed);
                    let mut rows: Vec<UserDailyRow<'_>> = Vec::with_capacity(user_records.len());
                    for &(u_off, u_len, day, sessions, events, revenue_cents, top_cat) in
                        &user_records
                    {
                        rows.push(UserDailyRow {
                            user_id: &user_id_bytes[u_off..u_off + u_len],
                            day,
                            sessions,
                            events,
                            revenue_cents,
                            top_category: top_cat
                                .map(|c| catalog.categories[c as usize].as_slice()),
                        });
                    }
                    let path = output_dir
                        .join("user_daily")
                        .join(format!("part-{worker_id:02}.parquet"));
                    if let Err(e) = write_user_daily_file(&path, &rows) {
                        *io_error.lock().unwrap() = Some(e);
                    }
                }

                // Merge local campaign accumulators into global
                {
                    let mut g_grid = global_camp_grid.lock().unwrap();
                    for (g, l) in g_grid.iter_mut().zip(local_camp_grid.iter()) {
                        g.sessions += l.sessions;
                        g.purchasing_sessions += l.purchasing_sessions;
                        g.revenue_cents += l.revenue_cents;
                        g.assisted_sessions += l.assisted_sessions;
                    }
                }
                if !local_camp_extra.is_empty() {
                    let mut g_extra = global_camp_extra.lock().unwrap();
                    for (k, l) in local_camp_extra {
                        let g = g_extra.entry(k).or_default();
                        g.sessions += l.sessions;
                        g.purchasing_sessions += l.purchasing_sessions;
                        g.revenue_cents += l.revenue_cents;
                        g.assisted_sessions += l.assisted_sessions;
                    }
                }
            });
        }
    });

    let t_sessions = t0.elapsed();
    // Include the large input buffers' destruction in reported end-to-end time.
    drop(pev_by_shard);
    drop(pcl_by_shard);
    if let Some(err) = io_error.into_inner().unwrap() {
        return Err(err);
    }

    // If no worker wrote any user_daily rows, write an empty part-00.parquet
    if total_user_rows.load(AtomicOrdering::Relaxed) == 0 {
        write_user_daily_file(&output_dir.join("user_daily").join("part-00.parquet"), &[])?;
    }

    // Write campaign_daily/part-00.parquet
    let g_grid = global_camp_grid.into_inner().unwrap();
    let g_extra = global_camp_extra.into_inner().unwrap();
    let mut camp_rows: Vec<CampaignDailyRow<'_>> = Vec::new();
    for c_idx in 0..num_campaigns {
        let cname = catalog.campaign_names[c_idx].as_slice();
        let cchan = catalog.campaign_channels[c_idx].as_slice();
        for d_off in 0..window_days {
            let acc = g_grid[c_idx * window_days + d_off];
            if acc.sessions > 0 || acc.assisted_sessions > 0 {
                camp_rows.push(CampaignDailyRow {
                    campaign_id: cname,
                    channel: cchan,
                    day: base_day + (d_off as i32),
                    sessions: acc.sessions,
                    purchasing_sessions: acc.purchasing_sessions,
                    revenue_cents: acc.revenue_cents,
                    assisted_sessions: acc.assisted_sessions,
                });
            }
        }
    }
    for ((c_idx, day), acc) in &g_extra {
        if acc.sessions > 0 || acc.assisted_sessions > 0 {
            let cname = catalog.campaign_names[*c_idx as usize].as_slice();
            let cchan = catalog.campaign_channels[*c_idx as usize].as_slice();
            camp_rows.push(CampaignDailyRow {
                campaign_id: cname,
                channel: cchan,
                day: *day,
                sessions: acc.sessions,
                purchasing_sessions: acc.purchasing_sessions,
                revenue_cents: acc.revenue_cents,
                assisted_sessions: acc.assisted_sessions,
            });
        }
    }
    write_campaign_daily_file(
        &output_dir.join("campaign_daily").join("part-00.parquet"),
        &camp_rows,
    )?;

    drop(camp_rows);
    drop(catalog);
    let t_total = t0.elapsed();
    if env::var("SHOPPING_TIMING").is_ok() {
        eprintln!("scanner={}", parse::scanner_name());
        eprintln!(
            "timing_ms: catalog={:.1} ev_scan={:.1} ev_dedup={:.1} cl_scan={:.1} sessions_and_user_write={:.1} cleanup_and_campaign_write={:.1} total={:.1}",
            t_cat.as_secs_f64() * 1000.0,
            (t_ev_scan - t_cat).as_secs_f64() * 1000.0,
            (t_ev_dedup - t_ev_scan).as_secs_f64() * 1000.0,
            (t_cl_scan - t_ev_dedup).as_secs_f64() * 1000.0,
            (t_sessions - t_cl_scan).as_secs_f64() * 1000.0,
            (t_total - t_sessions).as_secs_f64() * 1000.0,
            t_total.as_secs_f64() * 1000.0,
        );
        #[cfg(target_os = "linux")]
        if let Ok(status) = fs::read_to_string("/proc/self/status") {
            if let Some(hwm) = status.lines().find(|line| line.starts_with("VmHWM:")) {
                eprintln!("peak_rss: {hwm}");
            }
        }
    }

    Ok(())
}
