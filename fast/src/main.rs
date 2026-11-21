//! High-performance Shopping Sessions pipeline (`fast`).
//! Zero third-party dependencies. Implements every rule of `rules.md` with exact fallbacks.

mod fastpath;
mod id;
mod io_util;
mod parquet;
mod parse;
mod scan;
mod session;
mod sink;

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

use fastpath::{FastClick, FastRec, Keys, People, Q, QB};
use id::{FallbackArena, PackedId, fxhash_bytes};
use io_util::{MmapFile, for_each_line, release};
use sink::{CampAcc, Sink};
use parquet::{
    CampaignDailyRow, UserCols, ensure_output_dirs, write_campaign_daily_file,
    write_user_daily_cols,
};
use parse::{
    CLICK_LOOKBACK_MS, LATE_LIMIT_MS, MS_PER_DAY, find_newline, parse_csv_line,
    DateCache, parse_f1_cached, parse_purchase_price, parse_ts, parse_ts_cached,
};

const NUM_SHARDS: usize = 2048;
/// Person slots per click group (2^10).
const GROUP_BITS: u32 = 10;
/// Raw bytes per scan chunk; the read buffer stays inside L2.
const SCAN_CHUNK: u64 = 512 * 1024;

unsafe extern "C" {
    fn _exit(code: i32) -> !;
}

/// The image ships a portable build (the entry point), an x86-64-v3 build and
/// an x86-64-v4 (AVX-512) build, both of the latter also using PCLMULQDQ. The portable build re-executes the widest
/// build whose every feature this CPU has, so an older scorer CPU still runs
/// correct code. `SHOPPING_BUILD=portable|v3|v4` selects (v3 is the default).
#[cfg(target_arch = "x86_64")]
fn dispatch_cpu_build() {
    use std::os::unix::process::CommandExt;
    if cfg!(target_feature = "avx2") {
        return;
    }
    // Default: the v3 build. On Sapphire Rapids the v4 (AVX-512) build retires
    // 5% fewer instructions but takes about 2% more cycles over a full 20M run,
    // so it is opt-in (`SHOPPING_BUILD=v4`).
    let cap = match env::var("SHOPPING_BUILD").ok().as_deref() {
        Some("portable") => 0,
        Some("v4") => 4,
        _ if env::var_os("SHOPPING_PORTABLE").is_some() => 0,
        _ => 3,
    };
    let v3 = is_x86_feature_detected!("avx")
        && is_x86_feature_detected!("avx2")
        && is_x86_feature_detected!("bmi1")
        && is_x86_feature_detected!("bmi2")
        && is_x86_feature_detected!("fma")
        && is_x86_feature_detected!("f16c")
        && is_x86_feature_detected!("lzcnt")
        && is_x86_feature_detected!("movbe")
        && is_x86_feature_detected!("popcnt")
        && is_x86_feature_detected!("sse4.2")
        && is_x86_feature_detected!("xsave")
        && is_x86_feature_detected!("pclmulqdq");
    let v4 = v3
        && is_x86_feature_detected!("avx512f")
        && is_x86_feature_detected!("avx512bw")
        && is_x86_feature_detected!("avx512cd")
        && is_x86_feature_detected!("avx512dq")
        && is_x86_feature_detected!("avx512vl");
    let Ok(exe) = env::current_exe() else { return };
    let mut candidates = Vec::new();
    if v4 && cap >= 4 {
        candidates.push("shopping-fast-v4");
    }
    if v3 && cap >= 3 {
        candidates.push("shopping-fast-v3");
    }
    for name in candidates {
        let target = exe.with_file_name(name);
        if target.is_file() {
            // `exec` only returns on failure; then try the next build.
            let _ = std::process::Command::new(&target).args(env::args_os().skip(1)).exec();
        }
    }
}

fn main() -> ExitCode {
    #[cfg(target_arch = "x86_64")]
    dispatch_cpu_build();
    let code = real_main();
    if code == ExitCode::SUCCESS {
        // Every output file was flushed and closed by its writer. Skip
        // destructors and the unmapping of inputs and buffers.
        unsafe { _exit(0) }
    }
    code
}

fn real_main() -> ExitCode {
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

/// Packed product ID -> category, as two flat arrays (keys probed first), so
/// the purchase lookup stays in L2. Unpackable IDs use `Catalog::products`.
struct PackedMap {
    keys: Vec<u64>,
    vals: Vec<u32>,
    shift: u32,
}

impl PackedMap {
    fn new(n: usize) -> Self {
        let size = (n * 2).next_power_of_two().max(16);
        Self { keys: vec![0; size], vals: vec![0; size], shift: 64 - size.trailing_zeros() }
    }

    #[inline(always)]
    fn slot(&self, key: u64) -> usize {
        (key.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> self.shift) as usize
    }

    /// First insertion wins, as in `IdTable::insert_or_get`.
    fn insert(&mut self, key: u64, val: u32) {
        let mask = self.keys.len() - 1;
        let mut i = self.slot(key);
        while self.keys[i] != 0 {
            if self.keys[i] == key {
                return;
            }
            i = (i + 1) & mask;
        }
        self.keys[i] = key;
        self.vals[i] = val;
    }

    #[inline(always)]
    fn get(&self, key: u64) -> Option<u32> {
        let mask = self.keys.len() - 1;
        let mut i = self.slot(key);
        loop {
            let k = self.keys[i];
            if k == key {
                return Some(self.vals[i]);
            }
            if k == 0 {
                return None;
            }
            i = (i + 1) & mask;
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
    packed_products: PackedMap,   // the same, for packable product IDs
    campaign_names: Vec<Vec<u8>>, // index 0 is "organic", 1..=C are sorted by campaign_id
    campaign_channels: Vec<Vec<u8>>,
    campaign_arena: FallbackArena,
    campaigns: IdTable, // campaign_id -> campaign_idx (1..=C)
    packed_campaigns: PackedMap, // the same, for packable campaign IDs
    link_arena: FallbackArena,
    links: IdTable, // linked user_id -> canonical index in `canonical_links`
    canonical_links: Vec<PackedId>, // index -> canonical PackedId in `link_arena`
    link_nodes: Vec<PackedId>,      // index -> the node's own ID in `link_arena`
    link_roots: Vec<u32>,           // index -> component root index
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

fn build_catalog(data_dir: &Path) -> io::Result<Catalog> {
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
    let mut packed_products = PackedMap::new(raw_products.len());
    let mut tmp_prod_arena = FallbackArena::new();
    for (pid, cat) in raw_products {
        let cat_id = cat_set.binary_search(&cat).unwrap() as u32;
        tmp_prod_arena.bytes.clear();
        let packed = tmp_prod_arena.pack_or_push(pid);
        if packed.is_packed() {
            packed_products.insert(packed.0, cat_id);
        }
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
    let mut packed_campaigns = PackedMap::new(raw_campaigns.len());
    for (i, (cid, chan)) in raw_campaigns.into_iter().enumerate() {
        let idx = (i + 1) as u32;
        campaign_names.push(cid.to_vec());
        campaign_channels.push(chan.to_vec());
        let mut tmp_arena = FallbackArena::new();
        let packed = tmp_arena.pack_or_push(cid);
        if packed.is_packed() {
            packed_campaigns.insert(packed.0, idx);
        }
        campaigns.insert_or_get(packed, &tmp_arena.bytes, &mut campaign_arena, |_| idx);
    }

    Ok(Catalog {
        window_start,
        window_end,
        categories,
        unknown_cat_id,
        product_arena,
        products,
        packed_products,
        campaign_names,
        campaign_channels,
        campaign_arena,
        campaigns,
        packed_campaigns,
        link_arena: FallbackArena::new(),
        links: IdTable::with_capacity(16),
        canonical_links: Vec::new(),
        link_nodes: Vec::new(),
        link_roots: Vec::new(),
    })
}

/// The link components, built while the event scan runs (the scan never
/// needs them).
struct LinkPart {
    link_arena: FallbackArena,
    links: IdTable,
    canonical_links: Vec<PackedId>,
    link_nodes: Vec<PackedId>,
    link_roots: Vec<u32>,
}

impl Catalog {
    fn install_links(&mut self, part: LinkPart) {
        self.link_arena = part.link_arena;
        self.links = part.links;
        self.canonical_links = part.canonical_links;
        self.link_nodes = part.link_nodes;
        self.link_roots = part.link_roots;
    }
}

fn build_links(data_dir: &Path, ncpu: usize) -> io::Result<LinkPart> {
    let use_pread = env::var("SHOPPING_PREAD").ok().as_deref() == Some("1");
    // 3. Read links/*.csv in parallel chunks and build connected components
    let link_inputs = scan::plan(&data_dir.join("links"), SCAN_CHUNK)?;
    let link_chunks = &link_inputs.chunks;
    let next_chunk = AtomicUsize::new(0);
    // Links are small; two readers keep the link build from competing with the
    // event scan it overlaps.
    let thread_count = ncpu.min(2).min(link_chunks.len().max(1));
    let partial_pairs: Mutex<Vec<(FallbackArena, Vec<(PackedId, PackedId)>)>> =
        Mutex::new(Vec::new());

    thread::scope(|scope| {
        for _ in 0..thread_count {
            scope.spawn(|| {
                let mut arena = FallbackArena::new();
                let mut pairs = Vec::new();
                let mut reader = scan::Reader::default();
                loop {
                    let c_idx = next_chunk.fetch_add(1, AtomicOrdering::Relaxed);
                    let Some(&(file_idx, start, end)) = link_chunks.get(c_idx) else {
                        break;
                    };
                    let f = file_idx as usize;
                    let (bytes, first, stop, mapped) = if use_pread {
                        let (b, x, y) = reader
                            .load(&link_inputs.files[f], link_inputs.sizes[f], start, end)
                            .expect("read links");
                        (b, x, y, false)
                    } else {
                        reader
                            .load_mapped(
                                &link_inputs.files[f],
                                link_inputs.sizes[f],
                                link_inputs.maps[f].as_slice(),
                                start,
                                end,
                            )
                            .expect("read links")
                    };
                    scan::for_each_record::<2>(bytes, first, stop, |rec| {
                        let [id_a, id_b] = rec.fields();
                        if !id_a.is_empty() && !id_b.is_empty() {
                            let pa = arena.pack_or_push_padded(id_a);
                            let pb = arena.pack_or_push_padded(id_b);
                            pairs.push((pa, pb));
                        }
                    });
                    if mapped {
                        scan::unmap_pages(bytes, first, stop);
                    }
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
    let mut link_roots = Vec::with_capacity(num_nodes);
    for i in 0..num_nodes {
        let r = find(i as u32, &mut parent) as usize;
        canonical_links.push(min_id[r]);
        link_roots.push(r as u32);
    }

    Ok(LinkPart {
        link_arena,
        links,
        canonical_links,
        link_nodes: nodes,
        link_roots,
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

/// Sample the event files, propose generator keys and bounds from the data,
/// and classify person slots from the actual links. `None` disables the fast
/// path (everything then takes the exact solver).
fn prepare_fast(catalog: &Catalog, inputs: &scan::Inputs, ncpu: usize) -> Option<(Keys, People)> {
    // The fast record holds a 16-bit category and a 40-bit window offset (34.8
    // years of ms); beyond those the exact solver runs everything.
    if env::var("SHOPPING_FAST").ok().as_deref() == Some("0")
        || catalog.categories.len() > 65_536
        || catalog.window_end.saturating_sub(catalog.window_start) > fastpath::M40 as i64
    {
        return None;
    }
    let total: u64 = inputs.sizes.iter().sum();
    let mut order: Vec<usize> = (0..inputs.files.len()).collect();
    order.sort_by_key(|&f| std::cmp::Reverse(inputs.sizes[f]));
    let mut reader = scan::Reader::default();
    let (mut ev, mut us, mut pairs) = (Vec::new(), Vec::new(), Vec::new());
    let (mut lines, mut bytes_seen) = (0u64, 0u64);
    for &f in order.iter().take(8) {
        let end = inputs.sizes[f].min(64 * 1024);
        let Ok((bytes, first, stop)) = reader.load(&inputs.files[f], inputs.sizes[f], 0, end) else {
            return None;
        };
        bytes_seen += (stop - first) as u64;
        scan::for_each_record::<7>(bytes, first, stop, |rec| {
            let fields = rec.fields();
            lines += 1;
            let e = fastpath::unmixed(fields[0], b'e');
            let u = fastpath::unmixed(fields[1], b'u');
            if let Some(m) = e {
                ev.push(m);
            }
            if let Some(m) = u {
                us.push(m);
            }
            if let (Some(e), Some(u)) = (e, u) {
                pairs.push((e, u));
            }
        });
    }
    if lines < 64 {
        return None;
    }
    // Bounds come from this input's size, never from a practice set. Any row
    // beyond them simply takes the exact path.
    let est_lines = total / (bytes_seen / lines).max(1) + 1;
    let event_cap = (est_lines * 3 / 2 + Q as u64).div_ceil(Q as u64) * Q as u64;
    // About one generator person per 30 event lines: lines / 15 rounded up to a
    // power of two keeps at least 2x headroom (8M slots at 60M) while the
    // 16-byte slot table stays cache-sized (32 MB at 20M). People beyond the
    // bound take the exact path.
    let person_cap = (est_lines / 15).next_power_of_two().clamp(1 << 16, 1 << 26);
    let keys = fastpath::recover(&ev, &us, &pairs, event_cap, person_cap, est_lines, ncpu)?;
    Some((keys, People::new(keys.person_cap as usize)))
}

/// Person slots from the links: `LINKED` when one component holds exactly the
/// slot's linked IDs (aliases < 32) and nothing else; `COMPLEX` when a
/// component mixes slots, holds an ID that does not decode, or a slot's IDs
/// fall in two components.
fn classify_people(catalog: &Catalog, keys: &Keys, people: &People, ncpu: usize) {
    use std::sync::atomic::AtomicU32;
    const EMPTY: u32 = u32::MAX;
    const BAD: u32 = u32::MAX - 1;
    let n = catalog.link_nodes.len();
    let root_slot: Vec<AtomicU32> = (0..n).map(|_| AtomicU32::new(EMPTY)).collect();
    let mut node_user = vec![u64::MAX; n];
    let per = n.div_ceil(ncpu.max(1)).max(1);
    thread::scope(|scope| {
        for (chunk_idx, users) in node_user.chunks_mut(per).enumerate() {
            let root_slot = &root_slot;
            scope.spawn(move || {
                for (k, slot) in users.iter_mut().enumerate() {
                    let i = chunk_idx * per + k;
                    let mut tmp = [0u8; 10];
                    let bytes = catalog.link_nodes[i].as_bytes(&catalog.link_arena.bytes, &mut tmp);
                    let r = &root_slot[catalog.link_roots[i] as usize];
                    match keys.user_counter_unpadded(bytes) {
                        Some(u) => {
                            *slot = u;
                            let p = (u >> 9) as u32;
                            if u & 511 >= 32 {
                                r.store(BAD, AtomicOrdering::Relaxed);
                            } else if let Err(cur) =
                                r.compare_exchange(EMPTY, p, AtomicOrdering::Relaxed, AtomicOrdering::Relaxed)
                            {
                                if cur != p {
                                    r.store(BAD, AtomicOrdering::Relaxed);
                                }
                            }
                        }
                        None => r.store(BAD, AtomicOrdering::Relaxed),
                    }
                }
            });
        }
    });
    thread::scope(|scope| {
        for (chunk_idx, users) in node_user.chunks(per).enumerate() {
            let root_slot = &root_slot;
            scope.spawn(move || {
                for (k, &u) in users.iter().enumerate() {
                    if u == u64::MAX {
                        continue;
                    }
                    let i = chunk_idx * per + k;
                    let p = (u >> 9) as usize;
                    let root = catalog.link_roots[i];
                    if root_slot[root as usize].load(AtomicOrdering::Relaxed) == BAD {
                        people.complex(p);
                        continue;
                    }
                    if let Err(cur) = people.slots[p].node.compare_exchange(
                        0,
                        root + 1,
                        AtomicOrdering::Relaxed,
                        AtomicOrdering::Relaxed,
                    ) {
                        if cur != root + 1 {
                            people.complex(p);
                            continue;
                        }
                    }
                    people.slots[p].mask.fetch_or(1 << (u & 511), AtomicOrdering::Relaxed);
                    let canon = catalog.canonical_links[i];
                    if canon.is_packed() {
                        people.name[p].store(canon.0, AtomicOrdering::Relaxed);
                    } else {
                        people.complex(p);
                    }
                    let _ = people.slots[p].status.compare_exchange(
                        fastpath::UNLINKED,
                        fastpath::LINKED,
                        AtomicOrdering::Relaxed,
                        AtomicOrdering::Relaxed,
                    );
                }
            });
        }
    });
}

/// Resolve an exact user ID to its person (link component name, or itself)
/// inside `arena`.
#[inline]
fn resolve_person(catalog: &Catalog, user: &[u8], arena: &mut FallbackArena) -> PackedId {
    let mut tmp = FallbackArena::new();
    let uid = tmp.pack_or_push(user);
    match catalog.links.get(uid, &tmp.bytes, &catalog.link_arena.bytes) {
        Some(ci) => arena.import(catalog.canonical_links[ci as usize], &catalog.link_arena.bytes),
        None => arena.import(uid, &tmp.bytes),
    }
}

#[inline]
fn person_shard(id: PackedId, arena: &[u8]) -> usize {
    (id.hash64(arena) as usize) & (NUM_SHARDS - 1)
}

fn run_pipeline(
    data_dir: &Path,
    output_dir: &Path,
    ncpu: usize,
    use_bins: bool,
    assist_sweep: bool,
) -> io::Result<()> {
    let t0 = Instant::now();
    let use_pread = env::var("SHOPPING_PREAD").ok().as_deref() == Some("1");
    ensure_output_dirs(output_dir)?;

    // Stage 1: products and campaigns (links are built during the event scan)
    let mut catalog = build_catalog(data_dir)?;
    let t_cat = t0.elapsed();

    // Stage 2: Scan events/*.csv. Fast-path rows go to counter buckets; the rest
    // are partitioned by event_id into NUM_SHARDS for the exact solver.
    let event_inputs = scan::plan(&data_dir.join("events"), SCAN_CHUNK)?;
    let event_chunks = &event_inputs.chunks;
    let fast = prepare_fast(&catalog, &event_inputs, ncpu);
    let fast_ref = fast.as_ref();
    let n_buckets = fast.as_ref().map_or(0, |(k, _)| (k.event_cap >> QB) as usize);
    let t_fast_prep = t0.elapsed();
    let worker_fast: Mutex<Vec<(usize, Vec<Vec<FastRec>>, FallbackArena)>> = Mutex::new(Vec::new());
    let next_worker_id = AtomicUsize::new(0);
    let event_workers = ncpu.min(event_chunks.len().max(1)).min(fastpath::MAX_WORKERS);
    let next_ev_chunk = AtomicUsize::new(0);
    let worker_ev_shards: Mutex<Vec<Vec<EventBatch>>> =
        Mutex::new(Vec::with_capacity(event_workers));
    let bucket_reserve = fast.as_ref().map_or(0, |(k, _)| {
        (k.est_lines as usize / n_buckets.max(1)) * 13 / 10 / event_workers.max(1) + 64
    });
    let link_result: Mutex<Option<io::Result<LinkPart>>> = Mutex::new(None);

    thread::scope(|outer| {
    // Diagnostic: SHOPPING_SCAN_ONLY=1 measures the event scan alone (no link
    // build, exit right after the scan, no output).
    let scan_only = env::var("SHOPPING_SCAN_ONLY").ok().as_deref() == Some("1");
    if !scan_only {
        outer.spawn(|| {
            *link_result.lock().unwrap() = Some(build_links(data_dir, ncpu));
        });
    }
    thread::scope(|scope| {
        for _ in 0..event_workers {
            scope.spawn(|| {
                let mut shards: Vec<EventBatch> =
                    (0..NUM_SHARDS).map(|_| EventBatch::default()).collect();
                let mut fast_buckets: Vec<Vec<FastRec>> =
                    (0..n_buckets).map(|_| Vec::with_capacity(bucket_reserve)).collect();
                let mut fast_arena = FallbackArena::new();
                let wid = next_worker_id.fetch_add(1, AtomicOrdering::Relaxed) as u64;
                let mut dates = DateCache::default();
                let mut reader = scan::Reader::default();
                loop {
                    let c_idx = next_ev_chunk.fetch_add(1, AtomicOrdering::Relaxed);
                    let Some(&(file_idx, start, end)) = event_chunks.get(c_idx) else {
                        break;
                    };
                    let f = file_idx as usize;
                    let (bytes, first, stop, mapped) = if use_pread {
                        let (b, x, y) = reader
                            .load(&event_inputs.files[f], event_inputs.sizes[f], start, end)
                            .expect("read events");
                        (b, x, y, false)
                    } else {
                        reader
                            .load_mapped(
                                &event_inputs.files[f],
                                event_inputs.sizes[f],
                                event_inputs.maps[f].as_slice(),
                                start,
                                end,
                            )
                            .expect("read events")
                    };
                    scan::for_each_record::<7>(bytes, first, stop, |rec| {
                        let fields = rec.fields();
                        let [
                            event_id,
                            user_id,
                            ts_text,
                            ingest_text,
                            event_type,
                            product_id,
                            price_text,
                        ] = fields;
                        if event_id.is_empty() {
                            return;
                        }
                        let Some(ingest_ts) = parse_f1_cached(ingest_text, &mut dates) else {
                            return;
                        };

                        // Validate the copy now so only compact fields are stored
                        let mut valid = false;
                        let mut ts_val = 0i64;
                        let mut price_val = 0u32;
                        let mut cat_id = catalog.unknown_cat_id;

                        if !user_id.is_empty() && !product_id.is_empty() {
                            // Word compares: exactly `view`, `cart` or `purchase`.
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
                                        if ts >= catalog.window_start
                                            && ts < catalog.window_end
                                            && ingest_ts.saturating_sub(ts) <= LATE_LIMIT_MS
                                        {
                                            valid = true;
                                            ts_val = ts;
                                            // Only purchases use a category; CSV structure and nonempty product
                                            // were checked for every event above.
                                            let prod_pid = if type_code != 2 {
                                                None
                                            } else {
                                                match id::try_pack_padded(product_id) {
                                                    Some(p) => catalog.packed_products.get(p.0),
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

                        if let Some((keys, people)) = fast_ref {
                            let (event_counter, user_counter) = keys.pair_counters(event_id, user_id);
                            if let Some(counter) = event_counter {
                                let user = if !valid {
                                    fastpath::INVALID
                                } else if let Some(u) = user_counter {
                                    u
                                } else {
                                    let off = fast_arena.push_fallback(user_id).0 & !(1u64 << 63);
                                    assert!(off <= fastpath::M40, "opaque arena over 1 TiB");
                                    fastpath::OPAQUE | (wid << 40) | off
                                };
                                fast_buckets[(counter >> QB) as usize].push(FastRec::new(
                                    ingest_ts,
                                    counter,
                                    user,
                                    ts_val.wrapping_sub(catalog.window_start) as u64,
                                    price_val,
                                    cat_id,
                                ));
                                return;
                            }
                            // An exact-path row naming a fast-shaped user makes that person exact.
                            if valid {
                                if let Some(u) = user_counter {
                                    people.complex((u >> 9) as usize);
                                }
                            }
                        }

                        // Hash event_id to choose shard
                        let eid_tmp = id::try_pack_padded(event_id);
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
                        let packed_uid = if valid {
                            batch.arena.pack_or_push_padded(user_id)
                        } else {
                            PackedId(0)
                        };
                        batch.events.push(RawEvent {
                            event_id: packed_eid,
                            ingest_ts,
                            user_id: packed_uid,
                            ts: ts_val,
                            price: price_val,
                            cat_id,
                        });
                    });
                    if mapped {
                        scan::unmap_pages(bytes, first, stop);
                    }
                }
                worker_ev_shards.lock().unwrap().push(shards);
                if fast_ref.is_some() {
                    worker_fast.lock().unwrap().push((wid as usize, fast_buckets, fast_arena));
                }
            });
        }
    });
    });
    let t_ev_scan = t0.elapsed();
    if env::var("SHOPPING_SCAN_ONLY").ok().as_deref() == Some("1") {
        eprintln!("scan_only_ms={:.1}", (t_ev_scan - t_fast_prep).as_secs_f64() * 1000.0);
        unsafe { _exit(0) }
    }
    catalog.install_links(link_result.into_inner().unwrap().expect("link builder ran")?);
    if let Some((keys, people)) = fast_ref {
        classify_people(&catalog, keys, people, ncpu);
    }
    let t_links = t0.elapsed();
    let mut all_ev_shards = worker_ev_shards.into_inner().unwrap();

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

    // Stage 3b: fast buckets. Every copy of an event shares its counter, so a
    // direct array per bucket deduplicates exactly (earliest ingest wins, invalid
    // winners are dropped). Each surviving event claims its person for this bucket;
    // a second bucket, or an alias outside the person's component, makes it complex.
    let mut fast_parts = worker_fast.into_inner().unwrap();
    fast_parts.sort_by_key(|w| w.0);
    // Transpose to one list of per-thread row vectors per bucket, so C1 can
    // free a bucket's raw rows as soon as its winners are extracted.
    let mut by_bucket: Vec<Vec<Vec<FastRec>>> = (0..n_buckets).map(|_| Vec::new()).collect();
    let mut fast_workers: Vec<FallbackArena> = Vec::new();
    for (_, buckets, arena) in fast_parts {
        for (b, rows) in buckets.into_iter().enumerate() {
            if !rows.is_empty() {
                by_bucket[b].push(rows);
            }
        }
        fast_workers.push(arena);
    }
    let by_bucket: Vec<Mutex<Vec<Vec<FastRec>>>> = by_bucket.into_iter().map(Mutex::new).collect();
    // Claims first, from every valid row of each bucket (a superset of the
    // winners, so it can only mark more people complex): the alias check and
    // a claim of the bucket, with a small per-bucket filter so each user is
    // checked and claimed about once. Dedup and sessions then run in one pass
    // per bucket after this barrier, without materialising winners.
    if let Some((_, people)) = fast_ref {
        let next = AtomicUsize::new(0);
        thread::scope(|scope| {
            for _ in 0..ncpu {
                scope.spawn(|| {
                    let mut seen = vec![u64::MAX; 4096];
                    loop {
                        let b = next.fetch_add(1, AtomicOrdering::Relaxed);
                        if b >= n_buckets {
                            break;
                        }
                        let tag = b as u32 + 1;
                        let parts = by_bucket[b].lock().unwrap();
                        for part in parts.iter() {
                            for r in part {
                                if r.is_invalid() || r.is_opaque() {
                                    continue;
                                }
                                let u = r.counter();
                                let key = ((b as u64) << 40) | u;
                                let idx = (((u >> 9) ^ u) & 4095) as usize;
                                if seen[idx] == key {
                                    continue;
                                }
                                seen[idx] = key;
                                if people.check_alias(u) {
                                    let p = (u >> 9) as usize;
                                    if let Err(cur) = people.slots[p].claim.compare_exchange(
                                        0,
                                        tag,
                                        AtomicOrdering::Relaxed,
                                        AtomicOrdering::Relaxed,
                                    ) {
                                        if cur != tag {
                                            people.complex(p);
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
        });
    }
    let t_fast_dedup = t0.elapsed();

    // Stage 4: Scan clicks/*.csv, validate, resolve person_id, and partition by person_id into NUM_SHARDS
    let click_inputs = scan::plan(&data_dir.join("clicks"), SCAN_CHUNK)?;
    let click_chunks = &click_inputs.chunks;
    let click_workers = ncpu.min(click_chunks.len().max(1));
    let next_cl_chunk = AtomicUsize::new(0);
    let person_cl_partials: Mutex<Vec<Vec<ClickBatch>>> =
        Mutex::new(Vec::with_capacity(click_workers));
    let worker_fast_clicks: Mutex<Vec<Vec<Vec<FastClick>>>> = Mutex::new(Vec::new());
    let n_groups = fast.as_ref().map_or(0, |(k, _)| (k.person_cap >> GROUP_BITS) as usize + 1);
    let click_win_start = catalog.window_start - CLICK_LOOKBACK_MS;
    let click_win_end = catalog.window_end;

    thread::scope(|scope| {
        for _ in 0..click_workers {
            scope.spawn(|| {
                let mut shards: Vec<ClickBatch> =
                    (0..NUM_SHARDS).map(|_| ClickBatch::default()).collect();
                let mut click_groups: Vec<Vec<FastClick>> = (0..n_groups).map(|_| Vec::new()).collect();
                let mut dates = DateCache::default();
                let mut tmp_arena = FallbackArena::new();
                let mut reader = scan::Reader::default();
                loop {
                    let c_idx = next_cl_chunk.fetch_add(1, AtomicOrdering::Relaxed);
                    let Some(&(file_idx, start, end)) = click_chunks.get(c_idx) else {
                        break;
                    };
                    let f = file_idx as usize;
                    let (bytes, first, stop, mapped) = if use_pread {
                        let (b, x, y) = reader
                            .load(&click_inputs.files[f], click_inputs.sizes[f], start, end)
                            .expect("read clicks");
                        (b, x, y, false)
                    } else {
                        reader
                            .load_mapped(
                                &click_inputs.files[f],
                                click_inputs.sizes[f],
                                click_inputs.maps[f].as_slice(),
                                start,
                                end,
                            )
                            .expect("read clicks")
                    };
                    scan::for_each_record::<4>(bytes, first, stop, |rec| {
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

                        // A click naming a fast-shaped user is filed by person group; the
                        // routing pass below decides fast or exact with group-local lookups.
                        if let Some((keys, people)) = fast_ref {
                            if let Some(u) = keys.user_counter(user_id) {
                                match id::try_pack_padded(click_id) {
                                    Some(cid) => {
                                        click_groups[(u >> (9 + GROUP_BITS)) as usize].push(FastClick {
                                            ts,
                                            click_id: cid.0,
                                            user: u,
                                            campaign: camp_idx as u32,
                                        });
                                        return;
                                    }
                                    None => people.complex((u >> 9) as usize),
                                }
                            }
                        }

                        tmp_arena.bytes.clear();
                        let uid_pid = tmp_arena.pack_or_push_padded(user_id);
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
                        let stored_cid = batch.arena.pack_or_push_padded(click_id);
                        batch.clicks.push(PersonClick {
                            person_id: stored_pid,
                            ts,
                            click_id: stored_cid,
                            campaign_idx: camp_idx as u32,
                        });
                    });
                    if mapped {
                        scan::unmap_pages(bytes, first, stop);
                    }
                }
                person_cl_partials.lock().unwrap().push(shards);
                if fast_ref.is_some() {
                    worker_fast_clicks.lock().unwrap().push(click_groups);
                }
            });
        }
    });
    let mut all_person_cl = person_cl_partials.into_inner().unwrap();
    let t_cl_scan = t0.elapsed();
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

    // Campaign daily accumulators: [campaign_idx * window_days + day_offset],
    // plus a map for any day outside 0..window_days.

    let global_camp_grid: Mutex<Vec<CampAcc>> =
        Mutex::new(vec![CampAcc::default(); num_campaigns * window_days]);
    let global_camp_extra: Mutex<HashMap<(u32, i32), CampAcc>> = Mutex::new(HashMap::new());
    let io_error: Mutex<Option<io::Error>> = Mutex::new(None);
    let total_user_rows = AtomicUsize::new(0);


    // Stage 4b: sessions for verified people inside their bucket. A complex
    // person's events and clicks become exact-solver input instead.
    let click_group_parts: Vec<Vec<Vec<FastClick>>> = worker_fast_clicks.into_inner().unwrap();
    // Route filed clicks: a click whose alias is in its person's component and
    // whose person claimed a bucket (and is not complex) goes to that bucket;
    // every other click goes to the exact solver.
    let routed: Mutex<Vec<Vec<Vec<FastClick>>>> = Mutex::new(Vec::new());
    if let Some((keys, people)) = fast_ref {
        let next = AtomicUsize::new(0);
        let exact_cl: Mutex<Vec<Vec<ClickBatch>>> = Mutex::new(Vec::new());
        thread::scope(|scope| {
            for _ in 0..ncpu {
                scope.spawn(|| {
                    let mut to_bucket: Vec<Vec<FastClick>> = (0..n_buckets).map(|_| Vec::new()).collect();
                    let mut cl_out: Vec<ClickBatch> =
                        (0..NUM_SHARDS).map(|_| ClickBatch::default()).collect();
                    let mut scratch = FallbackArena::new();
                    let mut idbuf: Vec<u8> = Vec::new();
                    loop {
                        let g = next.fetch_add(1, AtomicOrdering::Relaxed);
                        if g >= n_groups {
                            break;
                        }
                        for part in &click_group_parts {
                            for c in &part[g] {
                                let p = (c.user >> 9) as usize;
                                let ok = people.check_alias(c.user);
                                let claim = people.slots[p].claim.load(AtomicOrdering::Relaxed);
                                if ok && claim != 0 && !people.is_complex(p) {
                                    to_bucket[(claim - 1) as usize].push(*c);
                                } else {
                                    keys.user_id(c.user, &mut idbuf);
                                    scratch.bytes.clear();
                                    let pid = resolve_person(&catalog, &idbuf, &mut scratch);
                                    let out = &mut cl_out[person_shard(pid, &scratch.bytes)];
                                    let pid = out.arena.import(pid, &scratch.bytes);
                                    out.clicks.push(PersonClick {
                                        person_id: pid,
                                        ts: c.ts,
                                        click_id: PackedId(c.click_id),
                                        campaign_idx: c.campaign,
                                    });
                                }
                            }
                        }
                    }
                    routed.lock().unwrap().push(to_bucket);
                    exact_cl.lock().unwrap().push(cl_out);
                });
            }
        });
        all_person_cl.extend(exact_cl.into_inner().unwrap());
    }
    std::mem::forget(click_group_parts);
    let fast_click_workers: Vec<Vec<Vec<FastClick>>> = routed.into_inner().unwrap();
    let t_click_route = t0.elapsed();
    let fast_people = AtomicUsize::new(0);
    if let Some((keys, people)) = fast_ref {
        let next = AtomicUsize::new(0);
        let exact_ev: Mutex<Vec<Vec<PersonEventBatch>>> = Mutex::new(Vec::new());
        let exact_cl: Mutex<Vec<Vec<ClickBatch>>> = Mutex::new(Vec::new());
        thread::scope(|scope| {
            for worker_id in 0..ncpu {
                let (next, exact_ev, exact_cl) = (&next, &exact_ev, &exact_cl);
                let (fast_workers, fast_click_workers, by_bucket) =
                    (&fast_workers, &fast_click_workers, &by_bucket);
                let (global_camp_grid, global_camp_extra, io_error, total_user_rows, fast_people) =
                    (&global_camp_grid, &global_camp_extra, &io_error, &total_user_rows, &fast_people);
                let catalog = &catalog;
                scope.spawn(move || {
                    let mut sink = Sink::new(
                        num_campaigns,
                        num_categories,
                        window_days,
                        base_day,
                        window_start,
                        catalog.window_end,
                        use_bins,
                        assist_sweep,
                    );
                    let mut ev_out: Vec<PersonEventBatch> =
                        (0..NUM_SHARDS).map(|_| PersonEventBatch::default()).collect();
                    let mut cl_out: Vec<ClickBatch> =
                        (0..NUM_SHARDS).map(|_| ClickBatch::default()).collect();
                    let mut scratch = FallbackArena::new();
                    let mut idbuf: Vec<u8> = Vec::new();
                    let mut fast_evs: Vec<(u32, i64, u32, u32)> = Vec::new();
                    let mut fast_cls: Vec<(u32, i64, u64, u32)> = Vec::new();
                    // Each person's events and clicks end up contiguous in these
                    // arrays and go to the sink as slices, without per-person copies.
                    let mut sp: Vec<u32> = Vec::new();
                    let mut se: Vec<(i64, u32, u32)> = Vec::new();
                    let mut scp: Vec<u32> = Vec::new();
                    let mut sc: Vec<(i64, PackedId, u32)> = Vec::new();
                    let mut counts: Vec<u32> = Vec::new();
                    // (person, first index into `se`) per person run, in person order.
                    let mut runs: Vec<(u32, u32)> = Vec::new();
                    let mut served = 0usize;
                    let mut tsub = [0u128; 5];
                    let blank = FastRec { key: 0, user: 0, time_price: 0 };
                    let mut slot = vec![blank; Q];
                    let mut gen_of = vec![0u32; Q];
                    let mut stamp = 0u32;

                    let mut exact_event = |user: &[u8], ts: i64, price: u32, cat: u32, scratch: &mut FallbackArena| {
                        scratch.bytes.clear();
                        let pid = resolve_person(catalog, user, scratch);
                        let out = &mut ev_out[person_shard(pid, &scratch.bytes)];
                        let pid = out.arena.import(pid, &scratch.bytes);
                        out.events.push(PersonEvent { person_id: pid, ts, price, cat_id: cat });
                    };
                    let mut exact_click = |user: &[u8], c: (i64, u64, u32), scratch: &mut FallbackArena| {
                        scratch.bytes.clear();
                        let pid = resolve_person(catalog, user, scratch);
                        let out = &mut cl_out[person_shard(pid, &scratch.bytes)];
                        let pid = out.arena.import(pid, &scratch.bytes);
                        out.clicks.push(PersonClick {
                            person_id: pid,
                            ts: c.0,
                            click_id: PackedId(c.1),
                            campaign_idx: c.2,
                        });
                    };

                    loop {
                        let b = next.fetch_add(1, AtomicOrdering::Relaxed);
                        if b >= n_buckets {
                            break;
                        }
                        // Dedup: every copy of an event shares its counter, so a
                        // direct array per bucket keeps the earliest ingest exactly;
                        // winners then come out in counter order, which for generator
                        // people is time order, so the sink's sort is a single pass.
                        stamp += 1;
                        let q0 = std::time::Instant::now();
                        let parts = std::mem::take(&mut *by_bucket[b].lock().unwrap());
                        let mut rows = 0usize;
                        for part in &parts {
                            rows += part.len();
                            for r in part {
                                let s = r.low();
                                if gen_of[s] != stamp {
                                    gen_of[s] = stamp;
                                    slot[s] = *r;
                                } else if r.ingest() < slot[s].ingest() {
                                    slot[s] = *r;
                                }
                            }
                        }
                        for part in parts {
                            release(part);
                        }
                        let q1 = std::time::Instant::now();
                        tsub[0] += (q1 - q0).as_nanos();
                        fast_cls.clear();
                        se.clear();
                        runs.clear();
                        // Generator people own contiguous counter ranges in person
                        // order, so winners arrive grouped by person with person
                        // indices rising: runs come straight from this walk. Any
                        // bucket where an index falls is regrouped by sorting below.
                        let mut cur_p = u32::MAX;
                        let mut rising = true;
                        for s in 0..if rows > 0 { Q } else { 0 } {
                            if gen_of[s] != stamp || slot[s].is_invalid() {
                                continue;
                            }
                            let r = &slot[s];
                            let ts = window_start + r.ts_off();
                            if r.is_opaque() {
                                let (t, off) = r.opaque_ref();
                                let arena = &fast_workers[t].bytes;
                                let id = PackedId::from_fallback(off);
                                let (o, l) = id.fallback_span(arena);
                                exact_event(&arena[o..o + l], ts, r.price(), r.cat(), &mut scratch);
                            } else if people.is_complex((r.counter() >> 9) as usize) {
                                keys.user_id(r.counter(), &mut idbuf);
                                exact_event(&idbuf, ts, r.price(), r.cat(), &mut scratch);
                            } else {
                                let p = (r.counter() >> 9) as u32;
                                if p != cur_p {
                                    rising &= cur_p == u32::MAX || p > cur_p;
                                    runs.push((p, se.len() as u32));
                                    cur_p = p;
                                }
                                se.push((ts, r.price(), r.cat()));
                            }
                        }
                        let q2 = std::time::Instant::now();
                        tsub[1] += (q2 - q1).as_nanos();
                        for clicks in fast_click_workers.iter() {
                            for c in &clicks[b] {
                                if people.is_complex((c.user >> 9) as usize) {
                                    keys.user_id(c.user, &mut idbuf);
                                    exact_click(&idbuf, (c.ts, c.click_id, c.campaign), &mut scratch);
                                } else {
                                    fast_cls.push(((c.user >> 9) as u32, c.ts, c.click_id, c.campaign));
                                }
                            }
                        }
                        if se.is_empty() {
                            for &(p, ts, cid, camp) in &fast_cls {
                                keys.user_id((p as u64) << 9, &mut idbuf);
                                exact_click(&idbuf, (ts, cid, camp), &mut scratch);
                            }
                            continue;
                        }
                        if !rising {
                            // Group by person: counting sort over the bucket's person range.
                            fast_evs.clear();
                            for k in 0..runs.len() {
                                let (p, at) = runs[k];
                                let end = runs.get(k + 1).map_or(se.len(), |r| r.1 as usize);
                                fast_evs.extend(se[at as usize..end].iter().map(|e| (p, e.0, e.1, e.2)));
                            }
                            let lo = runs.iter().map(|r| r.0).min().unwrap() as usize;
                            let hi = runs.iter().map(|r| r.0).max().unwrap() as usize;
                            let span = hi - lo + 1;
                            sp.clear();
                            se.clear();
                            if span <= 8 * Q {
                                counts.clear();
                                counts.resize(span + 1, 0);
                                for e in &fast_evs {
                                    counts[e.0 as usize - lo + 1] += 1;
                                }
                                for i in 1..=span {
                                    counts[i] += counts[i - 1];
                                }
                                sp.resize(fast_evs.len(), 0);
                                se.resize(fast_evs.len(), (0, 0, 0));
                                for e in &fast_evs {
                                    let k = e.0 as usize - lo;
                                    let at = counts[k] as usize;
                                    sp[at] = e.0;
                                    se[at] = (e.1, e.2, e.3);
                                    counts[k] += 1;
                                }
                            } else {
                                fast_evs.sort_unstable_by_key(|e| e.0);
                                sp.extend(fast_evs.iter().map(|e| e.0));
                                se.extend(fast_evs.iter().map(|e| (e.1, e.2, e.3)));
                            }
                            runs.clear();
                            for (i, &p) in sp.iter().enumerate() {
                                if runs.last().is_none_or(|r| r.0 != p) {
                                    runs.push((p, i as u32));
                                }
                            }
                        }
                        // Runs are now in strictly rising person order.
                        let lo = runs[0].0 as usize;
                        let hi = runs[runs.len() - 1].0 as usize;
                        let span = hi - lo + 1;
                        // Clicks by person: counting sort over the same person range;
                        // a click outside it has no fast event here and goes exact below.
                        scp.clear();
                        sc.clear();
                        if span <= 8 * Q {
                            counts.clear();
                            counts.resize(span + 1, 0);
                            let mut inside = 0usize;
                            for c in &fast_cls {
                                let p = c.0 as usize;
                                if p >= lo && p <= hi {
                                    counts[p - lo + 1] += 1;
                                    inside += 1;
                                }
                            }
                            for i in 1..=span {
                                counts[i] += counts[i - 1];
                            }
                            scp.resize(inside, 0);
                            sc.resize(inside, (0, PackedId(0), 0));
                            for c in &fast_cls {
                                let p = c.0 as usize;
                                if p >= lo && p <= hi {
                                    let at = counts[p - lo] as usize;
                                    scp[at] = c.0;
                                    sc[at] = (c.1, PackedId(c.2), c.3);
                                    counts[p - lo] += 1;
                                } else {
                                    keys.user_id((c.0 as u64) << 9, &mut idbuf);
                                    exact_click(&idbuf, (c.1, c.2, c.3), &mut scratch);
                                }
                            }
                        } else {
                            fast_cls.sort_unstable_by_key(|c| c.0);
                            scp.extend(fast_cls.iter().map(|c| c.0));
                            sc.extend(fast_cls.iter().map(|c| (c.1, PackedId(c.2), c.3)));
                        }
                        let q3 = std::time::Instant::now();
                        tsub[2] += (q3 - q2).as_nanos();
                        let mut ci = 0usize;
                        for k in 0..runs.len() {
                            let (p, i) = (runs[k].0, runs[k].1 as usize);
                            let j = runs.get(k + 1).map_or(se.len(), |r| r.1 as usize);
                            // Clicks of people with no fast event here cannot be claimed
                            // by this bucket; route any such click exactly anyway.
                            while ci < scp.len() && scp[ci] < p {
                                let (ts, cid, camp) = sc[ci];
                                keys.user_id((scp[ci] as u64) << 9, &mut idbuf);
                                exact_click(&idbuf, (ts, cid.0, camp), &mut scratch);
                                ci += 1;
                            }
                            let mut cj = ci;
                            while cj < scp.len() && scp[cj] == p {
                                cj += 1;
                            }
                            let pu = p as usize;
                            if people.slots[pu].status.load(AtomicOrdering::Relaxed) == fastpath::LINKED {
                                // The component's byte-wise smallest ID (packed: a linked
                                // person's IDs all decode, so they all pack).
                                let mut tmp = [0u8; 10];
                                let name = PackedId(people.name[pu].load(AtomicOrdering::Relaxed));
                                let bytes = id::unpack(name, &mut tmp);
                                idbuf.clear();
                                idbuf.extend_from_slice(bytes);
                            } else {
                                keys.user_id((p as u64) << 9, &mut idbuf);
                            }
                            // Fast click IDs are all packed, so (ts asc, ID desc) is a
                            // plain key order; the sink then skips its comparator sort.
                            sc[ci..cj].sort_unstable_by_key(|c| (c.0, std::cmp::Reverse(c.1.0)));
                            sink.person(&idbuf, &mut se[i..j], &mut sc[ci..cj], &[], true);
                            served += 1;
                            ci = cj;
                        }
                        while ci < scp.len() {
                            let (ts, cid, camp) = sc[ci];
                            keys.user_id((scp[ci] as u64) << 9, &mut idbuf);
                            exact_click(&idbuf, (ts, cid.0, camp), &mut scratch);
                            ci += 1;
                        }
                        tsub[3] += q3.elapsed().as_nanos();
                    }
                    fast_people.fetch_add(served, AtomicOrdering::Relaxed);
                    let q4 = std::time::Instant::now();

                    let ucols = &sink.ucols;
                    if ucols.len() > 0 {
                        total_user_rows.fetch_add(ucols.len(), AtomicOrdering::Relaxed);
                        let path = output_dir
                            .join("user_daily")
                            .join(format!("part-f{worker_id:02}.parquet"));
                        if let Err(e) = write_user_daily_cols(&path, ucols, &catalog.categories) {
                            *io_error.lock().unwrap() = Some(e);
                        }
                    }
                    {
                        let mut g_grid = global_camp_grid.lock().unwrap();
                        for (g, l) in g_grid.iter_mut().zip(sink.camp_grid.iter()) {
                            g.sessions += l.sessions;
                            g.purchasing_sessions += l.purchasing_sessions;
                            g.revenue_cents += l.revenue_cents;
                            g.assisted_sessions += l.assisted_sessions;
                        }
                    }
                    if !sink.camp_extra.is_empty() {
                        let mut g_extra = global_camp_extra.lock().unwrap();
                        for (k, l) in sink.camp_extra.drain() {
                            let g = g_extra.entry(k).or_default();
                            g.sessions += l.sessions;
                            g.purchasing_sessions += l.purchasing_sessions;
                            g.revenue_cents += l.revenue_cents;
                            g.assisted_sessions += l.assisted_sessions;
                        }
                    }
                    tsub[4] += q4.elapsed().as_nanos();
                    if std::env::var_os("SHOPPING_TIMING").is_some() {
                        eprintln!("sub w{worker_id} dedup={} walk={} clicks={} sink={} flush={}", tsub[0] / 1_000_000, tsub[1] / 1_000_000, tsub[2] / 1_000_000, tsub[3] / 1_000_000, tsub[4] / 1_000_000);
                    }
                    exact_ev.lock().unwrap().push(ev_out);
                    exact_cl.lock().unwrap().push(cl_out);
                });
            }
        });
        all_person_ev.extend(exact_ev.into_inner().unwrap());
        all_person_cl.extend(exact_cl.into_inner().unwrap());
    }
    std::mem::forget(fast_workers);
    std::mem::forget(fast_click_workers);
    let t_fast_sessions = t0.elapsed();


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
                let mut sink = Sink::new(
                    num_campaigns,
                    num_categories,
                    window_days,
                    base_day,
                    window_start,
                    catalog.window_end,
                    use_bins,
                    assist_sweep,
                );
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

                    for (p_idx, evs) in person_events.iter_mut().enumerate() {
                        let mut tmp = [0u8; 10];
                        let name = person_ids[p_idx].as_bytes(&shard_arena.bytes, &mut tmp);
                        sink.person(name, evs, &mut person_clicks[p_idx], &shard_arena.bytes, false);
                    }
                }

                // Write this worker's user_daily parquet file if non-empty
                let ucols = &sink.ucols;
                if ucols.len() > 0 {
                    total_user_rows.fetch_add(ucols.len(), AtomicOrdering::Relaxed);
                    let path = output_dir
                        .join("user_daily")
                        .join(format!("part-{worker_id:02}.parquet"));
                    if let Err(e) = write_user_daily_cols(&path, ucols, &catalog.categories) {
                        *io_error.lock().unwrap() = Some(e);
                    }
                }

                // Merge local campaign accumulators into global
                {
                    let mut g_grid = global_camp_grid.lock().unwrap();
                    for (g, l) in g_grid.iter_mut().zip(sink.camp_grid.iter()) {
                        g.sessions += l.sessions;
                        g.purchasing_sessions += l.purchasing_sessions;
                        g.revenue_cents += l.revenue_cents;
                        g.assisted_sessions += l.assisted_sessions;
                    }
                }
                if !sink.camp_extra.is_empty() {
                    let mut g_extra = global_camp_extra.lock().unwrap();
                    for (k, l) in sink.camp_extra.drain() {
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
    // The process ends with `_exit`, so the large buffers are never freed.
    std::mem::forget(pev_by_shard);
    std::mem::forget(pcl_by_shard);
    if let Some(err) = io_error.into_inner().unwrap() {
        return Err(err);
    }

    // If no worker wrote any user_daily rows, write an empty part-00.parquet
    if total_user_rows.load(AtomicOrdering::Relaxed) == 0 {
        write_user_daily_cols(
            &output_dir.join("user_daily").join("part-00.parquet"),
            &UserCols::default(),
            &catalog.categories,
        )?;
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
    std::mem::forget(catalog);
    let t_total = t0.elapsed();
    if env::var("SHOPPING_TIMING").is_ok() {
        eprintln!(
            "scanner={} build={}",
            parse::scanner_name(),
            if cfg!(target_feature = "avx512bw") {
                "v4"
            } else if cfg!(target_feature = "avx2") {
                "v3"
            } else {
                "portable"
            }
        );
        eprintln!(
            "timing_ms: catalog={:.1} fast_prep={:.1} ev_scan_and_links={:.1} classify={:.1} ev_dedup={:.1} fast_dedup={:.1} cl_scan={:.1} click_route={:.1} fast_sessions={:.1} sessions_and_user_write={:.1} cleanup_and_campaign_write={:.1} total={:.1} fast={} fast_people={}",
            t_cat.as_secs_f64() * 1000.0,
            (t_fast_prep - t_cat).as_secs_f64() * 1000.0,
            (t_ev_scan - t_fast_prep).as_secs_f64() * 1000.0,
            (t_links - t_ev_scan).as_secs_f64() * 1000.0,
            (t_ev_dedup - t_links).as_secs_f64() * 1000.0,
            (t_fast_dedup - t_ev_dedup).as_secs_f64() * 1000.0,
            (t_cl_scan - t_fast_dedup).as_secs_f64() * 1000.0,
            (t_click_route - t_cl_scan).as_secs_f64() * 1000.0,
            (t_fast_sessions - t_click_route).as_secs_f64() * 1000.0,
            (t_sessions - t_fast_sessions).as_secs_f64() * 1000.0,
            (t_total - t_sessions).as_secs_f64() * 1000.0,
            t_total.as_secs_f64() * 1000.0,
            fast.as_ref().map_or("off".to_string(), |(k, _)| format!(
                "ekey={:#x},ukey={:#x},ecap={},pcap={}",
                k.event_key, k.user_key, k.event_cap, k.person_cap
            )),
            fast_people.load(AtomicOrdering::Relaxed),
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
