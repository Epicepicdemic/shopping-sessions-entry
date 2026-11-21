//! Partition exact link edges by decoded slot. Only a proven isolated, connected
//! component can replace the global exact ID map with a name and alias bitset.

use super::*;

const SLOT_BITS: u32 = 12;

type ExactPairs = (FallbackArena, Vec<(PackedId, PackedId)>);

fn push_exact(out: &mut ExactPairs, a: &[u8], b: &[u8]) {
    let a = out.0.pack_or_push(a);
    let b = out.0.pack_or_push(b);
    out.1.push((a, b));
}

fn root(parent: &mut [u8; 32], mut n: usize) -> usize {
    while parent[n] as usize != n {
        parent[n] = parent[parent[n] as usize];
        n = parent[n] as usize;
    }
    n
}

fn connect(edges: &[u64]) -> Option<u32> {
    let mut parent = std::array::from_fn::<_, 32, _>(|n| n as u8);
    let mut aliases = 0u32;
    let mut components = 0u32;
    for &edge in edges {
        let a = (edge as usize >> 5) & 31;
        let b = edge as usize & 31;
        let pair = (1u32 << a) | (1u32 << b);
        components += (pair & !aliases).count_ones();
        aliases |= pair;
        let ra = root(&mut parent, a);
        let rb = root(&mut parent, b);
        if ra != rb {
            parent[rb] = ra as u8;
            components -= 1;
        }
    }
    (components == 1).then_some(aliases)
}

pub(super) struct LinkMetrics {
    pub parse: f64,
    pub components: f64,
    pub sparse: f64,
    pub local_slots: usize,
    pub external_edges: usize,
}

pub(super) fn build(
    data_dir: &Path,
    ncpu: usize,
    use_pread: bool,
    keys: &Keys,
    people: &People,
) -> io::Result<(LinkPart, LinkMetrics)> {
    let time = Instant::now();
    let inputs = scan::plan(&data_dir.join("links"), SCAN_CHUNK)?;
    let groups = people.slots.len().div_ceil(1 << SLOT_BITS);
    let readers = ncpu.min(2).min(inputs.chunks.len().max(1));
    let next = AtomicUsize::new(0);
    let scan_parts = thread::scope(|scope| {
        let mut jobs = Vec::with_capacity(readers);
        for _ in 0..readers {
            jobs.push(scope.spawn(|| -> io::Result<_> {
                let mut local = vec![Vec::new(); groups];
                let mut external = (FallbackArena::new(), Vec::new());
                let mut taint = Vec::new();
                let mut reader = scan::Reader::default();
                while let Some(&(file, start, end)) =
                    inputs.chunks.get(next.fetch_add(1, AtomicOrdering::Relaxed))
                {
                    let f = file as usize;
                    let (bytes, first, stop, mapped) = if use_pread {
                        let (bytes, first, stop) =
                            reader.load(&inputs.files[f], inputs.sizes[f], start, end)?;
                        (bytes, first, stop, false)
                    } else {
                        reader.load_mapped(
                            &inputs.files[f],
                            inputs.sizes[f],
                            inputs.maps[f].as_slice(),
                            start,
                            end,
                        )?
                    };
                    scan::for_each_record::<2>(bytes, first, stop, |rec| {
                        let [a, b] = rec.fields();
                        if a.is_empty() || b.is_empty() {
                            return;
                        }
                        let ua = keys.user_counter(a);
                        let ub = keys.user_counter(b);
                        if let (Some(ua), Some(ub)) = (ua, ub) {
                            let slot = ua >> 9;
                            if slot == ub >> 9 && (ua & 511) < 32 && (ub & 511) < 32 {
                                let edge = (slot << 10) | ((ua & 31) << 5) | (ub & 31);
                                local[slot as usize >> SLOT_BITS].push(edge);
                                return;
                            }
                        }
                        push_exact(&mut external, a, b);
                        taint.extend(ua.into_iter().chain(ub).map(|u| u >> 9));
                    });
                    if mapped {
                        scan::unmap_pages(bytes, first, stop);
                    }
                }
                Ok((local, external, taint))
            }));
        }
        jobs.into_iter().map(|job| job.join().unwrap()).collect::<io::Result<Vec<_>>>()
    })?;
    let parsed = time.elapsed();

    let mut external_edges = 0;
    let mut exact_parts = Vec::new();
    let mut tainted = vec![0u64; people.slots.len().div_ceil(64)];
    let mut partitions = (0..groups).map(|_| Vec::new()).collect::<Vec<_>>();
    for (local, external, taint) in scan_parts {
        external_edges += external.1.len();
        exact_parts.push(external);
        for p in taint {
            let p = p as usize;
            tainted[p >> 6] |= 1 << (p & 63);
            people.complex(p);
        }
        for (combined, part) in partitions.iter_mut().zip(local) {
            combined.push(part);
        }
    }

    let partitions: Vec<_> = partitions.into_iter().map(Mutex::new).collect();
    let next = AtomicUsize::new(0);
    let mut local_slots = 0;
    thread::scope(|scope| {
        let mut jobs = Vec::new();
        for _ in 0..ncpu.min(4).min(groups) {
            let (partitions, next, tainted) = (&partitions, &next, &tainted);
            jobs.push(scope.spawn(move || {
                let mut local_slots = 0;
                let mut exact = (FallbackArena::new(), Vec::new());
                let mut sorted = Vec::new();
                let (mut a, mut b) = (Vec::with_capacity(10), Vec::with_capacity(10));
                while let Some(part) = partitions.get(next.fetch_add(1, AtomicOrdering::Relaxed)) {
                    sorted.clear();
                    for mut edges in std::mem::take(&mut *part.lock().unwrap()) {
                        sorted.append(&mut edges);
                    }
                    sorted.sort_unstable();
                    for edges in sorted.chunk_by(|a, b| a >> 10 == b >> 10) {
                        let p = (edges[0] >> 10) as usize;
                        let slot = &people.slots[p];
                        let safe = tainted[p >> 6] & (1 << (p & 63)) == 0;
                        if let Some(mask) = safe.then(|| connect(edges)).flatten() {
                            let mut name = u64::MAX;
                            let mut remaining = mask;
                            while remaining != 0 {
                                let alias = remaining.trailing_zeros() as u64;
                                remaining &= remaining - 1;
                                keys.user_id(((p as u64) << 9) | alias, &mut a);
                                // Canonical generator-shaped IDs are all packable. Packed
                                // integer order is identical to byte order for those IDs.
                                name = name.min(id::try_pack(&a).unwrap().0);
                            }
                            people.name[p].store(name, AtomicOrdering::Relaxed);
                            slot.mask.store(mask, AtomicOrdering::Relaxed);
                            // Exact event rows may independently mark the slot complex.
                            // Their store wins regardless of execution order.
                            let _ = slot.status.compare_exchange(
                                fastpath::UNLINKED, fastpath::LINKED,
                                AtomicOrdering::Relaxed, AtomicOrdering::Relaxed,
                            );
                            local_slots += 1;
                        } else {
                            people.complex(p);
                            for &edge in edges {
                                let base = (p as u64) << 9;
                                keys.user_id(base | ((edge >> 5) & 31), &mut a);
                                keys.user_id(base | (edge & 31), &mut b);
                                push_exact(&mut exact, &a, &b);
                            }
                        }
                    }
                }
                (local_slots, exact)
            }));
        }
        for job in jobs {
            let (count, exact) = job.join().unwrap();
            local_slots += count;
            exact_parts.push(exact);
        }
    });
    let components = time.elapsed();
    let exact = exact_link_components(exact_parts);
    let finished = time.elapsed();
    Ok((exact, LinkMetrics {
        parse: parsed.as_secs_f64() * 1000.0,
        components: (components - parsed).as_secs_f64() * 1000.0,
        sparse: (finished - components).as_secs_f64() * 1000.0,
        local_slots,
        external_edges,
    }))
}

#[cfg(test)]
mod tests {
    use super::connect;

    #[test]
    fn alias_components_handle_reverse_duplicates_and_self_links() {
        let edge = |a: u64, b: u64| (17u64 << 10) | (a << 5) | b;
        assert_eq!(connect(&[edge(2, 31), edge(1, 2), edge(31, 2), edge(2, 2)]),
                   Some((1 << 31) | (1 << 2) | (1 << 1)));
        assert_eq!(connect(&[edge(31, 31)]), Some(1 << 31));
        assert_eq!(connect(&[edge(2, 31), edge(1, 0)]), None);
        assert_eq!(connect(&[]), None);
    }
}
