//! Exact counting order within half-hour bins. Only first/last times matter
//! inside a bin, because its entire span is strictly shorter than the gap limit.
use crate::parse::SESSION_GAP_MS;
pub type Event = (i64, u32, u32);

pub fn order(events: &mut Vec<Event>, bins: bool, window_start: i64, window_end: i64) {
    let nbin = window_end.saturating_sub(window_start) / SESSION_GAP_MS + 1;
    if !bins || events.len() < 64 || !(1..=2048).contains(&nbin) {
        events.sort_unstable_by_key(|e| e.0);
        return;
    }
    let nbin = nbin as usize;
    let mut counts = vec![0usize; nbin];
    let mut low = vec![i64::MAX; nbin];
    let mut high = vec![i64::MIN; nbin];
    for &(ts, _, _) in events.iter() {
        let b = ((ts - window_start) / SESSION_GAP_MS) as usize;
        counts[b] += 1;
        low[b] = low[b].min(ts);
        high[b] = high[b].max(ts);
    }
    let mut cursors = vec![0usize; nbin];
    let mut start = 0;
    for b in 0..nbin {
        cursors[b] = start;
        start += counts[b];
    }
    let mut ordered = vec![(0, 0, 0); events.len()];
    for &(ts, price, category) in events.iter() {
        let b = ((ts - window_start) / SESSION_GAP_MS) as usize;
        let dst = cursors[b];
        // Exactly one first event receives min, the rest max. Counts, purchases,
        // categories, session starts and gaps between bins are all preserved.
        let first = counts[b] > 0;
        ordered[dst] = (if first { low[b] } else { high[b] }, price, category);
        counts[b] = 0;
        cursors[b] += 1;
    }
    *events = ordered;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    fn signature(events: &[Event]) -> Vec<(i64, usize, BTreeMap<u32, u64>)> {
        let mut out = Vec::new();
        let mut prev = None;
        for &(ts, price, cat) in events {
            if prev.is_none_or(|p| ts - p > SESSION_GAP_MS) {
                out.push((ts, 0, BTreeMap::new()));
            }
            let last = out.last_mut().unwrap();
            last.1 += 1;
            if price > 0 {
                *last.2.entry(cat).or_default() += price as u64;
            }
            prev = Some(ts);
        }
        out
    }
    #[test]
    fn bins_match_sort_at_boundaries_and_randomly() {
        let mut rng = 123456789u64;
        for case in 0..1000 {
            let start = -86_400_000;
            let end = start + 7 * 86_400_000;
            let mut data = Vec::new();
            for i in 0..(64 + case % 900) {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let ts = if i % 3 == 0 {
                    start + (rng % 330) as i64 * SESSION_GAP_MS + (i % 3) as i64
                } else {
                    start + (rng % ((end - start) as u64)) as i64
                };
                data.push((ts, (i % 7) as u32, (i % 5) as u32));
            }
            data.extend([
                (start, 10, 1),
                (start + SESSION_GAP_MS, 20, 2),
                (start + 2 * SESSION_GAP_MS + 1, 30, 3),
                (start + 2 * SESSION_GAP_MS, 40, 4),
            ]);
            let mut sorted = data.clone();
            order(&mut sorted, false, start, end);
            order(&mut data, true, start, end);
            assert_eq!(signature(&data), signature(&sorted));
        }
    }
}
