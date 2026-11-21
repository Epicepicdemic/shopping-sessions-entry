//! Per-person sessions, attribution and assists, shared by the exact solver
//! and the verified fast path, so both produce rows by the same code.

use std::collections::HashMap;

use crate::id::PackedId;
use crate::parquet::UserCols;
use crate::parse::{CLICK_LOOKBACK_MS, MS_PER_DAY, SESSION_GAP_MS};
use crate::session;

#[derive(Clone, Copy, Default)]
pub struct CampAcc {
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue_cents: i64,
    pub assisted_sessions: i64,
}

pub struct Sink {
    pub ucols: UserCols,
    pub camp_grid: Vec<CampAcc>,
    pub camp_extra: HashMap<(u32, i32), CampAcc>,
    cat_revenue: Vec<i64>,
    sessions_buf: Vec<(i32, i64, i64, i64, bool)>,
    assist_stamp: Vec<u32>,
    cur_stamp: u32,
    active_counts: Vec<u32>,
    active_positions: Vec<usize>,
    active_campaigns: Vec<u32>,
    use_bins: bool,
    assist_sweep: bool,
    window_start: i64,
    window_end: i64,
    base_day: i32,
    window_days: usize,
}

impl Sink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        num_campaigns: usize,
        num_categories: usize,
        window_days: usize,
        base_day: i32,
        window_start: i64,
        window_end: i64,
        use_bins: bool,
        assist_sweep: bool,
    ) -> Self {
        Self {
            ucols: UserCols::with_capacity(64_000),
            camp_grid: vec![CampAcc::default(); num_campaigns * window_days],
            camp_extra: HashMap::new(),
            cat_revenue: vec![0i64; num_categories],
            sessions_buf: Vec::new(),
            assist_stamp: vec![0u32; num_campaigns],
            cur_stamp: 0,
            active_counts: vec![0u32; num_campaigns],
            active_positions: vec![0usize; num_campaigns],
            active_campaigns: Vec::new(),
            use_bins,
            assist_sweep,
            window_start,
            window_end,
            base_day,
            window_days,
        }
    }

    /// One person's surviving events `(ts, price, category)` and valid clicks
    /// `(ts, click_id, campaign)`; click IDs live in `click_arena`.
    pub fn person(
        &mut self,
        name: &[u8],
        evs: &mut Vec<(i64, u32, u32)>,
        clicks: &mut [(i64, PackedId, u32)],
        click_arena: &[u8],
    ) {
        self.sessions_buf.clear();

        session::order(evs, self.use_bins, self.window_start, self.window_end);

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
            |ucols: &mut UserCols,
             name: &[u8],
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
                ucols.push(name, day, sessions, events, rev, top_cat);
            };

        for &(ts, price, cat_id) in evs.iter() {
            if prev_ts == i64::MIN || ts - prev_ts > SESSION_GAP_MS {
                if prev_ts != i64::MIN {
                    self.sessions_buf.push((
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
                            &mut self.ucols,
                            name,
                                                                        cur_day,
                            day_sessions,
                            day_events,
                            day_rev,
                            day_has_purchases,
                            &mut self.cat_revenue,
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
                self.cat_revenue[cat_id as usize] += p64;
            }
            prev_ts = ts;
        }
        if prev_ts != i64::MIN {
            self.sessions_buf.push((
                sess_day,
                sess_start,
                sess_events,
                sess_rev,
                sess_has_purch,
            ));
            flush_day(
                &mut self.ucols,
                name,
                                                cur_day,
                day_sessions,
                day_events,
                day_rev,
                day_has_purchases,
                &mut self.cat_revenue,
            );
        }

        for c in self.active_campaigns.drain(..) {
            self.active_counts[c as usize] = 0;
        }
        // Click attribution and assists for this person's sessions
        
        if clicks.is_empty() {
            // All sessions are organic (campaign_idx = 0)
            for &(s_day, _s_start, _s_ev, s_rev, s_purch) in &self.sessions_buf {
                let d_off = s_day as i64 - self.base_day as i64;
                let acc = if d_off >= 0 && (d_off as usize) < self.window_days {
                    &mut self.camp_grid[d_off as usize]
                } else {
                    self.camp_extra.entry((0, s_day)).or_default()
                };
                acc.sessions += 1;
                if s_purch {
                    acc.purchasing_sessions += 1;
                }
                acc.revenue_cents += s_rev;
            }
        } else {
            // Sort by ts ascending, ID descending: the last eligible row wins the tie.
            let arena_ref = click_arena;
            clicks.sort_unstable_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| b.1.cmp_exact(a.1, arena_ref, arena_ref))
            });

            let mut left = 0usize;
            let mut right = 0usize;
            let n_clicks = clicks.len();

            for &(s_day, s_start, _s_ev, s_rev, s_purch) in &self.sessions_buf {
                let min_click_ts = s_start - CLICK_LOOKBACK_MS;
                while right < n_clicks && clicks[right].0 <= s_start {
                    if self.assist_sweep {
                        let c = clicks[right].2 as usize;
                        if self.active_counts[c] == 0 {
                            self.active_positions[c] = self.active_campaigns.len();
                            self.active_campaigns.push(c as u32);
                        }
                        self.active_counts[c] += 1;
                    }
                    right += 1;
                }
                while left < right && clicks[left].0 < min_click_ts {
                    if self.assist_sweep {
                        let c = clicks[left].2 as usize;
                        self.active_counts[c] -= 1;
                        if self.active_counts[c] == 0 {
                            let pos = self.active_positions[c];
                            self.active_campaigns.swap_remove(pos);
                            if pos < self.active_campaigns.len() {
                                self.active_positions[self.active_campaigns[pos] as usize] =
                                    pos;
                            }
                        }
                    }
                    left += 1;
                }

                if left == right {
                    // Organic
                    let d_off = s_day as i64 - self.base_day as i64;
                    let acc = if d_off >= 0 && (d_off as usize) < self.window_days {
                        &mut self.camp_grid[d_off as usize]
                    } else {
                        self.camp_extra.entry((0, s_day)).or_default()
                    };
                    acc.sessions += 1;
                    if s_purch {
                        acc.purchasing_sessions += 1;
                    }
                    acc.revenue_cents += s_rev;
                } else {
                    let credited_camp = clicks[right - 1].2;

                    let d_off = s_day as i64 - self.base_day as i64;
                    let in_grid = d_off >= 0 && (d_off as usize) < self.window_days;
                    let acc = if in_grid {
                        &mut self.camp_grid[(credited_camp as usize) * self.window_days
                            + (d_off as usize)]
                    } else {
                        self.camp_extra.entry((credited_camp, s_day)).or_default()
                    };
                    acc.sessions += 1;
                    if s_purch {
                        acc.purchasing_sessions += 1;
                    }
                    acc.revenue_cents += s_rev;

                    if self.assist_sweep {
                        for &c in &self.active_campaigns {
                            let a_acc = if in_grid {
                                &mut self.camp_grid
                                    [c as usize * self.window_days + d_off as usize]
                            } else {
                                self.camp_extra.entry((c, s_day)).or_default()
                            };
                            a_acc.assisted_sessions += 1;
                        }
                    } else {
                        self.cur_stamp = self.cur_stamp.wrapping_add(1);
                        if self.cur_stamp == 0 {
                            self.assist_stamp.fill(0);
                            self.cur_stamp = 1;
                        }
                        for cl in &clicks[left..right] {
                            let c_idx = cl.2 as usize;
                            if self.assist_stamp[c_idx] != self.cur_stamp {
                                self.assist_stamp[c_idx] = self.cur_stamp;
                                let a_acc = if in_grid {
                                    &mut self.camp_grid
                                        [c_idx * self.window_days + (d_off as usize)]
                                } else {
                                    self.camp_extra
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
