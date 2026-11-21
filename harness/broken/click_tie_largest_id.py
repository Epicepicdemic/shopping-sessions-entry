#!/usr/bin/env python3
"""BROKEN ON PURPOSE. Breaks Clicks step 4, "on a tie in `ts`, the smallest
`click_id`": the largest click_id wins the tie instead."""
import sys

from _load import reference as ref


def credited_campaign_largest_id(qualifying):
    if not qualifying:
        return ref.ORGANIC
    latest = max(ts for ts, _click_id, _campaign_id in qualifying)
    _click_id, campaign_id = max(
        (click_id, campaign_id) for ts, click_id, campaign_id in qualifying if ts == latest)
    return campaign_id


ref.credited_campaign = credited_campaign_largest_id
sys.exit(ref.main(sys.argv))
