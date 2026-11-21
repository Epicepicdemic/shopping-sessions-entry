# Adversarial fixtures

Written by `../build_adversarial.py`, which holds each case's input and hand-written expected rows with the reasoning.
Each `note.txt` names the rule the case tests and quotes the rules.md sentence that decides it.

| Case | Rule and deciding sentence |
| --- | --- |
| X01_invalid_earliest_copy | Events step 2: "if the kept copy is invalid the event is dropped, and a later valid copy does not rescue it. A malformed copy was already dropped in step 1, so it is never the kept copy." |
| X02_later_copy_changes_user | Events step 2: "Dedup on `event_id`. Keep only the copy with the smallest `ingest_ts`." Linked IDs: "an event or click is the person's as soon as it is parsed, whatever its own ID." |
| X03_stray_quotes | CSV dialect: "A line with the wrong number of fields, or with a quote anywhere except around a whole field, is malformed and is dropped." Events step 3: "`price_cents` is ignored whatever it holds" applies only to lines that parsed. |
| X04_carriage_returns | CSV dialect: "Only `\n` ends a line: a `\r` is an ordinary character of the field it is in". Strings: "IDs compare exactly, case-sensitive, untrimmed." |
| X05_thirty_minute_boundary | Events step 5: "A new session starts when the gap from the previous event is over 30 minutes (1,800,000 ms)." |
| X06_midnight | Events step 6: "One day per session: the UTC date of its start. All its events and purchases count on that day, even those after midnight." Timestamps F2: "subtract it to get UTC." |
| X07_long_chains | Linked IDs step 2: "joining is transitive ... however many links apart and in whichever files and order the lines come." Step 3: "An ID can be linked without appearing in any event or click, and can still be the smallest, and so the name." |
| X08_click_ties | Clicks step 4: "take the one with the latest `ts`; on a tie in `ts`, the smallest `click_id`, compared byte by byte." Step 7: "Each such campaign assists the session once". |
| X09_leap_day | Timestamps: "an impossible calendar value (month 13, February 30, February 29 outside a leap year ...)" is bad, so February 29 in a leap year is valid; Clicks step 4: "`session_start - click.ts <= 7 days` (604,800,000 ms)". |
| X10_ancient_ingest | Timestamps: "`ingest_ts` is always valid F1"; Events step 2: "Keep only the copy with the smallest `ingest_ts`"; step 4: "Exactly 24 hours is kept, and so is an `ingest_ts` before `ts`." |
| X11_f3_beyond_64_bits | Timestamps F3: "Leading zeros are allowed" and "one that is ... too long for a 64-bit integer is not malformed, only outside the window." Events step 3: price "digits only with a value from 1 to 10,000,000". |
| X12_byte_order | Linked IDs step 3: "Name each person by the byte-wise smallest of their IDs." user_daily top_category: "a tie goes to the byte-wise smallest". |
| X13_assists | Clicks step 7: "A session is assisted by every campaign with at least one of the person's valid clicks with `session_start - 7 days <= click.ts <= session_start`"; campaign_daily: "A campaign that only assisted that day has a row with 0 in the three credited columns." |
| X14_late_boundary | Events step 4: "drop the event if `ingest_ts - ts` is over 24 hours. Exactly 24 hours is kept, and so is an `ingest_ts` before `ts`." |
| X15_prices | Events step 3: "A `purchase` also needs `price_cents` to be digits only with a value from 1 to 10,000,000 (`0250` is 250). For `view` and `cart`, `price_cents` is ignored whatever it holds." |
| X16_files_and_headers | Input files: "Read every `*.csv` file in `events/`, `clicks/` and `links/`. ... some that hold only the header." "Every CSV file has a header row." |
| X17_quoted_empty_is_null | CSV dialect: "`""` is an empty field. ... An empty field means null." Events step 3: "Drop the event unless `user_id` is non-empty ... and `product_id` is non-empty." |
| X18_click_window_offsets | Timestamps: "A click's `ts` must satisfy `window_start - 7 days <= ts < window_end` ... A click at exactly `window_start - 7 days` is in its window". |
| X19_shared_device | Linked IDs step 2: "Every remaining line joins its two IDs into one person, and joining is transitive"; Events step 5: "Sessionize each person's surviving events, all of their IDs together". |
