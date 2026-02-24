# Estelita Progress — Milestones

## Current Status (2026-02-24)
- Gateway: healthy (Openclaw 2026.2.23, model openai/gpt-5.1-codex)
- Evidence extracted: extracted_samples/ populated (Globoplay confirmed)
- Inventory merged: _stage/inventory_merged.csv (40317 rows)
- Attachments flattened: _stage/attachments_flat.csv (53621 rows)
- Globoplay: normalized_raw -> cues_core -> dedup -> match_report (3.3% title-only)

## Milestones
### M0 — Environment/Agent
- [x] Gateway running locally
- [x] Concurrency set to 1
- [x] Lockfile issues handled

### M1 — Evidence & Inventory
- [x] Merge inventory parts to _stage/inventory_merged.csv
- [x] Flatten attachments to _stage/attachments_flat.csv
- [ ] Build vendor bucket coverage table (all buckets, counts by ext + size)

### M2 — Normalization (per vendor bucket)
- [x] Globoplay: _stage/globoplay_normalized_raw.csv
- [x] Globoplay: _stage/globoplay_cues_core.csv
- [ ] Repeat for Globo / SBT / Band / others

### M3 — Matching & Reporting
- [x] First-pass match report: _stage/globoplay_match_report.csv (3.3% title-only)
- [ ] Add fuzzy candidate matching report (top-5 suggestions)
- [ ] Decide acceptance thresholds + manual review flow

### M4 — Deliverables
- [ ] Export “match candidates + evidence references” for Estelita team review
- [ ] Produce final reconciled report per vendor + period
