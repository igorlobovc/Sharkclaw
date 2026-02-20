# Status

## 2026-02-20 15:39 (GMT-3)

### Current top goal
Implement **Gold/Silver/Bronze scorer with evidence flags** and run a first Fornecedores scoring pilot using the canonical Reference Truth.

### DONE (confirmed artifacts)
- Canonical Reference Truth (local-only) is built:
  - `runs/reference/reference_truth.csv`
  - `runs/reference/_truth_summary.json`
- Canonical builder script exists (committed):
  - `scripts/build_reference_truth.py`
- Repo commits are being used as the single source of truth (no raw data committed).
- Caffeinate is running (PID 9901).

### Canonical Truth v1 quick stats
- rows_out: 1007
- unique_titles: 979
- non_empty_isrc: 249
- non_empty_iswc: 3

### DONE (since last status)
- Scorer implemented + committed:
  - `scripts/score_rows.py`
  - `config/scoring_config.json`
  - `docs/SCORING_RULES.md`
  - `tests/fixtures.json` + `tests/test_scorer.py` (pytest)

### DONE (pilot run)
- Ran basic extractor + scorer on:
  - Band: `Raw/Fornecedores/Band/band UNIFICADO out dez 2023 jan fev 2024.xls`
  - SBT:  `Raw/Fornecedores/SBT/Unificado SBT nov dez 23 e jan fev mar 24.xls`
- Copied review CSV to: `~/Desktop/tempClaw/review_top300_matches.csv`

**Pilot result (with conservative gates):** only 1 match surfaced (ELEANOR RIGBY) due to explicit exception rule.

### IN PROGRESS (active run)
- Improve reference evidence tokens (contributors/publishers) so we can do `ARTIST_TOKEN_OVERLAP` safely and unlock real matches (e.g., VIVRE LA VIE, NHEENGATU, BALMAIN, ZIRIGUIDUM).
  - DONE (first pass): added `scripts/enrich_reference_truth_tokens.py` to join PDF-block participant names into a local-only `reference_truth_enriched.csv` (ISRC join).
  - Next step: repeat enrichment using **CLEAN XLSX** (OBRAS_CLEAN/FONOGRAMAS_CLEAN) for broader coverage beyond the first 20 PDF pages.

### NEXT checkpoints
- **T+45–90 min:** enrich reference truth with participant tokens (from CLEAN XLSX or PDF blocks) + rerun Band/SBT pilot; expect >0 matches without title-only.
