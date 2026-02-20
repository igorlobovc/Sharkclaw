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

### IN PROGRESS (active run)
1) Add scorer:
   - `scripts/score_rows.py`
   - `tests/fixtures.json` (your positive/negative labeled cases)
2) Define scorer contract doc:
   - `docs/SCORING_RULES.md`
3) Run first scoring pilot on 1–2 Fornecedores files (start with SBT unified + Band unified):
   - outputs local-only under `runs/fornecedores/`
   - copy a review CSV to `~/Desktop/tempClaw/`

### NEXT checkpoints
- **T+45 min:** scorer + fixtures committed; scoring run produces first `runs/fornecedores/scored_rows.csv`.
- **T+90–120 min:** tighten false-positive gates (artist/author overlap + denylist) and rerun pilot.
