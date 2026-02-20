# Status

## 2026-02-20 11:36 (GMT-3)

### Current top goal
Finish **canonical Reference Truth** build (single local-only `runs/reference/reference_truth.csv` + `_truth_summary.json`) so we can start scoring Fornecedores with evidence flags.

### DONE (confirmed artifacts)
- Repo initialized + rules/tokens committed.
- Parsers/normalizers exist:
  - `scripts/parse_estelita_reports.py`
  - `scripts/normalize_reference_tables.py`
  - `scripts/build_reference_truth_from_structured.py`
- Local-only reference artifacts exist (not yet canonical):
  - `runs/reference/obras_truth.csv`
  - `runs/reference/fonogramas_truth.csv`
  - `runs/reference/reference_truth_structured.csv`

### NOT DONE (blocking)
- No canonical output yet:
  - `runs/reference/reference_truth.csv` (missing)
  - `runs/reference/_truth_summary.json` (missing)
- No canonical builder script yet:
  - `scripts/build_reference_truth.py` (missing)

### IN PROGRESS (starting now)
1) Implement `scripts/build_reference_truth.py` that merges:
   - `obras_truth.csv` + `fonogramas_truth.csv`
   - `reference_truth_structured.csv`
   - token seeds from `Processed/Supplier_Matches_Sure.csv` (and later consolidated sheets)
2) Generate local-only canonical outputs:
   - `runs/reference/reference_truth.csv`
   - `runs/reference/_truth_summary.json`
3) Commit + push scripts + docs updates (no data committed).

### Next checkpoints
- **T+45 min:** `STATUS.md` updated + canonical builder committed + first local canonical outputs generated.
- **T+90 min:** refine merge rules + expand truth from CLEAN sources if needed.
