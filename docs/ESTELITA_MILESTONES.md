# ESTELITA milestones (Sharkclaw)

## A) Objective
Build a reproducible pipeline to ingest **Fornecedor** usage/cue-sheet reports (Band/SBT/Globo/Globoplay/etc.), match rows to the Estelita-owned catalogue (OBRAS/FONOGRAMAS), and score each row as **Gold/Silver/Bronze/NoMatch** with explicit evidence and audit-friendly outputs.

## B) Current state (as of now)
- Quality gate is in place on `main`: `requirements.txt`, `Makefile`, and GitHub Actions CI (`.github/workflows/python-ci.yml`).
- A full fornecedor file universe registry exists: `~/Desktop/TempClaw/fornecedores_file_registry.csv` (8,335 files across the approved roots).
- Structured-only workset exists: `~/Desktop/TempClaw/fornecedores_structured_workset.csv` (1,966 structured files) + `~/Desktop/TempClaw/fornecedores_top_candidates_structured.csv` (top 400).
- Reference Truth exists (local-only output): `runs/reference/reference_truth_enriched_clean.csv` (1007 rows) and scorer is implemented (`scripts/score_rows.py`, config in `config/scoring_config.json`).
- Latest “top400” run produced scored output: `~/Desktop/TempClaw/scored_master_workset_top400.csv` with summary `~/Desktop/TempClaw/scored_master_workset_top400__summary.json`.

## C) DONE milestones

### 2026-02-03 — Takeout: Financeiro sweep + Fornecedores inbox promotion
- Artifact paths:
  - `~/Desktop/Estelita_Exports/Takeout/Financeiro` (3,149 attachments stored)
  - `~/Desktop/Estelita_Exports/Takeout/Fornecedores_Inbox` (136 structured files)
  - `~/Desktop/Estelita_backup/Estelita/Raw/Fornecedores/Takeout_Inbox` (mirror, ~112MB)
- Command used: (takeout pipeline + sweep; tracked in memory)
- Proof:
  - memory note: `memory/2026-02-03.md`

### 2026-02-03 — Takeout_Inbox structured triage + matching outputs
- Artifacts copied to `~/Desktop/Estelita_backup/Estelita/Processed/`:
  - `Suppliers_Readiness_Report__takeout_inbox_2026-02-03.csv`
  - `Suppliers_Readiness_Top__takeout_inbox_2026-02-03.csv`
  - `Supplier_Matches_All__takeout_inbox_afterzip_2026-02-03.csv`
  - `Suppliers_Coverage_Summary__takeout_inbox_afterzip_2026-02-03.csv`
  - `Suppliers_Top_Unmatched__takeout_inbox_afterzip_2026-02-03.csv`
- Command used (as recorded):
  - `python3 /Users/igorcunha/SHRKVSCODE/_archive/Estelita_unrelated/analyze_supplier_readiness.py`
  - `python3 /Users/igorcunha/SHRKVSCODE/_archive/Estelita_unrelated/batch_match_suppliers.py`
- Proof:
  - memory note: `memory/2026-02-04-0105.md`

### 2026-02-20 — Reference Truth + scorer committed
- Repo artifacts:
  - `scripts/build_reference_truth.py`
  - `runs/reference/reference_truth.csv`
  - `runs/reference/_truth_summary.json`
  - `scripts/score_rows.py`
  - `config/scoring_config.json`
  - `docs/SCORING_RULES.md`
  - `tests/test_scorer.py` + `tests/fixtures.json`
- Proof:
  - `docs/STATUS.md`

### 2026-02-21 — Prevent “wrong loop”: multi-input extractor + locator
- Repo artifacts:
  - `scripts/extract_fornecedores_basic.py` (multi `--input`, optional `--needles`, skip corrupt)
  - `scripts/locate_known_titles.py` (batch Ctrl+F across structured sources)
- Proof:
  - `memory/2026-02-21.md`

### 2026-02-21 — Persistent sure-match catalog + regression harness (option 2)
- Repo artifacts:
  - `config/sure_match_catalog.csv`
  - `scripts/build_regression_cases.py`
  - `scripts/check_regression_cases.py`
- Generated artifacts (TempClaw):
  - `~/Desktop/TempClaw/regression_cases_v2_option2.csv`
  - `~/Desktop/TempClaw/regression_check_report.txt`

### 2026-02-21 — Fornecedor universe registry + structured workset + zip extraction
- Artifacts (TempClaw):
  - `~/Desktop/TempClaw/fornecedores_file_registry.csv`
  - `~/Desktop/TempClaw/fornecedores_file_registry__summary.txt`
  - `~/Desktop/TempClaw/fornecedores_structured_workset.csv`
  - `~/Desktop/TempClaw/fornecedores_top_candidates_structured.csv`
  - `~/Desktop/TempClaw/fornecedores_archives_queue.csv`
  - `~/Desktop/TempClaw/zip_extraction_report.csv` (zip_total=74, extracted=64, password=10)
- Commands used (executed in this session):
  - registry build: custom python scan writing `fornecedores_file_registry.csv`
  - structured filter + top candidates: python generating CSVs in TempClaw
  - zip extract: python `zipfile` extraction with report

### 2026-02-21 — Top400 run (A): extract + score
- Artifacts:
  - Extracted master: `~/Desktop/TempClaw/extracted_master_workset_top400.csv` (rows=1,072,944)
  - Scored output: `~/Desktop/TempClaw/scored_master_workset_top400.csv`
  - Summary: `~/Desktop/TempClaw/scored_master_workset_top400__summary.json`
- Command used:
  - Extract:
    - `python3 scripts/extract_fornecedores_basic.py --input <400 paths...> --output ~/Desktop/TempClaw/extracted_master_workset_top400.csv`
  - Score:
    - `python3 scripts/score_rows.py --input ~/Desktop/TempClaw/extracted_master_workset_top400.csv --reference runs/reference/reference_truth_enriched_clean.csv --config config/scoring_config.json --output ~/Desktop/TempClaw/scored_master_workset_top400.csv --summary ~/Desktop/TempClaw/scored_master_workset_top400__summary.json`
- Proof:
  - Summary JSON includes tier distribution (Gold/Silver/Bronze/NoMatch).

### 2026-02-21 — Codex + CI quality gate merged
- Repo artifacts on `main`:
  - `requirements.txt`
  - `Makefile`
  - `.github/workflows/python-ci.yml`
  - `AGENTS.md`
- Proof:
  - GitHub Actions runs “Python CI” are green on `main`.

## D) TODO milestones (ordered)

### 1) Build a fast locator/index for the structured workset
- Definition of done:
  - Can answer “where does this title/person occur?” without scanning all XLSX via openpyxl each time.
- Expected artifacts/paths:
  - `~/Desktop/TempClaw/workset_index.sqlite` (or CSV index)
  - `~/Desktop/TempClaw/known_titles_locator_hits_workset.csv`
- Command to run:
  - (to be added) `python3 scripts/build_workset_index.py --workset ~/Desktop/TempClaw/fornecedores_structured_workset.csv --out ~/Desktop/TempClaw/workset_index.sqlite`

### 2) Expand extraction coverage beyond top400 (phase 2)
- Definition of done:
  - Extract and score the full structured workset (1,966 files) in batches with skip/continue behavior.
- Expected artifacts:
  - `~/Desktop/TempClaw/extracted_master_workset_all.csv` (or partitioned shards)
  - `~/Desktop/TempClaw/scored_master_workset_all.csv` + summary
- Command to run:
  - (to be scripted) batch loop over `fornecedores_structured_workset.csv`.

### 3) Improve reference truth anchoring (reduce missing-from-truth alerts)
- Definition of done:
  - Regression cases show significantly more anchors than `anchored=1` (current baseline).
- Expected artifacts:
  - Updated `runs/reference/reference_truth_enriched_clean.csv`
  - Updated `_truth_summary.json`
- Command to run:
  - `python3 scripts/build_reference_truth.py ...`
  - `python3 scripts/enrich_reference_truth_from_clean_xlsx.py ...`

### 4) Add real tests for locator/extractor utilities
- Definition of done:
  - 3–5 tests for `scripts/locate_known_titles.py` with fixtures.
- Expected artifacts:
  - `tests/test_locate_known_titles.py` + fixtures
- Command to run:
  - `make test`

## E) Next 3 execution steps (commands + expected outputs)

1) Create a “review slice” from scored top400 using sure-match catalog terms
```bash
# (to be implemented) python3 scripts/slice_scored_by_sure_terms.py \
#   --scored ~/Desktop/TempClaw/scored_master_workset_top400.csv \
#   --sure config/sure_match_catalog.csv \
#   --out ~/Desktop/TempClaw/review_slice_top400_sure_terms.csv
```
Expected output:
- `~/Desktop/TempClaw/review_slice_top400_sure_terms.csv`

2) Run regression harness (baseline)
```bash
python3 scripts/build_regression_cases.py \
  --examples-csv '/Users/igorcunha/Desktop/tempClaw/Example matches with references .csv' \
  --truth-csv runs/reference/reference_truth_enriched_clean.csv \
  --sure-catalog config/sure_match_catalog.csv \
  --output /Users/igorcunha/Desktop/TempClaw/regression_cases_v2_option2.csv

python3 scripts/check_regression_cases.py \
  --cases /Users/igorcunha/Desktop/TempClaw/regression_cases_v2_option2.csv \
  --report /Users/igorcunha/Desktop/TempClaw/regression_check_report.txt
```
Expected outputs:
- `~/Desktop/TempClaw/regression_cases_v2_option2.csv`
- `~/Desktop/TempClaw/regression_check_report.txt`

3) Rerun targeted known-title extraction (sanity) on confirmed-hit fornecedor sources
```bash
python3 scripts/extract_fornecedores_basic.py \
  --input '/Users/igorcunha/Desktop/Estelita_backup/Estelita/Raw/Fornecedores/Band/band UNIFICADO out dez 2023 jan fev 2024.xls' \
  --input '/Users/igorcunha/Desktop/Estelita_backup/Estelita/Raw/Fornecedores/SBT/Unificado SBT nov dez 23 e jan fev mar 24.xls' \
  --output /Users/igorcunha/Desktop/TempClaw/extracted_master_known_titles_sources.csv \
  --needles 'VIVRE LA VIE,NHEENGATU,BALMAIN,ZIRIGUIDUM,DEIXE QUEIMAR'
```
Expected output:
- `~/Desktop/TempClaw/extracted_master_known_titles_sources.csv`

## F) Risks / blockers (top 5)
- **Truth coverage gap:** many expected-owned items are currently missing from `reference_truth_enriched_clean.csv`, so regression anchors remain low.
- **Workbook heterogeneity:** fornecedor spreadsheets vary widely; auto-header detection will miss some.
- **Corrupt/odd XLSX:** some files throw `BadZipFile`; extractor must skip/continue (implemented).
- **Password-protected archives:** 10 zips in the current extraction report require manual handling.
- **Performance:** scanning all XLSX repeatedly is slow; indexing is needed.

## G) Acceptance checklist for the next pilot rerun (success criteria)
- `make lint` and `make test` pass locally and in CI.
- Top400 extraction produces a stable master extract with skip/continue (no aborts).
- Scored output exists + summary JSON generated.
- Regression report is generated every run.
- At least one of these improves vs baseline:
  - anchored regression count increases (truth coverage improves), OR
  - sure-term review slice shows high-precision hits with provenance for manual confirmation.
