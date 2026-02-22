# PROJECT_STATE (Sharkclaw / Estelita)

## Objective
Build a reproducible pipeline to ingest **Fornecedor** usage reports, match rows to the Estelita-owned catalogue (OBRAS/FONOGRAMAS Reference Truth), and produce audit-friendly **Gold/Silver/Bronze/NoMatch** outputs for billing and verification.

## Current baseline run (canonical-only)
- Path: `~/Desktop/TempClaw/_canon2_20260221_1925/`
- Inputs (selected included XLS/XLSX): **files=151**
- Extracted rows: **427,332**
- Catalog sweep matches: **222**
  - Gold **19** / Silver **192** / Bronze **11**
- Action sheet size: **211** (Gold + top Silver)
- Truth gaps: **78** (rate **37%** of Gold+Silver)
- Dedup max occurrences: **26**

Key artifacts:
- `~/Desktop/TempClaw/_canon2_20260221_1925/report_package/`:
  - `match_report_overview.md`
  - `match_report_rows.csv`
  - `match_report_dedup.csv`
  - `match_report_truth_gaps.csv`
  - `action_sheet.csv`

## Detection/triage (known-good template mapping) — current phase

**Guardrails (active):** NO ZIP expansion. NO scoring/threshold/sweep/entity-promotion changes. Detection/triage/documentation only.

- Repo HEAD: `0a5b384`
- Canon2 report package: `~/Desktop/TempClaw/_canon2_20260221_1925/report_package/`

Artifacts regenerated/copied into canon2 report_package:
- `known_good_template_set.csv` (regenerated)
- `header_synonym_inventory.csv` (regenerated)
- `known_good_mapping_audit_v1.csv` + `known_good_mapping_audit_v1_summary.txt` (copied from prior runs/reference)
- `known_good_failures_triage_v1.csv` (copied from prior runs/reference)
- `known_good_mapping_audit_v2.csv` + `known_good_mapping_audit_v2_summary.txt` (regenerated)
- `known_good_failures_triage_v2.csv` (regenerated)

**v2 detection result:** templates_total=59; title AND (artist OR author)=23; pct=0.390

Main failing bucket (by notes): overwhelmingly `missing_title_detect;missing_artist_author_detect` on UBEM/Relatorio sheets and "Tabela de Sincronização" variants.

## Milestones

### DONE
- Coverage audit + quarantine loop:
  - `scripts/audit_fornecedores_coverage.py`
  - `config/fornecedores_inclusion_rules.yaml`
  - outputs: `~/Desktop/TempClaw/coverage_report.csv`, `~/Desktop/TempClaw/quarantine_parse_errors/`
- Catalog sweep mode:
  - `scripts/run_catalog_sweep.py`
- Match report package builder:
  - `scripts/build_match_report.py`
- Canonical lane enforcement (reduce duplicates at source):
  - `config/fornecedores_exclude_globs.txt`
  - inclusion roots narrowed to canonical lanes
- Sure-term slice + overrides + review queues (for targeted review):
  - `scripts/slice_scored_by_sure_terms.py`
  - `config/sure_match_catalog.csv`, `config/sure_match_overrides.csv`

### IN PROGRESS
- Improve Reference Truth anchoring so fewer high-confidence matches lack ref IDs (truth gaps).
- Extend truth coverage of publisher/org tokens (ORG hits currently weak).

### NEXT
- Use `match_report_truth_gaps.csv` to drive truth enrichment fixes (add missing ISRC/ISWC where appropriate).
- Add a small committed scored fixture so CI can enforce a minimum Gold/Silver count deterministically.

## Do not do yet
- **Do not expand to ZIPs yet**. Validate precision + truth-gap fixes on canonical structured sources first.
