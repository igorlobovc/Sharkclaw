# Status

## 2026-02-19
- Confirmed local authoritative owned catalog PDFs + XLSX versions exist.
- Built a stable file-selection workflow for Fornecedores reports using a manifest + targeted triage.
- Identified that prior matching runs were overly title-driven and caused false positives.
- Found strong evidence strings in `EML/consolidated_sheets.csv` (e.g. `Eduardo Melo Pereira Ltda (Editora Estelita)`).
- Confirmed Tagore/related owned-name hits appear in consolidated EML data.

## Next milestone
- Parse `ESTELITA OBRAS_v2.xlsx` + `ESTELITA Fonogramas_v2.xlsx` into a committed *schema* (not data), and generate a local-only `Reference_Truth.csv`.
- Implement Gold/Silver/Bronze scorer with evidence flags.
