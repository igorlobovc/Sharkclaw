# Reference audit (local-only sources)

This document audits which local files can be trusted as inputs to build a **Reference_Truth** dataset.

> Note: We do **not** commit the raw source XLSX/PDF/CSV data to git. We only document findings and schemas.

## Files audited

| Key | Path | Exists | Approx size |
|---|---|---:|---:|
| OBRAS_xlsx | `/Users/igorcunha/Desktop/Estelita_backup/Estelita/Raw/ESTELITA OBRAS_v2.xlsx` | ✅ | ~1.69 MB |
| FONOGRAMAS_xlsx | `/Users/igorcunha/Desktop/Estelita_backup/Estelita/Raw/ESTELITA Fonogramas_v2.xlsx` | ✅ | ~0.15 MB |
| Supplier_Matches_Sure | `/Users/igorcunha/Desktop/Estelita_backup/Estelita/Processed/Supplier_Matches_Sure.csv` | ✅ | ~0.03 MB |
| Curated_Titles_Authors | `/Users/igorcunha/Desktop/Estelita_backup/Estelita/Processed/Curated_Titles_Authors.csv` | ✅ | ~0.23 MB |
| EML_consolidated | `/Users/igorcunha/Desktop/Estelita_backup/Estelita/EML/consolidated_sheets.csv` | ✅ | ~64.6 MB |

## OBRAS_xlsx (ESTELITA OBRAS_v2.xlsx)

### Observations
- Single sheet: `Sheet1`.
- File is **not** a clean tabular XLSX with named headers at row 1.
- Detected header row at **row index ~8** (0-based) when reading as `header=None`.
- The sheet appears to contain a *report style* header:
  - `TITULAR:` and the entity **`EDUARDO MELO PEREIRA LTDA`** are present.
  - `PSEUDÔNIMO:` includes `ESTELITA`.
- The actual table header line includes fields like:
  - `CÓD. OBRA`, `ISWC`, `TÍTULO PRINCIPAL DA OBRA`, `SITUAÇÃO*`, etc.

### Implication
This is a **high-authority source** for owned works, but requires a parser that:
- locates the table header row
- normalizes column names
- extracts only the data block below the header.

## FONOGRAMAS_xlsx (ESTELITA Fonogramas_v2.xlsx)

### Observations
- Single sheet: `Sheet1`.
- Also report-style; detected header row at **row index ~7** (0-based).
- Table header includes:
  - `CÓD. ECAD`, `ISRC/GRA`, `SITUACAO`, `TÍTULO PRINCIPAL DA OBRA MUSICAL`, `RÓTULO`, etc.

### Implication
High-authority source for owned phonograms; needs the same style of header detection + extraction.

## EML_consolidated_sheets.csv

### Observations
- Very wide schema (~126 columns) combining multiple report layouts.
- Contains strong ownership evidence strings (e.g., `Eduardo Melo Pereira Ltda (Editora Estelita)` was found earlier by text search).
- Owned-name tokens like **Tagore** appear, but in non-obvious columns (`Unnamed: 7`, `Unnamed: 8`).

### Implication
This is a **supporting reference** for:
- gold-string detection
- owned-name token lists (Tagore, etc.)
- validating vendor report column semantics

But it is not the authoritative owned catalog by itself.

## Supplier_Matches_Sure.csv

### Observations
- Small file (~116 rows).
- Contains historically curated sure matches; includes Yago o Próprio matches.

### Implication
Useful as a **training/validation set** and as seed for deny/allow lists.

## Curated_Titles_Authors.csv

### Observations
- Columns: `title`, `author`.
- Many `author` values are codes like `T-...` or null.

### Implication
Useful for baseline title normalization and as one reference layer, but insufficient for Gold/Silver/Bronze evidence rules on its own.
