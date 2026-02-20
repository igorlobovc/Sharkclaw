# Scoring Rules (v0.1)

**Principle:** false positives are worse than false negatives.

## Inputs
We score **extracted Fornecedores rows** against the canonical owned-catalog reference truth (`runs/reference/reference_truth.csv`).

## Output fields
Each row gets:
- `match_tier`: `Gold | Silver | Bronze | NoMatch`
- `matched`: `1/0`
- `evidence_flags`: `;`-separated reasons
- `ref_match_count`: how many reference candidates share the same title (or same identifier)
- `ref_title_norm`, `ref_isrc`, `ref_iswc`: best candidate fields (for audit)

## Evidence flags
- `ISRC_MATCH`: strong identifier match
- `ISWC_MATCH`: strong identifier match
- `GOLD_TOKEN_HIT`: high-stakes owned entity token found in the row text
- `TITLE_EXACT`: normalized title equals reference title
- `NEGATIVE_TITLE_TRIGGER`: title contains an ambiguity trigger (e.g. “DEJA VU”, “DIANA”, “MACETANDO”…)
- `ARTIST_TOKEN_OVERLAP`: row artist/author tokens overlap reference evidence tokens
- `ARTIST_PRESENT_NO_SUPPORT`: artist/author exists but no supporting overlap (abstain)

## Tier rules (v0.1)
1. **Gold**
   - `ISRC_MATCH` OR `ISWC_MATCH`
   - OR (`TITLE_EXACT` AND `GOLD_TOKEN_HIT`)
2. **Silver**
   - `TITLE_EXACT` AND `ARTIST_TOKEN_OVERLAP`
3. **Bronze**
   - `TITLE_EXACT` AND **no artist/author provided** AND title length >= `min_title_len_for_bronze` AND NOT `NEGATIVE_TITLE_TRIGGER`
4. **NoMatch**
   - Anything else.

## Hard deny rule
If `NEGATIVE_TITLE_TRIGGER` is present, we **do not** match on title-only. We require `ISRC/ISWC` or `GOLD_TOKEN_HIT`.

## Notes
This scorer is intentionally conservative and will miss some true matches until we enrich:
- reference evidence tokens (contributors/publishers)
- supplier row author/artist parsing
- per-provider file structure extraction
