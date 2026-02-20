# Decisions / Guardrails

## Scope
- Focus: **Fornecedores usage / cue-sheet reports** under `/Users/igorcunha/Desktop/Estelita_backup/Estelita/Raw/Fornecedores`.
- Do not treat distro/streaming extratos or internal accounting as fornecedor reports.

## Evidence tiers
- **Gold**: any hit of high-stakes tokens (Tagore, Yago o Próprio, etc.) or explicit ownership entities (Editora Estelita / Eduardo Melo Pereira Ltda...).
- **Silver/Bronze**: only when Gold evidence absent (conservative; false positives are worse than false negatives).

## Negative rules (must always apply)
- Maintain a denylist of ambiguous/common titles that must not match on title-only (e.g., MACETANDO, VIDA LOKA, VERMELHO, BRISA, DEJA VU, DIANA, BEIJINHO NO OMBRO, ME CHAMA).
- If a row contains artist/author info that conflicts with ownership evidence, abstain.

## Repo policy
- Repo is **PUBLIC**. Do not commit raw data (XLSX/PDF/CSV outputs). Commit code + docs + small config only.
