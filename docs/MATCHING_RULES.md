# Matching rules (current)

## Scope
Only score rows from **Fornecedor usage/cue-sheet reports** (Globo/Globoplay, Band, SBT, Record, etc.).
Exclude:
- streaming/distro statements (extrato/strm/distro/dist/Deezer/Spotify/YouTube)
- internal accounting (contas a pagar/receber, caixa, faturamento, custos)
- catalogue maintenance / ISRC checking

## Evidence-driven tiers
- **Gold:** direct ownership evidence (e.g., `Editora Estelita`, `Eduardo Melo Pereira Ltda (Editora Estelita)`), exact identifiers, or strong multi-signal evidence.
- **Silver:** strong owned-name hit (e.g., Tagore) and/or strong title match with partial author support.
- **Bronze:** weak but plausible; requires manual review.
- **NoMatch:** insufficient evidence or conflict.

## Guardrails
- False positives are worse than false negatives.
- Title-only matches are risky:
  - if the title is common/ambiguous, require author/artist evidence.
  - maintain a denylist of common titles that must not match on title-only.

## Known special rules
- **Tagore:** any mention counts as a hit.
- **Beatles:** do not treat Lennon/McCartney as owned generally; `ELEANOR RIGBY` is an owned-work exception.

## Configuration
Evidence tokens live in `config/evidence_tokens.json`.
