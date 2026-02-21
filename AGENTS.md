# AGENTS.md

## Review guidelines

- Prefer minimal, high-confidence changes.
- Don’t introduce new dependencies unless necessary.
- Ensure tests run (`pytest -q`) and update/add tests when fixing bugs.
- Avoid logging sensitive data / absolute file paths to local data roots.
- Keep matching/scoring rules explicit and documented in `docs/`.

## High-impact repo rules

- **Never change scoring logic** without updating:
  - `docs/STATUS.md` (what changed + why)
  - and if needed `docs/SCORING_RULES.md` / `docs/MATCHING_RULES.md`
- Prefer **pure functions** in `scripts/` (easy to test) and avoid hidden global state.
- For any bugfix: add/adjust tests.
- Keep local data paths confined to docs like `docs/DATA_ROOTS.md`; avoid hardcoding user-specific absolute paths in code.
