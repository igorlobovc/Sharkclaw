# Dev workflow (Sharkclaw)

This repo uses a simple, repeatable **quality gate** so humans, Codex, and CI run the same checks.

## Local setup

From repo root:

```bash
make install
```

## Lint

```bash
make lint
```

- Runs: `python3 -m ruff check .`

## Tests

```bash
make test
```

- Runs: `PYTHONPATH=. python3 -m pytest -q`

## CI (GitHub Actions)

Workflow file:
- `.github/workflows/python-ci.yml`

Triggers:
- `pull_request`
- `push` to `main`

Jobs run (same as local quality gate):

```bash
make install
make lint
make test
```

## Using Codex on PRs

To request Codex changes, comment on the PR:

- `@codex <what you want changed>`

Examples:
- `@codex Run make lint. Fix Ruff violations until it passes. Keep changes minimal and don’t change behavior.`
- `@codex Add GitHub Actions CI to run: make install, make lint, make test on PRs and pushes to main.`

Repo-specific review rules live in:
- `AGENTS.md`
