# Sharkclaw (Estelita / Fornecedores)

This repo exists to stop the "where were we?" loop.

## Goal
Build a reproducible pipeline to:
1) Ingest **Fornecedor** usage / cue-sheet reports (Globo/Globoplay, Band, SBT, Record…)
2) Match rows to Estelita-owned works
3) Score each row as **Gold / Silver / Bronze / NoMatch** with explicit evidence
4) Produce audit-friendly outputs for billing.

## What is *not* in this repo
We do **not** commit raw spreadsheets, PDFs, mbox dumps, or other sensitive/source datasets.
We only commit:
- code
- documented matching rules
- small configuration files (token lists)
- pointers to where data lives on disk.

## Local data roots (not committed)
See: `docs/DATA_ROOTS.md`

## Matching rules
See: `docs/MATCHING_RULES.md`

## Project status
See: `docs/STATUS.md`
