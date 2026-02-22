#!/usr/bin/env python3
"""Build a Known-Good Template Set of fornecedor workbooks (provider-focused).

Outputs:
- known_good_template_set.csv

Strategy:
- Search canonical lanes only by default:
  - estelita_unified_audiovisual/dedup/unique
  - Desktop/Estelita_backup/Estelita/Raw/Fornecedores
- Filter to spreadsheets (.xls/.xlsx) and provider_guess in (globo, globoplay, ubem, sbt, band)
- For each workbook, read sheet names and a small sample of header row(s) to capture:
  - row_count (approx: number of rows in sample read; not full file)
  - col_headers_normalized (normalized column names)
  - example_header_row (raw header names joined)

NOTE: This is metadata-only; not a full extraction.
"""

from __future__ import annotations

import argparse
import os
import re
import unicodedata
from pathlib import Path

import pandas as pd


PROVIDERS = [
    ("band", re.compile(r"\bband\b|bandeirantes", re.I)),
    ("sbt", re.compile(r"\bsbt\b", re.I)),
    ("globo", re.compile(r"\bglobo\b|canais globo", re.I)),
    ("globoplay", re.compile(r"globoplay", re.I)),
    ("ubem", re.compile(r"ubem", re.I)),
]


def guess_provider(path: str) -> str:
    s = str(path)
    for name, rx in PROVIDERS:
        if rx.search(s):
            return name
    return "other"


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def norm_header(s: str) -> str:
    s = "" if s is None else str(s)
    s = strip_accents(s.casefold())
    s = re.sub(r"[^0-9a-z]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--root", action="append", required=True)
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--max-sheets", type=int, default=6)
    ap.add_argument("--sample-rows", type=int, default=200)
    args = ap.parse_args()

    roots = [Path(r).expanduser() for r in args.root]
    exts = {".xls", ".xlsx"}

    cands = []
    for root in roots:
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if not p.is_file() or p.suffix.lower() not in exts:
                continue
            prov = guess_provider(str(p))
            if prov in {"globo", "globoplay", "ubem", "sbt", "band"}:
                cands.append((prov, p))

    # prefer larger files first (heuristic for real reports)
    cands2 = []
    for prov, p in cands:
        try:
            cands2.append((prov, p, os.stat(p).st_size))
        except Exception:
            continue
    cands2.sort(key=lambda x: x[2], reverse=True)

    rows = []
    for prov, p, _sz in cands2[: args.limit]:
        try:
            xl = pd.ExcelFile(p)
        except Exception:
            continue

        for sh in xl.sheet_names[: args.max_sheets]:
            try:
                df = xl.parse(sh, dtype=str, nrows=args.sample_rows)
            except Exception:
                continue

            headers = [str(c) for c in df.columns]
            headers_norm = [norm_header(c) for c in headers]
            rows.append(
                {
                    "provider_guess": prov,
                    "file_path": str(p),
                    "sheet_name": str(sh),
                    "row_count": int(len(df)),
                    "col_headers_normalized": "|".join(headers_norm),
                    "example_header_row": "|".join(headers),
                }
            )

    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"Wrote: {out} rows={len(rows)}")


if __name__ == "__main__":
    main()
