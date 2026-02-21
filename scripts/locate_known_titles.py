#!/usr/bin/env python3
"""Locate known titles across fornecedor spreadsheets.

Purpose
- Prevent "wrong loop": tuning scoring when the pilot extracts don't even contain the target rows.
- Provides a Ctrl+F-equivalent batch scan over candidate XLS/XLSX/CSV sources.

Output
- CSV report listing (needle, file, sheet, hit_count)

Notes
- Conservative implementation: reads at most first N sheets per workbook and coerces to strings.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class Hit:
    needle: str
    file: str
    sheet: str
    hit_count: int


def iter_candidate_files(paths: list[str]) -> list[Path]:
    out: list[Path] = []
    for raw in paths:
        p = Path(raw).expanduser()
        if any(ch in raw for ch in ["*", "?", "["]):
            out.extend(Path().glob(raw))
            continue
        if p.is_dir():
            out.extend([x for x in p.rglob("*") if x.is_file()])
        else:
            out.append(p)

    exts = {".csv", ".xls", ".xlsx"}
    out = [p for p in out if p.exists() and p.suffix.lower() in exts]
    # de-dupe
    seen = set()
    uniq = []
    for p in out:
        s = str(p)
        if s in seen:
            continue
        seen.add(s)
        uniq.append(p)
    return uniq


def scan_df_for_needle(df: pd.DataFrame, needle: str) -> int:
    if df is None or df.empty:
        return 0
    # any cell contains needle
    m = df.apply(lambda col: col.astype(str).str.contains(needle, case=False, na=False))
    return int(m.any(axis=1).sum())


def scan_file(path: Path, needles: list[str], max_sheets: int) -> list[Hit]:
    hits: list[Hit] = []
    suf = path.suffix.lower()

    try:
        if suf == ".csv":
            df = pd.read_csv(path, dtype=str, low_memory=False)
            for needle in needles:
                n = scan_df_for_needle(df, needle)
                if n:
                    hits.append(Hit(needle=needle, file=str(path), sheet="csv", hit_count=n))
            return hits

        xl = pd.ExcelFile(path)
        for sheet in xl.sheet_names[:max_sheets]:
            df = xl.parse(sheet, dtype=str).fillna("")
            for needle in needles:
                n = scan_df_for_needle(df, needle)
                if n:
                    hits.append(Hit(needle=needle, file=str(path), sheet=str(sheet), hit_count=n))
        return hits
    except Exception:
        return hits


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--needle",
        action="append",
        required=True,
        help="Needle to search (repeatable). Example: --needle 'VIVRE LA VIE'",
    )
    ap.add_argument(
        "--path",
        action="append",
        required=True,
        help="File/dir/glob to scan (repeatable).",
    )
    ap.add_argument("--max-sheets", type=int, default=6)
    ap.add_argument("--output", required=True, help="CSV output path")
    args = ap.parse_args()

    needles = args.needle
    files = iter_candidate_files(args.path)

    all_hits: list[Hit] = []
    for f in files:
        all_hits.extend(scan_file(f, needles, max_sheets=args.max_sheets))

    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)

    if all_hits:
        df = pd.DataFrame([h.__dict__ for h in all_hits]).sort_values(
            ["needle", "hit_count", "file"], ascending=[True, False, True]
        )
    else:
        df = pd.DataFrame(columns=["needle", "file", "sheet", "hit_count"])

    df.to_csv(outp, index=False)
    print(f"Scanned files={len(files)} needles={len(needles)} hits={len(df)}")
    print(f"Wrote: {outp}")


if __name__ == "__main__":
    main()
