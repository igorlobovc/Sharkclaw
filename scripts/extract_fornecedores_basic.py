#!/usr/bin/env python3
"""Basic extractor for Fornecedores cue-sheet / usage Excel reports.

Scope (v0.1 pilot)
- Works on typical XLS/XLSX supplier reports (Band/SBT/Globo/Globoplay/etc.)
- Emits a normalized flat CSV with provenance so scoring is auditable.

This is NOT a full provider-specific parser; it's a conservative auto-header detector.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


def norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def guess_header_row(preview: pd.DataFrame, max_rows: int = 40) -> int | None:
    # preview: raw rows as strings, columns are 0..N-1
    for i in range(min(max_rows, len(preview))):
        row = preview.iloc[i].astype(str).tolist()
        blob = " | ".join(norm(x) for x in row)
        # must contain a title-ish hint
        if any(k in blob for k in ["obra", "título", "titulo", "musica", "música", "title", "track"]):
            # and likely contain some other field
            if any(k in blob for k in ["isrc", "autor", "author", "artista", "artist", "valor", "amount", "tempo", "duration"]):
                return i
    return None


def pick_col(cols: list[str], candidates: list[str]) -> str | None:
    cols_n = [norm(c) for c in cols]
    for cand in candidates:
        cand_n = norm(cand)
        for orig, c in zip(cols, cols_n):
            if cand_n == c:
                return orig
        for orig, c in zip(cols, cols_n):
            if cand_n in c:
                return orig
    return None


def extract_one_file(path: Path) -> pd.DataFrame:
    rows = []
    xls = pd.ExcelFile(path)

    for sheet in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sheet, header=None, dtype=str)
        raw = raw.fillna("")
        header_i = guess_header_row(raw)
        if header_i is None:
            continue

        df = pd.read_excel(xls, sheet_name=sheet, header=header_i, dtype=str)
        df = df.fillna("")
        cols = list(df.columns)

        c_title = pick_col(cols, ["obra", "título", "titulo", "musica", "música", "title", "track", "nome da obra"])
        c_artist = pick_col(cols, ["artista", "artist", "interprete", "intérprete", "performer", "banda"])
        c_author = pick_col(cols, ["autor", "author", "compositor", "composer"])
        c_isrc = pick_col(cols, ["isrc"])
        c_iswc = pick_col(cols, ["iswc"])
        c_amount = pick_col(cols, ["valor", "amount", "total", "r$", "bruto"])

        if c_title is None:
            continue

        for idx, r in df.iterrows():
            title = r.get(c_title, "")
            if not norm(title):
                continue
            out = {
                "source_file": str(path),
                "source_sheet": str(sheet),
                "source_row": str(int(idx) + 1),
                "title": str(title),
                "artist": str(r.get(c_artist, "")) if c_artist else "",
                "author": str(r.get(c_author, "")) if c_author else "",
                "isrc": str(r.get(c_isrc, "")) if c_isrc else "",
                "iswc": str(r.get(c_iswc, "")) if c_iswc else "",
                "amount": str(r.get(c_amount, "")) if c_amount else "",
            }
            rows.append(out)

    return pd.DataFrame(rows)


def iter_inputs(values: list[str]) -> list[Path]:
    out: list[Path] = []
    for v in values:
        v = str(v)
        p = Path(v).expanduser()
        # allow glob patterns
        if any(ch in v for ch in ["*", "?", "["]):
            out.extend(Path().glob(v))
            continue
        if p.is_dir():
            out.extend([x for x in p.rglob("*") if x.is_file()])
        else:
            out.append(p)

    exts = {".xls", ".xlsx"}
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        action="append",
        required=True,
        help="Input XLS/XLSX file/dir/glob (repeatable).",
    )
    ap.add_argument("--output", required=True, help="CSV output (combined)")
    ap.add_argument(
        "--needles",
        default="",
        help="Optional comma-separated list of titles to sanity-check in extracted rows.",
    )
    args = ap.parse_args()

    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)

    inputs = iter_inputs(args.input)
    if not inputs:
        raise SystemExit("No XLS/XLSX inputs found")

    dfs = []
    for inp in inputs:
        df = extract_one_file(inp)
        if len(df):
            dfs.append(df)

    out_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(
        columns=[
            "source_file",
            "source_sheet",
            "source_row",
            "title",
            "artist",
            "author",
            "isrc",
            "iswc",
            "amount",
        ]
    )

    out_df.to_csv(outp, index=False)
    print(f"Wrote: {outp} rows={len(out_df)} files={len(inputs)}")

    needles = [x.strip() for x in str(args.needles).split(",") if x.strip()]
    if needles:
        import re

        def _contains(title: str, needle: str) -> bool:
            return re.search(re.escape(needle), str(title), flags=re.IGNORECASE) is not None

        print("\nSanity check (needles):")
        for needle in needles:
            n = int(out_df["title"].apply(lambda t: _contains(t, needle)).sum()) if len(out_df) else 0
            print(f"  {needle}: {n}")


if __name__ == "__main__":
    main()
