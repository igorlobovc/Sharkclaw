#!/usr/bin/env python3
"""Audit catalog size for the canonical Reference Truth CSV.

Writes:
- runs/reference/catalog_size.json
- runs/reference/catalog_size.txt

By default reads:
- runs/reference/reference_truth_enriched_clean.csv

This script is intentionally robust to column presence.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def nonempty(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().replace({"nan": ""}).ne("")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--truth", default="runs/reference/reference_truth_enriched_clean.csv")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    truth_path = Path(args.truth)
    if not truth_path.is_absolute():
        truth_path = (repo_root / truth_path).resolve()

    df = pd.read_csv(truth_path, dtype=str, low_memory=False).fillna("")

    def uniq(col: str) -> int:
        if col not in df.columns:
            return 0
        s = df[col].astype(str).str.strip().replace({"nan": ""})
        s = s[s.ne("")]
        return int(s.nunique())

    total_rows = int(len(df))
    unique_titles = uniq("title_norm") if "title_norm" in df.columns else uniq("title_raw")
    unique_isrc = uniq("isrc")
    unique_iswc = uniq("iswc")

    rows_with_any_id = 0
    if "isrc" in df.columns or "iswc" in df.columns:
        isrc = df.get("isrc", "")
        iswc = df.get("iswc", "")
        rows_with_any_id = int((nonempty(isrc) | nonempty(iswc)).sum())

    # token fields if present
    rows_with_artist_tokens = 0
    rows_with_author_tokens = 0
    rows_with_publisher_tokens = 0

    # If the truth has explicit token columns, use them; otherwise attempt to infer from evidence_tokens
    if "artist_tokens" in df.columns:
        rows_with_artist_tokens = int(nonempty(df["artist_tokens"]).sum())
    if "author_tokens" in df.columns:
        rows_with_author_tokens = int(nonempty(df["author_tokens"]).sum())
    if "publisher_tokens" in df.columns:
        rows_with_publisher_tokens = int(nonempty(df["publisher_tokens"]).sum())

    if (rows_with_artist_tokens, rows_with_author_tokens, rows_with_publisher_tokens) == (0, 0, 0) and "evidence_tokens" in df.columns:
        ev = df["evidence_tokens"].astype(str)
        # crude inference: presence of labels in token blob
        rows_with_artist_tokens = int(ev.str.contains("artist", case=False, na=False).sum())
        rows_with_author_tokens = int(ev.str.contains("author|composer", case=False, na=False, regex=True).sum())
        rows_with_publisher_tokens = int(ev.str.contains("publisher|editora", case=False, na=False, regex=True).sum())

    out = {
        "truth_path": str(truth_path),
        "total_rows": total_rows,
        "unique_titles": unique_titles,
        "unique_isrc": unique_isrc,
        "unique_iswc": unique_iswc,
        "rows_with_any_id": rows_with_any_id,
        "rows_with_artist_tokens": rows_with_artist_tokens,
        "rows_with_author_tokens": rows_with_author_tokens,
        "rows_with_publisher_tokens": rows_with_publisher_tokens,
        "columns": list(df.columns),
    }

    out_dir = truth_path.parent
    json_path = out_dir / "catalog_size.json"
    txt_path = out_dir / "catalog_size.txt"

    json_path.write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    lines = [
        f"truth_path={out['truth_path']}",
        f"total_rows={total_rows}",
        f"unique_titles={unique_titles}",
        f"unique_isrc={unique_isrc}",
        f"unique_iswc={unique_iswc}",
        f"rows_with_any_id={rows_with_any_id}",
        f"rows_with_artist_tokens={rows_with_artist_tokens}",
        f"rows_with_author_tokens={rows_with_author_tokens}",
        f"rows_with_publisher_tokens={rows_with_publisher_tokens}",
    ]
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(txt_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
