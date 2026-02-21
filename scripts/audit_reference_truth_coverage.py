#!/usr/bin/env python3
"""Audit coverage of Reference Truth evidence fields.

Outputs a concise text report so we can see why matches are/aren't surfacing
(e.g., why ORG hits might be 0, or why ID anchoring is rare).

Input default:
- runs/reference/reference_truth_enriched_clean.csv

Output:
- stdout (and optional --out file)
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def nonempty(series: pd.Series) -> int:
    return int(series.astype(str).str.strip().replace({"nan": ""}).ne("").sum())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--truth", default="runs/reference/reference_truth_enriched_clean.csv")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    truth_path = Path(args.truth)
    if not truth_path.is_absolute():
        truth_path = (Path(__file__).resolve().parent.parent / truth_path).resolve()

    df = pd.read_csv(truth_path, dtype=str, low_memory=False).fillna("")

    lines = []
    lines.append(f"truth_path={truth_path}")
    lines.append(f"rows={len(df)}")
    lines.append("")

    for col in ["title_raw", "title_norm", "isrc", "iswc", "evidence_tokens", "source", "source_detail"]:
        if col in df.columns:
            lines.append(f"nonempty_{col}={nonempty(df[col])}")

    # evidence_tokens rough stats
    if "evidence_tokens" in df.columns:
        tok = df["evidence_tokens"].astype(str).str.strip()
        lines.append("")
        lines.append(f"evidence_tokens_nonempty={int(tok.ne('').sum())}")
        lines.append(f"evidence_tokens_avg_len={tok.map(len).mean():.2f}")

    # ID coverage
    if "isrc" in df.columns and "iswc" in df.columns:
        isrc = df["isrc"].astype(str).str.strip()
        iswc = df["iswc"].astype(str).str.strip()
        lines.append("")
        lines.append(f"rows_with_any_id={int((isrc.ne('') | iswc.ne('')).sum())}")
        lines.append(f"rows_with_isrc={int(isrc.ne('').sum())}")
        lines.append(f"rows_with_iswc={int(iswc.ne('').sum())}")

    out_text = "\n".join(lines) + "\n"
    print(out_text)

    if args.out:
        out_path = Path(args.out).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(out_text, encoding="utf-8")


if __name__ == "__main__":
    main()
