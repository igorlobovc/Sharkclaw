#!/usr/bin/env python3
"""Catalog sweep mode: find top matches across the whole Estelita reference catalog.

This is intentionally *not* driven by sure terms. It uses the scored output from
`scripts/score_rows.py` and surfaces the best matches overall.

Outputs
- catalog_sweep_top_matches.csv (top N by tier/score)
- catalog_sweep_summary.txt (tier counts + top ref titles/ids)

Example
python3 scripts/run_catalog_sweep.py \
  --scored ~/Desktop/TempClaw/_excl/scored_master_workset_top400.csv \
  --out-csv ~/Desktop/TempClaw/_excl/catalog_sweep_top_matches.csv \
  --out-summary ~/Desktop/TempClaw/_excl/catalog_sweep_summary.txt \
  --top-n 10000
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def tier_weight(tier: str) -> int:
    t = str(tier or "").strip().lower()
    return {"gold": 3, "silver": 2, "bronze": 1}.get(t, 0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scored", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-summary", required=True)
    ap.add_argument("--top-n", type=int, default=10000)
    args = ap.parse_args()

    scored_path = Path(args.scored).expanduser()
    out_csv = Path(args.out_csv).expanduser()
    out_sum = Path(args.out_summary).expanduser()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_sum.parent.mkdir(parents=True, exist_ok=True)

    # Load only needed columns
    cols = [
        "source_file",
        "source_sheet",
        "source_row",
        "title",
        "artist",
        "author",
        "amount",
        "match_tier",
        "evidence_flags",
        "ref_title_norm",
        "ref_isrc",
        "ref_iswc",
        "isrc",
        "iswc",
    ]

    sample = pd.read_csv(scored_path, nrows=5, dtype=str, low_memory=False).fillna("")
    avail = set(sample.columns)
    usecols = [c for c in cols if c in avail]

    df = pd.read_csv(scored_path, dtype=str, low_memory=False, usecols=usecols).fillna("")

    df["tier_weight"] = df.get("match_tier", "").map(tier_weight)

    # Keep only actual matches
    df = df[df["tier_weight"] > 0]

    # Rank best-first
    df = df.sort_values(["tier_weight"], ascending=[False])

    # Within same tier, prefer rows with more evidence flags and with IDs
    df["has_ref_id"] = df.get("ref_isrc", "").astype(str).str.strip().ne("") | df.get("ref_iswc", "").astype(str).str.strip().ne("")
    df["has_any_id"] = (
        df.get("isrc", "").astype(str).str.strip().ne("")
        | df.get("iswc", "").astype(str).str.strip().ne("")
        | df.get("ref_isrc", "").astype(str).str.strip().ne("")
        | df.get("ref_iswc", "").astype(str).str.strip().ne("")
    )
    df["flags_len"] = df.get("evidence_flags", "").astype(str).str.len()

    df = df.sort_values(["tier_weight", "has_ref_id", "has_any_id", "flags_len"], ascending=[False, False, False, False])

    top = df.head(args.top_n).copy()

    # Output compact columns
    out_cols = [
        "source_file",
        "source_sheet",
        "source_row",
        "title",
        "artist",
        "author",
        "amount",
        "match_tier",
        "tier_weight",
        "evidence_flags",
        "ref_title_norm",
        "ref_isrc",
        "ref_iswc",
        "isrc",
        "iswc",
    ]
    out_cols = [c for c in out_cols if c in top.columns]

    top[out_cols].to_csv(out_csv, index=False)

    # Summary
    lines = []
    lines.append(f"rows_total_scored={len(pd.read_csv(scored_path, usecols=[usecols[0]], dtype=str, low_memory=False))}")
    lines.append(f"rows_matched_any_tier={len(df)}")
    lines.append(f"rows_output_top_n={len(top)}")

    tier_counts = df.get("match_tier", "").value_counts().to_dict()
    lines.append("tier_counts=" + ",".join(f"{k}:{v}" for k, v in tier_counts.items()))

    # Top reference titles
    if "ref_title_norm" in df.columns:
        top_titles = df["ref_title_norm"].astype(str).str.strip()
        vc = top_titles[top_titles.ne("")].value_counts().head(20)
        lines.append("\nTop ref_title_norm by match count:")
        for t, c in vc.items():
            lines.append(f"- {t}: {int(c)}")

    # Top reference IDs
    ref_ids = (df.get("ref_isrc", "").astype(str).str.strip().replace({"nan": ""}) + "|" + df.get("ref_iswc", "").astype(str).str.strip().replace({"nan": ""}))
    vc2 = ref_ids[ref_ids.ne("|")].value_counts().head(20)
    lines.append("\nTop ref_id_key (ref_isrc|ref_iswc) by match count:")
    for k, c in vc2.items():
        lines.append(f"- {k}: {int(c)}")

    out_sum.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote: {out_csv} rows={len(top)}")
    print(f"Wrote: {out_sum}")


if __name__ == "__main__":
    main()
