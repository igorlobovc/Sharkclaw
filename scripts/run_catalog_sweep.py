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

import sys

import pandas as pd

# scripts/ is not a Python package; add it to sys.path for local imports
sys.path.append(str(Path(__file__).resolve().parent))

from entity_overrides import (  # noqa: E402
    apply_noisy_entity_controls,
    classify_entity_override_mode,
    compute_entity_override_hits,
    load_entity_overrides,
)


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

    # Rank helpers
    df["tier_weight"] = df.get("match_tier", "").map(tier_weight)
    df["has_ref_id"] = df.get("ref_isrc", "").astype(str).str.strip().ne("") | df.get("ref_iswc", "").astype(str).str.strip().ne("")
    df["has_any_id"] = (
        df.get("isrc", "").astype(str).str.strip().ne("")
        | df.get("iswc", "").astype(str).str.strip().ne("")
        | df.get("ref_isrc", "").astype(str).str.strip().ne("")
        | df.get("ref_iswc", "").astype(str).str.strip().ne("")
    )
    df["flags_len"] = df.get("evidence_flags", "").astype(str).str.len()

    # Entity override layer (highest priority): include entity hits even if tier_weight==0,
    # and enforce minimum Silver for priority>=4.
    ov_path = Path(__file__).resolve().parent.parent / "config/estelita_entity_overrides.csv"
    overrides = load_entity_overrides(ov_path)

    df, stats_df = compute_entity_override_hits(
        df,
        overrides,
        search_fields=["artist", "author", "publisher", "owner"],
        evidence_field_aliases=["evidence_flags", "evidence_tokens"],
    )

    # Add evidence flag
    def _append_flag(row):
        if int(row.get("entity_override_hit", 0)) != 1:
            return row.get("evidence_flags", "")
        ents = str(row.get("entity_override_entities", "")).strip()
        if not ents:
            return row.get("evidence_flags", "")
        # keep compact: add first 3
        e3 = ";".join(ents.split(";")[:3])
        base = str(row.get("evidence_flags", ""))
        add = f"ENTITY_OVERRIDE_HIT:{e3}"
        return (base + ";" + add).strip(";") if base else add

    df["evidence_flags"] = df.apply(_append_flag, axis=1)

    # Classify entity override mode
    df["entity_override_mode"] = ""
    if "entity_override_hit" in df.columns:
        m = df["entity_override_hit"] == 1
        df.loc[m, "entity_override_mode"] = classify_entity_override_mode(df.loc[m])

    # Promotions: do NOT auto-promote just because priority>=4.
    # Only promote when entity hits AND has a song-level anchor.
    # - ENTITY_PLUS_TITLE: allow min tier Silver
    # - ENTITY_PLUS_ID: allow tiers as usual (no extra gating)
    promoted = pd.Series([False] * len(df), index=df.index)

    m_plus_title = df.get("entity_override_mode", "").astype(str).eq("ENTITY_PLUS_TITLE")
    # ENTITY_PLUS_ID does not need special promotion; it is already a strong anchor.

    # record original tier weight
    df["original_tier_weight"] = df["tier_weight"]

    df.loc[m_plus_title, "tier_weight"] = df.loc[m_plus_title, "tier_weight"].clip(lower=2)
    promoted = promoted | (m_plus_title & (df["tier_weight"] > df["original_tier_weight"]))

    # Apply noisy entity controls (cordel etc.) on entity-hit rows
    df = apply_noisy_entity_controls(
        df,
        overrides,
        rank_cols=["tier_weight", "has_any_id", "flags_len"],
    )

    # Keep only matched rows for sweep (tier_weight>0)
    df = df[df["tier_weight"] > 0]

    # Add promotion columns
    df["promoted_by_entity"] = 0
    df.loc[df["tier_weight"] > df["original_tier_weight"], "promoted_by_entity"] = 1
    df["promotion_reason"] = ""
    df.loc[df["promoted_by_entity"] == 1, "promotion_reason"] = df.loc[
        df["promoted_by_entity"] == 1, "entity_override_mode"
    ]

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
        "original_tier_weight",
        "promoted_by_entity",
        "promotion_reason",
        "entity_override_hit",
        "entity_override_mode",
        "entity_override_best_priority",
        "entity_override_entities",
        "entity_override_hit_fields",
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
