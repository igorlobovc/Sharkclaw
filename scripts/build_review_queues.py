#!/usr/bin/env python3
"""Build practical review queues from a scored dataset using sure-term matching.

Outputs:
A) reviewable wins (Gold/Silver/Bronze per --min-tier)
B) high-signal PERSON hits even if NoMatch (evidence-based)

This script is intended to be run on large scored CSVs produced by score_rows.py.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def tier_weight(tier: str) -> int:
    t = str(tier or "").strip().lower()
    return {"gold": 3, "silver": 2, "bronze": 1}.get(t, 0)


def has_id_evidence(row: pd.Series) -> bool:
    for c in ("isrc", "iswc", "ref_isrc", "ref_iswc"):
        if c in row.index and str(row.get(c, "")).strip():
            return True
    return False


def flag_contains(row: pd.Series, needle: str) -> bool:
    s = str(row.get("evidence_flags", ""))
    return needle.lower() in s.lower()


def add_rank_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["tier_weight"] = out.get("tier", "").map(tier_weight)
    out["has_id_evidence"] = out.apply(has_id_evidence, axis=1)
    out["has_title_exact"] = out.get("evidence_flags", "").astype(str).str.contains("TITLE_EXACT", case=False, na=False)
    out["has_artist_overlap"] = out.get("evidence_flags", "").astype(str).str.contains("ARTIST_TOKEN_OVERLAP", case=False, na=False)

    # rank_key is represented by sortable columns
    # sort: tier_weight desc, score desc, has_id_evidence desc, TITLE_EXACT desc, ARTIST_TOKEN_OVERLAP desc
    out["score_num"] = pd.to_numeric(out.get("score", 0), errors="coerce").fillna(0).astype(int)
    out = out.sort_values(
        ["tier_weight", "score_num", "has_id_evidence", "has_title_exact", "has_artist_overlap"],
        ascending=[False, False, False, False, False],
    )
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slice", required=True, help="review slice csv produced by slice_scored_by_sure_terms.py")
    ap.add_argument("--out-wins", required=True)
    ap.add_argument("--out-person-evidence", required=True)
    ap.add_argument("--min-tier", default="Silver", choices=["Gold", "Silver", "Bronze", "NoMatch"])
    ap.add_argument("--summary-out", required=True)
    args = ap.parse_args()

    slice_df = pd.read_csv(args.slice, dtype=str, low_memory=False).fillna("")

    min_w = tier_weight(args.min_tier)

    # A) wins
    wins = slice_df.copy()
    wins["tier_weight"] = wins.get("tier", "").map(tier_weight)
    wins = wins[wins["tier_weight"] >= min_w]
    wins = add_rank_cols(wins)

    # B) person-evidence even if NoMatch
    pe = slice_df.copy()
    pe = pe[
        pe.get("evidence_flags", "").astype(str).str.contains("ARTIST_TOKEN_OVERLAP", case=False, na=False)
        | (
            pe.get("isrc", "").astype(str).str.strip().ne("")
            | pe.get("iswc", "").astype(str).str.strip().ne("")
            | pe.get("ref_isrc", "").astype(str).str.strip().ne("")
            | pe.get("ref_iswc", "").astype(str).str.strip().ne("")
        )
    ]
    pe = add_rank_cols(pe)

    out_wins = Path(args.out_wins)
    out_pe = Path(args.out_person_evidence)
    out_wins.parent.mkdir(parents=True, exist_ok=True)
    out_pe.parent.mkdir(parents=True, exist_ok=True)

    wins.to_csv(out_wins, index=False)
    pe.to_csv(out_pe, index=False)

    # summary
    def tier_dist(d: pd.DataFrame) -> str:
        if len(d) == 0 or "tier" not in d.columns:
            return ""
        vc = d["tier"].value_counts().to_dict()
        return ",".join(f"{k}:{v}" for k, v in vc.items())

    summary = []
    summary.append(f"wins_rows={len(wins)}")
    summary.append(f"wins_tiers={tier_dist(wins)}")
    summary.append(f"person_evidence_rows={len(pe)}")
    summary.append(f"person_evidence_tiers={tier_dist(pe)}")

    Path(args.summary_out).write_text("\n".join(summary) + "\n", encoding="utf-8")

    print(f"Wrote: {out_wins} rows={len(wins)}")
    print(f"Wrote: {out_pe} rows={len(pe)}")
    print(f"Wrote: {args.summary_out}")


if __name__ == "__main__":
    main()
