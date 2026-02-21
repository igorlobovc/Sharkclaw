#!/usr/bin/env python3
"""Create a compact "review slice" from a scored fornecedor dataset.

Filters rows where the (normalized) title contains any configured sure term.

Inputs
- --scored: scored CSV (e.g. TempClaw/scored_master_workset_top400.csv)
- --sure: sure-match catalog CSV (config/sure_match_catalog.csv)
- --out: output CSV

Notes
- This is a review/audit helper. It does not change scoring.
- "score" output is a simple numeric encoding of tier:
  Gold=3, Silver=2, Bronze=1, NoMatch/other=0
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


def build_terms(sure_df: pd.DataFrame) -> list[str]:
    # Prefer title-type terms; fall back to any term if no titles are present.
    sure_df = sure_df.fillna("")
    titles = sure_df[sure_df["kind"].str.lower() == "title"]["term"].tolist() if "kind" in sure_df.columns else []
    terms = titles if titles else sure_df["term"].tolist()
    terms = [t for t in (str(x).strip() for x in terms) if t]
    # de-dupe case-insensitive
    seen = set()
    out = []
    for t in sorted(terms, key=len, reverse=True):
        k = t.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def tier_score(tier: str) -> int:
    t = (tier or "").strip().lower()
    return {"gold": 3, "silver": 2, "bronze": 1}.get(t, 0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scored", required=True)
    ap.add_argument("--sure", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    scored_path = Path(args.scored)
    sure_path = Path(args.sure)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sure_df = pd.read_csv(sure_path, dtype=str, low_memory=False).fillna("")
    terms = build_terms(sure_df)
    if not terms:
        raise SystemExit("No sure terms found")

    # compile regex of terms (escaped) for title substring match
    pat = re.compile("|".join(re.escape(t) for t in terms), flags=re.IGNORECASE)

    df = pd.read_csv(scored_path, dtype=str, low_memory=False).fillna("")
    if "title" not in df.columns:
        raise SystemExit("scored CSV must contain a 'title' column")

    # fast filter: regex contains on title
    mask = df["title"].astype(str).str.contains(pat, na=False)
    sub = df.loc[mask].copy()

    # compute numeric score
    sub["score"] = sub.get("match_tier", "").apply(tier_score)

    # rename/provide compact columns
    out = pd.DataFrame(
        {
            "fornecedor_file": sub.get("source_file", ""),
            "sheet": sub.get("source_sheet", ""),
            "row_id": sub.get("source_row", ""),
            "title": sub.get("title", ""),
            "matched_title": sub.get("ref_title_norm", ""),
            "tier": sub.get("match_tier", ""),
            "score": sub.get("score", ""),
            "evidence_flags": sub.get("evidence_flags", ""),
            "isrc": sub.get("isrc", ""),
            "iswc": sub.get("iswc", ""),
            "ref_isrc": sub.get("ref_isrc", ""),
            "ref_iswc": sub.get("ref_iswc", ""),
        }
    )

    # sort best-first
    sort_cols = [c for c in ["score", "tier", "title"] if c in out.columns]
    out = out.sort_values(by=sort_cols, ascending=[False, True, True])

    out.to_csv(out_path, index=False)

    counts = out["tier"].value_counts().to_dict() if len(out) else {}
    print(f"Wrote: {out_path} rows={len(out)} tiers={counts}")


if __name__ == "__main__":
    main()
