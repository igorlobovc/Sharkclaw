#!/usr/bin/env python3
"""Build a human-reviewable match report package from a scored run.

Inputs
- --scored: scored_master_workset_topN.csv (full scored rows)
- --sweep: catalog_sweep_top_matches.csv (top matches selected by sweep)
- --out-dir: report_package folder

Outputs (in out-dir)
- match_report_overview.md
- match_report_rows.csv
- match_report_dedup.csv
- match_report_suspects.csv
- match_report_truth_gaps.csv

Also supports generating an action sheet:
- action_sheet.csv (all Gold + top 300 Silver, deduped)

Notes
- We trust the sweep CSV to already be filtered to matched tiers.
- Provider guess is path-based heuristic.
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from pathlib import Path

import pandas as pd


PROVIDERS = [
    ("band", re.compile(r"\bband\b|bandeirantes", re.I)),
    ("sbt", re.compile(r"\bsbt\b", re.I)),
    ("globo", re.compile(r"\bglobo\b|canais globo", re.I)),
    ("globoplay", re.compile(r"globoplay", re.I)),
    ("record", re.compile(r"\brecord\b", re.I)),
    ("canalbrasil", re.compile(r"canal\s*brasil", re.I)),
    ("ubem", re.compile(r"ubem", re.I)),
    ("ecad", re.compile(r"ecad", re.I)),
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


def norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = strip_accents(s.casefold().strip())
    s = re.sub(r"\s+", " ", s)
    return s


def tier_weight(tier: str) -> int:
    t = (tier or "").strip().lower()
    return {"gold": 3, "silver": 2, "bronze": 1}.get(t, 0)


def compute_rank(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["tier_weight"] = out["tier"].map(tier_weight)
    out["score_num"] = pd.to_numeric(out.get("tier_weight", 0), errors="coerce").fillna(0).astype(int)
    flags = out.get("evidence_flags", "").astype(str)
    out["has_id_evidence"] = (
        out.get("isrc", "").astype(str).str.strip().ne("")
        | out.get("iswc", "").astype(str).str.strip().ne("")
        | out.get("ref_isrc", "").astype(str).str.strip().ne("")
        | out.get("ref_iswc", "").astype(str).str.strip().ne("")
    )
    out["has_title_exact"] = flags.str.contains("TITLE_EXACT", case=False, na=False)
    out["has_artist_overlap"] = flags.str.contains("ARTIST_TOKEN_OVERLAP", case=False, na=False)

    # rank_key as a string for readability + stable sort columns
    out["rank_key"] = (
        out["tier_weight"].astype(str)
        + "|id="
        + out["has_id_evidence"].astype(int).astype(str)
        + "|te="
        + out["has_title_exact"].astype(int).astype(str)
        + "|ao="
        + out["has_artist_overlap"].astype(int).astype(str)
    )

    out = out.sort_values(
        ["tier_weight", "has_id_evidence", "has_title_exact", "has_artist_overlap"],
        ascending=[False, False, False, False],
    )
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scored", required=True)
    ap.add_argument("--sweep", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--top-silver", type=int, default=300)
    args = ap.parse_args()

    # scored is currently not used because sweep already contains all row fields we need.
    # Keep arg for future mode where we re-join from full scored.
    sweep = Path(args.sweep).expanduser()
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Sweep already contains the rows of interest (with source_file/sheet/row)
    sw = pd.read_csv(sweep, dtype=str, low_memory=False).fillna("")

    # Normalize sweep columns to report schema
    colmap = {
        "source_file": "fornecedor_file",
        "source_sheet": "sheet",
        "source_row": "row_id",
        "ref_title_norm": "matched_title",
        "match_tier": "tier",
    }
    for a, b in colmap.items():
        if a in sw.columns and b not in sw.columns:
            sw[b] = sw[a]

    # Ensure base columns
    base_cols = [
        "fornecedor_file",
        "sheet",
        "row_id",
        "title",
        "artist",
        "author",
        "matched_title",
        "tier",
        "evidence_flags",
        "isrc",
        "iswc",
        "ref_isrc",
        "ref_iswc",
    ]
    for c in base_cols:
        if c not in sw.columns:
            sw[c] = ""

    sw["provider_guess"] = sw["fornecedor_file"].map(guess_provider)

    rows = compute_rank(sw[base_cols + ["provider_guess"]].copy())

    # Write match_report_rows.csv
    rows_out = out_dir / "match_report_rows.csv"
    out_cols = base_cols + ["rank_key", "provider_guess"]
    rows[out_cols].to_csv(rows_out, index=False)

    # Dedup report
    d = rows.copy()
    d["matched_title_norm"] = d["matched_title"].map(norm)
    d["ref_id_key"] = d.get("ref_isrc", "").astype(str).str.strip() + "|" + d.get("ref_iswc", "").astype(str).str.strip()
    d["group_key"] = d["matched_title_norm"] + "|" + d["ref_id_key"]

    def top_files(series, n=5):
        vc = series.value_counts().head(n)
        return "; ".join([f"{idx}:{cnt}" for idx, cnt in vc.items()])

    dedup = d.groupby("group_key", as_index=False).agg(
        total_occurrences=("group_key", "count"),
        distinct_files=("fornecedor_file", lambda x: x.nunique()),
        top_files=("fornecedor_file", top_files),
        example_matched_title=("matched_title", "first"),
        ref_isrc=("ref_isrc", "first"),
        ref_iswc=("ref_iswc", "first"),
    )
    dedup = dedup.sort_values(["total_occurrences", "distinct_files"], ascending=[False, False])
    dedup_out = out_dir / "match_report_dedup.csv"
    dedup.to_csv(dedup_out, index=False)

    # Evidence flags
    flags = rows.get("evidence_flags", "").astype(str)
    has_ref_id = rows.get("ref_isrc", "").astype(str).str.strip().ne("") | rows.get("ref_iswc", "").astype(str).str.strip().ne("")
    # has_any_id could be useful later for QC; currently unused.
    has_title_exact = flags.str.contains("TITLE_EXACT", case=False, na=False)
    has_artist_overlap = flags.str.contains("ARTIST_TOKEN_OVERLAP", case=False, na=False)

    # Truth gaps: strong evidence but missing reference IDs
    truth_gaps = rows[
        (rows["tier"].str.lower().isin(["gold", "silver"]))
        & has_title_exact
        & has_artist_overlap
        & (~has_ref_id)
    ].copy()
    truth_gaps_out = out_dir / "match_report_truth_gaps.csv"
    truth_gaps[out_cols].to_csv(truth_gaps_out, index=False)

    # Suspects: weak-evidence rows only (quality control)
    suspects = rows[
        (
            rows["tier"].str.lower().isin(["gold", "silver"])
            & (~has_title_exact)
            & (~has_artist_overlap)
        )
        | ((rows["tier"].str.lower() == "silver") & (~has_title_exact) & (~has_artist_overlap))
    ].copy()

    suspects_out = out_dir / "match_report_suspects.csv"
    suspects[out_cols].to_csv(suspects_out, index=False)

    # Overview
    tier_counts = rows["tier"].value_counts().to_dict()
    ref_ids = rows.get("ref_isrc", "").astype(str).str.strip() + "|" + rows.get("ref_iswc", "").astype(str).str.strip()
    nonempty_ref_id = ref_ids != "|"
    unique_ref_id_key_count = int(ref_ids[nonempty_ref_id].nunique())

    unique_ref_title_norm_count = int(
        rows.get("matched_title", "").astype(str).str.strip().replace({"nan": ""}).map(norm).replace({"": None}).dropna().nunique()
    )

    top_ref_ids = ref_ids[nonempty_ref_id].value_counts().head(20)

    top_files_by_matches = rows["fornecedor_file"].value_counts().head(20)
    top_files_by_gold = rows[rows["tier"].str.lower() == "gold"]["fornecedor_file"].value_counts().head(20)

    # Top ref titles by UNIQUE fornecedor_file count (breadth)
    breadth = rows.copy()
    breadth["matched_title_norm"] = breadth["matched_title"].map(norm)
    breadth = breadth[breadth["matched_title_norm"].ne("")]
    breadth_vc = breadth.groupby("matched_title_norm")["fornecedor_file"].nunique().sort_values(ascending=False).head(20)

    overview = []
    overview.append("# Match Report Overview\n")
    overview.append(f"- rows_in_sweep: {len(rows)}")
    overview.append("- matches_by_tier: " + ", ".join(f"{k}:{v}" for k, v in tier_counts.items()))
    overview.append(f"- unique_ref_id_key_count: {unique_ref_id_key_count}")
    overview.append(f"- unique_ref_title_norm_count: {unique_ref_title_norm_count}")

    overview.append("\n## Top 20 ref IDs (ref_isrc|ref_iswc)\n")
    for k, v in top_ref_ids.items():
        overview.append(f"- {k}: {int(v)}")

    overview.append("\n## Top 20 fornecedor files by #matches\n")
    for k, v in top_files_by_matches.items():
        overview.append(f"- {k}: {int(v)}")

    overview.append("\n## Top 20 fornecedor files by #Gold\n")
    for k, v in top_files_by_gold.items():
        overview.append(f"- {k}: {int(v)}")

    overview.append("\n## Top 20 ref titles by UNIQUE fornecedor_file count (breadth)\n")
    for t, c in breadth_vc.items():
        overview.append(f"- {t}: {int(c)}")

    (out_dir / "match_report_overview.md").write_text("\n".join(overview) + "\n", encoding="utf-8")

    # Action sheet
    gold = rows[rows["tier"].str.lower() == "gold"].copy()
    silver = rows[rows["tier"].str.lower() == "silver"].copy()

    # Dedup by fornecedor_file+sheet+row_id
    gold["_k"] = gold["fornecedor_file"] + "||" + gold["sheet"] + "||" + gold["row_id"].astype(str)
    silver["_k"] = silver["fornecedor_file"] + "||" + silver["sheet"] + "||" + silver["row_id"].astype(str)

    silver = silver[~silver["_k"].isin(set(gold["_k"]))]
    silver = silver.head(args.top_silver)

    action = pd.concat([gold, silver], ignore_index=True)
    for c in ["decision", "reviewer_notes", "invoice_bucket"]:
        action[c] = ""

    action_cols = out_cols + ["decision", "reviewer_notes", "invoice_bucket"]
    action[action_cols].to_csv(out_dir / "action_sheet.csv", index=False)

    print(f"Wrote: {rows_out} rows={len(rows)}")
    print(f"Wrote: {dedup_out} rows={len(dedup)}")
    print(f"Wrote: {suspects_out} rows={len(suspects)}")
    print(f"Wrote: {out_dir / 'action_sheet.csv'} rows={len(action)}")


if __name__ == "__main__":
    main()
