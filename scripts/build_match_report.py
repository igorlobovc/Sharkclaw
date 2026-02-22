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

import sys

import pandas as pd

# scripts/ is not a Python package; add it to sys.path for local imports
sys.path.append(str(Path(__file__).resolve().parent))

from entity_overrides import compute_entity_override_hits, load_entity_overrides, load_top_entities  # noqa: E402


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

    # Entity override hits (audit)
    ov_path = Path(__file__).resolve().parent.parent / "config/estelita_entity_overrides.csv"
    overrides = load_entity_overrides(ov_path)
    rows2, stats_df = compute_entity_override_hits(
        rows,
        overrides,
        search_fields=["artist", "author", "publisher", "owner"],
        evidence_field_aliases=["evidence_flags", "evidence_tokens"],
    )
    rows = rows2

    # ALWAYS_MATCH_POLICY: TOP entities + previously reviewed matches
    top_path = Path(__file__).resolve().parent.parent / "config/top_estelita_entities.csv"
    top_entities = load_top_entities(top_path)
    # Expand searched fields: scan common people/entity columns and any column whose header
    # suggests it may contain names/entities.
    top_hits, top_stats = compute_entity_override_hits(
        rows,
        top_entities,
        search_fields=["artist", "author", "publisher", "owner"],
        evidence_field_aliases=["evidence_flags", "evidence_tokens"],
        include_columns_matching=r"artista|autor|compositor|interprete|int[ée]rprete|titular|particip|editora|publisher|owner|direito|obra|produtor|produc|observ|notas|repert[óo]rio|nome_",
    )

    # rename TOP columns
    rows["top_entity_hit"] = top_hits["entity_override_hit"]
    rows["top_entity_best_priority"] = top_hits["entity_override_best_priority"]
    rows["top_entity_entities"] = top_hits["entity_override_entities"]
    rows["top_entity_hit_fields"] = top_hits["entity_override_hit_fields"]

    # previously reviewed (may be empty)
    prev_path = Path(__file__).resolve().parent.parent / "config/previously_reviewed_matches.csv"
    prev = pd.read_csv(prev_path, dtype=str, low_memory=False).fillna("") if prev_path.exists() else pd.DataFrame()

    def _ref_id_key(df: pd.DataFrame) -> pd.Series:
        return df.get("ref_isrc", "").astype(str).str.strip() + "|" + df.get("ref_iswc", "").astype(str).str.strip()

    rows["match_reason"] = ""
    rows["is_match"] = 0

    # previously reviewed match by ref IDs (exact)
    if len(prev):
        prev_ids = set(
            (
                prev.get("ref_isrc", "").astype(str).str.strip() + "|" + prev.get("ref_iswc", "").astype(str).str.strip()
            ).tolist()
        )
        rid = _ref_id_key(rows)
        m_prev = rid.isin(prev_ids) & (rid != "|")
        rows.loc[m_prev, "is_match"] = 1
        rows.loc[m_prev, "match_reason"] = "PREVIOUSLY_REVIEWED"

    # top entity hit forces match (bypass all gating)
    m_top = rows["top_entity_hit"] == 1
    rows.loc[m_top, "is_match"] = 1
    rows.loc[m_top & (rows["match_reason"] == ""), "match_reason"] = "TOP_ENTITY"

    # Write match_report_rows.csv
    rows_out = out_dir / "match_report_rows.csv"
    out_cols = base_cols + [
        "rank_key",
        "provider_guess",
        "entity_override_hit",
        "entity_override_best_priority",
        "entity_override_entities",
        "entity_override_hit_fields",
    ]
    out_cols = [c for c in out_cols if c in rows.columns]
    rows[out_cols].to_csv(rows_out, index=False)

    # Entity override outputs (so we can verify quickly)
    # entity_override_hits.csv: all rows with entity_override_hit=1, ranked best-first, dedup by row key
    if "entity_override_hit" in rows.columns:
        eh = rows[rows["entity_override_hit"] == 1].copy()
        if len(eh):
            eh["_k"] = eh["fornecedor_file"] + "||" + eh["sheet"] + "||" + eh["row_id"].astype(str)
            eh = eh.drop_duplicates("_k")
            eh = compute_rank(eh)
            eh[out_cols].to_csv(out_dir / "entity_override_hits.csv", index=False)

    # entity_override_hit_counts.csv
    if len(stats_df):
        # add tier distribution per entity
        if "entity_override_entities" in rows.columns:
            tier_map = {}
            for ent in stats_df["entity_norm"].tolist():
                m = rows["entity_override_entities"].astype(str).str.contains(ent, na=False)
                vc = rows.loc[m, "tier"].value_counts().to_dict()
                tier_map[ent] = ",".join(f"{k}:{v}" for k, v in vc.items())
            stats_df["tier_distribution"] = stats_df["entity_norm"].map(lambda e: tier_map.get(e, ""))

        stats_df.to_csv(out_dir / "entity_override_hit_counts.csv", index=False)

    # ALWAYS_MATCH_POLICY outputs
    # top_entity_matches.csv: all rows forced match by TOP_ENTITY or PREVIOUSLY_REVIEWED
    forced = rows[rows["is_match"] == 1].copy()
    forced["_k"] = forced["fornecedor_file"] + "||" + forced["sheet"] + "||" + forced["row_id"].astype(str)
    forced_dedup = forced.drop_duplicates("_k")

    forced_rank = compute_rank(forced_dedup)

    forced_cols = [
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
        "match_reason",
        "top_entity_hit",
        "top_entity_best_priority",
        "top_entity_entities",
        "top_entity_hit_fields",
    ]
    forced_cols = [c for c in forced_cols if c in forced_rank.columns]

    (out_dir / "top_entity_matches.csv").write_text("", encoding="utf-8")
    forced_rank[forced_cols].to_csv(out_dir / "top_entity_matches.csv", index=False)

    # summary
    lines = []
    lines.append(f"total_rows={len(forced)}")
    lines.append(f"dedup_rows={len(forced_dedup)}")
    # match_reason breakdown
    mr = forced["match_reason"].value_counts().to_dict()
    lines.append("count_by_match_reason=" + ",".join(f"{k}:{v}" for k, v in mr.items()))

    # top entities by count
    ent_counts = {}
    for s in forced.get("top_entity_entities", "").astype(str):
        for e in [x for x in s.split(";") if x.strip()]:
            ent_counts[e] = ent_counts.get(e, 0) + 1
    top_ents = sorted(ent_counts.items(), key=lambda x: x[1], reverse=True)[:20]
    lines.append("top_entities_by_count=" + "; ".join(f"{e}:{c}" for e, c in top_ents))

    # top files by count
    top_files = forced["fornecedor_file"].value_counts().head(10).to_dict()
    lines.append("top_fornecedor_files_by_count=" + "; ".join(f"{f}:{c}" for f, c in top_files.items()))

    # breakdown by hit_field
    hb = {}
    for s in forced.get("top_entity_hit_fields", "").astype(str):
        for item in [x for x in s.split(";") if x.strip()]:
            # entity@field
            parts = item.split("@")
            if len(parts) == 2:
                hb[parts[1]] = hb.get(parts[1], 0) + 1
    lines.append("breakdown_by_hit_field=" + ",".join(f"{k}:{v}" for k, v in sorted(hb.items(), key=lambda x: x[1], reverse=True)))

    (out_dir / "top_entity_matches_summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Field coverage + raw variants audit
    # top_entity_field_coverage.csv: per entity stats + top source columns
    if len(top_stats):
        top_stats.to_csv(out_dir / "top_entity_field_coverage.csv", index=False)

    # top_entity_raw_variants.csv: collect distinct raw forms observed for each entity
    variants = []
    if len(top_entities):
        # scan the same columns we scanned for hits
        cols_scanned = [
            c
            for c in rows.columns
            if re.search(
                r"artista|autor|compositor|interprete|int[ée]rprete|titular|particip|editora|publisher|owner|direito|obra|produtor|produc|observ|notas|repert[óo]rio|nome_",
                str(c),
                flags=re.IGNORECASE,
            )
        ]
        cols_scanned += [c for c in ["artist", "author", "publisher", "owner", "evidence_flags", "evidence_tokens"] if c in rows.columns]
        cols_scanned = list(dict.fromkeys(cols_scanned))

        for ent in top_entities:
            ent_norm = ent.entity_norm
            # rows where this entity hit
            m = rows.get("top_entity_entities", "").astype(str).str.contains(ent_norm, na=False)
            if not int(m.sum()):
                continue
            sub = rows.loc[m, cols_scanned].copy()
            seen = set()
            for c in cols_scanned:
                for v in sub[c].dropna().astype(str).tolist():
                    if not v.strip():
                        continue
                    # only keep variants that plausibly contain the entity tokens
                    if ent_norm.replace(" ", "") not in norm(v).replace(" ", ""):
                        continue
                    vv = v.strip()
                    if vv in seen:
                        continue
                    seen.add(vv)
                    variants.append({"entity_norm": ent_norm, "source_column": c, "raw_variant": vv})
                    if len(seen) >= 50:
                        break

    pd.DataFrame(variants).to_csv(out_dir / "top_entity_raw_variants.csv", index=False)

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
