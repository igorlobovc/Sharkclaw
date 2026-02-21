#!/usr/bin/env python3
"""Build practical review queues from a scored dataset using sure-term matching.

Outputs:
A) reviewable wins (Gold/Silver/Bronze per --min-tier)
B) high-signal PERSON hits even if NoMatch (evidence-based)

This script is intended to be run on large scored CSVs produced by score_rows.py.
"""

from __future__ import annotations

import argparse
import re
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
    ap.add_argument("--sure", default="", help="Optional sure catalog (for term capping on person-evidence)")
    ap.add_argument("--overrides", default="config/sure_match_overrides.csv")
    ap.add_argument("--out-wins", required=True)
    ap.add_argument("--out-person-evidence", required=True)
    ap.add_argument("--out-strong-nomatch", required=True)
    ap.add_argument("--top-terms-out", required=True)
    ap.add_argument("--min-tier", default="Silver", choices=["Gold", "Silver", "Bronze", "NoMatch"])
    ap.add_argument("--person-evidence-global-cap", type=int, default=50000)
    ap.add_argument("--person-evidence-per-term-cap", type=int, default=2000)
    ap.add_argument("--strong-nomatch-cap", type=int, default=10000)
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

    # Cap person-evidence to be reviewable.
    # We don't have full term attribution in the slice, so we approximate "per term" using
    # sure TITLE terms matched in (title/evidence_flags). Cordel terms should already be
    # controlled by overrides in the slice.
    kept_by_term = {}
    if args.sure:
        # Local import helper (scripts/ isn't a package)
        import importlib.util
        from pathlib import Path as _Path

        slice_mod_path = _Path(__file__).resolve().parent / "slice_scored_by_sure_terms.py"
        spec = importlib.util.spec_from_file_location("slice_scored_by_sure_terms", slice_mod_path)
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore

        load_sure_terms = mod.load_sure_terms
        load_overrides = mod.load_overrides
        norm = mod.norm

        sure_path = _Path(args.sure)
        overrides_path = _Path(args.overrides)
        if not overrides_path.is_absolute():
            overrides_path = (_Path(__file__).resolve().parent.parent / overrides_path).resolve()
        overrides = load_overrides(overrides_path)
        sure_terms = load_sure_terms(sure_path, overrides=overrides)

        title_terms = [st.term_norm for st in sure_terms if st.term_type == 'TITLE' and st.term_norm]
        # compute per-term matches against normalized title+evidence_flags
        blob = (pe.get('title','').astype(str) + ' | ' + pe.get('evidence_flags','').astype(str)).map(norm)
        for t in title_terms:
            m = blob.str.contains(re.escape(t), na=False)
            if int(m.sum()) == 0:
                continue
            d = pe[m].head(args.person_evidence_per_term_cap)
            kept_by_term[t] = len(d)

        # union of capped sets + fill remainder by global ranking
        idx = set()
        for t in title_terms:
            m = blob.str.contains(re.escape(t), na=False)
            if int(m.sum()) == 0:
                continue
            idx.update(pe[m].head(args.person_evidence_per_term_cap).index.tolist())
        pe_capped = pe.loc[sorted(idx)] if idx else pe.head(0)
        if len(pe_capped) < args.person_evidence_global_cap:
            rest = pe.drop(index=pe_capped.index, errors='ignore').head(args.person_evidence_global_cap - len(pe_capped))
            pe_capped = pd.concat([pe_capped, rest], ignore_index=False)
        pe = pe_capped
    else:
        pe = pe.head(args.person_evidence_global_cap)

    # C) NoMatch but strong evidence (small)
    sn = slice_df.copy()
    sn = sn[sn.get('tier','').astype(str).str.lower().eq('nomatch')]
    sn = add_rank_cols(sn)
    sn = sn[(sn['has_artist_overlap']) | (sn['has_id_evidence'])]
    sn = sn[sn['score_num'] >= 2]
    sn = sn.head(args.strong_nomatch_cap)

    out_wins = Path(args.out_wins)
    out_pe = Path(args.out_person_evidence)
    out_sn = Path(args.out_strong_nomatch)
    out_terms = Path(args.top_terms_out)
    for p in [out_wins, out_pe, out_sn, out_terms, Path(args.summary_out)]:
        p.parent.mkdir(parents=True, exist_ok=True)

    wins.to_csv(out_wins, index=False)
    pe.to_csv(out_pe, index=False)
    sn.to_csv(out_sn, index=False)

    # top terms file (kept after caps)
    lines = []
    lines.append('Top terms kept after caps (TITLE-term approximation)')
    for t, c in sorted(kept_by_term.items(), key=lambda x: x[1], reverse=True)[:20]:
        lines.append(f"- {t}: {c}")
    out_terms.write_text("\n".join(lines) + "\n", encoding='utf-8')

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
    summary.append(f"strong_nomatch_rows={len(sn)}")
    summary.append(f"strong_nomatch_tiers={tier_dist(sn)}")

    Path(args.summary_out).write_text("\n".join(summary) + "\n", encoding="utf-8")

    print(f"Wrote: {out_wins} rows={len(wins)}")
    print(f"Wrote: {out_pe} rows={len(pe)}")
    print(f"Wrote: {out_sn} rows={len(sn)}")
    print(f"Wrote: {out_terms}")
    print(f"Wrote: {args.summary_out}")


if __name__ == "__main__":
    main()
