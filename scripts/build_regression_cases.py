#!/usr/bin/env python3
"""Build regression cases tying fornecedor evidence to expected reference anchor.

Mode (option 2): allow "missing-from-truth" alerts.

Inputs:
- --examples-csv: CSV like 'Example matches with references .csv'
- --truth-csv: reference truth CSV (e.g., reference_truth_enriched_clean.csv)
- --sure-catalog: sure_match_catalog.csv (terms + known positive titles)

Outputs:
- regression_cases.csv with:
  - evidence fields (title/artist/author/publisher/report_file/sheet/source_path/...)
  - expected reference anchor if found (title_norm/isrc/iswc)
  - anchor_method: TITLE_NORM_EXACT | MISSING_IN_TRUTH_TITLE
  - alert_expected_owned: 1 if sure-catalog suggests it should be owned/sure
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


def build_truth_index(truth: pd.DataFrame) -> dict[str, dict]:
    # de-dupe by title_norm
    truth = truth.fillna("")
    if "title_norm" not in truth.columns:
        raise SystemExit("truth must have title_norm")
    truth["_k"] = truth["title_norm"].map(norm)
    # keep first occurrence
    idx = truth.drop_duplicates("_k").set_index("_k")
    out = {}
    for k, r in idx.iterrows():
        out[k] = {
            "title_norm": r.get("title_norm", ""),
            "isrc": r.get("isrc", ""),
            "iswc": r.get("iswc", ""),
            "source": r.get("source", ""),
            "source_detail": r.get("source_detail", ""),
        }
    return out


def load_sure_patterns(catalog: pd.DataFrame) -> re.Pattern:
    # include person/entity terms + known positive titles
    terms = [str(x).strip() for x in catalog["term"].tolist() if str(x).strip()]
    # longest first
    terms = sorted(set(terms), key=len, reverse=True)
    if not terms:
        return re.compile(r"$a")
    return re.compile("|".join(re.escape(t) for t in terms), flags=re.IGNORECASE)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--examples-csv", required=True)
    ap.add_argument("--truth-csv", required=True)
    ap.add_argument("--sure-catalog", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    examples = pd.read_csv(args.examples_csv, dtype=str, low_memory=False).fillna("")
    truth = pd.read_csv(args.truth_csv, dtype=str, low_memory=False).fillna("")
    cat = pd.read_csv(args.sure_catalog, dtype=str, low_memory=False).fillna("")

    truth_idx = build_truth_index(truth)
    sure_pat = load_sure_patterns(cat)

    out_rows = []
    for _, r in examples.iterrows():
        title = r.get("work_title", "")
        tkey = norm(title)
        anchor = truth_idx.get(tkey)

        # expected-owned heuristic: sure tokens or known positive titles appear in row evidence
        evidence_blob = " | ".join(
            [
                r.get("work_title", ""),
                r.get("work_title_original", ""),
                r.get("author", ""),
                r.get("authors", ""),
                r.get("interpreter", ""),
                r.get("publisher", ""),
                r.get("tier_reasons", ""),
            ]
        )
        expected_owned = 1 if bool(sure_pat.search(evidence_blob)) else 0

        out_rows.append(
            {
                # evidence
                "fornecedor": r.get("fornecedor", ""),
                "tier": r.get("tier", ""),
                "family": r.get("family", ""),
                "channel": r.get("channel", ""),
                "program": r.get("program", ""),
                "exhibit_date": r.get("exhibit_date", ""),
                "work_title": title,
                "work_title_original": r.get("work_title_original", ""),
                "author": r.get("author", "") or r.get("authors", ""),
                "interpreter": r.get("interpreter", ""),
                "publisher": r.get("publisher", ""),
                "tier_reasons": r.get("tier_reasons", ""),
                "report_file": r.get("report_file", ""),
                "sheet": r.get("sheet", ""),
                "source_path": r.get("source_path", ""),
                "example_original_path": r.get("example_original_path", ""),
                "sha1": r.get("sha1", ""),
                # expected anchor
                "expected_ref_title_norm": anchor.get("title_norm", "") if anchor else "",
                "expected_ref_isrc": anchor.get("isrc", "") if anchor else "",
                "expected_ref_iswc": anchor.get("iswc", "") if anchor else "",
                "expected_ref_source": anchor.get("source", "") if anchor else "",
                "expected_ref_source_detail": anchor.get("source_detail", "") if anchor else "",
                "anchor_method": "TITLE_NORM_EXACT" if anchor else "MISSING_IN_TRUTH_TITLE",
                "alert_expected_owned": expected_owned,
            }
        )

    out = pd.DataFrame(out_rows)
    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(outp, index=False)

    anchored = int((out["anchor_method"] == "TITLE_NORM_EXACT").sum())
    missing = int((out["anchor_method"] != "TITLE_NORM_EXACT").sum())
    expected_owned = int(out["alert_expected_owned"].sum())
    print(f"Wrote: {outp} rows={len(out)} anchored={anchored} missing={missing} expected_owned_alerts={expected_owned}")


if __name__ == "__main__":
    main()
