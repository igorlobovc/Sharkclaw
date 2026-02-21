#!/usr/bin/env python3
"""Create a compact "review slice" from a scored fornecedor dataset.

Filters scored rows where any configured sure term matches across available fields.

Inputs
- --scored: scored CSV (e.g. TempClaw/scored_master_workset_top400.csv)
- --sure: sure-match catalog CSV (e.g. config/sure_match_catalog.csv)
- --out: output CSV
- --summary-out: optional summary text file

Sure catalog format
- Backward compatible:
  - If the file has a single column (no header), treat all values as TITLE terms.
  - Otherwise expects at least a `term` column.
- Optional `term_type` column with values: TITLE|PERSON|ORG.
  - If missing, we map legacy `kind` values as:
    - title -> TITLE
    - person -> PERSON
    - entity -> ORG
    - unknown/other -> TITLE

Normalization
- casefold
- strip accents
- collapse whitespace

Output schema
- unchanged vs prior version

Notes
- This is a review/audit helper. It does not change scoring.
- "score" output is a numeric encoding of tier:
  Gold=3, Silver=2, Bronze=1, NoMatch/other=0
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


def _strip_accents(s: str) -> str:
    # NFKD decomposition + drop combining marks
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.casefold().strip()
    s = _strip_accents(s)
    s = re.sub(r"\s+", " ", s)
    return s


@dataclass(frozen=True)
class SureTerm:
    term: str
    term_norm: str
    term_type: str  # TITLE|PERSON|ORG


def _infer_term_type(kind: str) -> str:
    k = norm(kind)
    if k == "title":
        return "TITLE"
    if k == "person":
        return "PERSON"
    if k == "entity":
        return "ORG"
    return "TITLE"


def load_sure_terms(path: Path) -> list[SureTerm]:
    # Try normal CSV with headers first.
    try:
        df = pd.read_csv(path, dtype=str, low_memory=False).fillna("")
        cols = [c.strip().lower() for c in df.columns]
        df.columns = cols
        if "term" not in df.columns:
            raise ValueError("no term col")

        terms: list[SureTerm] = []
        for _, r in df.iterrows():
            term = str(r.get("term", "")).strip()
            if not term:
                continue

            term_type = str(r.get("term_type", "")).strip().upper()
            if not term_type:
                # backwards compat: map kind
                term_type = _infer_term_type(str(r.get("kind", "")))

            if term_type not in {"TITLE", "PERSON", "ORG"}:
                term_type = "TITLE"

            terms.append(SureTerm(term=term, term_norm=norm(term), term_type=term_type))

    except Exception:
        # Backward compatibility: 1-column file without header.
        raw = path.read_text(encoding="utf-8").splitlines()
        terms = []
        for line in raw:
            t = line.strip().strip("\ufeff")
            if not t:
                continue
            terms.append(SureTerm(term=t, term_norm=norm(t), term_type="TITLE"))

    # de-dupe by (term_norm, term_type), prefer longer for regex stability
    uniq: dict[tuple[str, str], SureTerm] = {}
    for st in sorted(terms, key=lambda x: len(x.term_norm), reverse=True):
        uniq[(st.term_norm, st.term_type)] = st
    return list(uniq.values())


def tier_score(tier: str) -> int:
    t = norm(tier)
    return {"gold": 3, "silver": 2, "bronze": 1}.get(t, 0)


def _columns_present(df: pd.DataFrame, candidates: list[str]) -> list[str]:
    cols = set(df.columns)
    return [c for c in candidates if c in cols]


def _bucket_fields(df: pd.DataFrame) -> dict[str, list[str]]:
    """Decide which columns participate in which bucket.

    Buckets:
    - title: title-like columns
    - person: artist/author/participants-like columns
    - org: publisher/entity-like columns

    Always include evidence_flags if present for person+org, because it can contain
    extracted tokens.
    """

    title_cols = _columns_present(df, ["title", "work_title", "work_title_original"])

    person_cols = []
    person_cols += _columns_present(df, ["artist", "interpreter", "author", "authors", "participants"])
    # allow token-ish columns
    tokenish = [c for c in df.columns if "token" in c.lower() or "participant" in c.lower()]
    person_cols += [c for c in tokenish if c not in person_cols]

    org_cols = []
    org_cols += _columns_present(df, ["publisher", "organization", "org", "label"])

    if "evidence_flags" in df.columns:
        # evidence_flags can contain person/org tokens; include in both
        if "evidence_flags" not in person_cols:
            person_cols.append("evidence_flags")
        if "evidence_flags" not in org_cols:
            org_cols.append("evidence_flags")

    return {"TITLE": title_cols, "PERSON": person_cols, "ORG": org_cols}


def _compile_pat(terms: list[SureTerm], term_type: str) -> re.Pattern | None:
    t = [st.term for st in terms if st.term_type == term_type]
    if not t:
        return None
    return re.compile("|".join(re.escape(x) for x in t), flags=re.IGNORECASE)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scored", required=True)
    ap.add_argument("--sure", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--summary-out", default="")
    args = ap.parse_args()

    scored_path = Path(args.scored)
    sure_path = Path(args.sure)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sure_terms = load_sure_terms(sure_path)
    if not sure_terms:
        raise SystemExit("No sure terms found")

    df = pd.read_csv(scored_path, dtype=str, low_memory=False).fillna("")
    if "title" not in df.columns:
        raise SystemExit("scored CSV must contain a 'title' column")

    buckets = _bucket_fields(df)

    # Build normalized search text per bucket (only for existing columns)
    bucket_text: dict[str, pd.Series] = {}
    for btype, cols in buckets.items():
        if not cols:
            continue
        # concatenate fields per row
        s = df[cols[0]].astype(str)
        for c in cols[1:]:
            s = s + " | " + df[c].astype(str)
        # normalize
        bucket_text[btype] = s.map(norm)

    # compile regex per type (we match on normalized strings, but terms are matched with
    # case-insensitive regex; accent stripping happens on the row side.
    pats = {t: _compile_pat(sure_terms, t) for t in ["TITLE", "PERSON", "ORG"]}

    hits_by_type = {"TITLE": 0, "PERSON": 0, "ORG": 0}

    masks = []
    for ttype in ["TITLE", "PERSON", "ORG"]:
        if pats[ttype] is None or ttype not in bucket_text:
            continue
        m = bucket_text[ttype].str.contains(pats[ttype], na=False)
        hits_by_type[ttype] = int(m.sum())
        masks.append(m)

    if masks:
        mask_any = masks[0]
        for m in masks[1:]:
            mask_any = mask_any | m
    else:
        mask_any = pd.Series([False] * len(df))

    sub = df.loc[mask_any].copy()

    # compute numeric score
    sub["score"] = sub.get("match_tier", "").apply(tier_score)

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

    out = out.sort_values(by=["score", "tier", "title"], ascending=[False, True, True])
    out.to_csv(out_path, index=False)

    tier_counts = out["tier"].value_counts().to_dict() if len(out) else {}

    if args.summary_out:
        sp = Path(args.summary_out)
        sp.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        lines.append(f"rows_total_scored={len(df)}")
        lines.append(f"rows_in_slice={len(out)}")
        lines.append("hits_by_term_type=" + ",".join(f"{k}:{v}" for k, v in hits_by_type.items()))
        lines.append("tier_distribution=" + ",".join(f"{k}:{v}" for k, v in tier_counts.items()))
        lines.append("field_buckets=")
        for k, cols in buckets.items():
            lines.append(f"  {k}: {cols}")
        sp.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote: {out_path} rows={len(out)} tiers={tier_counts}")


if __name__ == "__main__":
    main()
