#!/usr/bin/env python3
"""Score extracted Fornecedores rows against canonical Reference Truth.

Design goals
- Conservative: false positives are worse than false negatives.
- Auditable: always emit evidence flags and provenance passthrough.

Inputs
- --input: extracted fornecedor rows (CSV)
- --reference: canonical reference truth (CSV) (default: runs/reference/reference_truth.csv)

Output
- --output: scored rows (CSV)
- --summary: JSON summary

Expected (flexible) input columns
- title (or title_raw)
- artist / artists / author (optional)
- plus provenance: source_file, sheet, row (optional)

The scorer is intentionally simple for v0.1; tighten rules iteratively.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REF = ROOT / "runs" / "reference" / "reference_truth.csv"
DEFAULT_CONFIG = ROOT / "config" / "scoring_config.json"


def norm_text(x: str) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_title(x: str) -> str:
    s = norm_text(x)
    s = s.replace("’", "'")
    # remove zero-widths
    s = re.sub(r"[\u200b\u200c\u200d]", "", s)
    return s


def tokenize(s: str) -> set[str]:
    s = norm_text(s)
    # words + numbers only
    parts = re.split(r"[^\w]+", s, flags=re.UNICODE)
    toks = {p for p in parts if len(p) >= 3}
    return toks


def is_isrc(x: str) -> bool:
    s = norm_text(x)
    return bool(re.fullmatch(r"[a-z]{2}[- ]?[a-z0-9]{3}[- ]?\d{2}[- ]?\d{5}", s))


def is_iswc(x: str) -> bool:
    s = norm_text(x)
    return bool(re.fullmatch(r"t-\d{3}\.\d{3}\.\d{3}-\d", s))


@dataclass
class ScoreResult:
    tier: str  # Gold|Silver|Bronze|NoMatch
    matched: bool
    ref_match_count: int
    evidence_flags: list[str]
    ref_title_norm: str | None = None
    ref_isrc: str | None = None
    ref_iswc: str | None = None


def load_config(path: Path) -> dict:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    cfg.setdefault("gold_tokens", [])
    cfg.setdefault("negative_title_triggers", [])
    cfg.setdefault("min_title_len_for_bronze", 8)
    return cfg


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def score_one(row: dict, ref_idx: dict, cfg: dict) -> ScoreResult:
    title_raw = row.get("title") or row.get("title_raw") or ""
    title_norm = norm_title(title_raw)

    artist_blob = " ".join([
        str(row.get(k, ""))
        for k in ["artist", "artists", "author", "authors", "artist_raw", "author_raw"]
        if k in row
    ])
    artist_norm = norm_text(artist_blob)

    evidence_flags: list[str] = []

    # Gold token hit anywhere in row
    row_text = norm_text(" ".join([title_raw, artist_blob] + [str(v) for v in row.values()]))
    gold_hits = [t for t in cfg["gold_tokens"] if t and t in row_text]
    if gold_hits:
        evidence_flags.append("GOLD_TOKEN_HIT")

    # hard negative title triggers block title-only
    neg_hits = [t for t in cfg["negative_title_triggers"] if t and t in title_norm]
    if neg_hits:
        evidence_flags.append("NEGATIVE_TITLE_TRIGGER")

    # ID matches if provided
    isrc_raw = norm_text(row.get("isrc", "") or row.get("isrc_raw", ""))
    iswc_raw = norm_text(row.get("iswc", "") or row.get("iswc_raw", ""))

    if isrc_raw and is_isrc(isrc_raw) and isrc_raw in ref_idx["isrc"]:
        m = ref_idx["isrc"][isrc_raw][0]
        return ScoreResult(
            tier="Gold",
            matched=True,
            ref_match_count=len(ref_idx["isrc"][isrc_raw]),
            evidence_flags=["ISRC_MATCH"],
            ref_title_norm=m.get("title_norm"),
            ref_isrc=m.get("isrc"),
            ref_iswc=m.get("iswc"),
        )

    if iswc_raw and is_iswc(iswc_raw) and iswc_raw in ref_idx["iswc"]:
        m = ref_idx["iswc"][iswc_raw][0]
        return ScoreResult(
            tier="Gold",
            matched=True,
            ref_match_count=len(ref_idx["iswc"][iswc_raw]),
            evidence_flags=["ISWC_MATCH"],
            ref_title_norm=m.get("title_norm"),
            ref_isrc=m.get("isrc"),
            ref_iswc=m.get("iswc"),
        )

    # Title match candidates
    cands = ref_idx["title"].get(title_norm, []) if title_norm else []
    if not cands:
        # allow gold token hit without catalog title match? for v0.1, abstain.
        return ScoreResult(tier="NoMatch", matched=False, ref_match_count=0, evidence_flags=evidence_flags)

    evidence_flags.append("TITLE_EXACT")

    # if negative trigger, require strong evidence beyond title
    if "NEGATIVE_TITLE_TRIGGER" in evidence_flags and "GOLD_TOKEN_HIT" not in evidence_flags:
        return ScoreResult(tier="NoMatch", matched=False, ref_match_count=len(cands), evidence_flags=evidence_flags)

    # If row has artist/author info, require overlap with reference evidence tokens (when available)
    if artist_norm:
        row_toks = tokenize(artist_norm)
        best = None
        best_overlap = 0
        for c in cands:
            ref_tok_blob = c.get("evidence_tokens", "") or ""
            ref_toks = tokenize(ref_tok_blob)
            overlap = len(row_toks & ref_toks)
            if overlap > best_overlap:
                best_overlap = overlap
                best = c

        if best_overlap >= 1:
            evidence_flags.append("ARTIST_TOKEN_OVERLAP")
            tier = "Silver" if "GOLD_TOKEN_HIT" not in evidence_flags else "Gold"
            return ScoreResult(
                tier=tier,
                matched=True,
                ref_match_count=len(cands),
                evidence_flags=evidence_flags,
                ref_title_norm=best.get("title_norm"),
                ref_isrc=best.get("isrc"),
                ref_iswc=best.get("iswc"),
            )
        else:
            # artist present but no supporting evidence => abstain
            evidence_flags.append("ARTIST_PRESENT_NO_SUPPORT")
            return ScoreResult(tier="NoMatch", matched=False, ref_match_count=len(cands), evidence_flags=evidence_flags)

    # No artist info: allow Bronze only if title not too short and not negative-triggered
    if len(title_norm) >= int(cfg.get("min_title_len_for_bronze", 8)):
        tier = "Bronze" if "GOLD_TOKEN_HIT" not in evidence_flags else "Gold"
        m = cands[0]
        return ScoreResult(
            tier=tier,
            matched=True,
            ref_match_count=len(cands),
            evidence_flags=evidence_flags,
            ref_title_norm=m.get("title_norm"),
            ref_isrc=m.get("isrc"),
            ref_iswc=m.get("iswc"),
        )

    return ScoreResult(tier="NoMatch", matched=False, ref_match_count=len(cands), evidence_flags=evidence_flags)


def build_ref_index(ref: pd.DataFrame) -> dict:
    rows = ref.fillna("").astype(str).to_dict(orient="records")
    idx = {"title": {}, "isrc": {}, "iswc": {}}
    for r in rows:
        t = norm_title(r.get("title_norm") or r.get("title_raw") or "")
        if t:
            idx["title"].setdefault(t, []).append(r)
        isrc = norm_text(r.get("isrc", ""))
        if isrc and is_isrc(isrc):
            idx["isrc"].setdefault(isrc, []).append(r)
        iswc = norm_text(r.get("iswc", ""))
        if iswc and is_iswc(iswc):
            idx["iswc"].setdefault(iswc, []).append(r)
    return idx


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--reference", default=str(DEFAULT_REF))
    ap.add_argument("--config", default=str(DEFAULT_CONFIG))
    ap.add_argument("--output", required=True)
    ap.add_argument("--summary", required=True)
    args = ap.parse_args()

    cfg = load_config(Path(args.config))

    inp = pd.read_csv(args.input, dtype=str, keep_default_na=False)

    # normalize to at least title column
    title_col = pick_col(inp, ["title", "title_raw", "work_title", "music_title"])
    if title_col is None:
        raise SystemExit(f"Input has no title column. Columns: {list(inp.columns)}")
    if title_col != "title":
        inp = inp.rename(columns={title_col: "title"})

    ref = pd.read_csv(args.reference, dtype=str, keep_default_na=False)
    ref_idx = build_ref_index(ref)

    out_rows = []
    for _, r in inp.iterrows():
        rr = r.to_dict()
        res = score_one(rr, ref_idx, cfg)
        out = dict(rr)
        out.update({
            "match_tier": res.tier,
            "matched": "1" if res.matched else "0",
            "ref_match_count": str(res.ref_match_count),
            "evidence_flags": ";".join(res.evidence_flags),
            "ref_title_norm": res.ref_title_norm or "",
            "ref_isrc": res.ref_isrc or "",
            "ref_iswc": res.ref_iswc or "",
        })
        out_rows.append(out)

    out_df = pd.DataFrame(out_rows)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.output, index=False)

    tiers = out_df["match_tier"].value_counts().to_dict() if "match_tier" in out_df.columns else {}
    summary = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "input": args.input,
        "reference": args.reference,
        "rows_in": int(len(inp)),
        "rows_out": int(len(out_df)),
        "tiers": tiers,
    }
    Path(args.summary).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote: {args.output} rows={len(out_df)}")
    print(f"Wrote: {args.summary}")


if __name__ == "__main__":
    main()
