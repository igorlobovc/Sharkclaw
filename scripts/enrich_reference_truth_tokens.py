#!/usr/bin/env python3
"""Enrich canonical reference truth with participant/author tokens.

Why
- Current reference_truth.csv has very sparse evidence_tokens.
- Fornecedores rows often include artist/author; we need overlap evidence.

Strategy (v0.1)
- Join fonogramas participants (from PDF block parser outputs) by ISRC.
- Aggregate participant names/pseudonyms into `evidence_tokens`.

Inputs (local-only)
- runs/reference/reference_truth.csv
- runs/reference/pdf_blocks_p1-20/fonogramas_participants.csv (or pdf_blocks/...)

Outputs (local-only)
- runs/reference/reference_truth_enriched.csv
- runs/reference/_truth_enriched_summary.json

No data is committed; only this script is.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RUNS_REF = ROOT / "runs" / "reference"


def norm_text(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_isrc(x: str) -> str:
    s = norm_text(x)
    s = s.replace("-", "").replace(" ", "")
    # store in canonical with hyphens? keep original in reference; for join, compare both normalized
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reference", default=str(RUNS_REF / "reference_truth.csv"))
    ap.add_argument(
        "--participants",
        default=str(RUNS_REF / "pdf_blocks_p1-20" / "fonogramas_participants.csv"),
        help="CSV with columns including isrc, formal_name, pseudonimo",
    )
    ap.add_argument("--output", default=str(RUNS_REF / "reference_truth_enriched.csv"))
    ap.add_argument("--summary", default=str(RUNS_REF / "_truth_enriched_summary.json"))
    args = ap.parse_args()

    ref = pd.read_csv(args.reference, dtype=str, keep_default_na=False)
    part = pd.read_csv(args.participants, dtype=str, keep_default_na=False)

    if "isrc" not in ref.columns:
        raise SystemExit("Reference missing isrc column")
    if "isrc" not in part.columns:
        raise SystemExit("Participants missing isrc column")

    # build aggregated tokens per isrc
    tokens_by_isrc: dict[str, set[str]] = defaultdict(set)
    for _, r in part.iterrows():
        isrc_raw = r.get("isrc", "")
        key = norm_isrc(isrc_raw)
        for c in ["formal_name", "pseudonimo", "participant_ecad"]:
            v = norm_text(r.get(c, ""))
            if v:
                tokens_by_isrc[key].add(v)

    def merge_tokens(row) -> str:
        isrc_key = norm_isrc(row.get("isrc", ""))
        base = norm_text(row.get("evidence_tokens", ""))
        base_set = set([t for t in base.split(";") if t]) if base else set()
        add = tokens_by_isrc.get(isrc_key, set())
        merged = sorted(base_set | add)
        return ";".join(merged)

    before_nonempty = int((ref.get("evidence_tokens", "").astype(str).str.len() > 0).sum()) if "evidence_tokens" in ref.columns else 0

    if "evidence_tokens" not in ref.columns:
        ref["evidence_tokens"] = ""

    ref["evidence_tokens"] = ref.apply(merge_tokens, axis=1)

    after_nonempty = int((ref["evidence_tokens"].astype(str).str.len() > 0).sum())

    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    ref.to_csv(outp, index=False)

    summary = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "reference_in": args.reference,
        "participants_in": args.participants,
        "rows": int(len(ref)),
        "participant_isrc_keys": int(len(tokens_by_isrc)),
        "evidence_nonempty_before": before_nonempty,
        "evidence_nonempty_after": after_nonempty,
    }
    Path(args.summary).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote: {outp} rows={len(ref)}")
    print(f"Wrote: {args.summary}")


if __name__ == "__main__":
    main()
