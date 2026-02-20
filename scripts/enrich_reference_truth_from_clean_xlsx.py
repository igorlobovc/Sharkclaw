#!/usr/bin/env python3
"""Enrich canonical reference truth using Processed CLEAN XLSX sources.

These CLEAN XLSX files are still report-shaped (multi-row blocks), but provide
broad coverage beyond the first 20 PDF pages.

Inputs (local-only)
- runs/reference/reference_truth.csv
- /Users/igorcunha/Desktop/Estelita_backup/Estelita/Processed/FONOGRAMAS_CLEAN.xlsx
- /Users/igorcunha/Desktop/Estelita_backup/Estelita/Processed/OBRAS_CLEAN.xlsx

Outputs (local-only)
- runs/reference/reference_truth_enriched_clean.csv
- runs/reference/_truth_enriched_clean_summary.json

No raw data is committed.
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

FONO_CLEAN = Path("/Users/igorcunha/Desktop/Estelita_backup/Estelita/Processed/FONOGRAMAS_CLEAN.xlsx")
OBRAS_CLEAN = Path("/Users/igorcunha/Desktop/Estelita_backup/Estelita/Processed/OBRAS_CLEAN.xlsx")


def norm_text(x: str) -> str:
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_title(x: str) -> str:
    s = norm_text(x)
    s = s.replace("’", "'")
    s = re.sub(r"[\u200b\u200c\u200d]", "", s)
    return s


def norm_isrc(x: str) -> str:
    s = norm_text(x)
    return s.replace("-", "").replace(" ", "")


def norm_iswc(x: str) -> str:
    s = norm_text(x)
    return s.replace(" ", "")


def looks_int(x: str) -> bool:
    s = norm_text(x)
    return bool(re.fullmatch(r"\d+", s))


def looks_isrc(x: str) -> bool:
    s = norm_text(x)
    return bool(re.fullmatch(r"[a-z]{2}[- ]?[a-z0-9]{3}[- ]?\d{2}[- ]?\d{5}", s))


def looks_iswc(x: str) -> bool:
    s = norm_text(x)
    return bool(re.fullmatch(r"t-\d{3}\.\d{3}\.\d{3}-\d", s)) or bool(
        re.fullmatch(r"t\d{9}\d", s.replace(".", "").replace("-", ""))
    )


def parse_fonogramas_clean(path: Path) -> tuple[dict[str, set[str]], dict[str, set[str]], int]:
    """Return tokens_by_isrc_norm, tokens_by_title_norm, rows_scanned."""
    df = pd.read_excel(path, sheet_name=0, header=None, dtype=str).fillna("")

    tokens_by_isrc: dict[str, set[str]] = defaultdict(set)
    tokens_by_title: dict[str, set[str]] = defaultdict(set)

    cur_isrc = None
    cur_title = None

    for _, r in df.iterrows():
        # Observed column layout (0-based) in CLEAN sheet:
        # 0: blank, 1: work_ecad_code, 2: ISRC, 3: situacao, 5: title
        c0 = r[0] if 0 in r.index else ""
        c1 = r[1] if 1 in r.index else ""
        c2 = r[2] if 2 in r.index else ""
        c3 = r[3] if 3 in r.index else ""
        c5 = r[5] if 5 in r.index else ""

        # Track header line
        if looks_int(c1) and looks_isrc(c2) and norm_text(c3) in {"liberado", "bloqueado", ""}:
            cur_isrc = norm_isrc(c2)
            cur_title = norm_title(c5)
            if cur_title:
                tokens_by_title[cur_title].add(cur_title)
            continue

        # Participant line pattern observed:
        # 1: participant_ecad (int), 2: formal_name, 6: pseudonimo
        p1 = r[1] if 1 in r.index else ""
        p2 = r[2] if 2 in r.index else ""
        p6 = r[6] if 6 in r.index else ""

        if cur_isrc and looks_int(p1) and norm_text(p2):
            name = norm_text(p2)
            pseudo = norm_text(p6)
            if name:
                tokens_by_isrc[cur_isrc].add(name)
                if cur_title:
                    tokens_by_title[cur_title].add(name)
            if pseudo:
                tokens_by_isrc[cur_isrc].add(pseudo)
                if cur_title:
                    tokens_by_title[cur_title].add(pseudo)

    return tokens_by_isrc, tokens_by_title, int(len(df))


def parse_obras_clean(path: Path) -> tuple[dict[str, set[str]], dict[str, set[str]], int]:
    """Return tokens_by_iswc_norm, tokens_by_title_norm, rows_scanned."""
    df = pd.read_excel(path, sheet_name=0, header=None, dtype=str).fillna("")

    tokens_by_iswc: dict[str, set[str]] = defaultdict(set)
    tokens_by_title: dict[str, set[str]] = defaultdict(set)

    cur_iswc = None
    cur_title = None

    for _, r in df.iterrows():
        # observed: header line has col1 cod_obra (int), col2 iswc (may be '- . . -' placeholder), col4 title
        c1 = r[1] if 1 in r.index else ""
        c2 = r[2] if 2 in r.index else ""
        c4 = r[4] if 4 in r.index else ""

        if looks_int(c1) and (looks_iswc(c2) or norm_text(c2).startswith("t-") or "-" in str(c2)) and norm_text(c4):
            cur_title = norm_title(c4)
            cur_iswc = norm_iswc(c2) if looks_iswc(c2) else None
            if cur_title:
                tokens_by_title[cur_title].add(cur_title)
            continue

        # participant lines: col1 codigo (int), col2 nome titular, col6 pseudonimo
        p1 = r[1] if 1 in r.index else ""
        p2 = r[2] if 2 in r.index else ""
        p6 = r[6] if 6 in r.index else ""

        if cur_title and looks_int(p1) and norm_text(p2):
            name = norm_text(p2)
            pseudo = norm_text(p6)
            tokens_by_title[cur_title].add(name)
            if pseudo:
                tokens_by_title[cur_title].add(pseudo)
            if cur_iswc:
                tokens_by_iswc[cur_iswc].add(name)
                if pseudo:
                    tokens_by_iswc[cur_iswc].add(pseudo)

    return tokens_by_iswc, tokens_by_title, int(len(df))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reference", default=str(RUNS_REF / "reference_truth.csv"))
    ap.add_argument("--fonogramas", default=str(FONO_CLEAN))
    ap.add_argument("--obras", default=str(OBRAS_CLEAN))
    ap.add_argument("--output", default=str(RUNS_REF / "reference_truth_enriched_clean.csv"))
    ap.add_argument("--summary", default=str(RUNS_REF / "_truth_enriched_clean_summary.json"))
    args = ap.parse_args()

    ref = pd.read_csv(args.reference, dtype=str, keep_default_na=False)
    if "evidence_tokens" not in ref.columns:
        ref["evidence_tokens"] = ""

    f_isrc, f_title, f_rows = parse_fonogramas_clean(Path(args.fonogramas))
    o_iswc, o_title, o_rows = parse_obras_clean(Path(args.obras))

    def merge(row) -> str:
        base = norm_text(row.get("evidence_tokens", ""))
        base_set = set([t for t in base.split(";") if t]) if base else set()

        t = norm_title(row.get("title_norm") or row.get("title_raw") or "")
        isrc = norm_isrc(row.get("isrc", "")) if row.get("isrc", "") else ""
        iswc = norm_iswc(row.get("iswc", "")) if row.get("iswc", "") else ""

        add: set[str] = set()
        if isrc and isrc in f_isrc:
            add |= f_isrc[isrc]
        if iswc and iswc in o_iswc:
            add |= o_iswc[iswc]
        if t and t in f_title:
            add |= f_title[t]
        if t and t in o_title:
            add |= o_title[t]

        merged = sorted(base_set | {norm_text(x) for x in add if norm_text(x)})
        return ";".join(merged)

    before_nonempty = int((ref["evidence_tokens"].astype(str).str.len() > 0).sum())
    ref["evidence_tokens"] = ref.apply(merge, axis=1)
    after_nonempty = int((ref["evidence_tokens"].astype(str).str.len() > 0).sum())

    outp = Path(args.output)
    outp.parent.mkdir(parents=True, exist_ok=True)
    ref.to_csv(outp, index=False)

    summary = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "reference_in": args.reference,
        "fonogramas_clean": args.fonogramas,
        "obras_clean": args.obras,
        "rows": int(len(ref)),
        "fonogramas_rows_scanned": f_rows,
        "obras_rows_scanned": o_rows,
        "fonogramas_isrc_keys": int(len(f_isrc)),
        "obras_iswc_keys": int(len(o_iswc)),
        "evidence_nonempty_before": before_nonempty,
        "evidence_nonempty_after": after_nonempty,
    }
    Path(args.summary).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote: {outp} rows={len(ref)}")
    print(f"Wrote: {args.summary}")


if __name__ == "__main__":
    main()
