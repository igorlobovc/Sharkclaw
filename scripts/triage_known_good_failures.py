#!/usr/bin/env python3
"""Triage failing known-good templates.

Inputs:
- known_good_template_set.csv (from build_known_good_template_set.py)
- known_good_mapping_audit.csv (from audit_known_good_mapping.py)

Output:
- known_good_failures_triage.csv

This script is *classification-only* (no resolver/scoring changes):
- classify sheet as expected_playlog=Y/N (playlog vs summary/payment/etc)
- record whether failure is missing_title vs missing_artist_author (or both)

Heuristics are intentionally simple and deterministic.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


SUMMARY_SHEET_RX = re.compile(
    r"\b(resumo|summary|capa|cover|sumario|sumário|param|config|tabela|cadastro|relat[óo]rio|relatorio)\b",
    re.I,
)
PLAYLOG_HINT_RX = re.compile(
    r"\b(planilha|unificado|cue|cuesheet|music|sincroniz|exibi|exibição|tv aberta|streaming|nov|dez|jan|fev|mar|abr|mai|jun|jul|ago|set|out)\b",
    re.I,
)


def classify_expected_playlog(sheet_name: str, col_headers_norm: str) -> tuple[str, str]:
    sh = str(sheet_name or "")
    cols = str(col_headers_norm or "")

    # Strong indicators of non-playlog
    if SUMMARY_SHEET_RX.search(sh):
        return "N", "sheet_name_summaryish"

    # Strong indicators of playlog
    if PLAYLOG_HINT_RX.search(sh):
        return "Y", "sheet_name_playlogish"

    # Column-based fallback
    has_titleish = bool(re.search(r"\b(titulo|t[íi]tulo|obra|musica|m[úu]sica|faixa|track|repertorio)\b", cols, re.I))
    has_peopleish = bool(re.search(r"\b(autor|compositor|interprete|int[ée]rprete|artista)\b", cols, re.I))
    has_moneyish = bool(re.search(r"\b(valor|pagamento|percentual|%|moeda|brl|usd|eur)\b", cols, re.I))

    if has_titleish and has_peopleish:
        return "Y", "cols_title_plus_people"

    if has_moneyish and not has_titleish:
        return "N", "cols_money_without_title"

    # Default conservative
    return "N", "default_non_playlog"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--templates", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    tmpl = pd.read_csv(args.templates, dtype=str, low_memory=False).fillna("")
    audit = pd.read_csv(args.audit, dtype=str, low_memory=False).fillna("")

    # Focus on rows with notes (failures)
    audit_f = audit[audit["notes"].astype(str).str.len() > 0].copy()

    # Join to get template metadata (normalized headers)
    key_cols = ["provider_guess", "file_path", "sheet_name"]
    # audit uses 'sheet'
    audit_f = audit_f.rename(columns={"sheet": "sheet_name"})

    merged = audit_f.merge(
        tmpl[["provider_guess", "file_path", "sheet_name", "col_headers_normalized"]],
        on=key_cols,
        how="left",
    )

    out_rows = []
    for _, r in merged.iterrows():
        prov = r.get("provider_guess", "")
        fp = str(r.get("file_path", ""))
        sh = str(r.get("sheet_name", ""))
        cols_norm = str(r.get("col_headers_normalized", ""))
        notes = str(r.get("notes", ""))

        expected, reason = classify_expected_playlog(sh, cols_norm)

        missing = []
        if "missing_title_detect" in notes:
            missing.append("missing_title")
        if "missing_artist_author_detect" in notes:
            missing.append("missing_artist_author")
        missing_s = "+".join(missing) if missing else "unknown"

        out_rows.append(
            {
                "provider_guess": prov,
                "file": Path(fp).name,
                "file_path": fp,
                "sheet": sh,
                "missing": missing_s,
                "expected_playlog": expected,
                "classification_reason": reason,
            }
        )

    out_df = pd.DataFrame(out_rows).sort_values(
        ["expected_playlog", "provider_guess", "file", "sheet"], ascending=[False, True, True, True]
    )

    out = Path(args.out).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out, index=False)
    print(f"Wrote: {out} rows={len(out_df)}")


if __name__ == "__main__":
    main()
