#!/usr/bin/env python3
"""Build canonical Reference Truth (local-only outputs).

Goal
- Produce a single canonical truth table we can use to score Fornecedores.
- Prefer already-parsed/normalized structured artifacts before PDFs.

Inputs (local-only, not committed)
- runs/reference/obras_truth.csv
- runs/reference/fonogramas_truth.csv
- runs/reference/reference_truth_structured.csv
- Optional seeds:
  - /Users/igorcunha/Desktop/Estelita_backup/Estelita/Processed/Supplier_Matches_Sure.csv

Outputs (local-only, gitignored)
- runs/reference/reference_truth.csv
- runs/reference/_truth_summary.json

This script is intentionally conservative and deterministic.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RUNS_REF = ROOT / "runs" / "reference"

OBRAS_TRUTH = RUNS_REF / "obras_truth.csv"
FONOGRAMAS_TRUTH = RUNS_REF / "fonogramas_truth.csv"
STRUCT_TRUTH = RUNS_REF / "reference_truth_structured.csv"

OUT_TRUTH = RUNS_REF / "reference_truth.csv"
OUT_SUMMARY = RUNS_REF / "_truth_summary.json"

SURE_MATCHES_DEFAULT = Path("/Users/igorcunha/Desktop/Estelita_backup/Estelita/Processed/Supplier_Matches_Sure.csv")


def norm_text(x: str) -> str:
    if x is None:
        return ""
    s = str(x)
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_title(x: str) -> str:
    s = norm_text(x)
    # light cleanup only; keep accents as-is (we can add unidecode later if needed)
    s = re.sub(r"[\u200b\u200c\u200d]", "", s)
    s = s.replace("’", "'")
    return s


def is_iswc(x: str) -> bool:
    s = norm_text(x)
    return bool(re.fullmatch(r"t-\d{3}\.\d{3}\.\d{3}-\d", s))


def is_isrc(x: str) -> bool:
    s = norm_text(x)
    # tolerate common variants
    return bool(re.fullmatch(r"[a-z]{2}[- ]?[a-z0-9]{3}[- ]?\d{2}[- ]?\d{5}", s))


@dataclass
class SourceInfo:
    name: str
    path: str
    rows: int


def load_csv(path: Path, required_cols: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{path} missing columns: {missing}. Has: {list(df.columns)}")
    return df


def safe_read(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def main():
    RUNS_REF.mkdir(parents=True, exist_ok=True)

    sources: list[SourceInfo] = []

    # 1) OBRAS truth
    obras = load_csv(OBRAS_TRUTH, required_cols=["title"])
    obras_out = pd.DataFrame({
        "title_raw": obras.get("title", ""),
        "title_norm": obras.get("title", "").map(norm_title),
        "iswc": obras.get("iswc", ""),
        "isrc": "",
        "source": "OBRAS_TRUTH",
        "source_detail": "runs/reference/obras_truth.csv",
        "evidence_tokens": "",
    })
    sources.append(SourceInfo("OBRAS_TRUTH", str(OBRAS_TRUTH), len(obras_out)))

    # 2) FONOGRAMAS truth
    fono = load_csv(FONOGRAMAS_TRUTH, required_cols=["title"])
    fono_out = pd.DataFrame({
        "title_raw": fono.get("title", ""),
        "title_norm": fono.get("title", "").map(norm_title),
        "iswc": fono.get("iswc", ""),
        "isrc": fono.get("isrc", ""),
        "source": "FONOGRAMAS_TRUTH",
        "source_detail": "runs/reference/fonogramas_truth.csv",
        "evidence_tokens": "",
    })
    sources.append(SourceInfo("FONOGRAMAS_TRUTH", str(FONOGRAMAS_TRUTH), len(fono_out)))

    # 3) Structured truth (small, but has tokens)
    struct = safe_read(STRUCT_TRUTH)
    if struct is not None and len(struct):
        # try to map flexible columns
        title_col = None
        for c in ["title_norm", "title", "work_title_norm", "work_title"]:
            if c in struct.columns:
                title_col = c
                break
        if title_col is None:
            raise ValueError(f"{STRUCT_TRUTH} has no recognizable title column")

        struct_out = pd.DataFrame({
            "title_raw": struct.get(title_col, ""),
            "title_norm": struct.get(title_col, "").map(norm_title),
            "iswc": struct.get("iswc", "") if "iswc" in struct.columns else "",
            "isrc": struct.get("isrc", "") if "isrc" in struct.columns else "",
            "source": "STRUCTURED_TRUTH",
            "source_detail": "runs/reference/reference_truth_structured.csv",
            "evidence_tokens": struct.get("evidence_tokens", "") if "evidence_tokens" in struct.columns else "",
        })
        sources.append(SourceInfo("STRUCTURED_TRUTH", str(STRUCT_TRUTH), len(struct_out)))
    else:
        struct_out = pd.DataFrame(columns=["title_raw", "title_norm", "iswc", "isrc", "source", "source_detail", "evidence_tokens"])

    # 4) Sure matches as extra token seed (not authoritative catalog, but useful)
    sure_path = Path(os.environ.get("SURE_MATCHES", str(SURE_MATCHES_DEFAULT)))
    sure = safe_read(sure_path)
    if sure is not None and len(sure):
        # keep minimal signal; columns vary, so guard
        tcol = "title_ref" if "title_ref" in sure.columns else ("title" if "title" in sure.columns else None)
        acol = "artist" if "artist" in sure.columns else ("artist_supplier" if "artist_supplier" in sure.columns else None)
        if tcol is not None:
            tok = ""
            if acol is not None:
                tok = sure[acol].map(norm_text)
            sure_out = pd.DataFrame({
                "title_raw": sure[tcol],
                "title_norm": sure[tcol].map(norm_title),
                "iswc": sure.get("author", "") if "author" in sure.columns else "",
                "isrc": "",
                "source": "SURE_MATCHES",
                "source_detail": str(sure_path),
                "evidence_tokens": tok,
            })
            sources.append(SourceInfo("SURE_MATCHES", str(sure_path), len(sure_out)))
        else:
            sure_out = pd.DataFrame(columns=["title_raw", "title_norm", "iswc", "isrc", "source", "source_detail", "evidence_tokens"])
    else:
        sure_out = pd.DataFrame(columns=["title_raw", "title_norm", "iswc", "isrc", "source", "source_detail", "evidence_tokens"])

    # Combine
    all_df = pd.concat([obras_out, fono_out, struct_out, sure_out], ignore_index=True)

    # Normalize identifiers
    all_df["iswc"] = all_df["iswc"].fillna("").astype(str)
    all_df["isrc"] = all_df["isrc"].fillna("").astype(str)

    # Keep only plausible iswc/isrc formats (avoid noise)
    all_df.loc[~all_df["iswc"].map(is_iswc), "iswc"] = ""
    all_df.loc[~all_df["isrc"].map(is_isrc), "isrc"] = ""

    # Drop empty titles
    all_df = all_df[all_df["title_norm"].astype(str).str.len() > 0].copy()

    # Deduplicate on (title_norm, iswc, isrc) keeping first occurrence; preserve sources list separately
    all_df["dedupe_key"] = (
        all_df["title_norm"].astype(str)
        + "|" + all_df["iswc"].astype(str)
        + "|" + all_df["isrc"].astype(str)
    )

    # Aggregate sources/tokens per dedupe key
    agg = all_df.groupby("dedupe_key", as_index=False).agg({
        "title_raw": "first",
        "title_norm": "first",
        "iswc": "first",
        "isrc": "first",
        "source": lambda s: ";".join(sorted(set([x for x in s if x]))),
        "source_detail": lambda s: ";".join(sorted(set([x for x in s if x]))),
        "evidence_tokens": lambda s: ";".join(sorted(set([norm_text(x) for x in s if norm_text(x)])))[0:20000],
    })

    # Write outputs
    agg = agg.drop(columns=["dedupe_key"], errors="ignore")
    agg.to_csv(OUT_TRUTH, index=False)

    summary = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "sources": [s.__dict__ for s in sources],
        "rows_out": int(len(agg)),
        "unique_titles": int(agg["title_norm"].nunique()),
        "non_empty_iswc": int((agg["iswc"].astype(str).str.len() > 0).sum()),
        "non_empty_isrc": int((agg["isrc"].astype(str).str.len() > 0).sum()),
    }
    OUT_SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"Wrote: {OUT_TRUTH} rows={len(agg)}")
    print(f"Wrote: {OUT_SUMMARY}")


if __name__ == "__main__":
    main()
