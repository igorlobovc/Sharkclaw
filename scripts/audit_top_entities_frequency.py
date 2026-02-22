#!/usr/bin/env python3
"""Entity frequency audit across included fornecedor candidates (NO ZIP), v2.

Goal
- Prove whether low TOP_ENTITY counts (e.g., Dudu Falcão) are due to extraction/field coverage vs true absence.

Inputs
- coverage_report.csv (from audit_fornecedores_coverage.py)
- fixed entity list (normalized / variants collapse)

Strategy (deep-but-cheap)
- Filter coverage to included=Y and structured only (.xls/.xlsx/.csv/.tsv; .xlsb only if supported).
- For XLS/XLSX/XLSB:
  - list sheet names
  - for each sheet, read headers to detect candidate columns
  - if candidate columns exist: scan ONLY those columns for up to N rows (default 20,000)
    (stop early per entity if desired via --hits-cap-per-entity)
  - if candidate columns do NOT exist: fallback to scanning title/obra columns only for long titles (len>=12)
    (does not attempt to find entities; records stop_reason)
- For CSV/TSV:
  - encoding/sep fallbacks
  - scan only candidate columns if present; else skip

Matching
- Token-boundary (all entity tokens present), accent-insensitive.
- For priority>=4 entities, also allow joined-form match (e.g., DuduFalcao).

Outputs (written into out-dir)
- top_entity_field_coverage.csv
- top_entity_file_hits.csv
- top_entity_raw_variants.csv (capped)
- top_entity_sampling_coverage.txt
- top_entity_zero_reasons.csv

Audit-only: does not change scoring/sweep.
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


# --- Normalization helpers (aligned with entity_overrides.py) ---
_NON_ALNUM = re.compile(r"[^0-9a-z]+")
_WS = re.compile(r"\s+")


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def norm_text(s: str) -> str:
    s = "" if s is None else str(s)
    s = strip_accents(s.casefold())
    s = _NON_ALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


def tokenize_norm(s: str) -> list[str]:
    s = norm_text(s)
    return [t for t in s.split(" ") if t]


def joined_norm(s: str) -> str:
    return "".join(tokenize_norm(s))


@dataclass(frozen=True)
class Entity:
    entity_norm: str
    priority: int

    @property
    def tokens(self) -> tuple[str, ...]:
        return tuple(tokenize_norm(self.entity_norm))

    @property
    def joined(self) -> str:
        return "".join(self.tokens)


def default_entities() -> list[Entity]:
    """Source-of-truth entity list for this audit.

    Variants are collapsed into the canonical entity_norm.
    """

    items = [
        ("editora estelita", 5),
        ("eduardo melo pereira", 5),
        ("dudu falcao", 5),
        ("eduardo falcao", 4),
        ("tagore", 5),
        ("yago o proprio", 5),
        ("yago oproprio", 5),
        ("zero quatro", 5),
        ("jose paes de lira filho", 4),
        ("henrique mendonca", 4),
        ("marconi de souza santos", 3),
        ("shevchenko e elloco", 3),
        ("marina peralta", 4),
        ("febre90s", 4),
        ("mc pumapjl", 4),
        ("sonotws", 3),
        ("mc draak", 4),
        ("bide ou balde", 4),
        ("kaya conky", 3),
        ("olegario", 3),
        ("nexoanexo", 3),
    ]

    return [Entity(entity_norm=norm_text(n), priority=p) for n, p in items]


def entity_match_in_text(ent: Entity, text: str) -> bool:
    toks = ent.tokens
    if not toks:
        return False

    tset = set(tokenize_norm(text))
    if all(t in tset for t in toks):
        return True

    if ent.priority >= 4:
        jt = joined_norm(text)
        if ent.joined and ent.joined in jt:
            return True

    return False


LIKELY_COL_RX = re.compile(
    r"artista|autor|compositor|interprete|int[ée]rprete|titular|particip|editora|publisher|owner|direito|obra|t[íi]tulo|title|nome_",
    re.IGNORECASE,
)

TITLE_COL_RX = re.compile(r"obra|t[íi]tulo|title", re.IGNORECASE)


def detect_candidate_columns(columns: list[str]) -> list[str]:
    return [c for c in columns if LIKELY_COL_RX.search(str(c))]


def detect_title_columns(columns: list[str]) -> list[str]:
    return [c for c in columns if TITLE_COL_RX.search(str(c))]


def guess_provider(path: str) -> str:
    s = str(path)
    if re.search(r"\bband\b|bandeirantes", s, re.I):
        return "band"
    if re.search(r"\bsbt\b", s, re.I):
        return "sbt"
    if re.search(r"\bglobo\b|canais globo", s, re.I):
        return "globo"
    if re.search(r"globoplay", s, re.I):
        return "globoplay"
    if re.search(r"ubem", s, re.I):
        return "ubem"
    return "other"


def probe_csv(path: Path, *, max_rows: int) -> tuple[list[str], pd.DataFrame, str, str]:
    encs = ["utf-8", "utf-8-sig", "latin-1"]
    seps = [",", ";", "\t"]
    last = None
    for enc in encs:
        for sep in seps:
            try:
                df = pd.read_csv(path, dtype=str, nrows=max_rows, low_memory=False, encoding=enc, sep=sep)
                cols = [str(c) for c in df.columns]
                return cols, df.fillna(""), enc, repr(sep)
            except Exception as e:
                last = e
                continue
    raise last or RuntimeError("csv probe failed")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--coverage", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--max-sheets", type=int, default=5)
    ap.add_argument("--max-rows", type=int, default=20000)
    ap.add_argument("--variants-cap", type=int, default=500)
    ap.add_argument("--hits-cap-per-entity", type=int, default=200)
    args = ap.parse_args()

    coverage = Path(args.coverage).expanduser()
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    entities = default_entities()

    cov = pd.read_csv(coverage, dtype=str, low_memory=False).fillna("")
    cov = cov[cov["included"] == "Y"].copy()

    def ext(p: str) -> str:
        return str(p).rsplit(".", 1)[-1].lower() if "." in str(p) else ""

    cov["ext"] = cov["file_path"].map(ext)
    cov = cov[cov["ext"].isin(["xls", "xlsx", "csv", "tsv", "xlsb"])].copy()

    have_pyxlsb = False
    try:
        import pyxlsb  # noqa: F401

        have_pyxlsb = True
    except Exception:
        have_pyxlsb = False
    if not have_pyxlsb:
        cov = cov[cov["ext"] != "xlsb"].copy()

    # accumulators
    ent_hit_count = Counter()
    ent_files = defaultdict(set)
    ent_field_counts = defaultdict(Counter)
    file_hits_rows = []

    variants_rows = []
    variants_seen = defaultdict(set)

    sampling_rows = []

    total_files = len(cov)

    for _, r in cov.iterrows():
        fp = Path(r["file_path"])
        if not fp.exists():
            continue

        provider = r.get("provider_guess", "") or guess_provider(str(fp))

        sheets_scanned = 0
        rows_scanned = 0
        columns_scanned = 0
        stop_reason = "ok"

        # CSV/TSV
        if fp.suffix.lower() in {".csv", ".tsv"}:
            try:
                cols, df, enc, sep = probe_csv(fp, max_rows=args.max_rows)
            except Exception:
                sampling_rows.append(
                    {
                        "file_path": str(fp),
                        "sheets_scanned": 1,
                        "rows_scanned": 0,
                        "columns_scanned": 0,
                        "stop_reason": "parse_error",
                    }
                )
                continue

            cand = detect_candidate_columns(cols)
            if not cand:
                sampling_rows.append(
                    {
                        "file_path": str(fp),
                        "sheets_scanned": 1,
                        "rows_scanned": min(len(df), args.max_rows),
                        "columns_scanned": 0,
                        "stop_reason": "no_candidate_columns",
                    }
                )
                continue

            scan_cols = cand[:50]
            try:
                sdf = df[scan_cols].astype(str)
            except Exception:
                continue

            sheets_scanned = 1
            rows_scanned = min(len(sdf), args.max_rows)
            columns_scanned = len(scan_cols)

            for ent in entities:
                if ent_hit_count[ent.entity_norm] >= args.hits_cap_per_entity:
                    continue
                hit_fields = []
                sample_values = []
                for c in scan_cols:
                    ser = sdf[c].tolist()[: args.max_rows]
                    for v in ser:
                        if not str(v).strip():
                            continue
                        if entity_match_in_text(ent, v):
                            hit_fields.append(c)
                            if len(variants_rows) < args.variants_cap and v not in variants_seen[ent.entity_norm]:
                                variants_seen[ent.entity_norm].add(v)
                                variants_rows.append(
                                    {"entity_norm": ent.entity_norm, "source_column": c, "raw_variant": v, "file_path": str(fp)}
                                )
                            if len(sample_values) < 3:
                                sample_values.append(v)
                            break

                if hit_fields:
                    ent_hit_count[ent.entity_norm] += 1
                    ent_files[ent.entity_norm].add(str(fp))
                    for hf in hit_fields:
                        ent_field_counts[ent.entity_norm][hf] += 1
                    file_hits_rows.append(
                        {
                            "file_path": str(fp),
                            "provider_guess": provider,
                            "entity_norm": ent.entity_norm,
                            "hit_fields": ";".join(sorted(set(hit_fields))),
                            "sample_values": " | ".join(sample_values[:3]),
                            "confidence": "HIGH" if len(hit_fields) >= 2 else "MED",
                        }
                    )

            sampling_rows.append(
                {
                    "file_path": str(fp),
                    "sheets_scanned": sheets_scanned,
                    "rows_scanned": rows_scanned,
                    "columns_scanned": columns_scanned,
                    "stop_reason": stop_reason,
                }
            )
            continue

        # Excel
        try:
            xl = pd.ExcelFile(fp)
            sheet_names = xl.sheet_names
        except Exception:
            sampling_rows.append(
                {
                    "file_path": str(fp),
                    "sheets_scanned": 0,
                    "rows_scanned": 0,
                    "columns_scanned": 0,
                    "stop_reason": "parse_error",
                }
            )
            continue

        for sh in sheet_names[: args.max_sheets]:
            sheets_scanned += 1
            try:
                hdr = xl.parse(sh, dtype=str, nrows=0)
                cols = [str(c) for c in hdr.columns]
            except Exception:
                continue

            cand = detect_candidate_columns(cols)
            if cand:
                scan_cols = cand[:50]
                try:
                    sdf = xl.parse(sh, dtype=str, nrows=args.max_rows, usecols=scan_cols).fillna("")
                except Exception:
                    continue

                columns_scanned += len(scan_cols)
                rows_scanned += len(sdf)

                for ent in entities:
                    if ent_hit_count[ent.entity_norm] >= args.hits_cap_per_entity:
                        continue
                    hit_fields = []
                    sample_values = []
                    for c in scan_cols:
                        ser = sdf[c].astype(str).tolist()
                        for v in ser:
                            if not str(v).strip():
                                continue
                            if entity_match_in_text(ent, v):
                                hit_fields.append(c)
                                if len(variants_rows) < args.variants_cap and v not in variants_seen[ent.entity_norm]:
                                    variants_seen[ent.entity_norm].add(v)
                                    variants_rows.append(
                                        {"entity_norm": ent.entity_norm, "source_column": c, "raw_variant": v, "file_path": str(fp)}
                                    )
                                if len(sample_values) < 3:
                                    sample_values.append(v)
                                break

                    if hit_fields:
                        ent_hit_count[ent.entity_norm] += 1
                        ent_files[ent.entity_norm].add(str(fp))
                        for hf in hit_fields:
                            ent_field_counts[ent.entity_norm][hf] += 1
                        file_hits_rows.append(
                            {
                                "file_path": str(fp),
                                "provider_guess": provider,
                                "entity_norm": ent.entity_norm,
                                "hit_fields": ";".join(sorted(set(hit_fields))),
                                "sample_values": " | ".join(sample_values[:3]),
                                "confidence": "HIGH" if len(hit_fields) >= 2 else "MED",
                            }
                        )
            else:
                title_cols = detect_title_columns(cols)
                if title_cols:
                    try:
                        sdf = xl.parse(sh, dtype=str, nrows=args.max_rows, usecols=title_cols[:5]).fillna("")
                        # long-title scan (debug only)
                        _ = int((sdf.astype(str).applymap(len) >= 12).any(axis=1).sum())
                        rows_scanned += len(sdf)
                        columns_scanned += len(title_cols[:5])
                    except Exception:
                        continue

        sampling_rows.append(
            {
                "file_path": str(fp),
                "sheets_scanned": sheets_scanned,
                "rows_scanned": rows_scanned,
                "columns_scanned": columns_scanned,
                "stop_reason": "ok" if columns_scanned else "no_candidate_columns",
            }
        )

    # Outputs
    fc_rows = []
    for ent in entities:
        e = ent.entity_norm
        files = sorted(ent_files.get(e, set()))
        top_files = ";".join([Path(x).name for x in files[:10]])
        field_breakdown = ent_field_counts.get(e, Counter())
        fb = ",".join(f"{k}:{v}" for k, v in field_breakdown.most_common(20))
        fc_rows.append(
            {
                "entity_norm": e,
                "hit_count": int(ent_hit_count.get(e, 0)),
                "files_hit_count": len(files),
                "hit_field_breakdown": fb,
                "top_10_files": top_files,
            }
        )

    out_fc = out_dir / "top_entity_field_coverage.csv"
    out_hits = out_dir / "top_entity_file_hits.csv"
    out_vars = out_dir / "top_entity_raw_variants.csv"
    out_sampling = out_dir / "top_entity_sampling_coverage.txt"
    out_zero = out_dir / "top_entity_zero_reasons.csv"

    pd.DataFrame(fc_rows).sort_values(["files_hit_count", "hit_count"], ascending=[False, False]).to_csv(out_fc, index=False)
    pd.DataFrame(file_hits_rows).to_csv(out_hits, index=False)
    pd.DataFrame(variants_rows).head(args.variants_cap).to_csv(out_vars, index=False)

    # sampling coverage text
    lines = []
    for sr in sampling_rows:
        lines.append(
            f"{sr['file_path']} | sheets_scanned={sr['sheets_scanned']} rows_scanned={sr['rows_scanned']} columns_scanned={sr['columns_scanned']} stop_reason={sr['stop_reason']}"
        )
    out_sampling.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # zero reasons
    zrows = []
    for ent in entities:
        if ent_files.get(ent.entity_norm):
            continue
        zrows.append(
            {
                "entity_norm": ent.entity_norm,
                "reason": "not_found_in_scanned_candidate_columns_or_skipped",
                "files_scanned": total_files,
                "max_rows_per_sheet": args.max_rows,
                "max_sheets": args.max_sheets,
            }
        )
    pd.DataFrame(zrows).to_csv(out_zero, index=False)

    print(f"Wrote: {out_fc}")
    print(f"Wrote: {out_hits}")
    print(f"Wrote: {out_vars}")
    print(f"Wrote: {out_sampling}")
    print(f"Wrote: {out_zero}")
    print(f"files_scanned={total_files}")


if __name__ == "__main__":
    main()
