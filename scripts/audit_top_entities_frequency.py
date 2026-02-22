#!/usr/bin/env python3
"""Entity frequency audit across included fornecedor candidates (NO ZIP).

Goal
- Prove whether low TOP_ENTITY counts are due to extraction/field coverage vs true absence.

Inputs
- coverage_report.csv (from audit_fornecedores_coverage.py)
- fixed entity list (normalized / variants collapse)

Behavior
- Filter coverage to included=Y and structured files only (.xls/.xlsx/.csv/.tsv; .xlsb only if engine available)
- Probe: sample up to N sheets and M rows per sheet (read column headers + small row sample)
- Match entities token-boundary, accent-insensitive.

Outputs (written into report_package of the run):
- top_entity_field_coverage.csv
- top_entity_file_hits.csv
- top_entity_raw_variants.csv (capped)

This script is audit-only: it does not change scoring.
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

    # Canonical entity norms + priorities (>=4 also gets joined-form matching)
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
    # token-boundary (require all tokens)
    toks = ent.tokens
    if not toks:
        return False

    tset = set(tokenize_norm(text))
    if all(t in tset for t in toks):
        return True

    # joined-form for priority>=4 (catch DuduFalcao)
    if ent.priority >= 4:
        jt = joined_norm(text)
        if ent.joined and ent.joined in jt:
            return True

    return False


# --- probing ---
LIKELY_COL_RX = re.compile(
    r"artista|autor|compositor|interprete|int[ée]rprete|titular|particip|editora|publisher|owner|direito|obra|t[íi]tulo|title|nome_",
    re.IGNORECASE,
)


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


def probe_csv(path: Path, *, max_rows: int) -> tuple[list[str], pd.DataFrame]:
    # Try a few encoding/sep combos
    encs = ["utf-8", "utf-8-sig", "latin-1"]
    seps = [",", ";", "\t"]
    last = None
    for enc in encs:
        for sep in seps:
            try:
                df = pd.read_csv(path, dtype=str, nrows=max_rows, low_memory=False, encoding=enc, sep=sep)
                cols = [str(c) for c in df.columns]
                return cols, df.fillna("")
            except Exception as e:
                last = e
                continue
    raise last or RuntimeError("csv probe failed")


def probe_excel(path: Path, *, max_sheets: int, max_rows: int) -> tuple[list[str], dict[str, pd.DataFrame]]:
    xl = pd.ExcelFile(path)
    sheets = xl.sheet_names
    out = {}
    for sh in sheets[:max_sheets]:
        try:
            df = xl.parse(sh, dtype=str, nrows=max_rows).fillna("")
            out[sh] = df
        except Exception:
            continue
    return sheets, out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--coverage", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--max-sheets", type=int, default=5)
    ap.add_argument("--max-rows", type=int, default=25)
    ap.add_argument("--variants-cap", type=int, default=500)
    args = ap.parse_args()

    coverage = Path(args.coverage).expanduser()
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    entities = default_entities()

    cov = pd.read_csv(coverage, dtype=str, low_memory=False).fillna("")
    cov = cov[cov["included"] == "Y"].copy()

    # structured only, NO ZIP
    def ext(p: str) -> str:
        return str(p).rsplit(".", 1)[-1].lower() if "." in str(p) else ""

    cov["ext"] = cov["file_path"].map(ext)
    cov = cov[cov["ext"].isin(["xls", "xlsx", "csv", "tsv", "xlsb"])].copy()

    # XLSB only if supported
    have_pyxlsb = False
    try:
        import pyxlsb  # noqa: F401

        have_pyxlsb = True
    except Exception:
        have_pyxlsb = False
    if not have_pyxlsb:
        cov = cov[cov["ext"] != "xlsb"].copy()

    # stats accumulators
    ent_hit_count = Counter()
    ent_files = defaultdict(set)
    ent_field_counts = defaultdict(Counter)
    ent_file_hits_rows = []

    variants_rows = []
    variants_seen = defaultdict(set)

    total_files = len(cov)

    for _, r in cov.iterrows():
        fp = Path(r["file_path"])
        if not fp.exists():
            continue

        provider = r.get("provider_guess", "") or guess_provider(str(fp))

        try:
            if fp.suffix.lower() in {".csv", ".tsv"}:
                cols, df = probe_csv(fp, max_rows=args.max_rows)
                sheets = ["csv"]
                sheet_dfs = {"csv": df}
            else:
                sheets, sheet_dfs = probe_excel(fp, max_sheets=args.max_sheets, max_rows=args.max_rows)
        except Exception:
            # ignore probe errors here; coverage audit already quarantines parse errors
            continue

        # choose columns to scan
        # if we can detect likely columns, use them; otherwise scan all columns (still small sample)
        for sh, sdf in sheet_dfs.items():
            if sdf is None or sdf.empty:
                continue

            cols = [str(c) for c in sdf.columns]
            likely_cols = [c for c in cols if LIKELY_COL_RX.search(c)]
            scan_cols = likely_cols if likely_cols else cols

            # cap number of columns scanned to avoid pathological sheets
            scan_cols = scan_cols[:50]

            # build raw sample values per column
            col_samples = {}
            for c in scan_cols:
                try:
                    ser = sdf[c].astype(str)
                    # take first non-empty 10
                    vals = [v for v in ser.tolist() if str(v).strip()][:10]
                    col_samples[c] = vals
                except Exception:
                    continue

            # scan
            for ent in entities:
                hit_fields = []
                sample_values = []
                for c, vals in col_samples.items():
                    # quick join search
                    for v in vals:
                        if entity_match_in_text(ent, v):
                            hit_fields.append(c)
                            # collect variants
                            if len(variants_rows) < args.variants_cap and v not in variants_seen[ent.entity_norm]:
                                variants_seen[ent.entity_norm].add(v)
                                variants_rows.append(
                                    {
                                        "entity_norm": ent.entity_norm,
                                        "source_column": c,
                                        "raw_variant": v,
                                        "file_path": str(fp),
                                    }
                                )
                            if len(sample_values) < 3:
                                sample_values.append(v)
                            break

                if hit_fields:
                    ent_hit_count[ent.entity_norm] += 1
                    ent_files[ent.entity_norm].add(str(fp))
                    for hf in hit_fields:
                        ent_field_counts[ent.entity_norm][hf] += 1

                    ent_file_hits_rows.append(
                        {
                            "file_path": str(fp),
                            "provider_guess": provider,
                            "entity_norm": ent.entity_norm,
                            "hit_fields": ";".join(sorted(set(hit_fields))),
                            "sample_values": " | ".join(sample_values[:3]),
                            "confidence": "HIGH" if len(hit_fields) >= 2 else "MED",
                        }
                    )

    # field coverage output
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

    pd.DataFrame(fc_rows).sort_values(["files_hit_count", "hit_count"], ascending=[False, False]).to_csv(out_fc, index=False)
    pd.DataFrame(ent_file_hits_rows).to_csv(out_hits, index=False)
    pd.DataFrame(variants_rows).head(args.variants_cap).to_csv(out_vars, index=False)

    print(f"Wrote: {out_fc}")
    print(f"Wrote: {out_hits}")
    print(f"Wrote: {out_vars}")
    print(f"files_scanned={total_files}")


if __name__ == "__main__":
    main()
