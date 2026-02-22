#!/usr/bin/env python3
"""ENTITY-ONLY lane audit (NO ZIP): find where TOP Estelita entities appear across lanes.

Goal
- Identify which fornecedor files/lanes contain Tagore/Yago/Zero Quatro/Dudu etc.
- Do this cheaply (no full extraction): scan detected people/entity columns only.

Inputs
- --entities: config/top_estelita_entities.csv
- --root (repeatable): roots to scan
- --out-dir: output folder (we will create report_package/ inside)

Outputs (in out-dir/report_package)
- entity_lane_hits.csv
  columns: entity_norm,lane,file_path,sheet,column,hit_count
- entity_lane_summary.txt
  per entity: lanes ranked by hits + top 20 files
- entity_lane_raw_variants.csv
  columns: entity_norm,lane,file_path,sheet,column,raw_variant

Constraints
- Only scans structured extensions: xls/xlsx/csv/tsv/xlsb (xlsb only if pyxlsb available)
- For each detected people column: scan up to 5,000 rows (or stop after 200 hits per entity per file)

Matching rules
- accent/case normalized
- token-boundary (all tokens present)
- special alias rule: "dudu" counts only if same cell also contains "falcao" (normalized)

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


# --- normalization ---
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
    entity_raw: str
    entity_norm: str
    entity_type: str
    priority: int

    @property
    def tokens(self) -> tuple[str, ...]:
        return tuple(tokenize_norm(self.entity_norm))

    @property
    def joined(self) -> str:
        return "".join(self.tokens)


CANDIDATE_COL_RX = re.compile(
    r"autor|autores|compositor|compositores|artista|interprete|int[ée]rprete|particip"
    r"|titular|titulares|direitos|editora|editoras|publisher|owner|propriet|sociedade|percent|%|split|ipi|cae"
    r"|detalhe|detalhamento|observa|\bobs\b|repert[óo]rio|obra|m[úu]sica|fonograma|isrc|iswc|cue|cuesheet|mcs|programa",
    re.IGNORECASE,
)

ROW_BLOB_SHEET_RX = re.compile(r"ubem|relat|mcs|cue|canais\s*globo", re.IGNORECASE)

TARGET_PROVIDERS = {"ubem", "globo", "globoplay", "deezer", "una"}

# Provider guess (for ROW_BLOB fallback gating)
_PROVIDER_RX = [
    ("band", re.compile(r"\bband\b|bandeirantes", re.I)),
    ("sbt", re.compile(r"\bsbt\b", re.I)),
    ("globo", re.compile(r"\bglobo\b|canais globo|globonews", re.I)),
    ("globoplay", re.compile(r"globoplay", re.I)),
    ("ubem", re.compile(r"ubem", re.I)),
    ("deezer", re.compile(r"deezer", re.I)),
    ("una", re.compile(r"\buna\b", re.I)),
]


def guess_provider(path: str) -> str:
    s = str(path)
    for name, rx in _PROVIDER_RX:
        if rx.search(s):
            return name
    return "other"


def looks_like_contributor_list(values: list[str]) -> bool:
    # heuristics: commas / hyphens / percents / multiple tokens
    for v in values[:50]:
        s = str(v)
        if not s.strip():
            continue
        if ("," in s) or ("-" in s) or ("%" in s):
            return True
        toks = tokenize_norm(s)
        if len(toks) >= 3:
            return True
    return False


def lane_for_path(p: str) -> str:
    s = str(p)
    if "/dedup/unique/" in s:
        return "canonical_dedup_unique"
    if "/Desktop/Estelita_backup/Estelita/Raw/Fornecedores/" in s:
        return "canonical_raw_fornecedores"
    if "/Desktop/Estelita_Exports/Takeout/" in s:
        return "takeout"
    if "/estelita_unified_audiovisual/staged/manual_raw_fornecedores/" in s:
        return "staged_manual_raw_fornecedores"
    if "/estelita_unified_audiovisual/staged/" in s:
        return "staged_other"
    return "other"


def load_entities(path: Path) -> list[Entity]:
    df = pd.read_csv(path, dtype=str, low_memory=False).fillna("")
    df.columns = [c.strip().lower() for c in df.columns]
    out = []
    for _, r in df.iterrows():
        raw = str(r.get("entity_raw", "")).strip()
        en = str(r.get("entity_norm", "")).strip() or raw
        if not raw:
            continue
        out.append(
            Entity(
                entity_raw=raw,
                entity_norm=norm_text(en),
                entity_type=str(r.get("entity_type", "PERSON")).strip().upper() or "PERSON",
                priority=int(str(r.get("priority", "0")).strip() or 0),
            )
        )

    # dedupe by entity_norm
    uniq = {}
    for e in out:
        uniq[e.entity_norm] = e
    return list(uniq.values())


def match_entity(ent: Entity, text: str) -> bool:
    t = norm_text(text)
    if not t:
        return False

    # special rule: dudu only if falcao present
    if ent.entity_norm == "dudu falcao":
        if "dudu" in t and "falcao" in t:
            return True
        # also joined form
        if "dudufalcao" in joined_norm(t):
            return True
        return False

    toks = ent.tokens
    if not toks:
        return False

    tset = set(tokenize_norm(t))
    if all(tok in tset for tok in toks):
        return True

    # joined form only for priority>=4
    if ent.priority >= 4:
        jt = joined_norm(t)
        if ent.joined and ent.joined in jt:
            return True

    return False


def probe_csv(path: Path, *, max_rows: int) -> tuple[list[str], pd.DataFrame]:
    encs = ["utf-8", "utf-8-sig", "latin-1"]
    seps = [",", ";", "\t"]
    last = None
    for enc in encs:
        for sep in seps:
            try:
                df = pd.read_csv(path, dtype=str, nrows=max_rows, low_memory=False, encoding=enc, sep=sep).fillna("")
                return [str(c) for c in df.columns], df
            except Exception as e:
                last = e
                continue
    raise last or RuntimeError("csv probe failed")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--entities", default="config/top_estelita_entities.csv")
    ap.add_argument("--root", action="append", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--max-sheets", type=int, default=6)
    ap.add_argument("--max-rows", type=int, default=5000)
    ap.add_argument("--hits-cap-per-entity-per-file", type=int, default=200)
    ap.add_argument("--variants-cap", type=int, default=500)
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    ent_path = Path(args.entities)
    if not ent_path.is_absolute():
        ent_path = (repo_root / ent_path).resolve()

    entities = load_entities(ent_path)

    out_dir = Path(args.out_dir).expanduser()
    pkg = out_dir / "report_package"
    pkg.mkdir(parents=True, exist_ok=True)

    # xlsb optional
    try:
        import pyxlsb  # noqa: F401

        have_xlsb = True
    except Exception:
        have_xlsb = False

    exts = {".xls", ".xlsx", ".csv", ".tsv", ".xlsb"}

    hits_rows = []
    variants_rows = []
    variants_seen = set()

    # per entity aggregation
    ent_lane_hits = defaultdict(lambda: Counter())  # entity -> lane -> hits
    ent_file_hits = defaultdict(Counter)  # entity -> file -> hits

    for root in [Path(r).expanduser() for r in args.root]:
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() not in exts:
                continue
            if p.suffix.lower() == ".xlsb" and not have_xlsb:
                continue

            lane = lane_for_path(str(p))

            # Probe
            try:
                if p.suffix.lower() in {".csv", ".tsv"}:
                    cols, df = probe_csv(p, max_rows=args.max_rows)
                    sheets = [("csv", df)]
                else:
                    xl = pd.ExcelFile(p)
                    sheets = []
                    for sh in xl.sheet_names[: args.max_sheets]:
                        try:
                            df = xl.parse(sh, dtype=str, nrows=args.max_rows).fillna("")
                            sheets.append((sh, df))
                        except Exception:
                            continue
            except Exception:
                continue

            for sh, df in sheets:
                cols = [str(c) for c in df.columns]

                # Candidate columns by header regex
                cand_cols = [c for c in cols if CANDIDATE_COL_RX.search(c)]

                # Include Unnamed columns if they look like contributor lists
                unnamed = [c for c in cols if str(c).lower().startswith('unnamed')]
                for c in unnamed:
                    try:
                        vals = df[c].astype(str).tolist()[:200]
                        if looks_like_contributor_list(vals):
                            cand_cols.append(c)
                    except Exception:
                        continue

                # de-dupe + cap
                cand_cols = list(dict.fromkeys(cand_cols))[:50]

                if cand_cols:
                    # scan each entity within candidate cols
                    for ent in entities:
                        cap = args.hits_cap_per_entity_per_file
                        hit_ct = 0
                        hit_cols = Counter()

                        for c in cand_cols:
                            ser = df[c].astype(str).tolist()
                            for v in ser:
                                if hit_ct >= cap:
                                    break
                                if not str(v).strip():
                                    continue
                                if match_entity(ent, v):
                                    hit_ct += 1
                                    hit_cols[c] += 1
                                    # variants
                                    key = (ent.entity_norm, lane, str(p), sh, c, v)
                                    if len(variants_rows) < args.variants_cap and key not in variants_seen:
                                        variants_seen.add(key)
                                        variants_rows.append(
                                            {
                                                "entity_norm": ent.entity_norm,
                                                "lane": lane,
                                                "file_path": str(p),
                                                "sheet": sh,
                                                "hit_field": c,
                                                "raw_variant": v,
                                            }
                                        )
                            if hit_ct >= cap:
                                break

                        if hit_ct:
                            hits_rows.append(
                                {
                                    "entity_norm": ent.entity_norm,
                                    "lane": lane,
                                    "file_path": str(p),
                                    "sheet": sh,
                                    "hit_field": ";".join([f"{k}:{v}" for k, v in hit_cols.most_common(10)]),
                                    "hit_count": hit_ct,
                                }
                            )
                            ent_lane_hits[ent.entity_norm][lane] += hit_ct
                            ent_file_hits[ent.entity_norm][str(p)] += hit_ct

                # ROW_BLOB fallback for targeted subset
                provider_guess = guess_provider(str(p))
                if (provider_guess in TARGET_PROVIDERS) or ROW_BLOB_SHEET_RX.search(sh):
                    # cap 3 sheets total for row_blob
                    # Build blob per row for up to max_rows
                    try:
                        # use at most first 50 columns to keep it cheap
                        df2 = df.iloc[: args.max_rows, :50].astype(str)
                        blobs = (df2.apply(lambda row: " | ".join([x for x in row.tolist() if x and x != 'nan']), axis=1)).tolist()
                    except Exception:
                        continue

                    for ent in entities:
                        cap = args.hits_cap_per_entity_per_file
                        hit_ct = 0
                        for blob in blobs:
                            if hit_ct >= cap:
                                break
                            if match_entity(ent, blob):
                                hit_ct += 1
                                key = (ent.entity_norm, lane, str(p), sh, 'ROW_BLOB', blob)
                                if len(variants_rows) < args.variants_cap and key not in variants_seen:
                                    variants_seen.add(key)
                                    variants_rows.append(
                                        {
                                            "entity_norm": ent.entity_norm,
                                            "lane": lane,
                                            "file_path": str(p),
                                            "sheet": sh,
                                            "hit_field": "ROW_BLOB",
                                            "raw_variant": blob[:300],
                                        }
                                    )
                        if hit_ct:
                            hits_rows.append(
                                {
                                    "entity_norm": ent.entity_norm,
                                    "lane": lane,
                                    "file_path": str(p),
                                    "sheet": sh,
                                    "hit_field": "ROW_BLOB",
                                    "hit_count": hit_ct,
                                }
                            )
                            ent_lane_hits[ent.entity_norm][lane] += hit_ct
                            ent_file_hits[ent.entity_norm][str(p)] += hit_ct

            # end sheets

    # write hits
    hits_df = pd.DataFrame(hits_rows)
    hits_csv = pkg / "entity_lane_hits.csv"
    hits_df.to_csv(hits_csv, index=False)

    # raw variants
    vars_df = pd.DataFrame(variants_rows).head(args.variants_cap)
    vars_csv = pkg / "entity_lane_raw_variants.csv"
    vars_df.to_csv(vars_csv, index=False)

    # per entity top files
    top_rows = []
    for ent, file_counts in ent_file_hits.items():
        for fp, ct in file_counts.most_common(50):
            top_rows.append({"entity_norm": ent, "file_path": fp, "hit_count": ct})
    (pd.DataFrame(top_rows)).to_csv(pkg / "entity_lane_top_files.csv", index=False)

    # zero reasons
    z = []
    for ent in entities:
        if ent_file_hits.get(ent.entity_norm):
            continue
        z.append({"entity_norm": ent.entity_norm, "reason": "scanned_no_hits"})
    pd.DataFrame(z).to_csv(pkg / "entity_lane_zero_reasons.csv", index=False)

    # summary
    lines = []
    for ent, lane_counts in sorted(ent_lane_hits.items(), key=lambda x: sum(x[1].values()), reverse=True):
        total = sum(lane_counts.values())
        lanes = ", ".join([f"{ln}:{ct}" for ln, ct in lane_counts.most_common()])
        top_files = ent_file_hits[ent].most_common(20)
        top_files_s = "; ".join([f"{Path(fp).name}:{ct}" for fp, ct in top_files])
        lines.append(f"{ent} total_hits={total} | lanes {lanes}")
        lines.append(f"  top_files: {top_files_s}")

    summary_txt = pkg / "entity_lane_summary.txt"
    summary_txt.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote: {hits_csv}")
    print(f"Wrote: {pkg / 'entity_lane_top_files.csv'}")
    print(f"Wrote: {summary_txt}")
    print(f"Wrote: {vars_csv}")
    print(f"Wrote: {pkg / 'entity_lane_zero_reasons.csv'}")


if __name__ == "__main__":
    main()
