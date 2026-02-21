#!/usr/bin/env python3
"""Audit fornecedor coverage/eligibility + quarantine parse errors.

Outputs
- ~/Desktop/TempClaw/coverage_report.csv
- ~/Desktop/TempClaw/coverage_summary.txt
- ~/Desktop/TempClaw/quarantine_parse_errors/ (copies)
- ~/Desktop/TempClaw/quarantine_parse_errors/index.csv

This script is intentionally conservative and dependency-light:
- It parses a small subset of YAML (key: value + lists) sufficient for our config.
- It uses pandas to probe CSV/XLS/XLSX; ZIPs are inspected by member names only.

"""

from __future__ import annotations

import argparse
import csv
import fnmatch
import hashlib
import re
import shutil
import unicodedata
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


EXTS_SUPPORTED = {"xlsx", "xls", "csv", "zip"}

PROVIDERS = [
    ("band", re.compile(r"\bband\b|bandeirantes", re.I)),
    ("sbt", re.compile(r"\bsbt\b", re.I)),
    ("globo", re.compile(r"\bglobo\b|canais globo", re.I)),
    ("globoplay", re.compile(r"globoplay", re.I)),
    ("record", re.compile(r"\brecord\b", re.I)),
    ("canalbrasil", re.compile(r"canal\s*brasil", re.I)),
    ("ubem", re.compile(r"ubem", re.I)),
    ("ecad", re.compile(r"ecad", re.I)),
]


def guess_provider(path: Path) -> str:
    s = str(path)
    for name, rx in PROVIDERS:
        if rx.search(s):
            return name
    return "other"


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = strip_accents(s.casefold().strip())
    s = re.sub(r"\s+", " ", s)
    return s


def parse_simple_yaml(path: Path) -> dict:
    """Parse minimal YAML subset for this config.

    Supports:
    - key: value
    - key: (then indented list)
      - item
    - nested dict via indentation (2 spaces)

    Not a general YAML parser.
    """

    root: dict = {}
    stack: list[tuple[int, dict]] = [(0, root)]
    current_key = None

    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.rstrip("\n")
        if not line.strip() or line.strip().startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        line = line.strip()

        # unwind stack
        while stack and indent < stack[-1][0]:
            stack.pop()
        if not stack:
            stack = [(0, root)]

        cur = stack[-1][1]

        if line.startswith("-"):
            item = line[1:].strip()
            if current_key is None:
                continue
            if not isinstance(cur.get(current_key), list):
                cur[current_key] = []
            cur[current_key].append(item)
            continue

        if ":" in line:
            key, val = line.split(":", 1)
            key = key.strip()
            val = val.strip()
            current_key = key
            if val == "":
                # start nested dict
                cur[key] = {}
                stack.append((indent + 2, cur[key]))
                current_key = None
            else:
                # scalar
                cur[key] = val

    return root


def load_exclude_globs(path: Path) -> list[str]:
    if not path.exists():
        return []
    globs = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        globs.append(line)
    return globs


def is_excluded(p: Path, globs: list[str]) -> bool:
    s = str(p)
    return any(fnmatch.fnmatch(s, g) for g in globs)


@dataclass
class ProbeResult:
    sheets_count: int
    sample_sheet_names: str
    parse_status: str
    parse_exception: str
    header_hit: bool
    sheetname_hit: bool


def probe_csv(path: Path, header_terms: set[str]) -> ProbeResult:
    try:
        df = pd.read_csv(path, dtype=str, nrows=25, low_memory=False)
        cols = [norm(c) for c in df.columns]
        header_hit = any(any(t in c for c in cols) for t in header_terms)
        return ProbeResult(
            sheets_count=1,
            sample_sheet_names="csv",
            parse_status="ok",
            parse_exception="",
            header_hit=header_hit,
            sheetname_hit=False,
        )
    except Exception as e:
        return ProbeResult(0, "", "error", f"{type(e).__name__}: {e}", False, False)


def probe_excel(path: Path, header_terms: set[str], sheet_terms: set[str], max_sheets: int, max_rows: int) -> ProbeResult:
    try:
        xl = pd.ExcelFile(path)
        sheets = xl.sheet_names
        sample_names = ",".join(sheets[:10])
        sheetname_hit = any(any(st in norm(name) for st in sheet_terms) for name in sheets)

        header_hit = False
        for sh in sheets[:max_sheets]:
            try:
                df = xl.parse(sh, dtype=str, nrows=max_rows)
                cols = [norm(c) for c in df.columns]
                if any(any(t in c for c in cols) for t in header_terms):
                    header_hit = True
                    break
            except Exception:
                continue

        return ProbeResult(
            sheets_count=len(sheets),
            sample_sheet_names=sample_names,
            parse_status="ok",
            parse_exception="",
            header_hit=header_hit,
            sheetname_hit=sheetname_hit,
        )
    except Exception as e:
        return ProbeResult(0, "", "error", f"{type(e).__name__}: {e}", False, False)


def probe_zip(path: Path) -> ProbeResult:
    try:
        with zipfile.ZipFile(path) as z:
            names = z.namelist()
            # treat as "ok" if it contains any structured file
            structured = [n for n in names if Path(n).suffix.lower() in {".csv", ".xls", ".xlsx"}]
            return ProbeResult(
                sheets_count=len(structured),
                sample_sheet_names=",".join(structured[:10]),
                parse_status="ok" if structured else "ok",
                parse_exception="",
                header_hit=bool(structured),
                sheetname_hit=False,
            )
    except Exception as e:
        return ProbeResult(0, "", "error", f"{type(e).__name__}: {e}", False, False)


def safe_copy_to_quarantine(src: Path, quarantine_dir: Path) -> Path:
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    # avoid collisions by hashing full path
    h = hashlib.sha1(str(src).encode("utf-8")).hexdigest()[:12]
    dest = quarantine_dir / f"{src.stem}__{h}{src.suffix}"
    if not dest.exists():
        shutil.copy2(src, dest)
    return dest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/fornecedores_inclusion_rules.yaml")
    ap.add_argument("--out-dir", default=str(Path.home() / "Desktop" / "TempClaw"))
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = (repo_root / cfg_path).resolve()

    cfg = parse_simple_yaml(cfg_path)

    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    include_roots = [Path(p) for p in cfg.get("include_roots", [])]
    include_ext = {str(x).strip().lower().lstrip(".") for x in cfg.get("include_ext", [])}

    exclude_file = cfg.get("exclude_globs_file", "")
    exclude_path = Path(exclude_file)
    if exclude_file and not exclude_path.is_absolute():
        exclude_path = (repo_root / exclude_path).resolve()

    exclude_globs = load_exclude_globs(exclude_path) if exclude_file else []

    mh = cfg.get("minimum_heuristics", {})
    header_terms = {norm(x) for x in mh.get("header_any_of", [])}
    sheet_terms = {norm(x) for x in mh.get("sheet_name_any_of", [])}

    sampling = cfg.get("sampling", {})
    max_sheets = int(str(sampling.get("max_sheets", "5")))
    max_rows = int(str(sampling.get("max_rows_per_sheet", "25")))

    report_rows = []
    quarantine_dir = out_dir / "quarantine_parse_errors"
    quarantine_index = []

    reasons = Counter()

    for root in include_roots:
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            ext = p.suffix.lower().lstrip(".")

            if ext not in include_ext:
                reasons["unsupported_ext"] += 1
                report_rows.append(
                    {
                        "file_path": str(p),
                        "included": "N",
                        "reason": "unsupported_ext",
                        "provider_guess": guess_provider(p),
                        "sheets_count": 0,
                        "sample_sheet_names": "",
                        "parse_status": "n/a",
                        "exception": "",
                    }
                )
                continue

            if is_excluded(p, exclude_globs):
                reasons["excluded_by_glob"] += 1
                report_rows.append(
                    {
                        "file_path": str(p),
                        "included": "N",
                        "reason": "excluded_by_glob",
                        "provider_guess": guess_provider(p),
                        "sheets_count": 0,
                        "sample_sheet_names": "",
                        "parse_status": "n/a",
                        "exception": "",
                    }
                )
                continue

            # probe
            if ext == "csv":
                pr = probe_csv(p, header_terms)
            elif ext in {"xls", "xlsx", "xlsm", "xlsb"}:
                pr = probe_excel(p, header_terms, sheet_terms, max_sheets=max_sheets, max_rows=max_rows)
            elif ext == "zip":
                pr = probe_zip(p)
            else:
                pr = ProbeResult(0, "", "n/a", "", False, False)

            if pr.parse_status != "ok":
                reasons["parse_error"] += 1
                dest = safe_copy_to_quarantine(p, quarantine_dir)
                quarantine_index.append({"src": str(p), "dest": str(dest), "error": pr.parse_exception})
                report_rows.append(
                    {
                        "file_path": str(p),
                        "included": "N",
                        "reason": "parse_error",
                        "provider_guess": guess_provider(p),
                        "sheets_count": pr.sheets_count,
                        "sample_sheet_names": pr.sample_sheet_names,
                        "parse_status": pr.parse_status,
                        "exception": pr.parse_exception,
                    }
                )
                continue

            # heuristics
            heur_ok = pr.header_hit or pr.sheetname_hit
            if not heur_ok:
                reasons["fails_heuristics"] += 1
                report_rows.append(
                    {
                        "file_path": str(p),
                        "included": "N",
                        "reason": "fails_heuristics",
                        "provider_guess": guess_provider(p),
                        "sheets_count": pr.sheets_count,
                        "sample_sheet_names": pr.sample_sheet_names,
                        "parse_status": pr.parse_status,
                        "exception": "",
                    }
                )
                continue

            reasons["included"] += 1
            report_rows.append(
                {
                    "file_path": str(p),
                    "included": "Y",
                    "reason": "included",
                    "provider_guess": guess_provider(p),
                    "sheets_count": pr.sheets_count,
                    "sample_sheet_names": pr.sample_sheet_names,
                    "parse_status": pr.parse_status,
                    "exception": "",
                }
            )

    report_path = out_dir / "coverage_report.csv"
    pd.DataFrame(report_rows).to_csv(report_path, index=False)

    summary_path = out_dir / "coverage_summary.txt"
    lines = []
    lines.append(f"generated_at={datetime.now().isoformat(timespec='seconds')}\n")
    total = sum(reasons.values())
    lines.append(f"total_files_considered={total}")
    for k, v in reasons.most_common():
        lines.append(f"{k}={v}")
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    quarantine_index_path = quarantine_dir / "index.csv"
    if quarantine_index:
        with quarantine_index_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["src", "dest", "error"])
            w.writeheader()
            w.writerows(quarantine_index)
    else:
        quarantine_index_path.write_text("src,dest,error\n", encoding="utf-8")

    print(f"Wrote: {report_path} rows={len(report_rows)}")
    print(f"Wrote: {summary_path}")
    print(f"Wrote: {quarantine_index_path} rows={len(quarantine_index)}")


if __name__ == "__main__":
    main()
