#!/usr/bin/env python3
"""Build fornecedor file registry + structured worksets.

This replaces ad-hoc one-off snippets with a repeatable script.

Outputs:
- fornecedores_file_registry.csv
- fornecedores_structured_workset.csv
- fornecedores_top_candidates_structured.csv
- fornecedores_archives_queue.csv

Supports exclude globs (one per line) to skip duplicate/non-supplier sources.
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import re
from datetime import datetime
from pathlib import Path


EXTS = {".csv", ".xls", ".xlsx", ".xlsm", ".xlsb", ".zip", ".rar", ".7z", ".pdf"}

PROVIDERS = [
    ("band", re.compile(r"\bband\b|bandeirantes", re.I)),
    ("sbt", re.compile(r"\bsbt\b", re.I)),
    ("globo", re.compile(r"\bglobo\b|canais globo", re.I)),
    ("globoplay", re.compile(r"globoplay", re.I)),
    ("record", re.compile(r"\brecord\b", re.I)),
    ("canalbrasil", re.compile(r"canal\s*brasil", re.I)),
    ("ubem", re.compile(r"ubem", re.I)),
    ("ecad", re.compile(r"ecad", re.I)),
    ("spotify", re.compile(r"spotify", re.I)),
]


def guess_provider(path: Path) -> str:
    s = str(path)
    for name, rx in PROVIDERS:
        if rx.search(s):
            return name
    return "unknown"


def sha1_head(path: Path, nbytes: int = 1024 * 1024) -> tuple[str, int]:
    h = hashlib.sha1()
    with path.open("rb") as f:
        h.update(f.read(nbytes))
    size = path.stat().st_size
    return h.hexdigest(), min(size, nbytes)


def load_excludes(path: Path) -> list[str]:
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
    for g in globs:
        if fnmatch.fnmatch(s, g):
            return True
    return False


def iter_files(roots: list[Path], globs: list[str]) -> list[Path]:
    out: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() not in EXTS:
                continue
            if is_excluded(p, globs):
                continue
            out.append(p)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", action="append", required=True, help="Root directory (repeatable)")
    ap.add_argument("--exclude-globs", default="config/fornecedores_exclude_globs.txt")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--top-n", type=int, default=400)
    args = ap.parse_args()

    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    repo_root = Path(__file__).resolve().parent.parent
    exclude_path = (repo_root / args.exclude_globs).resolve() if not Path(args.exclude_globs).is_absolute() else Path(args.exclude_globs)
    excludes = load_excludes(exclude_path)

    roots = [Path(r).expanduser() for r in args.root]
    files = iter_files(roots, excludes)

    registry_path = out_dir / "fornecedores_file_registry.csv"
    structured_path = out_dir / "fornecedores_structured_workset.csv"
    top_path = out_dir / "fornecedores_top_candidates_structured.csv"
    archives_path = out_dir / "fornecedores_archives_queue.csv"

    rows = []
    for p in files:
        try:
            st = p.stat()
            sh, shb = sha1_head(p)
            rows.append(
                {
                    "path": str(p),
                    "root": next((str(r) for r in roots if str(p).startswith(str(r))), ""),
                    "ext": p.suffix.lower(),
                    "size_bytes": st.st_size,
                    "mtime": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds"),
                    "provider_guess": guess_provider(p),
                    "sha1_head": sh,
                    "sha1_head_bytes": shb,
                }
            )
        except Exception:
            continue

    # write registry
    import pandas as pd

    df = pd.DataFrame(rows)
    df.to_csv(registry_path, index=False)

    structured = df[df["ext"].isin([".csv", ".xls", ".xlsx", ".xlsm", ".xlsb"])].copy()
    structured.to_csv(structured_path, index=False)

    # top candidates
    structured["provider_priority"] = structured["provider_guess"].apply(lambda x: 0 if x != "unknown" else 1)
    structured["mtime_dt"] = pd.to_datetime(structured["mtime"], errors="coerce")

    top = structured.sort_values(["provider_priority", "size_bytes", "mtime_dt"], ascending=[True, False, False]).head(args.top_n)
    top.to_csv(top_path, index=False)

    archives = df[df["ext"].isin([".zip", ".rar", ".7z"])].copy().sort_values(["size_bytes"], ascending=False)
    archives.to_csv(archives_path, index=False)

    print(f"Wrote: {registry_path} rows={len(df)}")
    print(f"Wrote: {structured_path} rows={len(structured)}")
    print(f"Wrote: {top_path} rows={len(top)}")
    print(f"Wrote: {archives_path} rows={len(archives)}")


if __name__ == "__main__":
    main()
