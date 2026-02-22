#!/usr/bin/env python3
"""Normalize + dedupe config/header_field_synonyms.yaml deterministically.

Design goals:
- No external deps (YAML parsing is purpose-built for the file's simple structure)
- Deterministic output ordering
- Normalize strings (case/whitespace/accents) and remove common artifact suffixes like "(2)"
- Remove duplicates within a field
- If a synonym appears in multiple fields, keep a single copy in the most appropriate field
  using conservative heuristics (e.g. "autor" => author, "titular" => rightsholder_owner).

This script is intentionally scoped to this repo's simple YAML shape:

field_name:\n
  - synonym 1\n
  - synonym 2\n
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from collections import OrderedDict, defaultdict
from pathlib import Path
from typing import DefaultDict


LIST_ITEM_RX = re.compile(r"^\s{2}-\s+(.*)\s*$")
TOP_KEY_RX = re.compile(r"^([a-zA-Z0-9_]+):\s*$")


def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def normalize_syn(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip()
    # remove simple "(2)" style artifacts
    s = re.sub(r"\s*\(\s*\d+\s*\)\s*$", "", s)
    s = strip_accents(s.casefold())
    s = re.sub(r"[^0-9a-z]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def preferred_field_for(syn_norm: str) -> str | None:
    """Return a better field for this synonym, or None to keep as-is."""

    # Author-ish
    if re.search(r"\b(autor|autores|compositor|compositores|composer)\b", syn_norm):
        return "author"

    # Rightsholder-ish
    if re.search(
        r"\b(titular|titulares|direitos|editora|editoras|publisher|owner|propriet)\b",
        syn_norm,
    ):
        return "rightsholder_owner"

    # Artist-ish
    if re.search(r"\b(artista|interprete|interprete|banda)\b", syn_norm):
        return "artist"

    # Date-ish
    if re.search(r"\b(data|hora|timestamp|time|exib)\b", syn_norm):
        return "date"

    return None


def parse_simple_yaml(path: Path) -> tuple[list[str], "OrderedDict[str, list[str]]"]:
    lines = path.read_text(encoding="utf-8").splitlines()

    header_comments: list[str] = []
    data: "OrderedDict[str, list[str]]" = OrderedDict()

    cur_key: str | None = None
    for ln in lines:
        if cur_key is None and (ln.strip().startswith("#") or ln.strip() == ""):
            header_comments.append(ln)
            continue

        m_key = TOP_KEY_RX.match(ln)
        if m_key:
            cur_key = m_key.group(1)
            data.setdefault(cur_key, [])
            continue

        m_item = LIST_ITEM_RX.match(ln)
        if m_item and cur_key is not None:
            data[cur_key].append(m_item.group(1))
            continue

        # ignore unexpected lines, but keep file shape stable by not trying to round-trip them

    return header_comments, data


def write_simple_yaml(path: Path, header_comments: list[str], data: "OrderedDict[str, list[str]]") -> None:
    out_lines: list[str] = []
    if header_comments:
        out_lines.extend(header_comments)
        if out_lines and out_lines[-1].strip() != "":
            out_lines.append("")

    for k, items in data.items():
        out_lines.append(f"{k}:")
        if items:
            for it in items:
                out_lines.append(f"  - {it}")
        out_lines.append("")

    path.write_text("\n".join(out_lines).rstrip() + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--in",
        dest="in_path",
        default="config/header_field_synonyms.yaml",
        help="Input YAML path",
    )
    ap.add_argument(
        "--out",
        dest="out_path",
        default="config/header_field_synonyms.yaml",
        help="Output YAML path (default: overwrite input)",
    )
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    header_comments, data_raw = parse_simple_yaml(in_path)

    # Normalize + dedupe within fields
    data_norm: "OrderedDict[str, list[str]]" = OrderedDict()
    seen_global: dict[str, str] = {}  # syn_norm -> field
    buckets: DefaultDict[str, list[str]] = defaultdict(list)  # field -> list[syn_norm]

    for field, items in data_raw.items():
        seen_local: set[str] = set()
        for raw in items:
            syn = normalize_syn(raw)
            if not syn:
                continue
            if syn in seen_local:
                continue
            seen_local.add(syn)
            buckets[field].append(syn)

    # Resolve duplicates across fields with conservative preference
    # Iterate fields in file order for determinism.
    field_order = list(data_raw.keys())

    for field in field_order:
        for syn in buckets.get(field, []):
            pref = preferred_field_for(syn) or field
            # If already assigned, keep the existing assignment.
            if syn in seen_global:
                continue
            seen_global[syn] = pref

    # Build final per-field lists in deterministic order: preserve original encounter order.
    final_per_field: DefaultDict[str, list[str]] = defaultdict(list)
    for field in field_order:
        for syn in buckets.get(field, []):
            assigned = seen_global.get(syn)
            if assigned is None:
                continue
            if syn not in final_per_field[assigned]:
                final_per_field[assigned].append(syn)

    for field in field_order:
        data_norm[field] = final_per_field.get(field, [])

    write_simple_yaml(out_path, header_comments, data_norm)


if __name__ == "__main__":
    main()
