#!/bin/zsh
set -euo pipefail
PARTS_DIR="/Volumes/Macntosh HD 2/shrkanalytics/Estelita/mbox_processed/parts_4gb"
OUT_DIR="/Users/igorcunha/.openclaw/workspace/estelita_progress/inventory_all"
SCRIPT="/Users/igorcunha/.openclaw/workspace/estelita_mbox_attachment_inventory.py"
mkdir -p "$OUT_DIR"

for i in {0..28}; do
  mbox="$PARTS_DIR/mbox_part_$(printf "%04d" $i).mbox"
  out="$OUT_DIR/inventory_part$(printf "%04d" $i).csv"
  if [ ! -f "$mbox" ]; then
    echo "SKIP missing $mbox" >&2
    continue
  fi
  if [ -f "$out" ]; then
    echo "SKIP already exists $out" >&2
    continue
  fi
  echo "inventory $mbox -> $out" >&2
  python3 "$SCRIPT" --mbox "$mbox" --out "$out"
done

echo "DONE" >&2
