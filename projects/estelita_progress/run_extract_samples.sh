#!/bin/zsh
set -euo pipefail
BASE="/Volumes/Macntosh HD 2/shrkanalytics/Estelita/mbox_processed/parts_4gb"
OUT="/Users/igorcunha/.openclaw/workspace/estelita_progress/extracted_samples"
mkdir -p "$OUT"
python3 - <<'PY'
import json
plan=json.load(open("/Users/igorcunha/.openclaw/workspace/estelita_progress/extraction_plan.json","r",encoding="utf-8"))
for it in plan:
  print(f"{it["bucket"]}	{it["part"]}	{it["msg_index"]}")
PY
echo "--- extracting ---"
python3 - <<'PY'
import json, os, subprocess
plan=json.load(open("/Users/igorcunha/.openclaw/workspace/estelita_progress/extraction_plan.json","r",encoding="utf-8"))
base="/Volumes/Macntosh HD 2/shrkanalytics/Estelita/mbox_processed/parts_4gb"
out_root="/Users/igorcunha/.openclaw/workspace/estelita_progress/extracted_samples"
script="/Users/igorcunha/.openclaw/workspace/estelita_extract_attachments.py"
os.makedirs(out_root, exist_ok=True)
for it in plan:
  bucket=it["bucket"]
  part=it["part"]
  msg=it["msg_index"]
  mbox=os.path.join(base, f"mbox_part_{part}.mbox")
  out_dir=os.path.join(out_root, bucket, f"part{part}_msg{msg:06d}")
  if os.path.exists(os.path.join(out_dir,"manifest.json")):
    continue
  os.makedirs(out_dir, exist_ok=True)
  pref=f"{bucket}__part{part}_msg{msg:06d}"
  subprocess.run(["python3", script, "--mbox", mbox, "--msg-index", str(msg), "--out-dir", out_dir, "--prefix", pref], check=True)
print("DONE")
PY
