#!/usr/bin/env python3
"""cleanup_shorts.py — delete shorts that were successfully published.

Only shorts recorded as "posted" in state/uploads.json are removed, together
with their .jpg/.txt/.json sidecars. Never touches a pending short.

Usage:
  python cleanup_shorts.py [--dry-run]
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SHORTS_DIR = HERE / "shorts"
STATE_DIR = HERE / "state"
UPLOADS_PATH = STATE_DIR / "uploads.json"

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return default


def main() -> int:
    dry_run = "--dry-run" in sys.argv[1:]
    uploads = load_json(UPLOADS_PATH, {})
    posted = {
        rec.get("short")
        for rec in uploads.values()
        if isinstance(rec, dict) and rec.get("status") == "posted" and rec.get("short")
    }

    if not posted:
        print("No posted shorts to clean up.", flush=True)
        return 0

    deleted = 0
    freed = 0
    for name in sorted(posted):
        for ext in (".mp4", ".jpg", ".txt", ".json"):
            f = SHORTS_DIR / f"{name}{ext}"
            if not f.exists():
                continue
            try:
                size = f.stat().st_size
                if dry_run:
                    print(f"  (dry) would delete {f.name} ({size / 1024 / 1024:.1f} MB)", flush=True)
                    continue
                f.unlink()
                freed += size
                deleted += 1
                print(f"  deleted {f.name} ({size / 1024 / 1024:.1f} MB)", flush=True)
            except Exception as e:
                print(f"  WARNING: could not delete {f.name}: {e}", flush=True)

    # stray temp files from interrupted encodes
    if SHORTS_DIR.exists() and not dry_run:
        for f in SHORTS_DIR.glob("*.tmp"):
            try:
                f.unlink()
                print(f"  removed temp {f.name}", flush=True)
            except OSError:
                pass

    if dry_run:
        print(f"\nDry run: {len(posted)} posted short(s) would be removed.", flush=True)
    else:
        print(f"\nCleanup: {deleted} file(s) deleted, {freed / 1024 / 1024:.1f} MB freed.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
