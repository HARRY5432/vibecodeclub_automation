#!/usr/bin/env python3
"""download_reels.py - top up the downloads library from creators.txt.

Downloads the latest reel from every Instagram creator listed in creators.txt,
using yt-dlp's download archive (state/download_archive.txt) so a reel is never
downloaded twice, even after its raw file is consumed or deleted.

Creators rotate, so every configured creator actually gets used.
.part cleanup only touches files older than 30 minutes.

Run:
  python download_reels.py              # download from every creator once
  python download_reels.py --max-downloads 3   # download at most 3 reels
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "config.json"
CREATORS_PATH = HERE / "creators.txt"
STATE_DIR = HERE / "state"
ARCHIVE_PATH = STATE_DIR / "download_archive.txt"
ROTATION_PATH = STATE_DIR / "downloads.json"

VIDEO_EXT = {".mp4", ".mov", ".mkv", ".avi", ".mxf", ".ts", ".webm", ".m4v"}
STALE_PART_SEC = 30 * 60

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


def atomic_write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_config() -> dict:
    with open(CONFIG, "r", encoding="utf-8-sig") as f:
        return json.load(f)


def load_creators() -> list[str]:
    if not CREATORS_PATH.exists():
        return []
    out = []
    for line in CREATORS_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(line)
    return out


def yt_dlp_cmd() -> list[str]:
    exe = shutil.which("yt-dlp")
    if exe:
        return [exe]
    return [sys.executable, "-m", "yt_dlp"]


def cleanup_stale_partials(raw_dir: Path) -> None:
    now = time.time()
    for pattern in ("*.part", "*.ytdl", "*.part-Frag*"):
        for f in raw_dir.glob(pattern):
            try:
                if now - f.stat().st_mtime > STALE_PART_SEC:
                    f.unlink(missing_ok=True)
                    print(f"  removed stale {f.name}", flush=True)
            except OSError:
                pass


def next_creator(cfg: dict) -> tuple[str, int]:
    creators = load_creators()
    if not creators:
        raise SystemExit("No creators configured in creators.txt")
    state = load_json(ROTATION_PATH, {})
    idx = int(state.get("next_creator", 0) or 0) % len(creators)
    creator = creators[idx]
    state["next_creator"] = (idx + 1) % len(creators)
    atomic_write_json(ROTATION_PATH, state)
    return creator, idx + 1


def download_one(cfg: dict, creator_url: str, out_dir: Path, slot: int) -> bool:
    fmt = str(
        cfg.get("download_format")
        or "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best"
    )
    items = int(cfg.get("download_scan_items", 10) or 10)

    cmd = yt_dlp_cmd() + [
        "--playlist-items", f"1-{items}",
        "--max-downloads", "1",
        "--download-archive", str(ARCHIVE_PATH),
        "-f", fmt,
        "--merge-output-format", "mp4",
        "-o", str(out_dir / "%(uploader)s_%(id)s.%(ext)s"),
        "--no-playlist",
        "--no-overwrites",
        "--continue",
        "--ignore-errors",
        "--no-progress",
        "--socket-timeout", "30",
        "--retries", "5",
        "--extractor-retries", "3",
        "--sleep-requests", "1",
    ]

    cookies = HERE / "cookies-instagram.txt"
    if cookies.exists():
        cmd += ["--cookies", str(cookies)]

    cmd.append(creator_url)
    print(f"Downloading from creator {slot}: {creator_url}", flush=True)
    print(f"  -> {out_dir}", flush=True)

    result = subprocess.run(cmd, cwd=str(HERE))
    if result.returncode != 0:
        print(f"  WARNING: yt-dlp exited with {result.returncode}", flush=True)
        return False
    return True


def count_raw(raw_dir: Path) -> int:
    if not raw_dir.is_dir():
        return 0
    return len([f for f in raw_dir.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXT])


def main() -> int:
    ap = argparse.ArgumentParser(description="Top up the downloads library from creators")
    ap.add_argument("--max-downloads", type=int, default=None)
    args = ap.parse_args()

    cfg = load_config()
    raw_dir = HERE / cfg.get("raw_dir", "downloads")
    raw_dir.mkdir(parents=True, exist_ok=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_PATH.touch(exist_ok=True)

    cleanup_stale_partials(raw_dir)

    want = int(args.max_downloads or cfg.get("downloads_per_run", 1) or 1)
    before = count_raw(raw_dir)
    print(f"downloads library: {before} video(s) - fetching up to {want} new one(s)", flush=True)

    failures = 0
    for i in range(want):
        try:
            creator, slot = next_creator(cfg)
        except SystemExit as e:
            print(f"ERROR: {e}", flush=True)
            return 1
        if not download_one(cfg, creator, raw_dir, slot):
            failures += 1
            if i + 1 < want:
                time.sleep(5)

    after = count_raw(raw_dir)
    print(f"\ndownloads library: {after} video(s) ({after - before:+d})", flush=True)

    if after == 0:
        print("ERROR: no downloads available after fetching.", flush=True)
        return 1
    if failures and after == before:
        print("WARNING: downloads failed but existing reels remain - continuing.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
