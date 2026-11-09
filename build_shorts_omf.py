#!/usr/bin/env python3
"""build_shorts_omf.py - slice raw footage into shorts, original aspect ratio.

Cuts each raw video into short_seconds-long blocks (default 40s):
shorts/s{YYYYMMDD}_001.mp4 = 0-40s, s{YYYYMMDD}_002.mp4 = 40-80s, etc.
The final block keeps whatever time is left, even if shorter.

No vertical conversion, no cropping, no re-rotation - original aspect ratio
is preserved exactly as downloaded.

Reliability notes
-----------------
* manifest.json is APPENDED to, never rebuilt from scratch.
* A failed encode no longer aborts the whole step when other clips succeeded;
  the failed ranges simply stay unclaimed and are retried next run.
* `-threads 0` from config no longer gets coerced to 1; the flag is omitted
  when <= 0.
* Encode timeout is configurable (4K clips can exceed 5 minutes).
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "config.json"
VIDEO_EXT = {".mp4", ".mov", ".mkv", ".avi", ".mxf", ".ts", ".webm", ".m4v"}
MAX_ENCODE_RETRIES = 2
RETRY_DELAY_SEC = 15

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def die(msg: str, code: int = 1):
    print(f"\nERROR: {msg}", flush=True)
    sys.exit(code)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        die(f"config.json not found at {CONFIG_PATH}")
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        die(f"config.json invalid JSON: {e}")


def natural_key(name: str):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", name)]


def list_videos(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(
        [directory / n for n in os.listdir(directory) if Path(n).suffix.lower() in VIDEO_EXT],
        key=lambda p: natural_key(p.name),
    )


def atomic_write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def consumed_sources(manifest) -> set:
    return {e.get("source") for e in manifest if isinstance(e, dict)}


def batch_prefix() -> str:
    """Date tag so short file names are unique across days."""
    return datetime.now().strftime("%Y%m%d")


def next_short_index(output_dir: Path, prefix: str) -> int:
    pattern = re.compile(rf"^s{prefix}_(\d+)\.mp4$", re.IGNORECASE)
    highest = 0
    if output_dir.is_dir():
        for n in os.listdir(output_dir):
            m = pattern.match(n)
            if m:
                highest = max(highest, int(m.group(1)))
    return highest + 1


def probe_duration(ffprobe: str, path: Path) -> float | None:
    try:
        out = subprocess.run(
            [ffprobe, "-v", "error", "-show_entries", "format=duration",
             "-of", "json", str(path)],
            capture_output=True, text=True, timeout=120,
        )
        return float(json.loads(out.stdout)["format"]["duration"])
    except Exception:
        return None


def file_is_stable(path: Path) -> bool:
    try:
        s1 = os.path.getsize(path)
        time.sleep(2)
        return s1 == os.path.getsize(path)
    except OSError:
        return False


def build_cut_command(cfg: dict, src: Path, start: float, dur: float,
                      out_path: Path, ffmpeg: str) -> list[str]:
    """Build ffmpeg command that slices [start, start+dur) without vertical
    conversion. Original aspect ratio and resolution are preserved."""
    cmd = [ffmpeg, "-y", "-ss", f"{start:.3f}", "-t", f"{dur:.3f}", "-i", str(src)]
    # No -vf filter: keep original resolution / aspect ratio as downloaded.
    if bool(cfg.get("keep_audio", True)):
        cmd += ["-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2"]
    else:
        cmd += ["-an"]
    cmd += [
        "-c:v", str(cfg.get("video_codec", "libx264")),
        "-preset", str(cfg.get("preset", "fast")),
        "-crf", str(cfg.get("crf", 20)),
    ]
    max_kbps = int(cfg.get("max_bitrate_kbps", 0) or 0)
    if max_kbps > 0:
        cmd += ["-maxrate", f"{max_kbps}k", "-bufsize", f"{max_kbps * 2}k"]
    threads = int(cfg.get("ffmpeg_threads", 0) or 0)
    if threads > 0:
        cmd += ["-threads", str(threads)]
    cmd += ["-movflags", "+faststart", str(out_path)]
    return cmd


def copy_sidecars(src: Path, out_path: Path) -> None:
    """Copy original .txt caption and .jpg thumbnail from source to output short."""
    for ext in (".txt", ".jpg"):
        src_sidecar = src.with_suffix(ext)
        if src_sidecar.exists():
            dst_sidecar = out_path.with_suffix(ext)
            try:
                shutil.copy2(src_sidecar, dst_sidecar)
                print(f"  sidecar copied: {dst_sidecar.name}", flush=True)
            except Exception as e:
                print(f"  WARNING: could not copy sidecar {src_sidecar.name}: {e}", flush=True)

def run_cut_with_retry(cfg, src, start, dur, out_path, ffmpeg) -> bool:
    timeout = int(cfg.get("encode_timeout_sec", 900))
    for attempt in range(1, MAX_ENCODE_RETRIES + 1):
        try:
            cmd = build_cut_command(cfg, src, start, dur, out_path, ffmpeg)
            proc = subprocess.run(
                cmd, capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=timeout,
            )
            if proc.returncode == 0 and out_path.exists() and out_path.stat().st_size > 1024:
                return True
            print(f"  ffmpeg attempt {attempt} failed (code {proc.returncode})", flush=True)
            if proc.stderr:
                print(f"  {proc.stderr.strip().splitlines()[-1][:200]}", flush=True)
        except subprocess.TimeoutExpired:
            print(f"  ffmpeg attempt {attempt} hit the {timeout}s timeout", flush=True)
        except Exception as e:
            print(f"  ffmpeg attempt {attempt} error: {e}", flush=True)

        try:
            if out_path.exists():
                out_path.unlink()
        except Exception:
            pass

        if attempt < MAX_ENCODE_RETRIES:
            time.sleep(RETRY_DELAY_SEC)
    return False


def process_pending(cfg: dict, raw_dir: Path, output_dir: Path, ffmpeg: str,
                    ffprobe: str, dry_run: bool, max_shorts: int | None) -> tuple[list, int]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    short_seconds = float(cfg.get("short_seconds", 40))
    manifest = load_json(output_dir / "manifest.json", [])
    if not isinstance(manifest, list):
        manifest = []
    done_sources = consumed_sources(manifest)
    skip = set(cfg.get("skip_files", []))

    pending = []
    for p in list_videos(raw_dir):
        if p.name in skip or p.name in done_sources:
            continue
        if file_is_stable(p):
            pending.append(p)
        else:
            print(f"(waiting: {p.name} is still being written)", flush=True)

    if not pending:
        print("No pending raw videos.", flush=True)
        return manifest, 0

    jobs = []
    planned: dict[str, list] = {}
    prefix = batch_prefix()
    idx = next_short_index(output_dir, prefix)
    for src in pending:
        dur = probe_duration(ffprobe, src)
        if dur is None:
            print(f"SKIP {src.name}: cannot read duration", flush=True)
            continue

        entries = []
        t = 0.0
        while t < dur:
            w = min(short_seconds, dur - t)
            if w < 1.0:
                print(f"  ({src.name}: dropping {w:.1f}s tail)", flush=True)
                break
            out_name = f"s{prefix}_{idx:03d}.mp4"
            idx += 1
            jobs.append((src, t, w, output_dir / out_name))
            entries.append(
                {"short": out_name, "source": src.name,
                 "start": round(t, 3), "duration": round(w, 3)}
            )
            t += w
            if max_shorts and len(jobs) >= max_shorts:
                break
        planned[src.name] = entries
        print(f"plan  {src.name}: {len(entries)} short(s)", flush=True)
        if max_shorts and len(jobs) >= max_shorts:
            break

    if not jobs:
        print("Nothing to encode.", flush=True)
        return manifest, 0

    total = len(jobs)
    done = 0
    lock = threading.Lock()
    workers = max(1, int(cfg.get("max_workers", 1)))
    failures: list[tuple[str, str]] = []
    made: list[str] = []
    reused: list[str] = []

    def run_cut(job):
        src, start, w, out_path = job
        if dry_run:
            return job, True, False
        if out_path.exists() and out_path.stat().st_size > 1024:
            return job, True, True
        return job, run_cut_with_retry(cfg, src, start, w, out_path, ffmpeg), False

    print(f"Encoding {total} short(s) with {workers} worker(s)...", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(run_cut, j) for j in jobs]
        for fut in as_completed(futures):
            (src, start, w, out_path), success, existed = fut.result()
            with lock:
                done += 1
                label = out_path.name
                if success and existed:
                    reused.append(label)
                    print(f"  [{done:>3}/{total}] = {label} already encoded", flush=True)
                elif success and dry_run:
                    print(f"  [{done:>3}/{total}] (dry) {label} <- {src.name} @ {start:.0f}s", flush=True)
                elif success:
                    made.append(label)
                    if not dry_run:
                        copy_sidecars(src, out_path)
                    print(f"  [{done:>3}/{total}] + {label} <- {src.name} @ {start:.0f}s", flush=True)
                else:
                    failures.append((label, src.name))
                    print(f"  [{done:>3}/{total}] x {label} <- {src.name} @ {start:.0f}s FAILED", flush=True)

    ok_names = set(made) | set(reused)
    if failures:
        print("\nFailed encodes (retried next run):", flush=True)
        for label, src_name in failures:
            print(f"    {label} (from {src_name})", flush=True)

    if not dry_run and ok_names:
        new_entries = [
            e for entries in planned.values() for e in entries if e["short"] in ok_names
        ]
        kept = [e for e in manifest if e.get("short") not in ok_names]
        merged = kept + new_entries
        merged.sort(key=lambda e: natural_key(str(e.get("short", ""))))
        atomic_write_json(output_dir / "manifest.json", merged)
        print(f"\n{len(made)} encoded, {len(reused)} reused - manifest now {len(merged)} entries.", flush=True)
        manifest = merged

    if failures and not ok_names:
        print("\nERROR: every encode failed.", flush=True)
        return manifest, 1

    return manifest, 0


def main() -> int:
    args = sys.argv[1:]
    dry_run = "--dry-run" in args
    force = "--force" in args
    max_shorts = None

    if "--max-shorts" in args:
        try:
            max_shorts = max(1, int(args[args.index("--max-shorts") + 1]))
        except Exception:
            die("--max-shorts requires a positive integer")

    cfg = load_config()
    raw_dir = HERE / cfg.get("raw_dir", "downloads")
    output_dir = HERE / cfg.get("output_dir", "shorts")

    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    if not dry_run:
        if not ffmpeg:
            die("ffmpeg not found on PATH. Install it (winget install ffmpeg / apt-get install ffmpeg).")
        if not ffprobe:
            die("ffprobe not found on PATH (it ships with ffmpeg).")
        ffmpeg = os.path.abspath(ffmpeg)
        ffprobe = os.path.abspath(ffprobe)

    if force:
        p = output_dir / "manifest.json"
        if p.exists():
            p.unlink()

    daily_limit = int(cfg.get("daily_shorts", 5))
    existing = len(list(output_dir.glob("s*.mp4"))) if not force else 0
    remaining = daily_limit - existing
    if remaining <= 0:
        print(f"Already {existing} shorts waiting (daily cap {daily_limit}). Nothing to build.", flush=True)
        return 0
    if max_shorts is None or max_shorts > remaining:
        max_shorts = remaining

    manifest, rc = process_pending(cfg, raw_dir, output_dir, ffmpeg, ffprobe, dry_run, max_shorts)
    if dry_run:
        print("\nDRY RUN - nothing written.", flush=True)
    else:
        print(f"\nDone: {len(manifest)} short(s) tracked in '{output_dir.name}'.", flush=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())

