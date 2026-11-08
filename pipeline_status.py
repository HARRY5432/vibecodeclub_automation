#!/usr/bin/env python3
"""pipeline_status.py â€” the shared brain that reports on BOTH nodes.

  local   â€” this Windows machine (builds shorts, posts the 08:30 slot)
  github  â€” the night workflow (posts 19:30 / 22:30 / 01:00 / 05:00)

Every run records itself in state/nodes.json, which is committed, so each node
can see the other's health. `render(snapshot())` is the message sent to
Telegram at the start and end of every run (and when the PC boots), showing:

  * today's slot matrix â€” who OWNED it and which NODE actually published it
  * per-node state (running / ok / failed), last run, run count, totals
  * how many shorts were built today vs waiting on disk
  * lock holder, disk space, git state, and anything that needs attention

Usage:
  python pipeline_status.py                  # print the block
  python pipeline_status.py --json           # machine-readable
  python pipeline_status.py --notify         # send to Telegram
  python pipeline_status.py --start          # mark this node "running"
  python pipeline_status.py --record --result ok --built 3 --posted 2
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

IST = ZoneInfo("Asia/Kolkata")
NODE = os.getenv("PIPELINE_NODE", "local")

HERE = Path(__file__).resolve().parent
RAW_DIR = HERE / "downloads"
SHORTS_DIR = HERE / "shorts"
STATE_DIR = HERE / "state"
LOGS_DIR = HERE / "logs"
UPLOADS_PATH = STATE_DIR / "uploads.json"
NODES_PATH = STATE_DIR / "nodes.json"
MANIFEST_PATH = SHORTS_DIR / "manifest.json"
LOCK_PATH = STATE_DIR / "pipeline.lock"
CONFIG_PATH = HERE / "config.json"
SLOTS_PATH = HERE / "slots.json"

VIDEO_EXTS = {".mp4", ".mov", ".mkv", ".avi", ".mxf", ".ts", ".webm", ".m4v"}

DEFAULT_SLOTS = [
    {"time": "19:30", "label": "US East Lunch + EU Afternoon", "node": "github"},
    {"time": "22:30", "label": "US East Prime Evening + EU Night", "node": "github"},
    {"time": "01:00", "label": "US East Late Night + AU Morning", "node": "github"},
    {"time": "05:00", "label": "US East Early Morning + Asia Start", "node": "github"},
    {"time": "08:30", "label": "US East Morning + India Night", "node": "local"},
]

LOW_DISK_GB = 10.0
MISSED_AFTER_MIN = 60
LOCAL_SILENT_MIN = 180
GITHUB_SILENT_HOURS = 30

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# IO
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
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
    return load_json(CONFIG_PATH, {})


def ist_now() -> datetime:
    return datetime.now(IST)


def _parse_slot(slot: dict, now: datetime) -> datetime | None:
    try:
        hh, mm = map(int, str(slot["time"]).split(":"))
    except Exception:
        return None
    return now.replace(hour=hh, minute=mm, second=0, microsecond=0)


def _fmt_ist(iso_utc: str | None) -> str:
    if not iso_utc:
        return "never"
    try:
        dt = datetime.fromisoformat(iso_utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(IST).strftime("%Y-%m-%d %H:%M IST")
    except Exception:
        return str(iso_utc)[:16]


def _ago(iso_utc: str | None) -> str:
    if not iso_utc:
        return "never"
    try:
        dt = datetime.fromisoformat(iso_utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        mins = (datetime.now(timezone.utc) - dt).total_seconds() / 60
    except Exception:
        return "?"
    if mins < 1:
        return "just now"
    if mins < 90:
        return f"{mins:.0f}m ago"
    if mins < 48 * 60:
        return f"{mins / 60:.1f}h ago"
    return f"{mins / 1440:.1f}d ago"


def _minutes_since(iso_utc: str | None) -> float | None:
    if not iso_utc:
        return None
    try:
        dt = datetime.fromisoformat(iso_utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 60
    except Exception:
        return None


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# FACTS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def slots() -> list[dict]:
    data = load_json(SLOTS_PATH, None)
    if isinstance(data, dict) and data.get("slots"):
        return data["slots"]
    return DEFAULT_SLOTS


def ledger() -> dict:
    return load_json(UPLOADS_PATH, {})


RAW_INDEX_PATH = STATE_DIR / "raw_index.json"


def _probe_duration(path: Path, cache: dict) -> float:
    """Length of a raw video in seconds, cached by name+size+mtime."""
    try:
        st = path.stat()
    except OSError:
        return 0.0
    key = f"{path.name}:{st.st_size}:{int(st.st_mtime)}"
    cached = cache.get(key)
    if isinstance(cached, (int, float)) and cached > 0:
        return float(cached)
    dur = 0.0
    try:
        r = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=nw=1:nk=1", str(path),
            ],
            capture_output=True, text=True, timeout=60,
        )
        dur = float((r.stdout or "").strip() or 0)
    except Exception:
        dur = 0.0
    if dur > 0:
        cache[key] = round(dur, 2)
    return dur


def raw_inventory(files: list[Path], manifest: list[dict], short_secs: int) -> list[dict]:
    """How much footage is LEFT in each raw video, in whole clips.

    A raw video is not "used up" because a clip was cut from it: the build
    slices 40 s windows off the front, so one long video yields many clips.
    clips_left is what the operator needs before deciding to add footage.
    """
    used_by_source: dict[str, int] = {}
    for entry in manifest:
        if isinstance(entry, dict) and entry.get("source"):
            src = str(entry["source"])
            used_by_source[src] = used_by_source.get(src, 0) + 1

    cache = load_json(RAW_INDEX_PATH, {})
    if not isinstance(cache, dict):
        cache = {}
    fresh_cache: dict = {}
    rows = []
    for f in files:
        dur = _probe_duration(f, cache)
        try:
            st = f.stat()
            key = f"{f.name}:{st.st_size}:{int(st.st_mtime)}"
            if key in cache:
                fresh_cache[key] = cache[key]
        except OSError:
            pass
        total = int(dur // short_secs) if dur > 0 and short_secs > 0 else 0
        used = int(used_by_source.get(f.name, 0))
        rows.append(
            {
                "file": f.name,
                "duration_min": round(dur / 60, 1) if dur > 0 else None,
                "clips_total": total,
                "clips_used": used,
                "clips_left": max(0, total - used),
            }
        )
    if fresh_cache != cache:
        atomic_write_json(RAW_INDEX_PATH, fresh_cache)
    rows.sort(key=lambda r: (r["clips_left"], r["file"]))
    return rows


def raw_counts() -> dict:
    cfg = load_config()
    short_secs = int(cfg.get("short_seconds", 40) or 40)
    manifest = load_json(MANIFEST_PATH, [])
    if not isinstance(manifest, list):
        manifest = []
    per_batch = max(1, int(cfg.get("daily_shorts", 5) or 5))
    empty = {
        "total": 0, "consumed": 0, "remaining": 0, "files": [],
        "slices": [], "slices_total": 0, "slices_used": 0, "slices_left": 0,
        "reels_total": 0, "reels_used": 0, "reels_left": 0,
        "footage_min": 0.0, "batches_left": 0, "per_batch": per_batch,
        "short_secs": short_secs, "dir": RAW_DIR.exists(),
    }
    if not RAW_DIR.exists():
        return empty
    skip = set(cfg.get("skip_files", []))
    files = [
        f for f in RAW_DIR.iterdir()
        if f.is_file() and f.suffix.lower() in VIDEO_EXTS and f.name not in skip
    ]
    consumed = {
        e.get("source") for e in manifest if isinstance(e, dict)
    }
    remaining = [f for f in files if f.name not in consumed]
    slices = raw_inventory(files, manifest, short_secs)
    # Everything below is counted in REELS of short_secs: one raw video holds
    # duration//short_secs reels, minus the ones already sliced out of it.
    reels_total = sum(r["clips_total"] for r in slices)
    reels_used = sum(r["clips_used"] for r in slices)
    reels_left = sum(r["clips_left"] for r in slices)
    return {
        "total": len(files),
        "consumed": len(files) - len(remaining),
        "remaining": len(remaining),
        "files": sorted(f.name for f in remaining),
        "slices": slices,
        "slices_total": reels_total,
        "slices_used": reels_used,
        "slices_left": reels_left,
        "reels_total": reels_total,
        "reels_used": reels_used,
        "reels_left": reels_left,
        "footage_min": round(sum((r.get("duration_min") or 0) for r in slices), 1),
        "batches_left": reels_left // per_batch,
        "per_batch": per_batch,
        "short_secs": short_secs,
        "dir": RAW_DIR.exists(),
    }


def pending_shorts() -> list[str]:
    if not SHORTS_DIR.exists():
        return []
    return sorted(p.stem for p in SHORTS_DIR.glob("s*.mp4"))


def manifest_stats() -> dict:
    entries = load_json(MANIFEST_PATH, [])
    if not isinstance(entries, list):
        entries = []
    today_tag = f"s{ist_now():%Y%m%d}"
    today = sum(1 for e in entries if str(e.get("short", "")).startswith(today_tag))
    return {"total": len(entries), "today": today}


def posted_on(date_str: str) -> dict:
    rows = {}
    for key, rec in ledger().items():
        if key.startswith(date_str + "#") and rec.get("status") == "posted":
            rows[key.split("#", 1)[1]] = rec
    return rows


def slot_matrix(date_str: str, now: datetime | None = None) -> list[dict]:
    """One row per slot: who owns it, and who actually published it."""
    now = now or ist_now()
    uploads = ledger()
    rows = []
    for i, slot in enumerate(slots()):
        row = {
            "index": i,
            "time": slot.get("time"),
            "label": slot.get("label", ""),
            "owner": str(slot.get("node", "any")).lower(),
        }
        rec = uploads.get(f"{date_str}#{i}")
        if isinstance(rec, dict) and rec.get("status") == "posted":
            row.update(
                state="posted",
                by=str(rec.get("node") or row["owner"]).lower(),
                short=rec.get("short"),
                posted_at=rec.get("posted_at"),
            )
        else:
            due_at = _parse_slot(slot, now)
            if due_at is None:
                row["state"] = "scheduled"
            else:
                delta_min = (now - due_at).total_seconds() / 60
                if delta_min < 0:
                    row["state"] = "scheduled"
                elif delta_min <= due_window_min():
                    row["state"] = "due"
                    row["overdue_min"] = int(delta_min)
                else:
                    row["state"] = "missed"
                    row["overdue_min"] = int(delta_min)
        rows.append(row)
    return rows


def due_window_min() -> int:
    """Minutes after a slot time during which it may still be published."""
    try:
        return max(0, int(load_config().get("slot_due_window_min", 360) or 360))
    except Exception:
        return 360


def due_slots(node: str, now: datetime | None = None) -> list[dict]:
    now = now or ist_now()
    out = []
    for i, slot in enumerate(slots()):
        if str(slot.get("node", "any")) not in (node, "any"):
            continue
        due_at = _parse_slot(slot, now)
        if due_at is not None and now >= due_at:
            out.append({"index": i, **slot})
    return out


def lock_state() -> dict:
    if not LOCK_PATH.exists():
        return {"present": False}
    age_min = None
    try:
        age_min = round((datetime.now().timestamp() - LOCK_PATH.stat().st_mtime) / 60, 1)
    except Exception:
        pass
    holder = {}
    raw = ""
    try:
        raw = LOCK_PATH.read_text(encoding="utf-8").strip()
        parsed = json.loads(raw)
        holder = parsed if isinstance(parsed, dict) else {"pid": int(parsed)}
    except Exception:
        try:
            holder = {"pid": int(raw.split()[0])}
        except Exception:
            holder = {}
    return {
        "present": True,
        "pid": holder.get("pid"),
        "node": holder.get("node"),
        "age_min": age_min,
    }


def disk_free_gb() -> float:
    try:
        return round(shutil.disk_usage(HERE).free / 1024 ** 3, 1)
    except Exception:
        return 0.0


def git_facts() -> dict:
    if not (HERE / ".git").exists():
        return {"repo": False}

    def g(*args: str) -> str:
        try:
            r = subprocess.run(
                ["git", *args], cwd=str(HERE), capture_output=True, text=True, timeout=20
            )
            return r.stdout.strip()
        except Exception:
            return ""

    return {
        "repo": True,
        "branch": g("rev-parse", "--abbrev-ref", "HEAD"),
        "head": g("rev-parse", "--short", "HEAD"),
        "dirty": len([x for x in g("status", "--porcelain").splitlines() if x.strip()]),
        "ahead": g("rev-list", "--count", "@{u}..HEAD") or "0",
        "behind": g("rev-list", "--count", "HEAD..@{u}") or "0",
    }


def node_records() -> dict:
    return load_json(NODES_PATH, {})


def record_node_run(
    result: str,
    note: str = "",
    built: int | None = None,
    posted: int | None = None,
    count_run: bool = True,
) -> dict:
    recs = node_records()
    rec = recs.setdefault(
        NODE, {"runs": 0, "total_built": 0, "total_posted": 0, "last_run_utc": None}
    )
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if count_run:
        rec["runs"] = int(rec.get("runs", 0)) + 1
    rec["last_run_utc"] = now
    rec["state"] = result
    rec["last_result"] = result
    rec["last_note"] = str(note)[:120]
    rec["host"] = socket.gethostname()
    if built is not None:
        rec["last_built"] = int(built)
        rec["total_built"] = int(rec.get("total_built", 0)) + int(built)
    if posted is not None:
        rec["last_posted"] = int(posted)
        rec["total_posted"] = int(rec.get("total_posted", 0)) + int(posted)
    atomic_write_json(NODES_PATH, recs)
    return rec


def attention(snap: dict) -> list[str]:
    """Everything a human should act on, in priority order."""
    items = []
    rows = snap["slots_today"]

    for row in rows:
        if row["state"] == "missed":
            items.append(
                f"slot {row['time']} ({row['owner']}) is {row['overdue_min']}m overdue and unposted"
            )
        elif row["state"] == "due":
            items.append(f"slot {row['time']} ({row['owner']}) is due now â€” waiting for {row['owner']}")

    raw = snap["raw"]
    slices_left = int(raw.get("slices_left") or 0)
    per_batch = max(1, int(raw.get("per_batch") or 5))
    secs = int(raw.get("short_secs") or 40)
    if not raw.get("dir"):
        # raw/ only exists on the PC; the night node must not cry "raw empty"
        pass
    elif raw["total"] == 0:
        items.append(
            "raw/ is empty â€” add footage by hand, the pipeline never downloads on its own"
        )
    elif slices_left == 0:
        items.append(
            f"RAW EXHAUSTED â€” 0 reels of {secs}s left in the raw videos; "
            "add new footage (nothing is downloaded automatically)"
        )
    elif slices_left <= per_batch:
        items.append(
            f"raw is running out: {slices_left} reels of {secs}s left "
            f"({slices_left / per_batch:.1f} batches of {per_batch})"
        )

    if snap["disk_free_gb"] < LOW_DISK_GB:
        items.append(f"disk is low: {snap['disk_free_gb']} GB free")

    lock = snap["lock"]
    if lock.get("present") and (lock.get("age_min") or 0) > 30:
        items.append(
            f"pipeline lock held for {lock['age_min']}m by {lock.get('node') or '?'} "
            f"(pid {lock.get('pid') or '?'})"
        )

    now = ist_now()
    nodes = snap["nodes"]
    local = nodes.get("local")
    if local:
        mins = _minutes_since(local.get("last_run_utc"))
        if mins is not None and 7 <= now.hour < 19 and mins > LOCAL_SILENT_MIN:
            items.append(f"local node silent for {mins / 60:.1f}h during the daytime window")
    gh = nodes.get("github")
    if gh:
        mins = _minutes_since(gh.get("last_run_utc"))
        if mins is not None and mins > GITHUB_SILENT_HOURS * 60:
            items.append(f"github node has not run in {mins / 60 / 24:.1f}d")
    elif 7 <= now.hour < 19:
        items.append("github node has never reported (first night run pending)")

    if snap["git"].get("dirty"):
        items.append(f"{snap['git']['dirty']} uncommitted change(s) in the repo")

    return items


def snapshot() -> dict:
    now = ist_now()
    date_str = now.strftime("%Y-%m-%d")
    rows = slot_matrix(date_str, now)
    posted = [r for r in rows if r["state"] == "posted"]
    raw = raw_counts()
    pending = pending_shorts()
    manifest = manifest_stats()
    recs = node_records()

    by_node = {}
    for row in posted:
        by_node[row["by"]] = by_node.get(row["by"], 0) + 1

    snap = {
        "node": NODE,
        "now_ist": now.strftime("%Y-%m-%d %H:%M IST"),
        "date": date_str,
        "raw": raw,
        "shorts": {
            "pending": len(pending),
            "names": pending[:10],
            "posted_today": len(posted),
            "slots_total": len(rows),
            "due_for_node": len(due_slots(NODE, now)),
        },
        "manifest": manifest,
        "slots_today": rows,
        "posted_by_node": by_node,
        "nodes": recs,
        "lock": lock_state(),
        "disk_free_gb": disk_free_gb(),
        "git": git_facts(),
    }
    snap["attention"] = attention(snap)
    return snap


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# RENDERING
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

_GLYPH = {
    "posted": ("âœ…", "posted"),
    "due": ("â³", "due"),
    "missed": ("âœ–", "MISSED"),
    "scheduled": ("Â·", "scheduled"),
}

_STATE_ICON = {
    "running": "â–¶",
    "ok": "â—",
    "failed": "âœ–",
    "success": "â—",
    "cancelled": "â¸",
}


def render(snap: dict) -> str:
    rows = snap["slots_today"]
    done = len([r for r in rows if r["state"] == "posted"])
    total = len(rows)
    manifest = snap["manifest"]

    lines = [
        "ðŸ›°  VIBECODERSCLUB COMMAND CENTRE",
        "â•" * 40,
        f"ðŸ•  {snap['now_ist']}",
        f"ðŸ–¥  this node: {snap['node'].upper()}",
        f"ðŸ“…  today: {done}/{total} slots published Â· "
        f"{snap['shorts']['pending']} short(s) waiting Â· built today {manifest['today']}",
        "",
        f"â–¸ TODAY'S SLOTS ({snap['date']})",
    ]

    for row in rows:
        glyph, label = _GLYPH.get(row["state"], ("Â·", row["state"]))
        who = row.get("by") or row["owner"]
        detail = ""
        if row["state"] == "posted":
            detail = f"{row.get('short') or '?'} Â· by {who.upper()}"
        elif row["state"] == "missed":
            detail = f"{row['overdue_min']}m overdue Â· owner {row['owner'].upper()}"
        elif row["state"] == "due":
            detail = f"due now Â· owner {row['owner'].upper()}"
        else:
            detail = f"owner {row['owner'].upper()}"
        lines.append(f"  {glyph} {row['time']}  {detail}")

    lines += ["", "â–¸ NODES"]
    nodes = snap["nodes"]
    if not nodes:
        lines.append("  no runs recorded yet")
    for name in sorted(nodes):
        rec = nodes[name]
        icon = _STATE_ICON.get(str(rec.get("state") or rec.get("last_result") or ""), "â—")
        marker = "â†’" if name == snap["node"] else " "
        lines.append(
            f" {marker}{icon} {name.upper():6} {str(rec.get('state') or rec.get('last_result') or '?'):7}"
            f" {_fmt_ist(rec.get('last_run_utc'))} ({_ago(rec.get('last_run_utc'))})"
        )
        lines.append(
            f"     runs {rec.get('runs', 0)} Â· built {rec.get('total_built', 0)}"
            f" Â· posted {rec.get('total_posted', 0)}"
            + (f" Â· {rec['last_note']}" if rec.get("last_note") else "")
        )

    lines += ["", "â–¸ SCHEDULE"]
    for owner in ("github", "local"):
        times = [s["time"] for s in slots() if str(s.get("node", "any")) in (owner, "any")]
        if times:
            lines.append(f"  {owner.upper():6} {', '.join(times)} IST")

    raw = snap["raw"]
    slices_left = int(raw.get("slices_left") or 0)
    secs = int(raw.get("short_secs") or 40)
    per_batch = max(1, int(raw.get("per_batch") or 5))
    footage_h = (raw.get("footage_min") or 0) / 60
    lines += [
        "",
        "â–¸ LIBRARY",
        f"  raw      {footage_h:.1f}h of footage in {raw['total']} video(s)",
        f"  shorts   {snap['shorts']['pending']} waiting on disk",
        f"  manifest {manifest['total']} short(s) tracked",
        "",
        f"â–¸ RAW â†’ REELS   {slices_left} reels of {secs}s left",
        f"  of {raw.get('slices_total', 0)} total Â· {raw.get('slices_used', 0)} already sliced"
        f" Â· â‰ˆ{raw.get('batches_left', 0)} batches of {per_batch}",
    ]
    if not raw.get("slices"):
        if raw.get("dir"):
            lines.append("  (no raw videos on disk â€” add footage by hand)")
        else:
            lines.append("  (raw/ lives on the PC â€” this node does not build)")
    for row in raw.get("slices", [])[:8]:
        name = row["file"]
        if len(name) > 38:
            name = name[:35] + "..."
        mins = f"{row['duration_min']:.0f}m" if row.get("duration_min") else "?m"
        flag = "âœ–" if row["clips_left"] == 0 else ("âš " if row["clips_left"] <= 2 else "Â·")
        lines.append(
            f"  {flag} {name}  {mins} Â· {row['clips_total']} reels Â· "
            f"{row['clips_used']} used Â· {row['clips_left']} left"
        )
    if raw.get("dir") and raw.get("total") and slices_left == 0:
        lines.append(f"  âš  0 reels left to slice â€” add new raw footage")

    lock = snap["lock"]
    git = snap.get("git", {})
    lines += ["", "â–¸ SYSTEM"]
    if lock.get("present"):
        lines.append(
            f"  lock  HELD Â· {lock.get('node') or '?'} pw "
            f"{lock.get('pid') or '?'} Â· {lock.get('age_min') or 0}m"
        )
    else:
        lines.append("  lock  free")
    lines.append(f"  disk  {snap['disk_free_gb']} GB free")
    if git.get("repo"):
        lines.append(
            f"  git   {git.get('branch') or '?'} @ {git.get('head') or '?'}"
            f" Â· dirty {git.get('dirty', 0)} Â· +{git.get('ahead', '?')}/-{git.get('behind', '?')}"
        )

    if snap["attention"]:
        lines += ["", "â–¸ ATTENTION"]
        lines += [f"  âš  {item}" for item in snap["attention"]]

    return "\n".join(lines)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# CLI
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def main() -> int:
    ap = argparse.ArgumentParser(description="vibecodersclub node status / command centre")
    ap.add_argument("--json", action="store_true", help="print the raw snapshot")
    ap.add_argument("--notify", action="store_true", help="send the rendered block to Telegram")
    ap.add_argument("--quiet", action="store_true", help="no stdout with --notify")
    ap.add_argument("--start", action="store_true", help="mark this node as running")
    ap.add_argument("--record", action="store_true", help="record a finished run")
    ap.add_argument("--result", default="ok", help="result for --record")
    ap.add_argument("--note", default="", help="short note for --record")
    ap.add_argument("--built", type=int, default=None)
    ap.add_argument("--posted", type=int, default=None)
    args = ap.parse_args()

    if args.start:
        record_node_run("running", note=args.note or "started", count_run=False)
    if args.record:
        record_node_run(args.result, args.note, built=args.built, posted=args.posted)

    snap = snapshot()

    if args.json:
        print(json.dumps(snap, indent=2, ensure_ascii=False))
        return 0

    text = render(snap)
    if args.notify:
        from telegram_notify import notify_status_report

        ok = notify_status_report(text)
        if not args.quiet:
            print(text)
            print(f"\n(telegram: {'sent' if ok else 'not sent'})")
        return 0

    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
