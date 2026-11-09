#!/usr/bin/env python3
"""generate_meta.py - caption fallback (original captions preferred).

Original captions come from download_reels.py (saved as .txt sidecars, with "@"
stripped). This script is a safety net: if a .txt sidecar is missing, it
generates a fallback caption via OpenRouter, also stripping "@".

Idempotent: an existing non-empty .txt sidecar is left alone.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

HERE = Path(__file__).resolve().parent
SHORTS_DIR = HERE / "shorts"
STATE_DIR = HERE / "state"
COUNTER_PATH = STATE_DIR / "meta_counter.json"
CONFIG_PATH = HERE / "config.json"

try:
    _CFG = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
except Exception:
    _CFG = {}

PER_BATCH = max(1, int(_CFG.get("daily_shorts", 5) or 5))
BATCH_HOURS = float(_CFG.get("batch_hours", 25) or 25)
BATCH_SECONDS = BATCH_HOURS * 3600

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openrouter/fusion")

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


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


def _parse_utc(value):
    try:
        dt = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def allocate(state):
    now = datetime.now(timezone.utc)
    state = dict(state or {})
    day = max(1, int(state.get("next_day", 1) or 1))
    reel = int(state.get("next_reel", 0) or 0)
    started = _parse_utc(state.get("batch_started_utc"))

    if reel < 1 or started is None:
        reel, started = 1, now
    elif (now - started).total_seconds() > BATCH_SECONDS:
        day += 1
        reel, started = 1, now

    used_day, used_reel = day, reel

    if reel >= PER_BATCH:
        state.update(next_day=day + 1, next_reel=1,
                     batch_started_utc=now.isoformat(timespec="seconds"))
    else:
        state.update(next_day=day, next_reel=reel + 1,
                     batch_started_utc=started.isoformat(timespec="seconds"))
    state["updated_utc"] = now.isoformat(timespec="seconds")
    return used_day, used_reel, state


def call_openrouter(prompt):
    if not OPENROUTER_API_KEY:
        print("  OPENROUTER_API_KEY not set - using fallback caption", flush=True)
        return None

    payload = json.dumps(
        {
            "model": OPENROUTER_MODEL,
            "messages": [
                {"role": "system", "content": "You are a premium short-form content writer."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.8,
            "max_tokens": 300,
        }
    ).encode("utf-8")

    for attempt in (1, 2):
        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=payload,
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            content = data["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            parsed = json.loads(content)
            if parsed.get("title") and parsed.get("description"):
                return parsed
            raise ValueError("response missing title/description")
        except Exception as e:
            print(f"  OpenRouter attempt {attempt} failed: {e}", flush=True)
            if attempt == 1:
                time.sleep(3)
    return None


def fallback_meta(day, reel):
    return {
        "title": f"Day {day} Reel {reel} of Vibe Coders Club - Highlight",
        "description": (
            f"Day {day}, reel {reel} of {PER_BATCH} - Vibe Coders Club content. "
            "Pure content, zero filler.\n\n#VibeCodersClub #Shorts"
        ),
    }


def main() -> int:
    args = sys.argv[1:]
    force = "--force" in args
    limit = None
    if "--limit" in args:
        try:
            limit = max(1, int(args[args.index("--limit") + 1]))
        except Exception:
            print("ERROR: --limit requires a positive integer", flush=True)
            return 2

    if not SHORTS_DIR.exists():
        print("No shorts/ folder found.", flush=True)
        return 0

    shorts = sorted(SHORTS_DIR.glob("s*.mp4"))
    if not shorts:
        print("No shorts found to generate captions for.", flush=True)
        return 0

    created = skipped = 0
    for short_path in shorts:
        txt_path = short_path.with_suffix(".txt")
        if txt_path.exists() and not force:
            try:
                if txt_path.stat().st_size > 0:
                    skipped += 1
                    continue
            except OSError:
                pass

        day, reel, state = allocate(load_json(COUNTER_PATH, {}))
        result = call_openrouter(
            f"Write a short caption for {short_path.name} (Day {day} Reel {reel} of Vibe Coders Club)"
        ) or fallback_meta(day, reel)

        title = str(result.get("title") or f"Day {day} Reel {reel} of Vibe Coders Club").replace("@", "").strip()
        description = str(result.get("description") or "").replace("@", "").strip()
        content = f"{title}\n\n{description}".strip()

        txt_path.write_text(content + "\n", encoding="utf-8")
        atomic_write_json(COUNTER_PATH, state)
        created += 1
        print(f"  {txt_path.name} -> Day {day} Reel {reel}: {title[:48]}", flush=True)

        if limit is not None and created >= limit:
            break

    print(f"\nCaptions: {created} new, {skipped} already present.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
