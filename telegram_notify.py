#!/usr/bin/env python3
"""telegram_notify.py â€” human alerts for the vibecodersclub pipeline.

Reliability notes
-----------------
* Every message body is plain text wrapped in ONE <pre> block. The previous
  version mixed <b>/<code> tags inside <pre> (and even closed <b> with
  </code>), which Telegram answers with HTTP 400 "can't parse entities" â€” so
  every alert silently vanished. Do not put HTML tags inside the box.
* Sends are retried (1 extra attempt, plus a pause on HTTP 429) and failures
  are printed to stdout so they land in logs/pipeline.log and Actions logs.
* A missing token/chat id is a warning, never an exception: notifications must
  never be able to kill the pipeline.
"""
import html
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

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# SENDING
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _send(text: str) -> bool:
    """Send one HTML message. Never raises."""
    if not BOT_TOKEN or not CHAT_ID:
        print("telegram: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set â€” skipping alert", flush=True)
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = json.dumps(
        {
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
    ).encode("utf-8")

    for attempt in (1, 2):
        try:
            req = urllib.request.Request(
                url, data=payload, headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                if resp.status == 200:
                    return True
                print(f"telegram: unexpected status {resp.status}", flush=True)
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", "replace")[:300]
            except Exception:
                pass
            print(f"telegram: HTTP {e.code} {body}", flush=True)
            if e.code == 429:
                time.sleep(3)
                continue
            return False
        except Exception as e:
            print(f"telegram: send failed ({e})", flush=True)
            if attempt == 1:
                time.sleep(2)
    return False


def _pre(text: str) -> bool:
    return _send(f"<pre>{html.escape(text)}</pre>")


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# FORMATTING HELPERS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _box(title: str, lines: list[str], icon: str = "â€¢") -> str:
    """Plain-text block. No markup allowed inside â€” it is escaped by _pre()."""
    rows = [f"{icon} {title}", "â”€" * 34]
    rows.extend(lines)
    return "\n".join(rows)


def _progress(done: int, total: int, width: int = 12) -> str:
    if total <= 0:
        return "â–±" * width
    filled = max(0, min(width, int(round(done / total * width))))
    return "â–ˆ" * filled + "â–±" * (width - filled)


def _pct(done: int, total: int) -> str:
    return f"{(done / total * 100):.0f}%" if total else "0%"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# PIPELINE NOTIFICATIONS
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def notify_pipeline_started(raw_status: dict, date_str: str, ist_now: str, node: str = "local") -> bool:
    total = raw_status.get("total", 0)
    consumed = raw_status.get("consumed", 0)
    remaining = raw_status.get("remaining", 0)
    lines = [
        f"node     : {node.upper()}",
        f"date     : {date_str}",
        f"time     : {ist_now} IST",
        f"raw total: {total}",
        f"consumed : {consumed}",
        f"remaining: {remaining}",
        f"progress : {_progress(consumed, max(1, total))} {_pct(consumed, max(1, total))}",
        "",
        "pipelines: build > meta > upload > push",
        "raw       : added by hand, never downloaded automatically",
    ]
    return _pre(_box("LOCAL PIPELINE STARTED", lines, "ðŸŒ…"))


def notify_raw_empty() -> bool:
    return _pre(
        _box(
            "RAW EMPTY",
            [
                "raw/ has no videos at all",
                "nothing will be downloaded automatically",
                "",
                "add footage by hand when ready",
            ],
            "ðŸ“­",
        )
    )


def notify_raw_budget(raw: dict) -> bool:
    """How many shorts of short_secs the raw footage can still produce.

    This is the plain answer to "how many reels are left in raw?": duration of
    every raw video divided by the reel length, minus the reels already sliced.
    Sent on every run so the number is always in the chat, not only when low.
    """
    left = int(raw.get("reels_left") or raw.get("slices_left") or 0)
    total = int(raw.get("reels_total") or raw.get("slices_total") or 0)
    used = int(raw.get("reels_used") or raw.get("slices_used") or 0)
    secs = int(raw.get("short_secs") or 40)
    per_batch = max(1, int(raw.get("per_batch") or 5))
    footage_h = (raw.get("footage_min") or 0) / 60

    lines = [
        f"reels left : {left} Ã— {secs}s  ({left * secs // 60}m of reels)",
        f"reels used : {used} of {total}",
        f"footage    : {footage_h:.1f}h across {raw.get('total', 0)} raw video(s)",
        f"batches    : ~{raw.get('batches_left', left // per_batch)} of {per_batch} reels",
        f"progress   : {_progress(used, max(1, total))} {_pct(used, max(1, total))}",
        "",
    ]
    for row in (raw.get("slices") or [])[:6]:
        name = str(row.get("file", "?"))
        if len(name) > 32:
            name = name[:29] + "..."
        lines.append(name)
        lines.append(
            f"  {row.get('clips_total', 0)} reels Â· {row.get('clips_used', 0)} used"
            f" Â· {row.get('clips_left', 0)} left"
        )
    if left == 0:
        lines += ["0 reels left â€” the raw footage is used up.", "add new videos to raw/ by hand."]
    else:
        lines += ["nothing is downloaded automatically â€” raw/ is manual only"]
    return _pre(_box(f"RAW â†’ REELS ({secs}s each)", lines, "ðŸŽž"))


def notify_raw_low(count: int, per_batch: int = 5, secs: int = 40) -> bool:
    return _pre(
        _box(
            "RAW RUNNING OUT",
            [
                f"only {count} reel(s) of {secs}s left to slice"
                + (f" ({count / per_batch:.1f} batches)" if per_batch else ""),
                "",
                "when they run out the pipeline stops building and alerts you",
                "nothing is downloaded automatically â€” add footage by hand",
            ],
            "ðŸ“‰",
        )
    )


def notify_raw_slices_exhausted(rows: list, secs: int = 40) -> bool:
    """Every reel has been sliced out of the raw footage: a human must add more."""
    lines = [f"0 reels of {secs}s left â€” every reel has been sliced out", ""]
    for row in (rows or [])[:6]:
        name = str(row.get("file", "?"))
        if len(name) > 34:
            name = name[:31] + "..."
        lines.append(name)
        lines.append(
            f"  {row.get('clips_total', 0)} reels Â· {row.get('clips_used', 0)} sliced Â· 0 left"
        )
    lines += [
        "",
        "shorts already built keep posting on schedule.",
        "add new raw videos to raw/ when you are ready.",
        "nothing is downloaded automatically.",
    ]
    return _pre(_box("RAW EXHAUSTED â€” ADD FOOTAGE", lines, "ðŸª«"))


def notify_short_built(short_name: str, index: int, total: int, title: str = "") -> bool:
    lines = [f"short    : {short_name}", f"sequence : {index} / {total}"]
    if title:
        lines.append(f"title    : {title}")
    return _pre(_box("SHORT BUILT", lines, "ðŸŽ¬"))


def notify_meta_generated(count: int, skipped: int) -> bool:
    return _pre(
        _box(
            "META GENERATED",
            [f"new     : {count} short(s)", f"skipped : {skipped} short(s)", "", "powered by OpenRouter"],
            "âœï¸",
        )
    )


def notify_upload_started(total_due: int, date_str: str, ist_now: str, node: str = "local") -> bool:
    return _pre(
        _box(
            "UPLOAD STARTED",
            [
                f"node : {node.upper()}",
                f"date : {date_str}",
                f"time : {ist_now} IST",
                f"due  : {total_due} short(s)",
            ],
            "ðŸ“¤",
        )
    )


def notify_short_uploaded(short_name: str, slot_time: str, slot_label: str, media_pk: str) -> bool:
    return _pre(
        _box(
            "SHORT UPLOADED",
            [
                f"short : {short_name}",
                f"slot  : {slot_time} IST",
                f"label : {slot_label}",
                f"pk    : {media_pk}",
                "",
                "published to Instagram",
            ],
            "âœ…",
        )
    )


def notify_upload_complete(posted: int, remaining: int, total: int) -> bool:
    return _pre(
        _box(
            "UPLOAD COMPLETE",
            [
                f"posted this run : {posted}",
                f"still pending   : {remaining}",
                f"total shorts    : {total}",
                f"progress        : {_progress(posted, max(1, total))} {_pct(posted, max(1, total))}",
            ],
            "ðŸ“‹",
        )
    )


def notify_raw_fully_processed(shorts_count: int, daily_limit: int) -> bool:
    return _pre(
        _box(
            "RAW FULLY PROCESSED",
            [
                f"shorts built : {shorts_count}",
                f"daily limit  : {daily_limit}",
                "raw clips    : all consumed",
            ],
            "ðŸ",
        )
    )


def notify_upload_failed(short_name: str, error: str) -> bool:
    return _pre(
        _box(
            "UPLOAD FAILED",
            [
                f"short : {short_name}",
                f"error : {str(error)[:300]}",
                "",
                "progress saved in state/uploads.json",
                "retried on the next scheduled run",
            ],
            "ðŸš¨",
        )
    )


def notify_download_failed() -> bool:
    return _pre(
        _box(
            "DOWNLOAD FAILED",
            ["download from channels failed", "check logs/pipeline.log", "retried on the next run"],
            "â›”",
        )
    )


def notify_build_failed() -> bool:
    return _pre(
        _box(
            "BUILD FAILED",
            ["ffmpeg / build step failed", "check logs/pipeline.log", "retried on the next run"],
            "â›”",
        )
    )


def notify_meta_failed() -> bool:
    return _pre(
        _box(
            "META FAILED",
            ["OpenRouter metadata failed", "shorts built with fallback captions", "retried on the next run"],
            "âš ï¸",
        )
    )


def notify_fatal(message: str) -> bool:
    return _pre(
        _box(
            "FATAL ERROR",
            [str(message)[:500], "", "pipeline stopped immediately", "lock released, next run will retry"],
            "ðŸ’€",
        )
    )


def notify_run_summary(summary: str) -> bool:
    """Free-form end-of-run summary (already plain text)."""
    return _pre(summary)


def notify_status_report(text: str) -> bool:
    """Command-centre status snapshot (already plain text)."""
    return _pre(text)


def notify_night_upload_complete(posted: int, remaining: int, total: int) -> bool:
    return _pre(
        _box(
            "NIGHT UPLOAD COMPLETE",
            [
                f"posted this run : {posted}",
                f"remaining       : {remaining}",
                f"total shorts    : {total}",
                f"progress        : {_progress(posted, max(1, total))} {_pct(posted, max(1, total))}",
            ],
            "ðŸŒ™",
        )
    )


def notify_night_upload_failed(short_name: str, error: str) -> bool:
    return _pre(
        _box(
            "NIGHT UPLOAD FAILED",
            [f"short : {short_name}", f"error : {str(error)[:300]}", "", "progress saved â€” retried next night"],
            "ðŸ’¥",
        )
    )
