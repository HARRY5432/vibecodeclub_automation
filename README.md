# Vibe Coders Club - Automated Shorts Pipeline

Automated Instagram Reels pipeline for the Vibe Coders Club content project. Mirrors the omfplays pipeline:

```
downloads/        <- Instagram reels from creators.txt (gallery-dl / instaloader / yt-dlp)
   |  python build_shorts_omf.py    # slice reels into shorts
   v
shorts/           s{YYYYMMDD}_{NNN}.mp4 + .txt sidecar (OpenRouter captions)
   |  python upload_shorts.py       # post to Instagram per slot ownership
   v
GitHub            <- push shorts + state; GitHub Actions uploads night slots
```

## Nodes & slots

See `slots.json`. Two nodes own slots:
- `local` - this Windows PC (runs `run_local_pipeline.py` via Task Scheduler / at logon)
- `github` - the GitHub Actions night workflow (`.github/workflows/night-upload.yml`)

Each slot names its `owner` node, so both nodes can run without double-posting.

## Run the local pipeline

```bash
python run_local_pipeline.py            # build -> meta -> upload -> cleanup -> push
python run_local_pipeline.py --status   # command-centre snapshot
python run_local_pipeline.py --dry-run  # report only, change nothing
```

`run_local.bat` wraps it for unattended (Task Scheduler / at logon) use.

## Config

`config.json` controls raw source (`downloads/`), output (`shorts/`), reel length, bitrate cap
(keeps shorts under GitHub's ~100 MB so they can be pushed), Telegram/OpenRouter credentials, etc.

Copy `.env.example` to `.env` and fill in tokens. Secrets are gitignored.

## State

Committed shared ledgers: `state/uploads.json` (posted slots), `state/nodes.json` (per-node health),
`state/meta_counter.json` (Day/Reel counter), `state/downloads.json`, `state/download_archive.txt`.

`shorts/` manifests and media are committed so the night node can publish them; `downloads/` (raw
reels) and `raw/` stay local.

## Secrets

Never commit `publisher_*.json`, `.env`, `cookies*.txt`, `client_secrets.json`, `token.json`.
