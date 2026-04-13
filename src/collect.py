"""
collect.py — Async Bluesky firehose collector.

Streams live posts from the Bluesky Jetstream websocket, filters by
configurable keywords and English language, and stores matches in either:
  • A local JSON file  (STORAGE_BACKEND=json, default)
  • MongoDB / Atlas    (STORAGE_BACKEND=mongodb)

Credentials are read from a .env file — copy .env.example to .env and fill
in the values before running.

Usage
-----
    python src/collect.py [--keywords "ai art" llm chatgpt] [--limit 500]

The --limit flag stops collection after N posts (omit for continuous mode).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import certifi
import websockets
from dotenv import load_dotenv

from misra_gries import MisraGries

load_dotenv()

# ── Defaults (overridden by .env) ─────────────────────────────────────────────
JETSTREAM_URI: str = os.getenv(
    "JETSTREAM_URI",
    "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
)
STORAGE_BACKEND: str = os.getenv("STORAGE_BACKEND", "json").lower()
JSON_PATH: Path = Path(os.getenv("JSON_PATH", "data/posts.json"))

MONGO_URI: str = os.getenv("MONGO_URI", "")
CERT_FILE_PATH: str = os.getenv("CERT_FILE_PATH", "")

DEFAULT_KEYWORDS: list[str] = [
    "generative ai",
    "llm",
    "stable diffusion",
    "midjourney",
    "chatgpt",
    "ai art",
    "creative workflow",
    "digital art",
    "automation",
    "artist rights",
    "decentralized",
    "data ethics",
    "job displacement",
    "ai ethics",
]

# Misra-Gries tracker: top-N keyword candidates (configurable via MG_K env var).
_mg = MisraGries(k=int(os.getenv("MG_K", "50")))


# ── Storage helpers ───────────────────────────────────────────────────────────

def _load_json_store(path: Path) -> list[dict]:
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    return []


def _save_json_store(path: Path, posts: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(posts, fh, indent=2, ensure_ascii=False)


async def _get_mongo_collection():
    """Return (client, collection) for the configured Atlas cluster."""
    try:
        from motor.motor_asyncio import AsyncIOMotorClient
    except ImportError as exc:
        raise RuntimeError(
            "motor is not installed. Run: pip install motor"
        ) from exc

    if not MONGO_URI:
        raise ValueError(
            "MONGO_URI is not set. Add it to your .env file."
        )

    kwargs: dict = {"tls": True, "tlsCAFile": certifi.where()}
    if CERT_FILE_PATH:
        kwargs["tlsCertificateKeyFile"] = CERT_FILE_PATH

    client = AsyncIOMotorClient(MONGO_URI, **kwargs)
    db = client["bluesky_research"]
    return client, db["posts"]


# ── Core collection loop ──────────────────────────────────────────────────────

async def collect(keywords: list[str], limit: int | None = None) -> None:
    """Connect to the Jetstream websocket and collect matching posts."""

    keywords_lower = [kw.lower() for kw in keywords]
    collected = 0

    # --- JSON backend setup ---
    json_posts: list[dict] = []
    if STORAGE_BACKEND == "json":
        json_posts = _load_json_store(JSON_PATH)
        print(f"[json] Loaded {len(json_posts)} existing posts from {JSON_PATH}")

    # --- MongoDB backend setup ---
    mongo_client = None
    mongo_col = None
    if STORAGE_BACKEND == "mongodb":
        print("[mongodb] Connecting to MongoDB …")
        mongo_client, mongo_col = await _get_mongo_collection()
        print("[mongodb] Connected.")

    print(f"Watching for keywords: {keywords_lower}")
    print(f"Storage backend: {STORAGE_BACKEND}")
    print("Connecting to Jetstream …")

    try:
        while True:
            try:
                async with websockets.connect(JETSTREAM_URI) as ws:
                    print("Connection established. Monitoring firehose …")

                    async for raw in ws:
                        data = json.loads(raw)
                        record = data.get("commit", {}).get("record", {})
                        post_text: str = record.get("text", "").lower()
                        post_langs: list[str] = record.get("langs", [])

                        is_english = "en" in post_langs
                        matched_kw = next(
                            (kw for kw in keywords_lower if kw in post_text), None
                        )

                        if matched_kw and is_english:
                            _mg.add(matched_kw)

                            post: dict = {
                                "text": record.get("text", ""),
                                "langs": post_langs,
                                "matched_keyword": matched_kw,
                                "collected_at": datetime.now(timezone.utc).isoformat(),
                                "raw": data,
                            }

                            if STORAGE_BACKEND == "json":
                                json_posts.append(post)
                                _save_json_store(JSON_PATH, json_posts)
                            else:
                                await mongo_col.insert_one(post)  # type: ignore[union-attr]

                            collected += 1
                            print(
                                f"[{collected}] ({matched_kw}) "
                                f"{record.get('text', '')[:70]} …"
                            )

                            if limit is not None and collected >= limit:
                                print(f"Reached limit of {limit} posts. Stopping.")
                                return

            except websockets.exceptions.ConnectionClosed as exc:
                print(f"Connection closed ({exc}). Retrying in 10 s …")
                await asyncio.sleep(10)
            except OSError as exc:
                print(f"Network error: {exc}. Retrying in 10 s …")
                await asyncio.sleep(10)
    finally:
        if mongo_client is not None:
            mongo_client.close()


# ── CLI entry point ───────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream Bluesky posts matching keywords."
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        default=DEFAULT_KEYWORDS,
        help="Keywords to filter posts (space-separated, quote multi-word terms).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after collecting this many posts (default: run indefinitely).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(collect(keywords=args.keywords, limit=args.limit))
    except KeyboardInterrupt:
        print("\nStopping data collection …")
