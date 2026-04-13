"""
export.py — Export collected posts to CSV or JSON.

Works with both storage backends:
  • json   — reads data/posts.json (or JSON_PATH from .env)
  • mongodb — queries the Atlas / local collection

Usage
-----
    # Export from JSON store to CSV
    python src/export.py --backend json --format csv --out data/export.csv

    # Export from MongoDB to JSON (last 1000 posts)
    python src/export.py --backend mongodb --format json --limit 1000 --out data/export.json

    # Delete posts older than 7 days from the JSON store, keeping top-k terms
    python src/export.py --backend json --prune --days 7 --top-k 20
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from misra_gries import MisraGries

load_dotenv()

STORAGE_BACKEND: str = os.getenv("STORAGE_BACKEND", "json").lower()
JSON_PATH: Path = Path(os.getenv("JSON_PATH", "data/posts.json"))
MONGO_URI: str = os.getenv("MONGO_URI", "")
CERT_FILE_PATH: str = os.getenv("CERT_FILE_PATH", "")


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"JSON store not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_mongo(limit: int | None) -> list[dict]:
    import certifi
    from pymongo import MongoClient

    if not MONGO_URI:
        raise ValueError("MONGO_URI is not set. Add it to your .env file.")

    kwargs: dict = {"tls": True, "tlsCAFile": certifi.where()}
    if CERT_FILE_PATH:
        kwargs["tlsCertificateKeyFile"] = CERT_FILE_PATH

    client = MongoClient(MONGO_URI, **kwargs)
    col = client["bluesky_research"]["posts"]
    cursor = col.find({}, {"_id": 0})
    if limit:
        cursor = cursor.limit(limit)
    posts = list(cursor)
    client.close()
    return posts


# ── Export ────────────────────────────────────────────────────────────────────

def export_posts(
    backend: str,
    fmt: str,
    out: Path,
    limit: int | None = None,
) -> None:
    """Load posts and write them to *out* in the requested format."""
    posts = _load_json(JSON_PATH) if backend == "json" else _load_mongo(limit)

    if limit and backend == "json":
        posts = posts[-limit:]  # take the most recent N

    df = pd.DataFrame(posts)

    # Drop the nested 'raw' column for cleaner exports
    if "raw" in df.columns:
        df = df.drop(columns=["raw"])

    out.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        df.to_csv(out, index=False)
    else:
        df.to_json(out, orient="records", indent=2, force_ascii=False)

    print(f"Exported {len(df)} posts → {out}")


# ── Pruning ───────────────────────────────────────────────────────────────────

def prune_json_store(days: int, top_k: int) -> None:
    """Remove posts older than *days* days whose keyword is not a top-k term.

    The Misra-Gries algorithm identifies the most frequent keywords in the
    current store; posts for evicted (low-frequency) keywords are also removed.
    """
    posts = _load_json(JSON_PATH)
    if not posts:
        print("No posts to prune.")
        return

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # Build Misra-Gries summary over matched keywords
    mg = MisraGries(k=top_k)
    for post in posts:
        kw = post.get("matched_keyword", "")
        if kw:
            mg.add(kw)

    top_keywords = set(mg.heavy_hitters().keys())
    print(f"Top-{top_k} heavy hitters: {top_keywords}")

    kept: list[dict] = []
    removed = 0
    for post in posts:
        collected_at_raw = post.get("collected_at", "")
        try:
            collected_at = datetime.fromisoformat(collected_at_raw)
            if collected_at.tzinfo is None:
                collected_at = collected_at.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            kept.append(post)
            continue

        kw = post.get("matched_keyword", "")
        if collected_at < cutoff and kw not in top_keywords:
            removed += 1
        else:
            kept.append(post)

    with JSON_PATH.open("w", encoding="utf-8") as fh:
        json.dump(kept, fh, indent=2, ensure_ascii=False)

    print(f"Pruned {removed} posts. {len(kept)} posts remain in {JSON_PATH}.")


# ── CLI entry point ───────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export or prune collected posts.")
    parser.add_argument(
        "--backend",
        choices=["json", "mongodb"],
        default=STORAGE_BACKEND,
        help="Storage backend to read from.",
    )
    parser.add_argument(
        "--format",
        dest="fmt",
        choices=["csv", "json"],
        default="csv",
        help="Output format for export.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/export.csv"),
        help="Output file path.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of posts to export.",
    )
    parser.add_argument(
        "--prune",
        action="store_true",
        help="Prune old posts using the Misra-Gries algorithm instead of exporting.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Remove posts older than this many days (used with --prune).",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=20,
        dest="top_k",
        help="Number of heavy-hitter keywords to retain (used with --prune).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.prune:
        if args.backend != "json":
            print("Error: --prune is only supported for the JSON backend.")
            raise SystemExit(1)
        prune_json_store(days=args.days, top_k=args.top_k)
    else:
        export_posts(
            backend=args.backend,
            fmt=args.fmt,
            out=args.out,
            limit=args.limit,
        )
