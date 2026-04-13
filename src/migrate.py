"""
migrate.py — Migrate posts from a local MongoDB instance to Atlas.

Reads credentials from the .env file. Copy .env.example → .env and fill in:
  LOCAL_MONGO_URI  — URI for the local MongoDB (default: mongodb://localhost:27017)
  CLUSTER_URI      — Atlas connection string
  CERT_FILE_PATH   — Path to the X.509 client certificate/key file (optional)

Usage
-----
    python src/migrate.py
"""

from __future__ import annotations

import os

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

load_dotenv()

LOCAL_URI: str = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017")
CLUSTER_URI: str = os.getenv("CLUSTER_URI", "")
CERT_FILE_PATH: str = os.getenv("CERT_FILE_PATH", "")

DB_NAME = "bluesky_research"
COLLECTION_NAME = "posts"


def migrate_to_cluster() -> None:
    """Copy all documents from the local MongoDB to the Atlas cluster."""

    if not CLUSTER_URI:
        raise ValueError(
            "CLUSTER_URI is not set. Add it to your .env file."
        )

    print(f"Connecting to local MongoDB at {LOCAL_URI} …")
    local_client = MongoClient(LOCAL_URI)
    local_col = local_client[DB_NAME][COLLECTION_NAME]

    print("Connecting to Atlas cluster …")
    atlas_kwargs: dict = {"tls": True, "tlsCAFile": certifi.where()}
    if CERT_FILE_PATH:
        atlas_kwargs["tlsCertificateKeyFile"] = CERT_FILE_PATH

    atlas_client = MongoClient(CLUSTER_URI, **atlas_kwargs)
    atlas_col = atlas_client[DB_NAME][COLLECTION_NAME]

    print("Fetching documents from local store …")
    documents = list(local_col.find({}))

    if not documents:
        print("No documents found in local database — nothing to migrate.")
        local_client.close()
        atlas_client.close()
        return

    print(f"Found {len(documents)} documents. Pushing to Atlas (skipping duplicates) …")

    try:
        result = atlas_col.insert_many(documents, ordered=False)
        print(f"Success! Inserted {result.inserted_count} new documents.")
    except BulkWriteError as bwe:
        inserted_count: int = bwe.details["nInserted"]
        skipped = len(documents) - inserted_count
        print(
            f"Partial success: {inserted_count} new documents inserted, "
            f"{skipped} duplicates skipped."
        )
    finally:
        local_client.close()
        atlas_client.close()


if __name__ == "__main__":
    migrate_to_cluster()
