"""
parquet-indexer.py
──────────────────
Watches ARCHIVE_DIR for new .parquet files written by ParquetArchiverWorker.
For every new file it:
  1. Reads all rows with DuckDB (fast columnar scan)
  2. Bulk-indexes them into Elasticsearch
  3. Moves the file to ARCHIVE_DIR/indexed/ so it is never re-processed

Environment variables (all have defaults):
  ES_HOST        Elasticsearch base URL   (default: http://elasticsearch:9200)
  ES_INDEX       Index name               (default: weather-readings)
  ARCHIVE_DIR    Parquet directory        (default: /archive)
  POLL_INTERVAL  Seconds between scans   (default: 30)
"""

import os
import time
import shutil
import duckdb
from datetime import datetime, timezone
from elasticsearch import Elasticsearch, helpers

# ── Config ────────────────────────────────────────────────────────────────────
ES_HOST       = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX      = os.getenv("ES_INDEX", "weather-readings")
ARCHIVE_DIR   = os.getenv("ARCHIVE_DIR", "/archive")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
INDEXED_DIR   = os.path.join(ARCHIVE_DIR, "indexed")
BULK_SIZE     = 500   # rows per bulk request to ES

# ── ES index mapping ──────────────────────────────────────────────────────────
INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "station_id":        {"type": "long"},
            "s_no":              {"type": "long"},
            "battery_status":    {"type": "keyword"},   # keyword → exact match + aggregations
            "status_timestamp":  {"type": "date", "format": "epoch_second"},
            "humidity":          {"type": "integer"},
            "temperature":       {"type": "integer"},
            "wind_speed":        {"type": "integer"},
            "indexed_at":        {"type": "date"},
        }
    },
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,   # single-node, no replicas needed
    }
}

def wait_for_es(es: Elasticsearch, retries: int = 20, delay: int = 5):
    """Block until ES is reachable."""
    for i in range(retries):
        try:
            if es.ping():
                print("[INDEXER] Elasticsearch is up.")
                return
        except Exception:
            pass
        print(f"[INDEXER] Waiting for Elasticsearch... ({i+1}/{retries})")
        time.sleep(delay)
    raise RuntimeError("Elasticsearch did not become available in time.")


def ensure_index(es: Elasticsearch):
    """Create the index with mapping if it doesn't exist yet."""
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX, body=INDEX_MAPPING)
        print(f"[INDEXER] Created index '{ES_INDEX}'.")
    else:
        print(f"[INDEXER] Index '{ES_INDEX}' already exists.")


def read_parquet(path: str) -> list[dict]:
    """Use DuckDB to read a parquet file and return rows as dicts."""
    conn = duckdb.connect(database=":memory:")
    try:
        rows = conn.execute(f"SELECT * FROM read_parquet('{path}')").fetchall()
        cols = [desc[0] for desc in conn.description]
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def rows_to_actions(rows: list[dict], source_file: str):
    """Convert row dicts to ES bulk action dicts."""
    indexed_at = datetime.now(timezone.utc).isoformat()
    for row in rows:
        yield {
            "_index": ES_INDEX,
            "_source": {
                "station_id":       row.get("station_id"),
                "s_no":             row.get("s_no"),
                "battery_status":   row.get("battery_status"),
                "status_timestamp": row.get("status_timestamp"),
                "humidity":         row.get("humidity"),
                "temperature":      row.get("temperature"),
                "wind_speed":       row.get("wind_speed"),
                "indexed_at":       indexed_at,
                "source_file":      os.path.basename(source_file),
            }
        }


def index_file(es: Elasticsearch, path: str):
    """Read one parquet file and bulk-index it into ES."""
    print(f"[INDEXER] Indexing {path} ...")
    rows = read_parquet(path)
    if not rows:
        print(f"[INDEXER] {path} is empty, skipping.")
        return 0

    # helpers.bulk handles chunking automatically; chunk_size controls batch size
    success, errors = helpers.bulk(
        es,
        rows_to_actions(rows, path),
        chunk_size=BULK_SIZE,
        raise_on_error=False,
    )

    if errors:
        print(f"[INDEXER] {len(errors)} errors while indexing {path}: {errors[:3]}")
    print(f"[INDEXER] Indexed {success} rows from {os.path.basename(path)}.")
    return success


def move_to_indexed(path: str):
    os.makedirs(INDEXED_DIR, exist_ok=True)
    dest = os.path.join(INDEXED_DIR, os.path.basename(path))
    shutil.move(path, dest)
    print(f"[INDEXER] Moved to {dest}")


def scan_and_index(es: Elasticsearch):
    """Find all .parquet files in ARCHIVE_DIR (not in indexed/) and process them."""
    files = [
        os.path.join(ARCHIVE_DIR, f)
        for f in os.listdir(ARCHIVE_DIR)
        if f.endswith(".parquet") and os.path.isfile(os.path.join(ARCHIVE_DIR, f))
    ]
    if not files:
        print("[INDEXER] No new parquet files found.")
        return

    for path in files:
        try:
            index_file(es, path)
            move_to_indexed(path)
        except Exception as e:
            print(f"[INDEXER] Failed to index {path}: {e}")


def main():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    os.makedirs(INDEXED_DIR, exist_ok=True)

    es = Elasticsearch(ES_HOST)
    wait_for_es(es)
    ensure_index(es)

    print(f"[INDEXER] Watching '{ARCHIVE_DIR}' every {POLL_INTERVAL}s ...")
    while True:
        scan_and_index(es)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()