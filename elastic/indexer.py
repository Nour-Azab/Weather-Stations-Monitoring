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
BULK_SIZE     = 500

# ── ES index mapping ──────────────────────────────────────────────────────────
INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "station_id":        {"type": "long"},
            "s_no":              {"type": "long"},
            "battery_status":    {"type": "keyword"},
            "status_timestamp":  {"type": "date", "format": "epoch_second"},
            "humidity":          {"type": "integer"},
            "temperature":       {"type": "integer"},
            "wind_speed":        {"type": "integer"},
            "indexed_at":        {"type": "date"},
        }
    },
    "settings": {
        "number_of_shards":   1,
        "number_of_replicas": 0,
    }
}

def wait_for_es(es: Elasticsearch, retries: int = 20, delay: int = 5):
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
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX, body=INDEX_MAPPING)
        print(f"[INDEXER] Created index '{ES_INDEX}'.")
    else:
        print(f"[INDEXER] Index '{ES_INDEX}' already exists.")

def read_parquet(path: str) -> list[dict]:
    conn = duckdb.connect(database=":memory:")
    try:
        rows = conn.execute(f"SELECT * FROM read_parquet('{path}')").fetchall()
        cols = [desc[0] for desc in conn.description]
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()

def rows_to_actions(rows: list[dict], source_file: str):
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
    print(f"[INDEXER] Indexing {path} ...")
    rows = read_parquet(path)
    if not rows:
        print(f"[INDEXER] {path} is empty, skipping.")
        return 0

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
    # Get relative path layout from original directory route
    rel_path = os.path.relpath(path, ARCHIVE_DIR)
    dest = os.path.join(INDEXED_DIR, rel_path)
    
    # Generate needed child structures inside indexed/
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    shutil.move(path, dest)
    print(f"[INDEXER] Moved to {dest}")

def scan_and_index(es: Elasticsearch):
    """Find all .parquet files recursively across the partition subfolders."""
    files = []
    
    for root, dirs, filenames in os.walk(ARCHIVE_DIR):
        # Skip checking the destination indexed backup folder entirely
        if "indexed" in root:
            continue
        for f in filenames:
            if f.endswith(".parquet"):
                files.append(os.path.join(root, f))
                
    if not files:
        print("[INDEXER] No new partitioned parquet files found.")
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

    print(f"[INDEXER] Watching '{ARCHIVE_DIR}' recursively every {POLL_INTERVAL}s ...")
    while True:
        scan_and_index(es)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()