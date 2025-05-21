import pandas as pd
import os
from dotenv import load_dotenv
from mysql.connector import connect
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import threading

# 1) LOAD ENVIRONMENT VARIABLES
load_dotenv()

# 2) CONFIG
INPUT_CSV = Path("daily_dump.csv")
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "127.0.0.1"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),      # empty string if no password
    "database": os.getenv("DB_NAME", "pipeline")
}
CHUNK_SIZE  = 200       # ~1,000 records/day → 5 chunks
MAX_WORKERS = 4

# 3) INITIALIZE DATABASE TABLES AND CLEAR OLD DATA
setup_conn   = connect(**DB_CONFIG, autocommit=True)
setup_cursor = setup_conn.cursor()

# Create if not exists…
setup_cursor.execute("""
  CREATE TABLE IF NOT EXISTS clean_data (
    id INT PRIMARY KEY,
    value DOUBLE,
    timestamp DATETIME
  );
""")
setup_cursor.execute("""
  CREATE TABLE IF NOT EXISTS bad_data (
    id INT,
    value VARCHAR(255),
    timestamp VARCHAR(255),
    error VARCHAR(255)
  );
""")

# …then wipe out any leftovers from prior runs
setup_cursor.execute("TRUNCATE TABLE clean_data;")
setup_cursor.execute("TRUNCATE TABLE bad_data;")

setup_cursor.close()
setup_conn.close()

# 4) THREAD-SAFE CACHE FOR IDs
seen_ids = set()
seen_lock = threading.Lock()

def process_chunk(df_chunk: pd.DataFrame):
    """Each thread creates its own connection & cursor."""
    conn   = connect(**DB_CONFIG, autocommit=True)
    cursor = conn.cursor()

    clean_rows = []
    bad_rows   = []

    for _, row in df_chunk.iterrows():
        try:
            rid = int(row["id"])
            val = float(row["value"])
            ts  = pd.to_datetime(row["timestamp"])

            # check+add must be atomic
            with seen_lock:
                if rid in seen_ids:
                    raise ValueError("duplicate id")
                seen_ids.add(rid)

            if val < 0:
                raise ValueError("negative value")

            clean_rows.append((rid, val, ts.to_pydatetime()))
        except Exception as e:
            bad_rows.append((
                row.get("id"),
                str(row.get("value")),
                row.get("timestamp"),
                str(e)
            ))

    if clean_rows:
        cursor.executemany(
            # Use INSERT IGNORE instead if you want to silently skip dupes
            "INSERT INTO clean_data (id, value, timestamp) VALUES (%s, %s, %s)",
            clean_rows
        )
    if bad_rows:
        cursor.executemany(
            "INSERT INTO bad_data (id, value, timestamp, error) VALUES (%s, %s, %s, %s)",
            bad_rows
        )

    cursor.close()
    conn.close()
    return len(clean_rows), len(bad_rows)

def run_pipeline():
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            pool.submit(process_chunk, chunk)
            for chunk in pd.read_csv(INPUT_CSV, chunksize=CHUNK_SIZE)
        ]

        total_good = total_bad = 0
        for f in as_completed(futures):
            good, bad = f.result()
            total_good += good
            total_bad  += bad

    print(f"✅ Done! Good rows: {total_good}, Bad rows: {total_bad}")

if __name__ == "__main__":
    run_pipeline()