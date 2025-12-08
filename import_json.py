import os
import json
import logging
from datetime import datetime
from pathlib import Path

import psycopg2

# ===== DB CONFIG =====
DB_CONFIG = {
    "dbname": "leakatlas",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": 5433,
}

# ===== FILE CONFIG =====
# JSON files are in: one level up -> data/database
BASE_DIR = Path(__file__).resolve().parent  # leaked-data-analysis
JSON_DIR = BASE_DIR.parent / "data" / "database"

# Starting file number: 0414700.json => 7 digits
START_NUMBER = 414700  # change if needed
LOG_FILE = BASE_DIR / "import_errors.log"

# ===== LOGGING =====
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def file_already_processed(conn, filename: str) -> bool:
    """Check if this file has been processed before (success or fail)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM processed_files WHERE filename = %s LIMIT 1;",
            (filename,),
        )
        return cur.fetchone() is not None


def mark_file_processed(conn, filename: str, success: bool, error_msg: str | None):
    """Insert or update processed_files entry for this file."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO processed_files (filename, success, error_msg, processed_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (filename) DO UPDATE
            SET success = EXCLUDED.success,
                error_msg = EXCLUDED.error_msg,
                processed_at = EXCLUDED.processed_at;
            """,
            (filename, success, error_msg),
        )
        conn.commit()


def parse_timestamp(ts_raw: str | None):
    if not ts_raw:
        return None
    try:
        # your sample is "2020-03-01"
        return datetime.strptime(ts_raw, "%Y-%m-%d").date()
    except Exception:
        return None


def process_one_file(conn, filepath: Path) -> int:
    """
    Process a single JSON file:
      - insert into records
      - insert into passwords, domain_names, services, usernames, email_addresses
    Returns 1 if the main record was inserted, 0 otherwise.
    """
    filename = filepath.name

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    record_id = int(data["id"])
    device_ip_addr = data.get("device_ip_addr")
    ts = parse_timestamp(data.get("timestamp"))
    country = data.get("country")
    keyboard = data.get("keyboard")

    passwords = data.get("passwords", []) or []
    domain_names = data.get("domain_names", []) or []
    services = data.get("services", []) or []
    usernames = data.get("usernames", []) or []
    email_addresses = data.get("email_addresses", []) or []

    # Use one transaction for this file's inserts
    cur = conn.cursor()
    try:
        # Insert into records
        cur.execute(
            """
            INSERT INTO records (id, device_ip_addr, timestamp, country, keyboard)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (record_id, device_ip_addr, ts, country, keyboard),
        )

        # Check if record is new or already existed
        inserted_main = cur.rowcount  # 1 if inserted, 0 if already existed

        # Insert related rows (you may want UNIQUE + ON CONFLICT in those tables later)
        for pw in passwords:
            cur.execute(
                "INSERT INTO passwords (record_id, password) VALUES (%s, %s);",
                (record_id, pw),
            )

        for dom in domain_names:
            cur.execute(
                "INSERT INTO domain_names (record_id, domain) VALUES (%s, %s);",
                (record_id, dom),
            )

        for svc in services:
            cur.execute(
                "INSERT INTO services (record_id, service_url) VALUES (%s, %s);",
                (record_id, svc),
            )

        for uname in usernames:
            cur.execute(
                "INSERT INTO usernames (record_id, username) VALUES (%s, %s);",
                (record_id, uname),
            )

        for email in email_addresses:
            cur.execute(
                "INSERT INTO email_addresses (record_id, email) VALUES (%s, %s);",
                (record_id, email),
            )

        conn.commit()
        cur.close()
        return inserted_main

    except Exception:
        conn.rollback()
        cur.close()
        raise


def main():
    if not JSON_DIR.exists():
        print(f"JSON directory does not exist: {JSON_DIR}")
        return

    conn = psycopg2.connect(**DB_CONFIG)

    success_records = 0
    processed_files = 0
    error_files = 0

    current = START_NUMBER

    while True:
        filename = f"{current:07d}.json"
        filepath = JSON_DIR / filename

        if not filepath.exists():
            print(f"No file found: {filepath}. Stopping.")
            break

        processed_files += 1

        # Check if already processed before
        if file_already_processed(conn, filename):
            print(f"[SKIP] Already processed: {filename}")
            current += 1
            continue

        print(f"[PROC] Processing: {filename}")

        try:
            inserted = process_one_file(conn, filepath)
            success_records += inserted

            # mark success in tracking table
            mark_file_processed(conn, filename, True, None)

        except Exception as e:
            error_files += 1
            msg = f"Error processing {filename}: {e}"
            logging.error(msg)
            # store error text but avoid insane length
            short_err = str(e)[:1000]
            mark_file_processed(conn, filename, False, short_err)
            print(f"[ERR ] {msg}")

        current += 1

    conn.close()

    print("\n=== IMPORT COMPLETE ===")
    print(f"Files seen (contiguous from start):   {processed_files}")
    print(f"Main records inserted (new rows):     {success_records}")
    print(f"Files with errors:                    {error_files}")
    print(f"Error log file:                       {LOG_FILE}")


if __name__ == "__main__":
    main()
