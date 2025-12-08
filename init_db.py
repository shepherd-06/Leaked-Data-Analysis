import psycopg2

DB_CONFIG = {
    "dbname": "leakatlas",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": 5433,
}

TABLES = [
    "records",
    "passwords",
    "domain_names",
    "services",
    "usernames",
    "email_addresses",
]


def create_tables(conn):
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS records (
            id BIGINT PRIMARY KEY,
            device_ip_addr TEXT,
            timestamp DATE,
            country TEXT,
            keyboard TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS passwords (
            id BIGSERIAL PRIMARY KEY,
            record_id BIGINT REFERENCES records(id) ON DELETE CASCADE,
            password TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS domain_names (
            id BIGSERIAL PRIMARY KEY,
            record_id BIGINT REFERENCES records(id) ON DELETE CASCADE,
            domain TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS services (
            id BIGSERIAL PRIMARY KEY,
            record_id BIGINT REFERENCES records(id) ON DELETE CASCADE,
            service_url TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS usernames (
            id BIGSERIAL PRIMARY KEY,
            record_id BIGINT REFERENCES records(id) ON DELETE CASCADE,
            username TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS email_addresses (
            id BIGSERIAL PRIMARY KEY,
            record_id BIGINT REFERENCES records(id) ON DELETE CASCADE,
            email TEXT
        );
        """,
    ]

    with conn.cursor() as cur:
        for ddl in ddl_statements:
            cur.execute(ddl)
        conn.commit()


def print_status(conn):
    with conn.cursor() as cur:
        # Check which of our tables exist
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ANY(%s)
            ORDER BY table_name;
            """,
            (TABLES,),
        )
        existing = [row[0] for row in cur.fetchall()]

        print("=== Schema Status ===")
        print(f"Total expected tables: {len(TABLES)}")
        print(f"Existing tables:       {len(existing)}")

        if not existing:
            print("No target tables exist yet.")
            return

        print("\nTables found:")
        for name in existing:
            print(f" - {name}")

        print("\nRow counts:")
        for name in existing:
            cur.execute(f"SELECT COUNT(*) FROM {name};")
            count = cur.fetchone()[0]
            print(f" {name:16s} -> {count} rows")


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        create_tables(conn)
        print_status(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
