import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def must_getenv(k: str) -> str:
    v = os.getenv(k)
    if not v:
        raise ValueError(f"Missing env var: {k}")
    return v

DB = must_getenv("SNOWFLAKE_DATABASE")
RAW = os.getenv("SCHEMA_RAW", "RAW")
CLEAN = os.getenv("SCHEMA_CLEAN", "CLEAN")
ANALYTICS = os.getenv("SCHEMA_ANALYTICS", "ANALYTICS")
VALIDATION = os.getenv("SCHEMA_VALIDATION", "VALIDATION")

BIG_CSV_PATH = os.path.abspath("big_stock_prices.csv")
BIG_CSV_PATH = BIG_CSV_PATH.replace("\\", "/")

def main():
    conn = snowflake.connector.connect(
        account=must_getenv("SNOWFLAKE_ACCOUNT"),
        user=must_getenv("SNOWFLAKE_USER"),
        password=must_getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=must_getenv("SNOWFLAKE_WAREHOUSE"),
        database=DB
    )
    cur = conn.cursor()

    def run(sql: str):
        print(f"\n {sql.strip()}")
        cur.execute(sql)

    # 1) Create DB + schemas (safe)
    run(f"CREATE DATABASE IF NOT EXISTS {DB}")
    run(f"USE DATABASE {DB}")

    run(f"CREATE SCHEMA IF NOT EXISTS {RAW}")
    run(f"CREATE SCHEMA IF NOT EXISTS {CLEAN}")
    run(f"CREATE SCHEMA IF NOT EXISTS {ANALYTICS}")
    run(f"CREATE SCHEMA IF NOT EXISTS {VALIDATION}")

    # 2) Create RAW table
    run(f"""
    CREATE OR REPLACE TABLE {RAW}.stock_prices_raw (
        date DATE,
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume NUMBER,
        ticker STRING
    )
    """)

    # 3) Stage
    run(f"CREATE OR REPLACE STAGE {RAW}.stage_stock")

    # 4) Upload (PUT)
    # NOTE: file:// path must be absolute
    run(f"PUT 'file://{BIG_CSV_PATH}' @{RAW}.stage_stock OVERWRITE=TRUE")


    # 5) Copy into RAW table
    run(f"""
    COPY INTO {RAW}.stock_prices_raw
    FROM @{RAW}.stage_stock
    FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"')
    ON_ERROR='ABORT_STATEMENT'
    """)

    # 6) Quick row count check
    run(f"SELECT COUNT(*) AS rows_loaded FROM {RAW}.stock_prices_raw")
    rows = cur.fetchone()[0]
    print(f"\n✅ Loaded rows into {RAW}.stock_prices_raw: {rows:,}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
