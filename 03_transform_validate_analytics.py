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

def connect():
    return snowflake.connector.connect(
        account=must_getenv("SNOWFLAKE_ACCOUNT"),
        user=must_getenv("SNOWFLAKE_USER"),
        password=must_getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=must_getenv("SNOWFLAKE_WAREHOUSE"),
        database=DB,
    )

def run(cur, sql: str):
    print("\n Running SQL:\n", sql.strip())
    cur.execute(sql)

def fetch1(cur, sql: str):
    run(cur, sql)
    return cur.fetchone()

def main():
    conn = connect()
    cur = conn.cursor()

    # Use correct DB
    run(cur, f"USE DATABASE {DB}")

    # ---------- STEP 1: CLEAN ----------
    run(cur, f"""
    CREATE OR REPLACE TABLE {CLEAN}.stock_prices_clean AS
    WITH ranked AS (
      SELECT
        ticker,
        date,
        open, high, low, close, volume,
        ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY date DESC) AS rn
      FROM {RAW}.stock_prices_raw
    )
    SELECT
      ticker,
      date,
      open, high, low, close, volume,
      (close / LAG(close) OVER (PARTITION BY ticker ORDER BY date) - 1) AS daily_return
    FROM ranked
    WHERE rn = 1;
    """)

    # ---------- STEP 2: VALIDATION ----------
    run(cur, f"""
    CREATE OR REPLACE TABLE {VALIDATION}.row_counts AS
    SELECT '{RAW}.stock_prices_raw' AS table_name, COUNT(*) AS row_count FROM {RAW}.stock_prices_raw
    UNION ALL
    SELECT '{CLEAN}.stock_prices_clean', COUNT(*) FROM {CLEAN}.stock_prices_clean;
    """)

    run(cur, f"""
    CREATE OR REPLACE TABLE {VALIDATION}.null_key_checks AS
    SELECT 'ticker_or_date_null' AS check_name, COUNT(*) AS bad_rows
    FROM {CLEAN}.stock_prices_clean
    WHERE ticker IS NULL OR date IS NULL;
    """)

    run(cur, f"""
    CREATE OR REPLACE TABLE {VALIDATION}.duplicate_checks AS
    SELECT 'dup_ticker_date' AS check_name, COUNT(*) AS dup_groups
    FROM (
      SELECT ticker, date, COUNT(*) c
      FROM {CLEAN}.stock_prices_clean
      GROUP BY ticker, date
      HAVING COUNT(*) > 1
    );
    """)

    run(cur, f"""
    CREATE OR REPLACE TABLE {VALIDATION}.source_target_diff AS
    SELECT
      (SELECT COUNT(*) FROM {RAW}.stock_prices_raw) -
      (SELECT COUNT(*) FROM {CLEAN}.stock_prices_clean) AS raw_minus_clean;
    """)

    # Print validation summary
    raw_minus_clean = fetch1(cur, f"SELECT raw_minus_clean FROM {VALIDATION}.source_target_diff")[0]
    nulls = fetch1(cur, f"SELECT bad_rows FROM {VALIDATION}.null_key_checks")[0]
    dups = fetch1(cur, f"SELECT dup_groups FROM {VALIDATION}.duplicate_checks")[0]

    print("\n VALIDATION SUMMARY")
    print("raw_minus_clean:", raw_minus_clean)
    print("null_key_bad_rows:", nulls)
    print("dup_groups:", dups)

    # ---------- STEP 3: ANALYTICS ----------
    run(cur, f"""
    CREATE OR REPLACE TABLE {ANALYTICS}.stock_kpis AS
    SELECT
      ticker,
      date,
      close,
      daily_return,

      AVG(close) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
      ) AS ma20,

      AVG(close) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
      ) AS ma50,

      STDDEV_SAMP(daily_return) OVER (
        PARTITION BY ticker
        ORDER BY date
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
      ) AS vol20

    FROM {CLEAN}.stock_prices_clean;
    """)

    # quick sample
    sample = fetch1(cur, f"SELECT ticker, date, close FROM {ANALYTICS}.stock_kpis ORDER BY date DESC LIMIT 1")
    print("\n ANALYTICS TABLE READY. Sample row:", sample)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
