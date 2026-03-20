import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
META_PATH = os.path.join(ARCHIVE_DIR, "symbols_valid_meta.csv")
STOCKS_DIR = os.path.join(ARCHIVE_DIR, "stocks")

OUT_PATH = os.path.join(BASE_DIR, "big_stock_prices.csv")

# Big-data feel
N_STOCKS = 200


def main():
    print(" Reading metadata...")
    meta = pd.read_csv(META_PATH)

    print(" Columns:", meta.columns.tolist())

    # Your file has 'ETF' column:
    # ETF = 'N' means it's a stock (not an ETF)
    if "ETF" not in meta.columns:
        raise RuntimeError(" 'ETF' column not found in symbols_valid_meta.csv")

    meta["ETF"] = meta["ETF"].astype(str).str.strip().str.upper()
    meta = meta[meta["ETF"] == "N"]   # keep only stocks

    print(f" Filtered to STOCKS (ETF == 'N'). Remaining symbols: {len(meta)}")

    # Pick symbols that actually have CSVs inside archive/stocks
    symbols = []
    for sym in meta["Symbol"].astype(str).str.strip().tolist():
        fp = os.path.join(STOCKS_DIR, f"{sym}.csv")
        if os.path.exists(fp):
            symbols.append(sym)
        if len(symbols) >= N_STOCKS:
            break

    if not symbols:
        raise RuntimeError(" No stock CSVs found. Check archive/stocks contains files like AAPL.csv")

    print(f" Using {len(symbols)} stocks")

    frames = []
    for i, sym in enumerate(symbols, start=1):
        path = os.path.join(STOCKS_DIR, f"{sym}.csv")
        df = pd.read_csv(path)

        # normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # dataset normally has: date, open, high, low, close, volume
        keep_candidates = ["date", "open", "high", "low", "close", "volume"]
        keep = [c for c in keep_candidates if c in df.columns]

        # must have date + close
        if "date" not in keep or "close" not in keep:
            continue

        df = df[keep].copy()
        df["ticker"] = sym

        frames.append(df)

        if i % 25 == 0:
            print(f"Loaded {i}/{len(symbols)} stocks...")

    if not frames:
        raise RuntimeError(" None of the selected CSVs had expected columns (date/close).")

    print(" Combining into ONE big table...")
    big = pd.concat(frames, ignore_index=True)

    big["date"] = pd.to_datetime(big["date"], errors="coerce").dt.date
    big = big.dropna(subset=["ticker", "date", "close"])

    big.to_csv(OUT_PATH, index=False)

    print("\n BIG DATASET READY")
    print(f"File: {OUT_PATH}")
    print(f"Rows: {len(big):,}")
    print(f"Tickers: {big['ticker'].nunique():,}")


if __name__ == "__main__":
    main()
