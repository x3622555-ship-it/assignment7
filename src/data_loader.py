# src/data_loader.py
import os
import time
import psutil
import pandas as pd
import polars as pl
from dateutil import parser as dateparser  # pip install python-dateutil

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
CSV_PATH = os.path.abspath(os.path.join(DATA_DIR, 'market_data-1.csv'))
JSON_PATH = os.path.abspath(os.path.join(DATA_DIR, 'portfolio_structure-1.json'))

def mem_rss_mb():
    proc = psutil.Process()
    return proc.memory_info().rss / (1024**2)

def preview_csv(path, n=5):
    print("--- CSV preview (first %d rows) ---" % n)
    with open(path, 'r', encoding='utf-8') as f:
        for _ in range(n):
            print(f.readline().strip())

def preview_json(path):
    print("--- JSON preview ---")
    with open(path, 'r', encoding='utf-8') as f:
        text = f.read(4000)  # first chunk
    print(text[:4000])

def guess_timestamp_format(sample_ts):
    # Try parsing with dateutil — robust for most formats
    try:
        dt = dateparser.parse(sample_ts)
        return dt.isoformat()
    except Exception:
        return None

# ---------- pandas loader ----------
def load_pandas(path, parse_dates=True):
    t0 = time.perf_counter()
    mem0 = mem_rss_mb()

    # if parse_dates False, we'll convert later
    df = pd.read_csv(path, parse_dates=['timestamp'] if parse_dates else None)
    # ensure expected columns
    expected = {'timestamp', 'symbol', 'price'}
    if not expected.issubset(df.columns):
        raise ValueError(f"CSV missing required cols. Found: {df.columns.tolist()}")

    # make timestamp the index (time-indexed)
    if 'timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    df = df.sort_values('timestamp').reset_index(drop=True)
    df = df.set_index('timestamp')

    t1 = time.perf_counter()
    mem1 = mem_rss_mb()
    print(f"[pandas] loaded {len(df)} rows in {t1-t0:.3f}s | mem delta: {mem1-mem0:.1f} MB")
    return df

# ---------- polars loader ----------
def load_polars(path, timestamp_format=None):
    t0 = time.perf_counter()
    mem0 = mem_rss_mb()

    # read raw (no parser) then parse timestamp explicitly
    df = pl.read_csv(path)

    # column check
    expected = {'timestamp', 'symbol', 'price'}
    if not expected.issubset(set(df.columns)):
        raise ValueError(f"CSV missing required cols. Found: {df.columns}")

    if timestamp_format:
        df = df.with_columns(
            pl.col('timestamp').str.strptime(pl.Datetime, timestamp_format)
        )
    else:
        # try polars auto-parse
        try:
            df = df.with_columns(pl.col('timestamp').str.strptime(pl.Datetime, fmt=None))
        except Exception:
            # fallback: leave as string and parse in pandas if needed
            pass

    # sort
    df = df.sort('timestamp')
    t1 = time.perf_counter()
    mem1 = mem_rss_mb()
    print(f"[polars] loaded {df.shape[0]} rows in {t1-t0:.3f}s | mem delta: {mem1-mem0:.1f} MB")
    return df

# ---------- quick checks ----------
def quick_checks_pandas(df):
    print("---- pandas quick checks ----")
    print("Rows:", len(df))
    print("Symbols (sample 10):", df['symbol'].unique()[:10])
    print("Price dtype:", df['price'].dtype)
    print("Nulls per column:\n", df.isna().sum())
    print("Time index head/tail:\n", df.index[:3], df.index[-3:])

def quick_checks_polars(df):
    print("---- polars quick checks ----")
    print("Rows:", df.shape[0])
    print("Symbols (sample 10):", df.select(pl.col('symbol')).unique().head(10))
    print("Price dtype:", df['price'].dtype)
    print("Nulls per column:\n", df.select([pl.col(c).is_null().sum().alias(c) for c in df.columns]))
    print("Time column head/tail:\n", df.select('timestamp').head(3), df.select('timestamp').tail(3))

# ---------- run these when executed directly ----------
if __name__ == "__main__":
    preview_csv(CSV_PATH, n=5)
    preview_json(JSON_PATH)

    # try to load small sample with pandas to inspect timestamp string
    with open(CSV_PATH, 'r', encoding='utf-8') as f:
        header = f.readline()
        sample = f.readline().strip().split(',')[0]  # crude: timestamp of 2nd line
    print("Sample timestamp string:", sample)
    print("Dateutil parse guess:", guess_timestamp_format(sample))

    # load full
    df_pd = load_pandas(CSV_PATH, parse_dates=True)
    quick_checks_pandas(df_pd)

    df_pl = load_polars(CSV_PATH)
    quick_checks_polars(df_pl)
