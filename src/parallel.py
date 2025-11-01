# src/parallel.py
"""
parallel.py
Implements threading and multiprocessing versions of rolling metrics computation.
"""

import os
import psutil
import time
import pandas as pd
from functools import partial
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from src.metrics import compute_rolling_metrics_pandas


# === Worker function (top-level, picklable) ===
def process_symbol_for_metrics(symbol: str, df_chunk: pd.DataFrame, window: int = 20):
    df_symbol = df_chunk[df_chunk["symbol"] == symbol]
    result = compute_rolling_metrics_pandas(df_symbol, window=window)
    return (symbol, result)


# === Threading version ===
def run_threading(df: pd.DataFrame, window: int = 20):
    symbols = df["symbol"].unique()
    results = {}
    start = time.perf_counter()

    print(f"🧵 Starting ThreadPoolExecutor with {os.cpu_count()} workers...")

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(process_symbol_for_metrics, sym, df, window): sym for sym in symbols}

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                sym, res = future.result()
                results[sym] = res
            except Exception as e:
                print(f"⚠️ Error processing {symbol}: {e}")

    duration = time.perf_counter() - start
    mem_usage = psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)
    print(f"✅ Threading completed in {duration:.2f}s | Memory: {mem_usage:.1f} MB")

    return results


# === Multiprocessing version ===
def run_multiprocessing(df: pd.DataFrame, window: int = 20):
    symbols = df["symbol"].unique()
    results = {}
    start = time.perf_counter()

    print(f"⚙️ Starting ProcessPoolExecutor with {os.cpu_count()} workers...")

    # Use functools.partial to avoid lambda (which isn't picklable)
    worker = partial(process_symbol_for_metrics, df_chunk=df, window=window)

    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        for symbol, result in executor.map(worker, symbols):
            results[symbol] = result

    duration = time.perf_counter() - start
    mem_usage = psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)
    print(f"✅ Multiprocessing completed in {duration:.2f}s | Memory: {mem_usage:.1f} MB")

    return results


if __name__ == "__main__":
    # Standalone test
    df_test = pd.DataFrame({
        "timestamp": pd.date_range(start="2024-01-01", periods=100, freq="D").tolist() * 2,
        "symbol": ["AAPL"] * 100 + ["MSFT"] * 100,
        "price": list(range(100, 200)) + list(range(200, 300))
    })

    print("\n--- THREADING TEST ---")
    run_threading(df_test)

    print("\n--- MULTIPROCESSING TEST ---")
    run_multiprocessing(df_test)
