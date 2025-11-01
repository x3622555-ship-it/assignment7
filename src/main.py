# src/main.py
"""
Main script to run the financial data pipeline.
Loads data, computes rolling metrics, benchmarks parallel methods, and generates reports.
"""

import time
import os
from src.data_loader import load_pandas, load_polars
from src.metrics import compute_rolling_metrics_pandas, compute_rolling_metrics_polars
from src.parallel import run_threading, run_multiprocessing
from src.reporting import summarize_performance, plot_comparison, write_performance_report


def main():
    print("\n🚀 Starting Financial Data Processing...\n")

    data_path = "data/market_data-1.csv"
    print(f"📂 Loading dataset from: {data_path}\n")

    # === Step 1: Load Data ===
    pandas_start = time.perf_counter()
    df_pandas = load_pandas(data_path)
    pandas_time = time.perf_counter() - pandas_start
    print(f"[pandas] Load time: {pandas_time:.2f}s")

    polars_start = time.perf_counter()
    df_polars = load_polars(data_path)
    polars_time = time.perf_counter() - polars_start
    print(f"[polars] Load time: {polars_time:.2f}s\n")

    # === Step 2: Compute Rolling Metrics (example symbol) ===
    symbol = "AAPL"
    print(f"📊 Computing rolling metrics for symbol: {symbol}\n")

    df_aapl_pd = df_pandas[df_pandas["symbol"] == symbol]
    start = time.perf_counter()
    result_pandas = compute_rolling_metrics_pandas(df_aapl_pd)
    pandas_metrics_time = time.perf_counter() - start
    print(f"[pandas] Rolling metrics done in {pandas_metrics_time:.2f}s")

    df_aapl_pl = df_polars.filter(df_polars["symbol"] == symbol)
    start = time.perf_counter()
    result_polars = compute_rolling_metrics_polars(df_aapl_pl)
    polars_metrics_time = time.perf_counter() - start
    print(f"[polars] Rolling metrics done in {polars_metrics_time:.2f}s\n")

    # === Step 3: Parallel Processing Benchmarks ===
    print("🧵 Benchmarking parallel performance (threading vs multiprocessing)...")
    start = time.perf_counter()
    run_threading(df_pandas)
    threading_time = time.perf_counter() - start

    start = time.perf_counter()
    run_multiprocessing(df_pandas)
    multiprocessing_time = time.perf_counter() - start

    print(f"[threading] {threading_time:.2f}s | [multiprocessing] {multiprocessing_time:.2f}s\n")

    # === Step 4: Save results ===
    os.makedirs("results", exist_ok=True)
    result_pandas.to_csv("results/aapl_metrics_pandas.csv")
    result_polars.write_csv("results/aapl_metrics_polars.csv")
    print("✅ Results saved to results/ folder.\n")

    # === Step 5: Reporting ===
    results_summary = {
        "pandas_load_time": pandas_time,
        "polars_load_time": polars_time,
        "pandas_metrics_time": pandas_metrics_time,
        "polars_metrics_time": polars_metrics_time,
        "threading_time": threading_time,
        "multiprocessing_time": multiprocessing_time
    }

    summarize_performance(results_summary)
    plot_comparison(results_summary)
    write_performance_report(results_summary)

    print("\n🎉 All processing steps completed successfully!")


if __name__ == "__main__":
    main()
