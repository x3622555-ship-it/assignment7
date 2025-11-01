# src/reporting.py
"""
reporting.py
Handles visualization, console summaries, and automatic writing of performance reports.
"""

import os
import datetime
import pandas as pd
import matplotlib.pyplot as plt


# === Console summary ===
def summarize_performance(results: dict):
    """
    Prints a clean performance summary to the console.
    """
    print("\n--- Performance Summary ---")

    df = pd.DataFrame([{
        "pandas_load_time": results.get("pandas_load_time", None),
        "polars_load_time": results.get("polars_load_time", None),
        "pandas_metrics_time": results.get("pandas_metrics_time", None),
        "polars_metrics_time": results.get("polars_metrics_time", None),
        "threading_time": results.get("threading_time", None),
        "multiprocessing_time": results.get("multiprocessing_time", None),
    }])

    print(df.to_markdown(index=False))
    return df


# === Visualization ===
def plot_comparison(results: dict = None):
    """
    Plots bar charts comparing Pandas vs Polars and Threading vs Multiprocessing.
    """
    print("📊 Generating comparison charts...")

    if results is None:
        print("⚠️ No results provided for plotting.")
        return

    os.makedirs("results", exist_ok=True)

    # --- Pandas vs Polars ---
    df1 = pd.DataFrame({
        "Library": ["pandas", "polars"],
        "Load Time (s)": [results.get("pandas_load_time", 0), results.get("polars_load_time", 0)],
        "Metrics Time (s)": [results.get("pandas_metrics_time", 0), results.get("polars_metrics_time", 0)]
    })

    fig, ax = plt.subplots(1, 2, figsize=(8, 4))
    df1.plot(x="Library", y="Load Time (s)", kind="bar", ax=ax[0], legend=False, color=["skyblue", "lightgreen"])
    df1.plot(x="Library", y="Metrics Time (s)", kind="bar", ax=ax[1], legend=False, color=["skyblue", "lightgreen"])
    ax[0].set_title("Load Time Comparison")
    ax[1].set_title("Rolling Metrics Comparison")
    plt.tight_layout()
    plt.savefig("results/performance_pandas_polars.png")
    plt.close()

    # --- Threading vs Multiprocessing ---
    df2 = pd.DataFrame({
        "Method": ["Threading", "Multiprocessing"],
        "Execution Time (s)": [results.get("threading_time", 0), results.get("multiprocessing_time", 0)]
    })

    fig, ax = plt.subplots(figsize=(5, 4))
    df2.plot(x="Method", y="Execution Time (s)", kind="bar", ax=ax, color=["orange", "purple"], legend=False)
    ax.set_title("Parallel Methods Comparison")
    plt.tight_layout()
    plt.savefig("results/performance_parallel.png")
    plt.close()

    print("✅ Charts saved to results/ folder.")


# === Markdown reporting ===
def write_performance_report(results: dict, filename: str = "performance_report.md"):
    """
    Appends benchmark results to the performance report file.
    """
    os.makedirs("results", exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    report_content = f"""
# 🧾 Performance Report Update
**Generated on:** {timestamp}

| Metric | pandas | polars |
|:--------|:--------:|:--------:|
| Load Time (s) | {results.get('pandas_load_time', 0):.2f} | {results.get('polars_load_time', 0):.2f} |
| Rolling Metrics Time (s) | {results.get('pandas_metrics_time', 0):.2f} | {results.get('polars_metrics_time', 0):.2f} |

| Parallel Method | Execution Time (s) |
|:-----------------|------------------:|
| Threading | {results.get('threading_time', 0):.2f} |
| Multiprocessing | {results.get('multiprocessing_time', 0):.2f} |

**Observations:**
- Polars loaded data ~{results.get('pandas_load_time', 1) / max(results.get('polars_load_time', 1e-9), 1e-9):.2f}× faster than Pandas.
- Polars rolling metrics were ~{results.get('pandas_metrics_time', 1) / max(results.get('polars_metrics_time', 1e-9), 1e-9):.2f}× faster.
- Multiprocessing outperformed threading by ~{results.get('threading_time', 1) / max(results.get('multiprocessing_time', 1e-9), 1e-9):.2f}×.

---

"""
    with open(filename, "a", encoding="utf-8") as f:
        f.write(report_content)

    print(f"📝 Performance summary appended to {filename}")
