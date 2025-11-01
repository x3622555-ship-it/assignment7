# 📊 Assignment 7 — Financial Data Processing & Benchmarking

This project implements a **data processing pipeline** for market data using **Pandas** and **Polars**, comparing performance across **threading** and **multiprocessing** approaches.  
It also computes rolling metrics, portfolio analytics, and generates automatic performance reports.

---

## 🧱 Project Structure

assignment7/
├── data/ # CSV & JSON input files
│ ├── market_data-1.csv
│ └── portfolio_structure-1.json
├── results/ # Output metrics, plots, and report
│ ├── aapl_metrics_pandas.csv
│ ├── performance_pandas_polars.png
│ └── performance_report.md
├── src/ # Source code modules
│ ├── data_loader.py
│ ├── metrics.py
│ ├── parallel.py
│ ├── portfolio.py
│ ├── reporting.py
│ └── main.py
└── tests/ # Pytest unit tests


---

## ⚙️ Setup Instructions

### 1. Create and activate a virtual environment
```bash
python -m venv .venv
.venv\Scripts\activate     # Windows
