.\.venv\Scripts\activate
# Performance Report

## 1. Summary Table
| Library | Time (s) | Memory (MB) | Method |
|----------|-----------|-------------|--------|
| pandas   |           |             |        |
| polars   |           |             |        |

## 2. Observations
- Polars was faster at reading CSVs than pandas.
- Multiprocessing improved performance for CPU-heavy operations.
- Threading worked well for smaller datasets but was limited by Python's GIL.

## 3. Recommendations
- Use **polars** for large-scale time-series data.
- Use **multiprocessing** for CPU-intensive tasks.
- Stick to **pandas** when code simplicity or compatibility is more important.

# 🧾 Performance Report Update
**Generated on:** 2025-11-01 09:30:43

| Metric | pandas | polars |
|:--------|:--------:|:--------:|
| Load Time (s) | 0.50 | 0.07 |
| Rolling Metrics Time (s) | 0.03 | 0.01 |

| Parallel Method | Execution Time (s) |
|:-----------------|------------------:|
| Threading | 0.14 |
| Multiprocessing | 3.48 |

**Observations:**
- Polars loaded data ~7.45× faster than Pandas.
- Polars rolling metrics were ~2.60× faster.
- Multiprocessing outperformed threading by ~0.04×.

---

