import pandas as pd
from src.parallel import run_threading, run_multiprocessing

def test_parallel_equivalence():
    # Dummy dataset
    data = {
        "timestamp": pd.date_range(start="2024-01-01", periods=50, freq="D").tolist() * 2,
        "symbol": ["AAPL"] * 50 + ["MSFT"] * 50,
        "price": list(range(100, 150)) + list(range(200, 250))
    }
    df = pd.DataFrame(data)

    res_thread = run_threading(df)
    res_process = run_multiprocessing(df)

    assert set(res_thread.keys()) == set(res_process.keys())
