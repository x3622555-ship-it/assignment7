import pandas as pd
from src.metrics import compute_rolling_metrics_pandas

def test_compute_rolling_metrics_pandas():
    # Create sample data
    df = pd.DataFrame({
        "timestamp": pd.date_range(start="2024-01-01", periods=30, freq="D"),
        "symbol": ["AAPL"] * 30,
        "price": range(100, 130)
    }).set_index("timestamp")

    result = compute_rolling_metrics_pandas(df, window=5)
    # Check columns exist
    assert "ma" in result.columns
    assert "volatility" in result.columns
    assert "sharpe" in result.columns
    # Check output length is same
    assert len(result) == 30
