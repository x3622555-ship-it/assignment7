import pandas as pd
from src.portfolio import compute_position_metrics

def test_compute_position_metrics():
    df = pd.DataFrame({
        "timestamp": pd.date_range(start="2024-01-01", periods=25, freq="D"),
        "symbol": ["AAPL"] * 25,
        "price": [100 + i for i in range(25)]
    })
    position = {"symbol": "AAPL", "quantity": 10}
    result = compute_position_metrics(position, df)

    assert "value" in result
    assert "volatility" in result
    assert "drawdown" in result
    assert result["symbol"] == "AAPL"
