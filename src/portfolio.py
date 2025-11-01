# src/portfolio.py
"""
portfolio.py
Computes per-position and aggregated portfolio metrics (value, volatility, drawdown).
"""

import pandas as pd
from concurrent.futures import ProcessPoolExecutor


def compute_position_metrics(position: dict, market_df: pd.DataFrame, window: int = 20) -> dict:
    """
    Compute latest value, rolling volatility, and max drawdown for one position.
    position = {"symbol": "AAPL", "quantity": 10}
    """
    symbol = position["symbol"]
    qty = position["quantity"]
    df = market_df[market_df["symbol"] == symbol].sort_values("timestamp")

    if df.empty:
        return {"symbol": symbol, "value": 0, "volatility": 0, "max_drawdown": 0}

    prices = df["price"]
    latest_price = prices.iloc[-1]
    value = qty * latest_price

    returns = prices.pct_change().dropna()
    volatility = returns.rolling(window).std().iloc[-1]
    cummax = prices.cummax()
    drawdown = (prices / cummax - 1).min()

    return {
        "symbol": symbol,
        "value": value,
        "volatility": float(volatility),
        "max_drawdown": float(drawdown)
    }


def aggregate_portfolio(positions: list, market_df: pd.DataFrame, window: int = 20) -> dict:
    """
    Aggregate portfolio metrics sequentially.
    """
    results = [compute_position_metrics(p, market_df, window) for p in positions]
    total_value = sum(r["value"] for r in results)
    weights = [r["value"] / total_value if total_value else 0 for r in results]
    agg_vol = sum(w * r["volatility"] for w, r in zip(weights, results))
    agg_dd = min(r["max_drawdown"] for r in results)
    return {
        "total_value": total_value,
        "aggregate_volatility": agg_vol,
        "max_drawdown": agg_dd,
        "positions": results
    }


def aggregate_portfolio_parallel(positions: list, market_df: pd.DataFrame, window: int = 20) -> dict:
    """
    Aggregate portfolio metrics using multiprocessing.
    """
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(lambda p: compute_position_metrics(p, market_df, window), positions))
    total_value = sum(r["value"] for r in results)
    weights = [r["value"] / total_value if total_value else 0 for r in results]
    agg_vol = sum(w * r["volatility"] for w, r in zip(weights, results))
    agg_dd = min(r["max_drawdown"] for r in results)
    return {
        "total_value": total_value,
        "aggregate_volatility": agg_vol,
        "max_drawdown": agg_dd,
        "positions": results
    }
