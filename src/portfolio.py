import pandas as pd

def compute_position_metrics(position, df):
    symbol = position["symbol"]
    qty = position["quantity"]
    price_series = df[df["symbol"] == symbol]["price"]
    latest_price = price_series.iloc[-1]
    value = qty * latest_price
    returns = price_series.pct_change()
    volatility = returns.rolling(20).std().iloc[-1]
    drawdown = ((price_series / price_series.cummax()) - 1).min()
    return {"symbol": symbol, "value": value, "volatility": volatility, "drawdown": drawdown}
