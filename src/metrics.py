import pandas as pd
import polars as pl

# pandas version
def compute_rolling_metrics_pandas(df_symbol, window=20):
    df = df_symbol.copy()
    df['returns'] = df['price'].pct_change()
    df['ma'] = df['price'].rolling(window).mean()
    df['volatility'] = df['returns'].rolling(window).std()
    df['sharpe'] = df['returns'].rolling(window).mean() / df['volatility']
    return df

# polars version
def compute_rolling_metrics_polars(df, window=20):
    df = df.with_columns([
        (pl.col("price") / pl.col("price").shift(1) - 1).alias("returns")
    ])
    df = df.with_columns([
        pl.col("price").rolling_mean(window).alias("ma"),
        pl.col("returns").rolling_std(window).alias("volatility"),
        (pl.col("returns").rolling_mean(window) / pl.col("returns").rolling_std(window)).alias("sharpe")
    ])
    return df
