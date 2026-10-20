"""
Extract robust microstructure features from Binance aggTrades Parquet files.

Per 1m window (group_by_dynamic, label="right", so the window timestamp ==
the kline close_time), ONLY features that 1m klines CANNOT provide are kept
(2026-10-20 精简，与用户确认):

  - 分钟内价格分布: vwap, vwap_disparity, price_median, price_mad,
    price_robust_zscore —— K 线只有 OHLC 四个价格点，不知道分钟内的典型
    成交价在哪、收盘价是否处在极端（收盘价相同的两根 K 线，一根全程低位
    成交尾盘拉高、一根均匀铺开，只有这些特征能分开）；
  - 真实已实现波动及方向分解: rv, rv_up, rv_down, semi_var_ratio ——
    逐笔收益才能干净地分出向上/向下半方差（K 线只能用 (high-low)/close
    或上下影线近似，把震荡和单边混在一起）；
  - 成交到达的时间结构: dt_mean, dt_std —— K 线的 count 只给笔数，
    给不了"均匀铺开 vs 几秒爆发后死寂"，后者常是行情启动前兆。

已删除（与 1m klines 冗余，或在拆单环境下无意义）:
  - trade_count == klines count；buy/sell 量 == taker_buy_volume 及其补数；
    volume/turnover、OHLC 同样冗余；
  - qty_* / sweep_depth（大单特征）—— 交易所普遍支持智能拆单 / Iceberg，
    单笔大小不再反映意图。

NOTE: log_return / dt_ms use a global shift(1) before the windowed
aggregation, so each window's first trade references the previous
window's last trade (a boundary approximation, negligible at 1m scale).
Per-day pre-aggregation (process_file) makes this per-day: each day's
first window has a null log_return / dt_ms -- same order of error.
"""
import argparse
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"

# 进 master 宽表的特征列（build_master_features 按市场加 um_agg_ / spot_agg_
# 前缀）。选取理由见模块文档字符串。
FEATURE_COLS = [
    "vwap", "vwap_disparity",
    "price_median", "price_mad", "price_robust_zscore",
    "rv", "rv_up", "rv_down", "semi_var_ratio",
    "dt_mean", "dt_std",
]


def compute_aggtrade_features(df_or_lf: pl.DataFrame | pl.LazyFrame, timeframe: str = "1m") -> pl.LazyFrame:
    """Compute microstructure features from an aggTrades frame.

    输出列 = timestamp + close + FEATURE_COLS。close 是窗口内最后一笔成交价
    （vwap_disparity / price_robust_zscore 需要它）；join 进 master 宽表时
    丢弃，因为 K 线里已有 close。
    """
    lf = df_or_lf.lazy() if isinstance(df_or_lf, pl.DataFrame) else df_or_lf

    # 1. Base derived columns
    lf = lf.with_columns([
        (pl.col("price") * pl.col("quantity")).alias("notional"),
        pl.col("transact_time").cast(pl.Datetime("ms")).alias("timestamp"),
    ]).sort("timestamp")

    # Log returns for Realized Volatility
    lf = lf.with_columns([
        (pl.col("price") / pl.col("price").shift(1)).log().alias("log_return"),
        (pl.col("transact_time") - pl.col("transact_time").shift(1)).alias("dt_ms"),
    ])

    # 2. Resample and aggregate per timeframe window (label="right" so window timestamp matches close_time)
    features_lf = lf.group_by_dynamic("timestamp", every=timeframe, label="right").agg([
        # Close (last trade price of the window; needed by the derived ratios)
        pl.col("price").last().alias("close"),

        # VWAP
        (pl.col("notional").sum() / (pl.col("quantity").sum() + 1e-8)).alias("vwap"),

        # Price Robust Statistics (Median & MAD)
        pl.col("price").median().alias("price_median"),
        (pl.col("price") - pl.col("price").median()).abs().median().alias("price_mad"),

        # Realized Volatility & Semi-Variance
        (pl.col("log_return").pow(2).sum() + 1e-12).sqrt().alias("rv"),
        (pl.col("log_return").filter(pl.col("log_return") > 0).pow(2).sum() + 1e-12).sqrt().alias("rv_up"),
        (pl.col("log_return").filter(pl.col("log_return") < 0).pow(2).sum() + 1e-12).sqrt().alias("rv_down"),

        # Inter-arrival Time (ms)
        pl.col("dt_ms").mean().alias("dt_mean"),
        pl.col("dt_ms").std().alias("dt_std"),
    ]).with_columns([
        # Secondary Derived Robust Indicators
        ((pl.col("close") - pl.col("vwap")) / (pl.col("vwap") + 1e-8)).alias("vwap_disparity"),
        ((pl.col("close") - pl.col("price_median")) / (1.4826 * pl.col("price_mad") + 1e-8)).alias("price_robust_zscore"),
        (pl.col("rv_up") / (pl.col("rv_down") + 1e-8)).alias("semi_var_ratio"),
    ])

    # 固定输出列序：timestamp + close + FEATURE_COLS（with_columns 会把派生列
    # 追加到末尾，这里显式定型，便于 build_master_features 按 FEATURE_COLS 前缀重命名）
    return features_lf.select(["timestamp", "close"] + FEATURE_COLS)


def process_file(file_path: Path, dest_dir: Path, timeframe: str = "1m") -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / f"{file_path.stem}_feat_{timeframe}.parquet"
    if out_path.exists():
        return out_path

    lf = compute_aggtrade_features(pl.scan_parquet(file_path), timeframe=timeframe)
    tmp_out = dest_dir / (out_path.name + ".part")
    lf.sink_parquet(str(tmp_out), compression="zstd")
    tmp_out.replace(out_path)
    return out_path


def main():
    ap = argparse.ArgumentParser(description="Extract robust microstructure features from aggTrades Parquet files")
    ap.add_argument("--input", required=True, help="Input aggTrades parquet file or directory")
    ap.add_argument("--output-dir", default=None, help="Output features directory (defaults to input dir/features)")
    ap.add_argument("--timeframe", default="1m", help="Resample window e.g. 1m, 5s, 10s")
    args = ap.parse_args()

    inp = Path(args.input)
    if inp.is_file():
        files = [inp]
    else:
        files = list(inp.rglob("*.parquet"))

    out_dir = Path(args.output_dir) if args.output_dir else (inp.parent / "features" if inp.is_file() else inp / "features")

    for f in files:
        if "_feat_" in f.name:
            continue
        out = process_file(f, out_dir, args.timeframe)
        print(f"Processed: {f.name} -> {out.name}", flush=True)


if __name__ == "__main__":
    main()
