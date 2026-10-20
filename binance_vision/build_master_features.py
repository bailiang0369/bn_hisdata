"""
Build consolidated Master Feature Parquet table for a symbol (e.g., BTCUSDT).

Combines:
 1. Futures 1m Klines (OHLC + Volume + Counts)
 2. Spot 1m Klines (Volume + Counts + Spot Close for Basis)
 3. Futures 1m aggTrades Microstructure Features (VWAP disparity, Robust
    Z-score, RV, Sweep Depth, etc.)  -- required unless --no-agg. Like the
    orderbook module, aggTrades only ever contributes DERIVED features, never
    raw rows: downloads are big/slow, so it is a SECOND stage built on top of
    the base table (klines + basis + metrics), not part of the base itself.
 4. Futures 5m Metrics (Open Interest, Long/Short ratios via join_asof / ffill)
 5. Derives Basis (Futures Close - Spot Close)
 6. Provides rolling_quantile_bucket helper for rolling quantile bucket
    discretizations.

Data-flow / correctness notes (from the 2026-10 review):
  * close_time is the bar END instant (open_time + interval - 1, then +1ms in
    the downloader, i.e. == the next candle's open_time). This is the natural
    leak-free join key: an orderbook minute bucket b corresponds to the master
    row with close_time == b + 60000 (same minute window), and a feature row
    with close_time == c is only knowable at instant c.
  * metrics create_time is shifted +5m before the asof join so a metrics
    snapshot is only visible to bars that end strictly after it (no lookahead).
  * The whole pipeline is lazy end to end (scan_parquet -> joins ->
    sink_parquet), so building a table from years of monthly files never
    materializes the frame; only the tiny metrics frame is joined via asof.
    The old implementation's pl.read_parquet(list_of_all_files) was an OOM
    waiting to happen once the kline backfill grows past ~1 year.
  * main() HARD FAILS when no aggTrades parquet is found -- unless --no-agg is
    passed, which builds the BASE table (klines + basis + metrics only) with a
    loud WARNING listing the 20 missing microstructure columns and a distinct
    default output name ({symbol}_base_features_1m.parquet). The hard fail
    exists because silently producing a "master" table without the
    microstructure features was a real bug: the tables generated on
    2026-10-20 were missing vwap_disparity / price_robust_zscore / rv /
    sweep_depth / qty_* / trade_count / dt_* / semi_var_ratio without any
    warning.
  * After writing, a single-column scan prints rows / time range /
    missing-minute count so silent gaps or duplicates cannot slip through.
"""
import argparse
import sys
from pathlib import Path

import polars as pl

# 允许 `python binance_vision/build_master_features.py` 从仓库根直接运行：
# 直接跑脚本时 sys.path[0] 是 binance_vision/ 而不是仓库根，包导入会失败。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from binance_vision.build_aggtrade_features import compute_aggtrade_features  # noqa: E402

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"


def rolling_quantile_bucket(col_name: str, window_size: int, n_buckets: int = 10, alias: str = None) -> pl.Expr:
    """Compute integer quantile bucket (0 .. n_buckets-1) for a column over a rolling window_size."""
    if alias is None:
        alias = f"{col_name}_bucket_{window_size}m_q{n_buckets}"
    col = pl.col(col_name)
    r_min = col.rolling_min(window_size=window_size)
    r_max = col.rolling_max(window_size=window_size)
    # Scale to [0, 1] relative position within the rolling window, then map to Int8 bucket index (0..n_buckets-1)
    rel_pos = (col - r_min) / (r_max - r_min + 1e-8)
    bucket = (rel_pos * n_buckets).floor().cast(pl.Int8)
    bucket_clipped = pl.when(bucket >= n_buckets).then(n_buckets - 1).when(bucket < 0).then(0).otherwise(bucket)
    return bucket_clipped.cast(pl.Int8).alias(alias)


def build_master_feature_table_lazy(
    df_um_klines: pl.DataFrame | pl.LazyFrame,
    df_spot_klines: pl.DataFrame | pl.LazyFrame,
    df_aggtrades: pl.DataFrame | pl.LazyFrame | None = None,
    df_metrics: pl.DataFrame | pl.LazyFrame | None = None,
    timeframe: str = "1m",
) -> pl.LazyFrame:
    """Lazy version of build_master_feature_table: join everything, return a
    LazyFrame the caller can sink_parquet (streaming, bounded memory)."""
    lf_um = df_um_klines.lazy() if isinstance(df_um_klines, pl.DataFrame) else df_um_klines
    lf_spot = df_spot_klines.lazy() if isinstance(df_spot_klines, pl.DataFrame) else df_spot_klines

    # 1. Futures Klines (Primary OHLC & Futures Volume)
    # close_time 归一化到 ms（防御 µs epoch 的文件；下载器输出的已是 ms，这里是 no-op）
    lf_um_prep = lf_um.with_columns(
        pl.when(pl.col("close_time") > 1e14)
        .then(pl.col("close_time") // 1000)
        .otherwise(pl.col("close_time"))
        .alias("close_time")
    ).select(
        [
            pl.col("close_time"),
            pl.col("open"),
            pl.col("high"),
            pl.col("low"),
            pl.col("close"),
            pl.col("buy_vol").alias("um_buy_vol"),
            pl.col("sell_vol").alias("um_sell_vol"),
            pl.col("count").alias("um_count"),
        ]
    )

    # 2. Spot Klines (Spot Volume & Spot Close for Basis)
    lf_spot_prep = lf_spot.with_columns(
        pl.when(pl.col("close_time") > 1e14)
        .then(pl.col("close_time") // 1000)
        .otherwise(pl.col("close_time"))
        .alias("close_time")
    ).select(
        [
            pl.col("close_time"),
            pl.col("close").alias("spot_close"),
            pl.col("buy_vol").alias("spot_buy_vol"),
            pl.col("sell_vol").alias("spot_sell_vol"),
            pl.col("count").alias("spot_count"),
        ]
    )

    # Deduplicate and Join Futures & Spot
    # （daily 与 monthly 目录若覆盖同一段时间，unique 按出现顺序保留第一份；
    #   两份数值相同，无歧义）
    lf_um_prep = lf_um_prep.unique(subset=["close_time"])
    lf_spot_prep = lf_spot_prep.unique(subset=["close_time"])
    master_lf = lf_um_prep.join(lf_spot_prep, on="close_time", how="inner")

    # 3. Compute Basis (Futures Close - Spot Close)
    master_lf = master_lf.with_columns(
        [
            (pl.col("close") - pl.col("spot_close")).alias("basis"),
        ]
    )

    # 4. Join aggTrades Microstructure Features (if provided)
    if df_aggtrades is not None:
        lf_agg = compute_aggtrade_features(df_aggtrades, timeframe=timeframe)
        lf_agg = lf_agg.with_columns(
            pl.col("timestamp").dt.epoch("ms").alias("close_time")
        ).drop(["open", "high", "low", "close", "volume", "turnover", "timestamp"])
        master_lf = master_lf.join(lf_agg, on="close_time", how="left")

    # join_asof 要求左侧按 close_time 有序：显式 sort（lazy 下 join 不保证保序）
    master_lf = master_lf.sort("close_time")

    # 5. Join Metrics (5m sampling via join_asof / ffill) (if provided)
    if df_metrics is not None:
        lf_met = df_metrics.lazy() if isinstance(df_metrics, pl.DataFrame) else df_metrics
        # Shift metrics +5m (300,000ms) to ensure no lookahead leakage
        lf_met = (
            lf_met.select(
                [
                    (pl.col("create_time") + 300000).alias("create_time"),
                    pl.col("sum_open_interest"),
                    pl.col("sum_open_interest_value"),
                    pl.col("count_toptrader_long_short_ratio"),
                    pl.col("sum_toptrader_long_short_ratio"),
                    pl.col("count_long_short_ratio"),
                    pl.col("sum_taker_long_short_vol_ratio"),
                ]
            )
            .sort("create_time")
        )
        master_lf = master_lf.join_asof(
            lf_met,
            left_on="close_time",
            right_on="create_time",
            strategy="backward",
        ).drop("create_time")

    return master_lf


def build_master_feature_table(
    df_um_klines: pl.DataFrame | pl.LazyFrame,
    df_spot_klines: pl.DataFrame | pl.LazyFrame,
    df_aggtrades: pl.DataFrame | pl.LazyFrame | None = None,
    df_metrics: pl.DataFrame | pl.LazyFrame | None = None,
    timeframe: str = "1m",
) -> pl.DataFrame:
    """Eager convenience wrapper (used by tests); main() uses the lazy path."""
    return build_master_feature_table_lazy(
        df_um_klines, df_spot_klines, df_aggtrades, df_metrics, timeframe
    ).collect()


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbol", default="BTCUSDT", help="Symbol (default: BTCUSDT)")
    ap.add_argument("--data-root", default=str(DATA), help="Root data directory")
    ap.add_argument("--output", default=None, help="Output master parquet file path")
    ap.add_argument("--no-agg", action="store_true",
                    help="允许在没有 aggTrades 数据时构建基础宽表（只含 um+spot "
                         "klines、basis、metrics；20 个微观结构列将缺失，会有醒目 "
                         "WARNING）。不带此选项时缺少 aggTrades 直接失败。基础宽表"
                         "默认输出 {symbol}_base_features_1m.parquet，与完整 master "
                         "表区分，便于后续补下 aggTrades 后重建。")
    args = ap.parse_args()

    root = Path(args.data_root)
    symbol = args.symbol

    um_kline_files = (
        sorted((root / "um" / "daily" / "klines" / symbol / "1m").glob("*.parquet"))
        + sorted((root / "um" / "monthly" / "klines" / symbol / "1m").glob("*.parquet"))
    )
    spot_kline_files = (
        sorted((root / "spot" / "daily" / "klines" / symbol / "1m").glob("*.parquet"))
        + sorted((root / "spot" / "monthly" / "klines" / symbol / "1m").glob("*.parquet"))
    )
    if not um_kline_files or not spot_kline_files:
        print(f"Error: Missing kline files for {symbol} under {root}")
        sys.exit(1)

    agg_files = sorted((root / "um" / "daily" / "aggTrades" / symbol).glob("*.parquet")) + sorted(
        (root / "um" / "monthly" / "aggTrades" / symbol).glob("*.parquet")
    )
    if not agg_files and not args.no_agg:
        # 硬失败：没有 aggTrades 就会静默产出缺全部微观特征的“master”表，
        # 而 docstring 宣称包含它们——2026-10-20 上传的表就是这么缺的。
        # --no-agg 是唯一逃生门（见下面的 WARNING 分支）。
        print(
            f"Error: no aggTrades parquet under "
            f"{root}/um/{{daily,monthly}}/aggTrades/{symbol}/ -- refusing to build a "
            f"master table WITHOUT the microstructure features "
            f"(vwap_disparity, price_robust_zscore, rv, rv_up/down, semi_var_ratio, "
            f"sweep_depth, qty_*, trade_count, dt_*).\n"
            f"Download aggTrades first, e.g.:\n"
            f"  python binance_vision/download_binance_vision.py --market um "
            f"--symbol {symbol} --category aggTrades --interval 1m "
            f"--granularity daily --start-date YYYY-MM-01 --end-date YYYY-MM-DD\n"
            f"(or pass --no-agg to build the BASE table without microstructure "
            f"features)"
        )
        sys.exit(1)
    if not agg_files:
        # --no-agg：显式要基础宽表。WARNING 列全将缺席的 20 个微观列，
        # 避免以后拿到表才发现缺特征。
        print("WARNING: --no-agg: no aggTrades parquet found; the base table will "
              "NOT contain the 20 microstructure columns: vwap, vwap_disparity, "
              "price_median, price_mad, price_robust_zscore, qty_median, qty_q25, "
              "qty_q75, qty_q90, qty_iqr, qty_tail_ratio, rv, rv_up, rv_down, "
              "semi_var_ratio, trade_count, mean_sweep_depth, max_sweep_depth, "
              "dt_mean, dt_std. Rebuild the full table after downloading aggTrades.")
        lf_agg = None
    else:
        lf_agg = pl.scan_parquet(agg_files)

    metric_files = sorted((root / "um" / "daily" / "metrics" / symbol).glob("*.parquet"))
    if not metric_files:
        print(f"WARNING: no metrics parquet under "
              f"{root}/um/daily/metrics/{symbol}/ -- open-interest and "
              f"long/short-ratio columns will be absent.")

    # 全 lazy：scan_parquet 不把文件读进内存，sink_parquet 流式写出
    lf_um = pl.scan_parquet(um_kline_files)
    lf_spot = pl.scan_parquet(spot_kline_files)
    lf_met = pl.scan_parquet(metric_files) if metric_files else None

    master_lf = build_master_feature_table_lazy(lf_um, lf_spot, lf_agg, lf_met, timeframe="1m")

    if args.output:
        out_path = Path(args.output)
    elif args.no_agg:
        # 基础宽表用独立名字，和以后补下 aggTrades 的完整表区分
        out_path = root / f"{symbol}_base_features_1m.parquet"
    else:
        out_path = root / f"{symbol}_master_features_1m.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_out = out_path.parent / (out_path.name + ".part")
    master_lf.sink_parquet(str(tmp_out), compression="zstd")
    tmp_out.replace(out_path)  # 原子：out_path 上只会出现完整文件

    # 单列扫描做健全性检查：行数、时间范围、缺失/重复分钟数
    chk = pl.scan_parquet(out_path).select(
        pl.len().alias("rows"),
        pl.col("close_time").min().alias("min_ct"),
        pl.col("close_time").max().alias("max_ct"),
        pl.col("close_time").n_unique().alias("n_unique"),
    ).collect()
    rows, mn, mx, nu = chk["rows"][0], chk["min_ct"][0], chk["max_ct"][0], chk["n_unique"][0]
    span = (mx - mn) // 60000 + 1
    print(f"Successfully generated {'base' if args.no_agg else 'master'} "
          f"feature table: {out_path}")
    print(f"  rows={rows} columns={len(master_lf.collect_schema().names())}")
    print(f"  range: {pl.Series([mn]).cast(pl.Datetime('ms'))[0]} .. "
          f"{pl.Series([mx]).cast(pl.Datetime('ms'))[0]} (UTC)")
    print(f"  minutes: span={span} unique={nu} missing={span - nu} duplicated={rows - nu}")
    if span != nu or rows != nu:
        print("  WARNING: time gaps or duplicates detected (see counts above).")


if __name__ == "__main__":
    main()
