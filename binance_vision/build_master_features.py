"""
Build consolidated Master Feature Parquet table for a symbol (e.g., BTCUSDT).

Combines:
 1. Futures 1m Klines (OHLC + Volume + Counts)
 2. Spot 1m Klines (Volume + Counts + Spot Close for Basis)
 3. Microstructure features from BOTH markets' aggTrades (required unless
    --no-agg): um features are prefixed `um_agg_*`, spot `spot_agg_*`, plus
    the cross-market `rv_dominance` (>0 = 波动由合约端主导, <0 = 现货端).
    Like the orderbook module, aggTrades only ever contributes DERIVED
    features, never raw rows: downloads are big/slow, so it is a SECOND
    stage built on top of the base table (klines + basis + metrics), not
    part of the base itself.
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
  * aggTrades feature selection (2026-10-20, with the user): keep ONLY what
    1m klines cannot provide -- intra-minute price distribution
    (vwap_disparity / price_robust_zscore / price_median / price_mad),
    true realized vol + semi-variance (rv / rv_up / rv_down /
    semi_var_ratio) and trade-arrival structure (dt_mean / dt_std).
    Dropped as redundant with klines: trade_count (== klines count),
    buy/sell volumes (== taker_buy_volume and its complement),
    volume/turnover, OHLC. Dropped as meaningless under exchange order
    splitting / iceberg: all large-trade stats (qty_*, sweep_depth).
    See build_aggtrade_features.py.
  * aggTrades input per market (see _scan_agg_files): raw
    `aggTrades/{symbol}/*.parquet` (features computed on the fly; shift(1)
    is global over all files, exact across day boundaries) OR precomputed
    `aggTrades/{symbol}/features/*_feat_1m.parquet` (from
    build_aggtrade_features.process_file; per-day boundary approximation).
    Precomputed files WIN when present -- that is the disk-lean path, since
    raw aggTrades run ~25MB/day/market/symbol and can then be deleted.
  * main() HARD FAILS when no um aggTrades (raw or precomputed) parquet is
    found -- unless --no-agg is passed, which builds the BASE table (klines +
    basis + metrics only) with a loud WARNING listing the missing
    microstructure columns and a distinct default output name
    ({symbol}_base_features_1m.parquet). The hard fail exists because
    silently producing a "master" table without the microstructure features
    was a real bug: the tables generated on 2026-10-20 were missing
    vwap_disparity / price_robust_zscore / rv / sweep_depth / qty_* /
    trade_count / dt_* / semi_var_ratio without any warning. A missing SPOT
    aggTrades only warns: um is the primary market and the table stays
    useful without the spot_agg_* columns and rv_dominance.
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

from binance_vision.build_aggtrade_features import FEATURE_COLS, compute_aggtrade_features  # noqa: E402

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


def _join_agg_features(master_lf: pl.LazyFrame, df_agg, prefix: str,
                       timeframe: str) -> pl.LazyFrame:
    """把一个市场的 aggTrades 微观特征 join 到 master（列加 prefix 防冲突）。

    df_agg 既可以是逐笔原始 parquet（LazyFrame/DataFrame，现场算特征，
    shift(1) 跨文件全局、日界精确），也可以是已预聚合的特征表
    （build_aggtrade_features.process_file 的产物：timestamp + close +
    FEATURE_COLS）——按是否含 transact_time 列区分。预聚合路径是磁盘
    吃紧时的正路（原始 aggTrades 可删，详见 _scan_agg_files）。
    """
    if df_agg is None:
        return master_lf
    lf_agg = df_agg.lazy() if isinstance(df_agg, pl.DataFrame) else df_agg
    cols = lf_agg.collect_schema().names()
    if "transact_time" in cols:
        prepared = compute_aggtrade_features(lf_agg, timeframe=timeframe)
    else:
        prepared = lf_agg
    prepared = prepared.with_columns(
        pl.col("timestamp").dt.epoch("ms").alias("close_time")
    ).drop(["timestamp", "close"])
    missing = [c for c in FEATURE_COLS if c not in prepared.collect_schema().names()]
    if missing:
        raise ValueError(f"aggTrades feature columns missing after prepare: {missing}")
    prepared = prepared.rename({c: f"{prefix}{c}" for c in FEATURE_COLS})
    return master_lf.join(prepared, on="close_time", how="left")


def build_master_feature_table_lazy(
    df_um_klines: pl.DataFrame | pl.LazyFrame,
    df_spot_klines: pl.DataFrame | pl.LazyFrame,
    df_aggtrades_um: pl.DataFrame | pl.LazyFrame | None = None,
    df_aggtrades_spot: pl.DataFrame | pl.LazyFrame | None = None,
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

    # 4. Join aggTrades microstructure features (if provided).
    #    两个市场分开算、分别加 um_agg_ / spot_agg_ 前缀；spot 缺失时 um
    #    侧仍然有效（主市场是 um，spot 缺只丢 spot_agg_* 与 rv_dominance）。
    master_lf = _join_agg_features(master_lf, df_aggtrades_um, "um_agg_", timeframe)
    master_lf = _join_agg_features(master_lf, df_aggtrades_spot, "spot_agg_", timeframe)

    # 4b. 跨市场：波动主导端。同一分钟分别用现货/合约的逐笔收益算 RV，
    #     rv_dominance = (rv_um - rv_spot) / (rv_um + rv_spot)：
    #     > 0 = 这分钟的波动由合约端造出（杠杆盘换手、连环爆仓、资金费
    #     压力，合约领价格发现）；< 0 = 源在现货的真实买卖盘。
    #     对 basis 是互补信息：basis 跳动时告诉你压力从哪端释放。
    #     仅当两个市场的 agg 特征都在时才有定义（缺 spot 则无此列）。
    if df_aggtrades_um is not None and df_aggtrades_spot is not None:
        master_lf = master_lf.with_columns(
            ((pl.col("um_agg_rv") - pl.col("spot_agg_rv"))
             / (pl.col("um_agg_rv") + pl.col("spot_agg_rv") + 1e-8)
             ).alias("rv_dominance")
        )

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
    df_aggtrades_um: pl.DataFrame | pl.LazyFrame | None = None,
    df_aggtrades_spot: pl.DataFrame | pl.LazyFrame | None = None,
    df_metrics: pl.DataFrame | pl.LazyFrame | None = None,
    timeframe: str = "1m",
) -> pl.DataFrame:
    """Eager convenience wrapper (used by tests); main() uses the lazy path."""
    return build_master_feature_table_lazy(
        df_um_klines, df_spot_klines, df_aggtrades_um, df_aggtrades_spot,
        df_metrics, timeframe
    ).collect()


def _scan_agg_files(root: Path, symbol: str, market: str) -> list[Path]:
    """一个市场的 aggTrades 输入文件：预聚合特征优先，否则逐笔原始。

    预聚合产物是 build_aggtrade_features.process_file 写出的
    `aggTrades/{symbol}/features/*_feat_1m.parquet`（~KB/天），体积只有
    原始的 ~1/1000，是磁盘吃紧时的正路（原始 aggTrades 清洗后
    ~25MB/天/市场/符号，全窗口两市场能到 100GB 量级）。存在特征文件即
    只用它，同市场的原始文件忽略——否则同一分钟既来自原始又来自特征，
    left join 会把行数翻倍。注意特征文件固定 1m 窗口，与 master 的 1m
    粒度绑定。
    """
    base = root / market
    feat = (
        sorted(base.glob(f"daily/aggTrades/{symbol}/features/*_feat_1m.parquet"))
        + sorted(base.glob(f"monthly/aggTrades/{symbol}/features/*_feat_1m.parquet"))
    )
    if feat:
        return feat
    return (
        sorted(base.glob(f"daily/aggTrades/{symbol}/*.parquet"))
        + sorted(base.glob(f"monthly/aggTrades/{symbol}/*.parquet"))
    )


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

    agg_um_files = _scan_agg_files(root, symbol, "um")
    agg_spot_files = _scan_agg_files(root, symbol, "spot")

    if args.no_agg:
        # --no-agg：显式要基础宽表。WARNING 列全将缺席的微观列，避免以后
        # 拿到表才发现缺特征。
        print("WARNING: --no-agg: aggTrades skipped; the base table will NOT "
              "contain the microstructure columns: um_agg_vwap, "
              "um_agg_vwap_disparity, um_agg_price_median, um_agg_price_mad, "
              "um_agg_price_robust_zscore, um_agg_rv, um_agg_rv_up, "
              "um_agg_rv_down, um_agg_semi_var_ratio, um_agg_dt_mean, "
              "um_agg_dt_std, the spot_agg_* equivalents, and rv_dominance. "
              "Rebuild the full table after downloading aggTrades.")
        lf_agg_um = lf_agg_spot = None
    else:
        if not agg_um_files:
            # 硬失败：没有 um aggTrades 就会静默产出缺全部微观特征的
            # “master”表，而 docstring 宣称包含它们——2026-10-20 上传的表
            # 就是这么缺的。--no-agg 是唯一逃生门（见上面的 WARNING 分支）。
            print(
                f"Error: no aggTrades parquet under "
                f"{root}/{{um,spot}}/{{daily,monthly}}/aggTrades/{symbol}/ "
                f"(raw *.parquet or features/*_feat_1m.parquet) -- refusing to "
                f"build a master table WITHOUT the microstructure features "
                f"(vwap_disparity, price_median, price_mad, price_robust_zscore, "
                f"rv, rv_up, rv_down, semi_var_ratio, dt_mean, dt_std, "
                f"rv_dominance).\n"
                f"Download aggTrades first, e.g.:\n"
                f"  python binance_vision/download_binance_vision.py --market both "
                f"--symbol {symbol} --category aggTrades --interval 1m "
                f"--granularity daily --start-date YYYY-MM-01 --end-date YYYY-MM-DD\n"
                f"(or pass --no-agg to build the BASE table without microstructure "
                f"features)"
            )
            sys.exit(1)
        lf_agg_um = pl.scan_parquet(agg_um_files)
        print(f"um aggTrades: {len(agg_um_files)} file(s) "
              f"[{agg_um_files[0].name} .. {agg_um_files[-1].name}]")
        if agg_spot_files:
            lf_agg_spot = pl.scan_parquet(agg_spot_files)
            print(f"spot aggTrades: {len(agg_spot_files)} file(s) "
                  f"[{agg_spot_files[0].name} .. {agg_spot_files[-1].name}]")
        else:
            # spot 缺只警告：um 是主市场，表在缺 spot_agg_* / rv_dominance
            # 的情况下仍然可用（下载命令见上面的报错信息）。
            print(f"WARNING: no spot aggTrades parquet under "
                  f"{root}/spot/{{daily,monthly}}/aggTrades/{symbol}/ -- "
                  f"spot_agg_* columns and rv_dominance will be absent.")
            lf_agg_spot = None

    metric_files = sorted((root / "um" / "daily" / "metrics" / symbol).glob("*.parquet"))
    if not metric_files:
        print(f"WARNING: no metrics parquet under "
              f"{root}/um/daily/metrics/{symbol}/ -- open-interest and "
              f"long/short-ratio columns will be absent.")

    # 全 lazy：scan_parquet 不把文件读进内存，sink_parquet 流式写出
    lf_um = pl.scan_parquet(um_kline_files)
    lf_spot = pl.scan_parquet(spot_kline_files)
    lf_met = pl.scan_parquet(metric_files) if metric_files else None

    master_lf = build_master_feature_table_lazy(lf_um, lf_spot, lf_agg_um,
                                                lf_agg_spot, lf_met, timeframe="1m")

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
