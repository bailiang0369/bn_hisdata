#!/usr/bin/env python3
"""从月度 band(+mid) 快照计算分钟级 L2 订单簿截面特征（L1）。

输入（`data-merged` release 的月度 parquet）
------------------------------------------
band : bucket(Int64) side(str) px(Float64) q(Float32) mid(Float64)
    每分钟 × 每档一行，仅保留 mid ±0.3% 内的档位。
mid  : bucket(Int64) best_bid best_ask mid spread（全部 Float64）

输出
----
主键 `close_time = bucket + 60_000`（Int64 毫秒），与 `vision-base` 的
`close_time` 同键同语义：

    bucket = t 的内容是「第 t 分钟内最后一条消息生效后的盘口」，而第 t 分钟内
    最后一条消息之后直到 t+60_000 不再有消息，所以该状态一直持续到分钟边界，
    即 bucket=t 就是 K 线 bar [t, t+60000) **收盘时刻**的盘口。因此
    close_time = bucket + 60000 是同一瞬间的右闭标签。

消费端须知（生产端未修的两处 quirk，必须在这里兜掉）
-------------------------------------------------
1. 重复分钟：相邻小时文件在每小时的第 59 分钟重叠，同一分钟被写两遍
   （实测 2026-09 futures：43891 行 -> 43172 个 bucket）。重复行内容基本相同
   （单日仅 ~1.7% 的档位 q 有 <1% 差异），故按 (bucket, side, px) 去重即可。
2. 缺分钟：源端断流 / 该分钟只有单边有量，月度文件里整分钟消失
   （2026-09 futures 缺 29 分钟、spot 缺 58 分钟）。本模块**不补洞**：
   输出就是实际存在的分钟，reindex 与滚动窗口交给 `ob_rolling.py`。

特征口径
--------
所有距离统一为**相对 mid 的 bps**（`d = |px-mid|/mid*1e4`），所有量都是 BTC 枚数。
这样跨交易所（futures tick 0.1 / spot tick 0.01，档位数差一个量级）和跨价位都可比。
深度按 0-2 / 2-5 / 5-10 / 10-20 / 20-30 bps 分箱（b0..b4）。

计算结构（避免把原始档位表展开成宽表）
--------------------------------------
150M 行 --group_by(bucket, side, bin)--> ~430K 行 --pivot--> 43K 行宽表。
唯一的重活是那一次 group_by，其余都在小表上做。

用法
----
    python features/ob_features.py \
        --band data/ob/binance_futures_minute_band_2026-09.parquet \
        --mid  data/ob/binance_futures_minute_band_mid_2026-09.parquet \
        --out  data/ob/ob_features_binance_futures_2026-09.parquet
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import polars as pl

# 分箱上界（bps）：[0,2) [2,5) [5,10) [10,20) [20,30]
BIN_EDGES = (2.0, 5.0, 10.0, 20.0, 30.0)
BIN_NAMES = ("b0", "b1", "b2", "b3", "b4")
BIN_IDX = {str(i): n for i, n in enumerate(BIN_NAMES)}
NEAR = ("b0", "b1")          # 0-5 bps
UP_TO_10 = ("b0", "b1", "b2")  # 0-10 bps
FAR = ("b3", "b4")           # 10-30 bps
T = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - T:7.1f}s] {msg}", flush=True)


def _sanitize(df: pl.DataFrame) -> pl.DataFrame:
    """把 inf / -inf / NaN 统一置 null，避免除零产生的非常数静默污染下游模型。"""
    cols = [c for c in df.columns if df[c].dtype in (pl.Float32, pl.Float64)]
    return df.with_columns([
        pl.when(pl.col(c).is_finite()).then(pl.col(c)).otherwise(None).alias(c)
        for c in cols
    ])


def _bin_expr(d: pl.Expr) -> pl.Expr:
    """距离(bps) -> 分箱索引 0..4；band 边界是 30bps，越界一律归到最后一箱。"""
    e = pl.lit(len(BIN_EDGES) - 1, dtype=pl.Int8)
    for i, edge in reversed(list(enumerate(BIN_EDGES))):
        e = pl.when(d < edge).then(pl.lit(i, dtype=pl.Int8)).otherwise(e)
    return e


def _prepare(band: pl.LazyFrame) -> pl.LazyFrame:
    """去重 + 逐行派生 sc / d(bps) / bin / key。"""
    return (
        band.unique(subset=["bucket", "side", "px"], keep="any")
        .with_columns(
            pl.when(pl.col("side") == "ask").then(1).otherwise(0)
            .cast(pl.Int8).alias("sc"),
            (((pl.col("px") - pl.col("mid")) / pl.col("mid")) * 1e4).abs().alias("d"),
        )
        .with_columns(_bin_expr(pl.col("d")).alias("b"))
        .with_columns(
            (pl.col("sc") * 8 + pl.col("b")).cast(pl.Int16).alias("key"),
        )
    )


def _bin_agg(band: pl.LazyFrame) -> pl.DataFrame:
    """唯一的重活：150M 行 -> ~430K 行。流式引擎优先，不支持时退回内存引擎。"""
    agg = _prepare(band).group_by(["bucket", "key"]).agg(
        pl.col("q").sum().alias("q_sum"),
        pl.len().cast(pl.Int32).alias("n"),
        (pl.col("q") * pl.col("px")).sum().alias("qx"),
        pl.col("q").max().alias("q_max"),
        (pl.col("q") ** 2).sum().alias("q2"),
        pl.col("d").sort_by(pl.col("q"), descending=True).first().alias("d_at_maxq"),
        pl.col("mid").first().alias("mid"),
    )
    last = None
    for engine in ("streaming", "in-memory"):
        try:
            return agg.collect(engine=engine)
        except Exception as exc:  # noqa: BLE001 - 流式算子缺失时退回内存引擎
            last = exc
            log(f"collect(engine={engine}) failed: {type(exc).__name__}")
    raise SystemExit(f"cannot collect bin aggregation: {last}")


def _side_totals(bin_agg: pl.DataFrame) -> pl.DataFrame:
    """分箱聚合 -> (bucket, side) 总量。"""
    return (
        bin_agg.with_columns((pl.col("key") // 8).cast(pl.Int8).alias("sc"))
        .group_by(["bucket", "sc"])
        .agg(
            pl.col("q_sum").sum().alias("q"),
            pl.col("n").sum().alias("n"),
            pl.col("qx").sum().alias("qx"),
            pl.col("q2").sum().alias("q2"),
            pl.col("q_max").max().alias("q_max"),
            pl.col("d_at_maxq").sort_by(pl.col("q_max"), descending=True)
            .first().alias("q_max_dist_bps"),
            pl.col("mid").first().alias("mid"),
        )
    )


def _pivot_bins(bin_agg: pl.DataFrame) -> pl.DataFrame:
    """分箱量 / 档数 / 分箱 VWAP(相对 mid 的 bps) 转宽表。

    pivot 输出列名是 `{value}_{key}`，这里显式改名成 `ob_q_bid_b0` 这类，
    不依赖隐含规则。某分钟某箱为空 -> 量与档数补 0，VWAP 保持 null（真没深度）。
    """
    wide = (
        bin_agg.with_columns(
            pl.when(pl.col("key") // 8 == 0).then(pl.lit("bid"))
            .otherwise(pl.lit("ask")).alias("side"),
            ((pl.col("qx") / pl.col("q_sum") - pl.col("mid")) / pl.col("mid") * 1e4)
            .alias("vwap_bps"),
        )
        .with_columns(
            (pl.col("side") + "_" + (pl.col("key") % 8).cast(pl.Utf8)
             .replace_strict(BIN_IDX)).alias("k")
        )
        .pivot(on="k", index="bucket", values=["q_sum", "n", "vwap_bps"])
    )
    ren = {}
    for s in ("bid", "ask"):
        for bn in BIN_NAMES:
            ren[f"q_sum_{s}_{bn}"] = f"ob_q_{s}_{bn}"
            ren[f"n_{s}_{bn}"] = f"ob_n_{s}_{bn}"
            ren[f"vwap_bps_{s}_{bn}"] = f"ob_vwap_{s}_{bn}_bps"
    wide = wide.rename({k: v for k, v in ren.items() if k in wide.columns})
    missing = [v for v in ren.values() if v not in wide.columns]
    if missing:  # 整月都没有某个箱（极端行情）
        wide = wide.with_columns([pl.lit(None).alias(c) for c in missing])
    return wide.with_columns([
        pl.col(f"ob_q_{s}_{bn}").fill_null(0.0)
        for s in ("bid", "ask") for bn in BIN_NAMES
    ] + [
        pl.col(f"ob_n_{s}_{bn}").fill_null(0)
        for s in ("bid", "ask") for bn in BIN_NAMES
    ])


def build_l1(band_path: Path, mid_path: Path, limit_rows: int | None = None) -> pl.DataFrame:
    """单个 band(+mid) 文件 -> 分钟截面特征表（主键 close_time）。

    `limit_rows` 只用于本地冒烟测试：截取文件最前面的 N 行跑通逻辑，避免在
    本地对 1.5 亿行做聚合（那是 Kaggle 的活）。全量计算必须走流式引擎。
    """
    mid = (
        pl.scan_parquet(str(mid_path))
        .unique(subset=["bucket"], keep="any")
        .select("bucket", "mid", "spread")
        .collect()
    )
    band = pl.scan_parquet(str(band_path))
    if limit_rows is not None:
        band = band.head(limit_rows)

    log("aggregating bins ...")
    bin_agg = _bin_agg(band)
    log(f"bin rows: {bin_agg.height:,}")

    df = _pivot_bins(bin_agg).join(mid, on="bucket", how="left")
    for sc, s in ((0, "bid"), (1, "ask")):
        df = df.join(
            _side_totals(bin_agg).filter(pl.col("sc") == sc).select(
                "bucket",
                pl.col("q").alias(f"ob_q_{s}"),
                pl.col("n").alias(f"ob_n_{s}"),
                pl.col("qx").alias(f"_qx_{s}"),
                pl.col("q2").alias(f"_q2_{s}"),
                pl.col("q_max").alias(f"ob_qmax_{s}"),
                pl.col("q_max_dist_bps").alias(f"ob_qmax_dist_bps_{s}"),
            ),
            on="bucket", how="left",
        )

    qb, qa = pl.col("ob_q_bid"), pl.col("ob_q_ask")

    def q(side: str, bins: tuple[str, ...]) -> pl.Expr:
        return pl.sum_horizontal([pl.col(f"ob_q_{side}_{b}") for b in bins])

    df = df.with_columns(
        (pl.col("spread") / pl.col("mid") * 1e4).alias("ob_spread_bps"),
        # ---- 不平衡 ----
        ((qb - qa) / (qb + qa)).alias("ob_obi_total"),
        ((pl.col("ob_q_bid_b0") - pl.col("ob_q_ask_b0"))
         / (pl.col("ob_q_bid_b0") + pl.col("ob_q_ask_b0"))).alias("ob_obi_b0"),
        ((q("bid", NEAR) - q("ask", NEAR))
         / (q("bid", NEAR) + q("ask", NEAR))).alias("ob_obi_near"),
        ((q("bid", FAR) - q("ask", FAR))
         / (q("bid", FAR) + q("ask", FAR))).alias("ob_obi_far"),
        # ---- 价格（用 mid 锚定的 band VWAP，不用单档） ----
        (((pl.col("_qx_ask") / qa) * qb + (pl.col("_qx_bid") / qb) * qa) / (qb + qa)
         - pl.col("mid")).truediv(pl.col("mid")).mul(1e4).alias("ob_wmid_bps"),
        # ---- 冲击 / 形态 ----
        (10.0 / q("bid", UP_TO_10)).alias("ob_impact_bid_bps_per_btc"),
        (10.0 / q("ask", UP_TO_10)).alias("ob_impact_ask_bps_per_btc"),
        (q("bid", NEAR) / qb).alias("ob_depth_conc_bid"),
        (q("ask", NEAR) / qa).alias("ob_depth_conc_ask"),
        (pl.col("ob_n_bid") / pl.col("ob_n_ask")).alias("ob_levels_ratio"),
        (qb / pl.col("ob_n_bid")).alias("ob_qmean_bid"),
        (qa / pl.col("ob_n_ask")).alias("ob_qmean_ask"),
        (pl.col("_q2_bid") / qb**2).alias("ob_hhi_bid"),
        (pl.col("_q2_ask") / qa**2).alias("ob_hhi_ask"),
        (pl.col("ob_qmax_bid") * pl.col("ob_n_bid") / qb).alias("ob_qmax_ratio_bid"),
        (pl.col("ob_qmax_ask") * pl.col("ob_n_ask") / qa).alias("ob_qmax_ratio_ask"),
        ((pl.col("_q2_bid") / pl.col("ob_n_bid")
          - (qb / pl.col("ob_n_bid")) ** 2).sqrt()
         / (qb / pl.col("ob_n_bid"))).alias("ob_qcv_bid"),
        ((pl.col("_q2_ask") / pl.col("ob_n_ask")
          - (qa / pl.col("ob_n_ask")) ** 2).sqrt()
         / (qa / pl.col("ob_n_ask"))).alias("ob_qcv_ask"),
    ).with_columns(
        (pl.col("ob_obi_far") - pl.col("ob_obi_near")).alias("ob_obi_tilt"),
        ((pl.col("ob_impact_bid_bps_per_btc") - pl.col("ob_impact_ask_bps_per_btc"))
         / (pl.col("ob_impact_bid_bps_per_btc")
            + pl.col("ob_impact_ask_bps_per_btc"))).alias("ob_impact_imb"),
    )

    df = df.with_columns(
        (pl.col("bucket") + 60_000).alias("close_time"),
        pl.col("mid").alias("ob_mid"),
    ).drop(["bucket", "mid", "_qx_bid", "_qx_ask", "_q2_bid", "_q2_ask"])
    cols = ["close_time", "ob_mid"]
    cols += [c for c in df.columns if c.startswith("ob_") and c != "ob_mid"]
    df = df.select(cols).sort("close_time")
    df = df.with_columns([
        pl.col(c).cast(pl.Float32) for c in df.columns
        if c not in ("close_time", "ob_mid")
    ])
    return _sanitize(df)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--band", required=True, help="*_minute_band[_YYYY-MM].parquet")
    ap.add_argument("--mid", required=True, help="*_minute_band_mid[_YYYY-MM].parquet")
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit-rows", type=int, default=None,
                    help="只读前 N 行做本地冒烟测试；不传则全量")
    ap.add_argument("--force", action="store_true",
                    help="确认在本地跑全量（>256MB 的 band）。默认拒绝，避免占满本机")
    args = ap.parse_args()

    band_mb = Path(args.band).stat().st_size / 1e6
    if args.limit_rows is None and band_mb > 256 and not args.force:
        raise SystemExit(
            f"band 文件 {band_mb:.0f} MB，全量聚合约 1.5 亿行，会把本机 CPU/内存打满。\n"
            "本地只做冒烟：加 --limit-rows 5000000 跑前几百万行；\n"
            "全量请放到 Kaggle 内核上跑（确认过再 --force）。"
        )

    df = build_l1(Path(args.band), Path(args.mid), limit_rows=args.limit_rows)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(args.out)
    log(f"wrote {args.out}: {df.height:,} rows x {df.width} cols")

    with pl.Config(tbl_cols=10, tbl_rows=6):
        print(df.select(["close_time", "ob_spread_bps", "ob_obi_total", "ob_obi_near",
                         "ob_wmid_bps", "ob_impact_bid_bps_per_btc"]).head(6))
    nulls = {c: int(v) for c, v in df.null_count().to_dicts()[0].items() if v}
    print("null cols:", nulls)


if __name__ == "__main__":
    main()
