#!/usr/bin/env python3
"""订单簿特征构建：L1 截面 -> L2 滚动 -> 月度 / 全窗口 parquet。**不含任何 K 线数据**。

数据流
------
ob_features_{exchange}_{YYYY-MM}.parquet      (L1，ob_features.py 产物)
    │ 全窗口拼接 -> 去重 -> reindex 到连续分钟 -> 滚动（跨月不截断）
    ▼
ob_{SYMBOL}_{YYYY-MM}.parquet   +   ob_{SYMBOL}.parquet（全窗口单文件）

为什么和 K 线分开
-----------------
基础 K 线（OHLC / 主动买卖量）的特征在 vision 侧单独算，订单簿这边只依赖盘口
自身（futures + spot 两市场可互相比较）。跨域特征——成交 vs 挂单（atk_*）、
振幅倍数处的不平衡（ob_amp_obi_*）——需要两边按 close_time 显式对齐，
放在 `ob_kline_cross.py` 里，不在本文件里隐式引入 K 线依赖。
这样盘口特征不会被 K 线更新滞后阻塞，两套特征也能各自独立迭代/复用。

时间与对齐
----------
主键 close_time（Int64 毫秒 UTC），与 vision / bn_bt 同键同语义。缺分钟被
reindex 成 `ob_mid=null` 的行并打 `ob_missing=1`，滚动窗口按**日历时间**滑过
缺口而不是压缩行序。输出只覆盖「有 L1 的月份」，不会为空月份造全 null 数据。

校验门禁（写盘前，不通过即退出）
--------------------------------
1. 主键：close_time 严格递增、无重复；缺口必须是整分钟（非整分钟 = 对齐错位）。
   整分钟缺失（源端断流 / band 缺天）只报告 missing_minutes，不拦截。
2. 基差：futures/spot 两边 mid 偏差 >1% 的行占比 < 0.5%（防止接错文件/错位）。

用法
----
    python features/build_ob_dataset.py \
        --fut  'data/ob/ob_features_binance_futures_*.parquet' \
        --spot 'data/ob/ob_features_binance_spot_*.parquet' \
        --out-dir data/ob_dataset
"""
from __future__ import annotations

import argparse
import glob
import time
from pathlib import Path

import polars as pl

from ob_rolling import build_l2, _reindex

T = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - T:7.1f}s] {msg}", flush=True)


def _ym(col: str = "close_time") -> pl.Expr:
    """毫秒时间戳 -> 'YYYY-MM'（UTC）。"""
    return pl.from_epoch(pl.col(col), time_unit="ms").dt.strftime("%Y-%m")


def _prefix(df: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """ob_xxx -> ob_f_xxx / ob_s_xxx，避免两侧同名列撞车。"""
    ren = {c: c.replace("ob_", prefix, 1) for c in df.columns if c != "close_time"}
    return df.rename(ren)


def load_rolled(l1_glob: str, prefix: str) -> tuple[pl.DataFrame, list[str]]:
    """全窗口 L1 拼接 -> reindex -> 滚动 -> 加前缀。

    返回 (表, 实际有 L1 的月份列表)。月份在 reindex **之前**取：reindex 会把
    min..max 之间所有分钟补齐，连没有 L1 的月份也会出现在网格里。
    """
    files = sorted(glob.glob(l1_glob))
    if not files:
        raise SystemExit(f"没有 L1 文件匹配 {l1_glob}")
    log(f"{prefix} L1 files: {len(files)}")
    lfs = [pl.scan_parquet(f) for f in files]
    # 各月 L1 的列顺序可能不同（极端行情缺某个价位箱时，补的 null 列被排到末尾）。
    # lazy 下 vertical_relaxed 在 collect 时仍会报 "schema names differ"，
    # 所以显式按首文件的列顺序 select 对齐，再拼接，最稳。
    base_cols = lfs[0].collect_schema().names()
    # 口径守卫：v0..v4 是 vol8 口径的标志列。混入 bps 口径的旧 L1 时列名对不上，
    # 会在这里直接失败，而不是等下游 feature 悄悄变成另一种单位。
    if "ob_q_bid_v0" not in base_cols:
        raise SystemExit(
            f"{prefix} L1 不是 vol8 口径（schema 里没有 ob_q_bid_v0）。\n"
            "请用 features/ob_features.py --bin-mode vol8 重算 L1。")
    lfs = [lf.select(base_cols) for lf in lfs]
    raw = pl.concat(lfs).collect().sort("close_time")
    # 月度 L1 在边界重叠（每月文件含上月最后一分钟），按 close_time 去重
    raw = raw.unique(subset=["close_time"], keep="last")

    months = sorted(set(raw.select(_ym().alias("ym"))["ym"].unique().to_list()))
    df = _reindex(raw)
    log(f"{prefix} after reindex: {df.height:,} rows, missing={df['ob_missing'].sum():,}")
    df = build_l2(df)
    return _prefix(df, prefix), months


def _cross(fut: pl.DataFrame, spot: pl.DataFrame) -> pl.DataFrame:
    """跨市场特征（以 futures 左表，spot 可能缺月）。仍然只用盘口自身的量。"""
    m = fut.join(spot, on="close_time", how="left")
    return m.with_columns(
        ((pl.col("ob_f_mid") - pl.col("ob_s_mid")) / pl.col("ob_s_mid") * 1e4)
        .alias("ob_basis_bps"),
        ((pl.col("ob_f_q_bid") + pl.col("ob_f_q_ask"))
         / (pl.col("ob_s_q_bid") + pl.col("ob_s_q_ask"))).alias("ob_depth_ratio_f_s"),
        (pl.col("ob_f_spread_bps") / pl.col("ob_s_spread_bps")).alias("ob_spread_ratio_f_s"),
        (pl.col("ob_f_ret_1m_bps") - pl.col("ob_s_ret_1m_bps")).alias("ob_lead_f_s_bps"),
    )


def validate(df: pl.DataFrame) -> dict:
    """返回校验报告；不通过抛 SystemExit。分母是 ob 自己的网格行数（不含 K 线）。"""
    rep: dict = {"rows": df.height}
    assert df["close_time"].is_sorted(), "close_time 未排序"
    gaps = df["close_time"].diff().drop_nulls()
    rep["max_gap_ms"] = int(gaps.max()) if gaps.len() else 60_000
    rep["dup_close_time"] = int(df.height - df["close_time"].n_unique())
    # 门禁只卡「非整分钟」的缺口：那是拼接/对齐错位的硬证据。
    # 整分钟缺口说明源端确实缺了这段（band 缺天、断流），属上游数据状态，
    # 报告出来但不拦截。
    assert rep["max_gap_ms"] % 60_000 == 0, \
        f"缺口不是整分钟，疑似对齐错位: {rep['max_gap_ms']}ms"
    assert rep["dup_close_time"] == 0, "close_time 重复"
    missing_min = int(((gaps - 60_000) // 60_000).sum())
    if missing_min:
        rep["missing_minutes"] = missing_min

    if "ob_f_mid" in df.columns:
        # 覆盖率只报告、不设门禁：源端断流、单边有量、band 缺天都会留下
        # ob_mid=null 的分钟（建模前统一 drop），属正常态。
        miss = df["ob_f_mid"].is_null()
        n_miss = int(miss.sum())
        rep["ob_coverage"] = round(1 - n_miss / df.height, 6)
        rep["ob_missing"] = n_miss
        if n_miss:
            rep["ob_missing_days"] = int(
                (df["close_time"].filter(miss) // 86_400_000).n_unique())
    if "ob_s_mid" in df.columns and "ob_f_mid" in df.columns:
        spread_bad = df.filter(
            pl.col("ob_f_mid").is_not_null() & pl.col("ob_s_mid").is_not_null()
            & (((pl.col("ob_f_mid") - pl.col("ob_s_mid")) / pl.col("ob_s_mid")).abs() > 0.01)
        ).height
        rep["basis_outliers"] = spread_bad
        # 极端行情下现货/合约短暂脱钩会产生少量真实 >1% 基差（实测 2025-10 有
        # 11 行 ≈0.02%），不能一票否决；只有占比超过 0.5% 才判定为错位/接错文件。
        limit = max(50, int(df.height * 0.005))
        assert spread_bad <= limit, f"futures/spot mid 偏差 >1% 的行过多: {spread_bad} > {limit}"
    return rep


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fut", required=True, help="glob: ob_features_binance_futures_*.parquet")
    ap.add_argument("--spot", default=None, help="glob: ob_features_binance_spot_*.parquet")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--symbol", default="BTCUSDT", help="输出文件名用的标的（默认 BTCUSDT）")
    args = ap.parse_args()

    fut, fut_months = load_rolled(args.fut, "ob_f_")
    spot = None
    if args.spot:
        spot, _ = load_rolled(args.spot, "ob_s_")
        fut = _cross(fut, spot)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    tagged = fut.with_columns(_ym().alias("_month"))
    # 只输出 futures 有 L1 的月份；spot 缺月的部分列保持 null（下游按需处理）
    log(f"months to write: {len(fut_months)} ({fut_months[0]} .. {fut_months[-1]})")
    for m in fut_months:
        sub = tagged.filter(pl.col("_month") == m).drop("_month").sort("close_time")
        if not sub.height:
            continue
        rep = validate(sub)
        fp = out_dir / f"ob_{args.symbol}_{m}.parquet"
        sub.write_parquet(fp)
        log(f"ob {m}: {fp.name} {sub.height:,} rows x {sub.width} cols | {rep}")

    full = tagged.drop("_month").sort("close_time")
    fp = out_dir / f"ob_{args.symbol}.parquet"
    full.write_parquet(fp, compression="zstd")
    log(f"ob full: {fp.name} {full.height:,} rows x {full.width} cols "
        f"{fp.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
