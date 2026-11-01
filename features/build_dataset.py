#!/usr/bin/env python3
"""合并订单簿特征（futures + spot）与 vision-base，产出月度 merged 宽表 + 校验门禁。

数据流
------
vision-base  {SYMBOL}_base_features_{YYYY-MM}.parquet   (close_time, 19 列)
ob L1        ob_features_{exchange}_{YYYY-MM}.parquet   (ob_features.py 产物)
                    │ 全窗口拼接 -> reindex -> 滚动（跨月无边界截断）
                    ▼
              ob_f_* / ob_s_* 前缀列 + 跨市场特征 ob_basis_bps 等
                    │ 以 vision 的 close_time 为左表 left join
                    ▼
merged_BTCUSDT_{YYYY-MM}.parquet

时间与对齐
----------
主键 close_time（Int64 毫秒 UTC），与 vision-base 完全同键。ob 侧缺的分钟在
merged 里保留为一行（vision 有该分钟），ob_* 列置 null 并带 ob_missing=1。
滚动在「全窗口拼接」的 ob 序列上做，跨月边界窗口不会截断（单月文件只含上月
最后一分钟的 warmup，15m 窗口远远不够）。

校验门禁（写盘前，不通过即退出）
--------------------------------
1. 主键：close_time 严格 60s 递增、无重复（对齐错位的硬证据）
   （覆盖率只报告不拦截：ob_mid 缺失是源端断流/单边有量/上游断档的正常结果，
    见 feature_spec §7。报告字段 ob_coverage / ob_missing / ob_missing_days）
2. 基差：futures/spot 两边 mid 偏差 >1% 的行占比 < 0.5%（防止某边接错文件/错位；
   极端行情短暂脱钩会产生个位数真实离群行，实测 2025-10 有 11 行 ≈0.02%，不拦）

用法
----
    python features/build_dataset.py \
        --vision 'data/vision/BTCUSDT_base_features_*.parquet' \
        --fut 'data/ob/ob_features_binance_futures_*.parquet' \
        --spot 'data/ob/ob_features_binance_spot_*.parquet' \
        --out-dir data/merged
"""
from __future__ import annotations

import argparse
import glob
import re
import time
from pathlib import Path

import polars as pl

from ob_rolling import build_l2, _reindex, _sanitize

T = time.time()
MONTH_RE = re.compile(r"(\d{4}-\d{2})")

# L1 深度分箱（bps）：与 ob_features.py 的 BIN_EDGES 对应 [0,2)[2,5)[5,10)[10,20)[20,30]
DEPTH_BINS = (("b0", 0.0, 2.0), ("b1", 2.0, 5.0), ("b2", 5.0, 10.0),
              ("b3", 10.0, 20.0), ("b4", 20.0, 30.0))
BAND_BPS = 30.0      # band ±0.3% 的上界，振幅倍数超过它就超出快照
AMP_WIN = "5m"       # 振幅的区间长度：近 5 分钟的 high - low
AMP_MEAN_WIN = "30m"  # 对 5 分钟振幅做滚动均值的窗口


def log(msg: str) -> None:
    print(f"[{time.time() - T:7.1f}s] {msg}", flush=True)


def _month(f: str) -> str:
    m = MONTH_RE.search(Path(f).name)
    if not m:
        raise SystemExit(f"文件名里没有 YYYY-MM: {f}")
    return m.group(1)


def _prefix(df: pl.DataFrame, prefix: str) -> pl.DataFrame:
    """ob_xxx -> ob_f_xxx / ob_s_xxx，避免两侧同名列撞车。"""
    ren = {c: c.replace("ob_", prefix, 1) for c in df.columns if c != "close_time"}
    return df.rename(ren)


def load_rolled(l1_glob: str, prefix: str) -> pl.DataFrame:
    """全窗口 L1 拼接 -> reindex -> 滚动 -> 加前缀。"""
    files = sorted(glob.glob(l1_glob))
    if not files:
        raise SystemExit(f"没有 L1 文件匹配 {l1_glob}")
    log(f"{prefix} L1 files: {len(files)}")
    lfs = [pl.scan_parquet(f) for f in files]
    # 各月 L1 的列顺序可能不同（极端行情缺某个价位箱时，补的 null 列被排到末尾）。
    # lazy 下 vertical_relaxed 在 collect 时仍会报 "schema names differ"，
    # 所以显式按首文件的列顺序 select 对齐，再拼接，最稳。
    base_cols = lfs[0].collect_schema().names()
    lfs = [lf.select(base_cols) for lf in lfs]
    df = pl.concat(lfs).collect().sort("close_time")
    # 月度 L1 在边界重叠（每月文件含上月最后一分钟），按 close_time 去重
    df = df.unique(subset=["close_time"], keep="last")
    df = _reindex(df)
    log(f"{prefix} after reindex: {df.height:,} rows, missing={df['ob_missing'].sum():,}")
    df = build_l2(df)
    return _prefix(df, prefix)


def _cross(fut: pl.DataFrame, spot: pl.DataFrame) -> pl.DataFrame:
    """跨市场特征（以 futures 左表，spot 可能缺月）。"""
    m = fut.join(spot, on="close_time", how="left")
    return m.with_columns(
        ((pl.col("ob_f_mid") - pl.col("ob_s_mid")) / pl.col("ob_s_mid") * 1e4)
        .alias("ob_basis_bps"),
        ((pl.col("ob_f_q_bid") + pl.col("ob_f_q_ask"))
         / (pl.col("ob_s_q_bid") + pl.col("ob_s_q_ask"))).alias("ob_depth_ratio_f_s"),
        (pl.col("ob_f_spread_bps") / pl.col("ob_s_spread_bps")).alias("ob_spread_ratio_f_s"),
        (pl.col("ob_f_ret_1m_bps") - pl.col("ob_s_ret_1m_bps")).alias("ob_lead_f_s_bps"),
    )


def _amplitude(v: pl.DataFrame) -> pl.DataFrame:
    """振幅 = 近 5 分钟这一整段的区间（high−low），再对其做 30m 滚动均值。

    - amp_5m_bps      = (近5分钟的 max(high) − min(low)) / close × 1e4
    - amp_mean_30m_bps = amp_5m_bps 的 30 分钟滚动均值

    vision 逐分钟无缝；滚动按日历时间（rolling 的 index_column）。
    """
    v = v.sort("close_time")
    ts = pl.from_epoch(pl.col("close_time"), time_unit="ms")

    rng = v.with_columns(ts.alias("_ts")).rolling(index_column="_ts", period=AMP_WIN).agg(
        (pl.col("high").max() - pl.col("low").min()).alias("_amp5")
    ).drop("_ts")
    v = pl.concat([v, rng], how="horizontal")
    v = v.with_columns((pl.col("_amp5") / pl.col("close") * 1e4).alias("amp_5m_bps"))

    mean = v.with_columns(ts.alias("_ts")).rolling(index_column="_ts",
                                                    period=AMP_MEAN_WIN).agg(
        pl.col("amp_5m_bps").mean().alias("amp_mean_30m_bps")
    ).drop("_ts")
    return pl.concat([v, mean], how="horizontal").drop("_amp5")


def _cum(x: pl.Expr, side: str) -> pl.Expr:
    """累计深度：mid 到距离 x(bps) 内的挂单量，按 L1 固定分箱线性插值。

    返回 futures 侧的累计深度表达式（side = 'bid' | 'ask'）。
    """
    acc = pl.lit(0.0)
    for bn, lo, hi in DEPTH_BINS:
        q = pl.col(f"ob_f_q_{side}_{bn}")
        acc = (acc
               + pl.when(x >= hi).then(q).otherwise(0.0)
               + pl.when((x > lo) & (x < hi)).then(q * (x - lo) / (hi - lo))
               .otherwise(0.0))
    return acc


def _atkdef_features(df: pl.DataFrame) -> pl.DataFrame:
    """攻防特征：主动成交（flow，攻）vs 当前挂单快照（stock，守）。

    时序：当前快照 + 近 1 分钟主动成交，同 close_time 对齐（都是"刚结束这一分钟"），
    预测未来，不用历史快照。

    逻辑（弱参考，假设主动买卖力量持续）：攻方 >> 守方 -> 可能突破、出大振幅；
    攻方 << 守方 -> 突破无望，方向信号弱。现货/合约结论常不一致，两侧都算、综合看。
    """
    def _side(buy, sell, qb, qa, pfx):
        return [
            ((buy - sell) / (buy + sell)).alias(f"{pfx}_imb"),
            ((buy - sell) / (buy + sell) - (qb - qa) / (qb + qa)).alias(f"{pfx}_def_gap"),
            (buy / qa).alias(f"{pfx}_absorb_buy"),
            (sell / qb).alias(f"{pfx}_absorb_sell"),
        ]

    cols = _side(pl.col("um_buy_vol"), pl.col("um_sell_vol"),
                 pl.col("ob_f_q_bid"), pl.col("ob_f_q_ask"), "atk_f")
    if "ob_s_q_bid" in df.columns:
        cols += _side(pl.col("spot_buy_vol"), pl.col("spot_sell_vol"),
                      pl.col("ob_s_q_bid"), pl.col("ob_s_q_ask"), "atk_s")
    return df.with_columns(cols)


def _amp_features(df: pl.DataFrame, ks: tuple[float, ...] = (0.15, 0.35, 0.8)) -> pl.DataFrame:
    """振幅倍数不平衡：k × 振幅（5m 区间的 30m 滚动均值）处的买卖不平衡（futures 盘口）。

    倍数取小数（0.15/0.35/0.8）：3/5 分钟的涨跌只看近端订单簿，不用远档。
    阈值仍 clip 到 30bps 兜底；超出部分打 ob_amp_clip_* = 1。
    """
    amp = pl.col("amp_mean_30m_bps")
    for k in ks:
        tag = f"{round(k * 100):03d}"  # 0.15 -> 015, 0.8 -> 080
        thr = (amp * k).clip(upper_bound=BAND_BPS)
        cb, ca = _cum(thr, "bid"), _cum(thr, "ask")
        df = df.with_columns(
            pl.when((cb + ca) > 0).then((cb - ca) / (cb + ca)).otherwise(None)
            .cast(pl.Float32).alias(f"ob_amp_obi_{tag}"),
            (amp * k > BAND_BPS).cast(pl.Int8).alias(f"ob_amp_clip_{tag}"),
        )
    return df


def validate(df: pl.DataFrame, n_vision: int) -> dict:
    """返回校验报告；不通过抛 SystemExit。"""
    rep: dict = {"vision_rows": n_vision, "merged_rows": df.height}
    assert df["close_time"].is_sorted(), "close_time 未排序"
    rep["max_gap_ms"] = int(df["close_time"].diff().drop_nulls().max())
    rep["dup_close_time"] = int(df.height - df["close_time"].n_unique())
    assert rep["max_gap_ms"] == 60_000, f"分钟缺口: {rep['max_gap_ms']}"
    assert rep["dup_close_time"] == 0, "close_time 重复"

    if "ob_f_mid" in df.columns:
        # 覆盖率只报告、不设门禁：源端断流、单边有量、上游某天 orderbook 缺失都会留下
        # ob_mid=null 的分钟（feature_spec §7，建模前统一 drop），属正常态；
        # 对齐是否错位由上面两项保证（close_time 严格 60s 无重复）与下面基差占比。
        miss = df["ob_f_mid"].is_null()
        n_miss = int(miss.sum())
        rep["ob_coverage"] = round(1 - n_miss / n_vision, 6)
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
        # 极端行情下现货/合约短暂脱钩会产生少量真实 >1% 基差（实测 2025-10 有 11 行 ≈0.02%），
        # 不能一票否决；只有占比超过 0.5%（错位/接错文件的量级）才中断。
        limit = max(50, int(n_vision * 0.005))
        assert spread_bad <= limit, f"futures/spot mid 偏差 >1% 的行过多: {spread_bad} > {limit}"
    return rep


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vision", required=True, help="glob: BTCUSDT_base_features_*.parquet")
    ap.add_argument("--fut", required=True, help="glob: ob_features_binance_futures_*.parquet")
    ap.add_argument("--spot", default=None, help="glob: ob_features_binance_spot_*.parquet")
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    vision_files = sorted(glob.glob(args.vision))
    if not vision_files:
        raise SystemExit(f"没有 vision 文件匹配 {args.vision}")
    log(f"vision files: {len(vision_files)}")

    v_lfs = [pl.scan_parquet(f) for f in vision_files]
    v_base = v_lfs[0].collect_schema().names()
    v_all = pl.concat([lf.select(v_base) for lf in v_lfs]).collect().sort("close_time")
    v_all = v_all.unique(subset=["close_time"], keep="last")
    v_all = _amplitude(v_all)  # 全窗口算振幅区间 + 30m 滚动均值，跨月不截断

    fut = load_rolled(args.fut, "ob_f_")
    spot = load_rolled(args.spot, "ob_s_") if args.spot else None
    if spot is not None:
        fut = _cross(fut, spot)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for vf in vision_files:
        m = _month(vf)
        vc = pl.read_parquet(vf).select("close_time")
        v = v_all.join(vc, on="close_time", how="semi").sort("close_time")
        out = v.join(fut, on="close_time", how="left")
        out = _amp_features(out)
        out = _atkdef_features(out)
        out = _sanitize(out)
        rep = validate(out, v.height)
        fp = out_dir / f"merged_BTCUSDT_{m}.parquet"
        out.write_parquet(fp)
        log(f"merged {m}: {fp.name} | {rep}")


if __name__ == "__main__":
    main()
