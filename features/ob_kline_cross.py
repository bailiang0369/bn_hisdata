#!/usr/bin/env python3
"""订单簿 × 基础 K 线 的跨域特征（成交 ↔ 挂单、振幅倍数处的不平衡）。

为什么不放在 build_ob_dataset.py 里
----------------------------------
订单簿数据的价值在于「只依赖盘口自身」：K 线更新滞后、或某个标的没有 K 线，
盘口特征照样能算完并独立迭代。只有这几列必须两边对齐：

    atk_*          主动成交量（K 线）÷ 盘口挂单量 -> 攻防关系
    ob_amp_obi_*   在「k × 近 5 分钟振幅」距离处的买卖不平衡

所以单独成一个显式 join：输入订单簿 parquet + K 线 parquet，输出跨域列。
后期在 bn_bt 里合并时，照这个口径复算即可（键都是 close_time）。

口径说明
--------
`ob_amp_obi_*` 的距离阈值来自 K 线振幅（bps），而盘口深度按 vol8（σ 倍数）分箱，
所以插值前要用 `ob_f_sigma_bps` 把 bps 阈值换算成 σ 倍数（见 `_cum`）。

用法
----
    python features/ob_kline_cross.py \
        --ob 'data/ob_dataset/ob_BTCUSDT_*.parquet' \
        --vision 'data/vision/BTCUSDT_1m_klines_*.parquet' \
        --out-dir data/obx
"""
from __future__ import annotations

import argparse
import glob
import time
from pathlib import Path

import polars as pl

T = time.time()

# L1 深度分箱：与 ob_features.py 的默认 vol8 口径对应，边界是 σ 倍数
# [0,1)[1,2)[2,4)[4,8)[8,∞) -> v0..v4。单位**不是 bps**：插值前要用 ob_f_sigma_bps
# 把目标距离换算成 σ 倍数（见 _cum）。
DEPTH_BINS = (("v0", 0.0, 1.0), ("v1", 1.0, 2.0), ("v2", 2.0, 4.0), ("v3", 4.0, 8.0))
DEPTH_TAIL = ("v4", 8.0)      # 8σ 以上：band 到 ±30bps 已基本封顶
BAND_BPS = 30.0               # band ±0.3% 的上界，振幅倍数超过它就超出快照
AMP_WIN = "5m"                # 振幅的区间长度：近 5 分钟的 high - low
AMP_MEAN_WIN = "30m"          # 对 5 分钟振幅做滚动均值的窗口
AMP_KS = (0.15, 0.35, 0.8)    # 振幅倍数：3/5 分钟的涨跌只看近端盘口，不用远档


def log(msg: str) -> None:
    print(f"[{time.time() - T:7.1f}s] {msg}", flush=True)


def _sanitize(df: pl.DataFrame) -> pl.DataFrame:
    """inf / -inf / NaN 统一置 null（与 ob_features / ob_rolling 同口径）。

    本模块的 `atk_*` 全是除法：主动买+主动卖 = 0、单侧挂单量 = 0、`Qb+Qa` = 0
    都会造出 inf，而 inf 不参与统计、不触发报错，只会静默污染下游模型。
    """
    cols = [c for c in df.columns if df[c].dtype in (pl.Float32, pl.Float64)]
    return df.with_columns([
        pl.when(pl.col(c).is_finite()).then(pl.col(c)).otherwise(None).alias(c)
        for c in cols
    ])


def _ym(col: str = "close_time") -> pl.Expr:
    """毫秒时间戳 -> 'YYYY-MM'（UTC），按 bar 起点归月（与 build_ob_dataset 一致）。

    close_time = bucket + 60000 是右端点，月末最后一分钟等于下月 1 号 00:00，
    减 1ms 还原到所属月份。
    """
    return pl.from_epoch(pl.col(col) - 1, time_unit="ms").dt.strftime("%Y-%m")


def _amplitude(v: pl.DataFrame) -> pl.DataFrame:
    """振幅 = 近 5 分钟这一整段的区间（high−low），再对其做 30m 滚动均值。

    - amp_5m_bps       = (近5分钟的 max(high) − min(low)) / close × 1e4
    - amp_mean_30m_bps = amp_5m_bps 的 30 分钟滚动均值

    K 线逐分钟无缝；滚动按日历时间（rolling 的 index_column）。
    """
    need = {"high", "low", "close"}
    if not need.issubset(v.columns):
        raise SystemExit(f"K 线缺列 {sorted(need - set(v.columns))}，无法算振幅")
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
    """累计深度：mid 到距离 x(bps) 内的挂单量，按 vol8 分箱在 σ 空间线性插值。

    v0..v4 的边界是 σ 倍数，所以先把 x 换算成 σ 倍数（x / ob_f_sigma_bps）。
    σ 缺失（上游断档/首月 warmup）时整行返回 null，下游一并置 null，不臆造深度。
    """
    xs = x / pl.col("ob_f_sigma_bps")
    tail, tail_lo = DEPTH_TAIL
    acc = pl.when(xs >= tail_lo).then(pl.col(f"ob_f_q_{side}_{tail}")).otherwise(0.0)
    for bn, lo, hi in DEPTH_BINS:
        q = pl.col(f"ob_f_q_{side}_{bn}")
        acc = (acc
               + pl.when(xs >= hi).then(q).otherwise(0.0)
               + pl.when((xs > lo) & (xs < hi)).then(q * (xs - lo) / (hi - lo))
               .otherwise(0.0))
    return pl.when(pl.col("ob_f_sigma_bps").is_null()).then(None).otherwise(acc)


def _amp_features(df: pl.DataFrame) -> pl.DataFrame:
    """振幅倍数不平衡：k × 振幅（5m 区间的 30m 滚动均值）处的买卖不平衡（futures 盘口）。

    阈值 clip 到 30bps 兜底；超出部分打 ob_amp_clip_* = 1。
    """
    amp = pl.col("amp_mean_30m_bps")
    for k in AMP_KS:
        tag = f"{round(k * 100):03d}"  # 0.15 -> 015, 0.8 -> 080
        thr = (amp * k).clip(upper_bound=BAND_BPS)
        cb, ca = _cum(thr, "bid"), _cum(thr, "ask")
        df = df.with_columns(
            pl.when((cb + ca) > 0).then((cb - ca) / (cb + ca)).otherwise(None)
            .cast(pl.Float32).alias(f"ob_amp_obi_{tag}"),
            (amp * k > BAND_BPS).cast(pl.Int8).alias(f"ob_amp_clip_{tag}"),
        )
    return df


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

    out = []
    if {"um_buy_vol", "um_sell_vol"}.issubset(df.columns):
        out += _side(pl.col("um_buy_vol"), pl.col("um_sell_vol"),
                     pl.col("ob_f_q_bid"), pl.col("ob_f_q_ask"), "atk_f")
    if {"spot_buy_vol", "spot_sell_vol", "ob_s_q_bid"}.issubset(df.columns):
        out += _side(pl.col("spot_buy_vol"), pl.col("spot_sell_vol"),
                     pl.col("ob_s_q_bid"), pl.col("ob_s_q_ask"), "atk_s")
    return df.with_columns(out) if out else df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ob", required=True, help="glob: ob_{SYMBOL}_{YYYY-MM}.parquet")
    ap.add_argument("--vision", required=True, help="glob: K 线 / base_features")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--symbol", default="BTCUSDT")
    args = ap.parse_args()

    ob_files = sorted(glob.glob(args.ob))
    if not ob_files:
        raise SystemExit(f"没有订单簿文件匹配 {args.ob}")
    vis_files = sorted(glob.glob(args.vision))
    if not vis_files:
        raise SystemExit(f"没有 K 线文件匹配 {args.vision}")

    ob = pl.concat([pl.scan_parquet(f) for f in ob_files],
                   how="vertical_relaxed").collect().sort("close_time")
    log(f"ob: {ob.height:,} rows x {ob.width} cols ({len(ob_files)} files)")

    v_lfs = [pl.scan_parquet(f) for f in vis_files]
    v_base = v_lfs[0].collect_schema().names()
    v = pl.concat([lf.select(v_base) for lf in v_lfs]).collect().sort("close_time")
    v = v.unique(subset=["close_time"], keep="last")
    log(f"vision: {v.height:,} rows x {v.width} cols ({len(vis_files)} files)")

    cols = ["close_time"]
    v_sel = v
    if "amp_mean_30m_bps" in v.columns:
        log("复用 K 线已有的 amp_mean_30m_bps")
    elif {"high", "low", "close"}.issubset(v.columns):
        log("从 high/low/close 计算振幅")
        # 只追加振幅列，K 线原有的成交量列要留着（atk_* 要用）
        v_sel = _amplitude(v).select(["close_time", "amp_mean_30m_bps"] + [
            c for c in v.columns if c != "close_time"])
    else:
        raise SystemExit("K 线既没有 amp_mean_30m_bps 也没有 high/low/close，无法算振幅")
    for c in ("amp_mean_30m_bps", "um_buy_vol", "um_sell_vol",
              "spot_buy_vol", "spot_sell_vol"):
        if c in v_sel.columns:
            cols.append(c)
    if len(cols) == 1:
        raise SystemExit("K 线里没有可用的成交量/振幅列")

    # 以订单簿为左表：盘口是主时序，K 线缺分钟只会让跨域列变 null
    df = ob.join(v_sel.select(cols), on="close_time", how="left")
    missing = df.height - df["amp_mean_30m_bps"].is_not_null().sum() \
        if "amp_mean_30m_bps" in df.columns else df.height
    log(f"joined: {df.height:,} rows, K 线覆盖缺口 {missing:,} 行")

    df = _amp_features(df) if "amp_mean_30m_bps" in df.columns else df
    df = _atkdef_features(df)
    df = _sanitize(df)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    df = df.with_columns(_ym().alias("_month"))
    months = sorted(df["_month"].unique().to_list())
    for m in months:
        sub = df.filter(pl.col("_month") == m).drop("_month").sort("close_time")
        fp = out_dir / f"obx_{args.symbol}_{m}.parquet"
        sub.write_parquet(fp)
        log(f"obx {m}: {fp.name} {sub.height:,} rows x {sub.width} cols")
    full = out_dir / f"obx_{args.symbol}.parquet"
    df.drop("_month").sort("close_time").write_parquet(full, compression="zstd")
    log(f"obx full: {full.name} {full.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
