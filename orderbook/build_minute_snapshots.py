"""
Build 1-minute, fixed-%width L2 order-book snapshots (as-of, no replay).

Approach (vectorized, single pass per hour file):
  1. Load one hour file (snapshot + updates).
  2. For each (side, price, minute_bucket) take the LAST quantity by event_time
     -> the book state at that minute.
  3. Forward-fill quantity within each (side, price) so a level keeps its value
     until its next change (qty=0 means removed -> excluded later).
  4. Per minute compute mid = (best_bid + best_ask)/2, then keep only levels in
     [mid*(1-band), mid*(1+band)] with qty>0.

Each hour file is self-contained (it carries its own initial snapshot), so we
process hour-by-hour and never need cross-hour state or a giant global grid.

Usage:
    python build_minute_snapshots.py --date 2026-10-12 --exchange binance_spot
"""
import argparse
import sys
import time
from pathlib import Path

import polars as pl

from ob_reconstruct import reconstruct_state, reconstruct_state_sparse

HERE = Path(__file__).resolve().parent
START = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - START:8.1f}s] {msg}", flush=True)


def process_hour(path: Path, band: float, mode: str = "dense"):
    df = pl.read_parquet(path).select(
        ["side", "price", "quantity", "event_time", "event_type"]
    )
    # single-pass as-of reconstruction (snapshots = full-book replacement)
    if mode == "sparse":
        merged = reconstruct_state_sparse(df, band=band)
    else:
        merged = reconstruct_state(df)

    # per-minute mid from best bid / best ask
    bids = merged.filter((pl.col("side") == "bid") & (pl.col("q") > 0))
    asks = merged.filter((pl.col("side") == "ask") & (pl.col("q") > 0))
    bb = bids.group_by("bucket").agg(pl.col("px").max().alias("best_bid"))
    ba = asks.group_by("bucket").agg(pl.col("px").min().alias("best_ask"))
    mb = bb.join(ba, on="bucket", how="full", coalesce=True)
    mb = mb.with_columns(
        ((pl.col("best_bid") + pl.col("best_ask")) / 2).alias("mid"),
        (pl.col("best_ask") - pl.col("best_bid")).alias("spread"),
    ).filter(pl.col("best_bid").is_not_null() & pl.col("best_ask").is_not_null())

    # clip to the fixed-% band around mid
    st = merged.join(mb.select(["bucket", "mid"]), on="bucket", how="left")
    band = st.filter(
        (pl.col("q") > 0)
        & (pl.col("px") >= pl.col("mid") * (1 - band))
        & (pl.col("px") <= pl.col("mid") * (1 + band))
    ).select(["bucket", "side", "px", "q", "mid"])

    return band, mb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--exchange", default="binance_spot")
    ap.add_argument("--band", type=float, default=0.003)
    ap.add_argument("--mode", choices=["dense", "sparse"], default="sparse")
    ap.add_argument("--dest-root", default=str(HERE / "data"))
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    root = Path(args.dest_root) / args.date / args.exchange
    files = sorted(root.glob("*/BTCUSDT_orderbook.parquet"))
    log(f"found {len(files)} hour files in {root} (mode={args.mode})")
    if not files:
        # 该天没有任何小时文件（全 404 / 下载失败 / 目录不存在）。用非零退出让
        # pipeline 感知失败，而不是让 pl.concat([]) 抛一个难懂的错。
        print(f"ERROR: no hour files under {root}; nothing to build", file=sys.stderr)
        raise SystemExit(3)

    bands, mids = [], []
    for i, f in enumerate(files):
        band, mb = process_hour(f, args.band, args.mode)
        bands.append(band)
        mids.append(mb)
        if (i + 1) % 6 == 0:
            log(f"processed {i + 1}/{len(files)} hours")

    band_all = pl.concat(bands).sort("bucket")
    mid_all = pl.concat(mids).sort("bucket")
    log(f"total band rows: {band_all.height:,} | total minute rows: {mid_all.height:,}")

    # sparse/dense 只是计算时的内存策略，产物（band+mid）schema 与内容一致，
    # 因此统一写成 {exchange}_minute_band.parquet，与 pipeline / merge_monthly 对齐，
    # 避免 sparse 产物因多带 _sparse 后缀而不被识别、无法续跑/聚合。
    out = args.out or str(
        Path(args.dest_root) / args.date / f"{args.exchange}_minute_band.parquet"
    )
    band_all.write_parquet(out)
    mid_all.write_parquet(out.replace(".parquet", "_mid.parquet"))
    log(f"wrote {out} and *_mid.parquet")

    # sanity preview
    with pl.Config(tbl_rows=5, tbl_cols=10):
        print("\n--- band sample ---")
        print(band_all.head(5))
        print("\n--- mid sample ---")
        print(mid_all.head(5))


if __name__ == "__main__":
    main()
