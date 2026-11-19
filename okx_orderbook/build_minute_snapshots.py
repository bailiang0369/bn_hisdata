#!/usr/bin/env python3
"""OKX L2 NDJSON 归档 → 1m 固定带宽（mid ±0.3%）订单簿快照。

与币安 orderbook/build_minute_snapshots.py 输出同构：
  - band: (bucket, side, px, q, mid)  稀疏长表，只保留 mid±band 内 q>0 的档
  - mid : (bucket, best_bid, best_ask, mid, spread)  每分钟一行

实时重建逻辑（逐事件，6.6M 行/天）：
  - snapshot 事件：清空该侧 book，载入全部 [px,qty,count]（qty=0 跳过）
  - update 事件：逐档 apply，qty=0 删除该档，否则覆盖
  - 每分钟第一次事件到达时（bucket 变化），先把**上一分钟的终态**写成快照，
    这样每个 bucket 的 mid 是"该分钟最后一刻的盘口"，与币安 close_time 语义对齐。

输入/输出（data/ 被 gitignore）：
  in : data/{date}/{instId}/{name}.tar.gz（download_day.py 产物）
  out: data/monthly/{instId}_minute_band_{YYYY-MM}.parquet
       data/monthly/{instId}_minute_band_mid_{YYYY-MM}.parquet
  与币安 merge_monthly.py 命名对齐，只是前缀用 instId 区分。

用法：
  python build_minute_snapshots.py --date 2025-10-01 [--band 0.003] [--inst-id BTC-USDT]
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import tarfile
import time
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
START = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - START:8.1f}s] {msg}", flush=True)


def build_day(tar_path: Path, band: float) -> tuple[pl.DataFrame, pl.DataFrame]:
    """流式重建单日，返回 (band_df, mid_df)。"""
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    cur_bucket: int | None = None
    band_rows: list[tuple[int, str, float, float, float]] = []
    mid_rows: list[tuple[int, float, float, float, float]] = []

    def flush(bucket: int) -> None:
        """把当前 book 在该 bucket 的终态写成一行 mid + 若干 band 档。"""
        bb = max((p for p, q in bids.items() if q > 0), default=None)
        ba = min((p for p, q in asks.items() if q > 0), default=None)
        if bb is None or ba is None:
            return
        mid = (bb + ba) / 2.0
        mid_rows.append((bucket, bb, ba, mid, ba - bb))
        lo, hi = mid * (1 - band), mid * (1 + band)
        for side, book in (("bid", bids), ("ask", asks)):
            for p, q in book.items():
                if q > 0 and lo <= p <= hi:
                    band_rows.append((bucket, side, p, q, mid))

    with tarfile.open(tar_path, mode="r:*") as tf:
        members = [m for m in tf.getmembers() if m.isfile()]
        if not members:
            raise RuntimeError(f"no file in {tar_path}")
        f = tf.extractfile(members[0])
        if f is None:
            raise RuntimeError(f"cannot extract {members[0].name}")
        n = 0
        for raw in f:
            n += 1
            obj = json.loads(raw)
            ts = int(obj["ts"])
            bucket = (ts // 60_000) * 60_000
            if cur_bucket is None:
                cur_bucket = bucket
            elif bucket != cur_bucket:
                # 新分钟到达：先把上一分钟终态落盘
                flush(cur_bucket)
                cur_bucket = bucket
            action = obj["action"]
            for side, key in (("bid", "bids"), ("ask", "asks")):
                book = bids if side == "bid" else asks
                levels = obj.get(key)
                if not levels:
                    continue
                if action == "snapshot":
                    book.clear()
                for arr in levels:
                    p = float(arr[0])
                    q = float(arr[1])
                    if q == 0:
                        book.pop(p, None)
                    else:
                        book[p] = q
            if n % 1_000_000 == 0:
                log(f"  {n:,} events | band rows {len(band_rows):,}")
        if cur_bucket is not None:
            flush(cur_bucket)
        log(f"  total {n:,} events | band {len(band_rows):,} | mid {len(mid_rows):,}")

    band_df = pl.DataFrame(band_rows, schema=["bucket", "side", "px", "q", "mid"],
                           orient="row").sort(["bucket", "side", "px"])
    mid_df = pl.DataFrame(mid_rows,
                          schema=["bucket", "best_bid", "best_ask", "mid", "spread"],
                          orient="row").sort("bucket")
    return band_df, mid_df


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=os.environ.get("DATE", ""))
    ap.add_argument("--inst-id", default=os.environ.get("INST_ID", "BTC-USDT"))
    ap.add_argument("--band", type=float, default=0.003)
    ap.add_argument("--dest-root", default=os.environ.get("DEST_ROOT", str(HERE / "data")))
    ap.add_argument("--out-root", default=os.environ.get("OUT_ROOT", ""))
    args = ap.parse_args()
    if not args.date:
        ap.error("--date or env DATE required")

    src_dir = Path(args.dest_root) / args.date / args.inst_id
    tars = sorted(src_dir.glob("*L2orderbook-400lv-*.tar.gz"))
    if not tars:
        print(f"ERROR: no L2 tar.gz under {src_dir}", file=sys.stderr)
        return 3
    log(f"found {len(tars)} archive(s) in {src_dir}")

    out_root = Path(args.out_root or (Path(args.dest_root) / "monthly"))
    out_root.mkdir(parents=True, exist_ok=True)
    ym = args.date[:7]
    band_all, mid_all = [], []
    for t in tars:
        log(f"processing {t.name}")
        b, m = build_day(t, args.band)
        band_all.append(b)
        mid_all.append(m)

    band = pl.concat(band_all).sort(["bucket", "side", "px"])
    mid = pl.concat(mid_all).sort("bucket")
    bp = out_root / f"{args.inst_id}_minute_band_{ym}.parquet"
    mp = out_root / f"{args.inst_id}_minute_band_mid_{ym}.parquet"
    band.write_parquet(bp)
    mid.write_parquet(mp)
    log(f"wrote {bp} ({band.height:,} rows), {mp} ({mid.height:,} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
