#!/usr/bin/env python3
"""按天下载 OKX L2 → 重建 1m → 合并月度 band → 一次性提特征（断点续传）。

两阶段（避免 O(31²) 重复聚合）：
  阶段1：逐天 download + build。每天产物是 data/{date}/ 下的 tar.gz 和
         data/daily/{instId}_minute_band_{date}.parquet（+ _mid_）。
         已存在的天自动跳过。
  阶段2：concat 所有天 → data/monthly/{instId}_minute_band_{YYYY-MM}.parquet，
         再调用 build_features 输出 data/features/okx_{instId}_{YYYY-MM}.parquet。

用法（Kaggle）：
  python pipeline_month.py --month 2025-10 [--dest-root /kaggle/working/okx_data]
"""
from __future__ import annotations

import argparse
import calendar
import os
import sys
import time
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(ROOT / "okx_orderbook"))
sys.path.insert(0, str(ROOT / "features"))

from download_day import main as download_main
from build_minute_snapshots import main as build_main
from build_features import main as features_main

T = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - T:7.1f}s] {msg}", flush=True)


def stage1_day(date: str, inst: str, dest: str) -> bool:
    """下载+重建单日，产物落到 daily/，成功返回 True。"""
    daily = Path(dest) / "daily"
    daily.mkdir(parents=True, exist_ok=True)
    bp = daily / f"{inst}_minute_band_{date}.parquet"
    mp = daily / f"{inst}_minute_band_mid_{date}.parquet"
    if bp.exists() and mp.exists():
        log(f"  skip {date}")
        return True

    os.environ["DATE"] = date
    os.environ["DEST_ROOT"] = dest
    os.environ["INST_ID"] = inst
    sys.argv = ["download_day.py"]
    if download_main() != 0:
        return False
    # build_minute_snapshots 把产物写到 monthly/，名字带月份；先删掉旧的避免混入
    monthly = Path(dest) / "monthly"
    mb = monthly / f"{inst}_minute_band_{date[:7]}.parquet"
    mm = monthly / f"{inst}_minute_band_mid_{date[:7]}.parquet"
    for p in (mb, mm):
        if p.exists():
            p.unlink()
    sys.argv = ["build_minute_snapshots.py"]
    if build_main() != 0:
        return False
    if not (mb.exists() and mm.exists()):
        return False
    mb.rename(bp)
    mm.rename(mp)
    return True


def stage2_month(month: str, inst: str, dest: str) -> bool:
    daily = Path(dest) / "daily"
    band_files = sorted(daily.glob(f"{inst}_minute_band_{month}-*.parquet"))
    mid_files = sorted(daily.glob(f"{inst}_minute_band_mid_{month}-*.parquet"))
    if not band_files or len(band_files) != len(mid_files):
        log(f"  band/mid file count mismatch: {len(band_files)}/{len(mid_files)}")
        return False
    monthly = Path(dest) / "monthly"
    monthly.mkdir(parents=True, exist_ok=True)
    bp = monthly / f"{inst}_minute_band_{month}.parquet"
    mp = monthly / f"{inst}_minute_band_mid_{month}.parquet"
    band = pl.concat([pl.read_parquet(p) for p in band_files]).sort(["bucket", "side", "px"])
    mid = pl.concat([pl.read_parquet(p) for p in mid_files]).unique(
        subset="bucket", keep="any").sort("bucket")
    band.write_parquet(bp)
    mid.write_parquet(mp)
    log(f"  monthly band={band.height:,} rows, mid={mid.height:,} rows")

    feat_dir = Path(dest) / "features"
    feat_dir.mkdir(parents=True, exist_ok=True)
    fp = feat_dir / f"okx_{inst}_{month}.parquet"
    os.environ["BAND"] = str(bp)
    os.environ["MID"] = str(mp)
    os.environ["OUT"] = str(fp)
    sys.argv = ["build_features.py"]
    return features_main() == 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", default=os.environ.get("MONTH", ""))
    ap.add_argument("--dest-root", default=os.environ.get("DEST_ROOT", "/kaggle/working/okx_data"))
    ap.add_argument("--inst-id", default=os.environ.get("INST_ID", "BTC-USDT"))
    args = ap.parse_args()
    if not args.month:
        ap.error("--month or env MONTH required (YYYY-MM)")
    y, m = map(int, args.month.split("-"))
    ndays = calendar.monthrange(y, m)[1]

    log(f"=== stage 1: download+rebuild {args.month} ({ndays} days)")
    ok = fail = 0
    for d in range(1, ndays + 1):
        date = f"{y:04d}-{m:02d}-{d:02d}"
        try:
            if stage1_day(date, args.inst_id, args.dest_root):
                ok += 1
            else:
                fail += 1
                log(f"  FAIL {date}")
        except Exception as e:
            log(f"  FAIL {date}: {type(e).__name__}: {e}")
            fail += 1
        if (d % 5 == 0) or d == ndays:
            log(f"  progress {d}/{ndays} ok={ok} fail={fail}")
    log(f"stage 1 done: ok={ok} fail={fail}")
    if fail:
        log(f"{fail} days failed; skip stage 2")
        return 1

    log("=== stage 2: merge + features")
    if not stage2_month(args.month, args.inst_id, args.dest_root):
        log("stage 2 FAILED")
        return 2
    log(f"DONE month={args.month}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
