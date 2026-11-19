#!/usr/bin/env python3
"""Kaggle runner：下载 OKX 单日 L2 归档 → 重建 1m 快照 → 打印统计。

env: DATE, INST_ID(默认BTC-USDT), BAND(默认0.003), DEST_ROOT
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent  # bn_hisdata/
sys.path.insert(0, str(ROOT / "okx_orderbook"))

from download_day import main as download_main
from build_minute_snapshots import main as build_main

T = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - T:7.1f}s] {msg}", flush=True)


def main() -> None:
    date = os.environ.get("DATE", "")
    if not date:
        raise SystemExit("env DATE required")
    dest = os.environ.get("DEST_ROOT", "/kaggle/working/okx_data")
    os.environ["DEST_ROOT"] = dest
    log(f"DATE={date} DEST={dest}")

    sys.argv = ["download_day.py"]
    rc = download_main()
    if rc != 0:
        raise SystemExit(f"download failed rc={rc}")

    sys.argv = ["build_minute_snapshots.py"]
    rc = build_main()
    if rc != 0:
        raise SystemExit(f"build failed rc={rc}")

    # 打印产物统计
    import polars as pl
    mp = Path(dest) / "monthly" / f"{os.environ.get('INST_ID', 'BTC-USDT')}_minute_band_mid_{date[:7]}.parquet"
    bp = Path(dest) / "monthly" / f"{os.environ.get('INST_ID', 'BTC-USDT')}_minute_band_{date[:7]}.parquet"
    for p in (mp, bp):
        if p.exists():
            df = pl.read_parquet(p)
            log(f"{p.name}: {df.height:,} rows, cols={df.columns}")
            log(f"  head: {df.head(2).to_dicts()}")
    log("DONE")


if __name__ == "__main__":
    main()
