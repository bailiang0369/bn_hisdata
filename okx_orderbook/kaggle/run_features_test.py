#!/usr/bin/env python3
"""Kaggle 端到端验证：下载 OKX 单日 → 重建 1m → 提特征 → 打印对比统计。

env: DATE, DEST_ROOT
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parent.parent.parent  # bn_hisdata/
sys.path.insert(0, str(ROOT / "okx_orderbook"))
sys.path.insert(0, str(ROOT / "features"))

from download_day import main as download_main
from build_minute_snapshots import main as build_main
from build_features import main as features_main, is_count_col

T = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - T:7.1f}s] {msg}", flush=True)


def main() -> None:
    date = os.environ.get("DATE", "2025-10-01")
    dest = os.environ.get("DEST_ROOT", "/kaggle/working/okx_data")
    inst = os.environ.get("INST_ID", "BTC-USDT")
    os.environ["DATE"] = date
    os.environ["DEST_ROOT"] = dest
    os.environ["INST_ID"] = inst
    log(f"DATE={date} DEST={dest} INST={inst}")

    sys.argv = ["download_day.py"]
    assert download_main() == 0
    sys.argv = ["build_minute_snapshots.py"]
    assert build_main() == 0

    bp = Path(dest) / "monthly" / f"{inst}_minute_band_{date[:7]}.parquet"
    mp = Path(dest) / "monthly" / f"{inst}_minute_band_mid_{date[:7]}.parquet"
    fp = Path(dest) / "features" / f"okx_{inst}_{date[:7]}.parquet"
    os.environ["BAND"] = str(bp)
    os.environ["MID"] = str(mp)
    os.environ["OUT"] = str(fp)
    sys.argv = ["build_features.py"]
    assert features_main() == 0

    df = pl.read_parquet(fp)
    log(f"features: {df.height:,} rows x {len(df.columns)} cols")
    count = [c for c in df.columns if is_count_col(c.removeprefix("okx_"))]
    qty = [c for c in df.columns if "ob_q_" in c or "ob_obi" in c]
    log(f"  count-type cols ({len(count)}): {count[:8]}")
    log(f"  qty/obi cols ({len(qty)}): {qty[:8]}")
    # 抽样看几个关键特征分布，确认不是全 null
    for c in ["okx_ob_obi_total", "okx_ob_spread_bps", "okx_ob_q_bid_b0"]:
        if c in df.columns:
            s = df[c]
            log(f"  {c}: null={s.null_count()} mean={s.mean()} std={s.std()}")
    log("DONE")


if __name__ == "__main__":
    main()
