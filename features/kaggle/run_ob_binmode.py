#!/usr/bin/env python3
"""Kaggle: 同一个月、两个市场，分别用 bps / vol / vol8 分箱算 L1 特征并对比。

不写 release、不删任何东西，只产出 parquet + 一份对照统计（打到日志）。

环境变量：
    MONTH=2026-09      要跑的月份
    MARKETS=binance_futures,binance_spot
    MODES=bps,vol,vol8 要对比的分箱口径

为什么放 Kaggle：单月单市场 band 有 300MB、约 5000 万行，group_by 在本地跑不动。
"""
import math
import os
import subprocess
import sys
import time
from pathlib import Path

import polars as pl
import requests

DL = "https://github.com/bailiang0369/bn_hisdata/releases/download/data-merged"
WORK = Path("/tmp/ob_bin")
MONTH = os.environ.get("MONTH", "2026-09")
MARKETS = [m.strip() for m in
           os.environ.get("MARKETS", "binance_futures,binance_spot").split(",") if m.strip()]
MODES = [m.strip() for m in
         os.environ.get("MODES", "bps,vol,vol8").split(",") if m.strip()]
T0 = time.time()


def log(msg: str) -> None:
    print(f"[{time.time() - T0:7.1f}s] {msg}", flush=True)


def download(name: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        log(f"cached {name} ({dest.stat().st_size / 1e6:.0f}MB)")
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{DL}/{name}"
    log(f"download {name}")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
        tmp.replace(dest)
    log(f"  -> {dest.stat().st_size / 1e6:.0f}MB")
    return dest


def bins_of(df: pl.DataFrame, side: str) -> list[str]:
    return [c for c in df.columns if c.startswith(f"ob_q_{side}_")]


for mkt in MARKETS:
    band = download(f"{mkt}_minute_band_{MONTH}.parquet", WORK / f"{mkt}_band.parquet")
    mid = download(f"{mkt}_minute_band_mid_{MONTH}.parquet", WORK / f"{mkt}_mid.parquet")

    outs = {}
    for mode in MODES:
        out = WORK / f"{mkt}_{MONTH}_{mode}.parquet"
        log(f"build {mkt} mode={mode}")
        rc = subprocess.run([sys.executable, "features/ob_features.py",
                             "--band", str(band), "--mid", str(mid),
                             "--out", str(out), "--bin-mode", mode, "--force"]).returncode
        if rc != 0:
            raise SystemExit(f"ob_features failed: {mkt} {mode} rc={rc}")
        outs[mode] = pl.read_parquet(out)

    log(f"{mkt}: " + " / ".join(f"{m} {d.shape}" for m, d in outs.items()))

    print(f"\n===== {mkt} {MONTH} =====", flush=True)
    for side in ("bid", "ask"):
        for mode, df in outs.items():
            cols = bins_of(df, side)
            tot = df.select(pl.sum_horizontal([pl.col(c) for c in cols]).alias("t"))["t"]
            shares = {c: float(df[c].sum() / tot.sum()) for c in sorted(cols)}
            # 均衡度：max 越小越好（最理想 0.2）；归一化熵越接近 1 越均匀
            h = -sum(v * math.log(v) for v in shares.values() if v > 0) / math.log(len(shares))
            log(f"  {mode} {side} 桶占比: "
                + ", ".join(f"{k.split('_')[-1]}={v:.3f}" for k, v in shares.items())
                + f" | max={max(shares.values()):.3f} 熵={h:.3f}")

    base = outs["bps"]
    vol_done = False
    for mode, df in outs.items():
        if mode == "bps":
            continue
        if not vol_done and "ob_sigma_bps" in df.columns:
            s = df["ob_sigma_bps"]
            log(f"  sigma(bps): p10={s.quantile(0.1):.2f} p50={s.quantile(0.5):.2f} "
                f"p90={s.quantile(0.9):.2f} max={s.max():.2f} null={s.null_count()}")
            vol_done = True
        for col in ("ob_obi_total", "ob_obi_near", "ob_obi_far",
                    "ob_impact_imb", "ob_depth_conc_bid", "ob_wmid_bps", "ob_spread_bps"):
            if col in base.columns and col in df.columns:
                j = base.select(["close_time", col]).join(
                    df.select(["close_time", col]), on="close_time", suffix="_v")
                c = j.select(pl.corr(f"{col}", f"{col}_v")).item()
                log(f"  相关性 {col} (bps vs {mode}) = {c:.4f}")

log("done")
