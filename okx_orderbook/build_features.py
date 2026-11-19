#!/usr/bin/env python3
"""OKX 1m band/mid 快照 → 订单簿特征（复用 features/ob_features.py）。

设计要点（见 docs/okx_notes.md §4）：
- **共用 bps 固定分箱**（--bin-mode bps），不用 vol8：OKX σ 尺度与币安不同，
  bps 口径才能跨所对比。产出的分箱列名仍是 b0..b4，与币安 bps 口径同构。
- 输出列全部加 `okx_` 前缀，避免与币安 ob_* 列拼接时冲突。
- 档数类特征（ob_n_* / ob_levels_ratio / ob_qmean_* / ob_qcv_* / ob_qmax_ratio_*）
  保留在文件里用于诊断，但**融合/联合训练时必须剔除**：OKX 400 档密集铺满
  ±0.3%（800 档/分钟），币安是稀疏长表，档数不可比。

用法：
  python build_features.py --band data/monthly/BTC-USDT_minute_band_2025-10.parquet \
      --mid data/monthly/BTC-USDT_minute_band_mid_2025-10.parquet \
      --out data/features/okx_BTC-USDT_2025-10.parquet
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent  # bn_hisdata/
sys.path.insert(0, str(ROOT / "features"))

from ob_features import build_l1  # noqa: E402

# 档数类列前缀：OKX 与币安不可比，融合时剔除
COUNT_PREFIXES = ("ob_n_",)
COUNT_EXACT = {
    "ob_levels_ratio", "ob_qmean_bid", "ob_qmean_ask",
    "ob_qcv_bid", "ob_qcv_ask", "ob_qmax_ratio_bid", "ob_qmax_ratio_ask",
}


def is_count_col(c: str) -> bool:
    return c.startswith(COUNT_PREFIXES) or c in COUNT_EXACT


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--band", default=os.environ.get("BAND", ""), required=False)
    ap.add_argument("--mid", default=os.environ.get("MID", ""), required=False)
    ap.add_argument("--out", default=os.environ.get("OUT", ""))
    ap.add_argument("--limit-rows", type=int, default=None)
    args = ap.parse_args()
    if not args.band or not args.mid or not args.out:
        ap.error("--band / --mid / --out (or env BAND/MID/OUT) required")

    df = build_l1(args.band, args.mid, limit_rows=args.limit_rows, bin_mode="bps")
    # 加 okx_ 前缀；close_time 保持原名以便 join
    ren = {c: f"okx_{c}" for c in df.columns if c != "close_time"}
    df = df.rename(ren)
    # 标记档数列（供下游过滤），写进 parquet schema 元数据不可行，改由列名规则识别
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out)
    count_cols = [c for c in df.columns if is_count_col(c.removeprefix("okx_"))]
    print(f"[okx_features] wrote {out} rows={df.height:,} cols={len(df.columns)}")
    print(f"[okx_features] count-type cols to DROP in fusion: {len(count_cols)} "
          f"(e.g. {count_cols[:6]})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
