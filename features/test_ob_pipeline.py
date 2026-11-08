#!/usr/bin/env python3
"""features 管线的回归测试：band(+mid) -> L1 -> reindex/L2。

这两件事以前只有临时脚本验证过，改一行聚合逻辑就可能悄悄错掉：
1. **逐日聚合再合并** == 单文件聚合。日包开头带上一日最后一分钟（warmup），
   按目录日期切掉后不能重复计入，也不能漏。差分测试直接比对两套产物的每一列。
2. **reindex + 滚动**：缺分钟必须补成整分钟网格、打 `ob_missing=1`，滚动窗口
   按日历时间滑过缺口（不压缩行序）。

数据全是合成值（无随机），失败时可直接看数值定位。
"""
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

FEAT = Path(__file__).resolve().parent
if str(FEAT) not in sys.path:
    sys.path.insert(0, str(FEAT))

from ob_features import build_l1            # noqa: E402
from ob_rolling import _reindex, build_l2    # noqa: E402

DAY_MS = 86_400_000
D1 = int(datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp() * 1000)
D2 = D1 + DAY_MS


def _frames(t0: int, n_minutes: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """合成 n_minutes 分钟的 band（每边 3 档）与 mid。"""
    band, mid = [], []
    for i in range(n_minutes):
        bucket = t0 + i * 60_000
        m = 100_000.0 + 20.0 * math.sin(i / 50.0) + 0.001 * i
        mid.append({"bucket": bucket, "best_bid": m - 0.05, "best_ask": m + 0.05,
                    "mid": m, "spread": 0.1})
        for k in (1, 2, 3):
            band.append({"bucket": bucket, "side": "bid", "px": m - 0.5 * k,
                         "q": 1.0 + 0.1 * k, "mid": m})
            band.append({"bucket": bucket, "side": "ask", "px": m + 0.5 * k,
                         "q": 1.2 + 0.1 * k, "mid": m})
    return pl.DataFrame(band), pl.DataFrame(mid)


def _write(df: pl.DataFrame, fp: Path) -> Path:
    fp.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(fp)
    return fp


def _parts(tmp_path: Path):
    """两天日包（day2 故意带 day1 最后一分钟）+ 单文件参照。"""
    b1, m1 = _frames(D1, 1440)
    b2, m2 = _frames(D2, 1440)
    # 日包边界重叠一分钟：day2 开头重复写入 day1 的最后一分钟（真实产物就是这样）
    b2 = pl.concat([b1.filter(pl.col("bucket") == D2 - 60_000), b2])
    m2 = pl.concat([m1.filter(pl.col("bucket") == D2 - 60_000), m2])

    _write(b1, tmp_path / "days" / "2026-09-01" / "binance_futures_minute_band.parquet")
    _write(m1, tmp_path / "days" / "2026-09-01" / "binance_futures_minute_band_mid.parquet")
    _write(b2, tmp_path / "days" / "2026-09-02" / "binance_futures_minute_band.parquet")
    _write(m2, tmp_path / "days" / "2026-09-02" / "binance_futures_minute_band_mid.parquet")

    ref_b = pl.concat([b1, b2]).unique(subset=["bucket", "side", "px"], keep="any")
    ref_m = pl.concat([m1, m2]).unique(subset=["bucket"], keep="any")
    ref_bp = _write(ref_b, tmp_path / "ref" / "band.parquet")
    ref_mp = _write(ref_m, tmp_path / "ref" / "mid.parquet")
    return (tmp_path / "days"), (ref_bp, ref_mp)


def test_daily_merge_matches_single_file(tmp_path):
    days, (ref_bp, ref_mp) = _parts(tmp_path)
    got = build_l1(days / "*" / "binance_futures_minute_band.parquet",
                   days / "*" / "binance_futures_minute_band_mid.parquet")
    ref = build_l1(ref_bp, ref_mp)

    # 两天 2880 分钟：日界那分钟只能算一次
    assert got.height == 2880, got.height
    assert got["close_time"].n_unique() == 2880
    diff = got["close_time"].diff().drop_nulls().unique().to_list()
    assert diff == [60_000], diff
    assert got["close_time"].is_sorted()

    # 逐列比对：聚合、分箱、跨日合并三者任一写错都会在这里露出来。
    # 浮点列用容差：逐日聚合再合并的**求和顺序**与单文件不同，IEEE754 下
    # 末位必然有 1e-9 级差异（不是 bug，是可接受的重结合误差）。
    assert got.columns == ref.columns
    for c in ref.columns:
        if c == "close_time" or c == "ob_mid":
            assert got[c].to_list() == ref[c].to_list(), f"列 {c} 不一致"
            continue
        assert np.allclose(got[c].to_numpy(), ref[c].to_numpy(),
                           rtol=0, atol=1e-5, equal_nan=True), f"列 {c} 不一致"


def test_reindex_and_rolling_fill_gap(tmp_path):
    band, mid = _frames(D1, 600)
    bp = _write(band, tmp_path / "one" / "band.parquet")
    mp = _write(mid, tmp_path / "one" / "mid.parquet")
    l1 = build_l1(bp, mp)

    gap = l1["close_time"][100:110].to_list()
    l1 = l1.filter(~pl.col("close_time").is_in(gap))
    df = build_l2(_reindex(l1))

    # 补成连续分钟网格，缺口行打标记、盘口置 null
    assert df.height == 600
    assert df["close_time"].diff().drop_nulls().unique().to_list() == [60_000]
    missing = df.filter(pl.col("close_time").is_in(gap))
    assert missing.height == 10
    assert missing["ob_missing"].sum() == 10
    assert missing["ob_mid"].null_count() == 10
    assert df["ob_missing"].sum() == 10

    # 滚动/波动分档列都在（窗口按日历时间滑过缺口，不靠行序）
    for c in ("ob_ret_1m_bps", "ob_rv_5m_bps", "ob_obi_z_5m",
              "ob_vol_ratio", "ob_vol_bucket", "ob_ret_vol_norm"):
        assert c in df.columns
    # 1m 收益的 null = 首行 + 缺口 10 行 + 缺口后第一行（前值是 null）
    assert df["ob_ret_1m_bps"].null_count() == 12
