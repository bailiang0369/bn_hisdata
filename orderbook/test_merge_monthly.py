#!/usr/bin/env python3
"""merge_monthly 的回归测试：回补时必须重建月表，且写出不能留半截文件。

历史坑：月表一旦存在就 `[skip]`，先合并了前半月、之后再补后半月时，
第二次运行直接跳过，新数据永远进不去（且不会报错）。
"""
import sys
from pathlib import Path

import polars as pl

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import merge_monthly            # noqa: E402


def _day_file(root: Path, day: str, n: int, exchange: str = "binance_futures") -> Path:
    """造一天的 band 窄表：bucket 从该日 00:00 起每分钟一行。"""
    from datetime import datetime, timezone
    t0 = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)
    df = pl.DataFrame({"bucket": [t0 + i * 60_000 for i in range(n)],
                       "q": [1.0] * n})
    fp = root / day / f"{exchange}_minute_band.parquet"
    fp.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(fp)
    return fp


def test_stale_detects_backfilled_days(tmp_path):
    f1 = _day_file(tmp_path, "2026-09-01", 3)
    out = tmp_path / "monthly" / "binance_futures_minute_band_2026-09.parquet"
    out.parent.mkdir(parents=True)
    merge_monthly._merge([f1], out)
    assert not out.with_name(out.name + ".part").exists()
    assert merge_monthly._stale(out, [f1]) is False

    f2 = _day_file(tmp_path, "2026-09-02", 4)
    assert merge_monthly._stale(out, [f1, f2]) is True


def test_main_rebuilds_after_backfill(tmp_path, monkeypatch):
    f1 = _day_file(tmp_path, "2026-09-01", 3)
    monkeypatch.setattr(sys, "argv", ["merge_monthly", "--dest-root", str(tmp_path)])
    merge_monthly.main()

    out = tmp_path / "monthly" / "binance_futures_minute_band_2026-09.parquet"
    assert pl.read_parquet(out).height == 3

    _day_file(tmp_path, "2026-09-02", 4)
    merge_monthly.main()          # 不带 --force 也必须重建
    got = pl.read_parquet(out)
    assert got.height == 7, got.height
    assert got["bucket"].is_sorted()
    assert f1.exists()            # 没有 --delete-daily 时每日文件保留
