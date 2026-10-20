"""
Unit tests for orderbook/build_minute_snapshots.py
"""
import polars as pl

from build_minute_snapshots import process_hour
from test_ob_reconstruct import T0, _df, _swing_events


def _write_hour(path, events) -> None:
    _df(events).write_parquet(path)


def test_sparse_equals_dense_on_big_mid_swing(tmp_path):
    """端到端：mid 摆动 10% 的小文件，sparse 与 dense 的 band+mid 产物
    必须逐行一致，且每一分钟都有 band 行（旧窗口会裁空第 3 分钟起）。"""
    f = tmp_path / "BTCUSDT_orderbook.parquet"
    _write_hour(f, _swing_events())
    band_d, mid_d = process_hour(f, 0.003, "dense")
    band_s, mid_s = process_hour(f, 0.003, "sparse")
    assert band_s.equals(band_d)
    assert mid_s.equals(mid_d)
    assert band_d["bucket"].n_unique() == 6, "every minute must keep band rows"
    assert band_d.height > 0


def test_band_parameter_is_honored(tmp_path):
    """--band 必须真正改变裁剪宽度（守护 2026-10-20 修掉的参数遮蔽：
    旧代码 process_hour 里局部变量曾把 float 参数 band 重绑成 DataFrame）。"""
    f = tmp_path / "BTCUSDT_orderbook.parquet"
    _write_hour(f, _swing_events(n_minutes=2))
    narrow, _ = process_hour(f, 0.01, "dense")
    wide, _ = process_hour(f, 0.1, "dense")
    assert wide.height > narrow.height
    assert wide["px"].max() > narrow["px"].max()
    assert wide["px"].min() < narrow["px"].min()
