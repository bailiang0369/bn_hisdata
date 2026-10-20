"""
Unit tests for binance_vision/build_master_features.py
"""
import math

import polars as pl
import pytest
from binance_vision.build_master_features import build_master_feature_table, rolling_quantile_bucket


def _mock_klines():
    t1 = 1700000040000
    t2 = 1700000100000
    # Mock Futures 1m Klines
    df_um = pl.DataFrame({
        "close_time": [t1, t2],
        "open": [100.0, 102.0],
        "high": [105.0, 106.0],
        "low": [99.0, 101.0],
        "close": [103.0, 105.0],
        "buy_vol": [10.0, 15.0],
        "sell_vol": [5.0, 8.0],
        "count": [100, 120],
    })
    # Mock Spot 1m Klines
    df_spot = pl.DataFrame({
        "close_time": [t1, t2],
        "close": [100.0, 102.0],
        "buy_vol": [8.0, 12.0],
        "sell_vol": [4.0, 6.0],
        "count": [80, 90],
    })
    return df_um, df_spot


def _mock_metrics():
    t1 = 1700000040000
    return pl.DataFrame({
        "create_time": [t1 - 300000],
        "sum_open_interest": [1000.0],
        "sum_open_interest_value": [500000.0],
        "count_toptrader_long_short_ratio": [1.5],
        "sum_toptrader_long_short_ratio": [1.6],
        "count_long_short_ratio": [1.2],
        "sum_taker_long_short_vol_ratio": [1.1],
    })


def _mock_aggtrades_um():
    # 两笔都落在 close_time == t1 的 1m 窗口内（t1 是 60000 的整数倍）
    return pl.DataFrame({
        "agg_trade_id": [1, 2],
        "price": [101.0, 102.0],
        "quantity": [1.0, 2.0],
        "first_trade_id": [10, 20],
        "last_trade_id": [10, 22],
        "transact_time": [1699999990000, 1700000010000],
        "is_buyer_maker": [False, True],
        "is_best_match": [True, True],
    })


def _mock_aggtrades_spot():
    return pl.DataFrame({
        "agg_trade_id": [11, 12],
        "price": [100.5, 100.8],
        "quantity": [2.0, 1.0],
        "first_trade_id": [30, 40],
        "last_trade_id": [30, 40],
        "transact_time": [1699999995000, 1700000005000],
        "is_buyer_maker": [False, False],
        "is_best_match": [True, True],
    })


def test_build_master_feature_table_basic():
    df_um, df_spot = _mock_klines()
    master = build_master_feature_table(
        df_um, df_spot,
        df_aggtrades_um=_mock_aggtrades_um(),
        df_aggtrades_spot=_mock_aggtrades_spot(),
        df_metrics=_mock_metrics(),
    )
    assert len(master) == 2
    assert "basis" in master.columns
    assert "basis_rate" not in master.columns
    assert "create_time" not in master.columns
    assert master["basis"][0] == pytest.approx(3.0)
    assert master["sum_open_interest"][0] == pytest.approx(1000.0)
    # 两个市场的微观特征必须带前缀真的 join 上（2026-10 修复的静默缺失）
    for c in ("um_agg_vwap_disparity", "um_agg_price_robust_zscore",
              "um_agg_rv", "um_agg_dt_std",
              "spot_agg_vwap_disparity", "spot_agg_price_robust_zscore",
              "spot_agg_rv", "spot_agg_dt_std"):
        assert c in master.columns
    # 跨市场特征：rv_um = ln(102/101), rv_spot = ln(100.8/100.5)
    rv_um = math.log(102.0 / 101.0)
    rv_spot = math.log(100.8 / 100.5)
    assert "rv_dominance" in master.columns
    assert master["rv_dominance"][0] == pytest.approx(
        (rv_um - rv_spot) / (rv_um + rv_spot), rel=1e-4)
    # 旧的无前缀列名不允许再出现（避免下游误用），已删除的列也不允许回来
    for c in ("vwap_disparity", "price_robust_zscore", "trade_count",
              "qty_iqr", "qty_tail_ratio", "mean_sweep_depth", "rv", "rv_up"):
        assert c not in master.columns


def test_build_master_feature_table_no_agg():
    """基础宽表：无 aggTrades 时核心列齐全、微观列缺席（--no-agg 路径）。"""
    df_um, df_spot = _mock_klines()
    master = build_master_feature_table(df_um, df_spot, df_aggtrades_um=None,
                                        df_aggtrades_spot=None, df_metrics=None)
    assert len(master) == 2
    # 核心列必须在
    for c in ("close_time", "close", "spot_close", "basis", "um_buy_vol",
              "um_sell_vol", "spot_buy_vol", "spot_sell_vol", "um_count", "spot_count"):
        assert c in master.columns
    # 微观列必须缺席
    for c in ("um_agg_rv", "spot_agg_rv", "um_agg_vwap_disparity",
              "spot_agg_vwap_disparity", "rv_dominance"):
        assert c not in master.columns
    assert master["basis"][0] == pytest.approx(3.0)


def test_build_master_feature_table_um_only_agg():
    """只有 um aggTrades（spot 没下）时：um 前缀列在，spot 前缀列与
    rv_dominance 缺席——spot 缺只警告、不阻断。"""
    df_um, df_spot = _mock_klines()
    master = build_master_feature_table(df_um, df_spot,
                                        df_aggtrades_um=_mock_aggtrades_um(),
                                        df_aggtrades_spot=None)
    assert "um_agg_rv" in master.columns
    assert "um_agg_rv_up" in master.columns
    assert "spot_agg_rv" not in master.columns
    assert "rv_dominance" not in master.columns


def test_build_master_feature_table_precomputed_agg_features():
    """预聚合特征文件路径（features/*_feat_1m.parquet）：直接 join，
    不再现场算；列与原始路径一致（带前缀）。"""
    df_um, df_spot = _mock_klines()
    df_agg_um = _mock_aggtrades_um()
    # 模拟 build_aggtrade_features.process_file 的产物
    from binance_vision.build_aggtrade_features import compute_aggtrade_features
    feat_um = compute_aggtrade_features(df_agg_um, "1m").collect()
    master = build_master_feature_table(df_um, df_spot, df_aggtrades_um=feat_um,
                                        df_aggtrades_spot=None)
    assert "um_agg_rv" in master.columns
    assert "um_agg_price_robust_zscore" in master.columns
    assert "um_agg_dt_std" in master.columns
    # 预聚合与现场算应得到同样的值
    master_raw = build_master_feature_table(df_um, df_spot,
                                            df_aggtrades_um=df_agg_um,
                                            df_aggtrades_spot=None)
    assert master["um_agg_rv"][0] == pytest.approx(master_raw["um_agg_rv"][0])
    assert master["um_agg_vwap_disparity"][0] == pytest.approx(
        master_raw["um_agg_vwap_disparity"][0])


def test_scan_agg_files_prefers_precomputed(tmp_path):
    """预聚合特征文件存在时优先，同市场原始文件被忽略（否则同一分钟既来自
    原始又来自特征，left join 会把行数翻倍）。"""
    from binance_vision.build_master_features import _scan_agg_files

    raw = tmp_path / "um" / "daily" / "aggTrades" / "BTCUSDT"
    feat = raw / "features"
    raw.mkdir(parents=True)
    feat.mkdir(parents=True)
    (raw / "BTCUSDT-aggTrades-2025-06-01.parquet").touch()
    (feat / "BTCUSDT-aggTrades-2025-06-01_feat_1m.parquet").touch()

    assert _scan_agg_files(tmp_path, "BTCUSDT", "um") == [
        feat / "BTCUSDT-aggTrades-2025-06-01_feat_1m.parquet"]
    # 没有特征文件时回退到逐笔原始
    (feat / "BTCUSDT-aggTrades-2025-06-01_feat_1m.parquet").unlink()
    assert _scan_agg_files(tmp_path, "BTCUSDT", "um") == [
        raw / "BTCUSDT-aggTrades-2025-06-01.parquet"]
    # 两个市场互相独立
    assert _scan_agg_files(tmp_path, "BTCUSDT", "spot") == []


def test_rolling_quantile_bucket():
    df = pl.DataFrame({"basis": [-10, 0, 10, 20, 30, 25, 15, 5, -5, -15]})
    res = df.with_columns(rolling_quantile_bucket("basis", window_size=5, n_buckets=5))
    assert "basis_bucket_5m_q5" in res.columns
    assert res["basis_bucket_5m_q5"].dtype == pl.Int8
    # Check non-null values mapped to buckets 0 .. 4
    non_null_buckets = res["basis_bucket_5m_q5"].drop_nulls()
    assert len(non_null_buckets) > 0
    assert non_null_buckets.min() >= 0
    assert non_null_buckets.max() <= 4
