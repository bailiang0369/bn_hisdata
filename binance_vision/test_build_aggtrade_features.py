"""
Unit tests for binance_vision/build_aggtrade_features.py
"""
import math

import polars as pl
import pytest

from binance_vision.build_aggtrade_features import FEATURE_COLS, compute_aggtrade_features


def _mock_aggtrades():
    """同一 1m 窗口内 4 笔：price 100/101/102/101，qty 1/2/3/1。"""
    return pl.DataFrame({
        "agg_trade_id": [1, 2, 3, 4],
        "price": [100.0, 101.0, 102.0, 101.0],
        "quantity": [1.0, 2.0, 3.0, 1.0],
        "first_trade_id": [10, 20, 30, 40],
        "last_trade_id": [10, 22, 30, 40],
        "transact_time": [1700000000000, 1700000010000, 1700000020000, 1700000030000],
        "is_buyer_maker": [False, True, False, True],
        "is_best_match": [True, True, True, True],
    })


def test_feature_columns_exact():
    """输出列 == timestamp + close + FEATURE_COLS。

    与 K 线冗余的列（trade_count / volume / turnover / OHLC）和大单列
    （qty_* / sweep_*）必须已删除（2026-10-20 精简）。
    """
    feat = compute_aggtrade_features(_mock_aggtrades(), "1m").collect()
    assert feat.columns == ["timestamp", "close"] + FEATURE_COLS
    for c in ("open", "high", "low", "volume", "turnover", "trade_count",
              "qty_median", "qty_iqr", "qty_tail_ratio",
              "mean_sweep_depth", "max_sweep_depth"):
        assert c not in feat.columns


def test_feature_values():
    feat = compute_aggtrade_features(_mock_aggtrades(), "1m").collect()
    assert len(feat) == 1
    row = feat.row(0, named=True)
    # vwap = (100*1 + 101*2 + 102*3 + 101*1) / 7 = 709/7
    assert row["vwap"] == pytest.approx(709.0 / 7.0)
    # (close - vwap)/vwap = (101 - 709/7)/(709/7) = -2/709
    assert row["vwap_disparity"] == pytest.approx(-2.0 / 709.0)
    # price_median = 101（排序后 [100,101,101,102] 的中间两值平均）
    assert row["price_median"] == pytest.approx(101.0)
    # price_mad: |100-101|=1, 0, |102-101|=1, 0 -> median = 0.5
    assert row["price_mad"] == pytest.approx(0.5)
    # close == price_median -> robust z-score = 0
    assert row["price_robust_zscore"] == pytest.approx(0.0, abs=1e-6)
    # log returns（首笔 null）: +ln(1.01), +ln(102/101), -ln(102/101)
    r_up = math.log(1.01)
    r_dn = math.log(102.0 / 101.0)
    assert row["rv"] == pytest.approx(math.sqrt(r_up**2 + 2 * r_dn**2), rel=1e-6)
    assert row["rv_up"] == pytest.approx(math.sqrt(r_up**2 + r_dn**2), rel=1e-6)
    assert row["rv_down"] == pytest.approx(r_dn, rel=1e-6)
    assert row["semi_var_ratio"] == pytest.approx(
        math.sqrt(r_up**2 + r_dn**2) / (r_dn + 1e-8), rel=1e-6)
    # dt: 10000/10000/10000 -> mean 10000, std 0
    assert row["dt_mean"] == pytest.approx(10000.0)
    assert row["dt_std"] == pytest.approx(0.0, abs=1e-9)


def test_multiple_windows_label_right():
    """group_by_dynamic label='right'：窗口右边界 == kline close_time。"""
    t0 = 1700000040000 - 60000  # 第一个窗口的起点
    df = pl.DataFrame({
        "agg_trade_id": [1, 2],
        "price": [100.0, 101.0],
        "quantity": [1.0, 1.0],
        "first_trade_id": [1, 2],
        "last_trade_id": [1, 2],
        "transact_time": [t0 + 1000, t0 + 61000],  # 相邻两个窗口各一笔
        "is_buyer_maker": [False, False],
        "is_best_match": [True, True],
    })
    feat = compute_aggtrade_features(df, "1m").collect().sort("timestamp")
    assert len(feat) == 2
    ts = feat["timestamp"].dt.epoch("ms").to_list()
    assert ts == [t0 + 60000, t0 + 120000]


def test_single_trade_window_boundary_nulls():
    """单笔窗口：log_return / dt_ms 为 null -> dt_std 为 null、rv 退化为
    1e-6（epsilon），不报错。"""
    df = pl.DataFrame({
        "agg_trade_id": [1],
        "price": [100.0],
        "quantity": [1.0],
        "first_trade_id": [1],
        "last_trade_id": [1],
        "transact_time": [1700000000000],
        "is_buyer_maker": [False],
        "is_best_match": [True],
    })
    row = compute_aggtrade_features(df, "1m").collect().row(0, named=True)
    assert row["dt_std"] is None
    assert row["rv"] == pytest.approx(1e-6, rel=1e-3)
    assert row["vwap"] == pytest.approx(100.0)
