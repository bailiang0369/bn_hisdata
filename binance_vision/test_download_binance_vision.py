"""
Unit tests for binance_vision/download_binance_vision.py
"""
import hashlib
import io
import time
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from binance_vision.download_binance_vision import (
    dest_path,
    download_one,
    fetch_expected_sha256,
    get_schema_overrides,
    parse_list_arg,
    period_strings,
    process,
    url_for,
)


def test_period_strings_daily():
    start = date(2025, 6, 1)
    end = date(2025, 6, 3)
    periods = list(period_strings("daily", start, end))
    assert periods == ["2025-06-01", "2025-06-02", "2025-06-03"]


def test_period_strings_monthly():
    start = date(2024, 11, 15)
    end = date(2025, 2, 10)
    periods = list(period_strings("monthly", start, end))
    assert periods == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_url_for():
    url_kline = url_for("spot", "daily", "klines", "BTCUSDT", "1m", "2025-06-01")
    assert url_kline == "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-06-01.zip"

    url_metrics = url_for("futures/um", "daily", "metrics", "BTCUSDT", "1m", "2025-06-01")
    assert url_metrics == "https://data.binance.vision/data/futures/um/daily/metrics/BTCUSDT/BTCUSDT-metrics-2025-06-01.zip"


def test_dest_path():
    root = Path("/tmp/data")
    path_kline = dest_path(root, "spot", "daily", "klines", "BTCUSDT", "1m", "2025-06-01")
    assert path_kline == Path("/tmp/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-06-01.zip")


def test_parse_list_arg():
    assert parse_list_arg("BTCUSDT, ETHUSDT ,SOLUSDT") == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


def test_get_schema_overrides():
    fdtype = pl.Float32
    schema_k = get_schema_overrides("klines", fdtype)
    assert schema_k["open"] == pl.Float32
    assert schema_k["open_time"] == pl.Int64
    # volume / taker_buy_volume 必须按 Float64 解析：sell_vol 的减法要在
    # cast 成 f32 之前按全精度算（2026-10 修复的精度回归）
    assert schema_k["volume"] == pl.Float64
    assert schema_k["taker_buy_volume"] == pl.Float64
    schema_a = get_schema_overrides("aggTrades", fdtype)
    assert schema_a["price"] == pl.Float32
    assert schema_a["is_buyer_maker"] == pl.Boolean


def create_zip_with_csv(csv_content: str, csv_filename: str = "data.csv") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(csv_filename, csv_content)
    return buf.getvalue()


def test_process_klines(tmp_path):
    csv_header = (
        "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n"
        "1700000000000,100.0,105.0,95.0,102.0,10.0,1700000059999,1020.0,50,4.0,408.0,0\n"
    )
    zip_bytes = create_zip_with_csv(csv_header)
    zip_file = tmp_path / "test-kline-header.zip"
    zip_file.write_bytes(zip_bytes)

    logs = []
    process(zip_file, "klines", "parquet", pl.Float32, logs.append)

    parquet_out = zip_file.with_suffix(".parquet")
    assert parquet_out.exists()

    df = pl.read_parquet(parquet_out)
    assert df.columns == ["open_time", "open", "high", "low", "close", "close_time", "count", "buy_vol", "sell_vol"]
    assert df["buy_vol"][0] == pytest.approx(4.0)
    assert df["sell_vol"][0] == pytest.approx(6.0)
    assert df["close_time"][0] == 1700000060000  # raw close_time (1700000059999) + 1ms


def test_process_klines_sell_vol_full_precision(tmp_path):
    """sell_vol = volume - taker_buy_volume 必须在 cast 成 f32 之前按全精度算。

    1e8 附近 f32 的 ULP 是 8：若 volume/taker_buy_volume 在解析期就被转成
    f32，两个值会round到同一个数，sell_vol 变成 0 而不是 1。
    """
    csv_header = (
        "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore\n"
        "1700000000000,100.0,105.0,95.0,102.0,100000000.0,1700000059999,10200000000.0,50,99999999.0,9999999900.0,0\n"
    )
    zip_bytes = create_zip_with_csv(csv_header)
    zip_file = tmp_path / "test-kline-precision.zip"
    zip_file.write_bytes(zip_bytes)

    logs = []
    process(zip_file, "klines", "parquet", pl.Float32, logs.append)

    df = pl.read_parquet(zip_file.with_suffix(".parquet"))
    assert df["buy_vol"][0] == pytest.approx(99999999.0, rel=1e-6)
    assert df["sell_vol"][0] == pytest.approx(1.0)  # f32 减法会得到 0


def test_process_aggtrades(tmp_path):
    csv_agg = (
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker,is_best_match\n"
        "1001,50000.5,0.02,2001,2002,1700000000000,true,true\n"
    )
    zip_bytes = create_zip_with_csv(csv_agg)
    zip_file = tmp_path / "test-aggtrades.zip"
    zip_file.write_bytes(zip_bytes)

    logs = []
    process(zip_file, "aggTrades", "parquet", pl.Float32, logs.append)

    parquet_out = zip_file.with_suffix(".parquet")
    assert parquet_out.exists()

    df = pl.read_parquet(parquet_out)
    assert len(df) == 1
    assert df["agg_trade_id"][0] == 1001
    assert df["is_buyer_maker"][0] is True
    # 显式 cast 保证输出 dtype 不依赖 polars 的 schema_overrides 行为
    assert df.schema["agg_trade_id"] == pl.Int64
    assert df.schema["transact_time"] == pl.Int64
    assert df.schema["price"] == pl.Float32
    assert df.schema["is_best_match"] == pl.Boolean


def test_process_aggtrades_um_7col_header(tmp_path):
    """um 真实格式（2026-10 实测）：有表头、7 列、无 is_best_match、毫秒时间戳。

    旧代码写死 8 列 cast，在这种文件上直接报
    unable to find column "is_best_match"。
    """
    csv_agg = (
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n"
        "3481665834,83723.1,0.002,8163478337,8163478337,1791763200006,false\n"
    )
    zip_bytes = create_zip_with_csv(csv_agg)
    zip_file = tmp_path / "test-aggtrades-um.zip"
    zip_file.write_bytes(zip_bytes)

    logs = []
    process(zip_file, "aggTrades", "parquet", pl.Float32, logs.append)

    df = pl.read_parquet(zip_file.with_suffix(".parquet"))
    assert df.columns == ["agg_trade_id", "price", "quantity", "first_trade_id",
                          "last_trade_id", "transact_time", "is_buyer_maker"]
    assert df["transact_time"][0] == 1791763200006  # 毫秒，原样保留
    assert df["is_buyer_maker"][0] is False


def test_process_aggtrades_spot_headerless_us(tmp_path):
    """spot 真实格式（2026-10 实测）：无表头、8 列、微秒时间戳、大写布尔。

    transact_time 必须归一化成毫秒，否则下游按 ms 解释全部错位。
    """
    csv_agg = (
        "4087145462,83773.12000000,0.00013000,6753490065,6753490065,1791763200107731,False,True\n"
        "4087145463,83773.11000000,0.00246000,6753490066,6753490066,1791763200128133,True,True\n"
    )
    zip_bytes = create_zip_with_csv(csv_agg)
    zip_file = tmp_path / "test-aggtrades-spot.zip"
    zip_file.write_bytes(zip_bytes)

    logs = []
    process(zip_file, "aggTrades", "parquet", pl.Float32, logs.append)

    df = pl.read_parquet(zip_file.with_suffix(".parquet"))
    assert df.height == 2
    assert df.schema == {
        "agg_trade_id": pl.Int64,
        "price": pl.Float32,
        "quantity": pl.Float32,
        "first_trade_id": pl.Int64,
        "last_trade_id": pl.Int64,
        "transact_time": pl.Int64,
        "is_buyer_maker": pl.Boolean,
        "is_best_match": pl.Boolean,
    }
    # 微秒 -> 毫秒（1791763200107731 // 1000 == 1791763200107）
    assert df["transact_time"][0] == 1791763200107
    assert df["transact_time"][1] == 1791763200128
    assert df["is_buyer_maker"][0] is False
    assert df["is_best_match"].null_count() == 0


def test_process_aggtrades_headerless(tmp_path):
    csv_agg = (
        "1001,50000.5,0.02,2001,2002,1700000000000,true,true\n"
        "1002,50001.0,0.03,2003,2004,1700000001000,false,true\n"
    )
    zip_bytes = create_zip_with_csv(csv_agg)
    zip_file = tmp_path / "test-aggtrades-noheader.zip"
    zip_file.write_bytes(zip_bytes)

    logs = []
    process(zip_file, "aggTrades", "parquet", pl.Float32, logs.append)

    df = pl.read_parquet(zip_file.with_suffix(".parquet"))
    assert len(df) == 2
    assert df.schema == {
        "agg_trade_id": pl.Int64,
        "price": pl.Float32,
        "quantity": pl.Float32,
        "first_trade_id": pl.Int64,
        "last_trade_id": pl.Int64,
        "transact_time": pl.Int64,
        "is_buyer_maker": pl.Boolean,
        "is_best_match": pl.Boolean,
    }


def test_fetch_expected_sha256():
    session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = "A1B2C3D4E5F6  BTCUSDT-1m-2025-06-01.zip\n"
    session.get.return_value = mock_resp

    logs = []
    sha = fetch_expected_sha256(session, "http://example.com/file.zip.CHECKSUM", logs.append)
    assert sha == "a1b2c3d4e5f6"


def test_download_one(tmp_path):
    session = MagicMock()
    data_bytes = b"sample zip content"
    expected_sha = hashlib.sha256(data_bytes).hexdigest()

    mock_checksum_resp = MagicMock()
    mock_checksum_resp.status_code = 200
    mock_checksum_resp.text = f"{expected_sha}  file.zip\n"

    mock_data_resp = MagicMock()
    mock_data_resp.status_code = 200
    mock_data_resp.iter_content.return_value = [data_bytes]
    mock_data_resp.__enter__.return_value = mock_data_resp

    def session_get_side_effect(url, **kwargs):
        if url.endswith(".CHECKSUM"):
            return mock_checksum_resp
        return mock_data_resp

    session.get.side_effect = session_get_side_effect

    dest = tmp_path / "sample.zip"
    logs = []
    res = download_one(session, "http://example.com/sample.zip", dest, True, logs.append)

    assert res == "ok"
    assert dest.exists()
    assert dest.read_bytes() == data_bytes


def test_download_one_cleans_part_on_checksum_mismatch(tmp_path, monkeypatch):
    """校验和不匹配的重试路径也必须删掉 .part：失败后不留残件。"""
    monkeypatch.setattr(time, "sleep", lambda s: None)

    session = MagicMock()
    bad_bytes = b"corrupted zip content"

    mock_checksum_resp = MagicMock()
    mock_checksum_resp.status_code = 200
    mock_checksum_resp.text = f"{'0' * 64}  file.zip\n"  # 故意对不上

    mock_data_resp = MagicMock()
    mock_data_resp.status_code = 200
    mock_data_resp.iter_content.return_value = [bad_bytes]
    mock_data_resp.__enter__.return_value = mock_data_resp

    def session_get_side_effect(url, **kwargs):
        if url.endswith(".CHECKSUM"):
            return mock_checksum_resp
        return mock_data_resp

    session.get.side_effect = session_get_side_effect

    dest = tmp_path / "bad.zip"
    logs = []
    res = download_one(session, "http://example.com/bad.zip", dest, True, logs.append)

    assert res == "failed"
    assert not dest.exists()
    assert not (tmp_path / "bad.zip.part").exists()  # 残件必须清掉
