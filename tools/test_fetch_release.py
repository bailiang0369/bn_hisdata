#!/usr/bin/env python3
"""fetch_release.download 的回归测试（不联网，monkeypatch requests.get）。

这两个分支踩过：
1. 上次已下满、只是 `.part` 改名失败（Windows 上文件被占用很常见）时，
   再发 `Range: bytes=<size>-` 会被源站判 416，于是每次都崩在同一资产上。
2. 单个资产 HTTP 失败不该抛异常中断整轮（其余资产还能下完，由 fail 计数收口）。
"""
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import fetch_release as fr        # noqa: E402


def test_complete_part_is_renamed_without_request(tmp_path, monkeypatch):
    dst = tmp_path / "a.parquet"
    part = tmp_path / "a.parquet.part"
    part.write_bytes(b"12345678")

    def _boom(*a, **kw):           # 任何网络调用都算失败
        raise AssertionError("不应发起请求")

    monkeypatch.setattr(fr.requests, "get", _boom)
    name, status, size = fr.download(
        {"name": "a.parquet", "size": 8, "browser_download_url": "http://x"}, tmp_path)
    assert (name, status, size) == ("a.parquet", "ok", 8)
    assert dst.read_bytes() == b"12345678"
    assert not part.exists()


def test_http_error_returns_status(tmp_path, monkeypatch):
    class _Resp:
        status_code = 500

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(fr.requests, "get", lambda *a, **kw: _Resp())
    name, status, _ = fr.download(
        {"name": "b.parquet", "size": 8, "browser_download_url": "http://x"}, tmp_path)
    assert status == "HTTP500", status
    assert not (tmp_path / "b.parquet").exists()
