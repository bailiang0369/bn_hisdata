#!/usr/bin/env python3
"""Minimal R2 download-speed probe for CryptoHFTData.

在任意能连 Cloudflare R2 的机器上（例如魔搭 CPU 实例）跑一次，判断 R2 出口速度
是否够快，再决定是否把整套 pipeline 迁过去。

用法：
    export CHD_API_KEY=your-key
    python orderbook/test_r2_speed.py 2025-07-01 32 all            # 整日（两交易所各 24h=48 文件）并发
    python orderbook/test_r2_speed.py 2025-07-01 8 0,1,2,3         # 轻量试水（8 个文件）
    python orderbook/test_r2_speed.py 2025-07-01 4 0 binance_futures   # 单文件测速

鉴权方式与 download_orderbook_s3.py 完全一致；只测下载吞吐，不做构建。
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from download_orderbook_s3 import (
    get_credentials, NATIVE_ZSTD_CUTOVER_DATE, EXCHANGES, SYMBOL, DATA_TYPE,
)
from s3lite import S3LiteClient

# 强制 IPv4：国内/机房环境 IPv6 常被黑洞，requests 会先试 v6 并卡满超时才回落，
# 表现为"看似永久卡死"。curl -4 能通而 Python 卡死即此因。
import socket
_orig_getaddrinfo = socket.getaddrinfo
def _ipv4_only(*args, **kwargs):
    return [ai for ai in _orig_getaddrinfo(*args, **kwargs) if ai[0] == socket.AF_INET]
socket.getaddrinfo = _ipv4_only

API_KEY = os.environ.get("CHD_API_KEY")
if not API_KEY:
    print("ERROR: set CHD_API_KEY environment variable first")
    sys.exit(2)

DATE = sys.argv[1] if len(sys.argv) > 1 else "2025-07-01"
WORKERS = int(sys.argv[2]) if len(sys.argv) > 2 else 32
HOURS_ARG = sys.argv[3] if len(sys.argv) > 3 else "all"
EXCHANGE_ARG = sys.argv[4] if len(sys.argv) > 4 else None
if HOURS_ARG == "all":
    HOURS = list(range(24))
else:
    HOURS = [int(h) for h in HOURS_ARG.split(",") if h.strip() != ""]
EXS = [EXCHANGE_ARG] if EXCHANGE_ARG else EXCHANGES

cutover = datetime.fromisoformat(DATE) >= datetime.fromisoformat(NATIVE_ZSTD_CUTOVER_DATE)
suffix = ".parquet.zst" if not cutover else ".parquet"

print(f"fetching R2 credentials for {DATE} (suffix={suffix}) ...", flush=True)
creds = get_credentials(API_KEY)
bucket = creds["bucket"]
client = S3LiteClient(
    endpoint=creds["endpoint"],
    access_key_id=creds["credentials"]["access_key_id"],
    secret_access_key=creds["credentials"]["secret_access_key"],
    session_token=creds["credentials"].get("session_token"),
    region=creds.get("region") or "auto",
    max_pool_connections=WORKERS, timeout=1800, max_attempts=5,
)


def keys_for(ex):
    return [f"{ex}/{DATE}/{h:02d}/{SYMBOL}_{DATA_TYPE}{suffix}" for h in HOURS]


KEYS = [k for ex in EXS for k in keys_for(ex)]


def fetch(key):
    t0 = time.time()
    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        body = resp["Body"]
        n = 0
        last = 0
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                break
            n += len(chunk)
            if n - last >= 2 * 1024 * 1024:  # 慢链路下也能看到实时速率，不再像卡死
                dt = time.time() - t0
                print(f"    {key.rsplit('/', 1)[-1]}: {n / 1e6:.1f} MB "
                      f"@ {n / 1e6 / dt:.2f} MB/s", flush=True)
                last = n
        body.close()
        return n, time.time() - t0, None
    except Exception as e:  # noqa: BLE001
        return 0, time.time() - t0, repr(e)[:140]


n0, dt0, err0 = fetch(KEYS[0])
if err0:
    print(f"[single] FAILED: {err0}")
    sys.exit(1)
print(f"[single] {n0 / 1e6:.2f} MB in {dt0:.2f}s -> {n0 / 1e6 / dt0:.2f} MB/s")

t0 = time.time()
total = 0
failed = 0
with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    for n, dt, err in pool.map(fetch, KEYS):
        total += n
        if err:
            failed += 1
dt = time.time() - t0
print(f"[concurrent {len(KEYS)} files @ {WORKERS}w] {total / 1e6:.1f} MB in {dt:.2f}s "
      f"-> {total / 1e6 / dt:.2f} MB/s aggregate (failed={failed})")

if HOURS_ARG == "all" and failed == 0 and dt > 0:
    span_days = 480  # 2025-07 ~ 2026-10 大致天数
    est = dt * span_days / 3600
    print(f"==> 整日下载约 {dt:.1f}s；若保持此吞吐，整段 ~{span_days} 天下载约需 "
          f"{est:.1f} 小时（仅下载，不含构建；且需跨 9h 窗口断点续传）。")
