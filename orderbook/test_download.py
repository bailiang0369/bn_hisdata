"""测试匿名下载 CryptoHFTData 的一个小时订单簿文件"""
import requests

url = "https://api.cryptohftdata.com/v1/download"
params = {"file": "binance_spot/2025-08-01/20/BTCUSDT_orderbook.parquet.zst"}
out = "data/sample_hour.parquet.zst"

import os
os.makedirs("data", exist_ok=True)

with requests.get(url, params=params, stream=True, timeout=300) as r:
    print("HTTP", r.status_code)
    print("headers:", dict(r.headers))
    r.raise_for_status()
    size = 0
    with open(out, "wb") as f:
        for chunk in r.iter_content(chunk_size=1 << 20):
            f.write(chunk)
            size += len(chunk)
print("downloaded bytes:", size)

# 检查文件头魔数
with open(out, "rb") as f:
    head = f.read(8)
print("first 8 bytes:", head)
print("is parquet:", head[:4] == b"PAR1")
print("is zstd frame:", head[:4] == b"\x28\xb5\x2f\xfd")
