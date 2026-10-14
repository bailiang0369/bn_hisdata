"""匿名下载 CryptoHFTData 一天的 BTCUSDT L2 订单簿（每小时一个文件）。

用法: python download_day.py --date 2026-10-13 [--exchange binance_futures]
本地布局: data/{date}/{exchange}/{HH}/BTCUSDT_orderbook.parquet[.zst]
"""
import argparse
import http.client
import sys
import time
from pathlib import Path

import requests

API_BASE = "https://api.cryptohftdata.com/v1"
SYMBOL = "BTCUSDT"
DATA_TYPE = "orderbook"
NATIVE_ZSTD_CUTOVER_DATE = "2026-08-19"  # 之后为裸 .parquet，之前为 .parquet.zst


def get_object(key: str, retries: int = 5) -> bytes | None:
    url = f"{API_BASE}/download"
    for attempt in range(retries):
        try:
            with requests.get(
                url, params={"file": key}, stream=True, timeout=300
            ) as resp:
                if resp.status_code == 404:
                    return None
                if resp.status_code == 429:
                    retry = int(resp.headers.get("Retry-After", 10))
                    print(f"  rate limited, sleep {retry}s", file=sys.stderr)
                    time.sleep(retry)
                    continue
                resp.raise_for_status()
                chunks = []
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    chunks.append(chunk)
                return b"".join(chunks)
        except (requests.RequestException, http.client.IncompleteRead) as exc:
            wait = 2 * (attempt + 1)
            print(f"  attempt {attempt + 1} failed for {key}: {exc}; retry in {wait}s",
                  file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"failed after {retries} retries: {key}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="UTC date YYYY-MM-DD")
    ap.add_argument("--exchange", default="binance_futures")
    ap.add_argument("--dest-root", default="data")
    ap.add_argument("--sleep", type=float, default=1.5)
    args = ap.parse_args()

    root = Path(args.dest_root) / args.date / args.exchange
    cutover = args.date >= NATIVE_ZSTD_CUTOVER_DATE
    suffixes = (".parquet", ".parquet.zst") if cutover else (".parquet.zst", ".parquet")

    ok = miss = 0
    for hour in range(24):
        dest_dir = root / f"{hour:02d}"
        dest = dest_dir / f"{SYMBOL}_{DATA_TYPE}.parquet"
        if dest.exists():
            print(f"skip {args.exchange} {hour:02d} (exists)")
            ok += 1
            continue
        got = None
        for suffix in suffixes:
            key = f"{args.exchange}/{args.date}/{hour:02d}/{SYMBOL}_{DATA_TYPE}{suffix}"
            data = get_object(key)
            if data is not None:
                dest = dest_dir / f"{SYMBOL}_{DATA_TYPE}{suffix}"
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(data)
                got = len(data)
                break
        if got is None:
            miss += 1
            print(f"404  {args.exchange} {hour:02d}")
        else:
            ok += 1
            print(f"ok   {args.exchange} {hour:02d} {got:,} bytes")
        time.sleep(args.sleep)

    total = sum(f.stat().st_size for f in root.rglob("*") if f.is_file())
    print(f"\nDone {args.date} {args.exchange}: {ok} files, {miss} missing, total {total:,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
