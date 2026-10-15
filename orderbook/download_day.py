"""匿名下载 CryptoHFTData 一天的 BTCUSDT L2 订单簿（每小时一个文件）。

用法: python download_day.py --date 2026-10-13 [--exchange binance_futures]
本地布局: data/{date}/{exchange}/{HH}/BTCUSDT_orderbook.parquet
"""
import argparse
import http.client
import sys
import time
from datetime import date
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
API_BASE = "https://api.cryptohftdata.com/v1"
SYMBOL = "BTCUSDT"
DATA_TYPE = "orderbook"
NATIVE_ZSTD_CUTOVER_DATE = "2026-08-19"  # 之后为裸 .parquet，之前为 .parquet.zst
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


def get_object(key: str, retries: int = 5) -> bytes | None:
    url = f"{API_BASE}/download"
    attempt = 0
    rate_limited = 0
    while attempt < retries:
        try:
            with requests.get(
                url, params={"file": key}, stream=True, timeout=300
            ) as resp:
                if resp.status_code == 404:
                    return None
                if resp.status_code == 429:
                    # 限流不消耗重试预算（否则最后一次尝试撞上 429 会直接 RuntimeError
                    # 中断整天），但仍单独计数，避免被永久限流时死循环。
                    rate_limited += 1
                    if rate_limited > retries:
                        raise RuntimeError(f"rate limited {rate_limited}x: {key}")
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
            attempt += 1
            wait = 2 * attempt
            print(f"  attempt {attempt} failed for {key}: {exc}; retry in {wait}s",
                  file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"failed after {retries} retries: {key}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="UTC date YYYY-MM-DD")
    ap.add_argument("--exchange", default="binance_futures")
    ap.add_argument("--dest-root", default=str(HERE / "data"))
    ap.add_argument("--sleep", type=float, default=1.5)
    args = ap.parse_args()

    root = Path(args.dest_root) / args.date / args.exchange
    cutover = date.fromisoformat(args.date) >= date.fromisoformat(NATIVE_ZSTD_CUTOVER_DATE)
    suffixes = (".parquet", ".parquet.zst") if cutover else (".parquet.zst", ".parquet")

    ok = miss = 0
    for hour in range(24):
        dest_dir = root / f"{hour:02d}"
        # pre-cutover 落盘为 .parquet.zst、cutover+ 落盘为 .parquet；两种都视为已存在，
        # 这样任意日期的重跑都是安全无操作，不会重复下载。
        existing = next(
            (dest_dir / f"{SYMBOL}_{DATA_TYPE}{s}" for s in suffixes
             if (dest_dir / f"{SYMBOL}_{DATA_TYPE}{s}").exists()),
            None,
        )
        if existing is not None:
            print(f"skip {args.exchange} {hour:02d} (exists)")
            ok += 1
            continue
        got = None
        for suffix in suffixes:
            key = f"{args.exchange}/{args.date}/{hour:02d}/{SYMBOL}_{DATA_TYPE}{suffix}"
            data = get_object(key)
            if data is None:
                continue
            if not data:
                # 空响应（与 S3 路径的 wire==0 保护对齐）：不当成有效文件落盘，
                # 否则会写出 0 字节 parquet，直到 build 阶段才报一个难懂的错。
                print(f"  empty body for {key}; treating as miss", file=sys.stderr)
                continue
            # 与 R2 下载器保持一致：无论命中哪个 key，落盘均为无压缩的 .parquet，
            # 使两种后端可互换、下游无需按扩展名嗅探魔数。
            if data[:4] == ZSTD_MAGIC:
                import zstandard as zstd
                data = zstd.ZstdDecompressor().decompress(data)
            dest = dest_dir / f"{SYMBOL}_{DATA_TYPE}.parquet"
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
