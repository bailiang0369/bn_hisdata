"""匿名下载 CryptoHFTData 一天的 BTCUSDT L2 订单簿（每小时一个文件）。

用法: python download_day.py --date 2026-10-13 [--exchange binance_futures]
本地布局: data/{date}/{exchange}/{HH}/BTCUSDT_orderbook.parquet
"""
import argparse
import http.client
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
API_BASE = "https://api.cryptohftdata.com/v1"
SYMBOL = "BTCUSDT"
DATA_TYPE = "orderbook"
NATIVE_ZSTD_CUTOVER_DATE = "2026-08-19"  # 之后为裸 .parquet，之前为 .parquet.zst
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"


def _parse_retry_after(header: str | None, default: int = 10) -> int:
    """解析 Retry-After：既支持秒数，也支持 RFC 7231 的 HTTP-date 字符串。"""
    if not header:
        return default
    header = header.strip()
    try:
        return int(header)
    except ValueError:
        pass
    for fmt in ("%a, %d %b %Y %H:%M:%S GMT", "%A, %d-%b-%y %H:%M:%S GMT"):
        try:
            then = datetime.strptime(header, fmt).replace(tzinfo=timezone.utc)
            delta = (then - datetime.now(timezone.utc)).total_seconds()
            return max(1, int(delta))
        except ValueError:
            continue
    return default


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
                    retry = _parse_retry_after(resp.headers.get("Retry-After"), 10)
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
    ap.add_argument("--quiet", action="store_true",
                   help="抑制逐小时 ok/404/skip 刷屏，只打印汇总")
    args = ap.parse_args()

    try:
        # 严格 YYYY-MM-DD；提前校验，避免非法日期抛 traceback。
        date.fromisoformat(args.date)
    except ValueError:
        print(f"invalid --date {args.date!r}; expected YYYY-MM-DD (UTC)", file=sys.stderr)
        return 2

    root = Path(args.dest_root) / args.date / args.exchange
    cutover = date.fromisoformat(args.date) >= date.fromisoformat(NATIVE_ZSTD_CUTOVER_DATE)
    suffixes = (".parquet", ".parquet.zst") if cutover else (".parquet.zst", ".parquet")

    ok = miss = err = 0
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
            if not args.quiet:
                print(f"skip {args.exchange} {hour:02d} (exists)")
            ok += 1
            continue
        got = None
        try:
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
                # 注意：必须用流式解压（stream_reader），不能用 ZstdDecompressor().
                # decompress(data)——后者要求帧头内嵌 content size，否则对未知大小
                # 的帧会抛 ZstdError: could not determine content size in frame header，
                # 直接导致整天下载中断。流式解压与 R2 路径等价，兼容任意 zstd 帧。
                if data[:4] == ZSTD_MAGIC:
                    import io
                    import zstandard as zstd
                    data = zstd.ZstdDecompressor().stream_reader(io.BytesIO(data)).read()
                dest = dest_dir / f"{SYMBOL}_{DATA_TYPE}.parquet"
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(data)
                got = len(data)
                break
        except Exception as exc:  # noqa: BLE001 - 单小时失败不中断其余小时（与 R2 路径一致）
            err += 1
            print(f"ERR  {args.exchange} {hour:02d}: {type(exc).__name__}: {exc}",
                  file=sys.stderr)
            time.sleep(args.sleep)
            continue
        if got is None:
            miss += 1
            if not args.quiet:
                print(f"404  {args.exchange} {hour:02d} -> day incomplete, stop remaining hours")
            break  # 当天已有文件缺失，其余小时不再下载
        else:
            ok += 1
            if not args.quiet:
                print(f"ok   {args.exchange} {hour:02d} {got:,} bytes")
        time.sleep(args.sleep)

    total = sum(f.stat().st_size for f in root.rglob("*") if f.is_file())
    print(f"\nDone {args.date} {args.exchange}: {ok} files, {miss} missing, "
          f"{err} errored, total {total:,} bytes")
    return 1 if err else 0


if __name__ == "__main__":
    raise SystemExit(main())
