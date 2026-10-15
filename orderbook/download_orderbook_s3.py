"""
Fast bulk download of BTC L2 order book (orderbook) parquet files from
CryptoHFTData via the R2 direct (S3) path -- the same fast path the SDK's
download_bulk uses, but with no SDK and no pandas.

Flow:
  1. POST /v1/jwt-token  (X-API-Key)            -> JWT
  2. POST /v1/s3-credentials (Bearer)           -> short-lived R2 credentials
  3. Download objects straight from Cloudflare R2 with a thread pool.

Object key layout (mirrored locally into data/{date}/...):
    {exchange}/{date}/{hour:02d}/{symbol}_{data_type}.parquet
e.g. binance_futures/2026-10-12/00/BTCUSDT_orderbook.parquet

Run:
    set CHD_API_KEY=your-key
    python download_orderbook_s3.py --date 2026-10-12
"""

import argparse
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

from s3lite import S3LiteClient, S3LiteError

HERE = Path(__file__).resolve().parent
API_BASE = "https://api.cryptohftdata.com/v1"
EXCHANGES = ["binance_spot", "binance_futures"]
SYMBOL = "BTCUSDT"
DATA_TYPE = "orderbook"
# After this date objects are bare .parquet; before it they are .parquet.zst.
NATIVE_ZSTD_CUTOVER_DATE = "2026-08-19"
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
OBJECT_SUFFIXES = (".parquet.zst", ".parquet")


def get_credentials(api_key: str, timeout: int = 90, retries: int = 5) -> dict:
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = requests.post(
                f"{API_BASE}/jwt-token",
                headers={"Content-Type": "application/json", "X-API-Key": api_key},
                timeout=timeout,
            )
            r.raise_for_status()
            jwt = r.json()["jwt_token"]
            r2 = requests.post(
                f"{API_BASE}/s3-credentials",
                headers={"Authorization": f"Bearer {jwt}"},
                timeout=timeout,
            )
            r2.raise_for_status()
            return r2.json()
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            print(f"  credential fetch attempt {attempt} failed: {repr(exc)[:160]}", file=sys.stderr)
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"Could not fetch R2 credentials: {last_err}")


def _write_payload(body, dest: Path, decompress: bool) -> int:
    """Write an object body, stripping an outer zstd frame if present.

    Returns bytes received on the wire. Mirrors cryptohftdata.bulk.write_payload.
    """
    import zstandard as zstd

    head = b""
    for chunk in body:
        if not chunk:
            continue
        head += chunk
        if len(head) >= len(ZSTD_MAGIC):
            break
    wire = len(head)
    if wire == 0:
        raise RuntimeError("empty object body received")
    wrapped = decompress and head.startswith(ZSTD_MAGIC)
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_name(dest.name + ".part")
    with partial.open("wb") as out:
        if wrapped:
            writer = zstd.ZstdDecompressor().stream_writer(out, closefd=False)
        else:
            writer = out
        try:
            writer.write(head)
            for chunk in body:
                if not chunk:
                    continue
                writer.write(chunk)
                wire += len(chunk)
        finally:
            if wrapped:
                writer.close()
    os.replace(partial, dest)
    return wire


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="UTC date YYYY-MM-DD")
    ap.add_argument("--dest-root", default=str(HERE / "data"),
                    help="Local output root (default: <module>/data)")
    ap.add_argument("--workers", type=int, default=32, help="Concurrent downloads (default 32)")
    ap.add_argument("--hours", type=int, nargs="+", default=None,
                   help="Hours to download, e.g. 0 1 2 (default: all 0-23)")
    args = ap.parse_args()

    api_key = os.environ.get("CHD_API_KEY")
    if not api_key:
        print("CHD_API_KEY environment variable not set", file=sys.stderr)
        return 2

    date = args.date
    creds = get_credentials(api_key, timeout=90, retries=5)
    bucket = creds["bucket"]

    def build_client(cr):
        # Large per-request timeout: a 17 MB futures file at limited egress speed
        # can take well over a minute to stream, so the default 60 s would time out.
        return S3LiteClient(
            endpoint=cr["endpoint"],
            access_key_id=cr["credentials"]["access_key_id"],
            secret_access_key=cr["credentials"]["secret_access_key"],
            session_token=cr["credentials"].get("session_token"),
            region=cr.get("region") or "auto",
            max_pool_connections=args.workers,
            timeout=1800,
            max_attempts=5,
        )

    client = build_client(creds)

    # R2 短期凭证可能在大批量/慢网下过期。worker 遇到鉴权错误时加锁刷新一次，
    # 由首个命中过期的线程执行，其余线程复用新凭证，避免重复关闭正在使用的 client。
    cred_state = {"client": client, "gen": 0}
    cred_lock = threading.Lock()
    AUTH_CODES = {"ExpiredToken", "InvalidToken", "AccessDenied",
                  "SignatureDoesNotMatch", "InvalidAccessKeyId"}

    def refresh_credentials(expected_gen: int) -> None:
        with cred_lock:
            if cred_state["gen"] != expected_gen:
                return  # 已有别的线程刷新过
            new_creds = get_credentials(api_key, timeout=90, retries=5)
            new_client = build_client(new_creds)
            old = cred_state["client"]
            cred_state["client"] = new_client
            cred_state["gen"] += 1
            old.close()

    def _is_auth_error(exc: S3LiteError) -> bool:
        code = exc.response.get("Error", {}).get("Code", "")
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return code in AUTH_CODES or status == 403

    root = Path(args.dest_root) / date
    hours = args.hours if args.hours is not None else list(range(24))
    jobs = [(ex, h) for ex in EXCHANGES for h in hours]

    cutover = datetime.fromisoformat(args.date) >= datetime.fromisoformat(NATIVE_ZSTD_CUTOVER_DATE)
    suffixes = (".parquet", ".parquet.zst") if cutover else (".parquet.zst", ".parquet")

    def worker(exchange: str, hour: int) -> tuple[str, str, int]:
        label = f"{exchange}/{hour:02d}"
        dest = root / exchange / f"{hour:02d}" / f"{SYMBOL}_{DATA_TYPE}.parquet"
        # _write_payload 总是解压，所以无论命中 .parquet 还是 .parquet.zst，落盘都是
        # 无压缩的 .parquet：文件名与内容一致，且跳过判断对 pre-cutover 也成立。
        if dest.exists():
            return (label, "skip", dest.stat().st_size)
        key0 = f"{exchange}/{date}/{hour:02d}/{SYMBOL}_{DATA_TYPE}"
        for suffix in suffixes:
            key = key0 + suffix
            my_gen = cred_state["gen"]
            refreshed = False
            while True:
                cl = cred_state["client"]
                try:
                    resp = cl.get_object(Bucket=bucket, Key=key)
                except S3LiteError as exc:
                    code = exc.response.get("Error", {}).get("Code", "")
                    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                    if code in ("NoSuchKey", "404", "NotFound") or status == 404:
                        break  # 该拼写不存在 -> 尝试另一个后缀，或最后报 miss
                    if _is_auth_error(exc) and not refreshed:
                        refreshed = True
                        # refresh_credentials 内部按 gen 幂等：若别的线程已刷新则为空操作，
                        # 随后用当前（已是最新）client 重试，避免过期瞬间整批误报 err。
                        try:
                            refresh_credentials(my_gen)
                        except Exception:  # noqa: BLE001 - 刷新失败则报 err 而非崩溃
                            return (label, "err:refresh", 0)
                        continue  # 用当前 client 重试本 key
                    return (label, f"err:{code}", 0)
                try:
                    size = _write_payload(_iter_body(resp["Body"]), dest, decompress=True)
                except Exception as exc:  # noqa: BLE001 - 单个小时失败不应中断整轮
                    return (label, f"err:{type(exc).__name__}", 0)
                finally:
                    resp["Body"].close()
                # 完整性校验：落盘字节数应等于响应声明的 Content-Length
                expected = resp.get("ContentLength", 0) or 0
                if expected and size != expected:
                    return (label, f"err:size {size}!={expected}", 0)
                return (label, "ok", size)
        return (label, "404", 0)

    def _iter_body(body):
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                return
            yield chunk

    ok = miss = skip = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(worker, ex, h): (ex, h) for ex, h in jobs}
        for fut in as_completed(futures):
            label, status, size = fut.result()
            if status == "ok":
                ok += 1
                print(f"ok   {label:24s} {size:,} bytes")
            elif status == "skip":
                skip += 1
            elif status == "404":
                miss += 1
                print(f"404  {label}")
            else:
                print(f"ERR  {label}: {status}", file=sys.stderr)
    cred_state["client"].close()

    dt = time.time() - t0
    print(f"\nDone in {dt:.1f}s: {ok} downloaded, {skip} present, {miss} missing for {date}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
