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
    try:
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
    except BaseException:
        # 失败不留下 .part 残file：多次失败会堆积孤儿文件，而续传判断只看 dest。
        partial.unlink(missing_ok=True)
        raise
    return wire


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="UTC date YYYY-MM-DD")
    ap.add_argument("--dest-root", default=str(HERE / "data"),
                    help="Local output root (default: <module>/data)")
    ap.add_argument("--workers", type=int, default=32, help="Concurrent downloads (default 32)")
    ap.add_argument("--hours", type=int, nargs="+", default=None,
                   help="Hours to download, e.g. 0 1 2 (default: all 0-23)")
    ap.add_argument("--quiet", action="store_true",
                   help="只打印每日汇总与缺数天探测行，抑制逐文件 ok/skip/abort 刷屏")
    args = ap.parse_args()

    api_key = os.environ.get("CHD_API_KEY")
    if not api_key:
        print("CHD_API_KEY environment variable not set", file=sys.stderr)
        return 2

    date = args.date
    try:
        # 严格 YYYY-MM-DD；提前校验，避免非法日期先打一次凭证请求再抛 traceback。
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        print(f"invalid --date {date!r}; expected YYYY-MM-DD (UTC)", file=sys.stderr)
        return 2
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

    # R2 短期凭证可能在大批量/慢网下过期。只有"令牌过期/失效"类错误刷新凭证才有意义；
    # AccessDenied / InvalidAccessKeyId / SignatureDoesNotMatch 说明 API key 本身有问题
    # （写错、被吊销、签名不匹配），刷新只会拿到同样无效的凭证，且在多线程下会触发
    # 重复的凭证拉取风暴，因此这些错误直接判失败、不刷新。
    cred_state = {"client": client, "gen": 0}
    cred_lock = threading.Lock()
    retired_clients: list = []  # 被换下的旧 client，统一在线程池结束后关闭
    TOKEN_EXPIRY_CODES = {"ExpiredToken", "InvalidToken"}

    def refresh_credentials(expected_gen: int) -> None:
        with cred_lock:
            if cred_state["gen"] != expected_gen:
                return  # 已有别的线程刷新过
            new_creds = get_credentials(api_key, timeout=90, retries=5)
            new_client = build_client(new_creds)
            old = cred_state["client"]
            cred_state["client"] = new_client
            cred_state["gen"] += 1
            # 不立即 close：此刻其他线程可能仍在旧 client 的连接上跑在途请求，
            # 立即关闭会把这些请求打断成 err。延后到线程池全部结束再统一关闭。
            retired_clients.append(old)

    def _is_token_expired(exc: S3LiteError) -> bool:
        # R2/S3 对过期临时凭证会返回带 <Code>ExpiredToken</Code> 的错误体；
        # s3lite.parse_error_response 已兼容有/无命名空间两种形态，能拿到真实错误码。
        code = exc.response.get("Error", {}).get("Code", "")
        return code in TOKEN_EXPIRY_CODES

    root = Path(args.dest_root) / date
    hours = args.hours if args.hours is not None else list(range(24))
    jobs = [(ex, h) for ex in EXCHANGES for h in hours]

    # 当天任意文件确认缺失（两个后缀都 404）即认为整日缺失：其余排队中的文件不再下载，
    # 立刻节省 CHD 额度。已在途的请求无法取消，但排队中的会被跳过。
    day_abort = threading.Event()

    cutover = datetime.fromisoformat(args.date) >= datetime.fromisoformat(NATIVE_ZSTD_CUTOVER_DATE)
    suffixes = (".parquet", ".parquet.zst") if cutover else (".parquet.zst", ".parquet")

    def worker(exchange: str, hour: int) -> tuple[str, str, int]:
        label = f"{exchange}/{hour:02d}"
        if day_abort.is_set():
            return (label, "abort", 0)
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
                    if _is_token_expired(exc) and not refreshed:
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
                # 日志里上报"落盘后实际文件大小"（解压后），而非线上的压缩字节数，
                # 避免把解压文件的真实体积误报成压缩体积。
                return (label, "ok", dest.stat().st_size)
        # 两个后缀都拿不到 -> 该小时文件确实缺失 -> 整日视为缺失，停止当天其余下载
        day_abort.set()
        return (label, "404", 0)

    def _iter_body(body):
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                return
            yield chunk

    def _probe_exists(exchange: str, hour: int):
        """探测某小时文件是否存在。True=存在，False=两后缀均 404（确认缺失），None=不确定。"""
        key0 = f"{exchange}/{date}/{hour:02d}/{SYMBOL}_{DATA_TYPE}"
        for suffix in suffixes:
            try:
                resp = cred_state["client"].get_object(Bucket=bucket, Key=key0 + suffix)
                resp["Body"].close()  # 只需存在性，不读完整 body
                return True
            except S3LiteError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if code in ("NoSuchKey", "404", "NotFound") or status == 404:
                    continue
                return None
        return False

    # ---- 阶段 1：先探测当天第一个小时（两交易所各 1 个请求）----
    # 任一确认 404 -> 整日视为缺失，置 day_abort 后所有任务直接跳过，
    # 一个多余请求都不发（缺数天成本从 ~32 个请求降到 2 个）。
    first_hour = hours[0]
    probe = {ex: _probe_exists(ex, first_hour) for ex in EXCHANGES}
    if any(r is False for r in probe.values()):
        day_abort.set()
        _p = ", ".join(f"{ex}={'MISSING' if r is False else ('ok' if r else 'unknown')}"
                       for ex, r in probe.items())
        print(f"probe {date} hour {first_hour:02d}: {_p} -> day incomplete, "
              f"skip all {len(jobs)} downloads")

    ok = miss = skip = err = abort = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(worker, ex, h): (ex, h) for ex, h in jobs}
        for fut in as_completed(futures):
            label, status, size = fut.result()
            if status == "ok":
                ok += 1
                if not args.quiet:
                    print(f"ok   {label:24s} {size:,} bytes")
            elif status == "skip":
                skip += 1
            elif status == "404":
                miss += 1
                if not args.quiet:
                    print(f"404  {label}")
            elif status == "abort":
                abort += 1
            else:
                err += 1
                print(f"ERR  {label}: {status}", file=sys.stderr)
    # 线程池已全部结束（无在途请求），此时关闭当前 client 与被换下的旧 client 都安全。
    cred_state["client"].close()
    for old in retired_clients:
        old.close()

    dt = time.time() - t0
    print(f"\nDone in {dt:.1f}s: {ok} downloaded, {skip} present, {miss} missing, "
          f"{abort} aborted(day-missing), {err} errored for {date}")
    if err:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
