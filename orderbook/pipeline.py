"""
Daily L2 orderbook pipeline: download raw hourly -> build minute snapshots
-> delete raw -> free memory.

Each calendar day is a fully independent unit. Run for one day or a date range.
The raw hourly files are removed after the snapshot is built, so disk and RAM
never accumulate across days (no OOM, no leftover raw data).

Download backends:
  api  - anonymous CryptoHFTData API (download_day.py); slow, sequential.
  r2   - Cloudflare R2 direct path (download_orderbook_s3.py); fast, threaded,
         downloads BOTH exchanges at once; requires CHD_API_KEY in env.

Usage:
  # one day, anonymous API
  python pipeline.py --date 2026-10-12

  # a range of days, fast R2 download
  python pipeline.py --start-date 2025-06-01 --end-date 2025-06-30 --download r2

  # only futures
  python pipeline.py --date 2026-10-12 --exchange binance_futures

Outputs (per day, per exchange):
  data/{date}/{exchange}_minute_band.parquet
  data/{date}/{exchange}_minute_band_mid.parquet

The per-day snapshots can be further aggregated into month-level parquet via
merge_monthly.py (useful when the full narrow table grows too large to load at once).
"""
from __future__ import annotations

import argparse
import gc
import shutil
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import zstandard as zstd

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"  # module-local data root (absolute, cwd-independent)
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
EXCHANGES = ["binance_spot", "binance_futures"]


def normalize_raw(raw_dir: Path) -> None:
    """Make every raw file a real .parquet.

    Files before 2026-08-19 are .parquet.zst. The R2 downloader decompresses in
    stream but keeps the .zst name (so the file is misnamed parquet bytes); the
    anonymous API downloader keeps the raw zst bytes. Normalize by sniffing the
    magic number, then writing a proper .parquet next to it.

    流式解压 + .part 原子改名（2026-10-20）：旧实现 read_bytes() 整个文件再
    write_bytes()，解压后单文件 ~100MB，瞬时 2-3 倍内存；已是裸 parquet 只是
    名字带 .zst 的直接 rename，零拷贝。两个下载器现在下载时即解压，本函数
    只剩历史遗留文件兜底的价值。
    """
    for f in raw_dir.rglob("*"):
        if not f.is_file():
            continue
        name = f.name
        if not (name.endswith(".parquet.zst") or name.endswith(".zst")):
            continue
        out = f.with_name(name.replace(".zst", ""))  # -> BTCUSDT_orderbook.parquet
        with f.open("rb") as fh:
            magic = fh.read(4)
        if magic != ZSTD_MAGIC:
            # 内容已是裸 parquet，只是名字带 .zst：改名即可，无需拷贝
            f.replace(out)
            continue
        tmp = out.with_name(out.name + ".part")
        try:
            with f.open("rb") as src, tmp.open("wb") as dst:
                zstd.ZstdDecompressor().copy_stream(src, dst)
            tmp.replace(out)  # 原子：out 上只会出现完整文件
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        f.unlink()


def run_cmd(args: list[str]) -> int:
    print("+ " + " ".join(args), flush=True)
    return subprocess.run(args, cwd=str(HERE)).returncode


def download(date: str, exchanges: list[str], dest_root: str, backend: str) -> None:
    if backend == "r2":
        # R2 fetches both exchanges in one invocation
        rc = run_cmd([sys.executable, "download_orderbook_s3.py",
                      "--date", date, "--dest-root", dest_root])
    else:
        rc = 0
        for ex in exchanges:
            rc = run_cmd([sys.executable, "download_day.py",
                          "--date", date, "--exchange", ex, "--dest-root", dest_root])
            if rc != 0:
                break
    if rc != 0:
        raise RuntimeError(f"download failed (rc={rc}) for {date}")


def build(date: str, exchanges: list[str], dest_root: str, band: float,
          mode: str) -> None:
    for ex in exchanges:
        rc = run_cmd([sys.executable, "build_minute_snapshots.py",
                      "--date", date, "--exchange", ex, "--band", str(band),
                      "--mode", mode,
                      "--dest-root", dest_root,
                      "--out", f"{dest_root}/{date}/{ex}_minute_band.parquet"])
        if rc != 0:
            raise RuntimeError(f"build failed (rc={rc}) for {ex} {date}")


def cleanup(date: str, exchanges: list[str], dest_root: str) -> None:
    for ex in exchanges:
        band = Path(f"{dest_root}/{date}/{ex}_minute_band.parquet")
        raw = Path(f"{dest_root}/{date}/{ex}")
        if band.exists():
            if raw.exists():
                shutil.rmtree(raw, ignore_errors=True)
                print(f"  removed raw: {raw}", flush=True)
        else:
            print(f"  WARNING band missing for {ex} {date}; keeping raw for debug",
                  file=sys.stderr, flush=True)
    gc.collect()


def process_day(date: str, exchanges: list[str], dest_root: str,
               band: float, backend: str, mode: str) -> None:
    t0 = time.time()
    print(f"\n===== {date} =====", flush=True)
    download(date, exchanges, dest_root, backend)
    for ex in exchanges:
        normalize_raw(Path(f"{dest_root}/{date}/{ex}"))
    build(date, exchanges, dest_root, band, mode)
    cleanup(date, exchanges, dest_root)
    print(f"===== {date} done in {time.time() - t0:.1f}s =====", flush=True)


def _all_built(date: str, exchanges: list[str], dest_root: str) -> bool:
    """band 与 mid 两个文件都在才算完成，避免两步写入之间崩溃导致 mid 永久缺失。"""
    for ex in exchanges:
        for suffix in ("_minute_band.parquet", "_minute_band_mid.parquet"):
            if not Path(f"{dest_root}/{date}/{ex}{suffix}").exists():
                return False
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="single UTC date YYYY-MM-DD")
    ap.add_argument("--start-date", help="range start YYYY-MM-DD (inclusive)")
    ap.add_argument("--end-date", help="range end YYYY-MM-DD (inclusive)")
    ap.add_argument("--exchange", nargs="+", default=EXCHANGES, choices=EXCHANGES)
    ap.add_argument("--dest-root", default=str(DATA),
                    help="Local data root (default: <module>/data)")
    ap.add_argument("--band", type=float, default=0.003)
    ap.add_argument("--mode", choices=["dense", "sparse"], default="dense",
                    help="dense=zero-caveat reference (default); sparse=8x less "
                         "memory, ~15%% faster, auto-falls-back to dense on any "
                         "unprovable minute window (see orderbook_lessons §12)")
    ap.add_argument("--download", choices=["api", "r2"], default="api",
                    help="api=anonymous (slow), r2=fast (needs CHD_API_KEY)")
    args = ap.parse_args()

    exchanges = list(args.exchange)
    if args.download == "r2" and exchanges != EXCHANGES:
        print("NOTE: --download r2 fetches BOTH exchanges; forcing --exchange to both",
              flush=True)
        exchanges = list(EXCHANGES)

    if args.date:
        days = [args.date]
    elif args.start_date and args.end_date:
        s, e = date.fromisoformat(args.start_date), date.fromisoformat(args.end_date)
        if s > e:
            print("--start-date must be <= --end-date", file=sys.stderr)
            return 2
        days = [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]
    else:
        print("specify --date or both --start-date and --end-date", file=sys.stderr)
        return 2

    failed: list[str] = []
    for d in days:
        if _all_built(d, exchanges, args.dest_root):
            print(f"skip {d} (band already built)", flush=True)
            continue
        try:
            process_day(d, exchanges, args.dest_root, args.band, args.download,
                        args.mode)
        except Exception as exc:  # noqa: BLE001 - 单日失败显式记录并继续，不静默吞掉
            print(f"  FAILED {d}: {exc}", file=sys.stderr, flush=True)
            failed.append(d)
    if failed:
        print(f"\n{len(failed)} day(s) failed (NOT marked done, will retry on re-run): "
              f"{', '.join(failed)}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
