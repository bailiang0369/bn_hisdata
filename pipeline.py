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
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
EXCHANGES = ["binance_spot", "binance_futures"]


def normalize_raw(raw_dir: Path) -> None:
    """Make every raw file a real .parquet.

    Files before 2026-08-19 are .parquet.zst. The R2 downloader decompresses in
    stream but keeps the .zst name (so the file is misnamed parquet bytes); the
    anonymous API downloader keeps the raw zst bytes. Normalize by sniffing the
    magic number, then writing a proper .parquet next to it.
    """
    for f in raw_dir.rglob("*"):
        if not f.is_file():
            continue
        name = f.name
        if not (name.endswith(".parquet.zst") or name.endswith(".zst")):
            continue
        data = f.read_bytes()
        if data[:4] == ZSTD_MAGIC:
            data = zstd.ZstdDecompressor().decompress(data)
        out = f.with_name(name.replace(".zst", ""))  # -> BTCUSDT_orderbook.parquet
        out.write_bytes(data)
        f.unlink()


def run_cmd(args: list[str]) -> int:
    print("+ " + " ".join(args), flush=True)
    return subprocess.run(args, cwd=str(HERE)).returncode


def download(date: str, exchanges: list[str], dest_root: str, backend: str) -> None:
    if backend == "r2":
        # R2 fetches both exchanges in one invocation
        run_cmd([sys.executable, "download_orderbook_s3.py",
                 "--date", date, "--dest-root", dest_root])
    else:
        for ex in exchanges:
            run_cmd([sys.executable, "download_day.py",
                     "--date", date, "--exchange", ex, "--dest-root", dest_root])


def build(date: str, exchanges: list[str], dest_root: str, band: float) -> None:
    for ex in exchanges:
        run_cmd([sys.executable, "build_minute_snapshots.py",
                 "--date", date, "--exchange", ex, "--band", str(band),
                 "--out", f"{dest_root}/{date}/{ex}_minute_band.parquet"])


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
               band: float, backend: str) -> None:
    t0 = time.time()
    print(f"\n===== {date} =====", flush=True)
    download(date, exchanges, dest_root, backend)
    for ex in exchanges:
        normalize_raw(Path(f"{dest_root}/{date}/{ex}"))
    build(date, exchanges, dest_root, band)
    cleanup(date, exchanges, dest_root)
    print(f"===== {date} done in {time.time() - t0:.1f}s =====", flush=True)


def _all_built(date: str, exchanges: list[str], dest_root: str) -> bool:
    return all(Path(f"{dest_root}/{date}/{ex}_minute_band.parquet").exists()
               for ex in exchanges)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="single UTC date YYYY-MM-DD")
    ap.add_argument("--start-date", help="range start YYYY-MM-DD (inclusive)")
    ap.add_argument("--end-date", help="range end YYYY-MM-DD (inclusive)")
    ap.add_argument("--exchange", nargs="+", default=EXCHANGES, choices=EXCHANGES)
    ap.add_argument("--dest-root", default="data")
    ap.add_argument("--band", type=float, default=0.003)
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

    for d in days:
        if _all_built(d, exchanges, args.dest_root):
            print(f"skip {d} (band already built)", flush=True)
            continue
        process_day(d, exchanges, args.dest_root, args.band, args.download)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
