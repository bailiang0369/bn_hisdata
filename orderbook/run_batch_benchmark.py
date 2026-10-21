"""
Orderbook Batch Benchmark Runner & Performance Logger (Optimized Concurrent Version).

Orchestrates orderbook processing across a date range (e.g. 2025-07-01 to 2026-09-30):
  1. Daily R2 download (download_orderbook_s3.py)
  2. Concurrent minute snapshot building for binance_spot & binance_futures in parallel
  3. Daily raw hourly cleanup
  4. Monthly parquet merging (merge_monthly.py --delete-daily)

Records precise timing metrics and file sizes at every step to a JSON benchmark log.
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"
EXCHANGES = ["binance_spot", "binance_futures"]


def run_cmd(args: list[str]) -> tuple[int, float]:
    """Run command and return (return_code, execution_time_seconds)."""
    t0 = time.time()
    rc = subprocess.run(args, cwd=str(HERE)).returncode
    dt = time.time() - t0
    return rc, dt


def get_dir_size_and_count(path: Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    total_size = 0
    file_count = 0
    for p in path.rglob("*"):
        if p.is_file():
            file_count += 1
            total_size += p.stat().st_size
    return total_size, file_count


def get_file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def cleanup_raw(date_str: str, dest_root: Path) -> tuple[float, int]:
    t0 = time.time()
    removed_bytes = 0
    day_dir = dest_root / date_str
    for ex in EXCHANGES:
        raw_dir = day_dir / ex
        if raw_dir.exists():
            sz, _ = get_dir_size_and_count(raw_dir)
            shutil.rmtree(raw_dir, ignore_errors=True)
            removed_bytes += sz
    gc.collect()
    return time.time() - t0, removed_bytes


def save_results(results: dict, json_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = json_path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    tmp_path.replace(json_path)


def build_snapshot_task(date_str: str, exchange: str, dest_root: Path, band: float, mode: str) -> tuple[str, int, float]:
    day_dir = dest_root / date_str
    out_file = day_dir / f"{exchange}_minute_band.parquet"
    cmd = [
        sys.executable, "build_minute_snapshots.py",
        "--date", date_str,
        "--exchange", exchange,
        "--band", str(band),
        "--mode", mode,
        "--dest-root", str(dest_root),
        "--out", str(out_file),
    ]
    rc, dt = run_cmd(cmd)
    return exchange, rc, dt


def main() -> int:
    ap = argparse.ArgumentParser(description="Orderbook Batch Benchmark Runner")
    ap.add_argument("--start-date", default="2025-07-01", help="YYYY-MM-DD")
    ap.add_argument("--end-date", default="2026-09-30", help="YYYY-MM-DD")
    ap.add_argument("--dest-root", default=str(DATA), help="Local data directory")
    ap.add_argument("--mode", choices=["sparse", "dense"], default="sparse")
    ap.add_argument("--band", type=float, default=0.003)
    ap.add_argument("--out-json", default=str(DATA / "benchmark_results.json"))
    ap.add_argument("--workers", type=int, default=32, help="R2 download workers")
    args = ap.parse_args()

    s = date.fromisoformat(args.start_date)
    e = date.fromisoformat(args.end_date)
    if s > e:
        print("--start-date must be <= --end-date", file=sys.stderr)
        return 2

    dest_root = Path(args.dest_root)
    out_json = Path(args.out_json)

    results = {
        "metadata": {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "mode": args.mode,
            "band": args.band,
            "chd_api_key_present": bool(os.environ.get("CHD_API_KEY")),
            "started_at": datetime.now().isoformat(),
        },
        "daily": {},
        "monthly": {},
    }
    if out_json.exists():
        try:
            with open(out_json, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                results["daily"].update(loaded.get("daily", {}))
                results["monthly"].update(loaded.get("monthly", {}))
        except Exception as exc:
            print(f"Warning: could not read existing benchmark json: {exc}")

    all_days = [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]

    months: dict[str, list[str]] = {}
    for d in all_days:
        months.setdefault(d[:7], []).append(d)

    total_start_time = time.time()

    for ym, month_days in sorted(months.items()):
        print(f"\n==================== MONTH {ym} ({len(month_days)} days) ====================", flush=True)

        for d in month_days:
            day_dir = dest_root / d

            # Skip if already successfully processed in benchmark results
            if d in results["daily"] and results["daily"][d].get("status") == "success":
                print(f"[{d}] skipped (already processed in benchmark)", flush=True)
                continue

            print(f"\n--- Processing {d} ---", flush=True)
            day_stats = {
                "date": d,
                "download_time_s": 0.0,
                "raw_bytes": 0,
                "raw_files": 0,
                "build_spot_time_s": 0.0,
                "spot_band_bytes": 0,
                "spot_mid_bytes": 0,
                "build_futures_time_s": 0.0,
                "futures_band_bytes": 0,
                "futures_mid_bytes": 0,
                "cleanup_time_s": 0.0,
                "total_daily_time_s": 0.0,
                "status": "success",
            }
            t_day_start = time.time()

            # 1. Download
            dl_cmd = [
                sys.executable, "download_orderbook_s3.py",
                "--date", d,
                "--dest-root", str(dest_root),
                "--workers", str(args.workers),
            ]
            rc, t_dl = run_cmd(dl_cmd)
            day_stats["download_time_s"] = round(t_dl, 3)

            raw_bytes, raw_files = 0, 0
            for ex in EXCHANGES:
                b, cnt = get_dir_size_and_count(day_dir / ex)
                raw_bytes += b
                raw_files += cnt
            day_stats["raw_bytes"] = raw_bytes
            day_stats["raw_files"] = raw_files

            if rc != 0:
                print(f"ERROR: download failed for {d}", file=sys.stderr)
                day_stats["status"] = "download_failed"
                results["daily"][d] = day_stats
                save_results(results, out_json)
                continue

            # 2. Build minute snapshots concurrently for spot and futures
            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_spot = pool.submit(build_snapshot_task, d, "binance_spot", dest_root, args.band, args.mode)
                fut_futures = pool.submit(build_snapshot_task, d, "binance_futures", dest_root, args.band, args.mode)

                _, rc_spot, t_spot = fut_spot.result()
                _, rc_fut, t_fut = fut_futures.result()

            day_stats["build_spot_time_s"] = round(t_spot, 3)
            day_stats["build_futures_time_s"] = round(t_fut, 3)
            day_stats["spot_band_bytes"] = get_file_size(day_dir / "binance_spot_minute_band.parquet")
            day_stats["spot_mid_bytes"] = get_file_size(day_dir / "binance_spot_minute_band_mid.parquet")
            day_stats["futures_band_bytes"] = get_file_size(day_dir / "binance_futures_minute_band.parquet")
            day_stats["futures_mid_bytes"] = get_file_size(day_dir / "binance_futures_minute_band_mid.parquet")

            if rc_spot != 0 or rc_fut != 0:
                print(f"ERROR: snapshot build failed for {d} (spot_rc={rc_spot}, fut_rc={rc_fut})", file=sys.stderr)
                day_stats["status"] = "build_failed"
                results["daily"][d] = day_stats
                save_results(results, out_json)
                continue

            # 3. Cleanup raw files
            t_cleanup, _ = cleanup_raw(d, dest_root)
            day_stats["cleanup_time_s"] = round(t_cleanup, 3)
            day_stats["total_daily_time_s"] = round(time.time() - t_day_start, 3)

            results["daily"][d] = day_stats
            save_results(results, out_json)
            print(f"[{d}] Completed in {day_stats['total_daily_time_s']}s "
                  f"(DL: {day_stats['download_time_s']}s, Spot: {day_stats['build_spot_time_s']}s, "
                  f"Fut: {day_stats['build_futures_time_s']}s)", flush=True)

        # Merge month
        print(f"\n--- Merging month {ym} ---", flush=True)
        m_merge_cmd = [
            sys.executable, "merge_monthly.py",
            "--dest-root", str(dest_root),
            "--delete-daily",
            "--force",
        ]
        rc_m, t_m = run_cmd(m_merge_cmd)

        m_stats = {
            "year_month": ym,
            "merge_time_s": round(t_m, 3),
            "spot_band_bytes": get_file_size(dest_root / "monthly" / f"binance_spot_minute_band_{ym}.parquet"),
            "spot_mid_bytes": get_file_size(dest_root / "monthly" / f"binance_spot_minute_band_mid_{ym}.parquet"),
            "futures_band_bytes": get_file_size(dest_root / "monthly" / f"binance_futures_minute_band_{ym}.parquet"),
            "futures_mid_bytes": get_file_size(dest_root / "monthly" / f"binance_futures_minute_band_mid_{ym}.parquet"),
            "status": "success" if rc_m == 0 else "merge_failed",
        }
        results["monthly"][ym] = m_stats
        save_results(results, out_json)
        print(f"Month {ym} merged in {t_m:.2f}s", flush=True)

    results["metadata"]["total_elapsed_s"] = round(time.time() - total_start_time, 3)
    results["metadata"]["finished_at"] = datetime.now().isoformat()
    save_results(results, out_json)
    print(f"\n==================== ALL DONE in {results['metadata']['total_elapsed_s']}s ====================")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
