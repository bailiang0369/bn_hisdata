"""
Batch runner for full historical orderbook data processing (2025-07 through 2026-10).

Processes days sequentially in a single process to allow Polars to fully utilize all
available CPU cores via internal multi-threading, avoiding inter-process overhead.
"""

import gc
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import zstandard as zstd

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
EXCHANGES = ["binance_spot", "binance_futures"]
SUMMARY_JSON_PATH = HERE / "docs" / "full_historical_batch_summary.json"
SUMMARY_MD_PATH = HERE / "docs" / "full_historical_batch_summary.md"


def normalize_raw(raw_dir: Path) -> float:
    t0 = time.time()
    for f in raw_dir.rglob("*"):
        if not f.is_file():
            continue
        name = f.name
        if not (name.endswith(".parquet.zst") or name.endswith(".zst")):
            continue
        data = f.read_bytes()
        if data[:4] == ZSTD_MAGIC:
            data = zstd.ZstdDecompressor().decompress(data)
        out = f.with_name(name.replace(".zst", ""))
        out.write_bytes(data)
        f.unlink()
    return time.time() - t0


def run_cmd(args: list[str]) -> tuple[int, float]:
    t0 = time.time()
    rc = subprocess.run(args, cwd=str(HERE)).returncode
    return rc, time.time() - t0


def process_day(date_str: str, dest_root: str, band: float, mode: str) -> dict:
    print(f"\n===== [POLARS MULTI-THREAD PIPELINE] Processing Day: {date_str} (mode={mode}) =====", flush=True)
    t_day_start = time.time()

    last_err = None
    for attempt in range(3):
        try:
            # 1. Download via R2
            dl_args = [sys.executable, "download_orderbook_s3.py", "--date", date_str, "--dest-root", dest_root]
            rc, t_download = run_cmd(dl_args)
            if rc != 0:
                raise RuntimeError(f"Download failed for {date_str}")

            # 2. Normalize raw files
            t_norm = 0.0
            for ex in EXCHANGES:
                t_norm += normalize_raw(Path(dest_root) / date_str / ex)

            # 3. Build snapshot (--mode sparse)
            t_build = 0.0
            for ex in EXCHANGES:
                b_args = [
                    sys.executable, "build_minute_snapshots.py",
                    "--date", date_str, "--exchange", ex, "--band", str(band), "--mode", mode,
                    "--dest-root", dest_root,
                    "--out", f"{dest_root}/{date_str}/{ex}_minute_band.parquet"
                ]
                rc, dt = run_cmd(b_args)
                if rc != 0:
                    raise RuntimeError(f"Build failed for {ex} on {date_str}")
                t_build += dt

            # 4. Cleanup raw files
            t0_clean = time.time()
            for ex in EXCHANGES:
                raw_path = Path(dest_root) / date_str / ex
                if raw_path.exists():
                    shutil.rmtree(raw_path, ignore_errors=True)
            gc.collect()
            t_cleanup = time.time() - t0_clean

            t_day_total = time.time() - t_day_start

            metrics = {
                "date": date_str,
                "t_download": t_download,
                "t_normalize": t_norm,
                "t_build": t_build,
                "t_cleanup": t_cleanup,
                "t_day_total": t_day_total
            }
            print(f"[{date_str}] DONE total={t_day_total:.2f}s (dl={t_download:.2f}s, build={t_build:.2f}s)", flush=True)
            return metrics
        except Exception as exc:
            last_err = exc
            print(f"Attempt {attempt+1} failed for {date_str}: {exc}. Retrying...", file=sys.stderr, flush=True)
            time.sleep(2)

    raise RuntimeError(f"All 3 attempts failed for {date_str}: {last_err}")


def get_month_days(year: int, month: int) -> list[str]:
    s = date(year, month, 1)
    if month == 12:
        e = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        e = date(year, month + 1, 1) - timedelta(days=1)

    latest = date(2026, 10, 12)
    if e > latest:
        e = latest
    if s > latest:
        return []

    return [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]


def save_batch_summary(monthly_results: list[dict], all_months: list[str]):
    summary_data = {
        "months_total": len(all_months),
        "months_completed": len(monthly_results),
        "monthly_results": monthly_results
    }
    SUMMARY_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_JSON_PATH.write_text(json.dumps(summary_data, indent=2), encoding="utf-8")

    md_lines = [
        "# 单进程 Polars 多线程 Orderbook 数据批量处理统计报告",
        "",
        f"- **目标月份区间**：2025-07 ~ 2026-10 (共 {len(all_months)} 个月份)",
        f"- **已完成月份数**：{len(monthly_results)} / {len(all_months)}",
        "",
        "## 月度处理统计汇总",
        "",
        "| 月份 | 处理天数 | 总耗时(分钟) | 日均下载(s) | 日均快照构建(s) | 月度文件生成 |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |"
    ]

    for mr in monthly_results:
        ym = mr["month"]
        days_cnt = mr["num_days"]
        tot_m = mr["month_wall_time_sec"] / 60
        avg_dl = mr["avg_daily_dl_sec"]
        avg_bld = mr["avg_daily_build_sec"]
        md_lines.append(f"| {ym} | {days_cnt} 天 | {tot_m:.2f} m | {avg_dl:.2f} s | {avg_bld:.2f} s | 已完成 |")

    monthly_dir = DATA / "monthly"
    md_lines.extend(["", "## 已生成的月度 Parquet 产物文件列表", ""])
    if monthly_dir.exists():
        for mf in sorted(monthly_dir.glob("*.parquet")):
            sz_mb = mf.stat().st_size / (1024 * 1024)
            md_lines.append(f"- `{mf.name}`: **{sz_mb:.2f} MB**")

    SUMMARY_MD_PATH.write_text("\n".join(md_lines), encoding="utf-8")


def main():
    if "CHD_API_KEY" not in os.environ:
        print("ERROR: CHD_API_KEY environment variable is missing!", file=sys.stderr)
        sys.exit(1)

    dest_root = str(DATA)

    target_months = []
    for m in range(7, 13):
        target_months.append((2025, m, f"2025-{m:02d}"))
    for m in range(1, 11):
        target_months.append((2026, m, f"2026-{m:02d}"))

    all_month_strs = [m[2] for m in target_months]

    monthly_results = []
    if SUMMARY_JSON_PATH.exists():
        try:
            saved = json.loads(SUMMARY_JSON_PATH.read_text(encoding="utf-8"))
            monthly_results = saved.get("monthly_results", [])
        except Exception:
            monthly_results = []

    completed_months = {mr["month"] for mr in monthly_results}

    print(f"===== [POLARS MULTI-THREAD BATCH] Target Months: {all_month_strs} =====")
    print("Mode: sparse | Execution: Single process sequential (Polars multi-threaded)")

    for y, m, ym_str in target_months:
        spot_band = DATA / "monthly" / f"binance_spot_minute_band_{ym_str}.parquet"
        futures_band = DATA / "monthly" / f"binance_futures_minute_band_{ym_str}.parquet"
        if ym_str in completed_months and spot_band.exists() and futures_band.exists():
            print(f"[SKIP] Month {ym_str} already completed and monthly parquet files exist.", flush=True)
            continue

        days = get_month_days(y, m)
        if not days:
            continue

        print(f"\n==================================================", flush=True)
        print(f"  >>> STARTING MONTH: {ym_str} ({len(days)} days) <<<", flush=True)
        print(f"==================================================", flush=True)

        t_month_start = time.time()
        daily_metrics = []

        for d in days:
            res = process_day(d, dest_root, band=0.003, mode="sparse")
            daily_metrics.append(res)

        daily_metrics.sort(key=lambda x: x["date"])

        print(f"\n===== [POLARS MULTI-THREAD BATCH] Merging Monthly Parquet for {ym_str} =====", flush=True)
        m_args = [
            sys.executable, "merge_monthly.py",
            "--dest-root", dest_root,
            "--delete-daily",
            "--force"
        ]
        rc, t_merge = run_cmd(m_args)
        if rc != 0:
            raise RuntimeError(f"Monthly merge failed for {ym_str}")

        t_month_wall = time.time() - t_month_start

        avg_dl = sum(x["t_download"] for x in daily_metrics) / len(daily_metrics)
        avg_build = sum(x["t_build"] for x in daily_metrics) / len(daily_metrics)

        m_summary = {
            "month": ym_str,
            "num_days": len(days),
            "month_wall_time_sec": t_month_wall,
            "avg_daily_dl_sec": avg_dl,
            "avg_daily_build_sec": avg_build,
            "t_merge_sec": t_merge,
            "daily_metrics": daily_metrics
        }

        monthly_results = [r for r in monthly_results if r["month"] != ym_str]
        monthly_results.append(m_summary)
        monthly_results.sort(key=lambda x: x["month"])

        save_batch_summary(monthly_results, all_month_strs)
        print(f"===== [POLARS MULTI-THREAD BATCH] Month {ym_str} DONE in {t_month_wall/60:.2f} minutes =====", flush=True)

    print("\n[POLARS MULTI-THREAD BATCH] ALL TARGET MONTHS PROCESSED SUCCESSFULLY!", flush=True)


if __name__ == "__main__":
    main()
