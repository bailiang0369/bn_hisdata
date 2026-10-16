"""多进程跨天加速批量处理与月度汇总脚本。

通过 ProcessPoolExecutor 并行运行多天 pipeline.py 构建，显著压榨多核 CPU 性能。
处理完成后按月调用 merge_monthly.py 进行增量/全量合并，并及时清理每日临时文件。

用法:
    python orderbook/run_batch_monthly_parallel.py --start-date 2026-09-01 --end-date 2026-09-30 --workers 4
"""
from __future__ import annotations

import argparse
import os
import sys
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"


def run_pipeline_day(d: str, dest_root: str, backend: str) -> tuple[str, int]:
    cmd = [
        sys.executable, "pipeline.py",
        "--date", d,
        "--download", backend,
        "--dest-root", dest_root
    ]
    res = subprocess.run(cmd, cwd=str(HERE))
    return (d, res.returncode)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", required=True, help="起始日期 YYYY-MM-DD")
    ap.add_argument("--end-date", required=True, help="截止日期 YYYY-MM-DD")
    ap.add_argument("--workers", type=int, default=2, help="跨天并行进程数 (建议 2-4)")
    ap.add_argument("--dest-root", default=str(DATA), help="数据存储根目录")
    ap.add_argument("--download", choices=["api", "r2"], default="r2", help="下载后端")
    args = ap.parse_args()

    s = date.fromisoformat(args.start_date)
    e = date.fromisoformat(args.end_date)
    days = [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]

    dest_root = Path(args.dest_root)
    out_dir = dest_root / "monthly"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"开始并行构建 {len(days)} 天的数据 ({args.start_date} -> {args.end_date}), workers={args.workers}...", flush=True)

    # 按月份分组
    month_groups: dict[str, list[str]] = {}
    for d in days:
        ym = d[:7]
        month_groups.setdefault(ym, []).append(d)

    for ym, m_dates in sorted(month_groups.items()):
        print(f"\n==================== 并行处理月份 {ym} (共 {len(m_dates)} 天) ====================", flush=True)
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(run_pipeline_day, d, str(dest_root), args.download): d for d in m_dates}
            for fut in as_completed(futures):
                d, code = fut.result()
                if code != 0:
                    print(f"ERROR: 日期 {d} 处理失败, 退出码 {code}", file=sys.stderr, flush=True)
                    return code
                print(f"[OK]   {d} 阶段构建完成", flush=True)

        print(f"\n---> 正在合并 {ym} 月度 Parquet 并清理每日窄表...", flush=True)
        res = subprocess.run([
            sys.executable, "merge_monthly.py",
            "--dest-root", str(dest_root),
            "--out-dir", str(out_dir),
            "--delete-daily",
            "--force"
        ], cwd=str(HERE))
        if res.returncode != 0:
            print(f"ERROR: 合并 {ym} 月度失败", file=sys.stderr, flush=True)
            return res.returncode

    print("\n==================== 批次构建与月度合并完全完成！====================", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
