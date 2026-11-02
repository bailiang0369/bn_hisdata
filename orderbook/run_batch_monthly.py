"""一键自动探测并批量处理全量历史订单簿数据为月度 Parquet 文件。

运行逻辑：
1. 从 Cloudflare R2 / CryptoHFTData 自动探测 2025-06-28 至今所有完整可用的 UTC 日期。
2. 排除数据不完整的日期（例如当天的未全量数据）。
3. 按自然月分组，按天依次执行 pipeline.py（下载原始小时文件 -> 重建分钟快照 -> 清理原始文件）。
4. 某一完整月份的所有天处理完成后，立即调用 merge_monthly.py 聚合为月度 Parquet，并清理已合并的每日窄表，最大程度节省磁盘空间与内存。
5. 当月未完结的月份（例如当前最新月份），处理完可用天后同样进行增量聚合。

用法:
    python orderbook/run_batch_monthly.py [--start-date 2025-06-28] [--end-date 2026-10-15] [--download r2]
"""
from __future__ import annotations

import argparse
import os
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"
EXCHANGES = ["binance_spot", "binance_futures"]


def check_day_complete(creds: dict, d_str: str) -> bool:
    from s3lite import S3LiteClient, S3LiteError
    cl = S3LiteClient(
        endpoint=creds["endpoint"],
        access_key_id=creds["credentials"]["access_key_id"],
        secret_access_key=creds["credentials"]["secret_access_key"],
        session_token=creds["credentials"].get("session_token"),
        region=creds.get("region") or "auto",
        timeout=10,
        max_attempts=2,
    )
    suffixes = (".parquet", ".parquet.zst")
    try:
        for ex in EXCHANGES:
            # 检查第 0 小时和第 23 小时，确认整天数据完整
            for h in (0, 23):
                found = False
                for s in suffixes:
                    key = f"{ex}/{d_str}/{h:02d}/BTCUSDT_orderbook{s}"
                    try:
                        resp = cl.get_object(Bucket=creds["bucket"], Key=key)
                        resp["Body"].close()
                        found = True
                        break
                    except S3LiteError:
                        pass
                if not found:
                    return False
        return True
    finally:
        cl.close()


def discover_available_dates(api_key: str, start_str: str, end_str: str) -> list[str]:
    from download_orderbook_s3 import get_credentials
    print(f"探查数据源完整日期区间 ({start_str} -> {end_str})...", flush=True)
    creds = get_credentials(api_key)

    s = date.fromisoformat(start_str)
    e = date.fromisoformat(end_str)
    all_dates = [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]

    with ThreadPoolExecutor(max_workers=32) as pool:
        futures = {pool.submit(check_day_complete, creds, d): d for d in all_dates}
        results = {d: fut.result() for fut, d in futures.items()}

    valid_dates = [d for d in all_dates if results[d]]
    print(f"探查完成: 共找到 {len(valid_dates)} 个数据完整的 UTC 日期.", flush=True)
    if valid_dates:
        print(f"有效日期范围: {valid_dates[0]} 至 {valid_dates[-1]}", flush=True)
    return valid_dates


def run_cmd(args: list[str]) -> int:
    print("+ " + " ".join(args), flush=True)
    return subprocess.run(args, cwd=str(HERE)).returncode


def process_batch(dates: list[str], dest_root: Path, backend: str) -> None:
    # 按月份 (YYYY-MM) 将日期分组
    month_groups: dict[str, list[str]] = {}
    for d in dates:
        ym = d[:7]
        month_groups.setdefault(ym, []).append(d)

    out_dir = dest_root / "monthly"
    out_dir.mkdir(parents=True, exist_ok=True)

    for ym, m_dates in sorted(month_groups.items()):
        print(f"\n==================== 开始处理月份: {ym} (共 {len(m_dates)} 天) ====================", flush=True)
        for d in m_dates:
            # 调用 pipeline.py 处理单天（内部会自动判断 skip/download/build/cleanup raw）
            code = run_cmd([
                sys.executable, "pipeline.py",
                "--date", d,
                "--download", backend,
                "--dest-root", str(dest_root)
            ])
            if code != 0:
                print(f"ERROR: 处理 {d} 失败，退出码 {code}", file=sys.stderr, flush=True)
                sys.exit(code)

        # 当月天数处理完毕后，立即聚合该月 parquet 并清理每日窄表
        print(f"\n---> 正在合并 {ym} 月度 Parquet 并清理每日窄表...", flush=True)
        code = run_cmd([
            sys.executable, "merge_monthly.py",
            "--dest-root", str(dest_root),
            "--out-dir", str(out_dir),
            "--delete-daily"
        ])
        if code != 0:
            print(f"ERROR: 合并 {ym} 月度 parquet 失败，退出码 {code}", file=sys.stderr, flush=True)
            sys.exit(code)

    print("\n==================== 所有全量日期处理完成！====================", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="2025-06-28", help="探查起始日期 YYYY-MM-DD")
    ap.add_argument("--end-date", default=(date.today()).isoformat(), help="探查截止日期 YYYY-MM-DD")
    ap.add_argument("--dest-root", default=str(DATA), help="数据存储根目录")
    ap.add_argument("--download", choices=["api", "r2"], default="r2", help="下载后端 (默认 r2)")
    args = ap.parse_args()

    api_key = os.environ.get("CHD_API_KEY")
    if not api_key and args.download == "r2":
        print("错误: 使用 r2 下载后端需要环境变量 CHD_API_KEY", file=sys.stderr)
        return 2

    sys.path.insert(0, str(HERE))
    valid_dates = discover_available_dates(api_key, args.start_date, args.end_date)
    if not valid_dates:
        print("未探测到任何完整的可处理日期，程序退出。", file=sys.stderr)
        return 1

    process_batch(valid_dates, Path(args.dest_root), args.download)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
