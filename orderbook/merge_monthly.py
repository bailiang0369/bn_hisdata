"""将 pipeline 产出的每日窄表按 (交易所, 月份) 合并为月份级 parquet。

每日产物:
  data/{date}/{exchange}_minute_band.parquet
  data/{date}/{exchange}_minute_band_mid.parquet
月份产物（默认输出到 data/monthly/）:
  data/monthly/{exchange}_minute_band_{YYYY-MM}.parquet
  data/monthly/{exchange}_minute_band_mid_{YYYY-MM}.parquet

用途: 全量窄表可能 >8GB，按日文件过多、整体一次性加载又过大；按月折中，便于后期
按特征计算时逐月 lazy 加载 / 流式处理。月份由日期文件夹名推导（每日数据天然属于
同一 UTC 日历月，不会跨月切割）。

内存安全: 合并用 polars 惰性 concat + sink_parquet 流式写出，单月数据也不会整月入内存。

用法:
  python orderbook/merge_monthly.py --dest-root orderbook/data
  python orderbook/merge_monthly.py --dest-root orderbook/data --exchange binance_futures
  python orderbook/merge_monthly.py --dest-root orderbook/data --delete-daily   # 合并后删每日文件
  python orderbook/merge_monthly.py --dest-root orderbook/data --force          # 强制重建已存在的月份
"""
import argparse
import re
from pathlib import Path

import polars as pl

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
EXCHANGES = ["binance_futures", "binance_spot"]
SYMBOL = "BTCUSDT"
DATA_TYPE = "orderbook"


def collect_days(dest_root: Path, exchange: str):
    """返回 [(date, band_file), ...] 与 [(date, mid_file), ...]，仅含真实存在的文件。"""
    band, mid = [], []
    for d in sorted(dest_root.iterdir()):
        if not d.is_dir() or not DATE_RE.match(d.name):
            continue
        b = d / f"{exchange}_minute_band.parquet"
        m = d / f"{exchange}_minute_band_mid.parquet"
        if b.exists():
            band.append((d.name, b))
        if m.exists():
            mid.append((d.name, m))
    return band, mid


def _merge(files, out: Path) -> None:
    # bucket 列必存在（每日窄表的绝对分钟时间戳），直接排序保证月份内有序
    lf = pl.concat([pl.scan_parquet(f) for f in files], how="vertical_relaxed")
    lf = lf.sort("bucket")
    lf.sink_parquet(str(out))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dest-root", required=True, help="pipeline 输出根目录（含 {date}/ 子目录）")
    ap.add_argument("--exchange", choices=EXCHANGES, default=None,
                    help="只合并某个交易所；默认两个都处理")
    ap.add_argument("--out-dir", default=None, help="月份文件输出目录，默认 <dest-root>/monthly")
    ap.add_argument("--delete-daily", action="store_true",
                    help="合并成功后删除对应的每日 band/mid 文件（节省空间）")
    ap.add_argument("--force", action="store_true", help="即使月份文件已存在也重建")
    args = ap.parse_args()

    dest_root = Path(args.dest_root)
    out_dir = Path(args.out_dir) if args.out_dir else dest_root / "monthly"
    out_dir.mkdir(parents=True, exist_ok=True)
    exchanges = [args.exchange] if args.exchange else EXCHANGES

    for exchange in exchanges:
        band_days, mid_days = collect_days(dest_root, exchange)
        if not band_days and not mid_days:
            print(f"[skip] {exchange}: 无每日窄表")
            continue
        groups: dict[str, list] = {}
        for name, f in band_days:
            groups.setdefault(name[:7], [[], []])[0].append(f)
        for name, f in mid_days:
            groups.setdefault(name[:7], [[], []])[1].append(f)

        for ym, (b_files, m_files) in sorted(groups.items()):
            band_out = out_dir / f"{exchange}_minute_band_{ym}.parquet"
            mid_out = out_dir / f"{exchange}_minute_band_mid_{ym}.parquet"
            rebuild = args.force or (not band_out.exists()) or (not mid_out.exists())
            if not rebuild:
                print(f"[skip] {exchange} {ym}: 月份文件已存在")
                continue
            if b_files and (args.force or not band_out.exists()):
                _merge(b_files, band_out)
                print(f"[ok]   {exchange} {ym} band: {len(b_files)} 天 -> {band_out.name} "
                      f"({band_out.stat().st_size:,} B)")
                if args.delete_daily:
                    for f in b_files:
                        f.unlink(missing_ok=True)
            if m_files and (args.force or not mid_out.exists()):
                _merge(m_files, mid_out)
                print(f"[ok]   {exchange} {ym} mid : {len(m_files)} 天 -> {mid_out.name} "
                      f"({mid_out.stat().st_size:,} B)")
                if args.delete_daily:
                    for f in m_files:
                        f.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
