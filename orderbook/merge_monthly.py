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

有序性: 每日文件若已在 build 阶段按 bucket 排序（新版产物），按日期顺序 concat 即
天然有序，跳过全局 sort（月级排序是原实现的主要耗时）；遗留的无序每日文件会被自动
单独排序后并入，月份内 bucket 仍严格升序。

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


def _bucket_sorted(path: Path) -> bool:
    """每日文件是否已按 bucket 升序（只读 bucket 单列，列存扫描很快）。"""
    s = pl.scan_parquet(path).select("bucket").collect()
    return s["bucket"].is_sorted()


def _merge(files, out: Path) -> None:
    """按日期顺序流式合并为月份 parquet，保证月份内 bucket 严格升序。

    `build_minute_snapshots.py` 产出的每日文件已按 bucket 排好序，且日与日之间
    bucket 区间不重叠，因此按日期顺序 concat 天然有序——不再需要（昂贵的）月级
    全局 sort。对早期遗留的无序每日文件，单独排序后按原日期位置并入：每个 concat
    段内部有序、段间按日期排列，整体仍然有序。
    """
    parts = []
    n_legacy = 0
    for f in files:
        lf = pl.scan_parquet(f)
        if not _bucket_sorted(f):
            lf = lf.sort("bucket")
            n_legacy += 1
        parts.append(lf)
    lf = pl.concat(parts, how="vertical_relaxed")
    # 先写 .part 再原子改名：续跑判定以"月份文件存在"为准，半截文件会被永久跳过。
    tmp = out.with_name(out.name + ".part")
    try:
        lf.sink_parquet(str(tmp))
        tmp.replace(out)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    if n_legacy:
        print(f"  note: {n_legacy}/{len(files)} 个每日文件为遗留无序产物，已单独排序并入")


def _rows(path: Path) -> int:
    """parquet 行数（读 footer 元数据，不扫数据）。"""
    return int(pl.scan_parquet(path).select(pl.len()).collect().item())


def _stale(out: Path, files) -> bool:
    """月份文件是否落后于每日文件。

    只判"文件存在"会漏掉**回补场景**：先合并了 6 月前半月，之后再补下半月，
    第二次运行会因为月表已存在直接 skip，新数据永远进不去。用行数总和比对
    （元数据级，成本可忽略）能精确发现"每日文件比月表多"。
    """
    try:
        return _rows(out) != sum(_rows(f) for f in files)
    except Exception:  # noqa: BLE001 - 旧文件损坏时宁可重建
        return True


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
            # 逐文件独立判断：只要每日文件比月表多（回补过几天），就重建。
            need_band = bool(b_files) and (
                args.force or not band_out.exists() or _stale(band_out, (f for _, f in b_files)))
            need_mid = bool(m_files) and (
                args.force or not mid_out.exists() or _stale(mid_out, (f for _, f in m_files)))
            if not (need_band or need_mid):
                print(f"[skip] {exchange} {ym}: 月份文件已存在且不落后于每日文件")
                continue
            if need_band:
                _merge(b_files, band_out)
                print(f"[ok]   {exchange} {ym} band: {len(b_files)} 天 -> {band_out.name} "
                      f"({band_out.stat().st_size:,} B)")
                if args.delete_daily:
                    for f in b_files:
                        f.unlink(missing_ok=True)
            if need_mid:
                _merge(m_files, mid_out)
                print(f"[ok]   {exchange} {ym} mid : {len(m_files)} 天 -> {mid_out.name} "
                      f"({mid_out.stat().st_size:,} B)")
                if args.delete_daily:
                    for f in m_files:
                        f.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
