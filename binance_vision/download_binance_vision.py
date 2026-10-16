"""
Download Binance public market data from https://data.binance.vision (S3).

Supports
  markets   : spot, futures/um  (--market both downloads both)
  symbols   : BTCUSDT, ETHUSDT  (--symbol all downloads both)
  categories: klines (needs --interval), metrics (um only). aggTrades is
              selectable via --category aggTrades but excluded from "all" for now
  granularity: daily  -> one zip per day   (good for testing / a few days)
               monthly -> one zip per month (much faster for bulk backfills)

Disk layout (under <module>/data by default):
  data/{market}/{granularity}/{category}/{symbol}/{interval?}/{symbol}-{type}-{period}.{parquet|csv}
  The source .zip is downloaded to the same folder, processed into this cleaned
  parquet (default) or CSV (--format csv), and then deleted (the zip is only a
  transient intermediate). Resume is keyed on the cleaned output file, so
  re-running is a safe no-op once it exists.

NOTE: historical Binance CSVs are inconsistent -- some periods have a header
row, others don't. process() detects this and always emits a clean, typed
output, then the source .zip is deleted. Two more format facts learned from
real files: aggTrades has 8 columns (the last is is_best_match, easy to miss),
and metrics' create_time is a datetime string ("2025-06-01 00:00:00"), not an
ms epoch -- it is converted so every timestamp in the output is Int64 ms.
klines output keeps open_time,
close_time + OHLC + count + buy_vol/sell_vol and drops volume /
quote_volume / taker_buy_quote_volume / ignore, where the taker-buy volume
buy_vol = taker_buy_volume and the taker-sell volume sell_vol = volume -
taker_buy_volume. close_time (+1ms) aligns to orderbook snapshots
(== next candle open_time).

Output defaults to parquet rather than CSV: typed columns (no re-parsing
strings on every read), much smaller on disk (zstd), columnar reads for feature
building, and consistent with the orderbook module which already emits parquet.

Both stages stream. The zip is downloaded in 1 MB chunks straight to disk
(sha256 is computed on the fly, so checksum verification never re-reads it),
and the inner CSV is expanded to a temp file, then parsed with scan_csv ->
sink_parquet/sink_csv in one pass -- the frame is never materialized. Measured
on a synthetic 6M-row / 400 MB aggTrades CSV: 533 MB peak RSS vs 771 MB for
the old read_csv -> write_parquet path; handing the zip entry to polars
directly is worse still (924 MB -- it buffers the whole file object to make it
seekable). At real monthly aggTrades scale (GBs uncompressed) that difference
is what keeps the run alive on an 8 GB box. The temp CSV costs transient disk
of roughly the CSV size and is always deleted afterwards.

Downloads and outputs land via a ".part" temp file + atomic rename. An
interrupted run can therefore never leave a truncated zip behind that a later
run would reuse as a "complete" download, nor a half-written output that the
resume check would mistake for finished work.

Float columns are float32 by default (--dtype float64 to opt out). At a ~100k
BTC price float32's ULP is ~0.008, i.e. a relative error of ~4e-8 -- far below
anything that matters for feature computation -- and it halves those columns.
Timestamps stay Int64 (ms epoch ~1.7e12 overflows int32). NOTE: do NOT apply
float32 to the orderbook module's px: above 131072 the ULP (0.0156) exceeds the
spot tick (0.01) and adjacent price levels would collapse into one value.

Scope decisions (Binance Vision layout):
  - spot -> klines only (spot has no metrics / fundingRate).
  - futures/um -> klines + metrics; metrics is daily-only (monthly auto-skipped).
  - fundingRate is a separate, monthly-only category, intentionally NOT
    downloaded: the spot/futures basis is computed from klines closes.

Every .zip has a sibling .CHECKSUM (sha256); verified by default. Already
downloaded files are skipped, so re-running is a safe resume.

Examples
  # test: a few days of BTC, U-margined futures, 1m klines + metrics
  python binance_vision/download_binance_vision.py --market um --symbol BTCUSDT \
      --category all --interval 1m --granularity daily \
      --start-date 2026-10-10 --end-date 2026-10-12

  # one calendar month of klines (e.g. 2025-06): ONE zip per market/symbol.
  # NOTE: monthly ignores the day-of-month and downloads whole months.
  python binance_vision/download_binance_vision.py --market both --symbol all \
      --category klines --interval 1m --granularity monthly \
      --start-date 2025-06-01 --end-date 2025-06-30

  # ... plus metrics for the same month. Binance publishes metrics per DAY
  # only, so --granularity monthly auto-skips it (see the run summary);
  # a full month of metrics needs one daily run (30 files per symbol here).
  python binance_vision/download_binance_vision.py --market um --symbol all \
      --category metrics --granularity daily \
      --start-date 2025-06-01 --end-date 2025-06-30
"""
import argparse
import hashlib
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import requests

HERE = Path(__file__).resolve().parent
DATA = HERE / "data"
BASE = "https://data.binance.vision/data"

VALID_INTERVALS = {
    "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h",
    "6h", "8h", "12h", "1d", "3d", "1w", "1mo",
}
MARKETS = {"spot": "spot", "um": "futures/um"}
CATEGORIES = ["klines", "aggTrades", "metrics"]  # trades omitted: aggTrades suffices

# "all" downloads klines + metrics; aggTrades is intentionally excluded for now
# (kept selectable via --category aggTrades once its usefulness is decided).
ALL_CATS = ["klines", "metrics"]

# Canonical column order per category. Used (a) when a file has NO header row
# -- some historical Binance CSVs omit the header -- and (b) to detect headers
# by checking the first token against EXPECTED_FIRST.
KLINE_COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
              "quote_volume", "count", "taker_buy_volume",
              "taker_buy_quote_volume", "ignore"]
# klines output: keep open_time + close_time + OHLC + count + taker buy/sell
# volume (buy_vol / sell_vol); drop volume, quote_volume, taker_buy_quote_volume
# and ignore. 主动买 buy_vol = taker_buy_volume；主动卖 sell_vol = volume -
# taker_buy_volume。close_time (+1ms) is the candle's end instant and the natural
# join key to orderbook snapshots (close_time + 1 == next candle's open_time).
# open_time is kept too since it is that same next-candle join key viewed from
# the preceding bar.
KLINE_KEEP = ["open_time", "open", "high", "low", "close", "close_time",
              "count", "buy_vol", "sell_vol"]
METRICS_COLS = ["create_time", "symbol", "sum_open_interest",
                "sum_open_interest_value", "count_toptrader_long_short_ratio",
                "sum_toptrader_long_short_ratio", "count_long_short_ratio",
                "sum_taker_long_short_vol_ratio"]
# aggTrades 实际是 8 列：最后一列 is_best_match（2022 年至今的真实文件均如此，
# 旧版只写了 7 列，会导致输出多出一个无名的 column_8）。
AGGTRADES_COLS = ["agg_trade_id", "price", "quantity", "first_trade_id",
                  "last_trade_id", "transact_time", "is_buyer_maker",
                  "is_best_match"]
CANONICAL = {"klines": KLINE_COLS, "metrics": METRICS_COLS,
             "aggTrades": AGGTRADES_COLS}
# First column name when a header IS present (to detect header-less files).
EXPECTED_FIRST = {"klines": "open_time", "metrics": "create_time",
                  "aggTrades": "agg_trade_id"}


def period_strings(granularity: str, start: date, end: date):
    if granularity == "daily":
        d = start
        while d <= end:
            yield d.strftime("%Y-%m-%d")
            d += timedelta(days=1)
    else:  # monthly
        y, m = start.year, start.month
        while (y, m) <= (end.year, end.month):
            yield f"{y:04d}-{m:02d}"
            m += 1
            if m > 12:
                m, y = 1, y + 1


def url_for(market_path: str, granularity: str, category: str,
            symbol: str, interval: str, period: str) -> str:
    if category == "klines":
        return (f"{BASE}/{market_path}/{granularity}/klines/{symbol}/{interval}/"
                f"{symbol}-{interval}-{period}.zip")
    return (f"{BASE}/{market_path}/{granularity}/{category}/{symbol}/"
            f"{symbol}-{category}-{period}.zip")


def dest_path(data_root: Path, market_key: str, granularity: str, category: str,
              symbol: str, interval: str, period: str) -> Path:
    if category == "klines":
        return (data_root / market_key / granularity / category / symbol /
                interval / f"{symbol}-{interval}-{period}.zip")
    return (data_root / market_key / granularity / category / symbol /
            f"{symbol}-{category}-{period}.zip")


def fetch_expected_sha256(url_checksum: str, log) -> str | None:
    """GET the sibling .CHECKSUM file; return the expected sha256 hex.

    None means "cannot verify" (missing/empty/unreachable) and the caller
    skips verification. Verification is a safety net, never a hard gate:
    only a real mismatch makes the caller discard the download.
    """
    try:
        r = requests.get(url_checksum, timeout=30)
        if r.status_code != 200:
            log(f"no checksum file (HTTP {r.status_code}), skipped")
            return None
        text = r.text.strip()
        if not text:
            log("empty checksum file, skipped")
            return None
        return text.split()[0].lower()
    except Exception as e:  # 校验是安全网：取不到就跳过，绝不误伤正常下载
        log(f"checksum check skipped: {e}")
        return None


def download_one(url: str, dest: Path, verify: bool, log) -> str:
    if dest.exists():
        log("skip (exists)")
        return "skip"
    dest.parent.mkdir(parents=True, exist_ok=True)
    # 先写 .part 再原子改名：中断/崩溃留下的残件永远不会被当成完整 zip
    # 复用（旧实现直接写 dest，Ctrl-C 会留下截断文件，下次运行 process 必败，
    # 且要再跑一次脚本才能真正下载到）。
    tmp = dest.parent / (dest.name + ".part")
    last = None
    for attempt in range(1, 4):
        try:
            h = hashlib.sha256()
            size = 0
            with requests.get(url, timeout=(30, 120), stream=True) as r:
                if r.status_code == 200:
                    # 流式落盘：不再把整个 zip 读进内存（月度 aggTrades 数百 MB），
                    # 同时边写边算 sha256，校验无需回读全文。
                    with tmp.open("wb") as fh:
                        for chunk in r.iter_content(chunk_size=1 << 20):
                            if chunk:
                                fh.write(chunk)
                                h.update(chunk)
                                size += len(chunk)
                    expected = (fetch_expected_sha256(url + ".CHECKSUM", log)
                                if verify else None)
                    if expected is not None and expected != h.hexdigest():
                        # 校验和真的不匹配 -> 丢弃本次响应并重试，绝不写出损坏文件
                        log(f"CHECKSUM MISMATCH: expected {expected}, "
                            f"got {h.hexdigest()}")
                        last = "checksum mismatch"
                    else:
                        if expected is not None:
                            log("checksum OK")
                        tmp.replace(dest)  # 原子：dest 上只会出现完整文件
                        log(f"saved {size / 1e6:.1f} MB")
                        return "ok"
                elif r.status_code == 404:
                    log(f"MISSING (HTTP 404): {url}")
                    return "missing"
                else:
                    last = r.status_code
        except Exception as e:  # transient network error -> retry
            last = e
        tmp.unlink(missing_ok=True)  # 残件不影响下次尝试（open("wb") 会截断）
        if attempt < 3:
            time.sleep(2 ** attempt)  # 指数退避，避免连续猛打源站
            log(f"attempt {attempt} failed ({last}); retrying")
    log(f"FAILED after retries: {url}")
    return "failed"


def process(zip_path: Path, category: str, fmt: str, fdtype, log) -> None:
    """Read the zip's CSV, normalize headers, trim klines, write a typed file.

    Binance historical files are inconsistent: some periods ship a header row,
    others don't. We detect this from the first token and always emit a clean,
    typed output next to the .zip (klines trimmed per KLINE_KEEP).

    Streaming end to end: the zip entry is expanded to a ".src.part" temp
    file, then parsed with scan_csv and sunk to parquet/CSV in one pass, so
    the frame is never materialized. Why the temp file instead of handing the
    zip entry to polars directly: polars buffers a Python file object whole to
    make it seekable -- measured on a 6M-row / 400 MB CSV, that alone cost
    ~400 MB of RSS (924 MB peak via file object vs 533 MB via path). Monthly
    aggTrades are GBs uncompressed, so this is the difference between working
    and OOM-ing on an 8 GB box. The trade is transient disk usage of roughly
    the CSV size; the temp file is always removed in a finally block.
    """
    out = zip_path.with_suffix(f".{fmt}")
    if out.exists():
        return
    # 输出先写 .part 再原子改名：sink 中途失败不会留下"看起来已完成"的输出
    # 文件（resume 以输出文件存在为准，残件会被误判为完成）。
    # 注意两个临时名不能撞车：--format csv 时 out 的后缀恰好是 .csv，
    # 若源临时文件也叫 .csv.part，sink 会覆盖自己正在读的文件（Windows 下
    # 直接 os error 1006）。源用 .src.part，输出用 .<fmt>.part。
    tmp_out = out.parent / (out.name + ".part")
    tmp_csv = out.parent / (zip_path.stem + ".src.part")
    try:
        with zipfile.ZipFile(zip_path) as z:
            csv_in = [n for n in z.namelist() if n.endswith(".csv")]
            if not csv_in:
                log("no csv inside zip")
                return
            with z.open(csv_in[0]) as src, tmp_csv.open("wb") as dst:
                while True:
                    chunk = src.read(1 << 20)
                    if not chunk:
                        break
                    dst.write(chunk)
        with tmp_csv.open("rb") as fh:
            first = fh.readline().decode("utf-8", errors="replace")
            if not first.strip():
                log("empty csv inside zip")
                return
            # Detect header by the first token (a header's first col name is text;
            # a data row's first field is a timestamp/number).
            exp = EXPECTED_FIRST.get(category)
            is_header = bool(exp) and first.split(",")[0].strip() == exp
        # lazy scan：sink 之前不物化。按路径扫描（mmap 流式），不要直接传
        # zip 条目/file object——见 docstring 的实测说明。
        if is_header:
            lf = pl.scan_csv(tmp_csv, has_header=True)
        else:
            lf = pl.scan_csv(tmp_csv, has_header=False,
                             new_columns=CANONICAL[category])
        if category == "klines":
            # 主动买成交量 buy_vol = taker_buy_volume；主动卖成交量 sell_vol = 总量 - 主动买。
            # volume / quote_volume / taker_buy_quote_volume / ignore 一律丢弃。
            # 时间戳与笔数保持 Int64（毫秒戳 ~1.7e12，int32 会溢出）；价格与成交量用
            # fdtype（默认 Float32：BTC 价位下 ULP≈0.008、相对误差 ~4e-8，对特征计算无影响）。
            # 减法在 cast 之前按全精度算，不会先丢精度再相减。
            lf = lf.with_columns([
                pl.col("taker_buy_volume").cast(fdtype).alias("buy_vol"),
                (pl.col("volume") - pl.col("taker_buy_volume")).cast(fdtype).alias("sell_vol"),
            ]).select(KLINE_KEEP).with_columns([
                pl.col("open_time").cast(pl.Int64),
                pl.col("open").cast(fdtype),
                pl.col("high").cast(fdtype),
                pl.col("low").cast(fdtype),
                pl.col("close").cast(fdtype),
                pl.col("close_time").cast(pl.Int64),
                pl.col("count").cast(pl.Int64),
                pl.col("buy_vol").cast(fdtype),
                pl.col("sell_vol").cast(fdtype),
            ])
        elif category == "aggTrades":
            lf = lf.with_columns([
                pl.col("agg_trade_id").cast(pl.Int64),
                pl.col("first_trade_id").cast(pl.Int64),
                pl.col("last_trade_id").cast(pl.Int64),
                pl.col("transact_time").cast(pl.Int64),
                pl.col("price").cast(fdtype),
                pl.col("quantity").cast(fdtype),
                pl.col("is_buyer_maker").cast(pl.Boolean),  # 1 字节 -> 1 位
                pl.col("is_best_match").cast(pl.Boolean),
            ])
        elif category == "metrics":
            # create_time 在 metrics CSV 里是日期时间字符串
            # （"2025-06-01 00:00:00"，2023-2025 真实文件实测一致），不是毫秒
            # epoch——早期实现直接 cast(Int64) 在这里必炸。统一转成 ms epoch
            # 与 klines 的 open_time/close_time 对齐，便于 join。按扫描出的
            # schema 判断：字符串走 strptime，万一遇到 epoch 整数则直接 cast。
            if lf.collect_schema()["create_time"].is_numeric():
                create_time = pl.col("create_time").cast(pl.Int64)
            else:
                create_time = (pl.col("create_time")
                               .str.to_datetime(format="%Y-%m-%d %H:%M:%S")
                               .dt.epoch("ms").cast(pl.Int64))
            lf = lf.with_columns([
                create_time,
                pl.col("sum_open_interest").cast(fdtype),
                pl.col("sum_open_interest_value").cast(fdtype),
                pl.col("count_toptrader_long_short_ratio").cast(fdtype),
                pl.col("sum_toptrader_long_short_ratio").cast(fdtype),
                pl.col("count_long_short_ratio").cast(fdtype),
                pl.col("sum_taker_long_short_vol_ratio").cast(fdtype),
            ])
        if fmt == "parquet":
            # parquet：带类型、zstd 压缩体积更小、列式读取，特征计算更省事
            lf.sink_parquet(str(tmp_out), compression="zstd")
        else:
            lf.sink_csv(str(tmp_out))  # --format csv 时才走这里
        tmp_out.replace(out)
        log(f"cleaned -> {out.name}")
    finally:
        # 临时 CSV 与 sink 残件都必须清掉：前者可达 GB 级，后者会让下次运行
        # 的 resume 检查看到"半完成"的输出。
        tmp_csv.unlink(missing_ok=True)
        tmp_out.unlink(missing_ok=True)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--market", choices=["spot", "um", "both"], default="both")
    ap.add_argument("--symbol", default="BTCUSDT",
                    help="BTCUSDT / ETHUSDT / all")
    ap.add_argument("--category", choices=["klines", "aggTrades", "metrics", "all"],
                    default="all")
    ap.add_argument("--interval", default="1m", help="klines interval")
    ap.add_argument("--granularity", choices=["daily", "monthly"], default="daily",
                    help="daily=每天一个文件（测试/少量天数）；monthly=每月一个文件"
                         "（批量回补快得多）。monthly 按整月取文件，起止日期中的“日”"
                         "会被忽略；metrics 只有日度文件，monthly 下会自动跳过")
    ap.add_argument("--start-date", dest="start_date", required=True,
                    help="ISO 日期，如 2025-06-01；monthly 粒度下同月内任意日期等价")
    ap.add_argument("--end-date", dest="end_date", required=True,
                    help="ISO 日期，如 2025-06-30")
    ap.add_argument("--dest-root", default=str(DATA),
                    help="Local output root (default: <module>/data)")
    ap.add_argument("--no-checksum", action="store_true")
    ap.add_argument("--no-extract", action="store_true")
    ap.add_argument("--format", choices=["parquet", "csv"], default="parquet",
                    help="输出格式，默认 parquet（带类型、zstd 压缩更小、列式读取更快）。"
                         "断点续传按输出文件判定，切换格式会重新下载")
    ap.add_argument("--dtype", choices=["float32", "float64"], default="float32",
                    help="浮点列精度，默认 float32：BTC 价位下 ULP≈0.008、相对误差 ~4e-8，"
                         "对特征计算无影响且体积减半；需要逐 tick 精确价位时用 float64")
    args = ap.parse_args()

    if args.interval not in VALID_INTERVALS:
        ap.error(f"--interval must be one of {sorted(VALID_INTERVALS)}")

    data_root = Path(args.dest_root)
    markets = ["spot", "um"] if args.market == "both" else [args.market]
    symbols = ["BTCUSDT", "ETHUSDT"] if args.symbol == "all" else [args.symbol]
    cats = ALL_CATS if args.category == "all" else [args.category]

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    if start > end:
        ap.error("--start-date must be <= --end-date")

    fdtype = pl.Float32 if args.dtype == "float32" else pl.Float64

    if args.granularity == "monthly" and not (
            start.day == 1 and (end + timedelta(days=1)).day == 1):
        # monthly 按整月取文件：提醒起止日会被忽略，避免"只想下 6 月 15-20 日"
        # 结果下了整个 6 月，或跨月区间多下整月。
        print(f"NOTE: --granularity monthly 按整月下载，起止日期中的“日”会被忽略："
              f"{args.start_date} ~ {args.end_date} 覆盖的每个整月都会被下载。",
              flush=True)

    def log(tag, msg):
        print(f"[{tag}] {msg}", flush=True)

    totals = {"ok": 0, "skip": 0, "missing": 0, "failed": 0}
    skipped = []  # 按设计跳过的组合，汇总时集中打印，避免淹没在逐文件日志里

    def handle_period(tag, url, dp, cat):
        """下载并清洗一个周期，结果计入 totals。process 失败时强制重新下载
        一次再试——旧实现复用坏的残留 zip 只能记 failed，要再跑一次整个脚本
        才可能恢复。"""
        status = download_one(url, dp, not args.no_checksum,
                              lambda m: log(tag, m))
        if status not in ("ok", "skip"):
            totals[status] = totals.get(status, 0) + 1
            return
        if args.no_extract:
            totals[status] = totals.get(status, 0) + 1
            return
        try:
            process(dp, cat, args.format, fdtype, lambda m: log(tag, m))
        except Exception as e:  # 坏 zip / 列数不符只影响这一个周期
            log(tag, f"process FAILED: {e}; forcing re-download and retrying once")
            if dp.exists():
                dp.unlink()
            if download_one(url, dp, not args.no_checksum,
                            lambda m: log(tag, m)) != "ok":
                totals["failed"] += 1
                return
            try:
                process(dp, cat, args.format, fdtype, lambda m: log(tag, m))
            except Exception as e2:
                log(tag, f"process FAILED again: {e2}; giving up on this period")
                if dp.exists():
                    dp.unlink()
                totals["failed"] += 1
                return
        # Source zip is now redundant; drop it to save space
        # (matters a lot for monthly backfills).
        if dp.exists():
            dp.unlink()
            log(tag, "removed source zip")
        # status=="skip" 表示复用了上次中断残留的 zip；本次确实完成了处理，计为 ok
        totals["ok"] += 1

    for market_key in markets:
        market_path = MARKETS[market_key]
        for symbol in symbols:
            for cat in cats:
                if cat == "metrics" and market_key == "spot":
                    log(f"{market_key}/{cat}/{symbol}", "skip: spot has no metrics")
                    skipped.append(f"{market_key}/metrics/{symbol}: spot 无 metrics 数据")
                    continue
                if cat == "metrics" and args.granularity == "monthly":
                    log(f"{market_key}/{cat}/{symbol}",
                        "skip: metrics is daily-only")
                    skipped.append(f"{market_key}/metrics/{symbol}: "
                                   "metrics 只有日度文件，monthly 自动跳过")
                    continue
                for period in period_strings(args.granularity, start, end):
                    tag = f"{market_key}/{cat}/{symbol}/{period}"
                    url = url_for(market_path, args.granularity, cat, symbol,
                                  args.interval, period)
                    dp = dest_path(data_root, market_key, args.granularity, cat,
                                   symbol, args.interval, period)
                    out = dp.with_suffix(f".{args.format}")
                    # Resume key is the cleaned output file, not the zip (we delete
                    # the zip after processing, so a leftover zip means an
                    # interrupted run, not a finished one).
                    if out.exists():
                        log(tag, f"skip (cleaned .{args.format} exists)")
                        totals["skip"] += 1
                        continue
                    handle_period(tag, url, dp, cat)

    print("\n==== summary ====")
    print(f"ok={totals['ok']} skip={totals['skip']} "
          f"missing={totals['missing']} failed={totals['failed']}")
    if skipped:
        print("---- skipped by design ----")
        for s in skipped:
            print(f"  {s}")
        print("  整月 metrics 需另跑一条日度命令，例如：")
        print("    python binance_vision/download_binance_vision.py "
              f"--market um --category metrics --granularity daily "
              f"--start-date {args.start_date} --end-date {args.end_date}")


if __name__ == "__main__":
    main()
