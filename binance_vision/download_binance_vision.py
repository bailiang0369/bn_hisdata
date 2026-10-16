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
output, then the source .zip is deleted. klines output keeps open_time,
close_time + OHLC + count + buy_vol/sell_vol and drops volume /
quote_volume / taker_buy_quote_volume / ignore, where the taker-buy volume
buy_vol = taker_buy_volume and the taker-sell volume sell_vol = volume -
taker_buy_volume. close_time (+1ms) aligns to orderbook snapshots
(== next candle open_time).

Output defaults to parquet rather than CSV: typed columns (no re-parsing
strings on every read), much smaller on disk (zstd), columnar reads for feature
building, and consistent with the orderbook module which already emits parquet.

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

  # bulk backfill: monthly klines + aggTrades for both symbols, both markets
  python binance_vision/download_binance_vision.py --market both --symbol all \
      --category klines --interval 1m --granularity monthly \
      --start-date 2025-06-01 --end-date 2026-10-15
"""
import argparse
import hashlib
import sys
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
AGGTRADES_COLS = ["agg_trade_id", "price", "quantity", "first_trade_id",
                  "last_trade_id", "transact_time", "is_buyer_maker"]
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


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def verify_checksum(url_checksum: str, data: bytes, log) -> bool:
    """True = 可用（校验通过，或无法校验而跳过）；False = 哈希确实不匹配。

    只有"真的不匹配"才返回 False，让调用方丢弃响应并重试，绝不写出损坏文件；
    取不到 CHECKSUM、文件为空等一律视为跳过（True），避免误伤正常下载。
    """
    try:
        r = requests.get(url_checksum, timeout=30)
        if r.status_code != 200:
            log(f"no checksum file (HTTP {r.status_code}), skipped")
            return True
        text = r.text.strip()
        if not text:
            log("empty checksum file, skipped")
            return True
        expected = text.split()[0]
        actual = sha256_of(data)
        if expected.lower() != actual.lower():
            log(f"CHECKSUM MISMATCH: expected {expected}, got {actual}")
            return False
        log("checksum OK")
        return True
    except Exception as e:  # 校验是安全网：只有真实不匹配才失败
        log(f"checksum check skipped: {e}")
        return True


def download_one(url: str, dest: Path, verify: bool, log) -> str:
    if dest.exists():
        log("skip (exists)")
        return "skip"
    dest.parent.mkdir(parents=True, exist_ok=True)
    last = None
    for attempt in range(1, 4):
        try:
            r = requests.get(url, timeout=120)
            if r.status_code == 200:
                data = r.content
                # 校验和真的不匹配 -> 丢弃本次响应并重试，绝不写出损坏文件
                if verify and not verify_checksum(url + ".CHECKSUM", data, log):
                    last = "checksum mismatch"
                else:
                    dest.write_bytes(data)
                    log(f"saved {len(data) / 1e6:.1f} MB")
                    return "ok"
            elif r.status_code == 404:
                log(f"MISSING (HTTP 404): {url}")
                return "missing"
            else:
                last = r.status_code
        except Exception as e:  # transient network error -> retry
            last = e
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

    Streams the inner CSV straight into polars instead of decoding/splitting/
    re-joining it in Python: monthly aggTrades can be hundreds of MB and the
    old triple-copy peaked at ~3-4x the CSV size.
    """
    out = zip_path.with_suffix(f".{fmt}")
    if out.exists():
        return
    with zipfile.ZipFile(zip_path) as z:
        csv_in = [n for n in z.namelist() if n.endswith(".csv")]
        if not csv_in:
            log("no csv inside zip")
            return
        # 直接把 zip 内的文件对象交给 polars 流式解析：不再 read+decode+splitlines
        # +join+StringIO 三重拷贝（旧实现峰值约 CSV 的 3-4 倍，月度 aggTrades 会爆内存）。
        with z.open(csv_in[0]) as fh:
            first = fh.readline().decode("utf-8", errors="replace")
            if not first.strip():
                log("empty csv inside zip")
                return
            # Detect header by the first token (a header's first col name is text;
            # a data row's first field is a timestamp/number).
            exp = EXPECTED_FIRST.get(category)
            is_header = bool(exp) and first.split(",")[0].strip() == exp
            fh.seek(0)
            if is_header:
                df = pl.read_csv(fh, has_header=True)
            else:
                df = pl.read_csv(fh, has_header=False,
                                 new_columns=CANONICAL[category])
    if category == "klines":
        # 主动买成交量 buy_vol = taker_buy_volume；主动卖成交量 sell_vol = 总量 - 主动买。
        # volume / quote_volume / taker_buy_quote_volume / ignore 一律丢弃。
        # 时间戳与笔数保持 Int64（毫秒戳 ~1.7e12，int32 会溢出）；价格与成交量用
        # fdtype（默认 Float32：BTC 价位下 ULP≈0.008、相对误差 ~4e-8，对特征计算无影响）。
        # 减法在 cast 之前按全精度算，不会先丢精度再相减。
        df = df.with_columns([
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
        df = df.with_columns([
            pl.col("agg_trade_id").cast(pl.Int64),
            pl.col("first_trade_id").cast(pl.Int64),
            pl.col("last_trade_id").cast(pl.Int64),
            pl.col("transact_time").cast(pl.Int64),
            pl.col("price").cast(fdtype),
            pl.col("quantity").cast(fdtype),
            pl.col("is_buyer_maker").cast(pl.Boolean),  # 1 字节 -> 1 位
        ])
    elif category == "metrics":
        df = df.with_columns([
            pl.col("create_time").cast(pl.Int64),
            pl.col("sum_open_interest").cast(fdtype),
            pl.col("sum_open_interest_value").cast(fdtype),
            pl.col("count_toptrader_long_short_ratio").cast(fdtype),
            pl.col("sum_toptrader_long_short_ratio").cast(fdtype),
            pl.col("count_long_short_ratio").cast(fdtype),
            pl.col("sum_taker_long_short_vol_ratio").cast(fdtype),
        ])
    if fmt == "parquet":
        # parquet：带类型、zstd 压缩体积更小、列式读取，特征计算更省事
        df.write_parquet(str(out), compression="zstd")
    else:
        df.write_csv(str(out))  # --format csv 时才走这里
    log(f"cleaned -> {out.name}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--market", choices=["spot", "um", "both"], default="both")
    ap.add_argument("--symbol", default="BTCUSDT",
                    help="BTCUSDT / ETHUSDT / all")
    ap.add_argument("--category", choices=["klines", "aggTrades", "metrics", "all"],
                    default="all")
    ap.add_argument("--interval", default="1m", help="klines interval")
    ap.add_argument("--granularity", choices=["daily", "monthly"], default="daily")
    ap.add_argument("--start-date", dest="start_date", required=True)
    ap.add_argument("--end-date", dest="end_date", required=True)
    ap.add_argument("--dest-root", default=str(DATA),
                    help="Local output root (default: <module>/data)")
    ap.add_argument("--no-checksum", action="store_true")
    ap.add_argument("--no-extract", action="store_true")
    ap.add_argument("--format", choices=["parquet", "csv"], default="parquet",
                    help="输出格式，默认 parquet（带类型、zstd 压缩更小、列式读取更快）")
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

    def log(tag, msg):
        print(f"[{tag}] {msg}", flush=True)

    totals = {"ok": 0, "skip": 0, "missing": 0, "failed": 0}
    for market_key in markets:
        market_path = MARKETS[market_key]
        for symbol in symbols:
            for cat in cats:
                if cat == "metrics" and market_key == "spot":
                    log(f"{market_key}/{cat}/{symbol}", "skip: spot has no metrics")
                    continue
                if cat == "metrics" and args.granularity == "monthly":
                    log(f"{market_key}/{cat}/{symbol}",
                        "skip: metrics is daily-only")
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
                    status = download_one(url, dp, not args.no_checksum,
                                          lambda m: log(tag, m))
                    if status not in ("ok", "skip"):
                        totals[status] = totals.get(status, 0) + 1
                        continue
                    if args.no_extract:
                        totals[status] = totals.get(status, 0) + 1
                        continue
                    try:
                        process(dp, cat, args.format, fdtype, lambda m: log(tag, m))
                    except Exception as e:  # 坏 zip / 列数不符只影响这一个周期
                        log(tag, f"process FAILED: {e}; removing zip for retry")
                        if dp.exists():
                            dp.unlink()
                        totals["failed"] += 1
                        continue
                    # Source zip is now redundant; drop it to save space
                    # (matters a lot for monthly backfills).
                    if dp.exists():
                        dp.unlink()
                        log(tag, "removed source zip")
                    # status=="skip" 表示复用了上次中断残留的 zip；本次确实完成了处理，计为 ok
                    totals["ok"] += 1

    print("\n==== summary ====")
    print(f"ok={totals['ok']} skip={totals['skip']} "
          f"missing={totals['missing']} failed={totals['failed']}")


if __name__ == "__main__":
    main()
