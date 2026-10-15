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
  data/{market}/{granularity}/{category}/{symbol}/{interval?}/{symbol}-{type}-{period}.csv
  The source .zip is downloaded to the same folder, processed into this cleaned
  CSV, and then deleted (the zip is only a transient intermediate). Resume is
  keyed on the cleaned CSV, so re-running is a safe no-op once it exists.

NOTE: historical Binance CSVs are inconsistent -- some periods have a header
row, others don't. process() detects this and always emits a clean header
CSV, then the source .zip is deleted. klines output keeps open_time,
close_time + OHLCV + count + taker_buy_volume and drops ignore /
quote_volume / taker_buy_quote_volume. close_time (+1ms) aligns to orderbook
snapshots (== next candle open_time).

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
import io
import sys
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
# klines output: keep open_time + close_time + OHLCV + count + taker_buy_volume;
# drop ignore, quote_volume and taker_buy_quote_volume. close_time (+1ms) is the
# candle's end instant and the natural join key to orderbook snapshots
# (close_time + 1 == next candle's open_time). open_time is kept too since it is
# that same next-candle join key viewed from the preceding bar.
KLINE_KEEP = ["open_time", "open", "high", "low", "close", "volume",
              "close_time", "count", "taker_buy_volume"]
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


def verify_checksum(url_checksum: str, data: bytes, log) -> None:
    try:
        r = requests.get(url_checksum, timeout=30)
        if r.status_code == 200:
            expected = r.text.strip().split()[0]
            actual = sha256_of(data)
            if expected.lower() != actual.lower():
                raise ValueError(f"expected {expected}, got {actual}")
            log("checksum OK")
        else:
            log(f"no checksum file (HTTP {r.status_code}), skipped")
    except Exception as e:  # checksum is a safety net, never fatal
        log(f"checksum check skipped: {e}")


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
                if verify:
                    verify_checksum(url + ".CHECKSUM", data, log)
                dest.write_bytes(data)
                log(f"saved {len(data) / 1e6:.1f} MB")
                return "ok"
            if r.status_code == 404:
                log(f"MISSING (HTTP 404): {url}")
                return "missing"
            last = r.status_code
        except Exception as e:  # transient network error -> retry
            last = e
        log(f"attempt {attempt} failed ({last}); retrying")
    log(f"FAILED after retries: {url}")
    return "failed"


def process(zip_path: Path, category: str, log) -> None:
    """Read the zip's CSV, normalize headers, trim klines, write a clean CSV.

    Binance historical files are inconsistent: some periods ship a header row,
    others don't. We detect this from the first token and always emit a clean,
    header-bearing CSV next to the .zip (klines trimmed per KLINE_KEEP).
    """
    out = zip_path.with_suffix(".csv")
    if out.exists():
        return
    with zipfile.ZipFile(zip_path) as z:
        csv_in = [n for n in z.namelist() if n.endswith(".csv")]
        if not csv_in:
            log("no csv inside zip")
            return
        raw = z.read(csv_in[0]).decode("utf-8").splitlines()
    if not raw:
        return
    # Detect header by the first token (a header's first col name is text;
    # a data row's first field is a timestamp/number).
    exp = EXPECTED_FIRST.get(category)
    is_header = bool(exp) and raw[0].split(",")[0].strip() == exp
    text = "\n".join(raw)
    if is_header:
        df = pl.read_csv(io.StringIO(text), has_header=True)
    else:
        df = pl.read_csv(io.StringIO(text), has_header=False,
                         new_columns=CANONICAL[category])
    if category == "klines":
        df = df.select(KLINE_KEEP).with_columns([
            pl.col("open_time").cast(pl.Int64),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.col("close_time").cast(pl.Int64),
            pl.col("count").cast(pl.Int64),
            pl.col("taker_buy_volume").cast(pl.Float64),
        ])
    out.write_text(df.write_csv(), encoding="utf-8")
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
                    out = dp.with_suffix(".csv")
                    # Resume key is the cleaned CSV, not the zip (we delete the
                    # zip after processing, so a leftover zip means an
                    # interrupted run, not a finished one).
                    if out.exists():
                        log(tag, "skip (cleaned csv exists)")
                        totals["skip"] += 1
                        continue
                    status = download_one(url, dp, not args.no_checksum,
                                          lambda m: log(tag, m))
                    totals[status] = totals.get(status, 0) + 1
                    if status in ("ok", "skip") and not args.no_extract:
                        process(dp, cat, lambda m: log(tag, m))
                        # Source zip is now redundant; drop it to save space
                        # (matters a lot for monthly backfills).
                        if dp.exists():
                            dp.unlink()
                            log(tag, "removed source zip")

    print("\n==== summary ====")
    print(f"ok={totals['ok']} skip={totals['skip']} "
          f"missing={totals['missing']} failed={totals['failed']}")


if __name__ == "__main__":
    main()
