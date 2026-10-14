"""Determine timestamp units and snapshot timing for the order book data."""
import datetime as dt
import polars as pl

ROOT = "data/2026-10-12"


def conv(v, unit):
    if v is None:
        return "null"
    v = int(v)
    try:
        if unit == "ns":
            sec = v / 1e9
        elif unit == "us":
            sec = v / 1e6
        elif unit == "ms":
            sec = v / 1e3
        else:
            return "?"
        return dt.datetime.fromtimestamp(sec, tz=dt.timezone.utc).isoformat()
    except (OverflowError, ValueError, OSError):
        return "overflow"


for ex in ["binance_spot", "binance_futures"]:
    d00 = pl.read_parquet(f"{ROOT}/{ex}/00/BTCUSDT_orderbook.parquet")
    d01 = pl.read_parquet(f"{ROOT}/{ex}/01/BTCUSDT_orderbook.parquet")
    print(f"\n========== {ex} ==========")
    for col in ["received_time", "event_time", "transaction_time"]:
        mn, mx = d00[col].min(), d00[col].max()
        if mn is None:
            print(f"  {col}: ALL NULL")
            continue
        print(f"  {col}: min={mn}  max={mx}")
        print(f"      ns -> {conv(mn,'ns')}")
        print(f"      us -> {conv(mn,'us')}")
        print(f"      ms -> {conv(mn,'ms')}")

    # snapshot timing inside the 00 hour
    snap = d00.filter(pl.col("event_type") == "snapshot")
    upd = d00.filter(pl.col("event_type") == "update")
    print(f"  snapshot rows in 00: {snap.height:,}   update rows: {upd.height:,}")
    s_sorted = snap.sort("event_time")
    first_snap_et = s_sorted["event_time"][0]
    print(f"  first snapshot event_time={first_snap_et} -> {conv(first_snap_et,'ms')}")
    # earliest & latest event of the hour
    e_sorted = d00.sort("event_time")
    print(f"  hour 00 earliest event: et={e_sorted['event_time'][0]} type={e_sorted['event_type'][0]} -> {conv(e_sorted['event_time'][0],'ms')}")
    print(f"  hour 00 latest  event: et={e_sorted['event_time'][-1]} type={e_sorted['event_type'][-1]} -> {conv(e_sorted['event_time'][-1],'ms')}")
    # does hour 01 start with a snapshot at its boundary?
    e01 = d01.sort("event_time")
    print(f"  hour 01 earliest event: et={e01['event_time'][0]} type={e01['event_type'][0]} -> {conv(e01['event_time'][0],'ms')}")
    # snapshot cadence: median gap between consecutive snapshot event_times
    gaps = s_sorted["event_time"].diff().drop_nulls()
    print(f"  snapshot gap (ms) median={gaps.median():.0f} min={gaps.min()} max={gaps.max()}")
