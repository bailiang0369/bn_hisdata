"""
Inspect the downloaded CryptoHFTData BTC L2 order book parquet files.

- Prints the on-disk directory structure and per-exchange totals.
- Loads sample hours with polars (NOT pandas) and shows schema, shape, sample rows.

Run:
    python inspect_orderbook.py
"""

from pathlib import Path

import polars as pl

ROOT = Path("data/2026-10-12")
EXCHANGES = ["binance_spot", "binance_futures"]


def show_structure() -> None:
    print("=== Directory structure ===")
    print(f"{ROOT}/")
    for ex in EXCHANGES:
        ex_dir = ROOT / ex
        files = sorted(ex_dir.rglob("*.parquet"))
        total = sum(p.stat().st_size for p in files)
        print(f"  {ex}/   files={len(files)}  total={total / 1024 / 1024:.1f} MB")
        for p in files:
            print(f"    {p.relative_to(ROOT)}  {p.stat().st_size / 1024 / 1024:6.2f} MB")


def show_sample(exchange: str, hour: str = "00") -> None:
    path = ROOT / exchange / hour / "BTCUSDT_orderbook.parquet"
    if not path.exists():
        print(f"\n[{exchange} {hour}] file not found: {path}")
        return
    df = pl.read_parquet(path)
    print(f"\n=== {exchange}  {hour}:00 UTC ===")
    print(f"shape: {df.shape[0]:,} rows x {df.shape[1]} cols")
    print("schema:")
    for name, dtype in df.schema.items():
        print(f"    {name:20s} {dtype}")
    with pl.Config(tbl_rows=3, tbl_cols=20, fmt_str_lengths=40):
        print(df.head(3))


def main() -> None:
    show_structure()
    for ex in EXCHANGES:
        show_sample(ex, "00")
    print("\nDone. (Loaded with polars, no pandas involved.)")


if __name__ == "__main__":
    main()
