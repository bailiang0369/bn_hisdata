"""
Validate the per-minute as-of reconstruction against ground truth.

For each hour H (0..22), compare:
  (A) our reconstructed book at the LAST minute of hour H  (from hour H's own
      snapshot + updates, as-of H:59:59.814)
  (B) the RAW full snapshot at the start of hour H+1         (ground truth at
      H:59:59.865, independent of hour H's data)
They differ by only ~51 ms, so within the +-band around mid the vast majority
of levels must match. A correct as-of method => high match rate; a buggy one =>
large mismatch.

Usage:
    python validate_snapshot_consistency.py --date 2026-10-12 --exchange binance_spot
"""
import argparse
from pathlib import Path

import polars as pl

from ob_reconstruct import reconstruct_state

HERE = Path(__file__).resolve().parent


def reconstruct_last_minute(path: Path):
    df = pl.read_parquet(path).select(
        ["side", "price", "quantity", "event_time", "event_type"]
    )
    merged = reconstruct_state(df)
    bvals = sorted(merged["bucket"].unique().to_list())
    last = (
        merged.filter(pl.col("bucket") == bvals[-1])
        .filter(pl.col("q") > 0)
        .select(["side", "px", "q"])
        .unique(subset=["side", "px"])
    )
    bb = last.filter(pl.col("side") == "bid")["px"].max()
    ba = last.filter(pl.col("side") == "ask")["px"].min()
    mid = (bb + ba) / 2
    return last, mid


def load_raw_snapshot(path: Path, target_ev: int):
    # use ONLY the snapshot whose event_time equals the boundary instant
    # (a feed file can contain >1 snapshot from reconnects; mixing them is wrong)
    df = pl.read_parquet(path).filter(
        (pl.col("event_type") == "snapshot") & (pl.col("event_time") == target_ev)
    )
    df = df.select(["side", "price", "quantity"]).with_columns([
        pl.col("price").cast(pl.Float64).alias("px"),
        pl.col("quantity").cast(pl.Float64).alias("qty"),
    ]).filter(pl.col("qty") > 0)
    return df.select(["side", "px", "qty"]).unique(subset=["side", "px"])


def compare(ours: pl.DataFrame, truth: pl.DataFrame, mid: float, band: float):
    lo, hi = mid * (1 - band), mid * (1 + band)
    o = ours.filter((pl.col("px") >= lo) & (pl.col("px") <= hi)).rename({"q": "qo"})
    t = truth.filter((pl.col("px") >= lo) & (pl.col("px") <= hi)).rename({"qty": "qt"})
    j = o.join(t, on=["side", "px"], how="full", coalesce=True)
    both = (pl.col("qo").is_not_null()) & (pl.col("qt").is_not_null())
    matched = j.filter(both & ((pl.col("qo") - pl.col("qt")).abs() < 1e-9))
    diff = j.filter(both & ((pl.col("qo") - pl.col("qt")).abs() >= 1e-9))
    only_ours = j.filter(pl.col("qt").is_null())
    only_truth = j.filter(pl.col("qo").is_null())
    return {
        "band_levels": o.height + t.height - matched.height - diff.height,
        "ours": o.height,
        "truth": t.height,
        "matched": matched.height,
        "diff": diff.height,
        "only_ours": only_ours.height,
        "only_truth": only_truth.height,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--exchange", default="binance_spot")
    ap.add_argument("--band", type=float, default=0.003)
    ap.add_argument("--dest-root", default=str(HERE / "data"))
    args = ap.parse_args()

    root = Path(args.dest_root) / args.date / args.exchange
    files = sorted(root.glob("*/BTCUSDT_orderbook.parquet"))
    n = len(files) - 1  # boundaries H=0..n-1 vs H+1 snapshot

    tot = {"matched": 0, "diff": 0, "only_ours": 0, "only_truth": 0}
    print(f"{'H':>2}  {'ours':>6} {'truth':>6} {'match':>6} {'diff':>5} "
          f"{'onlyO':>5} {'onlyT':>5} {'match%':>6}")
    for h in range(n):
        last_ev = pl.read_parquet(files[h])["event_time"].max()
        ours, mid = reconstruct_last_minute(files[h])
        truth = load_raw_snapshot(files[h + 1], last_ev)
        r = compare(ours, truth, mid, args.band)
        denom = r["matched"] + r["diff"] + r["only_ours"] + r["only_truth"]
        pct = 100.0 * r["matched"] / denom if denom else 0.0
        print(f"{h:>2}  {r['ours']:>6} {r['truth']:>6} {r['matched']:>6} "
              f"{r['diff']:>5} {r['only_ours']:>5} {r['only_truth']:>5} {pct:>5.2f}%")
        for k in ("matched", "diff", "only_ours", "only_truth"):
            tot[k] += r[k]

    denom = tot["matched"] + tot["diff"] + tot["only_ours"] + tot["only_truth"]
    pct = 100.0 * tot["matched"] / denom if denom else 0.0
    print("-" * 48)
    print(f"OVERALL band match rate across {n} boundaries: {pct:.2f}% "
          f"(matched={tot['matched']}, diff={tot['diff']}, "
          f"only_ours={tot['only_ours']}, only_truth={tot['only_truth']})")
    print("Note: diff/only_* are expected (<=51 ms of order churn between the "
          "two sources); large numbers would indicate a reconstruction bug.")


if __name__ == "__main__":
    main()
