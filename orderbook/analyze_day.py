"""全天 24 个小时订单簿文件的格式与内容汇总分析（纯 polars，无 pandas/pyarrow）。

对 data/{date}/{exchange}/{HH}/BTCUSDT_orderbook.parquet[.zst] 逐文件统计:
- schema 一致性、行数、event_type/side 分布
- 时间覆盖、消息条数、每消息行数
- update id 连续性、删除行比例、各列空值率
- 快照结构（次数、每侧档位数）
最后汇总全天指标, 并重建收盘订单簿验证重放逻辑。
"""
import io
from pathlib import Path

import polars as pl
import zstandard as zstd

ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
DATE = "2026-10-12"
ROOT = Path("data") / DATE
COLUMNS = [
    "received_time", "event_time", "transaction_time", "symbol", "event_type",
    "first_update_id", "final_update_id", "prev_final_update_id",
    "last_update_id", "side", "price", "quantity", "order_count",
]


def read_parquet_any(path: Path) -> pl.DataFrame:
    raw = path.read_bytes()
    if raw[:4] == ZSTD_MAGIC:
        raw = zstd.ZstdDecompressor().stream_reader(io.BytesIO(raw)).read()
    return pl.read_parquet(io.BytesIO(raw))


def _counts(df: pl.DataFrame, col: str) -> dict:
    return {r[0]: r[1] for r in df.group_by(col).len().sort("len", descending=True).iter_rows()}


def _null_rates(df: pl.DataFrame) -> dict:
    return {c: round(df[c].null_count() / df.height, 4) for c in df.columns}


def analyze_file(path: Path) -> dict:
    df = read_parquet_any(path)

    rec = {"file": str(path), "rows": df.height, "schema": dict(df.schema)}
    rec["event_types"] = _counts(df, "event_type")
    rec["sides"] = _counts(df, "side")
    rec["rt_min"] = df["received_time"].min()
    rec["rt_max"] = df["received_time"].max()
    rec["et_min"] = df["event_time"].min()
    rec["et_max"] = df["event_time"].max()
    rec["n_messages"] = df["event_time"].n_unique()
    rec["rows_per_msg_mean"] = df.height / rec["n_messages"]

    upd = df.filter(pl.col("event_type") == "update")
    if upd.height:
        # price/quantity 在原始 parquet 里是 String，先 cast 再比较
        rec["del_ratio"] = (upd["quantity"].cast(pl.Float64) == 0).mean()
        upd_sorted = upd.sort("final_update_id")
        rec["id_gap_rows"] = int(
            (upd_sorted["final_update_id"] != upd_sorted["prev_final_update_id"]).sum()
        )
        rec["first_id"] = upd["first_update_id"][0]
        rec["final_id"] = upd["final_update_id"][-1]

    snaps = df.filter(pl.col("event_type") == "snapshot")
    rec["n_snapshots"] = snaps["received_time"].n_unique() if snaps.height else 0
    if rec["n_snapshots"]:
        g = snaps.group_by("received_time").len()
        rec["snap_rows_mean"] = g["len"].mean()
        rec["snap_levels_per_side"] = _counts(
            snaps.filter(pl.col("received_time") == snaps["received_time"][0]), "side"
        )

    rec["null_rates"] = _null_rates(df)
    if upd.height:
        rec["null_rates_update"] = _null_rates(upd)
    if snaps.height:
        rec["null_rates_snapshot"] = _null_rates(snaps)
    return rec


def main() -> None:
    for exchange in ["binance_futures", "binance_spot"]:
        files = sorted((ROOT / exchange).glob("*/BTCUSDT_orderbook.parquet*"))
        print(f"\n{'='*70}\n{exchange}: {len(files)} files\n{'='*70}")
        if not files:
            continue

        records = [analyze_file(p) for p in files]

        # schema 一致性
        schemas = {tuple(sorted(r["schema"].items())) for r in records}
        print(f"schema 一致: {len(schemas) == 1}")
        print("schema:", records[0]["schema"])

        summary = pl.DataFrame([
            {
                "hour": Path(r["file"]).parent.name,
                "rows": r["rows"],
                "n_messages": r["n_messages"],
                "rows/msg": round(r["rows_per_msg_mean"], 1),
                "event_types": str(r["event_types"]),
                "n_snapshots": r["n_snapshots"],
                # 缺 update 的小时给 None（polars null，mean/sum 自动跳过）；
                # 不能用 float("nan")——polars 的 mean 不跳过 NaN。
                "del_ratio": (round(r["del_ratio"], 4) if "del_ratio" in r else None),
                "id_gap_rows": r.get("id_gap_rows"),
                "rt_min": r["rt_min"], "rt_max": r["rt_max"],
            }
            for r in records
        ])
        with pl.Config(tbl_rows=100, tbl_cols=-1, tbl_width_chars=250):
            print(summary)

        total_rows = summary["rows"].sum()
        total_msgs = summary["n_messages"].sum()
        print(f"\n全天总行数: {total_rows:,}  总消息数: {total_msgs:,}")
        print(f"平均每秒消息数: {total_msgs / 86400:.1f}")
        print(f"平均每消息行数: {total_rows / total_msgs:.1f}")
        dr = summary["del_ratio"].mean()
        if dr is not None:
            print(f"平均删除行比例(quantity==0): {dr:.4f}")
        print(f"update id 不连续总行数: {summary['id_gap_rows'].sum():,}")
        print("空值率(全部行):", records[0]["null_rates"])
        if "null_rates_update" in records[0]:
            print("空值率(update 行):", records[0]["null_rates_update"])
        if "null_rates_snapshot" in records[0]:
            print("空值率(snapshot 行):", records[0]["null_rates_snapshot"])

        # 快照明细
        snap_recs = [r for r in records if r["n_snapshots"]]
        if snap_recs:
            print(f"\n含快照的小时数: {len(snap_recs)}")
            for r in snap_recs[:3]:
                print(f"  {Path(r['file']).parent.name}: snapshots={r['n_snapshots']}, "
                      f"rows/snapshot={r['snap_rows_mean']:.0f}, "
                      f"levels/side={r['snap_levels_per_side']}")

        # 时间覆盖检查
        rt = pl.Series(
            [r["rt_min"] for r in records] + [r["rt_max"] for r in records]
        ).cast(pl.Datetime("ns"))
        print(f"\nreceived_time 覆盖: {rt.min()} -> {rt.max()} UTC")

        # 重建收盘订单簿 (最后一个小时)
        # 快照 = 整书替换：每个连续快照块的第一行清空全书，块内逐行覆盖；
        # quantity==0 的档位一律视为删除（与 ob_reconstruct 的语义一致）。
        last = read_parquet_any(files[-1]).select(
            ["event_type", "side", "price", "quantity"]
        )
        book = {"bid": {}, "ask": {}}
        prev_snap = False
        for row in last.iter_rows(named=True):
            if row["event_type"] == "snapshot":
                if not prev_snap:
                    book = {"bid": {}, "ask": {}}
                prev_snap = True
            else:
                prev_snap = False
            q = float(row["quantity"])
            if q == 0:
                book[row["side"]].pop(row["price"], None)
            else:
                book[row["side"]][row["price"]] = q
        bids = sorted(((float(p), q) for p, q in book["bid"].items()), reverse=True)
        asks = sorted(((float(p), q) for p, q in book["ask"].items()))
        print(f"\n[重放验证] 末小时({files[-1].parent.parent.name}/{files[-1].parent.name}) "
              f"重建簿: bid 档位 {len(bids)}, ask 档位 {len(asks)}")
        if bids and asks:
            print(f"  best bid: {bids[0]}, best ask: {asks[0]}, spread: {asks[0][0]-bids[0][0]}")
            print(f"  top5 bid: {bids[:5]}")
            print(f"  top5 ask: {asks[:5]}")
            print(f"  簿子有效 (best_bid < best_ask): {bids[0][0] < asks[0][0]}")


if __name__ == "__main__":
    main()
