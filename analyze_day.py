"""全天 24 个小时订单簿文件的格式与内容汇总分析。

对 data/{date}/{exchange}/{HH}/BTCUSDT_orderbook.parquet[.zst] 逐文件统计:
- schema 一致性、行数、event_type/side 分布
- 时间覆盖、消息条数、每消息行数
- update id 连续性、删除行比例、各列空值率
- 快照结构（次数、每侧档位数）
最后汇总全天指标, 并重建收盘订单簿验证重放逻辑。
"""
import io
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
DATE = "2026-10-12"
ROOT = Path("data") / DATE
COLUMNS = [
    "received_time", "event_time", "transaction_time", "symbol", "event_type",
    "first_update_id", "final_update_id", "prev_final_update_id",
    "last_update_id", "side", "price", "quantity", "order_count",
]


def read_parquet_any(path: Path) -> pq.ParquetFile:
    raw = path.read_bytes()
    if raw[:4] == ZSTD_MAGIC:
        import zstandard as zstd
        raw = zstd.ZstdDecompressor().stream_reader(io.BytesIO(raw)).read()
    return pq.ParquetFile(io.BytesIO(raw))


def analyze_file(path: Path) -> dict:
    pf = read_parquet_any(path)
    schema = {f.name: str(f.type) for f in pf.schema_arrow}
    df = pf.read().to_pandas()

    rec = {"file": str(path), "rows": len(df), "schema": schema}
    rec["event_types"] = df["event_type"].value_counts().to_dict()
    rec["sides"] = df["side"].value_counts().to_dict()
    rec["rt_min"] = int(df["received_time"].min())
    rec["rt_max"] = int(df["received_time"].max())
    rec["et_min"] = int(df["event_time"].min())
    rec["et_max"] = int(df["event_time"].max())
    rec["n_messages"] = int(df["event_time"].nunique())
    rec["rows_per_msg_mean"] = float(len(df) / rec["n_messages"])

    upd = df[df["event_type"] == "update"]
    if len(upd):
        rec["del_ratio"] = float((pd.to_numeric(upd["quantity"]) == 0).mean())
        upd_sorted = upd.sort_values("final_update_id")
        rec["id_gap_rows"] = int(
            (upd_sorted["final_update_id"] != upd_sorted["prev_final_update_id"]).sum()
        )
        rec["first_id"] = int(upd["first_update_id"].iloc[0])
        rec["final_id"] = int(upd["final_update_id"].iloc[-1])

    snaps = df[df["event_type"] == "snapshot"]
    rec["n_snapshots"] = int(snaps["received_time"].nunique()) if len(snaps) else 0
    if rec["n_snapshots"]:
        g = snaps.groupby("received_time")
        rec["snap_rows_mean"] = float(g.size().mean())
        rec["snap_levels_per_side"] = (
            g.apply(lambda x: x.groupby("side").size().to_dict(), include_groups=False)
            .iloc[0]
        )

    rec["null_rates"] = df.isna().mean().round(4).to_dict()
    if len(upd):
        rec["null_rates_update"] = upd.isna().mean().round(4).to_dict()
    if len(snaps):
        rec["null_rates_snapshot"] = snaps.isna().mean().round(4).to_dict()
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

        summary = pd.DataFrame([
            {
                "hour": Path(r["file"]).parent.name,
                "rows": r["rows"],
                "n_messages": r["n_messages"],
                "rows/msg": round(r["rows_per_msg_mean"], 1),
                "event_types": r["event_types"],
                "n_snapshots": r["n_snapshots"],
                "del_ratio": round(r.get("del_ratio", float("nan")), 4),
                "id_gap_rows": r.get("id_gap_rows"),
                "rt_min": r["rt_min"], "rt_max": r["rt_max"],
            }
            for r in records
        ])
        with pd.option_context("display.width", 200, "display.max_columns", None):
            print(summary.to_string(index=False))

        total_rows = summary["rows"].sum()
        total_msgs = summary["n_messages"].sum()
        print(f"\n全天总行数: {total_rows:,}  总消息数: {total_msgs:,}")
        print(f"平均每秒消息数: {total_msgs / 86400:.1f}")
        print(f"平均每消息行数: {total_rows / total_msgs:.1f}")
        print(f"平均删除行比例(quantity==0): {summary['del_ratio'].mean():.4f}")
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
        rt = pd.to_datetime(
            pd.Series([r["rt_min"] for r in records] + [r["rt_max"] for r in records]),
            unit="ns",
        )
        print(f"\nreceived_time 覆盖: {rt.min()} -> {rt.max()} UTC")

        # 重建收盘订单簿 (最后一个小时)
        last = read_parquet_any(files[-1]).read().to_pandas()
        book = {"bid": {}, "ask": {}}
        prev_snap = False
        for row in last.itertuples(index=False):
            if row.event_type == "snapshot":
                if not prev_snap:
                    book = {"bid": {}, "ask": {}}
                book[row.side][row.price] = float(row.quantity)
                prev_snap = True
            else:
                prev_snap = False
                q = float(row.quantity)
                if q == 0:
                    book[row.side].pop(row.price, None)
                else:
                    book[row.side][row.price] = q
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
