"""分析样本小时订单簿文件的数据格式（纯 polars，无 pandas/pyarrow）"""
import io
from pathlib import Path

import polars as pl
import zstandard as zstd

path = Path("data/sample_hour.parquet.zst")
ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"

# 依魔数决定是否解压：.zst 样本流式解压，裸 parquet 直接读
# （stream_reader 对非 zstd 输入会抛 ZstdError，不能无条件解压）
raw_bytes = path.read_bytes()
if raw_bytes[:4] == ZSTD_MAGIC:
    raw = zstd.ZstdDecompressor().stream_reader(io.BytesIO(raw_bytes)).read()
    print(f"compressed: {len(raw_bytes):,} bytes -> decompressed: {len(raw):,} bytes")
else:
    raw = raw_bytes
    print(f"plain parquet: {len(raw):,} bytes (no decompression)")

# 注意：原始 parquet 里 price / quantity 是 String 列（见 schema 输出），
# 数值比较/统计前必须先 cast(pl.Float64)。
df = pl.read_parquet(io.BytesIO(raw))
print("\n===== Schema =====")
print(df.schema)

print(f"\n===== 基本统计 =====\nrows: {df.height:,}")

print("\n--- event_type 分布 ---")
print(df.group_by("event_type").len().sort("len", descending=True))

print("\n--- side 分布 ---")
print(df.group_by("side").len().sort("len", descending=True))

print("\n--- symbol ---")
print(df["symbol"].unique().to_list())

print("\n--- 时间戳 ---")
print("received_time:", df["received_time"].min(), "->", df["received_time"].max(), "(ns)")
print("event_time   :", df["event_time"].min(), "->", df["event_time"].max(), "(ms)")
rt = df["received_time"].cast(pl.Datetime("ns"))
print("received_time range:", rt.min(), "->", rt.max(), "UTC")
et = df["event_time"].cast(pl.Datetime("ms"))
print("event_time range   :", et.min(), "->", et.max(), "UTC")
lag_ms = (df["received_time"] - df["event_time"] * 10**6) / 1e6
print("received-event lag (ms):")
print(lag_ms.describe())

with pl.Config(tbl_rows=10, tbl_cols=-1, fmt_str_lengths=250):
    print("\n--- 前 10 行 ---")
    print(df.head(10))

print("\n--- 快照结构分析 ---")
snaps = df.filter(pl.col("event_type") == "snapshot")
print(f"snapshot rows: {snaps.height:,}")
if snaps.height:
    snap_groups = snaps.group_by("received_time").len()
    print("每次快照行数分布:")
    print(snap_groups["len"].describe())
    print("快照次数:", snap_groups.height)
    one = snaps.filter(pl.col("received_time") == snaps["received_time"][0])
    print("\n第一次快照各 side 行数:")
    print(one.group_by("side").len())
    with pl.Config(tbl_rows=15, tbl_cols=-1, fmt_str_lengths=40):
        print(one.head(15))

print("\n--- 增量更新结构分析 ---")
upd = df.filter(pl.col("event_type") == "update")
print(f"update rows: {upd.height:,}")
if upd.height:
    zero_qty = (upd["quantity"].cast(pl.Float64) == 0).sum()
    print(f"quantity == 0 (删除档位) 的行数: {zero_qty:,} ({zero_qty / upd.height * 100:.2f}%)")
    msg_groups = upd.group_by("event_time").len()
    print("每个 event_time(每条WS消息) 的行数分布:")
    print(msg_groups["len"].describe())

    print("\n--- update id 连续性 (update 行) ---")
    upd_sorted = upd.sort("final_update_id")
    gap = (upd_sorted["final_update_id"] != upd_sorted["prev_final_update_id"]).sum()
    print(f"final_update_id != prev_final_update_id 的行数: {gap:,}")

print("\n--- price/quantity 字符串样例 ---")
with pl.Config(tbl_rows=12, tbl_cols=-1, fmt_str_lengths=40):
    print(df.select(["side", "price", "quantity", "order_count"]).head(12))
