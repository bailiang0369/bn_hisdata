"""分析样本小时订单簿文件的数据格式"""
import io
import zstandard as zstd
import pandas as pd
import pyarrow.parquet as pq

path = "data/sample_hour.parquet"

# zstd 解压 -> parquet
raw_bytes = open(path, "rb").read()
raw = zstd.ZstdDecompressor().stream_reader(io.BytesIO(raw_bytes)).read()
print(f"compressed: {len(raw_bytes):,} bytes -> decompressed: {len(raw):,} bytes")

table = pq.read_table(io.BytesIO(raw))
print("\n===== Arrow Schema =====")
print(table.schema)

df = table.to_pandas()
print(f"\n===== 基本统计 =====\nrows: {len(df):,}")
print("\n--- dtypes ---")
print(df.dtypes)
print("\n--- event_type 分布 ---")
print(df["event_type"].value_counts())
print("\n--- side 分布 ---")
print(df["side"].value_counts())
print("\n--- symbol ---")
print(df["symbol"].unique())

print("\n--- 时间戳 ---")
print("received_time:", df["received_time"].min(), "->", df["received_time"].max(), "(ns)")
print("event_time   :", df["event_time"].min(), "->", df["event_time"].max())
rt = pd.to_datetime(df["received_time"], unit="ns")
print("received_time range:", rt.min(), "->", rt.max(), "UTC")
et = pd.to_datetime(df["event_time"], unit="ms")
print("event_time range   :", et.min(), "->", et.max(), "UTC")
print("received-event lag (ms):", ((df["received_time"] - df["event_time"] * 10**6) / 1e6).describe())

print("\n--- 前 10 行 ---")
with pd.option_context("display.width", 250, "display.max_columns", None):
    print(df.head(10))

print("\n--- 快照结构分析 ---")
snaps = df[df["event_type"] == "snapshot"]
print(f"snapshot rows: {len(snaps):,}")
if len(snaps):
    snap_groups = snaps.groupby("received_time").size()
    print("每次快照行数分布:")
    print(snap_groups.describe())
    print("快照次数:", len(snap_groups))
    one = snaps[snaps["received_time"] == snaps["received_time"].iloc[0]]
    print("\n第一次快照各 side 行数:")
    print(one.groupby("side").size())
    with pd.option_context("display.width", 250, "display.max_columns", None):
        print(one.head(15))

print("\n--- 增量更新结构分析 ---")
upd = df[df["event_type"] == "update"]
print(f"update rows: {len(upd):,}")
zero_qty = (upd["quantity"].astype(float) == 0).sum()
print(f"quantity == 0 (删除档位) 的行数: {zero_qty:,} ({zero_qty/len(upd)*100:.2f}%)")
msg_groups = upd.groupby("event_time").size()
print("每个 event_time(每条WS消息) 的行数分布:")
print(msg_groups.describe())

print("\n--- update id 连续性 (update 行) ---")
upd_sorted = upd.sort_values("final_update_id")
gap = (upd_sorted["final_update_id"] != upd_sorted["prev_final_update_id"]).sum()
print(f"final_update_id != prev_final_update_id 的行数: {gap:,}")

print("\n--- price/quantity 字符串样例 ---")
print(df[["side", "price", "quantity", "order_count"]].head(12).to_string())
