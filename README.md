# bn_hisdata

BTCUSDT 历史数据工程。按数据源与处理逻辑分模块组织：

- `orderbook/` — L2 订单簿（CryptoHFTData / Binance local-depth diff）。从原始每小时
  snapshot+update 文件重建分钟级定宽（±0.3%）盘口快照。详见
  `orderbook/docs/orderbook_lessons.md`。
- `binance_vision/` — K 线 / aggTrades / metrics，直接来自官方
  `https://data.binance.vision`。下载并清洗成 parquet（默认）或 CSV：统一表头、
  klines 拆成主动买/卖成交量（`buy_vol` / `sell_vol`）、校验和校验、按天/按月、
  断点续传。处理逻辑与订单簿完全不同，故独立成模块。

## orderbook 模块

运行方式（从仓库根目录）：

```bash
# 端到端：下载 -> 重建快照 -> 删除原始 -> 清内存（按天/按区间）
python orderbook/pipeline.py --date 2026-10-12
python orderbook/pipeline.py --start-date 2025-06-01 --end-date 2025-06-30 --download r2

# 单独步骤
python orderbook/download_day.py --date 2026-10-12 --exchange binance_spot        # 匿名 API 下载
python orderbook/download_orderbook_s3.py --date 2026-10-12                       # R2 直连（需 CHD_API_KEY）
python orderbook/build_minute_snapshots.py --date 2026-10-12 --exchange binance_spot
python orderbook/validate_snapshot_consistency.py --date 2026-10-12 --exchange binance_spot

# 把每日窄表聚合成月度 parquet（特征计算的折中粒度，可选）
python orderbook/merge_monthly.py --dest-root orderbook/data
```

## binance_vision 模块

```bash
# 下载 + 清洗，默认输出 parquet（--format csv 可输出 CSV 便于肉眼检查）
# 整月 klines：每个 market×symbol 一个 zip。
# 注意 monthly 按整月取文件，起止日期中的“日”会被忽略。
python binance_vision/download_binance_vision.py --market both --symbol all \
    --category klines --interval 1m --granularity monthly \
    --start-date 2025-06-01 --end-date 2025-06-30

# 同月 metrics：Binance 只发布日度文件，--granularity monthly 会自动跳过，
# 需要另跑一条日度命令（6 月 = 每符号 30 个文件）
python binance_vision/download_binance_vision.py --market um --symbol all \
    --category metrics --granularity daily \
    --start-date 2025-06-01 --end-date 2025-06-30

# aggTrades（微观结构特征来源）：注意 --category all 只含 klines + metrics，
# 不含 aggTrades，必须显式指定。aggTrades 只有日度文件（月度文件不存在），
# 且体积大（BTCUSDT um 单日 ~1M 笔），建议 -j 提高并发。
python binance_vision/download_binance_vision.py --market um --symbol BTCUSDT \
    --category aggTrades --interval 1m --granularity daily \
    --start-date 2025-06-01 --end-date 2025-06-30 -j 8
```

> aggTrades 两种真实格式（下载器已自动适配，输出统一为毫秒）：
> um = 7 列有表头、毫秒时间戳；spot = 8 列无表头、微秒时间戳。

# 第一阶段：基础宽表（um+spot klines + basis + metrics，全流式 lazy 管道）。
# aggTrades 和 orderbook 一样：下载量大、只贡献衍生特征不进原表，放第二阶段。
# --no-agg 显式跳过 aggTrades，输出 {symbol}_base_features_1m.parquet
python binance_vision/build_master_features.py --symbol BTCUSDT --no-agg

# 第二阶段：补下 aggTrades 后重建完整 master 表（此时缺 aggTrades 会硬失败）
python binance_vision/download_binance_vision.py --market um --symbol BTCUSDT \
    --category aggTrades --interval 1m --granularity daily \
    --start-date 2025-06-01 --end-date 2026-10-01 -j 8
python binance_vision/build_master_features.py --symbol BTCUSDT

# 单元测试（从仓库根运行；根目录 conftest.py 负责把仓库根加入 sys.path）
python -m pytest binance_vision -q

数据默认落在各模块自己的 `data/` 目录（已在 `.gitignore` 中忽略，不入库）。项目依赖见
根目录 `requirements.txt`（`polars`、`zstandard`）。
