# bn_hisdata

BTCUSDT 历史数据工程。按数据源与处理逻辑分模块组织：

- `orderbook/` — L2 订单簿（CryptoHFTData / Binance local-depth diff）。从原始每小时
  snapshot+update 文件重建分钟级定宽（±0.3%）盘口快照。详见
  `orderbook/docs/orderbook_lessons.md`。
- （规划中）K 线 / aggTrades / metrics — 直接来自官方 `https://data.binance.vision`，
  处理逻辑与订单簿完全不同，将放在独立模块目录，避免相互耦合。

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
```

数据默认落在 `orderbook/data/`（已在 `.gitignore` 中忽略，不入库）。依赖见
`orderbook/requirements.txt`（`polars`、`zstandard`）。
