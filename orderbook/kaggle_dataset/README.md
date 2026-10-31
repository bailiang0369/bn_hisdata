# Orderbook Minute Snapshots (resumable build)

> **状态：已停用（2026-10-31）。** 构建进度的持久化通道已改为 **GitHub Release
> `data-{YYYY-MM}`**（每日一个 `{YYYY-MM-DD}.zip`），月度产物的发布通道是
> **Release `data-merged`**。原因：Kaggle Dataset 不支持增量上传，每轮都要全量重传
> 数 GB；Release 逐资产上传天然增量。实现见 `orderbook/kaggle/run_orderbook.py` 与
> `orderbook/kaggle_merge/merge_run.py`，产物说明见根目录 `DATA_ACCESS.md`。
>
> 本目录保留作为**备选方案**（`orderbook/docs/kaggle_runbook.md`）的元数据模板，
> 正常情况下不需要再跑 `kaggle datasets version`。

Binance orderbook 分钟级快照重建产物（dense 重建），覆盖 `binance_futures` 与
`binance_spot`，日期区间 2025-07-01 .. 2026-10-20。

原设计：构建内核（`byh850515/orderbook-runner`）每次运行把 `orderbook/data/{date}/`
下的每日窄表上传为新版本；内核被 9h 上限 kill 或暂停后，重投时先下载本数据集恢复进度，
再续跑未完成的日期。

目录结构：

```
{YYYY-MM-DD}/
  binance_futures_minute_band.parquet
  binance_futures_minute_band_mid.parquet
  binance_spot_minute_band.parquet
  binance_spot_minute_band_mid.parquet
```

月度合并文件（~数百 MB/交易所/月）不长期存于此数据集，以免超过 20GB 上限。
