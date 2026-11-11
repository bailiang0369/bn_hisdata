# 数据状态

> 本文件由 `binance_vision/pipeline_daily.py` 每天自动生成并提交，请勿手改。

- 最近一次运行：2026-11-11 14:16 UTC
- 目标日期：2026-11-10

## BTCUSDT

- 年度文件：7 个（2020、2021、2022、2023、2024、2025、2026）
- 最新覆盖日期：2026-11-10（最后一行 close_time = 2026-11-11 00:00 UTC）
- 本次写入 `BTCUSDT_1m_klines_2026.parquet`：450,720 行，覆盖到 2026-11-10

## ETHUSDT

- 年度文件：7 个（2020、2021、2022、2023、2024、2025、2026）
- 最新覆盖日期：2026-11-10（最后一行 close_time = 2026-11-11 00:00 UTC）
- 本次写入 `ETHUSDT_1m_klines_2026.parquet`：450,720 行，覆盖到 2026-11-10

## 取数

按年一个 parquet，列结构一致、时间不重叠，直接 scan 拼接：

```bash
python tools/fetch_release.py --tag vision-base \
    --pattern 'BTCUSDT_1m_klines_*.parquet' --dest ./data/vision
```

```python
import polars as pl
from pathlib import Path
lf = pl.scan_parquet(sorted(Path('./data/vision').glob('BTCUSDT_1m_klines_*.parquet')))
```

这些是**原始 K 线**（15 列），不含任何衍生特征；当年那份文件仍在增长（跨年后冻结），不要假定它的大小。完整 schema 与时间口径见 `DATA_ACCESS.md`。
