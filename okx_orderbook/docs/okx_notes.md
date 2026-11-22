# OKX 订单簿数据：建档事实与实验结论

> 建档 2026-11-19。**2026-11-19 已建成完整管线并完成实测，方向关闭**：跨所三路（独立特征 /
> 附加特征 / 档位合并）全部判负，详见 `bn_bt/实验详录.md` §45.10。本文保留数据契约与踩坑供复用。
> 原目的：为"跨所订单簿"实验提供数据基础，与现有 `orderbook/`（仅币安）平行。

## 1. 为什么要建这个模块

现有 `orderbook/` 的全部数据来自币安（`binance_futures` / `binance_spot`），没有任何其他交易所。
跨所 lead-lag 实验需要第二个所的订单簿，OKX 是唯一现实选择：

- **官方免费**提供历史 L2 订单簿（400 档从 2023-03-23 起，5000 档从 2025-11-01 起）
- 成交量约为币安的 **58~60%**（2026-11-19 实测：币安 24h 226K BTC / OKX 132K BTC），
  不是 1/3 以下，影响力足够
- Coinbase 没有官方免费历史（只有实时 WebSocket），第三方需付费，暂不考虑

## 2. 下载通道（已确认）

OKX 历史数据门户的前端调用私有接口生成下载链接：

```
POST https://www.okx.com/priapi/v5/broker/public/trade-data/download-link
{
  "module": 4,                        // 400档订单簿（5=5000档；10 是 UI dialogType，API 不认）
  "instType": "SWAP"|"SPOT"|"FUTURES",
  "instQueryParam": {
    "instIdList": ["BTC-USDT"],       // SPOT 用这个
    "instFamilyList": ["BTC-USDT"],   // SWAP/FUTURES 用这个（二选一）
    "ccyList": null
  },
  "dateQuery": {
    "dateAggrType": "daily",
    "begin": "<当天23:59:59.999的毫秒时间戳字符串>",
    "end": "<同上>"
  }
}
```

响应：`data.details[].groupDetails[]`，每项含 `filename / sizeMB / url`。
**真实下载 URL 来自接口返回，不硬编码 CDN 域名**（OKX 换域名时不会失效）。

**实测确认（2026-11-19，module 探测）**：
- `module=4` → `BTC-USDT-SWAP-L2orderbook-400lv-YYYY-MM-DD.tar.gz`，**388.59 MB/天**
- `module=5` → 5000 档（未测大小）
- `module=1` → `BTC-USDT-SWAP-trades-YYYY-MM-DD.zip`，11.98 MB/天
- 下载域名 `static.okx.com/cdn/okx/match/orderbook/L2/400lv/daily/...`
- 16 个月（2025-07~2026-10）压缩总量约 **190 GB**，非之前估的 TB 级

其他模块值（备用）：TRADE_HISTORY=1, CANDLESTICK=2, FUNDING_RATES=3, INTEREST_RATE=11。

## 3. 归档格式（2026-11-19 由 `inspect_day.py` 实测确认，kernel `okx-inspect-v4`）

单日文件（2025-10-01 样本）：

| 项 | 实测值 |
|---|---|
| 压缩大小 | 388 MB（接口标 389 MB） |
| 解压后 | 2.8 GB NDJSON，单文件 |
| 行数 | **6,618,837 行/天**（≈10ms 一次事件） |
| 字段 | 仅 5 个：`instId`, `action`, `ts`, `asks`, `bids`（**无** seqId/checksum） |
| 档位格式 | `asks`/`bids` = `[[price_str, qty_str, order_count_str], ...]`；**qty=0 表示删除该档** |
| action | `snapshot` 1440 次（每分钟 1 次）+ `update` 6,617,397 次 |
| ts 范围 | `1759276800005` ~ `1759363199991`，span **恰好 1440.0 分钟** |

**时间语义修正**：OKX 官方 L2 归档是**自然日 UTC 00:00~23:59**（span 1440 min），
与币安 `bucket` 自然日一致。开源项目说的"前一日 16:00 起"是它自己 trade 归档的语义，
不适用于官方 L2 归档。对齐可直接按 `bucket = ts // 60000 * 60000`。

## 4. 与币安数据结构的对齐（设计原则）

| 维度 | 币安 `orderbook/` | OKX 计划 | 是否可比 |
|---|---|---|---|
| 档位 | mid ±0.3% 内稀疏档 | 同样只取 mid ±0.3%（丢掉远档） | ✅ |
| 时间粒度 | 1m `bucket` = 分钟起点 | 重建为 1m `bucket` = 分钟起点 | ✅ 待验证时区 |
| 特征 | `ob_features.py` 185 列 | 用比例型特征（obi / 深度比 / spread），**不用绝对量** | ✅ |
| 标的 | BTCUSDT | BTC-USDT SWAP（对标配安 U 本位） | ✅ |

绝对挂单量两所不可比（币安更厚），必须用比例型特征才公平。

## 5. 脚本清单（均已建成并实测）

| 脚本 | 作用 | 状态 |
|---|---|---|
| `download_day.py` | 按日下载 400/5000 档归档（download-link API, module=4） | ✅ 实测可用 |
| `inspect_day.py` | 探测归档真实 schema / 大小 / 行数 | ✅ kernel `okx-inspect-v4` 已验证（见 §3） |
| `build_minute_snapshots.py` | NDJSON → 1m band/mid 快照 | ✅ 已建成；单月 5642s、3571 万行 band + 44640 行特征 |
| `build_features.py` | 复用 `ob_features.py` 算比例型特征（okx_ 前缀，剔除 19 列档数类） | ✅ 已建成（90 列） |
| `pipeline_month.py` | 月度批处理 | ✅ 已建成 |
| `kaggle/` | Kaggle runner + 推送 | ✅ 已建成 |

## 6. 实验结论：方向关闭（2026-11-19，不再执行）

原计划的证伪实验已全部做完（详见 `bn_bt/实验详录.md` §45.10）：

1. **独立预测力（E1 闸门）**：AUC 0.5247（<0.54 门槛）→ 判负。用户质疑样本不足后按 90 列 /
   4 个月重测，尾部 EV 一度转 +0.079，但严格 walk-forward 因果口径下 n=62、胜率 54.84%、
   EV −0.009、SE 10.95% → 仍判负。
2. **融合增量 / lead-lag**：按预登记规则 E1 未过即不启动；档位合并方案另因单位不同、覆盖不对等
   （OKX ±6bps vs 币安 ±30bps，覆盖比 0.202）、相对结构特征失真三点否决。

**跨所三路（独立特征 / 附加特征 / 档位合并）全部否决，方向关闭。** 数据管线与本文保留作复用资产，
但不再往前推进。
