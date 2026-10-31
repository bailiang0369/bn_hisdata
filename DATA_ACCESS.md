# 数据获取与使用（给下游项目）

本仓库是**数据生产方**：跑 Binance Vision（K线/aggTrades/metrics）与 L2 订单簿的重建，
把结果按 **月** 切成 parquet，发布到 GitHub Release。下游项目（特征研究、事件合约预测/
回测等）**不要** clone 本仓库重跑管线，直接拉 Release 资产即可。

- 所有时间戳都是 **UTC**，类型 **Int64 毫秒 epoch**（不是 datetime、不是秒）。
- 所有产物**按时间升序**写盘（`bucket` / `close_time` 单调增），lazy scan 的下推和
  `join_asof` 都依赖这一点。
- 覆盖窗口：**2025-07-01 ~ 2026-10-20**，分钟粒度。
- 仓库里 `**/data/` 被 gitignore，产物**只走 Release**，不在 git 里。

---

## 1. 三个 Release 是什么

| Release tag | 资产 | 命名 | 体量 | 用途 |
|---|---|---|---|---|
| `vision-base` | 32 个 parquet | `{SYMBOL}_base_features_{YYYY-MM}.parquet` | 54 MB | **主用**。K线+基差+合约 metrics 的 1 分钟宽表 |
| `data-merged` | 64 个 parquet | `{exchange}_minute_band_{YYYY-MM}.parquet`<br>`{exchange}_minute_band_mid_{YYYY-MM}.parquet` | 10.5 GB | **主用**。L2 订单簿的分钟级窄带快照 |
| `data-{YYYY-MM}` | 每日 zip | `{YYYY-MM-DD}.zip` | 16 个月，~10 GB | 重建的**中间态**（每日 band+mid + 原始小时文件），正常消费**不需要** |

- `SYMBOL`：`BTCUSDT` / `ETHUSDT`
- `exchange`：`binance_futures`（U 本位合约）/ `binance_spot`（现货）
- 月份：`2025-07` … `2026-10`，每个 tag 内该标的/交易所**月份齐全，没有缺月**

资产直链（公开仓库，**无需 token**）：

```
https://github.com/bailiang0369/bn_hisdata/releases/download/vision-base/BTCUSDT_base_features_2025-07.parquet
https://github.com/bailiang0369/bn_hisdata/releases/download/data-merged/binance_futures_minute_band_mid_2025-07.parquet
```

Release 都标了 **prerelease**，但内容可用；`gh release download` 需要 `--pattern` 指定
资产名（prerelease 不影响直链下载）。

---

## 2. 怎么拉

### 2.1 用 gh CLI

```bash
# 只要 vision 的 BTCUSDT，全部月份
gh release download vision-base --repo bailiang0369/bn_hisdata \
    --pattern 'BTCUSDT_base_features_*.parquet' --dir ./data/vision

# 只要订单簿 mid（每分钟一行，做时间对齐最常用）
gh release download data-merged --repo bailiang0369/bn_hisdata \
    --pattern 'binance_futures_minute_band_mid_*.parquet' --dir ./data/ob
```

### 2.2 用本仓库的下载工具（推荐，可续传、可过滤、无依赖 token）

```bash
python tools/fetch_release.py --tag vision-base --pattern 'BTCUSDT_*' --dest ./data/vision
python tools/fetch_release.py --tag data-merged --pattern '*minute_band_mid_*' --dest ./data/ob
```

### 2.3 逐月懒加载（不下全量）

单月 vision 文件 1.1–1.8 MB，ob 月度 band 几十~一两百 MB。研究阶段完全可以按需拉：

```python
import requests, pathlib

BASE = "https://github.com/bailiang0369/bn_hisdata/releases/download"

def fetch(tag: str, name: str, dest: pathlib.Path) -> pathlib.Path:
    """下载单个资产，已存在则跳过（幂等）。"""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    with requests.get(f"{BASE}/{tag}/{name}", stream=True, timeout=120) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
        tmp.replace(dest)
    return dest
```

---

## 3. Schema

### 3.1 `vision-base`：19 列

| 列 | 类型 | 说明 |
|---|---|---|
| `close_time` | Int64 | **主键**。该分钟 K 线的结束时刻（见 §4） |
| `open` `high` `low` `close` | Float32 | 合约（um）1m K 线 |
| `um_buy_vol` `um_sell_vol` | Float32 | 合约主动买/卖量（`sell_vol = volume - taker_buy_volume`，全精度相减后再降 f32） |
| `um_count` | Int64 | 合约成交笔数 |
| `spot_close` | Float32 | 现货 1m 收盘 |
| `spot_buy_vol` `spot_sell_vol` | Float32 | 现货主动买/卖量 |
| `spot_count` | Int64 | 现货成交笔数 |
| `basis` | Float32 | 基差（合约 − 现货） |
| `sum_open_interest` | Float32 | 合约持仓量 |
| `sum_open_interest_value` | Float32 | 持仓名义价值 |
| `count_toptrader_long_short_ratio` | Float32 | 大户多空比（按人数） |
| `sum_toptrader_long_short_ratio` | Float32 | 大户多空比（按仓位） |
| `count_long_short_ratio` | Float32 | 全市场多空比（按人数） |
| `sum_taker_long_short_vol_ratio` | Float32 | 主动买卖量比 |

**注意事项**

- **没有 `open_time`**，只有 `close_time`；要按 bar 起点索引得自己 `close_time - 60000`。
- 这是 `--no-agg` 的 **base** 表，**不含** aggTrades 微观结构列（`um_agg_*`、`spot_agg_*`、
  `rv_dominance` 共 20 列）。需要那些列得重跑第二阶段的 master 表，当前 Release 里没有。
- 末 6 列（`sum_open_interest` 起）源端是 **5 分钟一档**，已被前向填充到 1 分钟——
  相邻 5 根 bar 这几个值相同，**不要当 1 分钟独立信息**用。

### 3.2 `data-merged`：band 与 mid

`binance_{exchange}_minute_band_{YYYY-MM}.parquet`（长表，每分钟 × 每档）

| 列 | 类型 | 说明 |
|---|---|---|
| `bucket` | Int64 | **主键**。分钟起点（见 §4） |
| `side` | String | `bid` / `ask` |
| `px` | Float64 | 档位价（必须是 f64：现货 tick 0.01，f32 在 >131072 会把相邻档合并） |
| `q` | Float32 | 该档挂单量，`q > 0` 才有意义（0/负已在生产端过滤） |
| `mid` | Float64 | 该分钟的中间价，用于 band 过滤 |

`binance_{exchange}_minute_band_mid_{YYYY-MM}.parquet`（每分钟一行，做对齐就用它）

| 列 | 类型 | 说明 |
|---|---|---|
| `bucket` | Int64 | **主键** |
| `best_bid` `best_ask` | Float64 | 买一/卖一价 |
| `mid` | Float64 | `(best_bid + best_ask) / 2` |
| `spread` | Float64 | `best_ask - best_bid` |

**注意事项**

- band 只保留 **mid ±0.3%** 内的档位（面向分钟级超短线，远档无关）。要更宽得重跑管线加 `--band`。
- 一分钟一行的是 **mid 文件**，band 是稀疏长表（每分钟行数不定，行情剧烈时档更多）。
- `bucket` 的语义是"**该分钟内最后一条消息之后**的盘口状态"，不是该分钟的平均或起始状态。
- **mid 文件有重复行，join 前必须去重**。实测 `2025-07` futures：45 382 行 / 44 639 个
  去重后的 `bucket`，即 743 个 bucket 各出现两次（两行内容**完全相同**，不是不同 side），
  是生产端重复写入。所以一律 `unique(subset="bucket")` 或 `group_by("bucket").first()`。
- **mid 文件偶有缺分钟**。实测 `2025-07` 有 2 处 120 s 间隔、`2025-08` 少 19 个 bucket
  （该分钟无盘口，被单边过滤掉）。即"逐分钟无缝"只对 vision 侧成立：两个月合并后
  `inner join` 丢 **21/89 279 = 0.024%** 的分钟。
- vision 侧的 metrics 列也有零星空值（`count_toptrader_long_short_ratio` 约 0.5%，
  其余 metrics 列 ≈0），来源是源端缺档，不是 join 造成的。

---

## 4. 时间对齐（最容易踩的地方）

两个数据集用的是**不同锚点**，差整整一分钟：

| 数据集 | 时间列 | 语义 |
|---|---|---|
| `vision-base` | `close_time` | 该 bar 的**结束**时刻 +1ms = **下一分钟起点**。例：`close_time = 2026-06-01T00:00:00.000Z` 对应的 bar 覆盖 `05-31 23:59:00 ~ 06-01 00:00:00` |
| 订单簿 | `bucket` | 该分钟的**起点**，状态取 `[bucket, bucket+60s)` 内最后一条消息之后 |

**对齐公式**：同一根 bar 的盘口状态是

```python
bucket == close_time - 60_000
```

也就是 `close_time` 是**右开区间标签**，`bucket` 是**左闭区间标签**。生产端已经
把 `close_time + 1ms` 规整成整分钟边界，所以直接相减 60000 即可，不需要处理 999ms 尾巴。

```python
# 把订单簿挂到 K 线上
feat = vision.join(ob_mid, left_on=(pl.col("close_time") - 60_000), right_on="bucket")
```

**另一种常见做法（等价）**：用 `bucket` 作统一索引，把 vision 的
`close_time - 60000` 命名为 `bucket`，两边都按 `bucket` join，之后所有特征都在这个
"分钟起点"坐标上——**推荐**，因为订单簿只有 `bucket`，无法反向迁就。

> **错位不会报错，只会静默错一分钟。** 两边都是连续分钟序列，直接用
> `close_time == bucket` join **也能匹配上**（实测 44 639 行里匹配上 44 579 行），
> 只是每一行都配到了相邻一分钟的盘口。这类错误 join 不抛异常、行数看着也正常，
> 唯一能发现的方式是校验下面的差分：
>
> ```python
> # 正确对齐后，两边的 bucket 序列应当高度重叠；错位一分钟会整体偏移
> assert ku.join(ou, on="bucket").height > ku.height * 0.999
> ```
>
> 提交前务必跑一次 §5 末尾的对齐校验，别靠肉眼。

**月份切分按 `close_time` 的月归属**（不是按 bar 起点）。实测：

| 文件 | 行数 | 首行 `close_time` | 末行 `close_time` |
|---|---|---|---|
| `2025-07`（首月） | 44 639 | `07-01 00:01` | `07-31 23:59` |
| `2025-08` | 44 640 | `08-01 00:00` | `08-31 23:59` |
| `2026-06` | 43 200 | `06-01 00:00` | `06-30 23:59` |

读法：**每个月的文件都含"上月最后一根 bar"**（其 `close_time` 正好是当月 1 日 00:00），
并把"本月最后一根"推到下月文件。所以：

- **逐月拼接无缺口**，缺口只在首月 `2025-07` 前（无上月尾巴，故少 1 行 = 44 639）。
- **单独拿一个月做回测，边界整体左移一分钟**：所谓"6 月"实际是
  `05-31 23:59 ~ 06-30 23:58` 的 bar。长周期回测请**先全量拼接再按自己的窗口切**，
  不要用月份文件的边界当交易日边界。

---

## 5. 整合成一宽表（可直接抄）

```python
"""把 vision-base + 订单簿 mid 按分钟对齐成一张研究宽表。"""
import polars as pl
from pathlib import Path

VISION = Path("./data/vision")   # BTCUSDT_base_features_*.parquet
OB = Path("./data/ob")           # binance_futures_minute_band_mid_*.parquet


def load(symbol: str, months: list[str], exchange: str = "binance_futures"):
    """返回 (lazy 宽表, 两侧行数统计)。用 lazy 拼，别 collect 全量。"""
    # 1) K 线侧：统一坐标 close_time -> 分钟起点 bucket
    k = (
        pl.scan_parquet([VISION / f"{symbol}_base_features_{m}.parquet" for m in months])
        .select(
            (pl.col("close_time") - 60_000).alias("bucket"),
            pl.exclude("close_time"),
        )
    )
    # 2) 订单簿侧：必须去重（存在完全重复的 bucket 行），再改列名避免撞车
    ob = (
        pl.scan_parquet([OB / f"{exchange}_minute_band_mid_{m}.parquet" for m in months])
        .unique(subset="bucket", keep="last")
        .rename({"mid": "ob_mid"})
    )
    # 3) 内连接：只保留两边都有的分钟
    return k.join(ob, on="bucket", how="inner").sort("bucket"), k, ob


lf, k, ob = load("BTCUSDT", ["2025-07", "2025-08"])
df = lf.collect()          # 小体量随便 collect；全量 477 天请保持 lazy 或分月处理
print(df.shape, df.columns)
```

**对齐校验（务必做一次，尤其第一次接数据时）**

```python
k_n = k.select(pl.len()).collect().item()
ob_n = ob.select(pl.len()).collect().item()
j_n = df.height
print(f"vision={k_n} ob={ob_n} joined={j_n} 丢分钟={(k_n - j_n) / k_n:.4%}")

# 两侧分钟数应基本相等（ob 侧每月零星缺几到十几个分钟；2025-07+08 实测差 19/89279）
assert abs(k_n - ob_n) / k_n < 0.001, (k_n, ob_n)
# 实测丢 21/89279 ≈ 0.024%（订单簿侧偶有无盘口的分钟）
assert j_n > k_n * 0.999, (j_n, k_n)

# 正确对齐后 vision 的桶序列严格 60s 递增
assert k.select(pl.col("bucket").is_sorted()).collect().item()
assert k.select(pl.col("bucket").diff().drop_nulls().max()).collect().item() == 60_000
```

> 这几个断言的价值在于：如果你把 `- 60_000` 写漏了，`j_n` 依然会接近 `k_n`（都是连续
> 分钟序列），**只有断言通不过时才发现**——所以第一次接数据时一定跑一遍，之后可以固化成
> 下游项目里的冒烟测试。

---

## 6. 常见坑

- **用 `close_time` 直接 join `bucket`** → **能 join 上**，但每行配到相邻一分钟的盘口，
  静默错位、不报错、行数也正常。先减 60000。
- **mid 文件不去重直接 join** → 743 个/月的完全重复行会把结果放大，先 `unique(subset="bucket")`。
- **把 `bucket` 当 datetime 用** → 它是 Int64 ms。`pl.from_epoch(pl.col("bucket"), time_unit="ms")` 转。
- **只用单月文件切训练/测试窗口** → 边界偏一分钟（见 §4）。全量拼完再切。
- **把 metrics 那 6 列当分钟级信号** → 源端 5 分钟粒度，5 根 bar 内是常数。
- **以为 base 表含微观结构特征** → 不含，只有 19 列（无 `*_agg_*`、无 `rv_dominance`）。
- **以为 band 是全档** → 只到 mid ±0.3%。算深度/斜率类特征时注意这是截断后的盘口。
- **无脑拉 `data-2026-10` 那类日度 release** → 10 GB 且是中间态（含原始小时文件），
  消费端用 `data-merged` 的月度 parquet 就够了。
- **现货/合约混用同一个 `bucket` 的前提** → band 和 mid 都按交易所分开，`bucket` 是同一坐标系
  （同为 UTC 分钟起点），可以直接 join；但**不要**把 `binance_spot_*` 和
  `binance_futures_*` 的同名列直接拼一起，先加前缀。

---

## 7. 生产端在哪（要重跑/扩窗口时才看）

| 产物 | 生产脚本 | 说明 |
|---|---|---|
| `vision-base` | `binance_vision/kaggle/vision_run.py`<br>`binance_vision/run_base_table_cloud.sh` | Kaggle 内核 `byh850515/vision-base-features`；拉 klines/metrics → `build_master_features.py --no-agg` → 按月切 → 传 Release |
| `data-YYYY-MM` | `orderbook/kaggle/run_orderbook.py` | 逐天 `pipeline.py` → 打包 `{date}.zip` 上传，支持断点续传 |
| `data-merged` | `orderbook/kaggle_merge/merge_run.py` | 拉每日 zip → `merge_monthly.py` → 传 `data-merged` |

- 凭据一律走环境变量或 Kaggle Secrets（`GH_TOKEN` / `CHD_API_KEY` / `KAGGLE_USERNAME` /
  `KAGGLE_KEY`），**代码里不留密钥**（仓库是 public）。
- 扩窗口 = 改脚本里的 `START` / `END` 重推内核，产物按月增量，已有的月份会自动 skip。
