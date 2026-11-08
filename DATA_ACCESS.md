# 数据获取与使用（给下游项目）

本仓库是**数据生产方**：跑 Binance Vision（K线/aggTrades/metrics）与 L2 订单簿的重建，
把结果切成 parquet，发布到 GitHub Release。下游项目（特征研究、事件合约预测/
回测等）**不要** clone 本仓库重跑管线，直接拉 Release 资产即可。

- 所有时间戳都是 **UTC**，类型 **Int64 毫秒 epoch**（不是 datetime、不是秒）。
- 所有产物**按时间升序**写盘（`bucket` / `close_time` 单调增），lazy scan 的下推和
  `join_asof` 都依赖这一点。
- K 线（`vision-base`）**按自然年**切分，每日自动更新，最新覆盖到的日期见
  [`DATA_STATUS.md`](DATA_STATUS.md)；订单簿侧仍按月切分。
- 仓库里 `**/data/` 被 gitignore，产物**只走 Release**，不在 git 里。

---

## 1. Release 一览

**做研究/建模只需要两个 tag**：K 线用 `vision-base`，订单簿特征用 `ob-dataset-vol8`。
两者主键都是 `close_time`（Int64 ms UTC），可以直接 join。

| Release tag | 资产 | 命名 | 体量 | 用途 |
|---|---|---|---|---|
| `vision-base` | 每标的每年 1 个 parquet | `{SYMBOL}_1m_klines_{YYYY}.parquet` | 每年约 20–30 MB | **主用**。原始 1m K 线（um + spot，15 列），**不是特征**，每日自动更新 |
| `ob-dataset-vol8` | 17 个 parquet | `ob_BTCUSDT_{YYYY-MM}.parquet`（逐月）<br>`ob_BTCUSDT.parquet`（全窗口，**推荐**） | 月度 16–26 MB<br>全窗口 ~375 MB | **主用**。订单簿特征（**185 列**，仅盘口，不含 K 线），见 §3.3 |
| `ob-features-vol8` | 32 个 parquet | `ob_features_{exchange}_{YYYY-MM}.parquet` | 约 250 MB | L1 截面**中间态**（每交易所各自一份，重算数据集用；正常消费不需要） |
| `data-merged` | 64 个 parquet | `{exchange}_minute_band_{YYYY-MM}.parquet`<br>`{exchange}_minute_band_mid_{YYYY-MM}.parquet` | 10.5 GB | L2 原始窄带快照（要自己重算盘口特征时才用，见 §3.2） |
| `data-{YYYY-MM}` | 每日 zip | `{YYYY-MM-DD}.zip` | 16 个月，~10 GB | 重建的**中间态**（每日 band+mid），正常消费**不需要** |
| `ob-features` | 32 个 parquet | `ob_features_{exchange}_{YYYY-MM}.parquet` | 约 100 MB | **历史**：固定 bps 分箱（`b0..b4`）口径，与 vol8 不可混用 |
| `merged-features` | 17 个 parquet | `merged_BTCUSDT_{YYYY-MM}.parquet`<br>`BTCUSDT.parquet` | 约 200 MB | **历史**：订单簿 + K 线的合并宽表，仅保留给既有 bn_bt 模型；新项目请用上面两套自己 join |

- `SYMBOL`：`BTCUSDT` / `ETHUSDT`（`ob-dataset-vol8` 当前只有 `BTCUSDT`）
- `exchange`：`binance_futures`（U 本位合约）/ `binance_spot`（现货）
- 年份：`vision-base` 为 `2020` … `2026`（`2020-01-01` 起），每标的每年一个文件；
  **当年那份仍在增长**，跨年后冻结不再改动
- 月份：`2025-07` … `2026-10`（订单簿侧），每个 tag 内月份齐全、没有缺月。
  **订单簿目前是手动/月度重建，不是每日自动更新**（覆盖到哪天以最新资产为准）

资产直链（公开仓库，**无需 token**）：

```
https://github.com/bailiang0369/bn_hisdata/releases/download/vision-base/BTCUSDT_1m_klines_2026.parquet
https://github.com/bailiang0369/bn_hisdata/releases/download/ob-dataset-vol8/ob_BTCUSDT_2025-08.parquet
https://github.com/bailiang0369/bn_hisdata/releases/download/data-merged/binance_futures_minute_band_mid_2025-07.parquet
```

Release 都标了 **prerelease**，但内容可用；`gh release download` 需要 `--pattern` 指定
资产名（prerelease 不影响直链下载）。

---

## 2. 怎么拉

### 2.1 用 gh CLI

```bash
# K 线：BTCUSDT 全部年份（2 个文件、几十 MB）
gh release download vision-base --repo bailiang0369/bn_hisdata \
    --pattern 'BTCUSDT_1m_klines_*.parquet' --dir ./data/vision

# 订单簿特征：全窗口单文件（推荐，375 MB）
gh release download ob-dataset-vol8 --repo bailiang0369/bn_hisdata \
    --pattern 'ob_BTCUSDT.parquet' --dir ./data/ob

# 只要订单簿原始 mid（每分钟一行，自己算特征时用）
gh release download data-merged --repo bailiang0369/bn_hisdata \
    --pattern 'binance_futures_minute_band_mid_*.parquet' --dir ./data/ob
```

### 2.2 用本仓库的下载工具（推荐，可续传、可过滤、无依赖 token）

```bash
python tools/fetch_release.py --tag vision-base --pattern 'BTCUSDT_*' --dest ./data/vision
python tools/fetch_release.py --tag ob-dataset-vol8 --pattern 'ob_BTCUSDT_*.parquet' --dest ./data/ob
python tools/fetch_release.py --tag data-merged --pattern '*minute_band_mid_*' --dest ./data/ob

# 先看这个 tag 里有什么（不下载）
python tools/fetch_release.py --tag ob-dataset-vol8 --list
```

### 2.3 最小可跑样本（接新项目时先跑这个）

别一上来就拉全量。最小组合 = **1 年 K 线 + 1 个月订单簿特征**，约 55 MB、
30 秒内下完，足够验证读表、对齐、列名：

```bash
python tools/fetch_release.py --tag vision-base \
    --pattern 'BTCUSDT_1m_klines_2025.parquet' --dest ./data/vision
python tools/fetch_release.py --tag ob-dataset-vol8 \
    --pattern 'ob_BTCUSDT_2025-08.parquet' --dest ./data/ob

python - <<'PY'
import polars as pl
k = pl.read_parquet("data/vision/BTCUSDT_1m_klines_2025.parquet")
ob = pl.read_parquet("data/ob/ob_BTCUSDT_2025-08.parquet")
print(k.shape, ob.shape)                     # (525600, 15)  (44640, 185)
df = k.join(ob, on="close_time", how="inner")  # 两侧同键，直接 join
print(df.shape, df["ob_f_ret_1m_bps"].null_count())   # (44640, 199) ...
assert df.height > 44_000
PY
```

> Windows PowerShell 不支持 `<<'PY'` 这种 heredoc，把中间那段存成 `check.py` 再
> `python check.py` 即可。

想连原始盘口一起验，再加一个月的 `data-merged` 的 `*_band_mid_*`（注意它的主键是
`bucket`，对齐要 `close_time - 60000`，见 §4）。

### 2.4 按年/按月懒加载（不下全量）

单年 vision 文件约 20–30 MB（1 分钟粒度、每标的每年 52.5 万行），ob 月度 band
几十~一两百 MB。研究阶段完全可以只拉用到的那几年/几个月：

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

### 3.1 `vision-base`：15 列

| 列 | 类型 | 说明 |
|---|---|---|
| `close_time` | Int64 | **主键**。该分钟 K 线的结束时刻（见 §4） |
| `open` `high` `low` `close` | Float32 | 合约（um）1m K 线 |
| `um_buy_vol` `um_sell_vol` | Float32 | 合约主动买/卖量（`sell_vol = volume - taker_buy_volume`，全精度相减后再降 f32） |
| `um_count` | Int64 | 合约成交笔数 |
| `spot_open` `spot_high` `spot_low` `spot_close` | Float32 | 现货 1m K 线（2026-11 起保留 OHLC：只留 close 就算不出现货振幅/收盘位置） |
| `spot_buy_vol` `spot_sell_vol` | Float32 | 现货主动买/卖量 |
| `spot_count` | Int64 | 现货成交笔数 |

**注意事项**

- **没有 `open_time`**，只有 `close_time`；要按 bar 起点索引得自己 `close_time - 60000`。
- **没有 `basis`**（2026-11 删除）：合约 `close` 与现货 `spot_close` 都在表里，
  绝对价差 / bps / z-score 下游随时能算，基础表不放可推导的冗余列。
- **没有 metrics 那 5 列**（`sum_open_interest` / `sum_open_interest_value` /
  三个多空比，2026-11 删除）：源端 5 分钟一档，`+5m` 后才 asof 回填到 1 分钟，
  1 分钟表里是阶梯常量（实测 43200 行只约 8600 个独立值），对 3 分钟方向单变量
  `|IC| <= 0.025`，且 OI 水平与时间趋势共线（`spearman(OI, time) = -0.55`）属伪相关。
  taker 主动买卖量比同理由 `um_buy_vol / um_sell_vol` 自算（1 分钟、无回填）。
- 这是**默认**产出的 **base** 表，**不含** aggTrades 微观结构列（`um_agg_*`、`spot_agg_*`、
  `rv_dominance` 共 20 列；只有显式加 `--with-agg` 才会产出完整 master 表）。
  OHLC / 主动买卖量的衍生特征工程一律下沉到下游项目（现成底座见 §3.3 订单簿特征）。

**aggTrades 为什么不做（2026-11 决策 + 专项实测）**

最初的动机是"1 分钟聚合会抹平分钟内的成交方向"：可能前段净买、后段净卖、整分钟
接近均衡，但末段（最靠近收线）是卖主导，而这部分对 3~5 分钟方向也许更有解释力。
为验证这一点，用 1 个月（2026-09，43200 分钟）合约逐笔数据算了不同末窗的主动买卖
失衡 `ofi = (buy−sell)/(buy+sell)`：

| 末窗 | IC (h=3) | IC (h=5) |
|---|---|---|
| 5 秒 | +0.0038 | −0.0041 |
| 10 秒 | −0.0020 | −0.0050 |
| 30 秒 | −0.0096 | −0.0165 |
| 60 秒（= 整分钟） | −0.0179 | −0.0226 |

**IC 随窗口缩短单调变弱，不存在"末段溢价"**；"前段/后段背离"
（`ofi_tail30 − ofi_head30`）的 IC 仅 +0.0028 / +0.0018，等于噪声（该样本 IC 标准误
≈ 0.0048）。聚合在这里起的是**降噪**作用，1 分钟 taker 失衡已是该维度最优粒度。
自检：用 aggTrades 还原的整分钟失衡与 K 线 `taker_buy_volume` 推出的版本
相关系数 **0.9999**，说明解析与非对齐都没有问题。

另外做过 `|ofi|` 极端值 × 波动率分档的组合筛选（3 分钟方向、反向选边）：最高
**52.26%**（低波 × top10~20%），离 80% payout 的 **55.6%** 盈亏平衡差 3.3pp，
且档内十分位准确率在 47.7%~53.2% 之间无单调性。

成本对照：合约 aggTrades 约 **16MB/天**（zip），15 个月两市场约 7.3GB 流量 + 逐日
流式处理。用 480MB 的单月验证换来这个结论后，**决定不做**。

顺带记录一个探测结果，免得重复踩：**合约没有 1s K 线**
（`futures/um/{daily,monthly}/klines/BTCUSDT/1s/` 实测 404，多个月份都试过），
只有现货有（`spot/.../1s/` 约 2.5MB/天、70~85MB/月）。但现货末段不能替代合约末段
——合约才是价格发现主市场，且两个市场的末段订单流在 basis 变动时正好分道扬镳。

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
- （历史）vision 侧的 metrics 列曾有零星空值，2026-11 起 metrics 已整体移出基础表，
  该问题不再存在。

### 3.3 `ob-dataset-vol8`：订单簿特征（185 列）

- `ob_BTCUSDT.parquet`：**全窗口单文件**（2025-07 ~ 2026-10 共 16 个月，
  约 70 万行，375 MB），下游直接用这个（推荐）。
- `ob_BTCUSDT_{YYYY-MM}.parquet`：逐月，供单月消费 / 增量重跑。

**主键 `close_time`**，与 `vision-base` 同键同语义，`join(on="close_time")` 即可，
**不需要** 减 60000（那是 `data-merged` 的 `bucket` 才有的换算，见 §4）。

列构成：`1 + 90 + 90 + 4 = 185`——`close_time` + futures 块 + spot 块 + 跨市场。
两块完全对称，futures 用前缀 `ob_f_`、spot 用 `ob_s_`（下表把前缀写成 `{p}`）。

| 组 | 列 | 数量 |
|---|---|---|
| 中间价 | `{p}mid` | 1 |
| 深度分箱量 | `{p}q_{bid,ask}_v0..v4` | 10 |
| 深度分箱档数 | `{p}n_{bid,ask}_v0..v4` | 10 |
| 分箱 VWAP（相对 mid，bps） | `{p}vwap_{bid,ask}_v0..v4_bps` | 10 |
| 波动尺度 | `{p}sigma_bps`（过去 30 分钟 1m 收益标准差） | 1 |
| 价差 | `{p}spread_bps` | 1 |
| 侧总量 | `{p}q_bid` `{p}q_ask` `{p}n_bid` `{p}n_ask` | 4 |
| 最大单档 | `{p}qmax_{bid,ask}` `{p}qmax_dist_bps_{bid,ask}` | 4 |
| 不平衡 | `{p}obi_total` `{p}obi_v0` `{p}obi_near` `{p}obi_far` `{p}obi_tilt` | 5 |
| 加权中价 | `{p}wmid_bps` | 1 |
| 冲击 | `{p}impact_{bid,ask}_bps_per_btc` `{p}impact_imb` | 3 |
| 深度形态 | `{p}depth_conc_{bid,ask}` `{p}levels_ratio` `{p}qmean_{bid,ask}` `{p}hhi_{bid,ask}` `{p}qmax_ratio_{bid,ask}` `{p}qcv_{bid,ask}` | 11 |
| 缺分钟标记 | `{p}missing`（1=该分钟源端无盘口，`{p}*` 全 null） | 1 |
| 收益 | `{p}ret_1m_bps` | 1 |
| 一阶差分 | `{p}dobi_1m` `{p}dobi_5m` `{p}ddepth_bid_1m` `{p}ddepth_ask_1m` `{p}ddepth_bid_5m` `{p}dwmid_1m` `{p}dspread_1m` | 7 |
| 5m / 15m 滚动 | `{p}rv_{5m,15m}_bps`、`{p}obi_mean/std_{5m,15m}`、`{p}spread_mean/std_{5m,15m}`、`{p}impact_mean_{5m,15m}` | 12 |
| 滚动 z-score | `{p}obi_z_5m` `{p}obi_z_15m` `{p}spread_z_15m` | 3 |
| 波动自适应 | `{p}vol_med_7d_bps` `{p}vol_med_60m_bps` `{p}vol_ratio` `{p}vol_bucket` `{p}ret_vol_norm` | 5 |
| 跨市场 | `ob_basis_bps` `ob_depth_ratio_f_s` `ob_spread_ratio_f_s` `ob_lead_f_s_bps` | 4 |

- 分箱口径是 **vol8**：`|px−mid|/mid / σ_t` 落在 `[1,2,4,8]σ` 的哪一段（`v0..v4`），
  所以分箱既跨交易所可比、也跨波动时段可比。
- 单位：距离一律 bps，量一律 BTC；`close_time` 为 Int64，`{p}mid` 为 f64，
  其余特征为 f32/f64 混排（按列名取值，不要按位置）。
- 缺分钟不补数：reindex 到连续分钟网格后 `{p}mid` 为 null、`{p}missing=1`，
  滚动窗口按日历时间滑过缺口。建模前自行 drop 或填充。
- `ob_*` 在 `close_time=t` 表示"第 t 根 bar 收盘瞬间的盘口"，用它预测 t+1 及以后无前视；
  **不要**用它解释同一根 bar 的收益（同一时刻，内生）。
- 公式级口径见 [`features/docs/feature_spec.md`](features/docs/feature_spec.md)；
  本表**不含 K 线**，成交 × 挂单（`atk_*`）、振幅倍数不平衡（`ob_amp_obi_*`）由
  `features/ob_kline_cross.py` 显式 join K 线后生成（键仍是 `close_time`）。

### 3.4 `merged-features`（历史，逐步退役）

`BTCUSDT.parquet` = vision-base 15 列 + 订单簿特征 + `amp_*` / `atk_*` 的合并宽表，
只保留给既有 bn_bt 模型消费。新项目请用 `vision-base` + `ob-dataset-vol8` 自己 join
（§5 有现成代码）。

---

## 4. 时间对齐（最容易踩的地方）

**先看你的订单簿数据是哪一层**——`ob-dataset-vol8` 已经转成 `close_time`，
可以**直接 join**；只有 `data-merged` / `data-{YYYY-MM}` 的原始 `bucket` 才差一分钟。

| 数据 | 时间列 | 语义 |
|---|---|---|
| `vision-base` | `close_time` | 该 bar 的**结束**时刻 +1ms = **下一分钟起点**。例：`close_time = 2026-06-01T00:00:00.000Z` 对应的 bar 覆盖 `05-31 23:59:00 ~ 06-01 00:00:00` |
| `ob-dataset-vol8` | `close_time` | 同上（`close_time = bucket + 60000` 已由生产端换算），**直接 join** |
| `data-merged` / `data-{YYYY-MM}` | `bucket` | 该分钟的**起点**，状态取 `[bucket, bucket+60s)` 内最后一条消息之后 |

```python
# 订单簿特征 + K 线：同键，直接 join（推荐路径）
feat = k.join(ob, on="close_time", how="inner")
```

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

> 只要订单簿**特征**、不想自己算盘口，就别走这一节：`ob-dataset-vol8` 已经是
> `close_time` 主键，`k.join(ob, on="close_time")` 一行搞定（见 §3.3）。
> 下面是从**原始 band/mid** 自己拼研究表的完整示例（要自定义特征时才需要）。

```python
"""把 vision-base + 订单簿 mid 按分钟对齐成一张研究宽表。"""
import polars as pl
from pathlib import Path

VISION = Path("./data/vision")   # BTCUSDT_1m_klines_*.parquet
OB = Path("./data/ob")           # binance_futures_minute_band_mid_*.parquet


def load(symbol: str, years: list[int], months: list[str],
         exchange: str = "binance_futures"):
    """返回 (lazy 宽表, 两侧行数统计)。用 lazy 拼，别 collect 全量。

    K 线侧按年切文件（years），订单簿侧按月切文件（months），两侧粒度不同很正常：
    年份文件用 `scan_parquet` 一串目录拼起来即可，列结构一致、时间不重叠。
    """
    # 1) K 线侧：统一坐标 close_time -> 分钟起点 bucket
    k = (
        pl.scan_parquet([VISION / f"{symbol}_1m_klines_{y}.parquet" for y in years])
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


lf, k, ob = load("BTCUSDT", [2025], ["2025-07", "2025-08"])
df = lf.collect()          # 小体量随便 collect；全量请保持 lazy 或按年/月分批处理
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
- **以为还有 metrics 列可当分钟级信号** → 2026-11 已整体移出（源端 5 分钟粒度、
  5 根 bar 内是常数，且滞后 5~10 分钟）。
- **以为 base 表含微观结构特征** → 不含，只有 15 列（无 `*_agg_*`、无 `rv_dominance`）。
- **以为 base 表有 `basis` / metrics 5 列** → 2026-11 已删：`basis` 可由 `close`/`spot_close`
  现算；OI/多空比是 5 分钟一档再滞后 5m 回填的阶梯常量，对 3 分钟方向无信息；
  taker 比用 `um_buy_vol/um_sell_vol` 现算（1 分钟粒度）。
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
| `vision-base` | `binance_vision/pipeline_daily.py`（每日，`.github/workflows/vision-daily.yml`）<br>`binance_vision/kaggle/vision_run.py`<br>`binance_vision/run_base_table_cloud.sh` | 日常：`update` 补 UTC 昨天 → 并入**当年**年度 parquet → 覆盖发布（历史年份不重写）；首次/补洞：`backfill --start ... ` 按年聚合后每年上传一次 |
| `data-YYYY-MM` | `orderbook/kaggle/run_orderbook.py` | 逐天 `pipeline.py` → 打包 `{date}.zip` 上传，支持断点续传（**手动/批量运行，不自动**） |
| `data-merged` | `orderbook/kaggle_merge/merge_run.py` | 拉每日 zip → `merge_monthly.py` → 传 `data-merged` |
| `ob-features-{mode}`<br>`ob-dataset-{mode}` | `features/kaggle/run_features.py` | 直接拉 `data-YYYY-MM` 日包（不再依赖 `data-merged`）→ `ob_features.py` 算 L1 → `build_ob_dataset.py` 全窗口滚动成订单簿数据集（**不含 K 线**）→ 传 L1 与 `ob_BTCUSDT_{m}.parquet` + 单文件 `ob_BTCUSDT.parquet`。跨域特征（成交 vs 挂单）用 `features/ob_kline_cross.py` 单独 join K 线 |
| ~~`merged-features`~~ | （历史）`features/build_dataset.py` | 已拆分为上面两段：订单簿数据集 + 跨域特征。<br>该 Release 是旧的「订单簿 + K 线」合并宽表，仅保留给现有 bn_bt 模型消费 |

- 凭据一律走环境变量或 Kaggle Secrets（`GH_TOKEN` / `CHD_API_KEY` / `KAGGLE_USERNAME` /
  `KAGGLE_KEY`），**代码里不留密钥**（仓库是 public）。
- 扩窗口 = 改脚本里的 `START` / `END` 重推内核，产物按月增量，已有的月份会自动 skip；
  **边界月**（`END` 落在月中）的 L1 会按 `close_time` 覆盖度自动判定并重算覆盖上传。
- 只有 K 线是每日自动更新的（`vision-daily.yml`，用内置 `GITHUB_TOKEN`）。订单簿链路
  需要 CryptoHFTData 的 `CHD_API_KEY`，目前**按需/按月重跑**：`run_orderbook.py` →
  `merge_run.py` → `run_features.py` 三步，每一步都断点续传。
- 测试：`python -m pytest -q`（仓库根，约 10s）。`orderbook/bench_r2_speed.py`、
  `orderbook/test_download.py` 是**手动**联网脚本，不参与收集。
