# 订单簿 × vision 合并特征规格说明（feature_spec）

> 本文件是 `features/` 模块的特征口径与列清单。下游可直接按 Release 拉取，
> 不重跑管线：订单簿特征在 `ob-dataset-vol8`（`ob_BTCUSDT.parquet`，**185 列**），
> K 线在 `vision-base`，两者主键同为 `close_time`，直接 join。
>
> **边界**：本仓库只产出原始底座 + 订单簿衍生特征（`ob_*`）。OHLC / 主动买卖量的
> 衍生特征工程依赖下游预测目标、千变万化，**不在本仓库做**，下沉到下游项目；
> aggTrades 微观结构特征（`um_agg_*` 等）经评估增益小、成本高，亦不纳入
> （见 DATA_ACCESS §3.1）。**本表不含任何 K 线列**：`atk_*` / `ob_amp_obi_*` 由
> `features/ob_kline_cross.py` 显式 join K 线后另存（§5 / §6）。

---

## 0. 先说清楚：现货 vs 合约「不合并」

订单簿特征对 **futures 和 spot 是分别独立计算的**，各自产出完整的一套 L1+L2 特征，
**绝不做加权合并**。原因是两侧的档位粒度差一个量级（futures tick 0.1、spot tick 0.01，
同价位带内 spot 档数是 futures 的 ~10 倍），任何"按权重合并"都会把两个不同的盘口结构
糊在一起，丢失"谁先动、谁更深"的信息。

两侧通过**列前缀**区分：

| 前缀 | 含义 |
|---|---|
| `ob_f_*` | futures（U 本位合约）盘口特征 |
| `ob_s_*` | spot 盘口特征 |
| `ob_*`（无后缀） | 跨市场特征（见 §3） |

跨市场只保留少数几个**比率/差值**（基差、深度比、价差比、领先滞后），见 §3。

---

## 1. 数据流与主键

```
data-YYYY-MM 日包 ──> ob_features.py ──> L1 截面 (ob_*，61 列/交易所)
                                            │ 全窗口拼接 -> reindex -> 滚动 -> 校验
                                            ▼
                               build_ob_dataset.py ──> ob_BTCUSDT_{m}.parquet
                                                       ob_BTCUSDT.parquet（全窗口，185 列）

vision-base K 线 ───────────────────> ob_kline_cross.py ──> atk_* / ob_amp_obi_*
                                     （订单簿 × 成交量的跨域特征，显式 join）
```

订单簿特征**不掺 K 线**：盘口侧只依赖自身（futures + spot 互比），K 线更新滞后
不会阻塞它。只有 `atk_*`（成交 vs 挂单）和 `ob_amp_obi_*`（振幅倍数处的不平衡）
必须两边对齐，单独放在 `ob_kline_cross.py`，键仍是 `close_time`。

- **主键 `close_time`**：Int64 毫秒 UTC，与 vision-base 完全同键。
  订单簿原始 `bucket`（分钟起点）在 `ob_features.py` 内转成 `close_time = bucket + 60000`。
- **对齐语义**：`bucket=t` 是"第 t 分钟内最后一条消息后的盘口" = 该分钟收盘时刻的状态，
  所以 `close_time` 与 vision 的 K 线收盘时刻是同一瞬间。用它预测 **t+1 及以后** 无前视。
- 全窗口 2025-07 ~ 2026-10，分钟粒度。

---

## 2. L1 截面特征（每个交易所一份，`ob_f_`/`ob_s_`）

所有距离统一为**相对 mid 的 bps**（`d = |px−mid|/mid × 1e4`），所有量是 **BTC 枚数**。

深度按 **vol8** 分箱：先算当期波动尺度 `σ_t`（过去 30 个 1 分钟对数收益的标准差，
`shift(1)` 防前视、下限 0.5bps），再用 `d/σ_t` 落到 `v0..v4` =
`[0,1) [1,2) [2,4) [4,8) [8,∞)` **倍 σ**。σ 中位数约 3.8bps、band 到 ±30bps（≈8σ），
所以 1/2/4/8 正好铺满。相对尺度让周末低波动与美股开盘前后的分箱仍然可比。

> 固定 bps 口径 `b0..b4` = `[0,2) [2,5) [5,10) [10,20) [20,30)` bps 仍保留
> （`--bin-mode bps`），但**两套口径的 L1 不可混用**，release tag 也跟着口径走
> （`ob-features-vol8` vs 历史的 `ob-features`）。

### 2.1 基础

| 列 | 公式 | 含义 |
|---|---|---|
| `ob_mid` | `(best_bid+best_ask)/2` | 中间价 |
| `ob_spread_bps` | `(ask−bid)/mid ×1e4` | 相对价差 |
| `ob_sigma_bps` | 过去 30 个 1m 对数收益的 std（bps，`shift(1)`，下限 0.5） | 当期波动尺度，也是分箱分母 |

### 2.2 分箱（`{s}`=bid/ask，`{v}`=v0..v4）

| 列 | 含义 |
|---|---|
| `ob_q_{s}_{v}` | 该箱累计挂单量（BTC） |
| `ob_n_{s}_{v}` | 该箱档位数 |
| `ob_vwap_{s}_{v}_bps` | 该箱量加权均价相对 mid 的 bps（空箱为 null；量与档数补 0） |

### 2.3 侧总量（`{s}`=bid/ask）

| 列 | 含义 |
|---|---|
| `ob_q_{s}` | 带内（±30bps）总挂单量 |
| `ob_n_{s}` | 带内总档位数 |
| `ob_qmax_{s}` | 带内单档最大挂单量 |
| `ob_qmax_dist_bps_{s}` | 该最大单档距 mid 的 bps（"大墙"位置） |

### 2.4 不平衡（`Qb`/`Qa` 为 bid/ask 深度）

| 列 | 公式 | 含义 |
|---|---|---|
| `ob_obi_total` | `(Qb−Qa)/(Qb+Qa)` | 全带（±30bps）不平衡 |
| `ob_obi_v0` | `v0` 桶（0~1σ）不平衡 | 贴价档 |
| `ob_obi_near` | `v0+v1`（0~2σ）不平衡 | 近端 |
| `ob_obi_far` | `v3+v4`（4σ 以上）不平衡 | 远端 |
| `ob_obi_tilt` | `obi_far − obi_near` | 盘口形态倾斜（正=远端更偏买） |

### 2.5 价格

| 列 | 公式 | 含义 |
|---|---|---|
| `ob_wmid_bps` | `((VWAP_a·Qb+VWAP_b·Qa)/(Qa+Qb) − mid)/mid ×1e4` | 全带加权中价偏离 |

### 2.6 冲击 / 形态

| 列 | 公式 | 含义 |
|---|---|---|
| `ob_impact_bid_bps_per_btc` | `10 / Q_bid(v0..v2 即 0~4σ)` | 打进 10 BTC 的滑点（bps），反比近端深度 |
| `ob_impact_ask_bps_per_btc` | `10 / Q_ask(v0..v2)` | 同上（卖侧） |
| `ob_impact_imb` | `(imp_bid−imp_ask)/(imp_bid+imp_ask)` | 冲击不对称 |
| `ob_depth_conc_{s}` | `Q_{s}(v0+v1 即 0~2σ)/Q_{s}` | 近端深度占比 |
| `ob_levels_ratio` | `n_bid/n_ask` | 档数比 |
| `ob_qmean_{s}` | `Q_{s}/n_{s}` | 平均单档量 |
| `ob_hhi_{s}` | `Σq²/(Σq)²` | 挂单集中度（越大越集中） |
| `ob_qmax_ratio_{s}` | `qmax/qmean` | 最大单档相对均值 |
| `ob_qcv_{s}` | `std(q)/mean(q)` | 挂单量变异系数 |

---

## 3. L2 时序特征（滚动在**全窗口拼接**上做，跨月不截断）

| 列 | 公式 | 含义 |
|---|---|---|
| `ob_missing` | — | 该分钟订单簿缺失 = 1（见 §5） |
| `ob_ret_1m_bps` | `mid.pct_change() ×1e4` | 中间价 1 分钟收益 |
| `ob_dobi_1m` / `_5m` | `obi_total − shift(n)` | Δ不平衡（OFI 近似） |
| `ob_ddepth_bid_1m` / `_ask_1m` / `_bid_5m` | `Q_{s} − shift(n)` | Δ深度（挂单补充/消耗） |
| `ob_dwmid_1m` | Δ wmid_bps | 全带加权中价位移 |
| `ob_dspread_1m` | Δ spread | 价差变化 |
| `ob_rv_5m_bps` / `_15m_bps` | 1m 收益的滚动 std | 订单簿实现波动 |
| `ob_obi_mean_5m/15m`、`ob_obi_std_5m/15m` | 滚动均值/标准差 | 不平衡水平与波动 |
| `ob_spread_mean_5m/15m`、`ob_spread_std_5m/15m` | 同上 | 价差水平与波动 |
| `ob_impact_mean_5m/15m` | `(imp_bid+imp_ask)/2` 滚动均值 | 冲击水平 |
| `ob_obi_z_5m/15m`、`ob_spread_z_15m` | `(x−mean)/std` | 标准化 |
| `ob_vol_med_7d_bps` | `median(|ret_1m|)` 过去 7 天，`shift(1)` | 波动基准尺度（严格因果） |
| `ob_vol_med_60m_bps` | `median(|ret_1m|)` 过去 60 分钟 | 当前波动尺度 |
| `ob_vol_ratio` | `med_60m / med_7d` | 当前相对基准的波动倍数 |
| `ob_vol_bucket` | ratio 落在 `[0.8,1.0,1.25,1.6,2.0]` 的哪一段（0..5） | 波动分档（边界随波动整体平移，跨时段可比） |
| `ob_ret_vol_norm` | `ret_1m / med_7d` | 归一化收益幅度 |

> 窗口只取 **{5m, 15m}**，对齐预测周期 3/5/10/15 分钟。不用 60m 这类长窗——
> 订单簿挂单随时可撤/新增，只对短期有效，长周期趋势该用 K 线/资金面特征。
> 窗口用 `rolling(index_column=ts, period=...)`，按**日历时间**滑动；`shift(n)` 在
> reindex 之后恰好等于 n 分钟前。

---

## 4. 跨市场特征（futures × spot）

| 列 | 公式 | 含义 |
|---|---|---|
| `ob_basis_bps` | `(ob_f_mid − ob_s_mid)/ob_s_mid ×1e4` | 同刻基差（比 kline `basis` 更干净） |
| `ob_depth_ratio_f_s` | `(f_bid+f_ask)/(s_bid+s_ask)` | 合约/现货深度比 |
| `ob_spread_ratio_f_s` | `f_spread/s_spread` | 价差比 |
| `ob_lead_f_s_bps` | `ob_f_ret_1m − ob_s_ret_1m` | 领先滞后（谁先动） |

---

## 5. 振幅倍数不平衡（vision 振幅 × 订单簿不平衡）

振幅定义为**近 5 分钟这一整段的区间**（high−low），再对其做 30m 滚动均值，得到
"近期典型波动幅度"；再看该幅度的不同倍数处的买卖不平衡，且**确保在快照带
（±0.3% = 30bps）以内**。

| 列 | 公式 | 含义 |
|---|---|---|
| `amp_5m_bps` | `(近5分钟 max(high)−min(low)) / close ×1e4` | 5 分钟区间振幅 |
| `amp_mean_30m_bps` | `amp_5m_bps` 的 30m 滚动均值 | 振幅的近期水平（用于缩放） |
| `ob_amp_obi_015/035/080` | 距离 `0.15/0.35/0.8 × amp_mean_30m` 内的 `(Qb−Qa)/(Qb+Qa)` | 振幅倍数的涨跌不平衡（futures 盘口，近端） |
| `ob_amp_clip_015/035/080` | `k×amp_mean_30m > 30bps` ? 1 : 0 | 该倍数超出快照带、已被截断到 30bps |

实现要点：
- 深度在距离 `x` 处的累计量，用 L1 的 **vol8 分箱**做**线性插值**（箱内假设均匀）：
  先把 `x` 用 `ob_f_sigma_bps` 换算成 σ 倍数，再在 `v0..v4` 上插值
  （`σ` 为 null 时整行置 null，不臆造深度）。
- `thr = min(k×amp_mean_30m, 30)`；`amp_mean_30m` 为空或两侧深度都为 0 时不平衡置 null。
- 倍数取小数（0.15/0.35/0.8）：3/5 分钟涨跌只看近端订单簿。`0.8×` 实测 ~10bps，
  基本都在 30bps 带内，`ob_amp_clip_*` 仅在振幅极端时才会标 1。

---

## 6. 攻防特征（主动成交 × 当前挂单）

"攻"= 近 1 分钟主动成交（flow，`um_buy_vol/um_sell_vol`、`spot_buy_vol/spot_sell_vol`），
"守"= 当前挂单快照（stock，`ob_*_q_bid/q_ask`）。两者同 `close_time` 对齐、单位都是 BTC。
逻辑（**弱参考**，假设主动买卖力量持续）：攻方 ≫ 守方 → 可能突破、出大振幅；攻方 ≪ 守方 →
突破无望。现货/合约结论常不一致，**两侧都算、下游综合看**。

| 列 | 公式 | 含义 |
|---|---|---|
| `atk_f_imb` / `atk_s_imb` | `(主动买−主动卖)/(主动买+主动卖)` | 攻方方向不平衡 |
| `atk_f_def_gap` / `atk_s_def_gap` | `atk_imb − obi` | 攻守背离（>0：挂单偏卖但成交偏买） |
| `atk_f_absorb_buy` / `atk_s_absorb_buy` | `主动买量 / 卖盘总挂单` | 买方攻击相对卖方防守（能否破墙） |
| `atk_f_absorb_sell` / `atk_s_absorb_sell` | `主动卖量 / 买盘总挂单` | 卖方攻击相对买方防守 |

> `atk_f_*` 用合约（um 成交 vs futures 盘口），`atk_s_*` 用现货（spot 成交 vs spot 盘口）。
> 时序是当前快照 + 刚结束这一分钟的成交，不用历史快照。

---

## 7. 缺失值与缺分钟策略

- 缺分钟（源端断流 / 单边有量）**在特征层不补**：`build_ob_dataset` reindex 到
  连续分钟网格（首末时间戳之间），缺失分钟的 `ob_*` 全 null，并打 `ob_missing=1`。
- 滚动窗口按时间滑过缺口（缺口分钟不参与 agg），所以滚动特征在缺口后仍有效。
- 单边分钟（只有 bid 或只有 ask）同样产出 null + `ob_missing=1`。
- **统一去 null 放在建模前**（下游职责）：训练时 drop 掉含 `ob_missing=1` 或关键特征
  为 null 的行，或按列策略填充。特征层不做 imputation，避免静默造数。

---

## 8. 前视 / 因果约定

- 所有特征只用 `t` 及之前的信息，无前视。
- `ob_*` 在 `close_time=t` 的语义是"第 t 根 bar 收盘瞬间的盘口"。用它预测
  **t+1 及以后**的目标是因果的；**不要**用它解释同一根 bar 的收益率（同一时刻，内生）。
- 分箱 vwap/深度是 `±0.3%` 截断口径，不是全盘；跨市场比较只认 bps 分箱 + 相对量。

---

## 9. 消费注意事项

- **取数**：直接拉 `ob-dataset-vol8` 的 `ob_BTCUSDT.parquet`。185 列 =
  `close_time` + futures 90 + spot 90 + 跨市场 4；按组的列清单见
  `DATA_ACCESS.md` §3.3，不用自己跑管线。
- **列类型**：`close_time` 为 i64，`{p}mid` 为 f64；L1 截面特征一律 f32，
  L2 滚动/差分特征为 f64；`{p}missing` / `{p}vol_bucket` 为 i8。
  **按列名取值，不要按位置**。
- 非有限值（inf/NaN）已统一置 null（除零产生的 inf 曾会静默污染下游）。
- 月份文件按 `close_time` 月归属切分（月末最后一分钟归当月），逐月拼接无缺口。
- 本表**不含 K 线**：`metrics` 6 列已在 2026-11 整体移出基础表（见 DATA_ACCESS §3.1），
  `atk_*` / `ob_amp_obi_*` 要另跑 `ob_kline_cross.py`。
- 两端 `ob_f_mid` / `ob_s_mid` 是同一中间价的两次记录，差值即 `ob_basis_bps`。
