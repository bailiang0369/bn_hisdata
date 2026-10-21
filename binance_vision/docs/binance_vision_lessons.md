# binance_vision 数据处理：踩坑与经验总结

> 适用对象：在本仓库处理 Binance Vision 官方历史数据（`https://data.binance.vision`）的
> klines / aggTrades / metrics，产出 BTCUSDT / ETHUSDT 的分钟级宽表。与 orderbook 模块
> （CryptoHFTData L2）数据源和处理逻辑完全不同，两者只在最后的特征宽表层面对齐
> （join key 见 §3）。
>
> 本地布局（默认 `<module>/data`，已 gitignore）：
> ```
> data/{market}/{granularity}/{category}/{symbol}/{interval?}/{symbol}-{type}-{period}.{parquet|csv}
> ```

---

## 0. 数据源事实（2026-10 实测，写代码前先知道这些）

| category | spot | um（合约） | 粒度 |
|---|---|---|---|
| klines | ✅ | ✅ | 日度 + 月度（月度按整月取文件，起止“日”被忽略） |
| metrics | ❌ 没有 | ✅ | **仅日度**（monthly 自动跳过） |
| aggTrades | ✅ | ✅ | **仅日度**（月度文件不存在） |
| fundingRate | — | 有但**故意不下** | basis 用两市场 klines 收盘价算，不需要它 |

- `--category all` = klines + metrics，**不含 aggTrades**，必须显式 `--category aggTrades`。
- 每个 `.zip` 都有 `.CHECKSUM`（sha256）兄弟文件，默认校验；校验是安全网不是硬门槛
  （取不到校验文件只警告，真不匹配才丢弃重试）。

## 1. aggTrades 有两种真实格式，下载器按文件自适应

同一个 category，两个市场的 CSV 布局不一样（2026-10 实测，写死任何一种都会炸）：

| | um（合约） | spot（现货） |
|---|---|---|
| 列数 | **7 列**（没有 `is_best_match`） | **8 列** |
| 表头 | **有** | **无** |
| `transact_time` | 毫秒 | **微秒**（~1.79e15） |
| 布尔值 | `true/false` | `True/False` 大写 |

下载器的处理（`download_binance_vision.py` 的 `process()`）：按首 token 探测表头；
无表头时按**实际字段数**截取规范列名（写死 8 个名字会在 7 列文件上报
`unable to find column "is_best_match"`）；`transact_time > 1e14` 视为微秒并 `//1000`
归一化到毫秒——**不归一化则跨市场单位不一致，下游按 ms 解释会全部错位**。schema_overrides
按实际存在的列过滤，输出 dtype 再用显式 cast 钉死（不依赖 polars 各版本的 override 行为）。

## 2. klines：主动买/卖拆分的全精度陷阱

输出保留 `open_time, close_time, OHLC, count, buy_vol, sell_vol`，丢弃
`volume / quote_volume / taker_buy_quote_volume / ignore`：

- `buy_vol = taker_buy_volume`（主动买），`sell_vol = volume - taker_buy_volume`（主动卖）。
- **`sell_vol` 的减法必须在 cast 成 Float32 之前按 Float64 算**（`volume` /
  `taker_buy_volume` 在解析期就按 Float64 override）。f32 减两个大而接近的数，绝对误差
  可达 ~1e-4——在成交清淡的分钟，`sell_vol` 本身很小，这就是巨大的**相对**误差，恰好污染
  这套数据要研究的不平衡特征。回归测试：
  `test_process_klines_sell_vol_full_precision`。
- `close_time` 在清洗时 **+1ms**，等于下一根 K 线的 `open_time`——它是 bar 的**结束瞬间**，
  也是与 orderbook 分钟快照对齐的 join key（见 §3）。
- 默认 Float32：BTC 价位下 ULP≈0.008、相对误差 ~4e-8，对特征无影响且体积减半；
  `--dtype float64` 可退出。时间戳保持 Int64（ms epoch ~1.7e12，int32 会溢出）。

## 3. aggTrades：逐笔 → 分钟特征的聚合

aggTrades 比 klines 细一个量级（BTCUSDT um 单日 ~100 万笔），**不能逐笔进宽表**，
按 1m 窗口聚合成一行再 join（`build_aggtrade_features.py`）：

- `group_by_dynamic("timestamp", every="1m", label="right")`——窗口**右边界**恰好等于
  K 线 `close_time`，天然对齐，left join 即可。
- `log_return` / `dt_ms` 用全局 `shift(1)` 后在窗口内聚合：每个窗口的第一笔引用上一窗口
  的最后一笔（边界近似，1m 尺度可忽略）；**按天预聚合时这个近似变成逐日的**（每天第一个
  窗口首笔的 return/dt 为 null），见 §5 的 LEAN 路径。

### 特征清单：只保留 K 线给不了的（2026-10-20 与用户确认后精简）

保留三类共 11 列（`FEATURE_COLS`）：

| 类 | 特征 | 为什么 K 线给不了 |
|---|---|---|
| 分钟内价格分布 | `vwap`, `vwap_disparity`, `price_median`, `price_mad`, `price_robust_zscore` | K 线只有 OHLC 四个价格点，不知道分钟内的典型成交价在哪、收盘价是否处在极端 |
| 真实已实现波动及方向分解 | `rv`, `rv_up`, `rv_down`, `semi_var_ratio` | K 线只能用 `(high-low)/close` 或上下影线近似，把震荡和单边混在一起 |
| 成交到达时间结构 | `dt_mean`, `dt_std` | K 线的 `count` 只给笔数，给不了“均匀铺开 vs 几秒爆发后死寂” |

**主动砍掉的两类**（教训：不要把这些当“免费的都有益”）：

- 与 K 线冗余：`trade_count`（== klines `count`）、主动买卖量（== `taker_buy_volume`
  及其补数）、`volume`/`turnover`、OHLC——再算一遍是重复劳动；
- 大单统计（`qty_*`、`sweep_depth`）：**交易所普遍支持智能拆单 / Iceberg，单笔大小
  不再反映意图**，这类特征在拆单环境下基本无意义。

## 4. 宽表组装：无泄漏 join、双市场前缀、硬失败

`build_master_features.py` 把 um klines + spot klines + basis + 两市场 aggTrades 特征 +
um metrics 组成 master 宽表（完整表 42 列；无 metrics 时 36 列）：

- **`close_time` 是 bar 结束瞬间**（`open_time + interval - 1`，清洗时 +1ms == 下一根
  `open_time`）。它是无泄漏 join key：特征行 `close_time == c` 只在瞬间 c 可知。
  与 orderbook 的对应：orderbook 分钟桶 `b` ↔ master 行 `close_time == b + 60000`。
- **两个市场的 aggTrades 特征都消费**，分别加 `um_agg_` / `spot_agg_` 前缀防列名冲突；
  另有跨市场特征 `rv_dominance = (rv_um - rv_spot)/(rv_um + rv_spot + 1e-8)`
  （>0 波动由合约端主导：杠杆盘/爆仓/资金费压力；<0 源在现货）。
- **metrics 的 `create_time` 先 +5m 再做 asof join**——metrics 快照只对严格晚于它的 bar
  可见，防前视。且 metrics 的 `create_time` 在原始 CSV 里是日期时间**字符串**
  （`"2025-06-01 00:00:00"`）不是 epoch，直接 cast(Int64) 会炸。
- **um 缺 aggTrades → 硬失败**（除非 `--no-agg`）；**spot 缺只警告**（um 是主市场，
  表在缺 `spot_agg_*` 和 `rv_dominance` 时仍可用）。硬失败的存在是因为真出过事：
  2026-10-20 生成的表**静默缺失全部微观结构列**（vwap_disparity / price_robust_zscore /
  rv / qty_* / trade_count / dt_* / semi_var_ratio）而没有任何警告——docstring 宣称包含
  它们，实际没有。`--no-agg` 是唯一逃生门，输出独立的
  `{symbol}_base_features_1m.parquet` 并 loud WARNING 列全缺席的列。
- 全 lazy 管道（scan_parquet → joins → sink_parquet），多年的月度文件也不物化帧。

## 5. 断点续传、失败语义与磁盘lean路径

- **resume 以清洗输出文件存在为准**（不是 zip）：处理完 zip 即删，所以残留 zip 只代表
  “上次中断”，重跑安全。切换 `--format` 会重新下载。
- 下载和输出都走 `.part` 临时文件 + 原子改名；**每条失败路径**（网络异常、校验和不匹配、
  坏 zip）都清 `.part`残件——中断不能留下“看起来已完成”的文件。
- 坏 zip / 列数不符：强制重新下载一次再试，仍败才记 failed。
- worker 异常全部捕获、计数、进程以非零退出——自动化运行（如 Jules）不会把“有文件没下到”
  看成成功。
- **LEAN 路径（磁盘吃紧时）**：aggTrades 清洗后按天预聚合为
  `aggTrades/{symbol}/features/*_feat_1m.parquet`（~1440 行 × 13 列，KB 级/天），
  然后删原始；`build_master_features` 自动优先使用特征文件（`_scan_agg_files`），
  同一市场的原始文件被忽略（否则同一分钟双重 join 会把行数翻倍）。
  代价：边界近似变成逐日的（§3）。`run_aggtrades_cloud.sh` 默认 `LEAN=1`。

## 6. 运维：磁盘账与机器选型

先纠正一个常见误解：**“这个项目要 245GB”是把累计下载量和驻留量混了**。

| 项 | 体积 | 性质 |
|---|---|---|
| orderbook 原始小时文件（~490 天累计） | ~245GB | **按天删**，峰值只压当天 ~600MB |
| orderbook band 窄表（最终产物） | ~9–11GB | 要保留 |
| aggTrades 清洗后（全窗口峰值） | ~30–50GB（按 ~1M 笔/天 × 4 市场-符号组合估算） | LEAN=1 预聚合后删，只留 ~百 MB 特征 |
| klines / metrics / 宽表 | ~1–2GB | 要保留 |
| **跑一次全窗口的峰值** | **~45GB（估算）** | |
| **最终要保存的** | **~10–13GB** | |

- 100GB 配额的机器（魔搭 /mnt/workspace、Kaggle、Jules VM）都能装下最终产物；
  峰值 ~45GB 在 100GB 盘上也够，但想更宽裕可把 aggTrades 下载改成**按月分块**
  （下载当月 → 预聚合 → 删原始 → 下个月），峰值降到 ~15GB——**已识别未实现**，记录备查。
- **机器选型的硬约束**：作业要跑 10 小时以上（orderbook 两市场串行 ~17h 弱机下限，
  现代 4+ vCPU 约 6–10h；vision 下载清洗 1–3h），所以必须满足：① 不因闲置被回收
  （魔搭 DSW 空闲约 1 小时收实例，过夜没人盯着不行）；② 磁盘 ≥ 峰值；③ 产物能取出。
  Jules 类一次性 VM 的产出只能走 git 分支带回，10GB+ parquet 不可能这么搬——若其 VM
  真能跨会话保留状态，则本项目全部脚本可断点续传，多会话推进也行（2026-10-21 用户在验证）。
- 一键入口 `run_all_cloud.sh`（仓库根）：vision 阶段一（klines+metrics+基础宽表）→
  阶段二（aggTrades+master 宽表）→ orderbook pipeline → merge_monthly。每步可断点续传，
  中断后重跑同一条命令；`SKIP_VISION=1` / `SKIP_ORDERBOOK=1` 只跑一半；
  `START/END/JOBS/LEAN` 环境变量可覆盖。tmux + `tee run_all.log` 挂着过夜即可。

## 7. 测试

`binance_vision/` 下 25 个单测（`python -m pytest binance_vision -q`），锁定的关键行为：

- 两种 aggTrades 真实格式（um 7 列有表头毫秒 / spot 8 列无表头微秒）+ 无表头回退
- klines `sell_vol` 全精度减法（§2 的回归网）
- aggTrades 特征值（vwap/disparity/zscore/rv/semi_var_ratio/dt 的精确数值断言）、
  `label="right"` 窗口对齐、单笔窗口的边界 null
- 宽表：双市场前缀列、`rv_dominance` 数值、仅 um、预聚合特征文件路径、
  `_scan_agg_files` 的特征文件优先、`--no-agg` 路径的列缺席
- 下载器：URL/路径构造、校验和、`.part` 残件清理、进程失败语义

改动任何处理逻辑后跑全量：`python -m pytest binance_vision orderbook -q`（共 34 个）。
