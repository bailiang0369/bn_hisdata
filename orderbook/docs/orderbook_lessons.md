# L2 订单簿数据处理：踩坑与经验总结

> 适用对象：在本仓库处理 CryptoHFTData 的 BTCUSDT L2 orderbook 数据（Binance spot / futures）。
> 记录从下载、重建、校验到批处理链路中踩过的真实坑，以及沉淀下来的可靠做法。

---

## 0. 数据长什么样

- 数据源：[CryptoHFTData](https://cryptohftdata.com) 的 BTCUSDT L2 orderbook。
- **每小时一个文件**，每个文件混合了两类事件：
  - `snapshot`：某一时刻的**全量盘口快照**（深度可达 2 万+ 档）。
  - `update`：增量深度变更（挂单 / 撤单 / 改量）。
- 每个小时文件**自带初始全量快照**，所以单小时状态可以孤立重建，不依赖上一小时。
- 文件格式分界：`2026-08-19` 起为裸 `.parquet`；之前为 `.parquet.zst`（zstd 压缩）。

本地布局：
```
orderbook/data/{date}/{exchange}/{HH}/BTCUSDT_orderbook.parquet[.zst]   # 原始（按小时分存）
orderbook/data/{date}/{exchange}_minute_band.parquet                   # 处理后（按天合并）
orderbook/data/{date}/{exchange}_minute_band_mid.parquet
orderbook/data/monthly/{exchange}_minute_band_{YYYY-MM}.parquet        # 月度聚合（特征计算用，见 §9）
orderbook/data/monthly/{exchange}_minute_band_mid_{YYYY-MM}.parquet
```

> 本仓库的订单簿相关代码都放在 `orderbook/` 目录下。从**仓库根目录**运行，例如
> `python orderbook/pipeline.py --date 2026-10-12`；数据默认落在 `orderbook/data/`。
> （与 `data.binance.vision` 的 K 线 / aggTrades / metrics 处理逻辑完全不同，后续会放在独立的模块目录，互不影响。）

---

## 1. 最核心的坑：快照是「整本书替换」，不是增量

**这是整个项目里代价最大、最隐蔽的坑。**

### 现象
跨小时一致性校验（见 §2）发现：23 个边界里有 2 个偏差巨大——我们重建的盘口比真实值**多出 2.4 万个价位**，但窄 band（±0.3%）只暴露了其中 678 个，所以问题一直藏在水下。

### 根因
Binance 的 local-depth diff 流在**连接 / 重连**时会重发一份快照。交易所语义是：**快照 = 整本书替换**（"以这份为准，之前你记的统统作废"）。
第一版代码把快照当成**增量事件**——只更新快照里出现的价位，**没有删除"旧快照里有、新快照里没有"的价位**。于是旧快照的深档位被永久残留（向前填充一直带着），直到该小时末尾。

### 真实数据证据
- 现货 `hour16`、合约 `hour07` 各含 **2 个快照**：
  - ① 整点边界的正常初始全量快照（~2.6 万档 / ~3.3 万档）。
  - ② 小时中途重连时重发的快照，但**是限深的**（现货 200 档、合约 500 档/侧）。
- 正常文件只有 1 个快照，这个 bug 不显形；一旦某小时有重连，就漏。

### 修复（已固化在 `ob_reconstruct.py`）
每次快照到达时，对"**到达前存活、且不在该快照中**"的价位，合成一条 `qty=0` 的撤单记录，再做 as-of 合并。修复后 23/23 边界全部 100% 精确匹配。

**经验：永远把快照当整书替换，永远。**

---

## 2. 正确性守门员：跨小时边界一致性校验

思路（用户提出，极其有效）：
> 小时 H 最后一分钟重建出的盘口，应该和小时 H+1 文件开头的**原始全量快照**同范围数据一致（两者只差约 51ms）。

做法要点：
- 用**精确边界时间戳**取快照（`event_time == 边界值`），不要按"取该文件所有 snapshot 去重"——否则重连的多次快照会被混读，造成虚假差异。
- 逐边界对比 band 内（±0.3%）的匹配率 / 差异量。
- 再补两个内部自洽检验：① 用小时 H 自己的快照+更新重建首分钟，和它自己的原始快照比；② 小时 H 末分钟 vs 小时 H+1 首分钟（都是我们的方法）互比。

这一招**直接抓出了 §1 的 bug**。任何重建逻辑改动后都应重跑 `validate_snapshot_consistency.py`。

---

## 3. 重建方法：向量化 as-of（非回放）

- 单 pass：`group_by(side, px, bucket).agg(按 event_time 取最后 qty)` → 与 `(side,px) × 分钟桶` 网格 left join → 按 `(side,px)` 向前填充。
- `bucket = event_time // 60000`（分钟，绝对 UTC ms 时间戳）。
- **非回放**：确定性强，和"逐分钟事件回放"结果一致，但快得多、不被状态机 bug 困扰。
- 之所以选它，正是因为用户明确不要"逐分钟回放状态机"。

---

## 4. 内存：日级单文件 OOM，按小时 / 按天独立才稳

- 重建中间态是 `(价位数) × (分钟桶数)` 的**密网格**。
  - 小时级：~7 万价位 × 60 桶 ≈ 430 万行，轻松。
  - **日级**：× 1440 桶 → 合约全深度价位又多，实测 dense 与"窗口没收紧的稀疏版"都直接 **OOM 崩溃**（分配失败）。
- 结论：**不要拼单个日级大文件**。按"每小时文件 / 每一天"独立处理——每个文件自带初始快照，状态孤立可重建，天然跨天无关。
- 稀疏 as-of + 按 mid 浮动窗口裁剪能降内存，但收益有限且实现复杂，小时级已够用，最终**未采用**作为默认。
- 唯一真正能压墙钟的并行是**跨天并行**（天与天独立），天内并行无意义（polars 已占满核）。

---

## 5. 下载相关的坑

- **两条下载路径**：
  - `download_day.py`：匿名 CryptoHFTData API，顺序、每小时 sleep 1.5s，**慢**，一次一个市场。
  - `download_orderbook_s3.py`：Cloudflare **R2 直连**，线程池 32 workers，**快**，一次下两个市场，需要环境变量 `CHD_API_KEY`。批处理 400 天务必用 R2。
- **zst 文件归一化（关键）**：`2026-08-19` 前的文件是 `.parquet.zst`。
  - 两个下载器（匿名 API 的 `download_day.py` 与 R2 直连的 `download_orderbook_s3.py`）都会在下载时**就地解压**，
    落盘统一写成无压缩的 `.parquet`（文件名与内容一致，下游无需按扩展名嗅探魔数）；
    两者都用**流式解压**（`zstandard` 的 `stream_reader` / `stream_writer`），可兼容任意 zstd 帧（不依赖帧头内嵌的 content size）。
  - 早期（含已修正前）的 `download_day.py` 曾用一次性 `ZstdDecompressor().decompress()`，对无 content size 的帧会抛 `ZstdError` 中断整天——已改为流式。
  - 不论哪种后端，build 只认 `.parquet`，`pipeline.normalize_raw` 仍按 magic 嗅探、把残留的 `.parquet.zst` 重写为 `.parquet`。否则 2025-06 ~ 2026-08 的海量历史文件全读不出来。
- 大文件（合约单小时 ~17MB）在限速出口下可能 >60s，需大超时（R2 客户端已设 1800s）。

---

## 6. 输出设计

- 原始按小时分存；处理后**按天合并**成一个 band + 一个 mid 文件（`bucket` 是绝对分钟时间戳，天然带"哪天哪小时"信息，随时可 `bucket//3600000 % 24` 切回某小时）。
- 每日文件写出前按 `bucket` 排序（band 为 `(bucket, side, px)` 全规范序，mid 为 `bucket`）：单日排序约 1s，却让月度合并免去全局 sort，且有序数据 parquet 压缩率好约 5 倍。
- band 字段：`bucket`(i64) `side`(str) `px`(**f64**) `q`(**f32**) `mid`(f64)（默认 ±0.3% 内的档位）。
  这个宽度是**有意的取舍**：面向 3/5 分钟超短线事件合约预测，只保留 mid 附近真实可成交的挂单，
  更远的档位没有意义。`--band` 可调，且 `reconstruct_state_sparse` 的预滤窗口由该 band 推导
  （而非写死 ±0.3%），保证带内档位一个都不会被提前裁掉。
- mid 字段：`bucket, best_bid, best_ask, mid, spread`（每分钟一行，全部 f64）。
- **精度取舍（重要）**：`q` 用 Float32（挂单量不是标识键，BTC 量级下相对误差 ~6e-8，
  而 band 是最大的一张表，这一列占约 1/3 体积，实测改 f32 省 32%）；`px` 与
  `best_bid`/`best_ask` **必须保持 Float64**——价位是档位标识（现货 tick 0.01 / 合约 0.1），
  价格超过 131072 后 float32 的 ULP(0.0156) 大于现货 tick，两个相邻价位会四舍五入成同一个值，
  `group_by(side, px)` 就会把不同档位合并、盘口损坏。`mid` 也保持 f64：它参与 band 边界过滤
  （`px >= mid*(1-band)`），且实测改 f32 只再省 0.3%（zstd 已把桶内重复值压掉），不值得换精度。
- 窄表体积 ≈ 原始的 **~2%（合约）/ ~8%（现货）**，已是算特征直接可用的数值表。
- 全量窄表（两市场 ~474 天）约 8–9GB，按天文件有 ~950 个、过多；一次性整体加载又太大。提供
  `merge_monthly.py` 把每日窄表按 (交易所, 月份) 进一步聚合成**月度 parquet**（见 §9），作为特征计算的折中粒度。

---

## 7. 流水线（`pipeline.py`）

设计原则（用户定调）：**一天一个完全独立的单元，处理完立即删原始、清内存，不累积。**

```
下载(小时级原始) → 归一化(zst→parquet) → 重建快照 → 删除原始 → 清内存
```

- 支持 `--date` 单天，或 `--start-date/--end-date` 日期区间循环。
- 已存在 band 输出的日期**自动跳过**（防重复，也等于轻量断点续传）。
- 下载后端可选 `--download api`（匿名慢）/ `r2`（快，需 `CHD_API_KEY`）；r2 一次下两个市场。
- 每个下载 / 构建步骤都用子进程跑，**进程退出即释放内存**；最后 `shutil.rmtree` 删原始目录。
- 实测：两市场同一天 ~196s（build 阶段），与单独跑一致。

### 400 天批处理时间账（按 (日期, 市场) 单元 ≈ 109s）
| 范围 | 单元数 | 串行墙钟 |
|---|---|---|
| 1 市场 × 400 天 | 400 | ~12 小时 |
| 2 市场 × 400 天 | 800 | ~24 小时（两市场并行跑则墙钟仍 ~12h） |

一次性跑完即可；之后新增一天只跑 ~109s，纯增量。内存始终只占"一天"的量，循环里每天释放，**不会 OOM**。

> 注：原始数据盘占用 ≈ 400 × (113 + 500)MB ≈ **245GB**；跑完后可删原始只留窄表（~8GB）。若担心以后改方法论要重跑，保留原始或保留下载能力。

---

## 8. 给后来人的经验法则

1. **永远把快照当整书替换**（§1）——这是头号坑。
2. **跨小时边界校验当守门员**（§2），任何重建逻辑改动后必跑。
3. **不要碰单个日级大文件**；按天 / 按小时处理（§4）。
4. **处理完当天立即删原始、清内存**（§7），避免磁盘与内存累积。聚合成月度后同样删每日快照（§9），避免窄表两级并存占双倍空间。
5. **zst 历史文件必须按 magic 归一化**（§5），否则读不出。
6. **特征从窄表衍生，与重建解耦**（§6）——改特征不必重跑重建，重建是一次性基础工程。
7. 单文件内 polars 已多核并行，**天内并行无意义**，要提速就**跨天并行**。

---

## 9. 月度聚合（特征计算的折中粒度）

全量窄表（两市场 ~474 天）约 8–9GB，按天文件 ~950 个、太多；一次性整体加载又太大。
`merge_monthly.py` 把每日窄表按 **(交易所, 月份)** 合并成月度 parquet，作为特征计算的折中粒度：

- 输入：`data/{date}/{exchange}_minute_band.parquet` / `..._minute_band_mid.parquet`
- 输出：`data/monthly/{exchange}_minute_band_{YYYY-MM}.parquet` / `..._mid_{YYYY-MM}.parquet`
- 月份由日期文件夹名推导（每日数据天然属于同一 UTC 日历月，不会跨月切割）；月份内按 `bucket` 排序。
- 有序性来源：每日文件已在 build 阶段按 `bucket` 排好序，日与日 bucket 区间不重叠，按日期顺序 concat 天然有序——**不再做月级全局 sort**（实测 10 天数据全局 sort 占总耗时 ~75%）；遗留的无序每日文件会被自动单独排序后并入。
- 用 polars 惰性 `concat` + `sink_parquet` 流式写出，单月也不整月入内存（对 Colab/Kaggle 的 12–16GB 友好）。

```bash
# 全量聚合（两市场）
python orderbook/merge_monthly.py --dest-root orderbook/data
# 只聚合合约 + 合并后删除每日快照
python orderbook/merge_monthly.py --dest-root orderbook/data --exchange binance_futures --delete-daily
```

### 删除链（每一步只留"当前最细粒度"的产物）
1. `pipeline.py`：下载原始小时文件 → 构建快照 → **删除原始小时目录** `data/{date}/{exchange}/`（见 `cleanup()`）。
   所以磁盘同一时刻只压着"当天 raw + 已累计的窄表"，永不需要为 280GB 原始预留空间。
2. `merge_monthly.py`：每日快照 → 月度聚合。合并成功后可加 **`--delete-daily` 删除每日快照**
   `data/{date}/{exchange}_minute_band*.parquet`，只留月度文件。这一步**默认不删**（避免聚合出错时丢失每日数据），
   作为收尾步骤显式执行，最终磁盘只留 `data/monthly/`。

> 断点续传：月度文件已存在则跳过整月；若某个月是分多段补下的，补完后再跑一次
> `merge_monthly.py --force` 重建受影响月份，否则已存在的月份会被跳过、漏掉新增的天。

---

## 10. sparse 预滤窗口：从"快照 mid ± 2%"到可证明安全的按桶窗口（2026-10-20 修复）

**Bug**：`reconstruct_state_sparse` 原来的预滤窗口由**快照 mid** 加 2% margin 推导
（`margin = max(0.02*mid, 0.05*span)`）。快照只在小时初（和重连时）出现，mid 一小时
内的真实摆动可以远超 2%（BTC 每月数次）。一旦超出，band 内的档位在 as-of **之前**
就被裁掉：受影响分钟的 band 行整段消失、mid 为 null，**且不报错**。dense 是默认
模式所以主链路没中招，`--mode sparse` 是静默丢数据的陷阱。

**修复（可证明安全 + 按桶推导）**：窗口改为 `[(1-band)*mid_lo, (1+band)*mid_hi]`，
其中每个分钟桶 b 的界来自**治理该桶的消息**——桶末状态就是桶内最后一条消息后的状态
（桶内没有更晚消息），该消息的 max touched bid 是彼时的存活档位，故
`mid(b) ∈ [tb_b, ta_b]`，band 档位必在 `[(1-band)*tb_b, (1+band)*ta_b]` 内，
再对所有桶取 min/max 得到覆盖区间。**无条件成立，无需调 margin。**

**为什么必须按桶而不是全文件 min/max**：本数据源实测存在**垃圾价位**（BTC ~84k 的
盘口里出现 560 / 33.7k / 174 万的报价）。全文件取界会被一条野值消息拖垮——真实小时
文件级窗口 `[33.7k, 101.4k]`，只滤掉 2.3% 的网格行；按桶取界（野值几乎不可能恰好
是某分钟的末条消息）窗口 `[75.6k, 85.4k]`，滤掉 34%–54%。

**真实数据验证**（2026-10-12 futures，07 = 含 2 个快照的重连文件）：

| 文件 | 网格行 dense → sparse | band/mid | 耗时 dense → sparse |
|---|---|---|---|
| hour 07（2 快照） | 4,971,927 → 3,277,957（65.9%） | 完全一致 | 27.1s → 21.6s |
| hour 13（1 快照） | 4,800,029 → 2,224,609（46.3%） | 完全一致 | 16.7s → 13.1s |

修复前 sparse 比 dense 还慢（白算一遍窗口），修复后真正省内存且更快。

**已知边界（caveat）**：若治理消息之后还有消息**删掉了顶档**，best bid 可能跌到
tb_b 之下（下一存活档位是更老的消息设置的），此时该分钟的界不再覆盖 best bid/ask，
mid 会算错。连续报价的盘口（BTCUSDT）每隔几条消息就会重触顶档，实测未出现；
dense 模式无此 caveat。回归测试 `test_sparse_never_clips_band_levels_on_big_mid_swing`
（mid 摆动 10%）与 `test_sparse_ignores_non_final_wild_prices`（野值在非末条消息）
锁定已验证的场景。

**同时重构**：`reconstruct_state` / `reconstruct_state_sparse` 共享
`_prepare_base + _grid_and_fill`，快照移除逻辑只剩一份——此前两份拷贝正是 §1 那个
bug 长期存活的原因（修复只落在其中一份上）。另外 `build_minute_snapshots.process_hour`
的 mid 表改为显式 `sort("bucket")`（full join 不保证行序，不排序则 dense/sparse 的
逐小时产物行序可能不同），局部变量 `band` 重命名为 `band_df`（原先遮蔽 float 参数，
纯属 RHS 先求值才侥幸正确）。
