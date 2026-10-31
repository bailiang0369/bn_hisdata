# 在 Kaggle 上运行 orderbook 处理（归档到 GitHub Release）与 Kaggle Dataset 备选方案

> **现状（2026-10-31 更新）**：实际落地的归档通道是 **GitHub Release**，不是 Kaggle
> Dataset。对应脚本：
>
> | 步骤 | 脚本 | 产物 |
> |---|---|---|
> | 逐天重建 + 上传每日 zip | `orderbook/kaggle/run_orderbook.py`（内核 `byh850515/orderbook-runner`） | Release `data-{YYYY-MM}`，资产 `{YYYY-MM-DD}.zip` |
> | 拉回每日 zip + 月度合并 + 上传 | `orderbook/kaggle_merge/merge_run.py`（内核 `byh850515/orderbook-merge`） | Release `data-merged`，资产 `{exchange}_minute_band[_mid]_{YYYY-MM}.parquet` |
> | 本机推送/轮询内核 | `orderbook/kaggle/driver.py` | —— |
>
> 三个脚本都**不含密钥**：`CHD_API_KEY` / `GH_TOKEN` 走环境变量或 Kaggle Secrets
> （`_secret()` 读取），`KAGGLE_USERNAME` / `KAGGLE_KEY` 由 driver 从本机环境变量取。
> 选择原因、断点续传机制与产物清单见根目录 [`DATA_ACCESS.md`](../../DATA_ACCESS.md) §7。
>
> 下游消费数据只需拉 Release，**不需要**跑本页任何流程。
>
> 下面保留 **Kaggle Dataset 方案**作为备选（Dataset 不支持增量、且需额外令牌，故未采用），
> 以及当时设计的发布前校验门禁（仍未实现）。

---

## 0. 前提与凭据

| 凭据 | 用途 | 怎么来 |
|---|---|---|
| Kaggle API 令牌（`KGAT_...`） | 上传 / 下载 Kaggle Dataset | Kaggle 账号 → Settings → Create New Token，得到 `kaggle.json`（`{"username": "<你的用户名>", "key": "KGAT_..."}`）。**注意它同时需要你的 Kaggle 用户名**，不只这把 key。 |
| `CHD_API_KEY` | CryptoHFTData 的 R2 直连下载（快、两市场并发） | 数据源方提供；缺失则只能用匿名 `api` 下载（慢、串行）。 |
| （不需要）GitHub 令牌 | —— | 本次决定**不**走 GitHub Release，故不需要。 |

- 数据源是 CryptoHFTData 的 R2，**不是 Kaggle**。Kaggle 只提供算力 + 数据集存储。
- 仓库里 `**/data/` 已被 `.gitignore` 排除——产物**绝不**进 git，归档只走 Kaggle Dataset。
- Kaggle Dataset 单文件上限 2GB、数据集总上限 20GB；月度 parquet 约数百 MB/交易所/月，
  全量窄表 ~9GB（两市场、约 490 天），在 20GB 内装得下。

---

## 1. 为什么是 Kaggle Dataset（而不是 git / Release）

- `data/` 被 gitignore，commit 不进去。
- 月度文件体量中等（数百 MB），Dataset 的 20GB 上限充裕；且 Dataset 本就是为"数据集
  分发"设计的，取回用 `kaggle datasets download` 一条命令。
- 你已选 Dataset 路线，故下文"发布"= 给 Dataset 上传一个新版本。

---

## 2. Kaggle Notebook 运行步骤（手动版）

1. **新建 Notebook**：在 Kaggle 新建一个 Notebook，**务必打开侧栏的 "Internet" 开关**
   （默认关闭，关着 `git clone` / `pip` 全失败）。
2. **拉代码 + 装依赖**（Notebook cell）：
   ```bash
   !git clone https://github.com/bailiang0369/bn_hisdata.git
   %cd bn_hisdata
   !pip install -q polars zstandard "kaggle" requests
   ```
3. **注入凭据**（Notebook cell，用 Secrets 或手写环境变量，**别写进代码文件**）：
   ```python
   import os
   os.environ["CHD_API_KEY"] = "xxxx"          # R2 下载源
   # Kaggle 令牌写到默认位置，CLI 才能用：
   os.makedirs("/root/.kaggle", exist_ok=True)
   with open("/root/.kaggle/kaggle.json", "w") as f:
       f.write('{"username": "<你的用户名>", "key": "KGAT_..."}')
   os.chmod("/root/.kaggle/kaggle.json", 0o600)  # 权限不对 CLI 会报错
   ```
4. **跑 pipeline**（按天断点续传，已完成的天会自动跳过）：
   ```bash
   python orderbook/pipeline.py --start-date 2025-06-01 --end-date 2026-10-20 \
       --download r2 --mode dense
   ```
   - 单市场 / 单天：`--exchange binance_futures` 或 `--date 2026-10-12`。
   - 小内存 VM 用 `--mode sparse`（省 8 倍内存、快 ~15%，窗口不可证明时自动回退 dense）。
5. **月度合并 + 删每日省空间**：
   ```bash
   python orderbook/merge_monthly.py --dest-root orderbook/data --delete-daily
   ```
   合并后只剩 `orderbook/data/monthly/{exchange}_minute_band_{YYYY-MM}.parquet` 与
   `_minute_band_mid_{YYYY-MM}.parquet`，总量 ~9GB。

---

## 3. 断点续传 / 跨会话持久化（关键）

Kaggle Notebook 工作目录会话结束后**清空**，且单会话上限约 9h。两层对策：

1. **pipeline 天然可续**：`_all_built()` 跳过已完成天，`merge_monthly` 幂等（月份文件
   已存在则 skip）。9h 跑不完就分多次，每次接着跑。
2. **用 Kaggle Dataset 持久化 `data/monthly/`**：
   - 首次：`kaggle datasets create -p orderbook/data/monthly -m "orderbook monthly parquet"`
     （先在 Kaggle 网页建好 Dataset 占位，或让 create 新建）。
   - 每次会话结束前，把新产出的月度文件传为新版本：
     ```bash
     kaggle datasets version -p orderbook/data/monthly -m "orderbook monthly up to 2026-10"
     ```
   - 下次会话开头先拉回再继续：
     ```bash
     kaggle datasets download -d <你的用户名>/<dataset名> -p orderbook/data/monthly --unzip
     ```

> 空间纪律：合并后**必须** `--delete-daily`，只留月度文件。否则每日窄表 + 月度冗余会
> 很快顶到 20GB 上限。

---

## 4. 发布前自动校验门禁（设计，待实现）

决定：每月上传 Dataset **之前**，必须满足 `only_ours == 0 且 only_truth == 0`（无残留
档位、无漏档），否则中止该月上传并报告失败边界。

**为什么只用这两个计数**：它们是重建正确性的硬信号。跨文件一致性校验
（`validate_snapshot_consistency.py`）还会输出 `diff` / `match%`，但这些受两数据源间
≤51ms 的订单扰动影响，属正常噪声，**不应**阻断发布；只有 `only_ours`/`only_truth`
（凭空多出来的档位 / 该有却缺的档位）才是真 bug。

**关键约束（实现时必须处理）**：`validate_snapshot_consistency.py` 要读**下一小时的原
始快照**来做对账，而 `pipeline.py` 的 `cleanup()` 在 build 之后就把原始小时目录删了。
因此校验**必须在删原始之前**做——两种做法：

- 方案 A（推荐）：在 `pipeline.py` 的 `process_day()` 里，build 之后、cleanup 之前，对
  当天跑一次校验，把 `only_ours/only_truth` 汇总；非零则当天标红、不删原始以便排查，并
  计入失败列表（与现有单日失败机制复用）。
- 方案 B：pipeline 加一个 `--keep-raw` 开关，校验阶段单独跑完再手动清。

**当前缺口**：`validate_snapshot_consistency.py` 现在只打印每边界的匹配率，没有可被门禁
消费的 pass/fail 返回值 / `--json`。实现时需给它加一个汇总退出码或 JSON 输出（返回
`max(only_ours, only_truth)` 作为退出码即可，`0`=通过）。

---

## 5. 上传到 Kaggle Dataset（步骤，待实现脚本）

门禁通过后：

```bash
# 确认本月 parquet 齐全且门禁通过
ls orderbook/data/monthly/*_2026-10_*.parquet
# 上传为新版本（Dataset 名在首次 create 时确定）
kaggle datasets version -p orderbook/data/monthly -m "orderbook monthly 2026-10"
```

本地 / 别处取回：

```bash
kaggle datasets download -d <你的用户名>/<dataset名> -p ./orderbook/data/monthly --unzip
```

---

## 6. 坑清单

- Notebook 默认**无 Internet**，不手动开开关则 `git`/`pip` 全挂。
- `kaggle.json` 权限必须 `600`，否则 CLI 报 "Permission denied"。
- 会话结束工作目录清空；**不落 Dataset 的结果会丢**——每次结束前记得 `datasets version`。
- `CHD_API_KEY` 缺失时 `--download r2` 会失败；用 Secrets/环境变量注入，别写进代码。
- 某月数据不全（缺天）**不要**上传，等补齐再发；月度文件缺一天会让该月 bucket 不连续。
- 月度文件名带交易所前缀（`binance_futures_*` / `binance_spot_*`），两市场各自独立上传。

---

## 7. 待实现

仍然有效（与归档通道无关，属重建正确性）：

1. `pipeline.py` 接入校验门禁（build 后、cleanup 前，汇总 `only_ours/only_truth`，非零
   则当天失败并保留原始）。
2. `validate_snapshot_consistency.py` 增加 `--json` / 退出码，供门禁消费。

已废弃（改走 GitHub Release 后不再需要）：

3. ~~`publish_kaggle.py`~~ —— 上传逻辑已在 `kaggle/run_orderbook.py`（每日 zip）与
   `kaggle_merge/merge_run.py`（月度 parquet）里实现，且是增量的。
4. ~~`run_all_cloud.sh --publish`~~ —— 同上，由两个内核脚本承担。
