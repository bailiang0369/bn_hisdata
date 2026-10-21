#!/usr/bin/env bash
# =============================================================================
# 全链路一键：binance_vision + orderbook 的完整下载与处理。
#
# 为什么需要它 / 该在哪跑：
#   魔搭 DSW 空闲约 1 小时就回收实例，过夜没人盯着不行；Jules 之类的一次性
#   云 VM 也不行——任务结束 VM 即销毁，产出只能走 git 分支带回，10GB+ 的
#   parquet 不可能这么搬。正确做法：一台**按量付费、不因闲置被回收**的小
#   机器（AutoDL / 智星云 / ECS 等，系统盘 40GB+ 足够），tmux 挂着过夜：
#
#     git clone https://github.com/bailiang0369/bn_hisdata.git /mnt/workspace/bn_hisdata
#     cd /mnt/workspace/bn_hisdata
#     tmux new -s data
#     CHD_API_KEY=xxx bash run_all_cloud.sh 2>&1 | tee run_all.log
#
#   每一步都可断点续传（下载器按输出文件跳过、pipeline 按天跳过、merge 按月
#   跳过），中断/被杀后重跑同一条命令即可，不会重下。
#
# 步骤：
#   1. binance_vision 阶段一：um+spot 1m klines（月度整月 + 当月日度）+
#      um metrics（日度）-> 基础宽表 {symbol}_base_features_1m.parquet
#   2. binance_vision 阶段二：um+spot aggTrades（日度）-> 按天预聚合特征
#      （LEAN=1 删原始）-> 完整 master 宽表 {symbol}_master_features_1m.parquet
#   3. orderbook：R2 下载两市场小时文件 -> 分钟快照 -> 删原始（按天循环，
#      单日失败不中断，退出码非 0 时收尾仍会跑月度聚合）
#   4. orderbook：月度聚合 merge_monthly
#
# 环境变量：
#   WORK_DIR  代码与数据根（默认 /mnt/workspace）
#   START/END 数据窗口（默认 2025-06-01 ~ 昨天）
#   JOBS      下载并发（默认 8；Binance Vision 限流时调小）
#   LEAN      1=aggTrades 预聚合后删原始（默认，省 ~100GB）；0=保留原始
#   CHD_API_KEY  orderbook R2 下载必需
#   SKIP_VISION=1    只跑 orderbook
#   SKIP_ORDERBOOK=1 只跑 binance_vision
#   MODE       orderbook 重建模式：dense（默认，零 caveat 参照）或 sparse
#             （省 8 倍内存、快 ~15%，任一分钟窗口不可证明时自动回退 dense，
#              见 orderbook/docs/orderbook_lessons.md §12；小内存 VM 推荐 sparse）
#
# 磁盘：最终产物 ~10GB（orderbook 窄表 ~9GB + 特征/宽表）；原始按天删，
#       峰值只压"当天原始 + 已累计窄表"。
# 时长参考（2025-06-01 起 ~490 天）：vision 1-3h；orderbook 两市场串行 ~17h
# （弱机下限；现代 4+ vCPU 约 6-10h。实测与调优记录见
#  orderbook/docs/orderbook_lessons.md §7 / §11）。
# =============================================================================
set -euo pipefail

WORK_DIR="${WORK_DIR:-/mnt/workspace}"
REPO_DIR="$WORK_DIR/bn_hisdata"
START="${START:-2025-06-01}"
END="${END:-$(date -u -d '1 day ago' +%F)}"
JOBS="${JOBS:-8}"
LEAN="${LEAN:-1}"
MODE="${MODE:-dense}"
ob_rc=0

if [ ! -d "$REPO_DIR" ]; then
    echo "ERROR: $REPO_DIR 不存在。先准备代码："
    echo "  git clone https://github.com/bailiang0369/bn_hisdata.git \"$REPO_DIR\""
    exit 1
fi
cd "$REPO_DIR"

echo "== 环境 =="
df -h . | tail -1
echo "window: $START .. $END | jobs=$JOBS | lean=$LEAN | repo: $REPO_DIR"

# ---- 1+2. binance_vision ----------------------------------------------------
if [ "${SKIP_VISION:-0}" = "1" ]; then
    echo "=== [1/4] SKIP_VISION=1：跳过 binance_vision ==="
else
    echo "=== [1/4] vision 阶段一：klines + metrics + 基础宽表 ==="
    START="$START" KLINES_MONTHLY_END="${KLINES_MONTHLY_END:-}" DAILY_END="$END" \
        JOBS="$JOBS" bash binance_vision/run_base_table_cloud.sh

    echo "=== [2/4] vision 阶段二：aggTrades + master 宽表 ==="
    START="$START" END="$END" JOBS="$JOBS" LEAN="$LEAN" \
        bash binance_vision/run_aggtrades_cloud.sh
fi

# ---- 3. orderbook：下载 + 分钟快照（按天，原始即删）--------------------------
if [ "${SKIP_ORDERBOOK:-0}" = "1" ]; then
    echo "=== [3/4] SKIP_ORDERBOOK=1：跳过 orderbook ==="
else
    if [ -z "${CHD_API_KEY:-}" ]; then
        echo "ERROR: orderbook R2 下载需要 CHD_API_KEY。"
        echo "  export CHD_API_KEY=xxx 后重跑；或 SKIP_ORDERBOOK=1 只跑 vision。"
        exit 1
    fi
    echo "=== [3/4] orderbook pipeline（r2，两市场，$START .. $END，mode=$MODE）==="
    # pipeline 对单日失败只记录不中断（退出码非 0）；这里不让它中断收尾，
    # 失败的天重跑本命令即断点续传。
    set +e
    python orderbook/pipeline.py --start-date "$START" --end-date "$END" \
        --download r2 --band 0.003 --mode "$MODE"
    ob_rc=$?
    set -e
    echo "orderbook pipeline exit=$ob_rc（非 0 = 有天失败，重跑续传）"
fi

# ---- 4. orderbook：月度聚合（默认保留每日文件；确认无误后可加 --delete-daily）
if [ "${SKIP_ORDERBOOK:-0}" != "1" ]; then
    echo "=== [4/4] orderbook merge_monthly ==="
    python orderbook/merge_monthly.py --dest-root orderbook/data
fi

echo "== DONE（$(date -u '+%F %T') UTC）=="
echo "vision 宽表：    $REPO_DIR/binance_vision/data/*_features_1m.parquet"
ls -lh "$REPO_DIR"/binance_vision/data/*_features_1m.parquet 2>/dev/null || true
echo "orderbook 月表： $REPO_DIR/orderbook/data/monthly/"
du -sh "$REPO_DIR"/orderbook/data/monthly 2>/dev/null || true
echo "取回：SCP / rclone / JupyterLab 文件面板均可；data/ 已 gitignore，别 commit。"
if [ "$ob_rc" != "0" ]; then
    echo "NOTE: orderbook 有天失败（exit=$ob_rc），重跑同一命令补齐。"
fi
exit "$ob_rc"
