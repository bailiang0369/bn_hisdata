#!/usr/bin/env bash
# =============================================================================
# 魔搭 ModelScope Notebook：第二阶段 —— aggTrades + 完整 master 宽表
#   在 run_base_table_cloud.sh（基础宽表，klines+basis+metrics）之后跑。
#
#   流程：下载 um+spot 两市场日度 aggTrades -> 按天预聚合特征（LEAN=1 时）
#         -> 删除原始逐笔 parquet -> 重建 BTCUSDT/ETHUSDT 完整 master 表。
#
# 磁盘账（/mnt/workspace 100GB 配额）：aggTrades 清洗后 ~25MB/天/市场/符号，
# 全窗口 x 2 符号 x 2 市场可达 ~100GB 量级，与 orderbook 窄表叠加会爆配额；
# 预聚合后特征文件 ~KB/天（1440 行 x 13 列），总占用 ~百 MB 量级。
#   LEAN=1（默认）预聚合后删原始：省磁盘，但以后改特征要重下（Binance
#     Vision 数据随时可重下，见下面注释里的命令）。
#   LEAN=0 保留原始：rebuild 时 shift(1) 跨文件全局、日界精确，代价是磁盘。
#
# 环境变量：START / END / JOBS / LEAN / WORK_DIR 均可覆盖。
# =============================================================================
set -euo pipefail

WORK_DIR="${WORK_DIR:-/mnt/workspace}"
REPO_DIR="$WORK_DIR/bn_hisdata"
START="${START:-2025-06-01}"     # 与 orderbook / klines 窗口对齐
END="${END:-2026-10-19}"         # 日度 aggTrades 补到昨天
JOBS="${JOBS:-8}"                # 下载并发；Binance Vision 限流时调小
LEAN="${LEAN:-1}"

cd "$REPO_DIR"
pip install -q -r requirements.txt

# ---- 下载 um+spot 两市场日度 aggTrades（BTCUSDT + ETHUSDT）----------------
echo "== stage 2.1: download um+spot aggTrades (daily) =="
python binance_vision/download_binance_vision.py --market both --symbol all \
    --category aggTrades --interval 1m --granularity daily \
    --start-date "$START" --end-date "$END" -j "$JOBS"

if [ "$LEAN" = "1" ]; then
    # ---- 按天预聚合特征（4 个市场x符号目录并行；单目录内部 polars 已多核）--
    echo "== stage 2.2: pre-aggregate daily aggTrades -> features/*_feat_1m.parquet =="
    pids=()
    for mkt in um spot; do
        for sym in BTCUSDT ETHUSDT; do
            d="binance_vision/data/$mkt/daily/aggTrades/$sym"
            [ -d "$d" ] || continue
            python binance_vision/build_aggtrade_features.py --input "$d" --timeframe 1m &
            pids+=($!)
        done
    done
    fail=0
    for p in "${pids[@]}"; do
        wait "$p" || fail=1
    done
    if [ "$fail" != "0" ]; then
        echo "ERROR: pre-aggregation failed; raw aggTrades NOT deleted."
        exit 1
    fi

    # ---- 删原始逐笔 parquet（特征文件 *_feat_* 保留；随时可重下）-----------
    echo "== stage 2.3: delete raw aggTrades parquet (keep *_feat_*) =="
    find binance_vision/data -path "*/aggTrades/*" -name "*.parquet" \
        ! -name "*_feat_*" -delete
    echo "（以后想改特征：重跑 stage 2.1 下载命令即可，Binance Vision 数据不删库。）"
fi

# ---- 重建完整 master 表（缺 um aggTrades 会硬失败；缺 spot 只警告）----------
echo "== stage 2.4: rebuild full master tables =="
python binance_vision/build_master_features.py --symbol BTCUSDT
python binance_vision/build_master_features.py --symbol ETHUSDT

echo "== DONE =="
ls -lh "$REPO_DIR"/binance_vision/data/*_master_features_1m.parquet
echo "取回方式：JupyterLab 左侧文件面板右键 -> Download。"
echo "LEAN=$LEAN；改特征或改窗口请相应调整 START/END 后重跑。"
