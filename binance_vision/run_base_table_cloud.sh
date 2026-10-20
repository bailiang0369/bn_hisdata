#!/usr/bin/env bash
# =============================================================================
# 魔搭 ModelScope Notebook：基础宽表生成
#   基础宽表 = um+spot 1m klines + basis + metrics（--no-agg，13+6 列）
#   aggTrades / orderbook 只贡献衍生特征、下载量大，留第二阶段，本脚本不碰
#   （第二阶段见同目录 run_aggtrades_cloud.sh）。
#
# 设计原则（2026-10-20 修正版）：
#   - 所有数据落在 /mnt/workspace（持久化存储，100GB 配额），不碰 /root
#     （/root 是容器本地盘：重建实例即丢，且空间有限）
#   - 代码获取默认「本地 zip 上传」，不要求 push GitHub、不需要 token；
#     git clone 只是可选举退
#   - 结果默认不 push 回任何地方，用 JupyterLab 文件面板下载取回
#
# 用法：把 bn_hisdata.zip 上传到 /mnt/workspace 后，在 Notebook Terminal 里：
#   cd /mnt/workspace && python -c "import zipfile;zipfile.ZipFile('bn_hisdata.zip').extractall()"
#   cd /mnt/workspace/bn_hisdata && bash binance_vision/run_base_table_cloud.sh
# =============================================================================
set -euo pipefail

WORK_DIR="${WORK_DIR:-/mnt/workspace}"
REPO_DIR="$WORK_DIR/bn_hisdata"

START=2025-06-01                 # 与 orderbook 窗口对齐
KLINES_MONTHLY_END=2026-09-30    # monthly 按整月取文件：10 月未完结，文件还不存在
DAILY_END=2026-10-19             # 日度部分补到昨天
JOBS=8                           # 并发数；Binance Vision 限流时调小

mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

# ---- 代码获取：上传的 zip 优先；没有 zip 才尝试 clone --------------------------
if [ ! -d "$REPO_DIR" ]; then
    if [ -f "$WORK_DIR/bn_hisdata.zip" ]; then
        python -c "import zipfile; zipfile.ZipFile('$WORK_DIR/bn_hisdata.zip').extractall('$WORK_DIR')"
    elif [ -n "${REPO_URL:-}" ]; then
        git clone "$REPO_URL" "$REPO_DIR"
    else
        echo "ERROR: $REPO_DIR 不存在。请先上传 bn_hisdata.zip 到 $WORK_DIR，"
        echo "       或 export REPO_URL=https://github.com/<user>/bn_hisdata.git 后重跑。"
        exit 1
    fi
fi
cd "$REPO_DIR"

# ---- 依赖（镜像里可能没有 requests）------------------------------------------
pip install -q -r requirements.txt

# ---- 测速：先确认国内云 -> data.binance.vision 的连通性，再开大批量 ------------
echo "== speed test: 1 daily kline file =="
time python binance_vision/download_binance_vision.py --market um --symbol BTCUSDT \
    --category klines --interval 1m --granularity daily \
    --start-date 2026-10-15 --end-date 2026-10-15 -j 1

# ---- klines 月度（整月文件，起止日期中的"日"会被忽略）-------------------------
python binance_vision/download_binance_vision.py --market both --symbol all \
    --category klines --interval 1m --granularity monthly \
    --start-date "$START" --end-date "$KLINES_MONTHLY_END" -j "$JOBS"

# ---- klines 日度（补 2026-10 未完结部分）--------------------------------------
python binance_vision/download_binance_vision.py --market both --symbol all \
    --category klines --interval 1m --granularity daily \
    --start-date 2026-10-01 --end-date "$DAILY_END" -j "$JOBS"

# ---- metrics 日度（仅 um；spot 无 metrics，会自动跳过）-------------------------
python binance_vision/download_binance_vision.py --market um --symbol all \
    --category metrics --granularity daily \
    --start-date "$START" --end-date "$DAILY_END" -j "$JOBS"

# ---- 构建基础宽表（--no-agg：aggTrades 放第二阶段）----------------------------
python binance_vision/build_master_features.py --symbol BTCUSDT --no-agg
python binance_vision/build_master_features.py --symbol ETHUSDT --no-agg

# ---- 结果取回：默认不 push ------------------------------------------------------
echo "== DONE =="
echo "结果文件（在 $REPO_DIR/binance_vision/data/ 下）："
ls -lh "$REPO_DIR"/binance_vision/data/*_base_features_1m.parquet
echo "取回方式：JupyterLab 左侧文件面板里右键 -> Download，或整目录打包后下载。"
echo "（可选）想归档到 GitHub 再自己 git add -f + commit + push，本脚本不代劳。"
