#!/usr/bin/env python3
"""Kaggle kernel: 跑 `binance_vision/pipeline_daily.py`（按年切分的 15 列 K 线基础表）。

只做 klines：um + spot 各 7 列 = 15 列，不碰 aggTrades / metrics / 订单簿。

环境变量（推内核时用 --env 传，都有默认值）：
    MODE=backfill|update   默认 backfill
    START=2020-01-01       backfill 起点
    END=                   backfill 终点，默认 UTC 昨天
    SYMBOLS=BTCUSDT,ETHUSDT
    JOBS=16

为什么放 Kaggle：回填 2020 至今要下 84 个月度 zip（约 250MB），本地跨境网络
单次请求几十秒到几分钟，只能靠机房带宽。日常增量很小，交给 GitHub Actions。

凭据：Kaggle Secret `GH_TOKEN`（或环境变量），用于覆盖发布 release asset。
kk 默认用 --env-from 注入私有 kernel，优先走那条路。
"""
import os
import subprocess
import sys
from pathlib import Path

REPO = "https://github.com/bailiang0369/bn_hisdata.git"
CLONE_DIR = Path("/tmp/bn_hisdata")     # 不写 /kaggle/working，避免进产物


def _secret(name: str) -> str:
    v = os.environ.get(name)
    if v:
        return v
    try:
        from kaggle_secrets import UserSecretsClient
        v = UserSecretsClient().get_secret(name)
        if v:
            return v
    except Exception:  # noqa: BLE001 - 非 Kaggle 环境 / 未 attach 该 secret
        pass
    raise SystemExit(f"missing secret: {name} (attach Kaggle Secret or set env)")


def _ensure_repo() -> Path:
    """clone 模式（kk 已 clone 好）直接用；direct 模式自己 clone 到 /tmp。"""
    here = Path.cwd()
    if (here / "binance_vision" / "pipeline_daily.py").is_file():
        return here
    if not (CLONE_DIR / "binance_vision" / "pipeline_daily.py").is_file():
        subprocess.run(["git", "clone", "--depth", "1", REPO, str(CLONE_DIR)], check=True)
    return CLONE_DIR


def main() -> int:
    os.environ["GH_TOKEN"] = _secret("GH_TOKEN")
    repo = _ensure_repo()
    os.chdir(repo)
    print(f"repo: {repo}", flush=True)

    try:
        import polars  # noqa: F401
        import requests  # noqa: F401
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q",
                        "polars", "requests"], check=True)

    mode = os.environ.get("MODE", "backfill")
    cmd = [sys.executable, "binance_vision/pipeline_daily.py", mode,
           "--symbols", os.environ.get("SYMBOLS", "BTCUSDT,ETHUSDT"),
           "--jobs", os.environ.get("JOBS", "16")]
    if mode == "backfill":
        cmd += ["--start", os.environ.get("START", "2020-01-01")]
        if os.environ.get("END"):
            cmd += ["--end", os.environ["END"]]

    print("+ " + " ".join(cmd), flush=True)
    return subprocess.run(cmd).returncode


if __name__ == "__main__":
    raise SystemExit(main())
