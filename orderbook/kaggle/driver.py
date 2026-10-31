#!/usr/bin/env python3
"""Kaggle orderbook 全量重建的驱动脚本（用 kaggle Python SDK，绕过有鉴权 bug 的 CLI）。

用法（在本机执行，需先设置环境变量 KAGGLE_USERNAME / KAGGLE_KEY）：
  python orderbook/kaggle/driver.py launch        # 创建数据集 + 推送内核（触发首次运行）
  python orderbook/kaggle/driver.py status        # 查询内核运行状态
  python orderbook/kaggle/driver.py loop          # 自动续跑：推内核 -> 轮询 -> 完成或超时后重投，直到 _COMPLETE

设计：
- 进度持久化在 Kaggle Dataset byh850515/orderbook-data（内核每次跑完上传每日产物）。
- 内核 run_orderbook.py 在整段区间两市场都建成后写 _COMPLETE 标记；loop 看到标记即停。
- 单次内核封顶 ~9h，超时/被杀后本次新产出的天随工作目录消失，但数据集里已有之前各轮
  进度，重投即续跑（代价是最多重做一轮，正确性不受影响）。

注意：本文件与 run_orderbook.py **都不含密钥**。运行前需在本机设置环境变量
`KAGGLE_USERNAME` / `KAGGLE_KEY`；内核侧需要的 `CHD_API_KEY` / `GH_TOKEN` 走
Kaggle Secrets（Add-ons -> Secrets），由 `run_orderbook.py` 的 `_secret()` 读取。
仓库是 public，任何密钥都不要写回代码。
"""
import os
import sys
import time

from kaggle.api.kaggle_api_extended import KaggleApi

KERNEL = "byh850515/orderbook-runner"
DATASET = "byh850515/orderbook-data"
USER = "byh850515"
KERNEL_FOLDER = os.path.join(os.path.dirname(__file__))
DATASET_FOLDER = os.path.join(KERNEL_FOLDER, "..", "kaggle_dataset")
DATASET_FOLDER = os.path.abspath(DATASET_FOLDER)

TERMINAL = {"complete", "error", "cancelled", "timeout"}


def get_api() -> KaggleApi:
    api = KaggleApi()
    api.authenticate()
    return api


def launch(api: KaggleApi):
    # 数据集（若已存在则忽略错误）
    try:
        r = api.dataset_create_new(DATASET_FOLDER, convert_to_csv=False)
        print("dataset create:", getattr(r, "status", None), getattr(r, "error", None))
    except Exception as e:  # noqa: BLE001 - 已存在等情况忽略
        print("dataset create skipped:", type(e).__name__, str(e)[:160])
    # 推送内核（默认触发运行）
    r = api.kernels_push(KERNEL_FOLDER)
    print("kernel push:", getattr(r, "status", None), getattr(r, "error", None))


def status(api: KaggleApi):
    try:
        s = api.kernels_status(KERNEL)
    except Exception as e:  # noqa: BLE001
        print("status err:", type(e).__name__, str(e)[:200])
        return None
    print("kernel status:", getattr(s, "status", None),
          "| failure:", getattr(s, "failure_message", None))
    return getattr(s, "status", None)


def is_complete(api: KaggleApi) -> bool:
    """各月分片数据集（或旧单数据集）里是否存在 _COMPLETE 标记。"""
    slugs = [DATASET]
    y, m = 2025, 7
    while (y, m) <= (2026, 10):
        slugs.append(f"{USER}/orderbook-{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    for s in slugs:
        try:
            files = api.dataset_list_files(s)
            if any((f.get("name") == "_COMPLETE") for f in files):
                return True
        except Exception:
            continue
    return False


def loop(api: KaggleApi, poll_sec: int = 300, max_runs: int = 12):
    """自动续跑直到全部完成或达到 max_runs 次重投。"""
    for run_idx in range(1, max_runs + 1):
        if is_complete(api):
            print(f"[run {run_idx}] dataset already complete, stopping.")
            return
        print(f"\n===== launch run {run_idx} =====")
        launch(api)
        # 轮询直到终态
        deadline = time.time() + 9.5 * 3600
        while time.time() < deadline:
            st = status(api)
            if st in TERMINAL:
                print(f"[run {run_idx}] ended with status: {st}")
                break
            time.sleep(poll_sec)
        else:
            print(f"[run {run_idx}] poll deadline reached (likely still running); "
                  f"will re-push next loop iteration.")
        if is_complete(api):
            print(f"[run {run_idx}] _COMPLETE detected, all done.")
            return
        print(f"[run {run_idx}] not complete yet, re-pushing after short pause...")
        time.sleep(60)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "launch"
    if not (os.environ.get("KAGGLE_API_TOKEN")
            or (os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"))):
        print("ERROR: 请设置 KAGGLE_API_TOKEN，或 KAGGLE_USERNAME + KAGGLE_KEY")
        sys.exit(2)
    a = get_api()
    if mode == "launch":
        launch(a)
    elif mode == "status":
        status(a)
    elif mode == "loop":
        loop(a)
    else:
        print("unknown mode:", mode)
