#!/usr/bin/env python3
"""推送 make_dataset_eth.py，并把 kaggle.json 的凭据以环境变量注入（不经过命令行）。

用法：
    python tools/push_make_dataset.py                 # 推送并运行
    python tools/push_make_dataset.py --dry-run       # 只看生成的 code_file
    DS_ID=bailiang0369/xxx python tools/push_make_dataset.py   # 自定义目标 dataset
"""
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    cfg_path = Path.home() / ".kaggle" / "kaggle.json"
    if not cfg_path.exists():
        print("找不到 ~/.kaggle/kaggle.json，无法注入 kaggle 凭据", file=sys.stderr)
        return 2
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    env = dict(os.environ, KAGGLE_USERNAME=cfg["username"], KAGGLE_KEY=cfg["key"])

    cmd = [
        sys.executable, str(ROOT / "tools" / "push_kernel.py"),
        "--code", "features/kaggle/make_dataset_eth.py",
        "--slug", os.environ.get("SLUG", "byh850515/make-dataset-from-kernel-output"),
        "--title", "Make Dataset From Kernel Output",
        "--env-from", "KAGGLE_USERNAME",
        "--env-from", "KAGGLE_KEY",
        "--env", f"SRC_KERNEL={os.environ.get('SRC_KERNEL', 'byh850515/ethusdt-orderbook-features')}",
        "--env", f"DS_ID={os.environ.get('DS_ID', 'byh850515/ethusdt-merged-features')}",
    ]
    cmd += sys.argv[1:]
    return subprocess.run(cmd, env=env, cwd=str(ROOT)).returncode


if __name__ == "__main__":
    raise SystemExit(main())
