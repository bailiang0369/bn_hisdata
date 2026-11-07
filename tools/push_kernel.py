#!/usr/bin/env python3
"""本地推送 Kaggle 内核 —— kaggle-kit 的 direct 模式薄封装（保留旧参数写法）。

内部转调 `kaggle-kit/kk.py push --mode direct`，参数名一一对应：
    --code            -> --entry
    --dataset-source  -> --dataset-in

用法:
    python tools/push_kernel.py \
        --code features/kaggle/run_eth.py \
        --slug byh850515/ethusdt-orderbook-features \
        --title "ETHUSDT Orderbook Features" \
        --env MODE=build --env-from CHD_API_KEY

凭据一律用 `--env-from NAME`（从本地环境变量取值，不进命令行/日志）；
不要用 `--env CHD_API_KEY=明文`。凭据会写进私有 kernel 的 code_file 首行，
这是刻意的——Kaggle Secret 是 kernel 设置，`kernels push` 会覆盖它，一重推就丢；
写进 code_file 则重推照样有效，自动化链路才不会断。前提：kernel 必须私有。
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path


def find_kk() -> Path:
    """按 环境变量 -> 上级目录 -> 已安装 skill 的顺序找 kk.py。"""
    cands: list[Path] = []
    env = os.environ.get("KK_PY")
    if env:
        cands.append(Path(env))
    here = Path(__file__).resolve()
    for base in here.parents:
        cands.append(base / "kaggle-runner" / "scripts" / "kk.py")
        cands.append(base / "kaggle-kit" / "kk.py")   # 旧路径，留作兼容
    cands.append(Path.home() / ".codebuddy" / "skills" / "kaggle-runner" / "scripts" / "kk.py")
    for c in cands:
        if c.is_file():
            return c
    raise SystemExit("找不到 kk.py：请设置 KK_PY，或先运行 `python kk.py install-skill`")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", required=True, help="本地内核脚本路径（相对当前目录）")
    ap.add_argument("--slug", required=True, help="username/kernel-slug")
    ap.add_argument("--title", required=True)
    ap.add_argument("--env", action="append", default=[], help="K=V，可重复（非敏感值）")
    ap.add_argument("--env-from", action="append", default=[],
                    help="从本地环境变量取值注入（凭据用这个），可重复")
    ap.add_argument("--dataset-source", action="append", default=[], help="挂载的 dataset，可重复")
    ap.add_argument("--no-internet", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="只生成并打印，不推送")
    a = ap.parse_args()

    cmd = [sys.executable, str(find_kk()), "push", "--mode", "direct",
           "--entry", a.code, "--slug", a.slug, "--title", a.title]
    for item in a.env:
        cmd += ["--env", item]
    for name in a.env_from:
        cmd += ["--env-from", name]
    for ds in a.dataset_source:
        cmd += ["--dataset-in", ds]
    if a.no_internet:
        cmd.append("--no-internet")
    if a.dry_run:
        cmd.append("--dry-run")

    return subprocess.run(cmd).returncode


if __name__ == "__main__":
    raise SystemExit(main())
