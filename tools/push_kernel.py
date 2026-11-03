#!/usr/bin/env python3
"""本地推送 Kaggle 内核（仅本地使用，不进 kernel）。

Kaggle script kernel 只上传 code_file 一个文件、metadata 也不支持 env，
所以这里把 `--env K=V` 直接注入到代码首行（与 bn_bt/kaggle_launcher.py 的做法一致）。

用法:
    python tools/push_kernel.py \
        --code features/kaggle/run_eth.py \
        --slug byh850515/btcusdt-eth-orderbook-features \
        --title "ETHUSDT Orderbook Features" \
        --env MODE=probe --env OB_SYMBOL=ETHUSDT

注意：CHD_API_KEY 等凭据**不要**用 --env 传（会写进公开内核）。
请在 Kaggle 内核页 Settings -> Secrets 里 attach 同名 secret。
"""
import argparse
import json
import os
import subprocess
import tempfile


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", required=True, help="本地内核脚本路径")
    ap.add_argument("--slug", required=True, help="username/kernel-slug")
    ap.add_argument("--title", required=True)
    ap.add_argument("--env", action="append", default=[], help="K=V，可重复")
    ap.add_argument("--dataset-source", action="append", default=[])
    ap.add_argument("--no-internet", action="store_true")
    a = ap.parse_args()

    code = open(a.code, encoding="utf-8").read()
    inject = "\n".join(
        f"_os.environ[{k!r}] = {v!r}"
        for k, _, v in (e.partition("=") for e in a.env)
    )
    staged = "import os as _os\n" + (inject + "\n" if inject else "") + code

    d = tempfile.mkdtemp(prefix="kaggle_push_")
    name = "kernel_main.py"
    with open(os.path.join(d, name), "w", encoding="utf-8") as f:
        f.write(staged)
    meta = {
        "id": a.slug,
        "title": a.title,
        "code_file": name,
        "language": "python",
        "kernel_type": "script",
        "is_private": True,
        "enable_gpu": False,
        "enable_internet": not a.no_internet,
        "dataset_sources": a.dataset_source,
        "competition_sources": [],
        "kernel_sources": [],
    }
    with open(os.path.join(d, "kernel-metadata.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

    print(f"[stage] {d}")
    print(f"[push] {a.slug}")
    return subprocess.run(["kaggle", "kernels", "push", "-p", d]).returncode


if __name__ == "__main__":
    raise SystemExit(main())
