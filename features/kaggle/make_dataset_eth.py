"""Kaggle kernel: 把另一个 kernel 的 output 注册成 Dataset（容器内中转，不占本地带宽）。

背景：Kaggle 新版 Output 页已去掉 “Create Dataset” 按钮，只剩 Download。
430MB 的东西没必要绕本地一圈——容器内 `kaggle kernels output` 走内网，几秒到几十秒。

环境变量：
  SRC_KERNEL   源 kernel，默认 byh850515/ethusdt-orderbook-features
  FILE_PATTERN 只下载匹配的文件（正则），默认 ETHUSDT\\.parquet
  DS_ID        目标 dataset id（前缀必须是 Kaggle 用户名），默认 byh850515/ethusdt-merged-features
  DS_TITLE     dataset 标题
  FORCE_CREATE 1=即使已存在也走 create（默认自动判 version）

凭据：KAGGLE_USERNAME / KAGGLE_KEY 由本地推送时注入（私有 kernel）。
"""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

SRC_KERNEL = os.environ.get("SRC_KERNEL", "byh850515/ethusdt-orderbook-features")
FILE_PATTERN = os.environ.get("FILE_PATTERN", r"ETHUSDT\.parquet")
DS_ID = os.environ.get("DS_ID", "byh850515/ethusdt-merged-features")
DS_TITLE = os.environ.get("DS_TITLE", "ETHUSDT Orderbook x Vision Merged Features")
WORK = Path("/kaggle/working/ds")
SRC = Path("/tmp/src")


def log(msg: str) -> None:
    print(f"[mkds] {msg}", flush=True)


def sh(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    log("+ " + " ".join(cmd))
    return subprocess.run(cmd, check=check, text=True, encoding="utf-8", errors="replace")


def kaggle_cli() -> str:
    """注意：不能用 `python -m kaggle`——镜像自带 2.0.2 没有 __main__.py。"""
    for cand in (shutil.which("kaggle"),
                 str(Path.home() / ".local" / "bin" / "kaggle"),
                 "/usr/local/bin/kaggle"):
        if cand and Path(cand).exists():
            return cand
    raise SystemExit("[mkds] 找不到 kaggle CLI，pip install kaggle 未生效")


def main() -> int:
    if not (os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")):
        log("缺少 KAGGLE_USERNAME / KAGGLE_KEY（需要 kaggle API 凭证才能下载与发布）")
        return 3

    sh([sys.executable, "-m", "pip", "install", "-q", "--upgrade", "kaggle"])
    kg = kaggle_cli()
    log(f"kaggle CLI: {kg}")

    WORK.mkdir(parents=True, exist_ok=True)
    SRC.mkdir(parents=True, exist_ok=True)
    log(f"从 {SRC_KERNEL} 拉产物（内网），过滤 /{FILE_PATTERN}/")
    sh([kg, "kernels", "output", SRC_KERNEL,
        "-p", str(SRC), "--file-pattern", FILE_PATTERN])

    picked = [p for p in SRC.rglob("*") if p.is_file() and p.suffix == ".parquet"]
    if not picked:
        log(f"没下到 parquet，/tmp/src 内容：{[str(p) for p in SRC.rglob('*')][:20]}")
        return 4
    for p in picked:
        dst = WORK / p.name
        dst.write_bytes(p.read_bytes())
        log(f"copy {p.name} {dst.stat().st_size / 1e6:.1f} MB")

    meta = {"title": DS_TITLE[:50], "id": DS_ID, "licenses": [{"name": "CC0-1.0"}]}
    (WORK / "dataset-metadata.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    rc = subprocess.run([kg, "datasets", "create", "-p", str(WORK)],
                        text=True, capture_output=True).returncode
    if rc != 0:
        log("create 失败（多半已存在），改用 version")
        sh([kg, "datasets", "version", "-p", str(WORK),
            "-m", f"refresh from {SRC_KERNEL}"])
    log(f"DONE https://www.kaggle.com/datasets/{DS_ID}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
