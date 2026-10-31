#!/usr/bin/env python3
"""从本仓库的 GitHub Release 拉取月度 parquet 到本地（下游项目的取数入口）。

用法:
    # vision 宽表：全部标的全部月份
    python tools/fetch_release.py --tag vision-base --dest ./data/vision

    # 只要 BTCUSDT
    python tools/fetch_release.py --tag vision-base --pattern 'BTCUSDT_*' --dest ./data/vision

    # 订单簿分钟汇总（对齐最常用），两个交易所
    python tools/fetch_release.py --tag data-merged --pattern '*minute_band_mid_*' --dest ./data/ob

    # 先看有什么，不下载
    python tools/fetch_release.py --tag data-merged --list

设计:
  * 仓库 public，**不需要 token**；设了 `GH_TOKEN` 就带上，仅用于抬高 API 限额。
  * 幂等：目标文件已存在且大小与资产一致则跳过，中断后重跑即续传。
  * 先写 `.part` 再改名，避免半截文件被下游误当完整数据。
  * `--pattern` 是 shell 通配（fnmatch），匹配的是**资产名**。

Release 与命名约定见仓库根目录 `DATA_ACCESS.md`。
"""
import argparse
import fnmatch
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

REPO = "bailiang0369/bn_hisdata"
API = "https://api.github.com"
_HEADERS = {"Accept": "application/vnd.github+json"}
if os.environ.get("GH_TOKEN"):
    _HEADERS["Authorization"] = f"Bearer {os.environ['GH_TOKEN']}"


def _check(resp) -> None:
    """403 多半是匿名 API 限流（60 次/小时），给出可执行的提示而不是裸堆栈。"""
    if resp.status_code == 403 and "rate limit" in resp.text.lower():
        sys.exit("GitHub API 限流（匿名 60 次/小时）。设环境变量 GH_TOKEN 后重试，或等一会儿再跑。")
    resp.raise_for_status()


def list_assets(tag: str) -> list[dict]:
    """列出一个 release 的全部资产（分页，100/页）。"""
    r = requests.get(f"{API}/repos/{REPO}/releases/tags/{tag}", headers=_HEADERS, timeout=60)
    if r.status_code == 404:
        sys.exit(f"release tag 不存在: {tag}")
    _check(r)
    rid = r.json()["id"]

    out, page = [], 1
    while True:
        resp = requests.get(
            f"{API}/repos/{REPO}/releases/{rid}/assets",
            params={"per_page": 100, "page": page},
            headers=_HEADERS,
            timeout=60,
        )
        _check(resp)
        arr = resp.json()
        out += arr
        if len(arr) < 100:
            return out
        page += 1


def download(asset: dict, dest_dir: Path) -> tuple[str, str, int]:
    """下载单个资产；已完整则跳过。返回 (name, 状态, 字节数)。"""
    name = asset["name"]
    dst = dest_dir / name
    size = int(asset["size"])
    if dst.exists() and dst.stat().st_size == size:
        return name, "skip", size

    tmp = dst.with_suffix(dst.suffix + ".part")
    with requests.get(asset["browser_download_url"], stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
    got = tmp.stat().st_size
    if got != size:
        tmp.unlink(missing_ok=True)
        return name, f"SIZE MISMATCH ({got} != {size})", got
    tmp.replace(dst)
    return name, "ok", size


def main() -> None:
    ap = argparse.ArgumentParser(description="从 bn_hisdata 的 GitHub Release 拉取 parquet")
    ap.add_argument("--tag", required=True, help="release tag: vision-base | data-merged | data-YYYY-MM")
    ap.add_argument("--pattern", default="*", help="资产名通配符，默认全部（fnmatch 语法）")
    ap.add_argument("--dest", help="下载目录（--list 时可省略）")
    ap.add_argument("--list", action="store_true", help="只列出匹配的资产，不下载")
    ap.add_argument("--jobs", type=int, default=4, help="并发下载数，默认 4")
    args = ap.parse_args()

    assets = [a for a in list_assets(args.tag) if fnmatch.fnmatch(a["name"], args.pattern)]
    if not assets:
        sys.exit(f"没有资产匹配 {args.pattern!r}（tag={args.tag}）")

    total = sum(int(a["size"]) for a in assets)
    print(f"{args.tag}: {len(assets)} 个资产, {total / 1e6:.1f} MB")

    if args.list:
        for a in sorted(assets, key=lambda x: x["name"]):
            print(f"  {a['name']:<48} {int(a['size']) / 1e6:8.2f} MB")
        return

    if not args.dest:
        sys.exit("下载需要 --dest")

    dest_dir = Path(args.dest)
    dest_dir.mkdir(parents=True, exist_ok=True)

    ok = skip = fail = 0
    with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as ex:
        futures = [ex.submit(download, a, dest_dir) for a in assets]
        for fut in futures:
            name, status, nbytes = fut.result()
            if status == "ok":
                ok += 1
                print(f"  ok    {name} ({nbytes / 1e6:.2f} MB)")
            elif status == "skip":
                skip += 1
            else:
                fail += 1
                print(f"  FAIL  {name}: {status}")

    print(f"完成: 新下载 {ok}, 已存在跳过 {skip}, 失败 {fail} -> {dest_dir}")
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
