#!/usr/bin/env python3
"""从本仓库的 GitHub Release 拉取月度 parquet 到本地（下游项目的取数入口）。

用法:
    # vision 宽表：全部标的全部月份
    python tools/fetch_release.py --tag vision-base --dest ./data/vision

    # 只要 BTCUSDT
    python tools/fetch_release.py --tag vision-base --pattern 'BTCUSDT_*' --dest ./data/vision

    # 订单簿特征（185 列，close_time 主键，直接 join K 线）
    python tools/fetch_release.py --tag ob-dataset-vol8 --pattern 'ob_BTCUSDT.parquet' --dest ./data/ob

    # 订单簿原始分钟汇总（自己算盘口特征时才用），两个交易所
    python tools/fetch_release.py --tag data-merged --pattern '*minute_band_mid_*' --dest ./data/ob

    # 先看有什么，不下载
    python tools/fetch_release.py --tag ob-dataset-vol8 --list

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
    """下载单个资产；已完整则跳过，已下了一截则续传。返回 (name, 状态, 字节数)。

    GitHub release 资产支持 Range 请求（206），所以中断后重跑会从 `.part`
    的当前长度续传，而不是从头再来——大文件（月度 band 300+ MB）在慢网络上
    很容易被中断，没有续传基本下不完。服务端忽略 Range 时返回 200，此时
    从 0 重写（不能 append，否则是两份拼接的垃圾）。
    """
    name = asset["name"]
    dst = dest_dir / name
    size = int(asset["size"])
    if dst.exists() and dst.stat().st_size == size:
        return name, "skip", size

    tmp = dst.with_suffix(dst.suffix + ".part")
    start = tmp.stat().st_size if tmp.exists() else 0
    if start == size:
        # 上次已经下满、只是改名失败（Windows 上被 AV/索引占用很常见）。
        # 此时再发 `Range: bytes=<size>-` 会被判 416，每次都崩在同一资产上。
        tmp.replace(dst)
        return name, "ok", size
    if start > size:
        tmp.unlink()                 # 资产被重新上传且更小，残件已无意义
        start = 0
    headers = {"Range": f"bytes={start}-"} if start else None
    with requests.get(asset["browser_download_url"], stream=True,
                      timeout=300, headers=headers) as r:
        if start and r.status_code == 206:
            mode, base = "ab", start
        elif r.status_code in (200, 206):
            mode, base = "wb", 0
        else:
            # 不 raise：单个资产失败不该中断其余资产（main 里按 fail 计数收口）
            return name, f"HTTP{r.status_code}", start
        total = base + int(r.headers.get("Content-Length", 0))
        done, tick = base, base
        with open(tmp, mode) as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
                done += len(chunk)
                if done - tick >= 20 << 20:  # 每 20MB 打一次进度，别刷屏
                    tick = done
                    print(f"    {name}: {done / 1e6:.0f}/{size / 1e6:.0f} MB", flush=True)
    got = tmp.stat().st_size
    if got != size:
        # 保留 .part，下次续传；只有"比目标还大"才是真错
        return name, f"INCOMPLETE ({got} != {size}), rerun to resume", got
    tmp.replace(dst)
    print(f"    {name}: done ({total / 1e6:.0f} MB)")
    return name, "ok", size


def main() -> None:
    ap = argparse.ArgumentParser(description="从 bn_hisdata 的 GitHub Release 拉取 parquet")
    ap.add_argument("--tag", required=True,
                    help="release tag: vision-base | ob-dataset-vol8 | ob-features-vol8 | "
                         "data-merged | data-YYYY-MM（其余 tag 也能拉，不做白名单）")
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
