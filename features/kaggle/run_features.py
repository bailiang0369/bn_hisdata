#!/usr/bin/env python3
"""Kaggle kernel: 从 data-merged / vision-base 拉月度文件，算订单簿特征并合并。

产出（Release，按月可续传）：
  ob-features     : ob_features_{exchange}_{YYYY-MM}.parquet   (L1 截面，逐月中间态)
  merged-features : merged_BTCUSDT_{YYYY-MM}.parquet            (逐月中间态)
                    BTCUSDT.parquet                             (最终交付：全窗口单文件)

流程：
  1. 拉全部 vision-base（BTCUSDT，54MB），小。
  2. 逐月：拉 futures/spot 的 band+mid（~655MB/月） -> ob_features.py 算 L1
     -> 删 band+mid（控磁盘）。月与月之间 L1 都留在本地（rolling 要跨月）。
  3. 全月算完：build_dataset.py（全序列 reindex+滚动+合并 vision+校验门禁）
     -> merged_BTCUSDT_{YYYY-MM}.parquet。
  4. 上传 ob-features / merged-features；已存在的资产跳过（真正增量）。

安全：本文件**不含密钥**，GH_TOKEN 走 env / Kaggle Secrets（见 _secret）。
断点续传：上传成功的月份资产下次运行直接跳过；band 下载用 HTTP Range 续传。
"""
import os
import re
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

import requests

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "polars", "zstandard"],
               check=True)

import polars as pl  # noqa: E402 - 必须在 pip install 之后 import

REPO = "https://github.com/bailiang0369/bn_hisdata.git"
GH_REPO = "bailiang0369/bn_hisdata"
GH_API = "https://api.github.com"
GH_UPLOAD = "https://uploads.github.com"
DL_BASE = f"https://github.com/{GH_REPO}/releases/download"

START, END = "2025-07-01", "2026-10-20"
SYMBOL = "BTCUSDT"
EXCHANGES = ("binance_futures", "binance_spot")

WORK = Path("/kaggle/working")
ROOT = WORK / "bn_hisdata"
OB = WORK / "ob"
VIS = WORK / "vision"
L1 = WORK / "l1"
MERGED = WORK / "merged"
for p in (OB, VIS, L1, MERGED):
    p.mkdir(parents=True, exist_ok=True)


def _secret(name: str) -> str:
    v = os.environ.get(name)
    if v:
        print(f"[secret] {name}: from env", flush=True)
        return v
    try:
        try:
            from kaggle_secrets import UserSecretsClient as _Client
        except ImportError:
            from kaggle_secrets import UserSecretClient as _Client
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(
            f"missing secret: {name} (cannot import kaggle_secrets client: "
            f"{type(exc).__name__}: {exc})"
        )
    try:
        client = _Client()
        v = client.get_secret(name)
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(
            f"missing secret: {name} (get_secret raised "
            f"{type(exc).__name__}: {exc})"
        )
    if not v:
        # 诊断：尽量列出客户端能看到的 secret 名，帮定位 label 不匹配 / 未 attach
        avail = None
        for attr in ("get_secrets", "secrets", "list_secrets"):
            fn = getattr(client, attr, None)
            if fn is None:
                continue
            try:
                avail = fn() if callable(fn) else fn
                break
            except Exception:  # noqa: BLE001
                continue
        raise SystemExit(
            f"missing secret: {name} (empty value; available={avail})"
        )
    print(f"[secret] {name}: from kaggle_secrets", flush=True)
    return v


GH_TOKEN = _secret("GH_TOKEN")
HDRS = {"Authorization": f"Bearer {GH_TOKEN}", "Accept": "application/vnd.github+json"}


def months() -> list[str]:
    s, e = date.fromisoformat(START), date.fromisoformat(END)
    out, y, m = [], s.year, s.month
    while (y, m) <= (e.year, e.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def download(url: str, dst: Path) -> Path:
    """Range 续传下载；已完整则跳过。"""
    size = None
    if dst.exists() and dst.suffix == ".parquet":
        return dst  # 已完整（下游约定：parquet 存在即完整）
    tmp = dst.with_suffix(dst.suffix + ".part")
    start = tmp.stat().st_size if tmp.exists() else 0
    h = {"Range": f"bytes={start}-"} if start else {}
    with requests.get(url, stream=True, timeout=300, headers=h) as r:
        if start and r.status_code == 206:
            mode = "ab"
        elif r.status_code in (200, 206):
            mode = "wb"
        else:
            r.raise_for_status()
        with open(tmp, mode) as f:
            for c in r.iter_content(1 << 20):
                f.write(c)
    tmp.replace(dst)
    return dst


def gh(method, url, **kw):
    h = dict(HDRS)
    h.update(kw.pop("headers", {}))
    return requests.request(method, url, headers=h, timeout=300, **kw)


def get_release_id(tag):
    r = gh("GET", f"{GH_API}/repos/{GH_REPO}/releases/tags/{tag}")
    return r.json()["id"] if r.status_code == 200 else None


def ensure_release(tag):
    rid = get_release_id(tag)
    if rid:
        return rid
    r = gh("POST", f"{GH_API}/repos/{GH_REPO}/releases",
           json={"tag_name": tag, "name": tag, "prerelease": True})
    return r.json()["id"] if r.status_code in (200, 201) else None


def list_assets(rid):
    out, page = {}, 1
    while True:
        r = gh("GET", f"{GH_API}/repos/{GH_REPO}/releases/{rid}/assets"
                      f"?per_page=100&page={page}")
        if r.status_code != 200:
            break
        arr = r.json()
        for a in arr:
            out[a["name"]] = a["id"]
        if len(arr) < 100:
            break
        page += 1
    return out


def upload(release_tag: str, fp: Path):
    rid = ensure_release(release_tag)
    if not rid:
        raise SystemExit(f"cannot create release {release_tag}")
    existing = list_assets(rid)
    if fp.name in existing:
        return "skip"
    with open(fp, "rb") as fh:
        r = gh("POST", f"{GH_UPLOAD}/repos/{GH_REPO}/releases/{rid}/assets"
                       f"?name={fp.name}",
               headers={"Content-Type": "application/octet-stream"}, data=fh)
    return f"http{r.status_code}" if r.status_code not in (200, 201) else "ok"


def upload_replace(release_tag: str, fp: Path):
    """覆盖式上传：最终交付的单文件每次都要重新生成（新增月份后内容会变），
    所以先删旧资产再传，不能像月度文件那样 skip。"""
    rid = ensure_release(release_tag)
    if not rid:
        raise SystemExit(f"cannot create release {release_tag}")
    existing = list_assets(rid)
    if fp.name in existing:
        gh("DELETE", f"{GH_API}/repos/{GH_REPO}/releases/assets/{existing[fp.name]}")
    with open(fp, "rb") as fh:
        r = gh("POST", f"{GH_UPLOAD}/repos/{GH_REPO}/releases/{rid}/assets"
                       f"?name={fp.name}",
               headers={"Content-Type": "application/octet-stream"}, data=fh)
    return f"http{r.status_code}" if r.status_code not in (200, 201) else "ok"


def run(*args):
    subprocess.run([sys.executable, *args], check=True, cwd=str(ROOT))


def main():
    if not os.path.isdir(ROOT / ".git"):
        subprocess.run(["git", "clone", REPO, str(ROOT)], check=True)

    # 1) vision-base：全部月份（54MB，小）
    for m in months():
        n = f"{SYMBOL}_base_features_{m}.parquet"
        download(f"{DL_BASE}/vision-base/{n}", VIS / n)

    # 2) 逐月 L1（futures + spot），算完删 band+mid
    for m in months():
        done = all((L1 / f"ob_features_{ex}_{m}.parquet").exists() for ex in EXCHANGES)
        if done:
            print(f"[{m}] L1 exists, skip", flush=True)
            continue
        for ex in EXCHANGES:
            band = OB / f"{ex}_minute_band_{m}.parquet"
            mid = OB / f"{ex}_minute_band_mid_{m}.parquet"
            out = L1 / f"ob_features_{ex}_{m}.parquet"
            if out.exists():
                continue
            print(f"[{m}] download {ex} band ...", flush=True)
            download(f"{DL_BASE}/data-merged/{band.name}", band)
            download(f"{DL_BASE}/data-merged/{mid.name}", mid)
            print(f"[{m}] compute L1 {ex} ...", flush=True)
            run("features/ob_features.py", "--band", str(band), "--mid", str(mid),
                "--out", str(out), "--force")
            band.unlink(missing_ok=True)
            mid.unlink(missing_ok=True)
        # 上传本月的两份 L1（增量），本地保留供 rolling
        for ex in EXCHANGES:
            print(f"[{m}] upload L1 {ex}", upload("ob-features",
                                                  L1 / f"ob_features_{ex}_{m}.parquet"),
                  flush=True)

    # 3) 全序列 rolling + 合并 + 校验（build_dataset 内部用 glob 展开月度文件）
    run("features/build_dataset.py",
        "--vision", str(VIS / "BTCUSDT_base_features_*.parquet"),
        "--fut", str(L1 / "ob_features_binance_futures_*.parquet"),
        "--spot", str(L1 / "ob_features_binance_spot_*.parquet"),
        "--out-dir", str(MERGED))

    # 4) 上传逐月 merged（增量中间态，供单月消费/断点续传）
    for fp in sorted(MERGED.glob("merged_*.parquet")):
        print(f"upload {fp.name}", upload("merged-features", fp), flush=True)

    # 5) 合并成单个 BTCUSDT.parquet（最终交付）并覆盖上传
    final = WORK / "BTCUSDT.parquet"
    merged_files = sorted(MERGED.glob("merged_*.parquet"))
    if not merged_files:
        raise SystemExit("no merged_*.parquet to concat")
    print(f"concat {len(merged_files)} merged months -> {final.name} ...", flush=True)
    df = (
        pl.concat([pl.scan_parquet(f) for f in merged_files], how="vertical_relaxed")
        .unique(subset=["close_time"], keep="last")
        .sort("close_time")
        .collect()
    )
    df.write_parquet(final, compression="zstd")
    print(f"final: {final.name} {df.height:,} rows x {df.width} cols "
          f"{final.stat().st_size / 1e6:.1f} MB", flush=True)
    print(f"upload {final.name}", upload_replace("merged-features", final), flush=True)
    print("FEATURES DONE", flush=True)


if __name__ == "__main__":
    main()
