#!/usr/bin/env python3
"""Kaggle kernel: 从 data-merged 拉月度 band/mid，算**订单簿**特征（不含 K 线）。

产出（Release，按月可续传；tag 后缀随分箱口径，默认 vol8）：
  ob-features-vol8 : ob_features_{exchange}_{YYYY-MM}.parquet  (L1 截面，逐月中间态)
  ob-dataset-vol8  : ob_BTCUSDT_{YYYY-MM}.parquet              (L2 滚动后，逐月)
                     ob_BTCUSDT.parquet                        (最终交付：全窗口单文件)

数据源：`data-YYYY-MM` release 的日包（每天一个 zip，内含 futures/spot 的
band+mid 分钟快照）。这是唯一一手数据——`pipeline.py` 建完快照就删原始小时文件。
`data-merged` 只是把这些日包 concat 成月文件，同源可替代，因此不再依赖它。

流程：
  1. 逐月：拉当月日包并解压 -> ob_features.py 用通配直接读 30 个日 parquet 算 L1
     -> 删日包（控磁盘）。月与月之间 L1 都留在本地（rolling 要跨月）。
  2. 全月算完：build_ob_dataset.py（全序列 reindex+滚动+校验门禁）-> ob_*.parquet。
  3. 上传 L1 与 ob 数据集；已存在的资产跳过（真正增量）。

K 线不在这条链路里：基础 K 线特征由 vision / bn_bt 单独算，跨域特征
（成交 vs 挂单、振幅倍数处的不平衡）见 features/ob_kline_cross.py。

安全：本文件**不含密钥**，GH_TOKEN 走 env / Kaggle Secrets（见 _secret）。
断点续传：上传成功的月份资产下次运行直接跳过；band 下载用 HTTP Range 续传。
"""
import os
import shutil
import subprocess
import sys
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path

import requests

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "polars", "zstandard"],
               check=True)

REPO = "https://github.com/bailiang0369/bn_hisdata.git"
GH_REPO = "bailiang0369/bn_hisdata"
GH_API = "https://api.github.com"
GH_UPLOAD = "https://uploads.github.com"
DL_BASE = f"https://github.com/{GH_REPO}/releases/download"

START = os.environ.get("START", "2025-07-01")
END = os.environ.get("END", "2026-10-20")
# 标的可用环境变量覆盖（VISION_SYMBOL）；默认 BTCUSDT，保持既有行为不变。
SYMBOL = os.environ.get("VISION_SYMBOL", "BTCUSDT")
EXCHANGES = ("binance_futures", "binance_spot")
# 分箱口径。vol8 与 bps 的 L1 不可混用，所以 release tag 跟着口径走：
# 换口径 = 新 tag = 不复用旧 L1，也不会把新文件 skip 掉。
OB_BIN_MODE = os.environ.get("OB_BIN_MODE", "vol8")
L1_TAG = f"ob-features-{OB_BIN_MODE}"
DS_TAG = f"ob-dataset-{OB_BIN_MODE}"

# 仓库克隆和中间产物都放 /tmp：/kaggle/working 是内核 output，放这里会把
# 整个 repo（含 .git）当成产物打包上传，本地 pull 回来又是一堆本来就在手边的代码。
WORK = Path("/tmp/ob_build")
ROOT = WORK / "bn_hisdata"
OB = WORK / "ob"
L1 = WORK / "l1"
DS = WORK / "ob_dataset"
for p in (OB, L1, DS):
    p.mkdir(parents=True, exist_ok=True)
# 只有这个目录（parquet）进 output
OUT = Path("/kaggle/working/ob_dataset")


def _secret(name: str):
    """尝试从 env / Kaggle Secrets 取凭据；取不到返回 None（不中断）。

    这样内核没有 secret 也能跑完全部计算，产物留在 /kaggle/working（Kaggle output），
    由本地下载后用本地 token 上传——从而 `kaggle kernels push` 可一键触发，无需网页挂 secret。
    """
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
        print(f"[secret] {name}: kaggle_secrets unavailable "
              f"({type(exc).__name__}) -> 上传将跳过，产物留在 output", flush=True)
        return None
    try:
        v = _Client().get_secret(name)
    except Exception as exc:  # noqa: BLE001
        print(f"[secret] {name}: get_secret failed ({type(exc).__name__}) "
              f"-> 上传将跳过，产物留在 output", flush=True)
        return None
    if not v:
        print(f"[secret] {name}: empty -> 上传将跳过，产物留在 output", flush=True)
        return None
    print(f"[secret] {name}: from kaggle_secrets", flush=True)
    return v


GH_TOKEN = _secret("GH_TOKEN")
HDRS = {"Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github+json"} if GH_TOKEN else {}


def months() -> list[str]:
    s, e = date.fromisoformat(START), date.fromisoformat(END)
    out, y, m = [], s.year, s.month
    while (y, m) <= (e.year, e.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def days_of(m: str) -> list[str]:
    """某月的日期列表（YYYY-MM-DD），裁到 [START, END] 内。"""
    y, mo = int(m[:4]), int(m[5:])
    d, out = date(y, mo, 1), []
    while d.month == mo:
        if START <= d.isoformat() <= END:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def download(url: str, dst: Path, optional: bool = False) -> Path | None:
    """Range 续传下载；已完整则跳过。optional=True 时 404 返回 None（用于"可选复用"）。"""
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
        elif optional and r.status_code == 404:
            return None
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
    if not GH_TOKEN:
        return "skip(no-token)"
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
    if not GH_TOKEN:
        return "skip(no-token)"
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

    # 1) 逐月 L1（futures + spot），直接吃 data-YYYY-MM 的日包
    for m in months():
        done = all((L1 / f"ob_features_{ex}_{m}.parquet").exists() for ex in EXCHANGES)
        if done:
            print(f"[{m}] L1 exists, skip", flush=True)
            continue

        mdir = OB / m
        mdir.mkdir(parents=True, exist_ok=True)
        days = days_of(m)
        got, miss = 0, []
        for d in days:
            if (mdir / d / "binance_futures_minute_band.parquet").exists():
                got += 1
                continue
            # 一个 zip 同时含 futures/spot 的 band+mid，两个交易所共用
            zp = download(f"{DL_BASE}/data-{m}/{d}.zip", mdir / f"{d}.zip",
                          optional=True)
            if zp is None:
                miss.append(d)
                continue
            with zipfile.ZipFile(zp) as z:
                z.extractall(mdir / d)
            zp.unlink(missing_ok=True)
            got += 1
        print(f"[{m}] 日包 {got}/{len(days)} 天"
              + (f"，缺 {len(miss)} 天（{miss[0]}…{miss[-1]}）" if miss else ""),
              flush=True)

        for ex in EXCHANGES:
            out = L1 / f"ob_features_{ex}_{m}.parquet"
            if out.exists():
                continue
            # 优先复用 Release 上已算好的 L1（重跑时省掉重新下载 + 聚合）
            if download(f"{DL_BASE}/{L1_TAG}/{out.name}", out, optional=True) is not None:
                print(f"[{m}] reuse cached L1 {ex}", flush=True)
                continue
            print(f"[{m}] compute L1 {ex} ...", flush=True)
            run("features/ob_features.py",
                "--band", str(mdir / "*" / f"{ex}_minute_band.parquet"),
                "--mid", str(mdir / "*" / f"{ex}_minute_band_mid.parquet"),
                "--out", str(out), "--bin-mode", OB_BIN_MODE, "--force")
        # 上传本月的两份 L1（增量），本地保留供 rolling；日包已用完，删掉控磁盘
        for ex in EXCHANGES:
            print(f"[{m}] upload L1 {ex}", upload(L1_TAG,
                                                  L1 / f"ob_features_{ex}_{m}.parquet"),
                  flush=True)
        shutil.rmtree(mdir, ignore_errors=True)

    # 2) 全序列 rolling + 校验（build_ob_dataset 内部用 glob 展开，不碰 K 线）
    run("features/build_ob_dataset.py",
        "--fut", str(L1 / "ob_features_binance_futures_*.parquet"),
        "--spot", str(L1 / "ob_features_binance_spot_*.parquet"),
        "--out-dir", str(DS), "--symbol", SYMBOL)

    # 3) 上传逐月 ob 数据集（增量中间态，供单月消费/断点续传）
    for fp in sorted(DS.glob(f"ob_{SYMBOL}_2*.parquet")):
        print(f"upload {fp.name}", upload(DS_TAG, fp), flush=True)

    # 4) 全窗口单文件（build_ob_dataset 已产出）覆盖上传
    final = DS / f"ob_{SYMBOL}.parquet"
    if not final.exists():
        raise SystemExit(f"缺少全窗口文件 {final}")
    print(f"final: {final.name} {final.stat().st_size / 1e6:.1f} MB", flush=True)
    print(f"upload {final.name}", upload_replace(DS_TAG, final), flush=True)

    # 5) 只有 parquet 抄进 output（没有 token 时靠它把产物拉回本地）
    OUT.mkdir(parents=True, exist_ok=True)
    for fp in sorted(DS.glob("*.parquet")):
        shutil.copy2(fp, OUT / fp.name)
    print(f"output: {OUT} ({len(list(OUT.glob('*.parquet')))} 个 parquet)", flush=True)
    print("FEATURES DONE", flush=True)


if __name__ == "__main__":
    main()
