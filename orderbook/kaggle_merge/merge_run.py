#!/usr/bin/env python3
"""Kaggle kernel: 从 GitHub Releases 拉取所有每日 zip，按 (交易所, 月份) 合并为 parquet，
再上传到 release data-merged。逐月处理 + 用完即删，磁盘/内存占用始终很小。

产物（release data-merged 下）：
  binance_futures_minute_band_{YYYY-MM}.parquet
  binance_futures_minute_band_mid_{YYYY-MM}.parquet
  binance_spot_minute_band_{YYYY-MM}.parquet
  binance_spot_minute_band_mid_{YYYY-MM}.parquet
"""
import os, sys, re, io, shutil, subprocess, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

subprocess.run([sys.executable, "-m", "pip", "install", "-q", "polars"], check=True)
import requests
import polars as pl


def _secret(name: str) -> str:
    """从环境变量或 Kaggle Secrets 取凭据；绝不把密钥写进代码（仓库是 public）。"""
    v = os.environ.get(name)
    if v:
        return v
    try:
        from kaggle_secrets import UserSecretClient
        v = UserSecretClient().get_secret(name)
        if v:
            return v
    except Exception:  # noqa: BLE001 - 非 Kaggle 环境 / 未 attach 该 secret
        pass
    raise SystemExit(f"missing secret: {name} (set env var or attach Kaggle Secret)")


GH_REPO = "bailiang0369/bn_hisdata"
GH_TOKEN = _secret("GH_TOKEN")
GH_API = "https://api.github.com"
GH_UPLOAD = "https://uploads.github.com"
HDRS = {"Authorization": f"Bearer {GH_TOKEN}", "Accept": "application/vnd.github+json"}

START, END = "2025-07-01", "2026-10-20"
WORK = Path("/kaggle/working")
DATA = WORK / "data"
OUT = WORK / "monthly_out"
DATA.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)
EXCHANGES = ["binance_futures", "binance_spot"]

def gh(method, url, **kw):
    h = dict(HDRS)
    h.update(kw.pop("headers", {}))
    return requests.request(method, url, headers=h, timeout=300, **kw)

def get_release_id(tag):
    r = gh("GET", f"{GH_API}/repos/{GH_REPO}/releases/tags/{tag}")
    return r.json()["id"] if r.status_code == 200 else None

def ensure_release(tag, name=None):
    rid = get_release_id(tag)
    if rid:
        return rid
    r = gh("POST", f"{GH_API}/repos/{GH_REPO}/releases",
           json={"tag_name": tag, "name": name or tag, "prerelease": True})
    return r.json()["id"] if r.status_code in (200, 201) else None

def list_assets(rid):
    out, page = {}, 1
    while True:
        r = gh("GET", f"{GH_API}/repos/{GH_REPO}/releases/{rid}/assets?per_page=100&page={page}")
        if r.status_code != 200:
            break
        arr = r.json()
        for a in arr:
            out[a["name"]] = a["id"]
        if len(arr) < 100:
            break
        page += 1
    return out

def download_one(item):
    d, aid = item
    dst = DATA / d
    if ((dst / "binance_futures_minute_band.parquet").exists()
            and (dst / "binance_spot_minute_band.parquet").exists()):
        return d, "skip"
    r = requests.get(f"{GH_API}/repos/{GH_REPO}/releases/assets/{aid}",
                     headers={"Authorization": f"Bearer {GH_TOKEN}",
                              "Accept": "application/octet-stream"}, timeout=300)
    if r.status_code != 200:
        return d, f"http{r.status_code}"
    dst.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(dst)
    return d, "ok"

def merge_month(ch):
    outs = []
    for ex in EXCHANGES:
        for suffix, out_suffix in (("_minute_band.parquet", "_minute_band"),
                                   ("_minute_band_mid.parquet", "_minute_band_mid")):
            files = sorted(DATA.glob(f"{ch}-*/{ex}{suffix}"))
            if not files:
                continue
            out = OUT / f"{ex}{out_suffix}_{ch}.parquet"
            lf = pl.concat([pl.scan_parquet(f) for f in files], how="vertical_relaxed")
            lf.sink_parquet(str(out))
            print(f"  merged {ex}{out_suffix} {ch}: {len(files)} days -> "
                  f"{out.name} ({out.stat().st_size / 1e6:.1f} MB)", flush=True)
            outs.append(out)
    return outs

def main():
    s, e = date.fromisoformat(START), date.fromisoformat(END)
    months, y, m = [], s.year, s.month
    while (y, m) <= (e.year, e.month):
        months.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1

    mrid = ensure_release("data-merged", "Orderbook merged monthly parquet")
    if not mrid:
        print("FATAL: cannot create data-merged release", flush=True)
        return
    existing = list_assets(mrid)
    print(f"data-merged existing: {len(existing)} | months: {len(months)}", flush=True)

    for ch in months:
        rid = get_release_id(f"data-{ch}")
        if not rid:
            print(f"[{ch}] no data-{ch} release, skip", flush=True)
            continue
        assets = {n: i for n, i in list_assets(rid).items()
                  if re.match(r"\d{4}-\d{2}-\d{2}\.zip$", n)}
        items = [(n[:-4], i) for n, i in sorted(assets.items())]
        print(f"[{ch}] downloading {len(items)} daily zips ...", flush=True)
        with ThreadPoolExecutor(max_workers=8) as pool:
            futs = [pool.submit(download_one, it) for it in items]
            for fut in as_completed(futs):
                d, st = fut.result()
                if st not in ("ok", "skip"):
                    print(f"   {d}: {st}", flush=True)

        outs = merge_month(ch)
        for fp in outs:
            if fp.name in existing:
                print("  skip existing", fp.name, flush=True)
                continue
            if fp.stat().st_size > 2 * 1024 ** 3:
                print("  TOO BIG (>2GB):", fp.name, flush=True)
                continue
            with open(fp, "rb") as fh:
                r = gh("POST",
                       f"{GH_UPLOAD}/repos/{GH_REPO}/releases/{mrid}/assets?name={fp.name}",
                       headers={"Content-Type": "application/octet-stream"}, data=fh)
            print("  uploaded", fp.name, r.status_code, flush=True)
            if r.status_code in (200, 201):
                existing[fp.name] = None

        # 清理本月本地数据与产物，控制磁盘
        for dd in DATA.glob(f"{ch}-*"):
            shutil.rmtree(dd, ignore_errors=True)
        for fp in outs:
            fp.unlink(missing_ok=True)

    print("MERGE DONE", flush=True)

if __name__ == "__main__":
    main()
