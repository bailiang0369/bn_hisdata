#!/usr/bin/env python3
"""Kaggle kernel: 下载 Binance Vision klines(1m, spot+um) + metrics(um)，构建
base 宽表（--no-agg，不含 aggTrades），按 月 拆分 parquet 并上传到 GitHub
release `vision-base`。

窗口与 orderbook 对齐：2025-07-01 .. 2026-10-20。
klines 走 月度(2025-07..2026-09) + 日度(2026-10-01..2026-10-20)；metrics 仅日度。
"""
import os, sys, subprocess
from pathlib import Path


def _secret(name: str) -> str:
    """从环境变量或 Kaggle Secrets 取凭据；绝不把密钥写进代码（仓库是 public）。

    优先 env（本地跑 / 手动注入），其次 Kaggle Secrets（Add-ons -> Secrets 里
    attach 同名 secret）。取不到就硬失败，避免悄悄用空 token 跑出 401 一堆噪音。
    """
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

REPO = "https://github.com/bailiang0369/bn_hisdata.git"
WORK = Path("/kaggle/working")
START, END = "2025-07-01", "2026-10-20"
KLINES_MONTHLY_END = "2026-09-30"
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
JOBS = "8"

os.chdir(WORK)
if not (WORK / "bn_hisdata").is_dir():
    subprocess.run(f"git clone {REPO}", shell=True, check=True)
os.chdir("bn_hisdata")
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "polars", "requests"], check=True)

BV = "binance_vision"
DATA = Path(BV) / "data"

def run(args):
    print("+ " + " ".join(args), flush=True)
    rc = subprocess.run(args, check=False).returncode
    if rc != 0:
        print(f"  !! rc={rc}", flush=True)
    return rc

sym = ",".join(SYMBOLS)

# 1) klines 月度（整月文件）
run([sys.executable, f"{BV}/download_binance_vision.py", "--market", "both",
     "--symbol", sym, "--category", "klines", "--interval", "1m",
     "--granularity", "monthly", "--start-date", START,
     "--end-date", KLINES_MONTHLY_END, "-j", JOBS])
# 2) klines 日度（补未完结的 2026-10）
run([sys.executable, f"{BV}/download_binance_vision.py", "--market", "both",
     "--symbol", sym, "--category", "klines", "--interval", "1m",
     "--granularity", "daily", "--start-date", "2026-10-01",
     "--end-date", END, "-j", JOBS])
# 3) metrics 日度（仅 um；spot 会自动跳过）
run([sys.executable, f"{BV}/download_binance_vision.py", "--market", "um",
     "--symbol", sym, "--category", "metrics", "--granularity", "daily",
     "--start-date", START, "--end-date", END, "-j", JOBS])

# 4) 构建 base 宽表（--no-agg）
for s in SYMBOLS:
    run([sys.executable, f"{BV}/build_master_features.py", "--symbol", s, "--no-agg"])

# 5) 按月拆分 + 上传 GitHub release vision-base
import requests
import polars as pl

def gh(method, url, **kw):
    h = dict(HDRS)
    h.update(kw.pop("headers", {}))
    return requests.request(method, url, headers=h, timeout=300, **kw)

def ensure_release(tag, name):
    r = gh("GET", f"{GH_API}/repos/{GH_REPO}/releases/tags/{tag}")
    if r.status_code == 200:
        return r.json()["id"]
    r = gh("POST", f"{GH_API}/repos/{GH_REPO}/releases",
           json={"tag_name": tag, "name": name, "prerelease": True})
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

rid = ensure_release("vision-base", "Binance Vision base features (monthly)")
if not rid:
    print("FATAL: cannot create vision-base release", flush=True)
    raise SystemExit(1)
existing = list_assets(rid)
OUT = WORK / "vision_monthly"
OUT.mkdir(exist_ok=True)

for s in SYMBOLS:
    fp = DATA / f"{s}_base_features_1m.parquet"
    if not fp.exists():
        print(f"MISSING base table: {fp}", flush=True)
        continue
    df = pl.read_parquet(fp)
    df = df.with_columns(
        pl.col("close_time").cast(pl.Datetime("ms")).dt.strftime("%Y-%m").alias("_ym")
    )
    print(f"{s}: {df.height} rows, {df.width - 1} cols -> months "
          f"{sorted(df['_ym'].unique().to_list())}", flush=True)
    for ym in sorted(df["_ym"].unique().to_list()):
        sub = df.filter(pl.col("_ym") == ym).drop("_ym")
        out = OUT / f"{s}_base_features_{ym}.parquet"
        sub.write_parquet(out)
        name = out.name
        if name in existing:
            print("  skip existing", name, flush=True)
            continue
        with open(out, "rb") as fh:
            r = gh("POST", f"{GH_UPLOAD}/repos/{GH_REPO}/releases/{rid}/assets?name={name}",
                   headers={"Content-Type": "application/octet-stream"}, data=fh)
        print(f"  uploaded {name} ({out.stat().st_size / 1e6:.2f} MB) -> {r.status_code}", flush=True)
        if r.status_code in (200, 201):
            existing[name] = None

print("VISION DONE", flush=True)
