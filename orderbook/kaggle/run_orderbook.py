#!/usr/bin/env python3
"""Kaggle kernel: 在 Kaggle 上跑 orderbook 全量重建，进度持久化到 GitHub Release 以支持
断点续传 / 超时重投。

- 每次运行处理从 START 到 END 的全部日期，已构建的天自动跳过（本地已有 band 产物）。
- 每天构建完立即把 orderbook/data/{date}/ 打包成 {date}.zip 上传为对应月 Release
  （data-YYYY-MM）的资产：传过的绝不重传（真正增量，规避 Kaggle Dataset 的全量重传）。
- 启动时列出各月 Release 资产，补齐本地缺失的天。
- 注意： kernel 工作目录在运行结束/被杀后清空，唯一持久化通道是 GitHub Release。

安全：本文件**不含任何密钥**。需要的三个凭据（`KAGGLE_USERNAME` / `KAGGLE_KEY` /
`CHD_API_KEY` / `GH_TOKEN`）从环境变量或 Kaggle Secrets 读取，见 `_secret()`。
仓库是 public，任何密钥都不要写回代码。
"""
import os, sys, subprocess, json, shutil, re, time, io, zipfile
from datetime import date, timedelta
from pathlib import Path
import requests


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


# ---- 凭据：env / Kaggle Secrets，见 _secret() ----
_KG_USER = _secret("KAGGLE_USERNAME")
_KG_KEY = _secret("KAGGLE_KEY")
os.makedirs("/root/.kaggle", exist_ok=True)
with open("/root/.kaggle/kaggle.json", "w") as f:
    json.dump({"username": _KG_USER, "key": _KG_KEY}, f)
os.chmod("/root/.kaggle/kaggle.json", 0o600)
os.environ["KAGGLE_USERNAME"] = _KG_USER
os.environ["KAGGLE_KEY"] = _KG_KEY
os.environ["CHD_API_KEY"] = _secret("CHD_API_KEY")

REPO = "https://github.com/bailiang0369/bn_hisdata.git"
WORK = "/kaggle/working"
START, END = "2025-07-01", "2026-10-20"
_s, _e = date.fromisoformat(START), date.fromisoformat(END)

os.chdir(WORK)
if not os.path.isdir("bn_hisdata"):
    subprocess.run(f"git clone {REPO}", shell=True, check=True)
os.chdir("bn_hisdata")
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "polars", "zstandard"], check=True)

DATA = "orderbook/data"
os.makedirs(DATA, exist_ok=True)

# ---- 进度持久化：GitHub Releases（逐个上传 {date}.zip，天然增量，传过的绝不重传）----
# Kaggle Dataset 不支持增量、且当前 Kaggle token 无建库权限，故改用 GitHub Release 资产：
# 每天一个 {date}.zip 作为某月 release（data-YYYY-MM）的资产；启动时列出资产补齐缺失天。
GH_REPO = "bailiang0369/bn_hisdata"
GH_TOKEN = _secret("GH_TOKEN")
GH_API = "https://api.github.com"
GH_UPLOAD = "https://uploads.github.com"
GH_HDRS = {"Authorization": f"Bearer {GH_TOKEN}",
           "Accept": "application/vnd.github+json"}
GH_TAG = lambda ch: f"data-{ch}"      # ch = YYYY-MM

_rel_cache = {}    # ch -> release_id（None 表示建失败）
_ass_cache = {}    # ch -> {asset名: asset_id}

def _gh(method, url, **kw):
    h = dict(GH_HDRS)
    h.update(kw.pop("headers", {}))
    return requests.request(method, url, headers=h, timeout=180, **kw)

def _ensure_release(ch):
    if ch in _rel_cache:
        return _rel_cache[ch]
    tag = GH_TAG(ch)
    r = _gh("GET", f"{GH_API}/repos/{GH_REPO}/releases/tags/{tag}")
    if r.status_code == 200:
        rid = r.json()["id"]
    elif r.status_code == 404:
        r2 = _gh("POST", f"{GH_API}/repos/{GH_REPO}/releases",
                 json={"tag_name": tag, "name": f"Orderbook data {ch}",
                       "prerelease": True})
        if r2.status_code not in (200, 201):
            print(f"[gh] create release {tag} -> {r2.status_code}: {r2.text[:200]}")
            _rel_cache[ch] = None
            return None
        rid = r2.json()["id"]
    else:
        print(f"[gh] get release {tag} -> {r.status_code}: {r.text[:200]}")
        _rel_cache[ch] = None
        return None
    _rel_cache[ch] = rid
    return rid

def _list_assets(rid):
    """返回 {asset名: asset_id}。"""
    out = {}
    page = 1
    while True:
        r = _gh("GET", f"{GH_API}/repos/{GH_REPO}/releases/{rid}/assets?per_page=100&page={page}")
        if r.status_code != 200:
            break
        arr = r.json()
        for a in arr:
            out[a["name"]] = a["id"]
        if len(arr) < 100:
            break
        page += 1
    return out

def _upload_day(d):
    """把 DATA/{d}/ 打包成 {d}.zip 上传到对应月 release；已存在则跳过（不重传）。"""
    folder = os.path.join(DATA, d)
    if not os.path.isdir(folder):
        return False
    ch = d[:7]
    rid = _ensure_release(ch)
    if rid is None:
        return False
    if ch not in _ass_cache:
        _ass_cache[ch] = _list_assets(rid)
    if f"{d}.zip" in _ass_cache[ch]:
        return True
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for fn in os.listdir(folder):
            fp = os.path.join(folder, fn)
            if os.path.isfile(fp):
                z.write(fp, arcname=fn)
    blob = buf.getvalue()
    url = f"{GH_UPLOAD}/repos/{GH_REPO}/releases/{rid}/assets?name={d}.zip"
    for attempt in (1, 2, 3):
        r = _gh("POST", url, headers={"Content-Type": "application/zip"}, data=blob)
        if r.status_code in (200, 201):
            _ass_cache[ch][f"{d}.zip"] = None
            print(f"[gh] uploaded {d}.zip ({len(blob) // 1024} KB)")
            return True
        if r.status_code == 422 and "already_exists" in r.text:
            _ass_cache[ch][f"{d}.zip"] = None
            return True
        print(f"[gh] upload {d} try{attempt} -> {r.status_code}: {r.text[:150]}")
        time.sleep(5)
    return False

def _restore():
    """从 GitHub Releases 补齐本地缺失的天。"""
    y, m = 2025, 7
    while (y, m) <= (2026, 10):
        ch = f"{y:04d}-{m:02d}"
        r = _gh("GET", f"{GH_API}/repos/{GH_REPO}/releases/tags/{GH_TAG(ch)}")
        if r.status_code == 200:
            rid = r.json()["id"]
            amap = _list_assets(rid)
            _rel_cache[ch] = rid
            _ass_cache[ch] = dict(amap)
            for an, aid in amap.items():
                if not re.match(r"\d{4}-\d{2}-\d{2}\.zip$", an):
                    continue
                d = an[:-4]
                if _day_satisfied(d):
                    continue
                ar = requests.get(
                    f"{GH_API}/repos/{GH_REPO}/releases/assets/{aid}",
                    headers={"Authorization": f"Bearer {GH_TOKEN}",
                             "Accept": "application/octet-stream"},
                    timeout=300)
                if ar.status_code != 200:
                    print(f"[gh] restore {d} -> {ar.status_code}")
                    continue
                os.makedirs(os.path.join(DATA, d), exist_ok=True)
                try:
                    with zipfile.ZipFile(io.BytesIO(ar.content)) as z:
                        z.extractall(os.path.join(DATA, d))
                    print(f"[gh] restored {d}")
                except Exception as e:  # noqa: BLE001
                    print(f"[gh] unzip {d} err: {repr(e)[:120]}")
        m += 1
        if m > 12:
            m, y = 1, y + 1

def _final_sweep():
    """收尾兜底：本地已满足、但 release 里还没有的天补传。"""
    for _i in range((_e - _s).days + 1):
        d = (_s + timedelta(days=_i)).isoformat()
        if _day_satisfied(d):
            _upload_day(d)

# ---- 逐天跑全量（跳过已完成天）----
SKIP = "_SKIP_SOURCE_MISSING"
EXCHANGES = ("binance_futures", "binance_spot")

def _day_satisfied(d: str) -> bool:
    """band 两市场齐全，或已标记源端缺失 -> 视为该天处理完毕（不再重下）。"""
    if (os.path.exists(f"{DATA}/{d}/binance_futures_minute_band.parquet")
            and os.path.exists(f"{DATA}/{d}/binance_spot_minute_band.parquet")):
        return True
    return os.path.exists(f"{DATA}/{d}/{SKIP}")

def _raw_present(d: str) -> bool:
    """任一交易所任一小时真正下载到了原始文件 -> 源端当天确有数据。"""
    for ex in ("binance_futures", "binance_spot"):
        for h in range(24):
            if (Path(DATA) / d / ex / f"{h:02d}" / "BTCUSDT_orderbook.parquet").exists():
                return True
    return False

_restore()   # 启动时先从 GitHub Release 补齐缺失天

_s, _e = date.fromisoformat(START), date.fromisoformat(END)
_processed = 0
for _i in range((_e - _s).days + 1):
    _d = (_s + timedelta(days=_i)).isoformat()
    if _day_satisfied(_d):
        continue
    rc = subprocess.run([sys.executable, "orderbook/pipeline.py", "--date", _d,
                         "--download", "r2", "--mode", "dense"]).returncode
    if rc == 0:
        # 构建完成：删掉当天原始小时文件（每个 ~960MB），只保留 band 产物。
        for _ex in EXCHANGES:
            shutil.rmtree(f"{DATA}/{_d}/{_ex}", ignore_errors=True)
        _processed += 1
        _upload_day(_d)          # 当天构建完立即上传；传过的下次不会重传
    else:
        # 下载到 0 个文件（源端整日 404）-> 永久标记跳过，绝不跨轮重下
        if not _raw_present(_d):
            os.makedirs(f"{DATA}/{_d}", exist_ok=True)
            with open(f"{DATA}/{_d}/{SKIP}", "w") as _f:
                _f.write(_d)
            print(f"day {_d}: source missing (all 404) -> marked SKIP, will NOT retry")
            _upload_day(_d)      # SKIP 标记同样上传，便于跨轮跳过
        else:
            print(f"day {_d} rc={rc} (有原始文件但处理失败，下次运行重试)")
        _processed += 1

print("processed this run:", _processed)

# ---- 本地生成月度文件（不随数据集上传）----
subprocess.run([sys.executable, "orderbook/merge_monthly.py",
                "--dest-root", DATA, "--out-dir", "orderbook/monthly_out"], check=False)

# ---- 完成度检查：整段区间“应有数据”的天都齐了才写 _COMPLETE 标记 ----
_all_done = True
_missing = []
for _i in range((_e - _s).days + 1):
    _d = (_s + timedelta(days=_i)).isoformat()
    if os.path.exists(f"{DATA}/{_d}/{SKIP}"):
        continue
    for _ex in ("binance_futures", "binance_spot"):
        if not os.path.exists(f"{DATA}/{_d}/{_ex}_minute_band.parquet"):
            _all_done = False
            _missing.append(_d)
            break
if _all_done:
    with open(os.path.join(DATA, "_COMPLETE"), "w") as _f:
        _f.write("all available days built")
    print("ALL DAYS COMPLETE -> wrote _COMPLETE marker")
elif len(_missing) < 20:
    print("still missing sample:", _missing[:20])
else:
    print(f"still missing {len(_missing)} days, e.g.", _missing[:5])

# ---- 收尾兜底：把本地已满足但 release 里还没有的天补传 ----
_final_sweep()
print("DONE")
