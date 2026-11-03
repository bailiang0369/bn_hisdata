#!/usr/bin/env python3
"""Kaggle kernel: 用 bn_hisdata 管线构建 ETHUSDT 订单簿特征，产出可发布为 Kaggle Dataset 的 parquet。

为什么单独一个内核：`run_features.py` 依赖 Release 上**预构建好的 BTC band**，
而 ETH 的 band 在 Release 上不存在，必须从 CryptoHFTData 原始 L2 重新构建。

MODE（环境变量）：
  probe（默认）: 只精确探测 `{SYMBOL}_orderbook` 键是否存在，打印大小，不下载。
  smoke       : 探测 + 构建 MAX_DAYS 天 band/L1（端到端验证管线与耗时）。
  build       : 全量构建 band → 月度 → L1 → merged → /kaggle/working/eth_dataset/{SYMBOL}.parquet

环境变量：
  MODE=probe|smoke|build    默认 probe
  OB_SYMBOL=ETHUSDT         目标标的
  START / END               日期范围，默认 2025-07-01 / 2026-10-20
  MAX_DAYS=0                smoke 处理天数（默认 2）；build 时为 0 表示全部

需要 CHD_API_KEY（本内核用 --env 注入到私有内核，不入公开仓库）。

产物：/kaggle/working/eth_dataset/{SYMBOL}.parquet（内核输出页可 Create Dataset）
中间产物一律放 /tmp，避免 /kaggle/working 过大导致 `kaggle kernels output` 卡死。
"""
import os
import sys
import json
import time
import contextlib
import subprocess
from datetime import date, timedelta
from pathlib import Path

REPO = "https://github.com/bailiang0369/bn_hisdata.git"
GH_REPO = "bailiang0369/bn_hisdata"
DL_BASE = f"https://github.com/{GH_REPO}/releases/download"

WORK = Path("/kaggle/working")
TMP = Path("/tmp")
ROOT = TMP / "bn_hisdata"
SYMBOL = os.environ.get("OB_SYMBOL", "ETHUSDT")
START = os.environ.get("START", "2025-07-01")
END = os.environ.get("END", "2026-10-20")
MODE = os.environ.get("MODE", "probe").strip().lower()
MAX_DAYS = int(os.environ.get("MAX_DAYS", "2"))

EXCHANGES = ("binance_futures", "binance_spot")


def log(msg: str) -> None:
    print(f"[eth] {msg}", flush=True)


def sh(cmd, **kw):
    log("+ " + " ".join(cmd))
    return subprocess.run(cmd, check=kw.pop("check", True), **kw)


def secret(name: str):
    v = os.environ.get(name)
    if v:
        return v
    try:
        try:
            from kaggle_secrets import UserSecretsClient as _C
        except ImportError:
            from kaggle_secrets import UserSecretClient as _C
        return _C().get_secret(name)
    except Exception as exc:  # noqa: BLE001
        log(f"secret {name} unavailable ({type(exc).__name__})")
        return None


def months(start: str, end: str) -> list[str]:
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    out, y, m = [], s.year, s.month
    while (y, m) <= (e.year, e.month):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def days(start: str, end: str) -> list[str]:
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    return [(s + timedelta(days=i)).isoformat() for i in range((e - s).days + 1)]


def ensure_repo() -> None:
    if not (ROOT / ".git").exists():
        sh(["git", "clone", REPO, str(ROOT)])
    sh([sys.executable, "-m", "pip", "install", "-q", "polars", "zstandard", "requests"])


def download(url: str, dst: Path) -> bool:
    import requests

    if dst.exists() and dst.stat().st_size > 0:
        return True
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".part")
    start = tmp.stat().st_size if tmp.exists() else 0
    h = {"Range": f"bytes={start}-"} if start else {}
    try:
        with requests.get(url, stream=True, timeout=300, headers=h) as r:
            if start and r.status_code == 206:
                mode = "ab"
            elif r.status_code in (200, 206):
                mode = "wb"
            else:
                log(f"download {url} -> HTTP {r.status_code}")
                return False
            with open(tmp, mode) as f:
                for c in r.iter_content(1 << 20):
                    f.write(c)
        tmp.replace(dst)
        return True
    except Exception as exc:  # noqa: BLE001
        log(f"download {dst.name} failed: {repr(exc)[:120]}")
        return False


def _client(key: str):
    sys.path.insert(0, str(ROOT / "orderbook"))
    from download_orderbook_s3 import get_credentials, S3LiteClient  # noqa: E402

    creds = get_credentials(key)
    cli = S3LiteClient(
        endpoint=creds["endpoint"],
        access_key_id=creds["credentials"]["access_key_id"],
        secret_access_key=creds["credentials"]["secret_access_key"],
        session_token=creds["credentials"].get("session_token"),
        region=creds.get("region") or "auto",
        timeout=300,
        max_attempts=3,
    )
    return cli, creds["bucket"]


def _ls(cli, bucket: str, prefix: str) -> list[dict]:
    try:
        return cli.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=20).get("Contents", [])
    except Exception as exc:  # noqa: BLE001
        return [{"Key": f"ERR {repr(exc)[:100]}", "Size": 0}]


def probe(key: str) -> int:
    """精确探测 {exchange}/{date}/{hour}/{SYMBOL}_orderbook 是否存在。"""
    cli, bucket = _client(key)
    log(f"bucket={bucket}")
    sample = ["2025-07-01", "2026-01-15", "2026-10-15"]
    found, missing = 0, 0
    for d in sample:
        for ex in EXCHANGES:
            prefix = f"{ex}/{d}/00/{SYMBOL}_orderbook"
            hits = _ls(cli, bucket, prefix)
            keys = [h.get("Key", "?") for h in hits]
            sizes = [h.get("Size", 0) for h in hits]
            if keys and not keys[0].startswith("ERR"):
                found += 1
                log(f"✅ {ex} {d}: {keys}  size={sizes} ({sizes[0] / 1e6:.1f} MB)" if sizes else f"✅ {ex} {d}")
            else:
                missing += 1
                log(f"❌ {ex} {d}: 无 {SYMBOL}_orderbook（hits={len(keys)}）")
    res = {"symbol": SYMBOL, "bucket": bucket, "found": found, "missing": missing,
           "sample": sample, "exchanges": list(EXCHANGES)}
    WORK.mkdir(parents=True, exist_ok=True)
    (WORK / "eth_probe.json").write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"probe 结果：found={found} missing={missing}")
    return 0 if found and not missing else 1


def build(key: str, max_days: int) -> int:
    """逐日构建 band → 月度 → L1 → merged → 单文件。中间产物在 /tmp。"""
    base = TMP / "eth"
    data, vis, monthly, l1, merged = (base / "data", base / "vision",
                                      base / "monthly", base / "l1", base / "merged")
    for p in (data, vis, monthly, l1, merged):
        p.mkdir(parents=True, exist_ok=True)

    # 1) vision-base（K线+基差+metrics，含 ETHUSDT）
    for m in months(START, END):
        n = f"{SYMBOL}_base_features_{m}.parquet"
        download(f"{DL_BASE}/vision-base/{n}", vis / n)

    env = dict(os.environ, OB_SYMBOL=SYMBOL, CHD_API_KEY=key)

    # 2) 逐日构建 band
    todo = days(START, END)
    if max_days:
        todo = todo[:max_days]
    log(f"构建 band：{len(todo)} 天（{todo[0]} ~ {todo[-1]}）")
    t0 = time.time()
    for d in todo:
        sh([sys.executable, "orderbook/pipeline.py", "--date", d,
            "--download", "r2", "--mode", "dense", "--dest-root", str(data)],
           cwd=str(ROOT), env=env)
    log(f"band 完成，用时 {time.time() - t0:.0f}s")

    built = sorted(data.glob("*/binance_futures_minute_band.parquet"))
    log(f"band 产物：{len(built)} 天，样本大小 "
        f"{[round(p.stat().st_size / 1e6, 1) for p in built[:3]]} MB")

    # 3) 月度合并
    sh([sys.executable, "orderbook/merge_monthly.py",
        "--dest-root", str(data), "--out-dir", str(monthly)],
       cwd=str(ROOT), env=env)

    # 4) 逐月 L1
    for m in months(START, END):
        for ex in EXCHANGES:
            band = monthly / f"{ex}_minute_band_{m}.parquet"
            mid = monthly / f"{ex}_minute_band_mid_{m}.parquet"
            out = l1 / f"ob_features_{ex}_{m}.parquet"
            if not band.exists() or not mid.exists() or out.exists():
                continue
            sh([sys.executable, "features/ob_features.py",
                "--band", str(band), "--mid", str(mid), "--out", str(out), "--force"],
               cwd=str(ROOT), env=env)
    l1_files = sorted(l1.glob("ob_features_*.parquet"))
    log(f"L1 产物：{len(l1_files)} 个")

    if max_days:
        log("smoke 模式：跳过 build_dataset/拼接，只验证 band+L1 管线")
        return 0 if built and l1_files else 1

    # 5) 合并 vision + L1
    sh([sys.executable, "features/build_dataset.py",
        "--vision", str(vis / f"{SYMBOL}_base_features_*.parquet"),
        "--fut", str(l1 / "ob_features_binance_futures_*.parquet"),
        "--spot", str(l1 / "ob_features_binance_spot_*.parquet"),
        "--out-dir", str(merged)],
       cwd=str(ROOT), env=env)

    # 6) 拼单文件 → /kaggle/working/eth_dataset/
    import polars as pl

    files = sorted(merged.glob("merged_*.parquet"))
    if not files:
        log("没有 merged 产物")
        return 1
    out_dir = WORK / "eth_dataset"
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / f"{SYMBOL}.parquet"
    log(f"concat {len(files)} 个月度文件 -> {final}")
    df = (pl.concat([pl.scan_parquet(f) for f in files], how="vertical_relaxed")
          .unique(subset=["close_time"], keep="last").sort("close_time").collect())
    df.write_parquet(final, compression="zstd")
    log(f"DONE {final} {df.height:,} 行 x {df.width} 列 {final.stat().st_size / 1e6:.1f} MB")
    return 0


def main() -> int:
    log(f"MODE={MODE} SYMBOL={SYMBOL} START={START} END={END} MAX_DAYS={MAX_DAYS}")
    key = secret("CHD_API_KEY")
    if not key:
        log("缺少 CHD_API_KEY：用 --env CHD_API_KEY=... 注入，或 attach Kaggle Secret")
        return 3
    ensure_repo()
    if MODE == "probe":
        return probe(key)
    if MODE in ("smoke", "build"):
        rc = probe(key)
        if rc != 0:
            log("探测未通过，终止")
            return rc
        return build(key, MAX_DAYS if MODE == "smoke" else 0)
    log(f"未知 MODE={MODE}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
