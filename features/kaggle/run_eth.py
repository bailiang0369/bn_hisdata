#!/usr/bin/env python3
"""Kaggle kernel: 用 bn_hisdata 管线构建 ETHUSDT 订单簿特征，产出可发布为 Kaggle Dataset 的 parquet。

为什么单独一个内核：`run_features.py` 依赖 Release 上**预构建好的 BTC band**，
而 ETH 的 band 在 Release 上不存在，必须从 CryptoHFTData 原始 L2 重新构建。

MODE（环境变量）：
  probe（默认）: 只探测 CryptoHFTData 上 ETHUSDT 是否存在，打印样本，不下载。
  build       : 逐日构建 band → 月度 → L1 → merged → /kaggle/working/eth_dataset/{SYMBOL}.parquet

环境变量：
  MODE=probe|build          默认 probe
  OB_SYMBOL=ETHUSDT         目标标的
  START / END               日期范围，默认 2025-07-01 / 2026-10-20
  MAX_DAYS=0                只处理前 N 天（0=全部；用于单次运行内控时长）

需要 Kaggle Secret（或同名环境变量）：
  CHD_API_KEY   CryptoHFTData API key（probe/build 必需）
  GH_TOKEN      可选，未用（本内核不走 Release 断点续传）

产物：/kaggle/working/eth_dataset/{SYMBOL}.parquet
      在 Kaggle 内核输出页可直接 "Create Dataset" 发布。
"""
import os
import sys
import subprocess
from datetime import date, timedelta
from pathlib import Path

REPO = "https://github.com/bailiang0369/bn_hisdata.git"
GH_REPO = "bailiang0369/bn_hisdata"
DL_BASE = f"https://github.com/{GH_REPO}/releases/download"

WORK = Path("/kaggle/working")
ROOT = WORK / "bn_hisdata"
SYMBOL = os.environ.get("OB_SYMBOL", "ETHUSDT")
START = os.environ.get("START", "2025-07-01")
END = os.environ.get("END", "2026-10-20")
MODE = os.environ.get("MODE", "probe").strip().lower()
MAX_DAYS = int(os.environ.get("MAX_DAYS", "0"))

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
    """Range 续传；已完整则跳过。返回是否成功。"""
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


def probe() -> int:
    """探测 CryptoHFTData 是否有目标标的，并与 BTC 做对照。"""
    key = secret("CHD_API_KEY")
    if not key:
        log("缺少 CHD_API_KEY：请在 Kaggle 内核设置里 attach Secret（或设同名环境变量）")
        return 3
    sys.path.insert(0, str(ROOT / "orderbook"))
    from download_orderbook_s3 import get_credentials, S3LiteClient  # noqa: E402

    creds = get_credentials(key)
    bucket = creds["bucket"]
    cli = S3LiteClient(
        endpoint=creds["endpoint"],
        access_key_id=creds["credentials"]["access_key_id"],
        secret_access_key=creds["credentials"]["secret_access_key"],
        session_token=creds["credentials"].get("session_token"),
        region=creds.get("region") or "auto",
        timeout=300,
        max_attempts=3,
    )

    def ls(prefix: str):
        try:
            return cli.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=10).get("Contents", [])
        except Exception as exc:  # noqa: BLE001
            return [{"Key": f"ERR {repr(exc)[:100]}"}]

    log(f"bucket={bucket}")
    sample_days = ["2025-07-01", "2025-12-01", "2026-06-01", "2026-10-01"]
    summary = {}
    for d in sample_days:
        for sym in ("BTCUSDT", SYMBOL):
            hits = ls(f"binance_futures/{d}/00/{sym}")
            keys = [h.get("Key", "?") for h in hits]
            summary[(d, sym)] = keys
            log(f"futures {d} 00 {sym}: {len(keys)} hit(s)  {keys[:2]}")

    for d in sample_days[:2]:
        hits = ls(f"binance_spot/{d}/00/{SYMBOL}")
        log(f"spot    {d} 00 {SYMBOL}: {len(hits)} hit(s)")

    eth_hits = sum(len(v) for (d, s), v in summary.items() if s == SYMBOL)
    btc_hits = sum(len(v) for (d, s), v in summary.items() if s == "BTCUSDT")
    log("=" * 60)
    log(f"结论：BTC hits={btc_hits}（对照，应 >0）；{SYMBOL} hits={eth_hits}")
    if eth_hits:
        log(f"✅ {SYMBOL} 数据存在，可以 MODE=build 构建")
        return 0
    log(f"❌ {SYMBOL} 在 CryptoHFTData 上不存在（或 key 无权限）→ 订单簿腿无法构建")
    return 1


def build() -> int:
    """逐日构建 band → 月度 → L1 → merged → 单文件。"""
    key = secret("CHD_API_KEY")
    if not key:
        log("缺少 CHD_API_KEY，无法下载原始 L2")
        return 3

    base = WORK / "eth"
    data, vis, monthly, l1, merged = (base / "data", base / "vision",
                                      base / "monthly", base / "l1", base / "merged")
    for p in (data, vis, monthly, l1, merged):
        p.mkdir(parents=True, exist_ok=True)

    # 1) vision-base（K线+基差+metrics，含 ETHUSDT）
    for m in months(START, END):
        n = f"{SYMBOL}_base_features_{m}.parquet"
        download(f"{DL_BASE}/vision-base/{n}", vis / n)

    env = dict(os.environ, OB_SYMBOL=SYMBOL, CHD_API_KEY=key)

    # 2) 逐日构建 band（pipeline 会跳过已完成的日）
    todo = days(START, END)
    if MAX_DAYS:
        todo = todo[:MAX_DAYS]
    log(f"构建 band：{len(todo)} 天（{todo[0]} ~ {todo[-1]}）")
    for d in todo:
        sh([sys.executable, "orderbook/pipeline.py", "--date", d,
            "--download", "r2", "--mode", "dense", "--dest-root", str(data)],
           cwd=str(ROOT), env=env)

    # 3) 月度合并
    sh([sys.executable, "orderbook/merge_monthly.py",
        "--dest-root", str(data), "--out-dir", str(monthly)],
       cwd=str(ROOT), env=env)

    # 4) 逐月 L1 截面特征
    for m in months(START, END):
        for ex in EXCHANGES:
            band = monthly / f"{ex}_minute_band_{m}.parquet"
            mid = monthly / f"{ex}_minute_band_mid_{m}.parquet"
            out = l1 / f"ob_features_{ex}_{m}.parquet"
            if not band.exists() or not mid.exists():
                log(f"[{m}] 缺 {ex} band/mid，跳过 L1")
                continue
            if out.exists():
                continue
            sh([sys.executable, "features/ob_features.py",
                "--band", str(band), "--mid", str(mid), "--out", str(out), "--force"],
               cwd=str(ROOT), env=env)

    # 5) 合并 vision + L1（全序列 reindex + 滚动 + 校验）
    sh([sys.executable, "features/build_dataset.py",
        "--vision", str(vis / f"{SYMBOL}_base_features_*.parquet"),
        "--fut", str(l1 / "ob_features_binance_futures_*.parquet"),
        "--spot", str(l1 / "ob_features_binance_spot_*.parquet"),
        "--out-dir", str(merged)],
       cwd=str(ROOT), env=env)

    # 6) 拼成单文件 → /kaggle/working/eth_dataset/
    import polars as pl

    files = sorted(merged.glob("merged_*.parquet"))
    if not files:
        log("没有 merged 产物，构建失败")
        return 1
    out_dir = WORK / "eth_dataset"
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / f"{SYMBOL}.parquet"
    log(f"concat {len(files)} 个月度文件 -> {final}")
    df = (pl.concat([pl.scan_parquet(f) for f in files], how="vertical_relaxed")
          .unique(subset=["close_time"], keep="last")
          .sort("close_time")
          .collect())
    df.write_parquet(final, compression="zstd")
    log(f"DONE {final} {df.height:,} 行 x {df.width} 列 "
        f"{final.stat().st_size / 1e6:.1f} MB")
    log("在 Kaggle 输出页对 eth_dataset/ 执行 Create Dataset 即可发布")
    return 0


def main() -> int:
    log(f"MODE={MODE} SYMBOL={SYMBOL} START={START} END={END} MAX_DAYS={MAX_DAYS}")
    ensure_repo()
    if MODE == "probe":
        return probe()
    if MODE == "build":
        return build()
    log(f"未知 MODE={MODE}（应为 probe|build）")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
