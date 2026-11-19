#!/usr/bin/env python3
"""从 OKX 官方 Historical Data 下载 BTC-USDT 的 L2 订单簿归档（每日 tar.gz）。

数据源与版权
------------
OKX 官方历史数据门户（https://www.okx.com/en-us/historical-data）提供 L2 订单簿
历史（400 档从 2023-03-23 起，5000 档从 2025-11-01 起），免费公开下载。
下载链接由前端调用的私有接口动态生成：

  POST /priapi/v5/broker/public/trade-data/download-link
  {module:10, instType, instQueryParam:{instIdList:[...]}, dateQuery:{dateAggrType,begin,end}}

  module=10 = ORDER_BOOK；dateQuery 的 begin/end 是当天 23:59:59.999 的毫秒时间戳字符串。

本脚本不硬编码 CDN 域名（链接来自接口返回的 details[].groupDetails[].url），
避免 OKX 换域名时失效。

用法
----
  python download_day.py --date 2025-10-01 [--inst-type SWAP] [--levels 400]
  python download_day.py --date 2025-10-01 --inst-type SPOT --levels 400

产物布局（与 orderbook/ 保持一致，data/ 被 gitignore）
-------------------------------------------------------
  data/{date}/{instId}/BTC-USDT-L2orderbook-400lv-{date}.tar.gz

注意
----
- 归档是 NDJSON snapshots/updates（逐事件），不是 1m 快照；需 build_minute_snapshots.py 重建。
- OKX 归档覆盖前一日 16:00 UTC 到当日 15:59:59 UTC（与币安自然日不同），
  对齐时务必确认时区语义，见 docs/okx_notes.md。
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
API = "https://www.okx.com/priapi/v5/broker/public/trade-data/download-link"
# API 的 module：4=ORDER_BOOK_400, 5=ORDER_BOOK_5000（不是 UI dialogType 的 10）
MODULE_ORDER_BOOK_400 = 4
MODULE_ORDER_BOOK_5000 = 5
UA = {"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"}
TIMEOUT = 180
RETRIES = 4


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def day_end_ms(d: date) -> str:
    """OKX 前端对 Order Book 用当天 23:59:59.999 的毫秒时间戳。"""
    dt = datetime(d.year, d.month, d.day, 23, 59, 59, 999000, tzinfo=timezone.utc)
    return str(int(dt.timestamp() * 1000))


def get_download_urls(d: date, inst_type: str, inst_id: str, levels: int,
                      session: requests.Session) -> list[tuple[str, str, int]]:
    """返回 [(filename, url), ...]，来自 download-link 接口。"""
    is_spot = inst_type.upper() == "SPOT"
    payload = {
        "module": MODULE_ORDER_BOOK_400 if levels == 400 else MODULE_ORDER_BOOK_5000,
        "instType": inst_type.upper(),
        "instQueryParam": {
            "instIdList": [inst_id] if is_spot else None,
            "instFamilyList": None if is_spot else [inst_id],
            "ccyList": None,
        },
        "dateQuery": {
            "dateAggrType": "daily",
            "begin": day_end_ms(d),
            "end": day_end_ms(d),
        },
    }
    last: Exception | None = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = session.post(API, json=payload, headers=UA, timeout=60)
            r.raise_for_status()
            j = r.json()
            if j.get("code") != "0":
                raise RuntimeError(f"OKX code={j.get('code')} msg={j.get('msg')}")
            out = []
            for detail in j.get("data", {}).get("details", []):
                for g in detail.get("groupDetails", []):
                    name, url = g.get("filename", ""), g.get("url", "")
                    if not name or not url:
                        continue
                    tag = "400lv" if levels == 400 else "5000lv"
                    if tag not in name:
                        continue
                    size = int(g.get("sizeMB", 0) * 1e6)
                    out.append((name, url, size))
            return out
        except Exception as e:
            last = e
            if attempt < RETRIES:
                wait = 2 ** attempt
                log(f"  attempt {attempt} failed: {e}; retry in {wait}s")
                time.sleep(wait)
    raise RuntimeError(f"download-link failed after {RETRIES}: {last}")


def download(url: str, dst: Path, session: requests.Session, expect: int = 0) -> None:
    """流式下载到 .part，完成后改名；已存在则跳过。"""
    if dst.exists() and dst.stat().st_size > 0:
        log(f"  skip {dst.name} ({dst.stat().st_size / 1e6:.0f} MB)")
        return
    tmp = dst.with_suffix(dst.suffix + ".part")
    first = tmp.stat().st_size if tmp.exists() else 0
    headers = {"Range": f"bytes={first}-"} if first else None
    with session.get(url, stream=True, timeout=TIMEOUT, headers=headers) as r:
        if r.status_code not in (200, 206):
            raise RuntimeError(f"HTTP {r.status_code} for {url[:100]}")
        mode = "ab" if (first and r.status_code == 206) else "wb"
        n = first
        with open(tmp, mode) as f:
            for chunk in r.iter_content(1 << 20):
                if chunk:
                    f.write(chunk)
                    n += len(chunk)
    if n == 0:
        raise RuntimeError(f"empty download: {url[:100]}")
    if expect and abs(n - expect) > max(1e6, expect * 0.02):
        log(f"  WARN size {n} != expected {expect} for {dst.name}")
    tmp.replace(dst)
    log(f"  ok {dst.name} ({n / 1e6:.0f} MB)")


def main() -> int:
    ap = argparse.ArgumentParser(description="Download OKX L2 orderbook daily archive")
    ap.add_argument("--date", default=os.environ.get("DATE", ""), help="UTC date YYYY-MM-DD")
    ap.add_argument("--inst-type", default=os.environ.get("INST_TYPE", "SWAP"),
                    choices=["SWAP", "SPOT", "FUTURES"])
    ap.add_argument("--inst-id", default=os.environ.get("INST_ID", "BTC-USDT"),
                    help="SPOT 用 BTC-USDT；SWAP/FUTURES 用 instFamily")
    ap.add_argument("--levels", type=int, default=int(os.environ.get("LEVELS", "400")),
                    choices=[400, 5000])
    ap.add_argument("--dest-root", default=os.environ.get("DEST_ROOT", str(HERE / "data")))
    args = ap.parse_args()
    if not args.date:
        ap.error("--date or env DATE required")

    try:
        d = date.fromisoformat(args.date)
    except ValueError:
        print(f"invalid --date {args.date!r}; expected YYYY-MM-DD", file=sys.stderr)
        return 2

    out_dir = Path(args.dest_root) / args.date / args.inst_id
    out_dir.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    pairs = get_download_urls(d, args.inst_type, args.inst_id, args.levels, s)
    if not pairs:
        log(f"no {args.levels}lv archive for {args.date} {args.inst_type} {args.inst_id}")
        return 1
    log(f"{args.date}: {len(pairs)} file(s)")
    for name, url, expect in pairs:
        download(url, out_dir / name, s, expect)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
