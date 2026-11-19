#!/usr/bin/env python3
"""探测 OKX L2 归档的真实结构（NDJSON schema / 快照频率 / 单日行数 / 大小）。

在写重建脚本之前必须先跑这个：打印 tar.gz 内文件名、每个文件前 N 行原始内容、
字段名集合、时间戳范围。避免按猜测的 schema 写 parser。

用法：
  python inspect_day.py --date 2025-10-01 [--inst-type SWAP] [--levels 400]
"""
from __future__ import annotations

import argparse
import io
import json
import tarfile
import time
from collections import Counter
from pathlib import Path

import requests

from download_day import API, UA, day_end_ms, get_download_urls

HERE = Path(__file__).resolve().parent


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--inst-type", default="SWAP", choices=["SWAP", "SPOT", "FUTURES"])
    ap.add_argument("--inst-id", default="BTC-USDT")
    ap.add_argument("--levels", type=int, default=400)
    ap.add_argument("--n-lines", type=int, default=5, help="每个文件打印前几行原始内容")
    ap.add_argument("--local", default=None, help="直接探测本地 tar.gz，跳过下载")
    args = ap.parse_args()

    s = requests.Session()
    if args.local:
        targets = [(Path(args.local).name, None, Path(args.local))]
    else:
        d = time.strptime(args.date, "%Y-%m-%d")
        from datetime import date as _date
        dd = _date(d.tm_year, d.tm_mon, d.tm_mday)
        pairs = get_download_urls(dd, args.inst_type, args.inst_id, args.levels, s)
        if not pairs:
            log("no archive found")
            return 1
        # 只探测第一个文件（通常单日就一个）
        targets = []
        for name, url, expect in pairs[:1]:
            log(f"downloading {name} (expected {expect / 1e6:.0f} MB) ...")
            r = s.get(url, timeout=300)
            r.raise_for_status()
            targets.append((name, url, io.BytesIO(r.content)))

    for name, _url, src in targets:
        log(f"=== {name}")
        if isinstance(src, Path):
            data = src.read_bytes()
        else:
            data = src.getvalue()
        log(f"  compressed size: {len(data) / 1e6:.1f} MB")
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as tf:
            members = [m for m in tf.getmembers() if m.isfile()]
            log(f"  members: {len(members)}")
            for m in members[:10]:
                log(f"    {m.name}  {m.size / 1e6:.1f} MB")
            for m in members[:1]:
                f = tf.extractfile(m)
                if f is None:
                    continue
                keys, actions, n = Counter(), Counter(), 0
                tmin, tmax = None, None
                for i, raw in enumerate(f):
                    n += 1
                    line = raw.decode("utf-8", "replace").strip()
                    if i < args.n_lines:
                        log(f"    line[{i}]: {line[:400]}")
                    try:
                        obj = json.loads(line)
                        # 可能是逐事件扁平对象，也可能是 {"arg":..,"action":..,"data":[..]}
                        if isinstance(obj, dict):
                            keys.update(obj.keys())
                            if "action" in obj:
                                actions[str(obj["action"])] += 1
                            # 找时间戳
                            ts = obj.get("ts") or obj.get("timestamp")
                            if ts:
                                v = int(ts)
                                tmin = v if tmin is None else min(tmin, v)
                                tmax = v if tmax is None else max(tmax, v)
                            # 嵌套 data
                            if isinstance(obj.get("data"), list) and obj["data"]:
                                first = obj["data"][0]
                                if isinstance(first, dict):
                                    keys.update("data." + k for k in first.keys())
                    except Exception:
                        pass
                log(f"  total lines: {n:,}")
                log(f"  top-level keys: {keys.most_common(20)}")
                log(f"  actions: {actions.most_common(10)}")
                if tmin and tmax:
                    log(f"  ts range: {tmin} ~ {tmax} (span {(tmax - tmin) / 1000 / 60:.1f} min)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
