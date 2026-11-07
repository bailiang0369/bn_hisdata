#!/usr/bin/env python3
"""按年切分的 1m K 线基础表管线：日度增量 → 年度 parquet → GitHub Release。

为什么按年（2026-11 定稿）
--------------------------
"每标的一个全量文件"看着最省事，但每天更新要**下载整个历史 + 重传整个历史**：
IO 随历史线性增长，跑几年后单次更新就是几百 MB 白跑一趟，消费端也得反复拉一个
越来越大的文件。改成按自然年切分：

* 增量只碰**当年**那一个文件（几十 MB）；历史年份写完即冻结，不再读写；
* 消费端可以只取需要的年份，也可以一条命令并发拉全部年份再拼接（列一致）；
* 命名 `{SYMBOL}_base_features_{YYYY}.parquet`，沿用旧月度资产的 glob 习惯，
  只是把 `{YYYY-MM}` 换成 `{YYYY}`。

release `vision-base` 里的资产（每标的每年一个）
    BTCUSDT_base_features_2025.parquet   # 已冻结
    BTCUSDT_base_features_2026.parquet   # 当年，持续增长，跨年后冻结

幂等
----
按"已覆盖到哪一天"推进，而不是只补昨天：漏跑几天、数据源延迟发布，都会在下次
运行时自动补齐。目标日文件 404 = 尚未发布，本轮跳过且**不报错**（次日自愈）。
首次运行（release 里还没有年度文件）会自动从 `DEFAULT_START` 回填一次。

用法
----
    python binance_vision/pipeline_daily.py update                     # 补到 UTC 昨天
    python binance_vision/pipeline_daily.py update --date 2026-11-06   # 补到指定日
    python binance_vision/pipeline_daily.py backfill --start 2025-07-01 --end 2026-11-06
    python binance_vision/pipeline_daily.py prune-legacy                # 列旧月度文件；加 --yes 才删

凭据：环境变量 GH_TOKEN（GitHub Actions 里传 secrets.GITHUB_TOKEN）。
"""
from __future__ import annotations

import argparse
import io
import os
import re
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import requests

VISION = "https://data.binance.vision/data"
GH_API = "https://api.github.com"
GH_UPLOAD = "https://uploads.github.com"

RELEASE_TAG = "vision-base"
RELEASE_TITLE = "Binance Vision K-line base features（按年切分）"

ROOT = Path(__file__).resolve().parent.parent
STATUS_FILE = ROOT / "DATA_STATUS.md"

KL_COLS = ["open_time", "open", "high", "low", "close", "volume", "close_time",
           "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume",
           "ignore"]

BASE_COLS = ["close_time", "open", "high", "low", "close",
             "um_buy_vol", "um_sell_vol", "um_count",
             "spot_open", "spot_high", "spot_low", "spot_close",
             "spot_buy_vol", "spot_sell_vol", "spot_count"]

# 一天本应 1440 根。源站确有残缺日（如 2020-02-19 只有 1085 根），低于 600 才认定为
# 文件损坏；残缺但不至于丢弃的日子照收，断档由 check_frame 告警。
MIN_ROWS_PER_DAY = 600
DEFAULT_START = date(2020, 1, 1)   # 数据源最早可用日（um 2019-12-31 起、spot 更早）；首次运行自动从这里回填

YEARLY_RE = re.compile(r"^(?P<sym>[A-Z0-9]+USDT)_base_features_(?P<year>(?:19|20)\d{2})\.parquet$")
MONTHLY_RE = re.compile(r"^(?P<sym>[A-Z0-9]+USDT)_base_features_(?P<ym>(?:19|20)\d{2}-\d{2})\.parquet$")


def log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------------------------------------------- 小工具
def _utc_yesterday() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def _parse_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise SystemExit(f"日期格式应为 YYYY-MM-DD，收到 {s!r}")


def _ms(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


def _date_of_ms(ct: int) -> date:
    """close_time 已 +1ms 对齐到下一分钟起点，减 1ms 才落回该 bar 所属的日子。"""
    return datetime.fromtimestamp((int(ct) - 1) / 1000, tz=timezone.utc).date()


def _fmt_ms(ct: int) -> str:
    return datetime.fromtimestamp(int(ct) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def _day_last_ms(d: date) -> int:
    """该日最后一根 1m bar 的 close_time（次日 00:00:00.000）。"""
    return _ms(d + timedelta(days=1))


def _daterange(lo: date, hi: date) -> list[date]:
    return [lo + timedelta(days=i) for i in range((hi - lo).days + 1)]


def _yearly_name(sym: str, year: int) -> str:
    return f"{sym}_base_features_{year}.parquet"


def _symbols(args) -> list[str]:
    return [s.strip().upper() for s in args.symbols.split(",") if s.strip()]


def _parquet_bytes(df: pl.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="zstd", compression_level=3)
    return buf.getvalue()


# ---------------------------------------------------------------- GitHub API
def _token() -> str:
    tok = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not tok:
        raise SystemExit("missing GH_TOKEN / GITHUB_TOKEN")
    return tok


class GH:
    def __init__(self, repo: str):
        self.repo = repo
        self.h = {"Authorization": f"Bearer {_token()}",
                  "Accept": "application/vnd.github+json",
                  "X-GitHub-Api-Version": "2022-11-28"}

    def call(self, method: str, url: str, **kw):
        return requests.request(method, url, headers={**self.h, **kw.pop("headers", {})},
                                timeout=900, **kw)

    def ensure_release(self, tag: str, name: str) -> int:
        r = self.call("GET", f"{GH_API}/repos/{self.repo}/releases/tags/{tag}")
        if r.status_code == 200:
            return r.json()["id"]
        r = self.call("POST", f"{GH_API}/repos/{self.repo}/releases",
                      json={"tag_name": tag, "name": name,
                            "body": "由 `binance_vision/pipeline_daily.py` 每日自动维护；"
                                    "每个标的每年一个 parquet。",
                            "prerelease": False})
        if r.status_code not in (200, 201):
            raise SystemExit(f"cannot create release {tag}: {r.status_code} {r.text[:300]}")
        return r.json()["id"]

    def assets(self, rid: int) -> dict[str, int]:
        out, page = {}, 1
        while True:
            r = self.call("GET", f"{GH_API}/repos/{self.repo}/releases/{rid}"
                                 f"/assets?per_page=100&page={page}")
            if r.status_code != 200:
                break
            arr = r.json()
            for a in arr:
                out[a["name"]] = a["id"]
            if len(arr) < 100:
                break
            page += 1
        return out

    def download(self, asset_id: int) -> bytes:
        r = self.call("GET", f"{GH_API}/repos/{self.repo}/releases/assets/{asset_id}",
                      headers={"Accept": "application/octet-stream"})
        if r.status_code != 200:
            raise SystemExit(f"download asset {asset_id} failed: {r.status_code}")
        return r.content

    def upload(self, rid: int, name: str, data: bytes) -> None:
        r = self.call("POST",
                      f"{GH_UPLOAD}/repos/{self.repo}/releases/{rid}/assets?name={name}",
                      headers={"Content-Type": "application/octet-stream",
                               "Accept": "application/vnd.github+json"},
                      data=data)
        if r.status_code not in (200, 201):
            raise SystemExit(f"upload {name} failed: {r.status_code} {r.text[:300]}")
        log(f"  uploaded {name} {len(data) / 1e6:.1f}MB")

    def delete(self, asset_id: int, name: str) -> None:
        r = self.call("DELETE", f"{GH_API}/repos/{self.repo}/releases/assets/{asset_id}")
        if r.status_code not in (200, 204, 404):
            raise SystemExit(f"delete {name} failed: {r.status_code}")
        log(f"  deleted old {name}")


# ---------------------------------------------------------------- 数据侧
class Unavailable(Exception):
    """该日数据源尚未发布（404）或行数明显不足，跳过等下次。"""


def read_kline_zip(raw: bytes) -> pl.DataFrame:
    """日度 1m klines zip -> 规范字段（含拆分的 buy_vol / sell_vol）。

    vision 的 CSV 表头不统一（部分文件带、部分不带），按首行首字段判断。
    """
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        name = [n for n in z.namelist() if n.endswith(".csv")][0]
        with z.open(name) as f:
            body = f.read()
    first = body[:200].decode("utf-8", "replace").split("\n")[0]
    has_header = first.split(",")[0].strip().strip('"') in KL_COLS
    df = pl.read_csv(io.BytesIO(body), has_header=has_header, new_columns=KL_COLS,
                     schema_overrides={"open_time": pl.Int64, "close_time": pl.Int64,
                                       "volume": pl.Float64,
                                       "taker_buy_volume": pl.Float64})
    # 与 download_binance_vision.py 口径一致：close_time + 1ms = 下一分钟起点
    # （原始值是 bar 结束 -1ms，如 23:59:59.999）；这样下游 `close_time - 60000`
    # 才能对齐订单簿的分钟 bucket，且每日最后一根的 close_time == 次日 00:00。
    df = df.with_columns(
        pl.when(pl.col("close_time") > 1e14)
        .then(pl.col("close_time") // 1000 + 1)
        .otherwise(pl.col("close_time") + 1)
        .cast(pl.Int64)
        .alias("close_time")
    )
    return df.select([
        pl.col("open_time").cast(pl.Int64),
        pl.col("close_time"),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
        pl.col("count").cast(pl.Int64),
        pl.col("taker_buy_volume").cast(pl.Float64),
        (pl.col("volume") - pl.col("taker_buy_volume")).alias("sell_vol"),
        pl.col("taker_buy_volume").alias("buy_vol"),
    ])


_local = threading.local()


def _session() -> requests.Session:
    """线程内复用连接：跨境访问 data.binance.vision 时 TLS 握手占了大头。"""
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        ad = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
        s.mount("https://", ad)
        _local.s = s
    return s


def _download(urls: dict[str, str], label: str, retries: int = 3) -> dict[str, bytes]:
    """下载同一标的/区间的 um + spot 两个 zip；404 视为源站未发布。"""
    raw = {}
    for market, url in urls.items():
        for i in range(retries):
            try:
                r = _session().get(url, timeout=300)
                if r.status_code == 404:
                    raise Unavailable(f"{market} {label} 未发布")
                r.raise_for_status()
                raw[market] = r.content
                break
            except Unavailable:
                raise
            except Exception as e:  # noqa: BLE001
                if i == retries - 1:
                    raise SystemExit(f"download failed: {url} ({e})")
                log(f"  retry {market} {label} {i + 1}/{retries}: {e}")
                time.sleep(3 * (i + 1))
    return raw


def _build_pair(raw: dict[str, bytes]) -> pl.DataFrame:
    """um+spot 原始 klines -> 15 列基础表（close_time = 下一分钟起点）。

    刻意不 import build_master_features：那边还在迭代 aggTrades/metrics 相关的列，
    而本管线的契约就是这 15 列，自己拼死比跟着上游变来变去更可靠。
    """
    um = (read_kline_zip(raw["um"])
          .select([pl.col("close_time"),
                   pl.col("open"), pl.col("high"), pl.col("low"), pl.col("close"),
                   pl.col("buy_vol").alias("um_buy_vol"),
                   pl.col("sell_vol").alias("um_sell_vol"),
                   pl.col("count").alias("um_count")])
          .unique(subset=["close_time"]))
    spot = (read_kline_zip(raw["spot"])
            .select([pl.col("close_time"),
                     pl.col("open").alias("spot_open"),
                     pl.col("high").alias("spot_high"),
                     pl.col("low").alias("spot_low"),
                     pl.col("close").alias("spot_close"),
                     pl.col("buy_vol").alias("spot_buy_vol"),
                     pl.col("sell_vol").alias("spot_sell_vol"),
                     pl.col("count").alias("spot_count")])
            .unique(subset=["close_time"]))
    return (um.join(spot, on="close_time", how="inner")
            .sort("close_time").select(BASE_COLS))


def fetch_day(symbol: str, d: date, retries: int = 3) -> pl.DataFrame:
    """下载某天的 um+spot 1m K 线，合成 15 列基础表。"""
    ds = d.isoformat()
    urls = {
        "um": f"{VISION}/futures/um/daily/klines/{symbol}/1m/{symbol}-1m-{ds}.zip",
        "spot": f"{VISION}/spot/daily/klines/{symbol}/1m/{symbol}-1m-{ds}.zip",
    }
    out = _build_pair(_download(urls, ds, retries))
    if out.height < MIN_ROWS_PER_DAY:
        raise Unavailable(f"{ds} 仅 {out.height} 行（< {MIN_ROWS_PER_DAY}）")
    return out


def fetch_month(symbol: str, year: int, month: int, retries: int = 3) -> pl.DataFrame:
    """下载整月的 um+spot 1m K 线（月度 zip，1 次请求换 ~30 次日度请求）。"""
    tag = f"{year}-{month:02d}"
    urls = {
        "um": f"{VISION}/futures/um/monthly/klines/{symbol}/1m/{symbol}-1m-{tag}.zip",
        "spot": f"{VISION}/spot/monthly/klines/{symbol}/1m/{symbol}-1m-{tag}.zip",
    }
    out = _build_pair(_download(urls, tag, retries))
    n_days = (date(year + (month == 12), month % 12 + 1, 1) - date(year, month, 1)).days
    if out.height < MIN_ROWS_PER_DAY * n_days:
        raise Unavailable(f"{tag} 仅 {out.height} 行（< {MIN_ROWS_PER_DAY * n_days}）")
    return out


def _fetch_plan(days: list[date]) -> list[tuple[str, object]]:
    """缺失区间里，能整月取的一律走月度文件（回填 7 年：84 个月 vs 2500 个日）。"""
    by_month: dict[tuple[int, int], list[date]] = {}
    for d in days:
        by_month.setdefault((d.year, d.month), []).append(d)
    today = datetime.now(timezone.utc).date()
    plan: list[tuple[str, object]] = []
    for (y, m), ds in sorted(by_month.items()):
        m_end = date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1)
        if len(ds) == m_end.day and m_end < today:      # 整月（且该月已结束）
            plan.append(("month", (y, m)))
        else:
            plan.extend(("day", d) for d in ds)
    return plan


def fetch_batch(symbol: str, days: list[date], jobs: int = 8
                ) -> tuple[list[tuple[int, pl.DataFrame]], list[str]]:
    """并发抓取多天，返回 ([(year, frame)], [缺口说明])；单个缺口不阻断其余区间。"""
    plan = _fetch_plan(days)
    n_month = sum(1 for k, _ in plan if k == "month")
    log(f"  {symbol}: {len(days)} 天 -> {len(plan)} 个请求"
        f"（其中整月 {n_month} 个）")

    def run(item):
        kind, arg = item
        return (fetch_month(symbol, *arg) if kind == "month" else fetch_day(symbol, arg))

    pairs, missing = [], []
    with ThreadPoolExecutor(max_workers=max(1, jobs)) as ex:
        futs = [(item, ex.submit(run, item)) for item in plan]
        for item, fut in futs:                 # 按计划顺序收集，年分组后仍会有序
            try:
                df = fut.result()
            except Unavailable as e:
                missing.append(str(e))
                continue
            year = item[1][0] if item[0] == "month" else item[1].year
            pairs.append((year, df))
    return pairs, missing


def _missing_days(df: pl.DataFrame, lo: date, hi: date) -> list[date]:
    """[lo, hi] 内 bar 数不足的日子（含整天缺失）——用来补中间缺口。

    只按首尾判断"已覆盖"是不够的：某天源站没发布、被跳过，区间首尾照样齐全，
    缺口就会永久留在年度文件里。
    """
    if df.height == 0:
        return _daterange(lo, hi)
    cnt = (df.select((pl.col("close_time") - 1)
                     .cast(pl.Datetime("ms")).dt.date().alias("d"))
             .group_by("d").len())
    have = dict(cnt.iter_rows())
    return [d for d in _daterange(lo, hi) if have.get(d, 0) < MIN_ROWS_PER_DAY]


def check_frame(df: pl.DataFrame, label: str) -> None:
    """合并后的硬校验：严格递增、无重复、无空值；断档只告警。"""
    if df.height == 0:
        raise SystemExit(f"{label}: 合并后为空")
    ct = df["close_time"]
    if ct.null_count():
        raise SystemExit(f"{label}: close_time 存在空值")
    if df.height != ct.n_unique():
        raise SystemExit(f"{label}: close_time 重复 {df.height - ct.n_unique()} 行")
    if not ct.is_sorted():
        raise SystemExit(f"{label}: close_time 未排序")
    gaps = ct.diff().drop_nulls()
    bad = gaps.filter(gaps != 60_000)
    if bad.len():
        log(f"  WARNING {label}: {bad.len()} 处非 60s 间隔（缺日或数据源断档）")


def publish_year(gh: GH, rid: int, assets: dict[str, int], symbol: str, year: int,
                 frames: list[pl.DataFrame],
                 cache: dict[str, pl.DataFrame] | None = None) -> tuple[int, int]:
    """把新数据并入该年文件并覆盖发布；返回 (行数, 最新 close_time)。

    cache 用于避免同一次运行里把同一个年度文件下载两遍（判断进度时已经读过一次）。
    """
    name = _yearly_name(symbol, year)
    base = (cache or {}).get(name)
    if base is None and name in assets:
        log(f"  {symbol} {year}: 下载现有 {name} ...")
        base = pl.read_parquet(io.BytesIO(gh.download(assets[name])))

    merged = pl.concat(([base] if base is not None else []) + frames, how="vertical_relaxed")
    merged = (merged.select(BASE_COLS)
              .unique(subset=["close_time"], keep="last")   # 新下的日子覆盖旧值
              .sort("close_time"))
    check_frame(merged, name)

    data = _parquet_bytes(merged)
    last = int(merged["close_time"][-1])
    log(f"  {symbol} {year}: {merged.height:,} 行 {merged.width} 列 "
        f"{len(data) / 1e6:.1f}MB，覆盖到 {_date_of_ms(last)}")
    if name in assets:
        gh.delete(assets[name], name)
        assets.pop(name, None)
    gh.upload(rid, name, data)
    return merged.height, int(merged["close_time"][-1])


def _years_present(assets: dict[str, int], sym: str) -> dict[int, int]:
    out = {}
    for name, aid in assets.items():
        m = YEARLY_RE.match(name)
        if m and m.group("sym") == sym:
            out[int(m.group("year"))] = aid
    return out


def _warn_legacy(assets: dict[str, int]) -> None:
    legacy = sorted(n for n in assets if MONTHLY_RE.match(n))
    if legacy:
        log(f"NOTE: release 里仍有 {len(legacy)} 个旧月度文件（与年度文件数据重叠，"
            f"glob `*_base_features_*` 会拿到双份）。确认年度文件齐全后跑 "
            f"`pipeline_daily.py prune-legacy --yes` 清掉。")


# ---------------------------------------------------------------- 命令
def cmd_update(args) -> int:
    gh = GH(args.repo)
    rid = gh.ensure_release(RELEASE_TAG, RELEASE_TITLE)
    assets = gh.assets(rid)
    _warn_legacy(assets)

    target = _parse_date(args.date) if args.date else _utc_yesterday()
    log(f"目标日期 {target}（默认 UTC 昨天）")

    info: dict[str, dict] = {}
    for sym in _symbols(args):
        years = _years_present(assets, sym)
        if not years:
            # 首次运行 / 年度文件被清空：自动回填一次，后续就只走增量
            log(f"{sym}: 尚无年度文件，自动回填 {DEFAULT_START} ~ {target}")
            _backfill_symbol(gh, rid, assets, sym, DEFAULT_START, target, args.jobs)
            assets = gh.assets(rid)
            years = _years_present(assets, sym)
            if not years:
                raise SystemExit(f"{sym}: 回填后仍无年度文件，检查凭据与数据源")

        last_year = max(years)
        old = pl.read_parquet(io.BytesIO(gh.download(years[last_year])))
        cache = {_yearly_name(sym, last_year): old}
        last_ct = int(old["close_time"].max())
        last_day = _date_of_ms(last_ct)
        # 该日若已收满（最后一根 bar 的 close_time == 次日 00:00），从次日续；否则重下该日
        covered_to = (last_day + timedelta(days=1)
                      if last_ct >= _day_last_ms(last_day) else last_day)
        info[sym] = {"years": sorted(years), "latest": last_ct, "touched": {}}

        if covered_to > target:
            log(f"{sym}: 已覆盖到 {last_day}，无需更新")
            continue

        days = _daterange(covered_to, target)
        log(f"{sym}: 补 {len(days)} 天 {days[0]} ~ {days[-1]}")
        pairs, missing = fetch_batch(sym, days, args.jobs)
        for m in missing:
            log(f"  SKIP {m}（尚未发布，下次重试）")
        if not pairs:
            log(f"{sym}: 本轮没有可并入的新数据")
            continue

        by_year: dict[int, list[pl.DataFrame]] = {}
        for year, df in pairs:
            by_year.setdefault(year, []).append(df)
        for year, frames in sorted(by_year.items()):
            rows, last = publish_year(gh, rid, assets, sym, year, frames, cache)
            info[sym]["touched"][year] = (rows, last)
            info[sym]["latest"] = max(info[sym]["latest"], last)
            if year not in info[sym]["years"]:
                info[sym]["years"].append(year)
        info[sym]["years"] = sorted(info[sym]["years"])

    STATUS_FILE.write_text(status_md(target, info), encoding="utf-8")
    log(f"{STATUS_FILE.name} 已更新")
    return 0


def _backfill_symbol(gh: GH, rid: int, assets: dict[str, int], sym: str,
                     start: date, end: date, jobs: int) -> None:
    """回填单个标的：按年聚合，每年只上传一次（避免逐日重传同一个文件）。"""
    for year in range(start.year, end.year + 1):
        lo = max(start, date(year, 1, 1))
        hi = min(end, date(year, 12, 31))
        name = _yearly_name(sym, year)
        cache: dict[str, pl.DataFrame] = {}
        if name in assets:
            have = pl.read_parquet(io.BytesIO(gh.download(assets[name])))
            cache[name] = have
            days = _missing_days(have, lo, hi)
            if not days:
                log(f"{sym} {year}: {lo} ~ {hi} 已完整，跳过")
                continue
            log(f"{sym} {year}: 已有文件缺 {len(days)} 天，补 {days[0]} ~ {days[-1]}")
        else:
            days = _daterange(lo, hi)
            log(f"{sym} {year}: 新建，下载 {len(days)} 天 {lo} ~ {hi}")

        pairs, missing = fetch_batch(sym, days, jobs)
        for m in missing:
            log(f"  SKIP {m}")
        if not pairs:
            log(f"{sym} {year}: 无数据，跳过")
            continue
        publish_year(gh, rid, assets, sym, year, [df for _, df in pairs], cache)


def cmd_backfill(args) -> int:
    """手动回填/修复区间（首次用；也可用来补历史缺口）。"""
    if not args.start:
        raise SystemExit("backfill 需要 --start YYYY-MM-DD")
    gh = GH(args.repo)
    rid = gh.ensure_release(RELEASE_TAG, RELEASE_TITLE)
    assets = gh.assets(rid)
    start = _parse_date(args.start)
    end = _parse_date(args.end) if args.end else _utc_yesterday()
    if end < start:
        raise SystemExit(f"--end {end} 早于 --start {start}")

    for sym in _symbols(args):
        _backfill_symbol(gh, rid, assets, sym, start, end, args.jobs)
    log("backfill 完成")
    return 0


def cmd_prune_legacy(args) -> int:
    """删除旧月度资产。默认只列出；确认年度文件齐全后再加 --yes。"""
    gh = GH(args.repo)
    rid = gh.ensure_release(RELEASE_TAG, RELEASE_TITLE)
    assets = gh.assets(rid)
    legacy = sorted(n for n in assets if MONTHLY_RE.match(n))
    if not legacy:
        log("没有旧月度文件")
        return 0

    years_by_sym = {s: set(_years_present(assets, s)) for s in
                    {MONTHLY_RE.match(n).group("sym") for n in legacy}}
    doomed, keep = [], []
    for n in legacy:
        m = MONTHLY_RE.match(n)
        year = int(m.group("ym")[:4])
        (doomed if year in years_by_sym[m.group("sym")] else keep).append(n)

    for n in doomed:
        log(f"  待删 {n}")
    for n in keep:
        log(f"  保留 {n}（对应 {MONTHLY_RE.match(n).group('ym')[:4]} 年年度文件还不存在）")
    if not args.yes:
        log(f"共 {len(doomed)} 个可删。加 --yes 执行。")
        return 0
    for n in doomed:
        gh.delete(assets[n], n)
    log(f"已删除 {len(doomed)} 个旧月度文件")
    return 0


# ---------------------------------------------------------------- 状态文件
def status_md(target: date, info: dict[str, dict]) -> str:
    lines = [
        "# 数据状态", "",
        "> 本文件由 `binance_vision/pipeline_daily.py` 每天自动生成并提交，请勿手改。", "",
        f"- 最近一次运行：{datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC",
        f"- 目标日期：{target}",
        "",
    ]
    for sym, v in info.items():
        lines += [f"## {sym}", ""]
        if v["years"]:
            ys = "、".join(str(y) for y in v["years"])
            lines.append(f"- 年度文件：{len(v['years'])} 个（{ys}）")
        lines.append(f"- 最新覆盖日期：{_date_of_ms(v['latest'])}"
                     f"（最后一行 close_time = {_fmt_ms(v['latest'])} UTC）")
        for year, (rows, last) in sorted(v["touched"].items()):
            lines.append(f"- 本次写入 `{sym}_base_features_{year}.parquet`："
                         f"{rows:,} 行，覆盖到 {_date_of_ms(last)}")
        lines.append("")

    lines += [
        "## 取数", "",
        "按年一个 parquet，列结构一致、时间不重叠，直接 scan 拼接：", "",
        "```bash",
        "python tools/fetch_release.py --tag vision-base \\",
        "    --pattern 'BTCUSDT_base_features_*.parquet' --dest ./data/vision",
        "```", "",
        "```python",
        "import polars as pl",
        "from pathlib import Path",
        "lf = pl.scan_parquet(sorted(Path('./data/vision').glob('BTCUSDT_base_features_*.parquet')))",
        "```", "",
        "当年那份文件仍在增长（跨年后冻结），不要假定它的大小；"
        "完整 schema 与时间口径见 `DATA_ACCESS.md`。", "",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------- 入口
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=("update", "backfill", "prune-legacy"))
    ap.add_argument("--repo", default="bailiang0369/bn_hisdata")
    ap.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    ap.add_argument("--date", default=None, help="update: 补到哪一天（YYYY-MM-DD）")
    ap.add_argument("--start", default=None, help="backfill: 起始日（YYYY-MM-DD）")
    ap.add_argument("--end", default=None, help="backfill: 结束日，默认 UTC 昨天")
    ap.add_argument("--jobs", type=int, default=8, help="并发下载天数")
    ap.add_argument("--yes", action="store_true", help="prune-legacy: 真正执行删除")
    args = ap.parse_args()

    if args.cmd == "update":
        return cmd_update(args)
    if args.cmd == "backfill":
        return cmd_backfill(args)
    return cmd_prune_legacy(args)


if __name__ == "__main__":
    raise SystemExit(main())
