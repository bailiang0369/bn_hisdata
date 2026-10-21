"""
Order-book reconstruction via vectorized as-of (single pass, no replay).

Key correctness point:
    Binance's local-depth diff stream sends snapshots on (re)connect. A snapshot
    is a FULL book replacement, not an incremental update. So when a snapshot
    arrives at time Ts, every level that was alive just before Ts but is ABSENT
    from the snapshot must be treated as removed at Ts. The naive "snapshot =
    set the levels it lists" approach leaks stale levels from a previous snapshot
    (the bug that corrupted files containing >1 snapshot / feed reconnects).

This module implements the correct behaviour by synthesizing qty=0 removal
records for exactly those levels, then doing a single as-of last-value groupby.

Two expansion strategies (2026-10-21):
  * dense  -- the full (side, px) x bucket grid (the caveat-free reference);
  * sparse -- per-bucket pair expansion: only the (level, bucket) pairs whose
    price lies inside that bucket's provably-safe window are materialized
    (~12% of the grid on real hours), via a range join + one as-of merge.
    A post-hoc self-check (every bounded bucket yields a mid, and no alive
    level sits in the strip between the band and window edges) falls back to
    the dense path whenever the bound cannot be confirmed -- the fast path
    is either provably equivalent to dense or unused.
"""
import warnings

import polars as pl


def _prepare_base(df_raw: pl.DataFrame) -> pl.DataFrame:
    """Cast + split snapshot/update rows + synthesize snapshot removals.

    Returns the event base (side, px, qty, event_time) with, for every snapshot
    time Ts, qty=0 removal records for the levels alive just before Ts but
    absent from that snapshot (full-book-replacement semantics).
    """
    df = df_raw.with_columns([
        pl.col("price").cast(pl.Float64).alias("px"),
        pl.col("quantity").cast(pl.Float64).alias("qty"),
    ])
    snaps = df.filter(pl.col("event_type") == "snapshot").filter(pl.col("qty") > 0)
    upds = df.filter(pl.col("event_type") == "update")
    snap_times = sorted(snaps["event_time"].unique().to_list())

    snap_present = snaps.select(["side", "px", "qty", "event_time"])
    base = pl.concat(
        [upds.select(["side", "px", "qty", "event_time"]), snap_present],
        how="diagonal",
    )

    # synthesize removals for levels alive just before each snapshot but absent
    removal_frames = []
    for Ts in snap_times:
        before = base.filter(pl.col("event_time") < Ts)
        if before.height == 0:
            continue
        # 注意不要在这里先 sort(["side","px","event_time"])：agg 里的
        # sort_by("event_time").last() 已经按 event_time 取每档最后值，
        # 外层三键排序是纯浪费（实测让这一步慢 7 倍：2.8s -> 0.4s）。
        alive = (
            before.group_by(["side", "px"])
            .agg(pl.col("qty").sort_by("event_time").last().alias("qv"))
            .filter(pl.col("qv") > 0)
            .select(["side", "px"])
        )
        in_snap = snaps.filter(pl.col("event_time") == Ts).select(["side", "px"])
        rem = alive.join(in_snap, on=["side", "px"], how="anti")
        rem = rem.with_columns([
            pl.lit(0.0).cast(pl.Float64).alias("qty"),
            pl.lit(Ts).cast(pl.Int64).alias("event_time"),
        ])
        removal_frames.append(rem)
    if removal_frames:
        base = pl.concat([base] + removal_frames, how="diagonal")
    return base


def _bucket_windows(base: pl.DataFrame, band: float) -> pl.DataFrame | None:
    """Per-bucket rigorous bounds on the minute's mid, from touched prices.

    Bucket b's state is the state after the LAST message in [b, b+60s); that
    message's max touched bid (qty>0) is a live level at that instant, hence
    best_bid >= it and mid >= best_bid; symmetrically mid <= best_ask <= its
    min touched ask. Therefore mid(b) in [tb_b, ta_b] and every band level
    (px within +-band of mid(b)) lies in [(1-band)*tb_b, (1+band)*ta_b].

    Per side, the bound is taken from the last message OF THAT MINUTE that
    touched the side, then forward-filled across buckets that did not touch
    the side (no touch since -> the best price cannot have moved up; it can
    only have dropped via a removal, which is the documented gap caveat and
    is caught by _pairs_self_verified when it moves the mid out of bounds).

    Returns (bucket, tb, ta, lo, hi) or None when no qty>0 rows exist at all.
    """
    touched = base.filter(pl.col("qty") > 0)
    if touched.height == 0:
        return None
    msg = touched.group_by("event_time").agg(
        pl.col("px").filter(pl.col("side") == "bid").max().alias("tb"),
        pl.col("px").filter(pl.col("side") == "ask").min().alias("ta"),
    ).with_columns(
        ((pl.col("event_time") // 60000) * 60000).cast(pl.Int64).alias("bucket")
    )
    all_buckets = pl.DataFrame({"bucket": sorted(msg["bucket"].unique().to_list())})
    gov_bid = msg.filter(pl.col("tb").is_not_null()).group_by("bucket").agg(
        pl.col("tb").sort_by("event_time").last().alias("tb")
    )
    gov_ask = msg.filter(pl.col("ta").is_not_null()).group_by("bucket").agg(
        pl.col("ta").sort_by("event_time").last().alias("ta")
    )
    win = (
        all_buckets.join(gov_bid, on="bucket", how="left")
        .join(gov_ask, on="bucket", how="left")
        .sort("bucket")
        .with_columns(pl.col("tb").forward_fill(), pl.col("ta").forward_fill())
        .filter(pl.col("tb").is_not_null() & pl.col("ta").is_not_null())
    )
    if win.height == 0:
        return None
    return win.select(
        "bucket", "tb", "ta",
        ((1 - band) * pl.col("tb")).alias("lo"),
        ((1 + band) * pl.col("ta")).alias("hi"),
    )


def _agg_keys(base: pl.DataFrame):
    """(agg, keys): last qty per (sc, px, bucket) and the distinct levels."""
    b2 = base.with_columns([
        ((pl.col("event_time") // 60000) * 60000).cast(pl.Int64).alias("bucket"),
        pl.when(pl.col("side") == "bid").then(0)
        .when(pl.col("side") == "ask").then(1).otherwise(2)
        .cast(pl.Int8).alias("sc"),
    ])
    agg = b2.group_by(["sc", "px", "bucket"]).agg(
        pl.col("qty").sort_by("event_time").last().alias("q")
    )
    keys = agg.select(["sc", "px"]).unique().sort(["sc", "px"])
    return agg, keys


def _asof(left_sorted: pl.DataFrame, agg: pl.DataFrame) -> pl.DataFrame:
    """join_asof(backward): each left row takes the agg row with the largest
    bucket' <= its own bucket, per (sc, px) group."""
    with warnings.catch_warnings():
        # by= 时 polars 不自行校验左侧有序性（只 warning）；调用方保证。
        warnings.filterwarnings("ignore", message="Sortedness of columns")
        return left_sorted.join_asof(
            agg.sort(["sc", "px", "bucket"]),
            left_on="bucket", right_on="bucket", by=["sc", "px"], strategy="backward",
        )


def _decode(merged: pl.DataFrame) -> pl.DataFrame:
    """q -> Float32 + sc -> side string, canonical column order."""
    return merged.with_columns([
        pl.col("q").cast(pl.Float32),
        pl.when(pl.col("sc") == 0).then(pl.lit("bid"))
        .when(pl.col("sc") == 1).then(pl.lit("ask"))
        .otherwise(pl.lit("?")).alias("side"),
    ]).select(["side", "px", "bucket", "q"])


def _grid_dense(agg: pl.DataFrame, keys: pl.DataFrame) -> pl.DataFrame:
    """The full (sc, px) x bucket grid + as-of expansion (dense path)."""
    bvals = sorted(agg["bucket"].unique().to_list())
    grid = keys.join(pl.DataFrame({"bucket": bvals}), how="cross")
    if not grid.select(
        pl.struct(["sc", "px", "bucket"]).alias("k")
    ).to_series().is_sorted():
        grid = grid.sort(["sc", "px", "bucket"])
    return _asof(grid, agg)


def _pairs_self_verified(merged: pl.DataFrame, win: pl.DataFrame,
                         band: float) -> bool:
    """Post-hoc check that the pair expansion cannot have clipped anything.

    Two conditions, both cheap:
      1. every bucket that has a rigorous bound must yield a mid -- a bucket
         missing here means that minute lost an entire side to the window
         (full gap: the best price dropped below the window edge), and it
         also implies the computed best bid/ask ARE the true ones;
      2. no ALIVE level sits in the strips between the band edges and the
         window edges -- that is the exact condition for "the band filter
         cannot have missed a level", checked directly instead of via the
         (stricter, false-positive-prone) proxy mid in [tb, ta].
    Either failing -> the caller falls back to the dense path.
    """
    q = merged.filter(pl.col("q") > 0)
    bb = q.filter(pl.col("sc") == 0).group_by("bucket").agg(
        pl.col("px").max().alias("best_bid"))
    ba = q.filter(pl.col("sc") == 1).group_by("bucket").agg(
        pl.col("px").min().alias("best_ask"))
    mb = bb.join(ba, on="bucket", how="inner").with_columns(
        ((pl.col("best_bid") + pl.col("best_ask")) / 2).alias("mid"))
    if mb.height != win.height:
        return False
    m2 = merged.join(win.select(["bucket", "lo", "hi"]), on="bucket", how="inner") \
        .join(mb.select(["bucket", "mid"]), on="bucket", how="inner")
    # 注意 (pl.col("q") > 0) 的括号不能省：Python 里 & 的优先级高于比较
    # 运算符，写成 q > 0 & X 会被解析成 q > (0 & X)（恒为 q > 0，全部通过），
    # 这个自校验就废了。build_minute_snapshots 的 band 过滤同样依赖括号。
    strip = m2.filter(
        (pl.col("q") > 0)
        & (((pl.col("px") >= pl.col("mid") * (1 - band)) & (pl.col("px") < pl.col("lo")))
           | ((pl.col("px") > pl.col("hi")) & (pl.col("px") <= pl.col("mid") * (1 + band))))
    )
    return strip.height == 0


def _grid_pairs(base: pl.DataFrame, band: float) -> pl.DataFrame | None:
    """Per-bucket pair expansion (sparse path). Returns None to signal
    "could not prove equivalence -- caller must use the dense path".

    Measured on real reconnect hours: ~614k pairs vs ~4.97M grid rows (12%),
    band output identical to dense. The covering interval (union of the
    per-bucket windows) pre-filters the events so the agg and the key set
    shrink too; it cannot affect rigor because every window is inside it.
    """
    win = _bucket_windows(base, band)
    if win is None:
        return None
    cover_lo, cover_hi = win["lo"].min(), win["hi"].max()
    if cover_lo is None or cover_hi is None:
        return None
    base_f = base.filter(pl.col("px").is_between(cover_lo, cover_hi))
    agg, keys = _agg_keys(base_f)
    if keys.height == 0:
        return None
    pairs = keys.join_where(
        win, pl.col("px") >= pl.col("lo"), pl.col("px") <= pl.col("hi")
    ).select(["sc", "px", "bucket"])
    if pairs.height == 0:
        return None
    merged = _asof(pairs.sort(["sc", "px", "bucket"]), agg)
    if not _pairs_self_verified(merged, win, band):
        return None
    return _decode(merged)


def reconstruct_state(df_raw: pl.DataFrame) -> pl.DataFrame:
    """Return the full per-(side, px, minute-bucket) book state (dense).

    Output columns: side (str), px (f64), bucket (i64, minute-ms), q (f32).
    See _decode / _grid_dense for semantics and the dtype rationale
    (q is Float32 to shrink the band table ~1/3; px MUST stay Float64 because
    it identifies a price level and f32's ULP exceeds the tick above 131072).
    """
    base = _prepare_base(df_raw)
    agg, keys = _agg_keys(base)
    return _decode(_grid_dense(agg, keys))


def reconstruct_state_sparse(df_raw: pl.DataFrame, band: float = 0.003) -> pl.DataFrame:
    """Memory-bounded variant of :func:`reconstruct_state` (sparse).

    Materializes only the (level, bucket) pairs inside each bucket's
    provably-safe window (~12% of the dense grid on real hours) and falls
    back to the dense path whenever the post-hoc self-check cannot confirm
    the bounds -- so the output equals the dense reconstruction for every
    band-level row, or the hour is rebuilt densely.
    """
    base = _prepare_base(df_raw)
    out = _grid_pairs(base, band)
    if out is not None:
        return out
    agg, keys = _agg_keys(base)
    return _decode(_grid_dense(agg, keys))
