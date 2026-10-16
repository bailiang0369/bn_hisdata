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
"""
import polars as pl


def _synthesize_removals(base: pl.DataFrame, snaps: pl.DataFrame, snap_times: list[int]) -> list[pl.DataFrame]:
    if not snap_times:
        return []
    base_sorted = base.sort(["side", "px", "event_time"])
    removal_frames = []
    for Ts in snap_times:
        before = base_sorted.filter(pl.col("event_time") < Ts)
        if before.height == 0:
            continue
        alive = (
            before.group_by(["side", "px"])
            .last()
            .filter(pl.col("qty") > 0)
            .select(["side", "px"])
        )
        in_snap = snaps.filter(pl.col("event_time") == Ts).select(["side", "px"])
        rem = alive.join(in_snap, on=["side", "px"], how="anti")
        rem = rem.with_columns([
            pl.lit(0.0).cast(pl.Float64).alias("qty"),
            pl.lit(Ts).cast(pl.Int64).alias("event_time"),
        ])
        removal_frames.append(rem)
    return removal_frames


def reconstruct_state(df_raw: pl.DataFrame) -> pl.DataFrame:
    """Return the full per-(side, px, minute-bucket) book state.

    Output columns: side (str), px (f64), bucket (i64, minute-ms), q (f32).
    `q` is the resting quantity at the start of each minute bucket (last change
    on/before that bucket), with qty=0/removed levels forward-filled to None
    (filtered out by callers via q>0).

    `q` is deliberately Float32: a resting quantity is not an identifier, and at
    BTC magnitudes the relative error is ~6e-8 (irrelevant for features), while
    band is by far the largest table -- this one column is ~1/3 of its size.
    `px` MUST stay Float64: it identifies a price level (tick 0.01 spot / 0.1
    futures) and above 131072 float32's ULP (0.0156) exceeds the spot tick, so
    adjacent levels would collapse into one value and corrupt the book.
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
    removal_frames = _synthesize_removals(base, snaps, snap_times)
    if removal_frames:
        base = pl.concat([base] + removal_frames, how="diagonal")

    base = base.with_columns(
        ((pl.col("event_time") // 60000) * 60000).cast(pl.Int64).alias("bucket")
    )
    agg = base.group_by(["side", "px", "bucket"]).agg(
        pl.col("qty").sort_by("event_time").last().alias("q")
    )
    keys = agg.select(["side", "px"]).unique()
    bvals = sorted(agg["bucket"].unique().to_list())
    grid = keys.join(pl.DataFrame({"bucket": bvals}), how="cross")
    merged = grid.join(agg, on=["side", "px", "bucket"], how="left")
    merged = merged.sort(["side", "px", "bucket"]).with_columns(
        pl.col("q").fill_null(strategy="forward").over(["side", "px"])
    )
    # q -> Float32（省 band 表约 1/3 体积）；px 保持 Float64，原因见函数文档。
    return merged.with_columns(pl.col("q").cast(pl.Float32))


def reconstruct_state_sparse(df_raw: pl.DataFrame, band: float = 0.003,
                             safety: float = 0.02) -> pl.DataFrame:
    """Memory-bounded variant of :func:`reconstruct_state`.

    A level can only ever appear inside the +-band around mid if its price lies
    in the (conservative) window ``[(1-band)*mid_lo, (1+band)*mid_hi]`` where
    ``mid_lo/mid_hi`` are the file's extreme mids. We derive a *conservative*
    mid range from the file's snapshot mids, then widen it by a small margin
    that covers intra-file price swings (absolute ``safety`` of the mid, plus a
    fraction of the mid span) so we never clip a real band level. Every price
    outside that window is dropped BEFORE the as-of, so far resting levels
    (which exist at every price depth) are removed up front and the
    (side, px) x bucket grid stays small enough for a daily file.

    Output matches :func:`reconstruct_state` for every band-level row, provided
    the margin covers the file's true mid swing (raise ``safety`` for wild days).
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
    removal_frames = _synthesize_removals(base, snaps, snap_times)
    if removal_frames:
        base = pl.concat([base] + removal_frames, how="diagonal")

    # conservative mid window from snapshot mids (+ safety margin)
    snap_mids = []
    for Ts in snap_times:
        s = snaps.filter(pl.col("event_time") == Ts)
        bb = s.filter(pl.col("side") == "bid")["px"].max()
        ba = s.filter(pl.col("side") == "ask")["px"].min()
        if bb is not None and ba is not None:
            snap_mids.append((bb + ba) / 2)
    if snap_mids:
        mid_lo, mid_hi = min(snap_mids), max(snap_mids)
        span = mid_hi - mid_lo
        # margin covers both intra-file swings and snapshot gaps:
        # a small absolute fraction of mid, and a fraction of the mid span
        margin = max(safety * mid_hi, 0.05 * span)
        # 窗口由 band 推导（不再写死 0.997/1.003）：保证绝不会把目标带内的档位提前
        # 裁掉。band=0.003 时即 ±0.3%，与 process_hour 最终的 band 过滤完全一致；
        # 若以后换更宽的 band，这里也随之放大，不会 silently 漏档位。
        win_lo = (1 - band) * mid_lo - margin
        win_hi = (1 + band) * mid_hi + margin
        base = base.filter(pl.col("px").is_between(win_lo, win_hi))

    base = base.with_columns(
        ((pl.col("event_time") // 60000) * 60000).cast(pl.Int64).alias("bucket")
    )
    agg = base.group_by(["side", "px", "bucket"]).agg(
        pl.col("qty").sort_by("event_time").last().alias("q")
    )
    keys = agg.select(["side", "px"]).unique()
    bvals = sorted(agg["bucket"].unique().to_list())
    grid = keys.join(pl.DataFrame({"bucket": bvals}), how="cross")
    merged = grid.join(agg, on=["side", "px", "bucket"], how="left")
    merged = merged.sort(["side", "px", "bucket"]).with_columns(
        pl.col("q").fill_null(strategy="forward").over(["side", "px"])
    )
    # 与 dense 版一致：q -> Float32，px 保持 Float64
    return merged.with_columns(pl.col("q").cast(pl.Float32))
