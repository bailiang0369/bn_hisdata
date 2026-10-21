"""
Unit tests for orderbook/ob_reconstruct.py

合成数据回归网，覆盖两个曾真实发生/潜伏的问题：
  1. 快照 = 整书替换（仓库 docs/orderbook_lessons.md §1 的大 bug）：
     重连快照里不存在的档位（含深档位）必须被移除，不能从旧快照泄漏；
  2. sparse 预滤窗口的可证明安全性（2026-10-20 修复）：mid 大幅摆动时
     sparse 与 dense 的带内结果必须完全一致，任何一分钟都不许被裁空。
"""
import polars as pl

from ob_reconstruct import (_grid_pairs, _prepare_base, reconstruct_state,
                            reconstruct_state_sparse)

T0 = 1700000040000  # 60000 的整数倍，一个整点边界
BAND = 0.003


def _df(events):
    """events: (event_time_ms, event_type, side, price, qty)。

    price/quantity 用字符串——与真实文件一致（原始 parquet 里这两列是 String）。
    """
    return pl.DataFrame({
        "side": [e[2] for e in events],
        "price": [str(e[3]) for e in events],
        "quantity": [str(e[4]) for e in events],
        "event_time": [e[0] for e in events],
        "event_type": [e[1] for e in events],
    })


def _q(merged: pl.DataFrame, side: str, px: float, bucket: int) -> float:
    row = merged.filter(
        (pl.col("side") == side) & (pl.col("px") == px) & (pl.col("bucket") == bucket)
    )
    assert row.height == 1, f"missing grid row: {side} px={px} bucket={bucket}"
    return row["q"][0]


def _band_rows(m: pl.DataFrame, bucket: int, mid: float, band: float) -> pl.DataFrame:
    return m.filter(
        (pl.col("bucket") == bucket)
        & pl.col("px").is_between(mid * (1 - band), mid * (1 + band))
        & (pl.col("q") > 0)
    ).select(["side", "px", "q"])


def _mids(m: pl.DataFrame) -> list[dict]:
    q = m.filter(pl.col("q") > 0)
    bb = q.filter(pl.col("side") == "bid").group_by("bucket").agg(
        pl.col("px").max().alias("best_bid"))
    ba = q.filter(pl.col("side") == "ask").group_by("bucket").agg(
        pl.col("px").min().alias("best_ask"))
    return bb.join(ba, on="bucket", how="inner").with_columns(
        ((pl.col("best_bid") + pl.col("best_ask")) / 2).alias("mid")
    ).iter_rows(named=True)


# ---------------------------------------------------------------- §1 回归

def test_snapshot_is_full_book_replacement():
    """重连快照（限深）必须整书替换：快照里没有的档位一律移除。

    旧实现把快照当增量，深档位（bid 50 / ask 150）会从旧快照永久泄漏。
    """
    events = [
        (T0, "snapshot", "bid", 100.0, 5.0),
        (T0, "snapshot", "bid", 99.0, 3.0),
        (T0, "snapshot", "bid", 50.0, 1.0),   # 深档位：旧 bug 的泄漏重灾区
        (T0, "snapshot", "ask", 101.0, 4.0),
        (T0, "snapshot", "ask", 150.0, 1.0),
        (T0 + 10_000, "update", "bid", 100.0, 6.0),
        # 重连快照：100.0 / 50.0 / 150.0 均不在其中
        (T0 + 20_000, "snapshot", "bid", 99.0, 7.0),
        (T0 + 20_000, "snapshot", "ask", 101.0, 6.0),
        (T0 + 30_000, "update", "bid", 99.5, 2.0),
    ]
    m = reconstruct_state(_df(events))
    assert _q(m, "bid", 100.0, T0) == 0.0   # 旧快照有、新快照无 -> 移除
    assert _q(m, "bid", 50.0, T0) == 0.0    # 深档位同样移除（naive 实现留下 1.0）
    assert _q(m, "ask", 150.0, T0) == 0.0
    assert _q(m, "bid", 99.0, T0) == 7.0    # 新快照的值生效
    assert _q(m, "bid", 99.5, T0) == 2.0
    assert _q(m, "ask", 101.0, T0) == 6.0


def test_snapshot_removal_lands_in_right_bucket():
    """跨分钟的快照替换：移除事件归属第二个快照所在的分钟桶。"""
    events = [
        (T0, "snapshot", "bid", 100.0, 5.0),
        (T0, "snapshot", "bid", 98.0, 2.0),
        (T0, "snapshot", "ask", 101.0, 4.0),
        (T0 + 70_000, "snapshot", "bid", 100.0, 5.0),  # 98.0 缺席 -> 移除
        (T0 + 70_000, "snapshot", "ask", 101.0, 4.0),
    ]
    m = reconstruct_state(_df(events))
    assert _q(m, "bid", 98.0, T0) == 2.0
    assert _q(m, "bid", 98.0, T0 + 60_000) == 0.0  # t0+70s 属于第二个桶
    assert _q(m, "bid", 100.0, T0 + 60_000) == 5.0


def test_forward_fill_and_explicit_removal():
    """qty=0 的 update 是显式删除；无变更的档位按桶前向填充。

    注意：网格只覆盖"有事件的分钟"（bvals 来自 agg），所以中间必须放一个
    事件才能检验跨桶填充——真实数据每分钟都有成千上万事件，不会遇到空桶。
    """
    events = [
        (T0, "snapshot", "bid", 100.0, 5.0),
        (T0, "snapshot", "ask", 101.0, 4.0),
        (T0 + 70_000, "update", "ask", 101.0, 4.0),   # 第 2 分钟一个事件，占住桶
        (T0 + 200_000, "update", "bid", 100.0, 0.0),  # 第 4 分钟删除
    ]
    m = reconstruct_state(_df(events))
    for b in (T0, T0 + 60_000):
        assert _q(m, "bid", 100.0, b) == 5.0          # 前向填充
        assert _q(m, "ask", 101.0, b) == 4.0
    assert _q(m, "bid", 100.0, T0 + 180_000) == 0.0   # 删除生效
    assert _q(m, "ask", 101.0, T0 + 180_000) == 4.0   # ask 不受影响


# ------------------------------------------------- sparse 窗口安全性回归

def _swing_events(n_minutes: int = 6, step: float = 2.0) -> list:
    """mid 一小时摆动 n*step 个点的深簿文件。

    初始深快照（bid 89..50 / ask 101..150）+ 每分钟把顶档下移 step（旧顶
    显式删除，否则 best bid 不会下移）。深簿必须整体位于摆动区间之外——
    若深快照里有 97 这种档位，顶档下移到 96 后它反而变成 best bid，盘口
    交叉、mid 失真，那是数据设计错误不是代码 bug。

    顶档价差固定 0.2，保证 mid±0.3% 的带内始终有档位（用整数顶档时
    ±0.3% < 半个价差，末分钟会出现空带）。

    mid 从 100.1 一路到 100.1-2*5=90.1，摆动 10%——旧版"快照 mid ± 2%"
    窗口（约 [92.8, 97.1]）会裁空最后两分钟。
    """
    events = []
    for i in range(40):
        events.append((T0, "snapshot", "bid", float(89 - i), 1.0))
    for i in range(50):
        events.append((T0, "snapshot", "ask", float(101 + i), 1.0))
    for m in range(n_minutes):
        t = T0 + m * 60_000 + 1_000
        top_bid, top_ask = 100.0 - step * m, 100.2 - step * m
        if m > 0:  # 删除上一分钟的顶档（含 ±0.1 的伴生档）
            for d in (0.0, 0.1):
                events.append((t, "update", "bid", float(top_bid + step - d), 0.0))
                events.append((t, "update", "ask", float(top_ask + step + d), 0.0))
        # 新顶档 + 各一侧一个伴生档，让带内不止 2 行、断言更扎实
        events.append((t, "update", "bid", float(top_bid), 5.0))
        events.append((t, "update", "bid", float(top_bid - 0.1), 1.0))
        events.append((t, "update", "ask", float(top_ask), 4.0))
        events.append((t, "update", "ask", float(top_ask + 0.1), 1.0))
    return events


def test_sparse_never_clips_band_levels_on_big_mid_swing():
    """逐分钟断言：dense 带内档位必须全部出现在 sparse 中（一个都不许裁）。"""
    df = _df(_swing_events())
    dense = reconstruct_state(df)
    sparse = reconstruct_state_sparse(df, band=BAND)
    assert _mids(dense), "synthetic file must produce per-minute mids"
    for row in _mids(dense):
        d = _band_rows(dense, row["bucket"], row["mid"], BAND)
        s = _band_rows(sparse, row["bucket"], row["mid"], BAND)
        assert d.height > 0, f"bucket {row['bucket']} has no band levels at all"
        assert s.equals(d), f"sparse clipped band levels in bucket {row['bucket']}"


def test_sparse_rows_are_subset_of_dense():
    """预滤只允许丢"永远不可能进带内"的行：sparse 的每个 (side,px,bucket)
    都必须能在 dense 里找到，且 q 一致。

    注意不能用 left join 后数 q_d 的 null 来判断"无匹配"——匹配到但 dense
    该行 q 本身就是 null（档位尚未出现的早期桶）时 q_d 也是 null；真正的
    子集校验要用 anti join。
    """
    df = _df(_swing_events())
    dense = reconstruct_state(df)
    sparse = reconstruct_state_sparse(df, band=BAND)
    assert sparse.height < dense.height, "prefilter did not shrink the grid"
    assert sparse.join(dense, on=["side", "px", "bucket"], how="anti").height == 0, \
        "sparse produced rows absent from dense"
    both = sparse.join(dense, on=["side", "px", "bucket"], how="inner",
                       suffix="_d").filter(
        pl.col("q").is_not_null() & pl.col("q_d").is_not_null())
    assert (both["q"] - both["q_d"]).abs().max() <= 1e-6


def test_sparse_single_sided_falls_back_to_dense():
    """只有一侧的触及价时 mid 界无法定义 -> 不预滤，结果与 dense 一致。"""
    events = [
        (T0, "snapshot", "bid", 100.0, 5.0),
        (T0 + 10_000, "update", "bid", 99.0, 3.0),
    ]
    df = _df(events)
    assert reconstruct_state_sparse(df, band=BAND).equals(reconstruct_state(df))


def test_sparse_pairs_path_is_actually_used():
    """快速路径必须真的被用上——不能永远悄悄回退 dense（那优化就白做了）。
    正常文件（每分钟都有双侧触及）应当走出 pairs 路径且行数少于 dense。"""
    df = _df(_swing_events())
    out = _grid_pairs(_prepare_base(df), BAND)
    assert out is not None, "pairs path fell back on a normal file"
    assert out.height < reconstruct_state(df).height


def test_sparse_pairs_falls_back_when_window_cannot_be_proven():
    """安全网：治理消息之后本分钟只剩 removal、把带内买档全部删掉时，
    自校验（缺桶 / mid 出界）必须不过 -> 整个小时回退 dense，输出与 dense
    完全一致（若不过校验，快速路径会静默丢掉全部买档行）。"""
    events = [
        (T0, "snapshot", "bid", 100.0, 5.0),
        (T0, "snapshot", "bid", 99.0, 3.0),
        (T0, "snapshot", "bid", 50.0, 1.0),   # 深档：窗口外
        (T0, "snapshot", "ask", 101.0, 4.0),
        (T0 + 10_000, "update", "bid", 100.0, 6.0),  # 本分钟最后一次买档触及
        (T0 + 20_000, "update", "bid", 100.0, 0.0),  # 之后只剩 removal
        (T0 + 30_000, "update", "bid", 99.0, 0.0),
    ]
    df = _df(events)
    assert _grid_pairs(_prepare_base(df), BAND) is None, \
        "self-check passed on an unprovable window -- safety net broken"
    assert reconstruct_state_sparse(df, band=BAND).equals(reconstruct_state(df))


def test_sparse_ignores_non_final_wild_prices():
    """野值价位（本数据源实测存在 560 / 33.7k / 174 万这类报价）若不在分钟
    末条消息里，按桶窗口必须忽略它；sparse 仍与 dense 的带内结果一致。"""
    events = _swing_events()
    # 第 4 分钟中段插一条只触及深档 bid 60 的消息（该分钟末条仍是顶档更新）
    events.append((T0 + 3 * 60_000 + 500, "update", "bid", 60.0, 1.0))
    df = _df(events)
    dense = reconstruct_state(df)
    sparse = reconstruct_state_sparse(df, band=BAND)
    # 野值档位：dense 里有（深簿 key），sparse 里必须没有（窗口忽略它）
    assert dense.filter(pl.col("px") == 60.0).height > 0
    assert sparse.filter(pl.col("px") == 60.0).height == 0
    # 但带内结果仍然完全一致
    for row in _mids(dense):
        d = _band_rows(dense, row["bucket"], row["mid"], BAND)
        s = _band_rows(sparse, row["bucket"], row["mid"], BAND)
        assert s.equals(d), f"bucket {row['bucket']} mismatch with a wild price present"
