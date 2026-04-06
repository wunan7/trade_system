"""自适应信号权重 — 基于历史预测准确率动态调整各信号源权重"""

from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import text

from src.config import RATING_WEIGHTS
from src.db.engine import get_finance_engine


# 回测中用的 5 个评级信号源
_RATING_SOURCES = list(RATING_WEIGHTS.keys())

# 信号方向与期望收益方向的映射
_EXPECTED_DIRECTION = {"bullish": 1, "bearish": -1}


def compute_signal_accuracy(
    engine,
    eval_date: date,
    window_months: int = 6,
    forward_days: int = 20,
    prices_cache: dict | None = None,
) -> dict[str, float]:
    """
    计算各信号源在滚动窗口内的预测准确率。

    对每条 bullish/bearish 信号，检查发出信号后 forward_days 个交易日的
    实际收益方向是否一致。

    参数:
        engine: SQLAlchemy engine
        eval_date: 评估日（只使用此日期之前的数据）
        window_months: 滚动窗口月数
        forward_days: 前向收益观察天数
        prices_cache: 可选的内存价格缓存 {code: [(date, close), ...]}

    返回:
        {source: accuracy} — 准确率 0.0~1.0，无足够数据的源返回 None
    """
    window_start = eval_date - timedelta(days=window_months * 31)
    # 信号必须在 eval_date 之前足够早，使得前向收益已经完全可观测
    # 近似: forward_days 交易日 ≈ forward_days * 1.5 自然日
    signal_cutoff = eval_date - timedelta(days=int(forward_days * 1.5))

    # 1. 读取窗口内的 bullish/bearish 信号
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, date, source, signal
            FROM stock_signals
            WHERE date >= :ws AND date <= :sc
              AND source IN :sources
              AND signal IN ('bullish', 'bearish')
        """), {
            "ws": window_start,
            "sc": signal_cutoff,
            "sources": tuple(_RATING_SOURCES),
        }).fetchall()

    if not rows:
        return {s: None for s in _RATING_SOURCES}

    # 2. 按 source 分组
    signals_by_source = defaultdict(list)
    for code, sig_date, source, signal in rows:
        signals_by_source[source].append((code, sig_date, signal))

    # 3. 加载价格（优先用缓存）
    if prices_cache is None:
        prices_cache = _load_prices(engine, window_start, eval_date)

    # 4. 获取交易日历用于精确计算前向日期
    trading_days = _get_trading_days(engine, window_start, eval_date)
    trading_day_set = set(trading_days)

    # 5. 逐源计算准确率
    accuracy = {}
    for source in _RATING_SOURCES:
        sigs = signals_by_source.get(source, [])
        if not sigs:
            accuracy[source] = None
            continue

        hits = 0
        total = 0

        for code, sig_date, signal in sigs:
            expected = _EXPECTED_DIRECTION.get(signal)
            if expected is None:
                continue

            price_list = prices_cache.get(code)
            if not price_list:
                continue

            base_price = _find_price_on_or_after(price_list, sig_date)
            if base_price is None:
                continue

            future_date = _advance_trading_days(
                sig_date, forward_days, trading_days
            )
            if future_date is None or future_date > eval_date:
                continue

            future_price = _find_price_on_or_after(price_list, future_date)
            if future_price is None:
                continue

            forward_return = (future_price - base_price) / base_price
            if (expected > 0 and forward_return > 0) or \
               (expected < 0 and forward_return < 0):
                hits += 1
            total += 1

        accuracy[source] = hits / total if total >= 30 else None

    return accuracy


def derive_adaptive_weights(
    accuracy: dict[str, float | None],
    base_weights: dict[str, float],
    exponent: float = 2.0,
    min_floor: float = 0.05,
) -> dict[str, float]:
    """
    将准确率转化为归一化权重。

    准确率较高的信号源获得更大权重，通过指数放大差异。
    准确率为 None（数据不足）的源保留 base_weights 中的权重。

    参数:
        accuracy: {source: accuracy_or_None}
        base_weights: 兜底静态权重
        exponent: 指数放大因子（默认 2.0）
        min_floor: 最低权重保底（默认 0.05）

    返回:
        {source: weight} — 归一化后总和为 1.0
    """
    raw = {}
    for source in base_weights:
        acc = accuracy.get(source)
        if acc is None:
            raw[source] = base_weights[source]
        else:
            raw[source] = max(acc ** exponent, min_floor)

    total = sum(raw.values())
    if total == 0:
        n = len(base_weights)
        return {s: 1.0 / n for s in base_weights}

    return {s: round(v / total, 4) for s, v in raw.items()}


def compute_adaptive_weights(
    engine,
    eval_date: date,
    base_weights: dict[str, float] | None = None,
    window_months: int = 6,
    forward_days: int = 20,
    exponent: float = 2.0,
    min_floor: float = 0.05,
    prices_cache: dict | None = None,
) -> tuple[dict[str, float], dict[str, float | None]]:
    """
    便捷入口：计算自适应权重 + 准确率。

    返回:
        (weights, accuracy) 元组
    """
    if base_weights is None:
        base_weights = dict(RATING_WEIGHTS)

    accuracy = compute_signal_accuracy(
        engine, eval_date, window_months, forward_days, prices_cache
    )
    weights = derive_adaptive_weights(accuracy, base_weights, exponent, min_floor)
    return weights, accuracy


# ─────────────────────────────────────────────────
# 内部辅助函数
# ─────────────────────────────────────────────────

def _load_prices(engine, start: date, end: date) -> dict[str, list[tuple[date, float]]]:
    """加载区间内所有股票的收盘价"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, trade_date, close
            FROM stock_daily
            WHERE trade_date >= :s AND trade_date <= :e
            ORDER BY code, trade_date
        """), {"s": start, "e": end}).fetchall()

    prices = defaultdict(list)
    for code, d, close in rows:
        prices[code].append((d, float(close)))
    return dict(prices)


def _get_trading_days(engine, start: date, end: date) -> list[date]:
    """获取区间内的交易日列表"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT trade_date FROM stock_daily
            WHERE trade_date >= :s AND trade_date <= :e
            ORDER BY trade_date
        """), {"s": start, "e": end}).fetchall()
    return [r[0] for r in rows]


def _advance_trading_days(
    base_date: date, n: int, trading_days: list[date]
) -> date | None:
    """从 base_date 起向前推进 n 个交易日，返回目标日期"""
    try:
        # 找到 base_date 在交易日历中的位置（或之后最近的交易日）
        idx = next(i for i, d in enumerate(trading_days) if d >= base_date)
    except StopIteration:
        return None

    target_idx = idx + n
    if target_idx >= len(trading_days):
        return None
    return trading_days[target_idx]


def _find_price_on_or_after(
    price_list: list[tuple[date, float]], target: date
) -> float | None:
    """在排序价格列表中找到 target 当天或之后最近的价格"""
    for d, p in price_list:
        if d >= target:
            return p
    return None
