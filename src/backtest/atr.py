"""ATR (Average True Range) 计算模块 — 支持动态止损止盈"""

from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import text

from src.db.engine import get_finance_engine


def load_hlc_prices(
    engine,
    start: date,
    end: date,
) -> dict[str, list[tuple[date, float, float, float]]]:
    """
    批量加载 high/low/close 数据。

    Returns:
        {code: [(trade_date, high, low, close), ...]}  按日期升序
    """
    # 多加载前 60 天，确保 ATR 在 start 附近有足够窗口
    padded_start = start - timedelta(days=90)
    padded_end = end + timedelta(days=90)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, trade_date, high, low, close
            FROM stock_daily
            WHERE trade_date >= :s AND trade_date <= :e
              AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL
            ORDER BY code, trade_date
        """), {"s": padded_start, "e": padded_end}).fetchall()

    result = defaultdict(list)
    for code, td, high, low, close in rows:
        result[code].append((td, float(high), float(low), float(close)))
    return dict(result)


def compute_atr(
    hlc_list: list[tuple[date, float, float, float]],
    period: int = 20,
) -> dict[date, float]:
    """
    计算 ATR 时间序列。

    Args:
        hlc_list: [(date, high, low, close), ...] 按日期升序
        period: ATR 平滑周期（默认 20 日）

    Returns:
        {date: atr_value}
    """
    if len(hlc_list) < 2:
        return {}

    # 计算 True Range 序列
    true_ranges = []
    for i in range(1, len(hlc_list)):
        d, high, low, close = hlc_list[i]
        prev_close = hlc_list[i - 1][3]
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        true_ranges.append((d, tr))

    if len(true_ranges) < period:
        return {}

    # SMA 初始值
    atr_values = {}
    initial_atr = sum(tr for _, tr in true_ranges[:period]) / period
    atr_values[true_ranges[period - 1][0]] = initial_atr

    # EMA 递推：ATR_t = (ATR_{t-1} × (period-1) + TR_t) / period
    prev_atr = initial_atr
    for i in range(period, len(true_ranges)):
        d, tr = true_ranges[i]
        atr = (prev_atr * (period - 1) + tr) / period
        atr_values[d] = atr
        prev_atr = atr

    return atr_values


def compute_all_atr(
    engine,
    start: date,
    end: date,
    period: int = 20,
) -> dict[str, dict[date, float]]:
    """
    批量计算所有股票的 ATR。

    Returns:
        {code: {date: atr_value}}
    """
    hlc_data = load_hlc_prices(engine, start, end)

    all_atr = {}
    for code, hlc_list in hlc_data.items():
        atr = compute_atr(hlc_list, period)
        if atr:
            all_atr[code] = atr
    return all_atr


def get_atr_at_date(
    atr_dict: dict[date, float],
    target: date,
) -> float | None:
    """在 ATR 时间序列中找到 target 当天或之前最近的 ATR 值"""
    if not atr_dict:
        return None

    best_date = None
    for d in atr_dict:
        if d <= target:
            if best_date is None or d > best_date:
                best_date = d

    return atr_dict[best_date] if best_date else None
