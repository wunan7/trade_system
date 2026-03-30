"""价格数据批量加载器 — 从 stock_daily 加载前复权收盘价"""

from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import text

from src.db.engine import get_finance_engine


def load_close_prices(
    start_date: date,
    end_date: date,
    codes: list[str] | None = None,
) -> dict[str, dict[date, float]]:
    """
    批量加载收盘价。

    Returns:
        {code: {trade_date: close_price}}
    """
    engine = get_finance_engine()
    # 多加载 80 天，确保 T+60 的前向价格可用
    padded_end = end_date + timedelta(days=80)

    query = """
        SELECT code, trade_date, close
        FROM stock_daily
        WHERE trade_date >= :start AND trade_date <= :end
    """
    params = {"start": start_date, "end": padded_end}

    if codes:
        placeholders = ", ".join(f"'{c}'" for c in codes)
        query += f" AND code IN ({placeholders})"

    query += " ORDER BY code, trade_date"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    prices = defaultdict(dict)
    for code, trade_date, close in rows:
        if close is not None:
            prices[code][trade_date] = float(close)

    return dict(prices)


def load_open_prices(
    start_date: date,
    end_date: date,
    codes: list[str] | None = None,
) -> dict[str, dict[date, float]]:
    """
    批量加载开盘价（用于 T+1 入场价计算）。

    Returns:
        {code: {trade_date: open_price}}
    """
    engine = get_finance_engine()
    padded_end = end_date + timedelta(days=80)

    query = """
        SELECT code, trade_date, open
        FROM stock_daily
        WHERE trade_date >= :start AND trade_date <= :end
    """
    params = {"start": start_date, "end": padded_end}

    if codes:
        placeholders = ", ".join(f"'{c}'" for c in codes)
        query += f" AND code IN ({placeholders})"

    query += " ORDER BY code, trade_date"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    prices = defaultdict(dict)
    for code, trade_date, open_price in rows:
        if open_price is not None:
            prices[code][trade_date] = float(open_price)

    return dict(prices)


def get_trading_days(start_date: date, end_date: date) -> list[date]:
    """获取实际交易日列表"""
    engine = get_finance_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT trade_date FROM stock_daily
            WHERE trade_date >= :start AND trade_date <= :end
            ORDER BY trade_date
        """), {"start": start_date, "end": end_date}).fetchall()
    return [r[0] for r in rows]


def get_forward_date(
    trading_days: list[date],
    signal_date: date,
    n_days: int,
) -> date | None:
    """从交易日列表中找到 signal_date 后第 n_days 个交易日"""
    try:
        idx = trading_days.index(signal_date)
        target_idx = idx + n_days
        if target_idx < len(trading_days):
            return trading_days[target_idx]
    except ValueError:
        # signal_date 不在交易日列表中，找最近的下一个交易日
        for i, d in enumerate(trading_days):
            if d > signal_date:
                target_idx = i + n_days
                if target_idx < len(trading_days):
                    return trading_days[target_idx]
                break
    return None
