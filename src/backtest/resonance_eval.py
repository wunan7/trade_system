"""三重共振收益率评估 — 评估 resonance_buy/sell 信号的持仓收益"""

from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import text

from src.db.engine import get_finance_engine
from src.backtest.price_loader import (
    load_close_prices, load_open_prices, get_trading_days, get_forward_date,
)

DEFAULT_HOLDING_DAYS = [5, 10, 20, 60]


def evaluate_resonance(
    start_date: date,
    end_date: date,
    holding_days: list[int] | None = None,
) -> dict:
    """
    评估三重共振信号的持仓收益率。

    入场价 = T+1 开盘价（防止前视偏差）
    出场价 = T+N 收盘价

    Returns:
        {"buy": {...}, "sell": {...}, "benchmark": {...}}
    """
    holding_days = holding_days or DEFAULT_HOLDING_DAYS
    engine = get_finance_engine()
    max_hold = max(holding_days)

    # 1. 读取共振信号
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, date, resonance_buy, resonance_sell, rating, weighted_score
            FROM integrated_ratings
            WHERE date >= :start AND date <= :end
              AND (resonance_buy = true OR resonance_sell = true)
        """), {"start": start_date, "end": end_date}).fetchall()

    if not rows:
        print("[resonance_eval] 无共振信号数据")
        return {"buy": None, "sell": None}

    buy_signals = [(code, d, rating, score) for code, d, rb, rs, rating, score in rows if rb]
    sell_signals = [(code, d, rating, score) for code, d, rb, rs, rating, score in rows if rs]

    # 2. 加载价格
    close_prices = load_close_prices(start_date, end_date)
    open_prices = load_open_prices(start_date, end_date)
    trading_days = get_trading_days(start_date, end_date + timedelta(days=max_hold * 2))

    # 3. 计算买入信号收益
    buy_result = _eval_group(buy_signals, close_prices, open_prices, trading_days, holding_days, "buy")
    sell_result = _eval_group(sell_signals, close_prices, open_prices, trading_days, holding_days, "sell")

    return {"buy": buy_result, "sell": sell_result}


def _eval_group(
    signals: list[tuple],
    close_prices: dict,
    open_prices: dict,
    trading_days: list[date],
    holding_days: list[int],
    direction: str,
) -> dict | None:
    if not signals:
        return None

    all_returns = []

    for code, sig_date, rating, score in signals:
        code_close = close_prices.get(code, {})
        code_open = open_prices.get(code, {})

        # 入场价 = T+1 开盘价
        entry_date = get_forward_date(trading_days, sig_date, 1)
        if entry_date is None:
            continue
        entry_price = code_open.get(entry_date)
        if not entry_price or entry_price <= 0:
            continue

        returns = {}
        for n in holding_days:
            exit_date = get_forward_date(trading_days, sig_date, 1 + n)
            if exit_date is None:
                continue
            exit_price = code_close.get(exit_date)
            if exit_price is None:
                continue
            returns[n] = (exit_price - entry_price) / entry_price

        if returns:
            all_returns.append(returns)

    if not all_returns:
        return None

    result = {"count": len(all_returns)}
    for n in holding_days:
        rets = [r[n] for r in all_returns if n in r]
        if not rets:
            continue

        if direction == "buy":
            wins = sum(1 for r in rets if r > 0)
        else:
            wins = sum(1 for r in rets if r < 0)

        result[f"count_{n}d"] = len(rets)
        result[f"win_{n}d"] = round(wins / len(rets) * 100, 1)
        result[f"avg_ret_{n}d"] = round(sum(rets) / len(rets) * 100, 2)
        result[f"median_ret_{n}d"] = round(sorted(rets)[len(rets) // 2] * 100, 2)

    return result
