"""各模型信号胜率评估 — 按 source 统计 bullish/bearish 前向收益和胜率"""

from collections import defaultdict
from datetime import date

from sqlalchemy import text

from src.db.engine import get_finance_engine
from src.backtest.price_loader import (
    load_close_prices, get_trading_days, get_forward_date,
)

DEFAULT_HOLDING_DAYS = [5, 10, 20]


def evaluate_signals(
    start_date: date,
    end_date: date,
    holding_days: list[int] | None = None,
) -> list[dict]:
    """
    评估各 source 信号的前向收益率和胜率。

    Returns:
        [{source, signal, count, win_Nd, avg_ret_Nd, ...}]
    """
    holding_days = holding_days or DEFAULT_HOLDING_DAYS
    engine = get_finance_engine()

    # 1. 读取信号
    with engine.connect() as conn:
        signals = conn.execute(text("""
            SELECT code, date, source, signal, score
            FROM stock_signals
            WHERE date >= :start AND date <= :end
              AND signal IN ('bullish', 'bearish')
        """), {"start": start_date, "end": end_date}).fetchall()

    if not signals:
        print("[signal_eval] 无信号数据")
        return []

    # 2. 加载价格和交易日
    max_hold = max(holding_days)
    prices = load_close_prices(start_date, end_date)
    trading_days = get_trading_days(start_date, end_date + __import__("datetime").timedelta(days=max_hold * 2))

    # 3. 逐条计算前向收益
    # key: (source, signal) → list of {hold_days: return}
    stats = defaultdict(list)

    for code, sig_date, source, signal, score in signals:
        code_prices = prices.get(code)
        if not code_prices:
            continue

        # T 日收盘价
        base_price = code_prices.get(sig_date)
        if not base_price or base_price <= 0:
            continue

        returns = {}
        for n in holding_days:
            fwd_date = get_forward_date(trading_days, sig_date, n)
            if fwd_date is None:
                continue
            fwd_price = code_prices.get(fwd_date)
            if fwd_price is None:
                continue
            returns[n] = (fwd_price - base_price) / base_price

        if returns:
            stats[(source, signal)].append(returns)

    # 4. 汇总统计
    results = []
    for (source, signal), return_list in sorted(stats.items()):
        row = {
            "source": source,
            "signal": signal,
            "count": len(return_list),
        }
        for n in holding_days:
            rets = [r[n] for r in return_list if n in r]
            if not rets:
                row[f"win_{n}d"] = None
                row[f"avg_ret_{n}d"] = None
                continue

            if signal == "bullish":
                wins = sum(1 for r in rets if r > 0)
            else:  # bearish
                wins = sum(1 for r in rets if r < 0)

            row[f"win_{n}d"] = round(wins / len(rets) * 100, 1)
            row[f"avg_ret_{n}d"] = round(sum(rets) / len(rets) * 100, 2)

        results.append(row)

    return results
