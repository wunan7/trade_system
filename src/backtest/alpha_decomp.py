"""综合评级 Alpha 分解 — 按评级分组计算前向收益与超额收益"""

from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import text

from src.db.engine import get_finance_engine
from src.backtest.price_loader import (
    load_close_prices, get_trading_days, get_forward_date,
)

DEFAULT_HOLDING_DAYS = [5, 10, 20]
RATING_ORDER = ["A+", "A", "B", "C", "D"]


def evaluate_alpha(
    start_date: date,
    end_date: date,
    holding_days: list[int] | None = None,
) -> list[dict]:
    """
    按综合评级分组，计算前向收益率和 Alpha（相对全市场平均）。

    Returns:
        [{rating, count, avg_ret_Nd, alpha_Nd, ...}, ..., {rating: "market", ...}]
    """
    holding_days = holding_days or DEFAULT_HOLDING_DAYS
    engine = get_finance_engine()
    max_hold = max(holding_days)

    # 1. 读取评级
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, date, rating, weighted_score
            FROM integrated_ratings
            WHERE date >= :start AND date <= :end
        """), {"start": start_date, "end": end_date}).fetchall()

    if not rows:
        print("[alpha_decomp] 无评级数据")
        return []

    # 2. 按评级分组
    groups = defaultdict(list)  # rating → [(code, sig_date)]
    for code, sig_date, rating, score in rows:
        groups[rating].append((code, sig_date))

    # 3. 加载价格和交易日
    prices = load_close_prices(start_date, end_date)
    trading_days = get_trading_days(start_date, end_date + timedelta(days=max_hold * 2))

    # 4. 计算每组的前向收益
    group_returns = {}  # rating → {hold_days: [returns]}
    for rating, entries in groups.items():
        hold_rets = defaultdict(list)
        for code, sig_date in entries:
            code_prices = prices.get(code, {})
            base = code_prices.get(sig_date)
            if not base or base <= 0:
                continue

            for n in holding_days:
                fwd_date = get_forward_date(trading_days, sig_date, n)
                if fwd_date is None:
                    continue
                fwd_price = code_prices.get(fwd_date)
                if fwd_price is None:
                    continue
                hold_rets[n].append((fwd_price - base) / base)

        group_returns[rating] = hold_rets

    # 5. 计算全市场平均（作为基准）
    all_rets = defaultdict(list)
    for rating_rets in group_returns.values():
        for n, rets in rating_rets.items():
            all_rets[n].extend(rets)

    market_avg = {}
    for n in holding_days:
        if all_rets[n]:
            market_avg[n] = sum(all_rets[n]) / len(all_rets[n])
        else:
            market_avg[n] = 0

    # 6. 汇总结果
    results = []
    for rating in RATING_ORDER:
        if rating not in group_returns:
            continue
        rets = group_returns[rating]
        row = {
            "rating": rating,
            "count": len(groups[rating]),
        }
        for n in holding_days:
            r = rets.get(n, [])
            if not r:
                row[f"avg_ret_{n}d"] = None
                row[f"alpha_{n}d"] = None
                row[f"win_{n}d"] = None
                continue
            avg = sum(r) / len(r)
            wins = sum(1 for x in r if x > 0)
            row[f"avg_ret_{n}d"] = round(avg * 100, 2)
            row[f"alpha_{n}d"] = round((avg - market_avg[n]) * 100, 2)
            row[f"win_{n}d"] = round(wins / len(r) * 100, 1)

        results.append(row)

    # 市场基准行
    market_row = {"rating": "market", "count": sum(len(g) for g in groups.values())}
    for n in holding_days:
        market_row[f"avg_ret_{n}d"] = round(market_avg[n] * 100, 2)
        market_row[f"alpha_{n}d"] = 0.0
        if all_rets[n]:
            wins = sum(1 for x in all_rets[n] if x > 0)
            market_row[f"win_{n}d"] = round(wins / len(all_rets[n]) * 100, 1)
    results.append(market_row)

    return results
