"""历史回测模拟 — 6个月月度检查点回测"""

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
from sqlalchemy import text

from src.config import RATING_WEIGHTS, RATING_THRESHOLDS
from src.db.engine import get_finance_engine


def _get_monthly_checkpoints(start: date, end: date) -> list[date]:
    """获取每月第一个交易日作为检查点"""
    engine = get_finance_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT trade_date FROM stock_daily
            WHERE trade_date >= :s AND trade_date <= :e
            ORDER BY trade_date
        """), {"s": start, "e": end}).fetchall()
    all_days = [r[0] for r in rows]

    checkpoints = []
    seen_months = set()
    for d in all_days:
        key = (d.year, d.month)
        if key not in seen_months:
            seen_months.add(key)
            checkpoints.append(d)
    return checkpoints


def _load_fundamental_signals(engine) -> dict[str, dict]:
    """加载今日的基本面信号（screener/buffett/munger），复用于所有检查点"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, source, signal, score
            FROM stock_signals
            WHERE date = (SELECT max(date) FROM stock_signals)
              AND source IN ('screener', 'buffett', 'munger')
        """)).fetchall()

    signals = defaultdict(dict)
    for code, source, signal, score in rows:
        signals[code][source] = {"signal": signal, "score": float(score or 50)}
    return dict(signals)


def _load_eps_data(engine) -> dict[str, float]:
    """加载最新 EPS 数据"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT ON (code) code, basic_eps
            FROM financial_summary
            WHERE basic_eps IS NOT NULL AND basic_eps > 0
            ORDER BY code, report_date DESC
        """)).fetchall()
    return {code: float(eps) for code, eps in rows}


def _load_prices_range(engine, start: date, end: date) -> dict[str, list[tuple]]:
    """加载日K线 (code → [(date, close)])"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, trade_date, close FROM stock_daily
            WHERE trade_date >= :s AND trade_date <= :e AND close IS NOT NULL
            ORDER BY code, trade_date
        """), {"s": start - timedelta(days=90), "e": end + timedelta(days=90)}).fetchall()

    prices = defaultdict(list)
    for code, td, close in rows:
        prices[code].append((td, float(close)))
    return dict(prices)


def _load_index_prices(engine, start: date, end: date) -> dict[date, float]:
    """加载沪深300指数"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, close FROM index_daily
            WHERE code = '000300' AND trade_date >= :s AND trade_date <= :e
            ORDER BY trade_date
        """), {"s": start - timedelta(days=10), "e": end + timedelta(days=90)}).fetchall()
    return {td: float(c) for td, c in rows}


def _compute_signals_at_checkpoint(
    code: str,
    checkpoint: date,
    price_list: list[tuple],
    eps: float | None,
    fund_signals: dict,
) -> dict[str, dict] | None:
    """在检查点日期计算该股票的各维度信号"""
    # 截取检查点及之前的价格
    hist = [(d, p) for d, p in price_list if d <= checkpoint]
    if len(hist) < 60:
        return None

    close = hist[-1][1]
    closes = [p for _, p in hist]

    signals = {}

    # 1. 基本面信号（复用今日）
    fund = fund_signals.get(code, {})
    for src in ("screener", "buffett", "munger"):
        if src in fund:
            signals[src] = fund[src]

    # 2. 简化估值信号（PE based）
    if eps and eps > 0 and close > 0:
        pe = close / eps
        if pe < 15:
            val_signal = "bullish"
        elif pe > 40:
            val_signal = "bearish"
        else:
            val_signal = "neutral"
        val_score = max(0, min(100, 100 - pe))
        signals["valuation"] = {"signal": val_signal, "score": val_score}

    # 3. 技术面代理（MA交叉）
    if len(closes) >= 60:
        ma20 = np.mean(closes[-20:])
        ma60 = np.mean(closes[-60:])
        if close > ma20 > ma60:
            chan_signal = "bullish"
            chan_score = min(100, (close / ma60 - 1) * 500 + 50)
        elif close < ma20 < ma60:
            chan_signal = "bearish"
            chan_score = max(0, 50 - (1 - close / ma60) * 500)
        else:
            chan_signal = "neutral"
            chan_score = 50
        signals["chan"] = {"signal": chan_signal, "score": round(chan_score, 2)}

    return signals if len(signals) >= 3 else None


def _compute_rating(signals: dict) -> tuple[str, float]:
    """计算综合评级"""
    signal_score_map = {"bullish": 100, "neutral": 50, "bearish": 0}
    weighted_sum = 0
    total_weight = 0

    for source, weight in RATING_WEIGHTS.items():
        sig = signals.get(source)
        if sig is None:
            continue
        sv = signal_score_map.get(sig["signal"], 50)
        blended = sv * 0.6 + sig["score"] * 0.4
        weighted_sum += weight * blended
        total_weight += weight

    if total_weight == 0:
        return "D", 0

    score = weighted_sum / total_weight
    for rating, threshold in sorted(RATING_THRESHOLDS.items(), key=lambda x: -x[1]):
        if score >= threshold:
            return rating, round(score, 2)
    return "D", round(score, 2)


def _get_forward_return(price_list: list[tuple], checkpoint: date, hold_days: int) -> float | None:
    """计算检查点后 hold_days 个交易日的收益率"""
    future = [(d, p) for d, p in price_list if d > checkpoint]
    if len(future) < hold_days:
        return None
    entry = future[0][1]  # T+1 开盘近似
    exit_price = future[min(hold_days - 1, len(future) - 1)][1]
    if entry <= 0:
        return None
    return (exit_price - entry) / entry


def run_historical_backtest(
    start_date: date | None = None,
    end_date: date | None = None,
    hold_days: int = 20,
) -> str:
    """
    运行 6 个月历史回测。

    在每月检查点评级 → 买入 A 级股票持有 hold_days 天 → 统计收益。
    """
    end_date = end_date or date(2026, 3, 27)
    start_date = start_date or end_date - timedelta(days=180)

    engine = get_finance_engine()
    print(f"回测区间: {start_date} ~ {end_date}, 持仓 {hold_days} 个交易日")

    # 1. 获取月度检查点
    checkpoints = _get_monthly_checkpoints(start_date, end_date)
    print(f"检查点: {len(checkpoints)} 个 — {[str(d) for d in checkpoints]}")

    # 2. 加载数据
    print("加载基本面信号...")
    fund_signals = _load_fundamental_signals(engine)
    print(f"  基本面信号: {len(fund_signals)} 只")

    print("加载 EPS 数据...")
    eps_data = _load_eps_data(engine)
    print(f"  EPS 数据: {len(eps_data)} 只")

    print("加载价格数据...")
    prices = _load_prices_range(engine, start_date, end_date)
    print(f"  价格数据: {len(prices)} 只")

    print("加载沪深300...")
    index_prices = _load_index_prices(engine, start_date, end_date)

    # 3. 每个检查点评级并计算收益
    results = []
    for cp in checkpoints:
        ratings = defaultdict(list)
        all_returns = []

        for code, plist in prices.items():
            sigs = _compute_signals_at_checkpoint(code, cp, plist, eps_data.get(code), fund_signals)
            if sigs is None:
                continue
            rating, score = _compute_rating(sigs)
            fwd_ret = _get_forward_return(plist, cp, hold_days)
            ratings[rating].append({"code": code, "score": score, "return": fwd_ret})
            if fwd_ret is not None:
                all_returns.append(fwd_ret)

        # 沪深300 基准收益
        idx_dates = sorted(index_prices.keys())
        idx_ret = None
        idx_future = [d for d in idx_dates if d > cp]
        if len(idx_future) >= hold_days:
            idx_entry = index_prices.get(idx_future[0])
            idx_exit = index_prices.get(idx_future[min(hold_days - 1, len(idx_future) - 1)])
            if idx_entry and idx_exit:
                idx_ret = (idx_exit - idx_entry) / idx_entry

        cp_result = {
            "checkpoint": cp,
            "total_stocks": sum(len(v) for v in ratings.values()),
            "benchmark_return": idx_ret,
            "market_avg_return": np.mean(all_returns) if all_returns else None,
            "ratings": {},
        }

        for level in ["A+", "A", "B", "C", "D"]:
            stocks = ratings.get(level, [])
            rets = [s["return"] for s in stocks if s["return"] is not None]
            cp_result["ratings"][level] = {
                "count": len(stocks),
                "avg_return": round(np.mean(rets) * 100, 2) if rets else None,
                "win_rate": round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1) if rets else None,
                "median_return": round(np.median(rets) * 100, 2) if rets else None,
            }

        results.append(cp_result)
        a_count = cp_result["ratings"].get("A", {}).get("count", 0)
        a_ret = cp_result["ratings"].get("A", {}).get("avg_return", "-")
        print(f"  {cp}: A级{a_count}只 均值{a_ret}%, 基准{round(idx_ret*100,2) if idx_ret else '-'}%")

    # 4. 生成报告
    return _format_report(results, start_date, end_date, hold_days)


def _format_report(results, start_date, end_date, hold_days) -> str:
    lines = []
    lines.append(f"# 历史回测报告 {start_date} ~ {end_date}")
    lines.append(f"\n持仓周期: {hold_days} 个交易日 | 检查点: {len(results)} 个月度节点")
    lines.append("")

    # 汇总表
    lines.append("## 各评级月度收益率")
    lines.append("")
    lines.append("| 检查点 | A级收益 | B级收益 | C级收益 | D级收益 | 沪深300 | A级超额 |")
    lines.append("|--------|--------|--------|--------|--------|--------|--------|")

    a_cum, bench_cum = 1.0, 1.0
    a_rets_all, bench_rets_all = [], []

    for r in results:
        a = r["ratings"].get("A", {})
        b = r["ratings"].get("B", {})
        c = r["ratings"].get("C", {})
        d = r["ratings"].get("D", {})
        bench = r["benchmark_return"]

        a_ret = a.get("avg_return")
        b_ret = b.get("avg_return")
        c_ret = c.get("avg_return")
        d_ret = d.get("avg_return")

        alpha = None
        if a_ret is not None and bench is not None:
            alpha = round(a_ret - bench * 100, 2)
            a_cum *= (1 + a_ret / 100)
            bench_cum *= (1 + bench)
            a_rets_all.append(a_ret / 100)
            bench_rets_all.append(bench)

        def _fmt(v):
            return f"{v:+.2f}%" if v is not None else "-"

        lines.append(
            f"| {r['checkpoint']} "
            f"| {_fmt(a_ret)} "
            f"| {_fmt(b_ret)} "
            f"| {_fmt(c_ret)} "
            f"| {_fmt(d_ret)} "
            f"| {_fmt(bench * 100 if bench is not None else None)} "
            f"| {_fmt(alpha)} |"
        )

    lines.append("")

    # 累计收益
    lines.append("## 累计收益")
    lines.append("")
    lines.append(f"- **A级策略累计收益**: {(a_cum - 1) * 100:+.2f}%")
    lines.append(f"- **沪深300累计收益**: {(bench_cum - 1) * 100:+.2f}%")
    lines.append(f"- **累计超额收益**: {(a_cum - bench_cum) * 100:+.2f}%")
    lines.append("")

    if a_rets_all:
        lines.append("## 策略统计")
        lines.append("")
        lines.append(f"- A级月均收益: {np.mean(a_rets_all) * 100:+.2f}%")
        lines.append(f"- A级月度胜率: {sum(1 for r in a_rets_all if r > 0) / len(a_rets_all) * 100:.0f}%")
        lines.append(f"- 基准月均收益: {np.mean(bench_rets_all) * 100:+.2f}%")
        lines.append("")

    # 胜率详情
    lines.append("## 各评级胜率详情")
    lines.append("")
    lines.append("| 检查点 | A级数量 | A级胜率 | B级胜率 | C级胜率 | D级胜率 |")
    lines.append("|--------|--------|--------|--------|--------|--------|")
    for r in results:
        def _wr(level):
            v = r["ratings"].get(level, {}).get("win_rate")
            return f"{v}%" if v is not None else "-"
        a_cnt = r["ratings"].get("A", {}).get("count", 0)
        lines.append(f"| {r['checkpoint']} | {a_cnt} | {_wr('A')} | {_wr('B')} | {_wr('C')} | {_wr('D')} |")

    lines.append("")
    lines.append("---")
    lines.append("*基本面信号(screener/buffett/munger)复用最新评分, 估值信号用历史PE, 技术面用MA交叉代理*")

    return "\n".join(lines)
