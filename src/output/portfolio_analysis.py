"""持仓深度分析 — 基于持仓明细的收益、风险、信号分析"""

import csv
from datetime import date
from pathlib import Path

from sqlalchemy import text

from src.config import PROJECT_ROOT
from src.db.engine import get_finance_engine


def load_portfolio() -> list[dict]:
    """加载持仓明细（CSV格式）"""
    csv_path = PROJECT_ROOT / "portfolio.csv"
    if not csv_path.exists():
        return []

    holdings = []
    # 尝试多种编码
    for encoding in ['gbk', 'utf-8', 'utf-8-sig', 'gb2312']:
        try:
            with open(csv_path, encoding=encoding) as f:
                reader = csv.DictReader(f)
                temp = []
                for row in reader:
                    try:
                        # 处理日期格式 2025/10/10 或 2025-7-31
                        buy_date_str = row["buy_date"].strip().replace('/', '-')
                        # 补零：2025-7-31 → 2025-07-31
                        parts = buy_date_str.split('-')
                        if len(parts) == 3:
                            buy_date_str = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"

                        temp.append({
                            "code": row["code"].strip(),
                            "name": row.get("name", "").strip(),
                            "shares": int(row["shares"]),
                            "cost_price": float(row["cost_price"]),
                            "buy_date": date.fromisoformat(buy_date_str),
                            "notes": row.get("notes", "").strip(),
                        })
                    except (ValueError, KeyError) as e:
                        continue
                if temp:
                    holdings = temp
                    break
        except (UnicodeDecodeError, ValueError):
            continue
    return holdings


def analyze_portfolio(analysis_date: date) -> dict:
    """
    持仓深度分析。

    Returns:
        {
            "holdings": [...],  # 每只持仓的详细分析
            "summary": {...},   # 组合汇总
        }
    """
    holdings = load_portfolio()
    if not holdings:
        return None

    engine = get_finance_engine()
    codes = [h["code"] for h in holdings]

    # 1. 加载最新价格
    with engine.connect() as conn:
        placeholders = ", ".join(f"'{c}'" for c in codes)
        prices = conn.execute(text(f"""
            SELECT DISTINCT ON (code) code, close, trade_date
            FROM stock_daily
            WHERE code IN ({placeholders})
            ORDER BY code, trade_date DESC
        """)).fetchall()
        price_map = {code: (float(close), td) for code, close, td in prices}

        # 2. 加载当日信号
        signals = conn.execute(text(f"""
            SELECT code, source, signal, score, confidence
            FROM stock_signals
            WHERE code IN ({placeholders}) AND date = :d
        """), {"d": analysis_date}).fetchall()

        signal_map = {}
        for code, source, signal, score, conf in signals:
            if code not in signal_map:
                signal_map[code] = []
            signal_map[code].append({
                "source": source,
                "signal": signal,
                "score": float(score or 0),
                "confidence": float(conf or 0),
            })

        # 3. 加载综合评级
        ratings = conn.execute(text(f"""
            SELECT code, rating, weighted_score, position_pct, resonance_buy, resonance_sell
            FROM integrated_ratings
            WHERE code IN ({placeholders}) AND date = :d
        """), {"d": analysis_date}).fetchall()
        rating_map = {
            code: {
                "rating": rating,
                "score": float(score),
                "position_pct": float(pos),
                "resonance_buy": rb,
                "resonance_sell": rs,
            }
            for code, rating, score, pos, rb, rs in ratings
        }

    # 4. 逐只分析
    analyzed = []
    total_cost = 0
    total_market_value = 0
    total_profit = 0

    for h in holdings:
        code = h["code"]
        price_info = price_map.get(code)
        if price_info is None:
            continue

        current_price, price_date = price_info
        cost = h["cost_price"]
        shares = h["shares"]

        market_value = current_price * shares
        cost_value = cost * shares
        profit = market_value - cost_value
        profit_pct = (current_price / cost - 1) * 100

        hold_days = (analysis_date - h["buy_date"]).days

        signals = signal_map.get(code, [])
        rating_info = rating_map.get(code, {})

        # 风险信号统计
        bearish_sources = [s["source"] for s in signals if s["signal"] == "bearish"]
        bullish_sources = [s["source"] for s in signals if s["signal"] == "bullish"]
        neutral_sources = [s["source"] for s in signals if s["signal"] == "neutral"]

        # 建议操作
        action = "持有"
        if rating_info.get("resonance_sell"):
            action = "减仓"
        elif len(bearish_sources) >= 3:
            action = "观察"
        elif rating_info.get("resonance_buy"):
            action = "加仓"
        elif len(bullish_sources) >= 3:
            action = "持有"

        analyzed.append({
            "code": code,
            "name": h["name"],
            "shares": shares,
            "cost_price": cost,
            "current_price": current_price,
            "market_value": market_value,
            "profit": profit,
            "profit_pct": profit_pct,
            "hold_days": hold_days,
            "rating": rating_info.get("rating", "-"),
            "score": rating_info.get("score", 0),
            "bearish_count": len(bearish_sources),
            "bullish_count": len(bullish_sources),
            "neutral_count": len(neutral_sources),
            "bearish_sources": bearish_sources,
            "bullish_sources": bullish_sources,
            "action": action,
            "notes": h["notes"],
        })

        total_cost += cost_value
        total_market_value += market_value
        total_profit += profit

    # 5. 组合汇总
    summary = {
        "total_cost": total_cost,
        "total_market_value": total_market_value,
        "total_profit": total_profit,
        "total_profit_pct": (total_market_value / total_cost - 1) * 100 if total_cost > 0 else 0,
        "position_count": len(analyzed),
        "high_risk_count": sum(1 for h in analyzed if h["bearish_count"] >= 3),
        "suggest_reduce": [h["code"] for h in analyzed if h["action"] == "减仓"],
        "suggest_add": [h["code"] for h in analyzed if h["action"] == "加仓"],
    }

    return {
        "holdings": analyzed,
        "summary": summary,
    }


def format_portfolio_analysis(analysis: dict) -> str:
    """格式化持仓分析为 Markdown"""
    if analysis is None:
        return ""

    lines = []
    holdings = analysis["holdings"]
    summary = analysis["summary"]

    lines.append("## 持仓深度分析")
    lines.append("")

    # 组合概览
    lines.append("### 组合概览")
    lines.append("")
    lines.append(f"- 持仓数量: {summary['position_count']} 只")
    lines.append(f"- 总成本: ¥{summary['total_cost']:,.2f}")
    lines.append(f"- 总市值: ¥{summary['total_market_value']:,.2f}")
    lines.append(f"- 浮动盈亏: ¥{summary['total_profit']:+,.2f} ({summary['total_profit_pct']:+.2f}%)")
    lines.append(f"- 高风险持仓: {summary['high_risk_count']} 只")

    if summary["suggest_reduce"]:
        lines.append(f"- **建议减仓**: {', '.join(summary['suggest_reduce'])}")
    if summary["suggest_add"]:
        lines.append(f"- **建议加仓**: {', '.join(summary['suggest_add'])}")
    lines.append("")

    # 持仓明细表
    lines.append("### 持仓明细")
    lines.append("")
    lines.append("| 代码 | 名称 | 股数 | 成本价 | 现价 | 盈亏% | 持有天数 | 评级 | 看多 | 看空 | 建议 |")
    lines.append("|------|------|------|--------|------|-------|---------|------|------|------|------|")

    for h in sorted(holdings, key=lambda x: x["profit_pct"], reverse=True):
        lines.append(
            f"| {h['code']} "
            f"| {h['name']} "
            f"| {h['shares']} "
            f"| {h['cost_price']:.2f} "
            f"| {h['current_price']:.2f} "
            f"| {h['profit_pct']:+.2f}% "
            f"| {h['hold_days']} "
            f"| {h['rating']} "
            f"| {h['bullish_count']} "
            f"| {h['bearish_count']} "
            f"| {h['action']} |"
        )
    lines.append("")

    # 信号明细
    lines.append("### 信号明细")
    lines.append("")
    for h in holdings:
        lines.append(f"**{h['code']} {h['name']}**")
        lines.append(f"- 看多信号: {', '.join(h['bullish_sources']) if h['bullish_sources'] else '无'}")
        lines.append(f"- 看空信号: {', '.join(h['bearish_sources']) if h['bearish_sources'] else '无'}")
        lines.append("")

    # 风险提示
    high_risk = [h for h in holdings if h["bearish_count"] >= 3]
    if high_risk:
        lines.append("### 风险提示")
        lines.append("")
        for h in high_risk:
            sources = ", ".join(h["bearish_sources"])
            lines.append(f"- **{h['code']} {h['name']}**: {h['bearish_count']} 个看空信号 ({sources})")
        lines.append("")

    return "\n".join(lines)
