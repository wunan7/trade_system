"""每日综合简报生成"""

from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import text

from src.config import PROJECT_ROOT
from src.db.engine import get_finance_engine

# 持仓文件路径
PORTFOLIO_CSV = PROJECT_ROOT / "portfolio.csv"


def _load_portfolio() -> set[str]:
    """从 portfolio.csv 读取当前持仓列表"""
    import csv

    if not PORTFOLIO_CSV.exists():
        return set()
    codes = set()
    with open(PORTFOLIO_CSV, encoding="gbk") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("code", "").strip()
            if code:
                codes.add(code)
    return codes


def _get_prev_ratings(engine, analysis_date: date) -> dict[str, tuple[str, float]]:
    """获取前一个交易日的评级 {code: (rating, weighted_score)}"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, rating, weighted_score
            FROM integrated_ratings
            WHERE date = (
                SELECT MAX(date) FROM integrated_ratings WHERE date < :d
            )
        """), {"d": analysis_date}).fetchall()
    return {code: (rating, score) for code, rating, score in rows}


def generate_briefing(analysis_date: date | None = None) -> str:
    """
    生成每日投资决策简报 Markdown 文本。
    包含：三重共振预警、评级变动、候选池监控、持仓风控提醒、评级概览、Top20
    """
    analysis_date = analysis_date or date.today()
    engine = get_finance_engine()

    with engine.connect() as conn:
        # 当日综合评级
        ratings = conn.execute(text("""
            SELECT code, rating, weighted_score, resonance_buy, resonance_sell,
                   position_pct, detail_json
            FROM integrated_ratings
            WHERE date = :d
            ORDER BY weighted_score DESC
        """), {"d": analysis_date}).fetchall()

        # 股票名称
        stock_names = {}
        rows = conn.execute(text("""
            SELECT code, name, industry_l1 FROM stock_info
            WHERE is_active = true
        """)).fetchall()
        for code, name, ind in rows:
            stock_names[code] = {"name": name, "industry": ind or ""}

        # 持仓中股票的卖出信号（缠论/舆情）
        portfolio = _load_portfolio()
        portfolio_signals = {}
        if portfolio:
            placeholders = ", ".join(f"'{c}'" for c in portfolio)
            sig_rows = conn.execute(text(f"""
                SELECT code, source, signal, score, detail_json
                FROM stock_signals
                WHERE date = :d AND code IN ({placeholders})
                  AND signal = 'bearish'
            """), {"d": analysis_date}).fetchall()
            for code, source, signal, score, detail in sig_rows:
                if code not in portfolio_signals:
                    portfolio_signals[code] = []
                portfolio_signals[code].append({
                    "source": source,
                    "signal": signal,
                    "score": score,
                    "detail": detail,
                })

    if not ratings:
        return f"# 每日投资决策简报 {analysis_date}\n\n暂无评级数据。\n"

    # 前一日评级（用于变动对比和候选池追踪）
    prev_ratings = _get_prev_ratings(engine, analysis_date)

    # 当日评级字典
    today_ratings = {r[0]: (r[1], r[2]) for r in ratings}

    lines = []
    lines.append(f"# 每日投资决策简报 {analysis_date}")
    lines.append("")

    # ========== 1. 三重共振预警 ==========
    buy_resonance = [(r[0], r[2], r[5], r[6]) for r in ratings if r[3]]
    sell_resonance = [(r[0], r[2], r[5], r[6]) for r in ratings if r[4]]

    if buy_resonance:
        lines.append("## 三重共振买入预警")
        lines.append("")
        for code, score, pos, detail in buy_resonance:
            info = stock_names.get(code, {})
            name = info.get("name", "")
            reasons = ""
            if detail and isinstance(detail, dict):
                reasons = ", ".join(detail.get("resonance_buy_reasons", []))
            lines.append(f"- **{code} {name}** (评分 {score:.1f}, 建议仓位 {pos or 0:.1f}%)")
            if reasons:
                lines.append(f"  - {reasons}")
        lines.append("")

    if sell_resonance:
        lines.append("## 三重共振卖出预警")
        lines.append("")
        for code, score, pos, detail in sell_resonance:
            info = stock_names.get(code, {})
            name = info.get("name", "")
            reasons = ""
            if detail and isinstance(detail, dict):
                reasons = ", ".join(detail.get("resonance_sell_reasons", []))
            lines.append(f"- **{code} {name}** (评分 {score:.1f})")
            if reasons:
                lines.append(f"  - {reasons}")
        lines.append("")

    # ========== 2. 综合评级变动（缺口2）==========
    upgrades = []
    downgrades = []
    _LEVEL_ORDER = {"A+": 5, "A": 4, "B": 3, "C": 2, "D": 1}

    for code, (today_rating, today_score) in today_ratings.items():
        prev = prev_ratings.get(code)
        if prev is None:
            continue
        prev_rating, prev_score = prev
        if prev_rating == today_rating:
            continue
        today_lvl = _LEVEL_ORDER.get(today_rating, 0)
        prev_lvl = _LEVEL_ORDER.get(prev_rating, 0)
        info = stock_names.get(code, {})
        entry = {
            "code": code,
            "name": info.get("name", ""),
            "prev": prev_rating,
            "today": today_rating,
            "score": today_score,
        }
        if today_lvl > prev_lvl:
            upgrades.append(entry)
        else:
            downgrades.append(entry)

    if upgrades or downgrades:
        lines.append("## 综合评级变动")
        lines.append("")
        if upgrades:
            upgrades.sort(key=lambda x: -_LEVEL_ORDER.get(x["today"], 0))
            for e in upgrades[:15]:
                lines.append(f"- **{e['code']} {e['name']}**: {e['prev']} -> {e['today']} (评分 {e['score']:.1f})")
        if downgrades:
            downgrades.sort(key=lambda x: _LEVEL_ORDER.get(x["today"], 0))
            for e in downgrades[:15]:
                lines.append(f"- **{e['code']} {e['name']}**: {e['prev']} -> {e['today']} (评分 {e['score']:.1f})")
        lines.append("")

    # ========== 3. 候选池监控（缺口3）==========
    prev_pool = {code for code, (rating, _) in prev_ratings.items() if rating in ("A+", "A")}
    today_pool = {code for code, (rating, _) in today_ratings.items() if rating in ("A+", "A")}
    new_in = today_pool - prev_pool
    dropped = prev_pool - today_pool

    if new_in or dropped:
        lines.append("## 候选池变动 (A+/A级)")
        lines.append("")
        if new_in:
            lines.append("**新入池：**")
            for code in sorted(new_in):
                info = stock_names.get(code, {})
                name = info.get("name", "")
                rating, score = today_ratings[code]
                lines.append(f"- {code} {name} ({rating}, 评分 {score:.1f})")
        if dropped:
            lines.append("**出池：**")
            for code in sorted(dropped):
                info = stock_names.get(code, {})
                name = info.get("name", "")
                prev_rating, _ = prev_ratings.get(code, ("?", 0))
                today_r = today_ratings.get(code)
                today_rating = today_r[0] if today_r else "退出"
                lines.append(f"- {code} {name} ({prev_rating} -> {today_rating})")
        lines.append(f"\n*候选池: {len(today_pool)} 只 (前日 {len(prev_pool)} 只)*")
        lines.append("")

    # ========== 4. 持仓风控提醒（缺口4）==========
    if portfolio:
        alerts = []
        for code in portfolio:
            bearish_signals = portfolio_signals.get(code, [])
            if not bearish_signals:
                continue
            info = stock_names.get(code, {})
            name = info.get("name", "")
            sources = [s["source"] for s in bearish_signals]
            alerts.append((code, name, sources, bearish_signals))

        if alerts:
            lines.append("## 持仓风控提醒")
            lines.append("")
            for code, name, sources, sigs in alerts:
                source_desc = []
                for s in sigs:
                    src = s["source"]
                    detail = s.get("detail") or {}
                    if src == "chan":
                        cn_signal = detail.get("cn_signal", "卖出信号")
                        source_desc.append(f"缠论{cn_signal}")
                    elif src == "trendradar":
                        source_desc.append(f"板块利空")
                    elif src == "valuation":
                        source_desc.append("估值高估")
                    else:
                        source_desc.append(f"{src}看空")
                lines.append(f"- **{code} {name}**: {', '.join(source_desc)}")
            lines.append("")
        else:
            lines.append("## 持仓风控提醒")
            lines.append("")
            lines.append(f"*{len(portfolio)} 只持仓暂无风险预警*")
            lines.append("")

    # ========== 5.5 持仓深度分析 ==========
    from src.output.portfolio_analysis import analyze_portfolio, format_portfolio_analysis
    portfolio_analysis = analyze_portfolio(analysis_date)
    if portfolio_analysis:
        lines.append(format_portfolio_analysis(portfolio_analysis))

    # ========== 6. 评级分布 ==========
    dist = defaultdict(list)
    for code, rating, score, *_ in ratings:
        dist[rating].append((code, score))

    lines.append("## 综合评级概览")
    lines.append("")
    for level in ["A+", "A", "B", "C", "D"]:
        stocks = dist.get(level, [])
        lines.append(f"- **{level}** 级: {len(stocks)} 只")
    lines.append("")

    # ========== 6. Top 20 ==========
    top_stocks = [(r[0], r[1], r[2], r[5]) for r in ratings if r[1] in ("A+", "A")][:20]
    if top_stocks:
        lines.append("## Top 20 优选股票")
        lines.append("")
        lines.append("| 排名 | 代码 | 名称 | 行业 | 评级 | 评分 | 仓位% |")
        lines.append("|------|------|------|------|------|------|-------|")
        for i, (code, rating, score, pos) in enumerate(top_stocks, 1):
            info = stock_names.get(code, {})
            name = info.get("name", "")
            ind = info.get("industry", "")
            lines.append(f"| {i} | {code} | {name} | {ind} | {rating} | {score:.1f} | {pos or 0:.1f} |")
        lines.append("")

    # 统计
    lines.append("---")
    lines.append(f"*共分析 {len(ratings)} 只股票*")

    return "\n".join(lines)
