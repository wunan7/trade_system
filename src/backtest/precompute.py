"""历史信号预计算 — 让回测使用与实时评级一致的信号"""

import sys
from collections import defaultdict
from datetime import date, timedelta

import numpy as np
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import SUBSYSTEM_PATHS
from src.db.engine import get_finance_engine
from src.db.models import StockSignal


# ─────────────────────────────────────────────────
# 数据批量加载（一次性加载到内存，避免逐只查询）
# ─────────────────────────────────────────────────

def _load_all_financial_summary(engine) -> dict[str, list[dict]]:
    """加载全部 financial_summary，按 code 分组，按 report_date 降序"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, report_date, basic_eps, roe, gross_margin, net_margin,
                   debt_to_assets, revenue_growth, earnings_growth,
                   current_ratio, quick_ratio, total_revenue, net_profit,
                   bps, ocf_per_share, operating_profit_growth,
                   debt_to_equity, operating_profit
            FROM financial_summary
            ORDER BY code, report_date DESC
        """)).fetchall()

    data = defaultdict(list)
    for r in rows:
        data[r[0]].append({
            "report_date": r[1],
            "basic_eps": _f(r[2]),
            "roe": _f(r[3]),
            "gross_margin": _f(r[4]),
            "net_margin": _f(r[5]),
            "debt_to_assets": _f(r[6]),
            "revenue_growth": _f(r[7]),
            "earnings_growth": _f(r[8]),
            "current_ratio": _f(r[9]),
            "quick_ratio": _f(r[10]),
            "total_revenue": _f(r[11]),
            "net_profit": _f(r[12]),
            "bps": _f(r[13]),
            "ocf_per_share": _f(r[14]),
            "operating_profit_growth": _f(r[15]),
            "debt_to_equity": _f(r[16]),
            "operating_profit": _f(r[17]),
        })
    return dict(data)


def _load_all_balance(engine) -> dict[str, list[dict]]:
    """加载 financial_balance 关键字段"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, report_date, assets_total, total_debt,
                   current_total_debt, goodwill, accounts_receivable,
                   parent_holder_equity_total, holder_equity_total,
                   cash, short_term_loans, long_term_loan
            FROM financial_balance
            ORDER BY code, report_date DESC
        """)).fetchall()

    data = defaultdict(list)
    for r in rows:
        data[r[0]].append({
            "report_date": r[1],
            "assets_total": _f(r[2]),
            "total_debt": _f(r[3]),
            "current_total_debt": _f(r[4]),
            "goodwill": _f(r[5]),
            "accounts_receivable": _f(r[6]),
            "parent_holder_equity_total": _f(r[7]),
            "holder_equity_total": _f(r[8]),
            "cash": _f(r[9]),
            "short_term_loans": _f(r[10]),
            "long_term_loan": _f(r[11]),
        })
    return dict(data)


def _load_all_cashflow(engine) -> dict[str, list[dict]]:
    """加载 financial_cashflow 关键字段"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, report_date, act_cash_flow_net,
                   invest_cash_flow_net, financing_cash_flow_net,
                   pay_fixed_assets_etc_cash, sale_received_cash
            FROM financial_cashflow
            ORDER BY code, report_date DESC
        """)).fetchall()

    data = defaultdict(list)
    for r in rows:
        data[r[0]].append({
            "report_date": r[1],
            "act_cash_flow_net": _f(r[2]),
            "invest_cash_flow_net": _f(r[3]),
            "financing_cash_flow_net": _f(r[4]),
            "capex": _f(r[5]),
            "sale_received_cash": _f(r[6]),
        })
    return dict(data)


def _load_all_dividends(engine) -> dict[str, list[dict]]:
    """加载分红数据"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, report_year, dividend_per_10, ex_dividend_date
            FROM stock_dividend
            WHERE dividend_per_10 IS NOT NULL AND dividend_per_10 > 0
            ORDER BY code, report_year DESC
        """)).fetchall()

    data = defaultdict(list)
    for r in rows:
        data[r[0]].append({
            "report_year": r[1],
            "dividend_per_10": _f(r[2]),
            "ex_dividend_date": r[3],
        })
    return dict(data)


def _load_stock_info(engine) -> dict[str, dict]:
    """加载股票基本信息"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, name, industry_l1, list_date, is_st
            FROM stock_info
        """)).fetchall()
    return {r[0]: {"name": r[1], "industry": r[2], "list_date": r[3], "is_st": r[4]} for r in rows}


def _load_prices_all(engine) -> dict[str, list[tuple]]:
    """加载全部日K线 (code → [(date, close)])"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, trade_date, close FROM stock_daily
            WHERE close IS NOT NULL
            ORDER BY code, trade_date
        """)).fetchall()

    data = defaultdict(list)
    for code, td, close in rows:
        data[code].append((td, float(close)))
    return dict(data)


def _f(v):
    """安全转 float"""
    if v is None:
        return None
    return float(v)


def _filter_by_date(records: list[dict], checkpoint: date, limit: int = 5) -> list[dict]:
    """筛选 checkpoint 之前的记录（避免前视偏差），只取年报"""
    filtered = [r for r in records if r["report_date"] <= checkpoint]
    # 优先取年报 (12-31)，补充半年报 (6-30)
    annual = [r for r in filtered if r["report_date"].month == 12]
    if len(annual) >= limit:
        return annual[:limit]
    return filtered[:limit]


def _filter_annual_only(records: list[dict], checkpoint: date, limit: int = 5) -> list[dict]:
    """只取年报"""
    return [r for r in records
            if r["report_date"] <= checkpoint and r["report_date"].month == 12][:limit]


# ─────────────────────────────────────────────────
# 1. Screener 历史信号（8 维度评分）
# ─────────────────────────────────────────────────

def screener_historical(code: str, checkpoint: date,
                        summaries: list[dict], balances: list[dict],
                        cashflows: list[dict], dividends: list[dict],
                        info: dict) -> dict | None:
    """复刻 a-stock-screener 的 8 维度评分"""
    fs = _filter_annual_only(summaries, checkpoint)
    if len(fs) < 2:
        return None

    # dim1: 一票否决 — 净资产为负
    bl = _filter_annual_only(balances, checkpoint)
    if bl:
        for b in bl:
            equity = b.get("parent_holder_equity_total")
            if equity is not None and equity < 0:
                return _make_signal("screener", 0, "bearish", checkpoint)

    # dim2: 增长质量 (15%)
    dim2 = _score_growth(fs)

    # dim3: 盈利持续性 (20%)
    dim3 = _score_profitability(fs)

    # dim4: 资产负债质量 (15%)
    dim4 = _score_balance_sheet(fs, bl)

    # dim5: 现金流质量 (15%)
    cf = _filter_annual_only(cashflows, checkpoint)
    dim5 = _score_cashflow(fs, cf)

    # dim6: 资本配置 (10%)
    dim6 = _score_capital_allocation(dividends, checkpoint, bl)

    # dim7: 抗风险 (15%)
    dim7 = _score_resilience(fs)

    # dim8: 竞争壁垒 (10%)
    dim8 = _score_competitive_advantage(fs)

    total = (dim2 * 0.15 + dim3 * 0.20 + dim4 * 0.15 + dim5 * 0.15 +
             dim6 * 0.10 + dim7 * 0.15 + dim8 * 0.10)

    # 分红惩罚：上市 >5 年但分红 <3 年
    list_date = info.get("list_date")
    if list_date and (checkpoint - list_date).days > 5 * 365:
        div_years = len(set(d["report_year"] for d in dividends if d.get("ex_dividend_date") and d["ex_dividend_date"] <= checkpoint))
        if div_years < 3:
            total = max(0, total - 15)

    signal = "bullish" if total >= 75 else ("bearish" if total < 40 else "neutral")
    return _make_signal("screener", round(total, 2), signal, checkpoint)


def _score_growth(fs: list[dict]) -> float:
    """dim2 增长质量"""
    rev_growths = [f["revenue_growth"] for f in fs if f["revenue_growth"] is not None]
    earn_growths = [f["earnings_growth"] for f in fs if f["earnings_growth"] is not None]

    if not rev_growths:
        return 30

    # CAGR 近似：使用平均增长率
    rev_cagr = np.mean(rev_growths)
    positive_ratio = sum(1 for g in rev_growths if g > 0) / len(rev_growths)

    if rev_cagr > 15 and positive_ratio > 0.8:
        base = 90
    elif rev_cagr > 10 and positive_ratio > 0.7:
        base = 75
    elif rev_cagr > 5 and positive_ratio > 0.6:
        base = 55
    elif rev_cagr > 0:
        base = 35
    else:
        base = 15

    # 利润增速 > 收入增速 bonus
    if earn_growths and rev_growths:
        if np.mean(earn_growths) > np.mean(rev_growths):
            base = min(100, base + 5)

    return base


def _score_profitability(fs: list[dict]) -> float:
    """dim3 盈利持续性"""
    gm = [f["gross_margin"] for f in fs if f["gross_margin"] is not None]
    roes = [f["roe"] for f in fs if f["roe"] is not None]

    score = 0

    # 毛利率 (0-35)
    if gm:
        mean_gm = np.mean(gm)
        std_gm = np.std(gm) if len(gm) > 1 else 0
        if mean_gm > 40 and std_gm < 5:
            score += 35
        elif mean_gm > 30:
            score += 28
        elif mean_gm > 20:
            score += 20
        elif mean_gm > 10:
            score += 12
        else:
            score += 5

    # ROE (0-40)
    if roes:
        mean_roe = np.mean(roes)
        roe_above_15 = sum(1 for r in roes if r > 15)
        if mean_roe > 20 and roe_above_15 >= 4:
            score += 40
        elif mean_roe > 15 and roe_above_15 >= 3:
            score += 32
        elif mean_roe > 10:
            score += 22
        elif mean_roe > 5:
            score += 12
        else:
            score += 5

    # ROIC 代理 (0-25)：用 ROE × (1 - debt_ratio) 近似
    debt_ratios = [f["debt_to_assets"] for f in fs if f["debt_to_assets"] is not None]
    if roes and debt_ratios:
        avg_dr = np.mean(debt_ratios) / 100
        avg_roic = np.mean(roes) * (1 - avg_dr)
        if avg_roic > 15:
            score += 25
        elif avg_roic > 10:
            score += 20
        elif avg_roic > 5:
            score += 12
        else:
            score += 5

    return score


def _score_balance_sheet(fs: list[dict], bl: list[dict]) -> float:
    """dim4 资产负债质量"""
    score = 0

    # 资产负债率 (0-35)
    if fs and fs[0]["debt_to_assets"] is not None:
        dr = fs[0]["debt_to_assets"]
        if dr < 30:
            score += 35
        elif dr < 45:
            score += 28
        elif dr < 60:
            score += 18
        else:
            score += 8

    # 有息负债率 (0-30)：(短期借款+长期借款)/总资产
    if bl and bl[0].get("assets_total"):
        assets = bl[0]["assets_total"]
        st_loan = bl[0].get("short_term_loans") or 0
        lt_loan = bl[0].get("long_term_loan") or 0
        interest_dr = (st_loan + lt_loan) / assets * 100 if assets > 0 else 100
        if interest_dr < 5:
            score += 30
        elif interest_dr < 15:
            score += 22
        elif interest_dr < 30:
            score += 14
        else:
            score += 5

    # 商誉/净资产 (0-20)
    if bl and bl[0].get("parent_holder_equity_total"):
        gw = bl[0].get("goodwill") or 0
        equity = bl[0]["parent_holder_equity_total"]
        gw_ratio = gw / equity * 100 if equity > 0 else 100
        if gw_ratio < 5:
            score += 20
        elif gw_ratio < 15:
            score += 14
        elif gw_ratio < 30:
            score += 8
        else:
            score += 2

    # 应收账款趋势 (0-15)
    if len(bl) >= 2 and bl[0].get("accounts_receivable") is not None and bl[-1].get("accounts_receivable") is not None:
        ar0 = bl[0]["accounts_receivable"] or 0
        ar1 = bl[-1]["accounts_receivable"] or 0
        assets0 = bl[0].get("assets_total") or 1
        assets1 = bl[-1].get("assets_total") or 1
        change = ar0 / assets0 - ar1 / assets1
        if change < -0.02:
            score += 15
        elif abs(change) < 0.02:
            score += 12
        elif change < 0.05:
            score += 7
        else:
            score += 3

    return score


def _score_cashflow(fs: list[dict], cf: list[dict]) -> float:
    """dim5 现金流质量"""
    base = 50

    # 经营现金流/净利润 累计
    nps = [f["net_profit"] for f in fs if f["net_profit"] is not None and f["net_profit"] > 0]
    ocfs = [c["act_cash_flow_net"] for c in cf if c["act_cash_flow_net"] is not None]

    if nps and ocfs:
        cum_np = sum(nps[:len(ocfs)])
        cum_ocf = sum(ocfs[:len(nps)])
        if cum_np > 0:
            ratio = cum_ocf / cum_np
            if ratio > 1.2:
                base += 25
            elif ratio > 1.0:
                base += 18
            elif ratio > 0.8:
                base += 10
            elif ratio > 0.5:
                base += 0
            else:
                base -= 15

    # 自由现金流正比例
    if cf:
        capex_list = [c.get("capex") or 0 for c in cf]
        fcf_list = [(ocf - capex) for ocf, capex in zip(ocfs[:len(capex_list)], capex_list)] if ocfs else []
        if fcf_list:
            pos_ratio = sum(1 for f in fcf_list if f > 0) / len(fcf_list)
            if pos_ratio > 0.8:
                base += 10
            elif pos_ratio > 0.6:
                base += 5
            elif pos_ratio < 0.3:
                base -= 10

    # 销售收现/收入
    if cf and fs:
        sale_cash = [c.get("sale_received_cash") for c in cf if c.get("sale_received_cash") is not None]
        revenues = [f["total_revenue"] for f in fs if f["total_revenue"] is not None and f["total_revenue"] > 0]
        if sale_cash and revenues:
            ratio = np.mean(sale_cash[:len(revenues)]) / np.mean(revenues[:len(sale_cash)])
            if ratio > 1.1:
                base += 5
            elif ratio > 1.0:
                base += 2
            elif ratio < 0.8:
                base -= 5

    return max(0, min(100, base))


def _score_capital_allocation(dividends: list[dict], checkpoint: date, bl: list[dict]) -> float:
    """dim6 资本配置"""
    # 分红一致性
    div_before = [d for d in dividends if d.get("ex_dividend_date") and d["ex_dividend_date"] <= checkpoint]
    years_with_div = len(set(d["report_year"] for d in div_before))

    if years_with_div >= 8:
        base = 85
    elif years_with_div >= 6:
        base = 70
    elif years_with_div >= 4:
        base = 55
    elif years_with_div >= 2:
        base = 40
    else:
        base = 25

    # 商誉变化调整
    if len(bl) >= 2:
        gw0 = bl[0].get("goodwill") or 0
        gw1 = bl[-1].get("goodwill") or 0
        if gw1 > 0:
            gw_change = (gw0 - gw1) / gw1
            if gw_change < 0:
                base += 5  # 商誉减少
            elif gw_change > 0.5:
                base -= 15  # 商誉大幅增加
            elif gw_change > 0.2:
                base -= 8

    return max(0, min(100, base))


def _score_resilience(fs: list[dict]) -> float:
    """dim7 抗风险"""
    nps = [f["net_profit"] for f in fs if f["net_profit"] is not None]
    if not nps:
        return 30

    # 最大回撤
    peak = nps[0]
    max_dd = 0
    for np_val in nps:
        if np_val > peak:
            peak = np_val
        if peak > 0:
            dd = (peak - np_val) / peak
            max_dd = max(max_dd, dd)

    if max_dd < 0.1:
        base = 90
    elif max_dd < 0.2:
        base = 75
    elif max_dd < 0.3:
        base = 60
    elif max_dd < 0.5:
        base = 45
    else:
        base = 25

    # 危机年（2020/2022）调整
    crisis_years = {2020, 2022}
    for f in fs:
        if f["report_date"].year in crisis_years and f["earnings_growth"] is not None:
            if f["earnings_growth"] > 0:
                base = min(100, base + 10)
            elif f["earnings_growth"] > -10:
                base = min(100, base + 5)
            elif f["earnings_growth"] > -30:
                base = max(0, base - 5)
            else:
                base = max(0, base - 15)
            break  # 只取一个危机年

    return base


def _score_competitive_advantage(fs: list[dict]) -> float:
    """dim8 竞争壁垒"""
    gm = [f["gross_margin"] for f in fs if f["gross_margin"] is not None]
    roes = [f["roe"] for f in fs if f["roe"] is not None]

    score = 0

    # 毛利率稳定性 (0-35)
    if gm and len(gm) > 1:
        std = np.std(gm)
        if std < 3:
            score += 35
        elif std < 5:
            score += 28
        elif std < 8:
            score += 18
        else:
            score += 8

    # 毛利率水平 (0-35)
    if gm:
        mean_gm = np.mean(gm)
        if mean_gm > 50:
            score += 35
        elif mean_gm > 35:
            score += 28
        elif mean_gm > 20:
            score += 18
        elif mean_gm > 10:
            score += 10
        else:
            score += 3

    # ROE >15% 持续年数 (0-30)
    if roes:
        above_15 = sum(1 for r in roes if r > 15)
        if above_15 >= 4:
            score += 30
        elif above_15 >= 3:
            score += 22
        elif above_15 >= 2:
            score += 14
        elif above_15 >= 1:
            score += 8
        else:
            score += 3

    return score


# ─────────────────────────────────────────────────
# 2. Valuation 历史信号
# ─────────────────────────────────────────────────

def valuation_historical(code: str, checkpoint: date,
                         summaries: list[dict], prices: list[tuple]) -> dict | None:
    """基于历史 PE/PB/PEG 的估值信号"""
    # 获取 checkpoint 日期的收盘价
    hist = [(d, p) for d, p in prices if d <= checkpoint]
    if not hist:
        return None
    close = hist[-1][1]
    if close <= 0:
        return None

    fs = _filter_by_date(summaries, checkpoint, limit=5)
    if not fs:
        return None

    # 取最近年报的 EPS 和 BPS
    annual = [f for f in fs if f["report_date"].month == 12]
    if not annual:
        annual = fs

    eps = annual[0].get("basic_eps")
    bps = annual[0].get("bps")
    roe = annual[0].get("roe")
    earnings_growth = annual[0].get("earnings_growth")

    scores = []

    # PE 估值 (0-100)
    if eps and eps > 0:
        pe = close / eps
        if pe < 10:
            scores.append(95)
        elif pe < 15:
            scores.append(80)
        elif pe < 20:
            scores.append(65)
        elif pe < 30:
            scores.append(50)
        elif pe < 40:
            scores.append(35)
        elif pe < 60:
            scores.append(20)
        else:
            scores.append(10)

    # PB 估值 (0-100)，ROE 调整
    if bps and bps > 0:
        pb = close / bps
        if roe and roe > 15:
            # 高 ROE 允许更高 PB
            if pb < 1.5:
                scores.append(90)
            elif pb < 3:
                scores.append(70)
            elif pb < 5:
                scores.append(50)
            else:
                scores.append(25)
        else:
            if pb < 1:
                scores.append(90)
            elif pb < 2:
                scores.append(65)
            elif pb < 3:
                scores.append(45)
            else:
                scores.append(20)

    # PEG 估值 (0-100)
    if eps and eps > 0 and earnings_growth and earnings_growth > 0:
        pe = close / eps
        peg = pe / earnings_growth
        if peg < 0.5:
            scores.append(95)
        elif peg < 1:
            scores.append(80)
        elif peg < 1.5:
            scores.append(60)
        elif peg < 2:
            scores.append(40)
        else:
            scores.append(20)

    # PS 估值 (0-100)
    if fs[0].get("total_revenue") and fs[0]["total_revenue"] > 0 and bps:
        # 近似 PS = 市值/收入，这里用 close/每股收入
        revenue_per_share = fs[0]["total_revenue"] / (close / bps * 1) if bps > 0 else None
        # 简化：用 net_margin 反推
        nm = fs[0].get("net_margin")
        if nm and nm > 0 and eps and eps > 0:
            revenue_ps = eps / (nm / 100)
            ps = close / revenue_ps if revenue_ps > 0 else 100
            if ps < 1:
                scores.append(90)
            elif ps < 3:
                scores.append(70)
            elif ps < 5:
                scores.append(50)
            elif ps < 10:
                scores.append(35)
            else:
                scores.append(15)

    if not scores:
        return None

    weighted_score = np.mean(scores)
    weighted_gap = weighted_score - 50  # 正=低估, 负=高估

    signal = "bullish" if weighted_score >= 65 else ("bearish" if weighted_score < 35 else "neutral")
    return _make_signal("valuation", round(max(0, min(100, 50 + weighted_gap)), 2), signal, checkpoint)


# ─────────────────────────────────────────────────
# 3. Buffett 历史信号
# ─────────────────────────────────────────────────

def buffett_historical(code: str, checkpoint: date,
                       summaries: list[dict], balances: list[dict],
                       cashflows: list[dict], dividends: list[dict],
                       prices: list[tuple]) -> dict | None:
    """复刻巴菲特投资体系评分"""
    fs = _filter_annual_only(summaries, checkpoint)
    if len(fs) < 2:
        return None

    bl = _filter_annual_only(balances, checkpoint)
    cf = _filter_annual_only(cashflows, checkpoint)

    total = 0

    # 1. 基本面质量 (0-10)
    latest = fs[0]

    # ROE > 15% (0-2)
    if latest.get("roe") and latest["roe"] > 15:
        total += 2
    elif latest.get("roe") and latest["roe"] > 10:
        total += 1

    # 资产负债率 < 50% (0-2)
    if latest.get("debt_to_assets") and latest["debt_to_assets"] < 40:
        total += 2
    elif latest.get("debt_to_assets") and latest["debt_to_assets"] < 50:
        total += 1

    # 净利率 > 15% (0-2)
    if latest.get("net_margin") and latest["net_margin"] > 15:
        total += 2
    elif latest.get("net_margin") and latest["net_margin"] > 10:
        total += 1

    # 流动比率 > 1.5 (0-1)
    if latest.get("current_ratio") and latest["current_ratio"] > 1.5:
        total += 1

    # 盈利一致性 (0-3)
    eg = [f["earnings_growth"] for f in fs if f["earnings_growth"] is not None]
    if eg:
        pos_ratio = sum(1 for g in eg if g > 0) / len(eg)
        if pos_ratio >= 0.8:
            total += 3
        elif pos_ratio >= 0.6:
            total += 2
        elif pos_ratio >= 0.4:
            total += 1

    # 2. 护城河 (0-5)
    roes = [f["roe"] for f in fs if f["roe"] is not None]
    if roes:
        roe_above_15 = sum(1 for r in roes if r > 15) / len(roes)
        if roe_above_15 >= 0.8:
            total += 3
        elif roe_above_15 >= 0.5:
            total += 2

    gm = [f["gross_margin"] for f in fs if f["gross_margin"] is not None]
    if gm and len(gm) > 1:
        gm_std = np.std(gm)
        if gm_std < 5:
            total += 2
        elif gm_std < 10:
            total += 1

    # 3. 管理层 (0-2)
    div_before = [d for d in dividends if d.get("ex_dividend_date") and d["ex_dividend_date"] <= checkpoint]
    if len(set(d["report_year"] for d in div_before)) >= 5:
        total += 2
    elif len(set(d["report_year"] for d in div_before)) >= 3:
        total += 1

    # 4. 估值 (0-5) — PE/PB 历史分位
    hist = [(d, p) for d, p in prices if d <= checkpoint]
    if hist and fs[0].get("basic_eps") and fs[0]["basic_eps"] > 0:
        close = hist[-1][1]
        pe = close / fs[0]["basic_eps"]
        # 计算 PE 在历史中的分位
        all_pe = []
        for f in fs:
            if f.get("basic_eps") and f["basic_eps"] > 0:
                # 用各年年末价格近似
                yr_prices = [(d, p) for d, p in prices if d.year == f["report_date"].year and d.month == 12]
                if yr_prices:
                    all_pe.append(yr_prices[-1][1] / f["basic_eps"])
        if all_pe:
            percentile = sum(1 for p in all_pe if p > pe) / len(all_pe)
            if percentile > 0.7:  # PE 低于 70% 的历史值
                total += 5
            elif percentile > 0.5:
                total += 3
            elif percentile > 0.3:
                total += 1

    # 归一化到 0-100
    max_score = 22
    normalized = total / max_score * 100

    signal = "bullish" if normalized >= 70 else ("bearish" if normalized < 40 else "neutral")
    return _make_signal("buffett", round(normalized, 2), signal, checkpoint)


# ─────────────────────────────────────────────────
# 4. Munger 历史信号
# ─────────────────────────────────────────────────

def munger_historical(code: str, checkpoint: date,
                      summaries: list[dict], balances: list[dict],
                      cashflows: list[dict], dividends: list[dict],
                      prices: list[tuple]) -> dict | None:
    """复刻芒格投资体系评分"""
    fs = _filter_annual_only(summaries, checkpoint)
    if len(fs) < 2:
        return None

    bl = _filter_annual_only(balances, checkpoint)
    cf = _filter_annual_only(cashflows, checkpoint)

    # 1. 护城河强度 (0-100, 权重 35%)
    moat = 0
    roes = [f["roe"] for f in fs if f["roe"] is not None]
    gm = [f["gross_margin"] for f in fs if f["gross_margin"] is not None]

    # ROIC 持续性
    if roes:
        debt_ratios = [f["debt_to_assets"] for f in fs if f["debt_to_assets"] is not None]
        avg_dr = np.mean(debt_ratios) / 100 if debt_ratios else 0.5
        roic_approx = [r * (1 - avg_dr) for r in roes]
        above_15 = sum(1 for r in roic_approx if r > 15) / len(roic_approx)
        if above_15 >= 0.8:
            moat += 40
        elif above_15 >= 0.5:
            moat += 25
        else:
            moat += 10

    # 毛利率水平+稳定性
    if gm:
        mean_gm = np.mean(gm)
        if mean_gm > 40:
            moat += 30
        elif mean_gm > 25:
            moat += 20
        elif mean_gm > 15:
            moat += 10

        if len(gm) > 1:
            improving = sum(1 for i in range(1, len(gm)) if gm[i-1] >= gm[i]) / (len(gm) - 1)
            if improving >= 0.7:
                moat += 30
            elif improving >= 0.5:
                moat += 20
            else:
                moat += 10

    moat = min(100, moat)

    # 2. 管理层质量 (0-100, 权重 25%)
    mgmt = 50

    # FCF/净利润
    nps = [f["net_profit"] for f in fs if f["net_profit"] is not None and f["net_profit"] > 0]
    ocfs = [c["act_cash_flow_net"] for c in cf if c["act_cash_flow_net"] is not None] if cf else []
    capexs = [c.get("capex") or 0 for c in cf] if cf else []

    if nps and ocfs and capexs:
        fcf = [o - c for o, c in zip(ocfs[:len(capexs)], capexs[:len(ocfs)])]
        avg_fcf = np.mean(fcf[:len(nps)])
        avg_np = np.mean(nps[:len(fcf)])
        if avg_np > 0:
            ratio = avg_fcf / avg_np
            if ratio > 1.1:
                mgmt += 20
            elif ratio > 0.9:
                mgmt += 10
            elif ratio < 0.5:
                mgmt -= 15

    # 资产负债率
    latest = fs[0]
    if latest.get("debt_to_assets"):
        dr = latest["debt_to_assets"]
        if dr < 30:
            mgmt += 15
        elif dr < 50:
            mgmt += 5
        elif dr > 70:
            mgmt -= 15

    # 分红
    div_years = len(set(d["report_year"] for d in dividends if d.get("ex_dividend_date") and d["ex_dividend_date"] <= checkpoint))
    if div_years >= 5:
        mgmt += 10
    elif div_years >= 3:
        mgmt += 5

    mgmt = max(0, min(100, mgmt))

    # 3. 可预测性 (0-100, 权重 25%)
    pred = 50

    # 收入增长稳定性
    rev_g = [f["revenue_growth"] for f in fs if f["revenue_growth"] is not None]
    if rev_g and len(rev_g) > 1:
        std_rg = np.std(rev_g)
        if std_rg < 5:
            pred += 20
        elif std_rg < 15:
            pred += 10
        elif std_rg > 30:
            pred -= 10

    # 净利率波动
    nm = [f["net_margin"] for f in fs if f["net_margin"] is not None]
    if nm and len(nm) > 1:
        std_nm = np.std(nm)
        if std_nm < 3:
            pred += 20
        elif std_nm < 8:
            pred += 10
        elif std_nm > 15:
            pred -= 10

    # 经营现金流正比例
    if ocfs:
        pos_ratio = sum(1 for o in ocfs if o > 0) / len(ocfs)
        if pos_ratio >= 0.9:
            pred += 15
        elif pos_ratio >= 0.7:
            pred += 5
        elif pos_ratio < 0.5:
            pred -= 10

    pred = max(0, min(100, pred))

    # 4. 估值 (0-100, 权重 15%)
    val = 50
    hist = [(d, p) for d, p in prices if d <= checkpoint]
    if hist and latest.get("basic_eps") and latest["basic_eps"] > 0:
        close = hist[-1][1]
        pe = close / latest["basic_eps"]
        if pe < 10:
            val = 90
        elif pe < 15:
            val = 75
        elif pe < 20:
            val = 60
        elif pe < 30:
            val = 45
        elif pe < 50:
            val = 30
        else:
            val = 15

    # FCF 收益率
    if nps and ocfs and capexs and hist:
        avg_fcf = np.mean([o - c for o, c in zip(ocfs[:3], capexs[:3])])
        # 近似市值 = close * (净利润/EPS) 如果有 EPS
        if latest.get("basic_eps") and latest["basic_eps"] > 0:
            shares = latest["net_profit"] / latest["basic_eps"] if latest.get("net_profit") else None
            if shares and shares > 0:
                market_cap = hist[-1][1] * shares
                fcf_yield = avg_fcf / market_cap * 100
                if fcf_yield > 8:
                    val = min(100, val + 15)
                elif fcf_yield > 5:
                    val = min(100, val + 8)

    val = max(0, min(100, val))

    # 加权总分
    total = moat * 0.35 + mgmt * 0.25 + pred * 0.25 + val * 0.15

    signal = "bullish" if total >= 75 else ("bearish" if total < 45 else "neutral")
    return _make_signal("munger", round(total, 2), signal, checkpoint)


# ─────────────────────────────────────────────────
# 5. Chan 历史信号（调用 a-stock-chan）
# ─────────────────────────────────────────────────

_CHAN_SIGNAL_MAP = {
    "强烈买入信号": "bullish",
    "买入信号": "bullish",
    "关注信号": "neutral",
    "卖出信号": "bearish",
    "观望": "neutral",
}


def _init_chan_module():
    """初始化缠论模块"""
    chan_path = str(SUBSYSTEM_PATHS["chan"])
    if chan_path not in sys.path:
        sys.path.insert(0, chan_path)


def chan_historical(code: str, name: str, industry: str,
                    checkpoint: date) -> dict | None:
    """调用 a-stock-chan 完整缠论分析"""
    _init_chan_module()

    from batch_analyze import analyze_one_from_db, score_stock

    try:
        analysis = analyze_one_from_db(
            code,
            start_date="2020-01-01",
            end_date=str(checkpoint),
        )
    except Exception:
        return None

    if analysis is None:
        return None

    try:
        scored = score_stock(code, name, analysis, industry)
    except Exception:
        return None

    cn_signal = scored.get("signal", "观望")
    signal = _CHAN_SIGNAL_MAP.get(cn_signal, "neutral")
    total_score = scored.get("total_score", 0)

    return _make_signal("chan", round(total_score, 2), signal, checkpoint)


# ─────────────────────────────────────────────────
# 通用辅助
# ─────────────────────────────────────────────────

def _make_signal(source: str, score: float, signal: str, checkpoint: date) -> dict:
    return {
        "source": source,
        "signal": signal,
        "score": score,
        "confidence": round(min(score, 100), 1),
    }


# ─────────────────────────────────────────────────
# 批量预计算入口
# ─────────────────────────────────────────────────

def _get_monthly_checkpoints(engine, start: date, end: date) -> list[date]:
    """获取每月第一个交易日"""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT trade_date FROM stock_daily
            WHERE trade_date >= :s AND trade_date <= :e
            ORDER BY trade_date
        """), {"s": start, "e": end}).fetchall()

    all_days = [r[0] for r in rows]
    checkpoints = []
    seen = set()
    for d in all_days:
        key = (d.year, d.month)
        if key not in seen:
            seen.add(key)
            checkpoints.append(d)
    return checkpoints


def run_precompute(
    start_date: date,
    end_date: date,
    sources: list[str] | None = None,
    skip_chan: bool = False,
) -> int:
    """
    预计算历史信号并存入 stock_signals 表。

    Parameters:
        start_date: 起始日期
        end_date: 结束日期
        sources: 指定 source 列表，None 表示全部
        skip_chan: 跳过缠论计算（节省时间）
    Returns:
        写入的信号总数
    """
    all_sources = ["screener", "valuation", "buffett", "munger"]
    if not skip_chan:
        all_sources.append("chan")
    if sources:
        all_sources = [s for s in all_sources if s in sources]

    engine = get_finance_engine()

    # 获取检查点
    checkpoints = _get_monthly_checkpoints(engine, start_date, end_date)
    print(f"预计算: {start_date} ~ {end_date}, {len(checkpoints)} 个检查点")
    print(f"信号源: {all_sources}")

    # 批量加载数据
    print("加载数据到内存...")
    all_summaries = _load_all_financial_summary(engine)
    print(f"  financial_summary: {len(all_summaries)} 只股票")

    all_balances = _load_all_balance(engine)
    print(f"  financial_balance: {len(all_balances)} 只股票")

    all_cashflows = _load_all_cashflow(engine)
    print(f"  financial_cashflow: {len(all_cashflows)} 只股票")

    all_dividends = _load_all_dividends(engine)
    print(f"  stock_dividend: {len(all_dividends)} 只股票")

    stock_info = _load_stock_info(engine)
    print(f"  stock_info: {len(stock_info)} 只股票")

    all_prices = _load_prices_all(engine)
    print(f"  stock_daily: {len(all_prices)} 只股票")

    # 已有信号检查（断点续传）
    existing = set()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT code, date, source FROM stock_signals
            WHERE date >= :s AND date <= :e
        """), {"s": start_date, "e": end_date}).fetchall()
        for code, d, src in rows:
            existing.add((code, d, src))
    print(f"已有信号: {len(existing)} 条")

    # 股票列表（排除 ST 和银行/非银金融）
    codes = [
        code for code, info in stock_info.items()
        if not info.get("is_st")
        and info.get("industry") not in ("银行", "非银金融")
        and code in all_summaries
    ]
    print(f"待计算股票: {len(codes)} 只")

    total_saved = 0

    for cp_idx, cp in enumerate(checkpoints):
        results = []
        print(f"\n[{cp_idx+1}/{len(checkpoints)}] 检查点: {cp}")

        for i, code in enumerate(codes):
            info = stock_info.get(code, {})
            summaries = all_summaries.get(code, [])
            balances = all_balances.get(code, [])
            cashflows = all_cashflows.get(code, [])
            dividends = all_dividends.get(code, [])
            prices = all_prices.get(code, [])

            for source in all_sources:
                if (code, cp, source) in existing:
                    continue

                sig = None

                if source == "screener":
                    sig = screener_historical(code, cp, summaries, balances, cashflows, dividends, info)
                elif source == "valuation":
                    sig = valuation_historical(code, cp, summaries, prices)
                elif source == "buffett":
                    sig = buffett_historical(code, cp, summaries, balances, cashflows, dividends, prices)
                elif source == "munger":
                    sig = munger_historical(code, cp, summaries, balances, cashflows, dividends, prices)
                elif source == "chan":
                    name = info.get("name", "")
                    industry = info.get("industry", "")
                    sig = chan_historical(code, name, industry, cp)

                if sig:
                    results.append({
                        "code": code,
                        "date": cp,
                        "source": sig["source"],
                        "signal": sig["signal"],
                        "score": float(sig["score"]),
                        "confidence": float(sig["confidence"]),
                        "detail_json": {},
                    })

            if (i + 1) % 500 == 0:
                print(f"  进度: {i+1}/{len(codes)}")

        # 批量写入
        if results:
            saved = _batch_save(engine, results)
            total_saved += saved
            print(f"  写入: {saved} 条信号")

            # 统计分布
            from collections import Counter
            dist = Counter(r["signal"] for r in results)
            src_dist = Counter(r["source"] for r in results)
            print(f"  信号分布: {dict(dist)}")
            print(f"  源分布: {dict(src_dist)}")

    print(f"\n预计算完成! 共写入 {total_saved} 条信号")
    return total_saved


def _batch_save(engine, results: list[dict]) -> int:
    """批量 upsert 到 stock_signals"""
    from sqlalchemy.orm import Session

    with Session(engine) as session:
        try:
            # 分批写入（每批 1000 条）
            for i in range(0, len(results), 1000):
                batch = results[i:i+1000]
                stmt = pg_insert(StockSignal).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["code", "date", "source"],
                    set_={
                        "signal": stmt.excluded.signal,
                        "score": stmt.excluded.score,
                        "confidence": stmt.excluded.confidence,
                        "detail_json": stmt.excluded.detail_json,
                    },
                )
                session.execute(stmt)
            session.commit()
            return len(results)
        except Exception:
            session.rollback()
            raise
