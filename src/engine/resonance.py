"""三重共振检测器 — 价值×技术×情绪信号交叉验证"""

from datetime import date, timedelta

from sqlalchemy import text

from src.config import RESONANCE
from src.db.engine import get_finance_engine


def detect_resonance(analysis_date: date | None = None) -> dict[str, dict]:
    """
    检测三重共振信号。
    返回 {code: {"buy": bool, "sell": bool, "reasons": [...]}}
    """
    analysis_date = analysis_date or date.today()
    engine = get_finance_engine()
    lookback = analysis_date - timedelta(days=RESONANCE["chan_lookback_days"])

    with engine.connect() as conn:
        # 读取当日所有信号
        rows = conn.execute(text("""
            SELECT code, source, signal, score, confidence, detail_json
            FROM stock_signals
            WHERE date = :d
        """), {"d": analysis_date}).fetchall()

        # 缠论可能是 lookback 天内的信号
        chan_rows = conn.execute(text("""
            SELECT code, signal, score, detail_json
            FROM stock_signals
            WHERE source = 'chan' AND date >= :start AND date <= :end
        """), {"start": lookback, "end": analysis_date}).fetchall()

    # 按股票分组当日信号
    from collections import defaultdict
    signals = defaultdict(dict)
    for code, source, signal, score, confidence, detail in rows:
        signals[code][source] = {
            "signal": signal,
            "score": score or 0,
            "confidence": confidence or 0,
            "detail": detail or {},
        }

    # 缠论信号（取 lookback 内最新的）
    chan_signals = {}
    for code, signal, score, detail in chan_rows:
        if code not in chan_signals or (score or 0) > chan_signals[code].get("score", 0):
            chan_signals[code] = {
                "signal": signal,
                "score": score or 0,
                "detail": detail or {},
            }

    # 检测三重共振
    result = {}
    min_chan_score = RESONANCE["chan_min_score"]
    min_tr_conf = RESONANCE["trendradar_min_confidence"]

    for code, sigs in signals.items():
        buy_reasons = []
        sell_reasons = []

        # --- 买入共振 ---
        # 条件 1：价值面看多
        val = sigs.get("valuation", {})
        buf = sigs.get("buffett", {})
        mun = sigs.get("munger", {})
        value_bullish = (
            val.get("signal") == "bullish"
            or buf.get("signal") == "bullish"
            or mun.get("signal") == "bullish"
        )
        if value_bullish:
            buy_reasons.append("价值面看多")

        # 条件 2：技术面确认（缠论买入信号 + 分数达标）
        chan = chan_signals.get(code, sigs.get("chan", {}))
        chan_sig = chan.get("signal", "")
        chan_score = chan.get("score", 0)
        tech_bullish = chan_sig == "bullish" and chan_score >= min_chan_score
        if tech_bullish:
            buy_reasons.append(f"缠论买入(score={chan_score})")

        # 条件 3：情绪面催化
        tr = sigs.get("trendradar", {})
        tr_bullish = tr.get("signal") == "bullish" and tr.get("confidence", 0) >= min_tr_conf
        if tr_bullish:
            buy_reasons.append(f"板块利多(conf={tr.get('confidence')})")

        resonance_buy = value_bullish and tech_bullish and tr_bullish

        # --- 卖出共振 ---
        value_bearish = val.get("signal") == "bearish"
        if value_bearish:
            sell_reasons.append("估值看空")

        tech_bearish = chan_sig == "bearish"
        if tech_bearish:
            sell_reasons.append("缠论卖出")

        tr_bearish = tr.get("signal") == "bearish"
        if tr_bearish:
            sell_reasons.append("板块利空")

        resonance_sell = value_bearish and tech_bearish and tr_bearish

        if resonance_buy or resonance_sell:
            result[code] = {
                "buy": resonance_buy,
                "sell": resonance_sell,
                "buy_reasons": buy_reasons if resonance_buy else [],
                "sell_reasons": sell_reasons if resonance_sell else [],
            }

    return result
