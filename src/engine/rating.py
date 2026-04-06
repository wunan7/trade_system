"""综合评级系统 — 多模型加权投票"""

from collections import defaultdict
from datetime import date

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import RATING_WEIGHTS, RATING_THRESHOLDS
from src.db.engine import get_finance_engine, get_finance_session
from src.db.models import IntegratedRating
from src.engine.resonance import detect_resonance
from src.engine.position import calculate_position, get_risk_limits

# 信号 → 数值
_SIGNAL_SCORE = {"bullish": 100, "neutral": 50, "bearish": 0}


def _compute_rating(weighted_score: float) -> str:
    """将加权分数映射为评级"""
    for rating, threshold in sorted(RATING_THRESHOLDS.items(), key=lambda x: -x[1]):
        if weighted_score >= threshold:
            return rating
    return "D"


def run_rating(analysis_date: date | None = None, use_adaptive: bool = False) -> int:
    """
    对当日 stock_signals 中的所有股票计算综合评级。
    返回写入 integrated_ratings 的行数。

    Args:
        use_adaptive: 启用自适应权重（基于历史信号准确率动态调整）。
                     默认 False，使用 config.RATING_WEIGHTS 静态权重。
    """
    analysis_date = analysis_date or date.today()
    engine = get_finance_engine()

    # 1. 读取当日所有信号
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, source, signal, score, confidence
            FROM stock_signals
            WHERE date = :d
        """), {"d": analysis_date}).fetchall()

    if not rows:
        print(f"[rating] 无 {analysis_date} 的信号数据")
        return 0

    # 2. 按股票分组
    stock_signals = defaultdict(dict)
    for code, source, signal, score, confidence in rows:
        stock_signals[code][source] = {
            "signal": signal,
            "score": score,
            "confidence": confidence,
        }

    # 3. 读取 Risk Manager 仓位上限
    risk_limits = get_risk_limits(analysis_date)

    # 3.1 计算自适应权重（若启用）
    if use_adaptive:
        from src.backtest.adaptive_weights import compute_adaptive_weights
        adaptive_w, accuracy = compute_adaptive_weights(engine, analysis_date)
        rating_weights = adaptive_w
        print(f"[adaptive] 自适应权重 (滚动6个月准确率):")
        for src, w in sorted(rating_weights.items(), key=lambda x: -x[1]):
            acc = accuracy.get(src)
            acc_str = f"{acc*100:.1f}%" if acc is not None else "N/A"
            print(f"  {src:12s}: {w*100:.1f}% (准确率 {acc_str})")
    else:
        rating_weights = RATING_WEIGHTS

    # 4. 逐股计算加权评级
    results = []
    for code, signals in stock_signals.items():
        weighted_sum = 0
        total_weight = 0
        detail = {}

        for source, weight in rating_weights.items():
            sig = signals.get(source)
            if sig is None:
                continue

            signal_val = _SIGNAL_SCORE.get(sig["signal"], 50)
            # 混合使用：signal 方向 (60%) + score 细化 (40%)
            blended = signal_val * 0.6 + (sig["score"] or 50) * 0.4
            weighted_sum += weight * blended
            total_weight += weight

            detail[source] = {
                "signal": sig["signal"],
                "score": sig["score"],
                "blended": round(blended, 2),
                "weight": weight,
            }

        if total_weight == 0:
            continue

        # 归一化（部分 source 可能缺失）
        weighted_score = weighted_sum / total_weight
        rating = _compute_rating(weighted_score)

        results.append({
            "code": code,
            "date": analysis_date,
            "rating": rating,
            "weighted_score": round(weighted_score, 2),
            "resonance_buy": False,
            "resonance_sell": False,
            "position_pct": calculate_position(rating, risk_limits.get(code)),
            "detail_json": {
                "sources_count": len(detail),
                "sources": detail,
            },
        })

    if not results:
        return 0

    # 3.5 三重共振检测
    resonances = detect_resonance(analysis_date)
    buy_count = 0
    sell_count = 0
    for r in results:
        res = resonances.get(r["code"])
        if res:
            r["resonance_buy"] = res["buy"]
            r["resonance_sell"] = res["sell"]
            if res["buy"]:
                buy_count += 1
                r["detail_json"]["resonance_buy_reasons"] = res["buy_reasons"]
            if res["sell"]:
                sell_count += 1
                r["detail_json"]["resonance_sell_reasons"] = res["sell_reasons"]

    if buy_count or sell_count:
        print(f"[resonance] 三重共振: 买入 {buy_count} 只, 卖出 {sell_count} 只")

    # 4. 批量 upsert
    session = get_finance_session()
    try:
        stmt = pg_insert(IntegratedRating).values(results)
        stmt = stmt.on_conflict_do_update(
            index_elements=["code", "date"],
            set_={
                "rating": stmt.excluded.rating,
                "weighted_score": stmt.excluded.weighted_score,
                "resonance_buy": stmt.excluded.resonance_buy,
                "resonance_sell": stmt.excluded.resonance_sell,
                "position_pct": stmt.excluded.position_pct,
                "detail_json": stmt.excluded.detail_json,
            },
        )
        session.execute(stmt)
        session.commit()
        count = len(results)
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    # 5. 打印分布
    dist = defaultdict(int)
    for r in results:
        dist[r["rating"]] += 1
    print(f"[rating] 评级分布: {dict(sorted(dist.items()))}")

    return count
