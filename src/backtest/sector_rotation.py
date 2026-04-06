"""行业轮动策略 — 基于 TrendRadar 板块信号识别强势/弱势板块"""

import json
from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import text

from src.db.engine import get_opinion_engine


def load_sector_signals_history(
    engine,
    start: date,
    end: date,
) -> list[tuple[date, dict]]:
    """
    从 ai_analysis_results 加载板块信号历史。

    Returns:
        [(data_date, {sector: {impact, confidence}}), ...]
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT data_date, sector_impacts_json
            FROM ai_analysis_results
            WHERE data_date::date >= :s AND data_date::date <= :e
              AND sector_impacts_json IS NOT NULL
              AND sector_impacts_json != '[]'
              AND sector_impacts_json != ''
            ORDER BY data_date
        """), {"s": start, "e": end}).fetchall()

    result = []
    for data_date, sector_json in rows:
        try:
            impacts = json.loads(sector_json)
            sector_dict = {}
            for item in impacts:
                sector = item.get("sector")
                impact = item.get("impact")
                confidence = item.get("confidence", 0.5)
                if sector and impact:
                    sector_dict[sector] = {
                        "impact": impact,
                        "confidence": float(confidence),
                    }
            if sector_dict:
                result.append((data_date, sector_dict))
        except (json.JSONDecodeError, KeyError):
            continue

    return result


def compute_sector_strength(
    sector_history: list[tuple[date, dict]],
    eval_date: date,
    window: int = 3,
) -> dict[str, float]:
    """
    计算各板块在 eval_date 的强度分数（基于滚动窗口）。

    Args:
        sector_history: [(date, {sector: {impact, confidence}}), ...]
        eval_date: 评估日期
        window: 滚动窗口大小（数据点数）

    Returns:
        {sector: strength_score}
        strength_score = (bullish_count - bearish_count) × avg_confidence
    """
    # 筛选 eval_date 之前的最近 window 个数据点
    recent = [
        (d, sectors) for d, sectors in sector_history
        if d <= eval_date
    ]
    if len(recent) < window:
        window = len(recent)
    if window == 0:
        return {}

    recent = recent[-window:]

    # 统计各板块
    sector_stats = defaultdict(lambda: {"bullish": 0, "bearish": 0, "confidences": []})

    for _, sectors in recent:
        for sector, info in sectors.items():
            impact = info["impact"]
            confidence = info["confidence"]
            if impact == "利多":
                sector_stats[sector]["bullish"] += 1
            elif impact == "利空":
                sector_stats[sector]["bearish"] += 1
            sector_stats[sector]["confidences"].append(confidence)

    # 计算强度分数
    strength = {}
    for sector, stats in sector_stats.items():
        bullish = stats["bullish"]
        bearish = stats["bearish"]
        avg_conf = sum(stats["confidences"]) / len(stats["confidences"])
        strength[sector] = (bullish - bearish) * avg_conf

    return strength


def classify_sectors(
    sector_strength: dict[str, float],
    strong_threshold: float = 1.5,
    weak_threshold: float = -1.5,
) -> dict:
    """
    分类板块为 strong/weak/neutral。

    Returns:
        {
            "strong": [sector1, sector2, ...],
            "weak": [sector3, sector4, ...],
            "neutral": [sector5, ...],
            "scores": {sector: score, ...}
        }
    """
    strong = []
    weak = []
    neutral = []

    for sector, score in sector_strength.items():
        if score >= strong_threshold:
            strong.append(sector)
        elif score <= weak_threshold:
            weak.append(sector)
        else:
            neutral.append(sector)

    return {
        "strong": strong,
        "weak": weak,
        "neutral": neutral,
        "scores": sector_strength,
    }


def get_sector_multiplier(
    code: str,
    sector_classification: dict,
    stock_info: dict,
    strong_mult: float = 1.3,
    weak_mult: float = 0.7,
) -> float:
    """
    返回该股票的板块乘数。

    Args:
        code: 股票代码
        sector_classification: classify_sectors() 的返回值
        stock_info: {code: {"industry": str, ...}}
        strong_mult: 强势板块乘数
        weak_mult: 弱势板块乘数

    Returns:
        sector_multiplier (1.0 = neutral)
    """
    if code not in stock_info:
        return 1.0

    industry = stock_info[code].get("industry")
    if not industry:
        return 1.0

    if industry in sector_classification["strong"]:
        return strong_mult
    elif industry in sector_classification["weak"]:
        return weak_mult
    else:
        return 1.0


def precompute_sector_rotation(
    opinion_engine,
    checkpoints: list[date],
    stock_info: dict,
    window: int = 3,
    strong_threshold: float = 1.5,
    weak_threshold: float = -1.5,
) -> dict[date, dict]:
    """
    批量预计算全部检查点的板块强度和分类。

    Returns:
        {
            checkpoint: {
                "classification": {strong: [...], weak: [...], scores: {...}},
                "multipliers": {code: multiplier, ...}
            }
        }
    """
    if not checkpoints:
        return {}

    start = min(checkpoints) - timedelta(days=120)  # 多加载确保窗口足够
    end = max(checkpoints)

    sector_history = load_sector_signals_history(opinion_engine, start, end)

    results = {}
    for cp in checkpoints:
        strength = compute_sector_strength(sector_history, cp, window)
        classification = classify_sectors(strength, strong_threshold, weak_threshold)

        # 预计算所有股票的乘数（可选优化）
        # multipliers = {
        #     code: get_sector_multiplier(code, classification, stock_info)
        #     for code in stock_info
        # }

        results[cp] = {
            "classification": classification,
            # "multipliers": multipliers,
        }

    return results
