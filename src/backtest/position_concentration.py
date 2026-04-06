"""仓位集中度优化 — 基于信号一致性、历史胜率、流动性的动态仓位调整"""

from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import text


def compute_signal_consensus(
    ratings_detail: dict,
    consensus_5_mult: float = 1.3,
    consensus_4_mult: float = 1.15,
    consensus_3_mult: float = 1.0,
    consensus_2_mult: float = 0.85,
) -> float:
    """
    计算信号一致性乘数。

    Args:
        ratings_detail: integrated_ratings 的 detail_json，格式：
            {source: {"signal": "bullish"/"bearish"/"neutral", ...}, ...}
        consensus_*_mult: 各档位乘数

    Returns:
        consensus_multiplier (0.85 - 1.3)
    """
    if not ratings_detail:
        return 1.0

    bullish_count = sum(
        1 for info in ratings_detail.values()
        if info.get("signal") == "bullish"
    )

    if bullish_count >= 5:
        return consensus_5_mult
    elif bullish_count == 4:
        return consensus_4_mult
    elif bullish_count == 3:
        return consensus_3_mult
    else:
        return consensus_2_mult


def compute_accuracy_boost(
    code: str,
    signal_sources: list[str],
    accuracy_dict: dict[str, float],
    high_threshold: float = 0.70,
    high_mult: float = 1.2,
    mid_threshold: float = 0.60,
    mid_mult: float = 1.1,
) -> float:
    """
    计算历史胜率乘数。

    Args:
        code: 股票代码
        signal_sources: 该股票涉及的信号源列表
        accuracy_dict: {source: accuracy} (来自 adaptive_weights.compute_signal_accuracy)
        high_threshold: 高准确率阈值
        high_mult: 高准确率乘数
        mid_threshold: 中等准确率阈值
        mid_mult: 中等准确率乘数

    Returns:
        accuracy_multiplier (1.0 - 1.2)
    """
    if not signal_sources or not accuracy_dict:
        return 1.0

    # 计算该股票相关信号源的平均准确率
    accuracies = [
        accuracy_dict[source]
        for source in signal_sources
        if source in accuracy_dict and accuracy_dict[source] is not None
    ]

    if not accuracies:
        return 1.0

    avg_accuracy = sum(accuracies) / len(accuracies)

    if avg_accuracy >= high_threshold:
        return high_mult
    elif avg_accuracy >= mid_threshold:
        return mid_mult
    else:
        return 1.0


def compute_liquidity_mult(
    code: str,
    liquidity_data: dict[str, float],
    high_threshold: float = 100_000_000,
    mid_threshold: float = 50_000_000,
    low_threshold: float = 10_000_000,
    mid_mult: float = 0.85,
    low_mult: float = 0.7,
) -> float:
    """
    计算流动性乘数。

    Args:
        code: 股票代码
        liquidity_data: {code: avg_daily_amount} (日均成交额)
        high_threshold: 高流动性阈值（1亿）
        mid_threshold: 中等流动性阈值（5000万）
        low_threshold: 低流动性阈值（1000万）
        mid_mult: 中等流动性乘数
        low_mult: 低流动性乘数

    Returns:
        liquidity_multiplier (0.7 - 1.0)
    """
    if code not in liquidity_data:
        return 1.0

    avg_amount = liquidity_data[code]

    if avg_amount >= high_threshold:
        return 1.0
    elif avg_amount >= mid_threshold:
        return mid_mult
    elif avg_amount >= low_threshold:
        return mid_mult
    else:
        return low_mult


def load_liquidity_data(
    engine,
    start: date,
    end: date,
    window_days: int = 20,
) -> dict[str, float]:
    """
    加载股票的日均成交额数据。

    Args:
        engine: 数据库引擎
        start: 开始日期
        end: 结束日期
        window_days: 计算日均的窗口天数

    Returns:
        {code: avg_daily_amount}
    """
    padded_start = start - timedelta(days=window_days + 10)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, trade_date, amount
            FROM stock_daily
            WHERE trade_date >= :s AND trade_date <= :e
              AND amount IS NOT NULL
            ORDER BY code, trade_date
        """), {"s": padded_start, "e": end}).fetchall()

    # 按股票分组
    stock_amounts = defaultdict(list)
    for code, trade_date, amount in rows:
        stock_amounts[code].append((trade_date, float(amount)))

    # 计算每只股票在 end 日期前 window_days 的日均成交额
    result = {}
    for code, amounts in stock_amounts.items():
        # 筛选 end 日期前的最近 window_days 个交易日
        recent = [amt for dt, amt in amounts if dt <= end]
        if len(recent) >= window_days:
            recent = recent[-window_days:]
        if recent:
            result[code] = sum(recent) / len(recent)

    return result
