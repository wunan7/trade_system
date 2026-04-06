"""组合优化 — 相关性约束 + 行业集中度上限"""

from datetime import date, timedelta
import numpy as np


def compute_pairwise_correlation(
    prices: dict,
    code_a: str,
    code_b: str,
    checkpoint: date,
    window: int = 60,
) -> float:
    """计算两只股票在指定窗口内的收益率相关系数"""
    if code_a not in prices or code_b not in prices:
        return 0.0

    # 提取窗口内的价格序列
    start_date = checkpoint - timedelta(days=window)
    prices_a = [(d, p) for d, p in prices[code_a] if start_date <= d <= checkpoint]
    prices_b = [(d, p) for d, p in prices[code_b] if start_date <= d <= checkpoint]

    if len(prices_a) < 20 or len(prices_b) < 20:
        return 0.0

    # 对齐日期（只保留两只股票都有数据的日期）
    dates_a = {d for d, _ in prices_a}
    dates_b = {d for d, _ in prices_b}
    common_dates = sorted(dates_a & dates_b)

    if len(common_dates) < 20:
        return 0.0

    # 构建价格序列
    price_dict_a = {d: p for d, p in prices_a}
    price_dict_b = {d: p for d, p in prices_b}
    series_a = [price_dict_a[d] for d in common_dates]
    series_b = [price_dict_b[d] for d in common_dates]

    # 计算收益率
    returns_a = np.diff(series_a) / series_a[:-1]
    returns_b = np.diff(series_b) / series_b[:-1]

    if len(returns_a) < 10:
        return 0.0

    # 计算相关系数
    corr_matrix = np.corrcoef(returns_a, returns_b)
    return float(corr_matrix[0, 1]) if not np.isnan(corr_matrix[0, 1]) else 0.0


def compute_avg_correlation_with_holdings(
    prices: dict,
    new_code: str,
    holdings: dict,
    checkpoint: date,
    window: int = 60,
) -> float:
    """计算新股票与所有持仓的平均相关系数"""
    if not holdings:
        return 0.0

    correlations = []
    for held_code in holdings.keys():
        corr = compute_pairwise_correlation(prices, new_code, held_code, checkpoint, window)
        correlations.append(corr)

    return float(np.mean(correlations)) if correlations else 0.0


def get_correlation_multiplier(
    avg_corr: float,
    high_threshold: float = 0.7,
    mid_threshold: float = 0.5,
) -> float:
    """根据平均相关系数返回仓位乘数"""
    if avg_corr >= high_threshold:
        return 0.6  # 高度相关，大幅降仓
    elif avg_corr >= mid_threshold:
        return 0.8  # 中度相关，小幅降仓
    else:
        return 1.0  # 低相关，不调整


def get_industry_remaining_capacity(
    code: str,
    holdings: dict,
    stock_info: dict,
    current_prices: dict,
    total_value: float,
    max_industry_pct: float = 0.30,
) -> float:
    """计算该股票所属行业的剩余可用仓位比例"""
    if code not in stock_info:
        return max_industry_pct

    industry = stock_info[code].get("industry")
    if not industry:
        return max_industry_pct

    # 计算该行业当前持仓市值
    industry_value = 0.0
    for held_code, holding in holdings.items():
        if held_code in stock_info and stock_info[held_code].get("industry") == industry:
            price = current_prices.get(held_code, 0)
            industry_value += holding["shares"] * price

    # 当前行业占比
    current_industry_pct = industry_value / total_value if total_value > 0 else 0.0

    # 剩余可用空间
    remaining = max_industry_pct - current_industry_pct
    return max(0.0, remaining)
