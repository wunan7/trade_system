"""仓位计算器 — 基于 Risk Manager 波动率上限 × 综合评级系数"""

from datetime import date

from sqlalchemy import text

from src.config import POSITION
from src.db.engine import get_finance_engine

# 默认仓位上限（无 Risk Manager 数据时）
_DEFAULT_LIMIT_PCT = 20.0


def get_risk_limits(analysis_date: date) -> dict[str, float]:
    """
    从 stock_signals 读取 Risk Manager 的 position_limit_pct。
    返回 {code: limit_pct}
    """
    engine = get_finance_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT code, detail_json->>'position_limit_pct' AS limit_pct
            FROM stock_signals
            WHERE source = 'risk_manager' AND date = :d
              AND detail_json->>'position_limit_pct' IS NOT NULL
        """), {"d": analysis_date}).fetchall()

    limits = {}
    for code, limit_pct in rows:
        try:
            # position_limit_pct 存储为小数 (0.25 = 25%)，转换为百分比
            limits[code] = float(limit_pct) * 100
        except (ValueError, TypeError):
            pass
    return limits


def calculate_position(
    rating: str,
    risk_manager_limit: float | None = None,
) -> float:
    """
    根据综合评级和 Risk Manager 仓位上限计算建议仓位。

    最终仓位 = Risk Manager 仓位上限 × 信号强度系数

    Args:
        rating: 综合评级 (A+/A/B/C/D)
        risk_manager_limit: Risk Manager 给出的仓位上限百分比 (5%-25%)

    Returns:
        建议仓位百分比
    """
    coefficient = POSITION.get(rating, 0.0)
    limit = risk_manager_limit if risk_manager_limit is not None else _DEFAULT_LIMIT_PCT
    return round(limit * coefficient, 2)
