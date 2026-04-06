"""宏观择时信号 — 基于指数趋势、市场宽度、波动率判断市场状态"""

from collections import defaultdict
from datetime import date, timedelta

from sqlalchemy import text

from src.db.engine import get_finance_engine


def _load_index_closes(engine, start: date, end: date) -> list[tuple[date, float]]:
    """加载沪深300 收盘价序列"""
    padded_start = start - timedelta(days=120)  # 多加载，确保 MA60 在 start 有值
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT trade_date, close FROM index_daily
            WHERE code = '000300'
              AND trade_date >= :s AND trade_date <= :e
            ORDER BY trade_date
        """), {"s": padded_start, "e": end}).fetchall()
    return [(r[0], float(r[1])) for r in rows]


def _sma(values: list[float], period: int) -> list[float | None]:
    """简单移动平均"""
    result = []
    for i in range(len(values)):
        if i < period - 1:
            result.append(None)
        else:
            result.append(sum(values[i - period + 1: i + 1]) / period)
    return result


def compute_trend_score(
    index_data: list[tuple[date, float]],
    eval_date: date,
) -> tuple[int, dict]:
    """
    沪深300 趋势判断。

    close > ma20 > ma60 → +1 (上升趋势)
    close < ma20 < ma60 → -1 (下降趋势)
    otherwise           →  0 (震荡)
    """
    # 截取到 eval_date
    data = [(d, c) for d, c in index_data if d <= eval_date]
    if len(data) < 60:
        return 0, {"reason": "数据不足"}

    closes = [c for _, c in data]
    ma20_list = _sma(closes, 20)
    ma60_list = _sma(closes, 60)

    close = closes[-1]
    ma20 = ma20_list[-1]
    ma60 = ma60_list[-1]

    if ma20 is None or ma60 is None:
        return 0, {"reason": "MA 数据不足"}

    detail = {
        "close": round(close, 2),
        "ma20": round(ma20, 2),
        "ma60": round(ma60, 2),
    }

    if close > ma20 > ma60:
        return 1, {**detail, "reason": "上升趋势 (close>ma20>ma60)"}
    elif close < ma20 < ma60:
        return -1, {**detail, "reason": "下降趋势 (close<ma20<ma60)"}
    else:
        return 0, {**detail, "reason": "震荡"}


def compute_breadth_score(engine, eval_date: date) -> tuple[int, dict]:
    """
    市场宽度：股价在 20 日均线上方的比例。

    > 0.6 → +1 (多数上涨)
    < 0.3 → -1 (多数下跌)
    """
    with engine.connect() as conn:
        row = conn.execute(text("""
            WITH recent AS (
                SELECT code, trade_date, close,
                       AVG(close) OVER (
                           PARTITION BY code
                           ORDER BY trade_date
                           ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                       ) as ma20
                FROM stock_daily
                WHERE trade_date <= :d
                  AND trade_date >= :d - INTERVAL '40 days'
            ),
            latest AS (
                SELECT DISTINCT ON (code) code, close, ma20
                FROM recent
                WHERE trade_date = (
                    SELECT MAX(trade_date) FROM recent WHERE trade_date <= :d
                )
            )
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN close > ma20 THEN 1 ELSE 0 END) as above
            FROM latest
            WHERE ma20 IS NOT NULL
        """), {"d": eval_date}).fetchone()

    total = row[0] or 1
    above = row[1] or 0
    breadth = above / total

    detail = {
        "breadth": round(breadth, 3),
        "above_ma20": int(above),
        "total": int(total),
    }

    if breadth > 0.6:
        return 1, {**detail, "reason": f"宽度 {breadth:.1%} > 60%"}
    elif breadth < 0.3:
        return -1, {**detail, "reason": f"宽度 {breadth:.1%} < 30%"}
    else:
        return 0, {**detail, "reason": f"宽度 {breadth:.1%} 中性"}


def compute_volatility_score(engine, eval_date: date) -> tuple[int, dict]:
    """
    市场波动率：全市场 20 日收益率标准差在过去 1 年中的百分位。

    > 80% → -1 (高波动 = 恐慌)
    < 30% → +1 (低波动 = 平稳)
    """
    one_year_ago = eval_date - timedelta(days=365)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            WITH daily_ret AS (
                SELECT trade_date,
                       STDDEV(pct_change) as daily_vol
                FROM stock_daily
                WHERE trade_date >= :ya AND trade_date <= :d
                  AND pct_change IS NOT NULL
                GROUP BY trade_date
                HAVING COUNT(*) > 100
            )
            SELECT trade_date, daily_vol
            FROM daily_ret
            ORDER BY trade_date
        """), {"ya": one_year_ago, "d": eval_date}).fetchall()

    if len(rows) < 20:
        return 0, {"reason": "波动率数据不足"}

    vols = [float(r[1]) for r in rows]

    # 最近 20 天均值作为当前波动率
    current_vol = sum(vols[-20:]) / 20

    # 百分位
    rank = sum(1 for v in vols if v <= current_vol) / len(vols)

    detail = {
        "current_vol": round(current_vol, 4),
        "percentile": round(rank, 3),
    }

    if rank > 0.80:
        return -1, {**detail, "reason": f"高波动 (百分位 {rank:.0%})"}
    elif rank < 0.30:
        return 1, {**detail, "reason": f"低波动 (百分位 {rank:.0%})"}
    else:
        return 0, {**detail, "reason": f"波动率中性 (百分位 {rank:.0%})"}


def detect_regime(
    engine,
    eval_date: date,
    index_data: list[tuple[date, float]] | None = None,
) -> tuple[str, dict]:
    """
    综合判定市场状态。

    Returns:
        (regime, detail_dict)
        regime: "risk_on" / "neutral" / "risk_off"
    """
    if index_data is None:
        index_data = _load_index_closes(engine, eval_date - timedelta(days=400), eval_date)

    trend_score, trend_detail = compute_trend_score(index_data, eval_date)
    breadth_score, breadth_detail = compute_breadth_score(engine, eval_date)
    vol_score, vol_detail = compute_volatility_score(engine, eval_date)

    total = trend_score + breadth_score + vol_score

    if total >= 2:
        regime = "risk_on"
    elif total <= -2:
        regime = "risk_off"
    else:
        regime = "neutral"

    detail = {
        "regime": regime,
        "total_score": total,
        "trend": {"score": trend_score, **trend_detail},
        "breadth": {"score": breadth_score, **breadth_detail},
        "volatility": {"score": vol_score, **vol_detail},
    }

    return regime, detail


def precompute_regimes(
    engine,
    checkpoints: list[date],
) -> dict[date, tuple[str, dict]]:
    """
    批量计算全部检查点的 regime，预加载指数数据避免重复查询。
    """
    if not checkpoints:
        return {}

    start = min(checkpoints)
    end = max(checkpoints)
    index_data = _load_index_closes(engine, start, end)

    results = {}
    for cp in checkpoints:
        regime, detail = detect_regime(engine, cp, index_data=index_data)
        results[cp] = (regime, detail)

    return results
