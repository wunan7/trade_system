"""每周检查策略 — 优化买入时机"""

from datetime import date, timedelta
from src.backtest.advanced_strategy import AdvancedStrategy, _format_advanced_report
from src.backtest.historical_sim import (
    _load_fundamental_signals,
    _load_eps_data,
    _load_prices_range,
    _load_index_prices,
    _compute_signals_at_checkpoint,
    _compute_rating,
)
from src.db.engine import get_finance_engine


def _get_weekly_checkpoints(start_date: date, end_date: date) -> list[date]:
    """获取每周一作为检查点"""
    from sqlalchemy import text
    engine = get_finance_engine()

    with engine.connect() as conn:
        # 获取所有交易日
        rows = conn.execute(text("""
            SELECT DISTINCT trade_date FROM stock_daily
            WHERE trade_date >= :start AND trade_date <= :end
            ORDER BY trade_date
        """), {"start": start_date, "end": end_date}).fetchall()

        trading_days = [row[0] for row in rows]

    # 筛选每周第一个交易日
    checkpoints = []
    last_week = None

    for td in trading_days:
        # 获取周数（年份 + 周数）
        week_key = (td.year, td.isocalendar()[1])
        if week_key != last_week:
            checkpoints.append(td)
            last_week = week_key

    return checkpoints


def run_weekly_backtest(
    start_date: date | None = None,
    end_date: date | None = None,
) -> str:
    """运行每周检查策略回测"""
    end_date = end_date or date(2026, 3, 27)
    start_date = start_date or end_date - timedelta(days=180)

    engine = get_finance_engine()
    print(f"每周检查策略回测: {start_date} ~ {end_date}")

    # 加载数据
    checkpoints = _get_weekly_checkpoints(start_date, end_date)
    fund_signals = _load_fundamental_signals(engine)
    eps_data = _load_eps_data(engine)
    prices = _load_prices_range(engine, start_date, end_date)
    index_prices = _load_index_prices(engine, start_date, end_date)

    print(f"检查点: {len(checkpoints)} 个（每周）")

    # 初始化策略
    strategy = AdvancedStrategy(initial_capital=100000)

    # 逐周执行
    results = []
    for cp in checkpoints:
        # 评级
        ratings = {}
        for code, plist in prices.items():
            sigs = _compute_signals_at_checkpoint(code, cp, plist, eps_data.get(code), fund_signals)
            if sigs is None:
                continue
            rating, score = _compute_rating(sigs)
            ratings[code] = {"rating": rating, "score": score, "resonance_buy": False}

        # 调仓
        strategy.rebalance(cp, ratings, prices)

        # 记录净值
        portfolio_value = strategy.get_portfolio_value(cp, prices)
        idx_price = index_prices.get(cp)

        results.append({
            "date": cp,
            "portfolio_value": portfolio_value,
            "cash": strategy.cash,
            "holdings_count": len(strategy.holdings),
            "index_price": idx_price,
        })

        if len(results) % 4 == 0:  # 每月打印一次
            print(f"  {cp}: 净值 {portfolio_value:,.0f}, 持仓 {len(strategy.holdings)} 只")

    # 生成报告
    return _format_weekly_report(results, strategy, start_date, end_date, checkpoints)


def _format_weekly_report(results, strategy, start_date, end_date, checkpoints) -> str:
    lines = []
    lines.append(f"# 每周检查策略回测报告 {start_date} ~ {end_date}")
    lines.append("")
    lines.append("## 策略特点")
    lines.append("")
    lines.append("- **检查频率：每周一**（vs 原策略每月一次）")
    lines.append("- 动态仓位：A+ 20%、A 15%（不买入 B 级）")
    lines.append("- 三重共振加仓 50%")
    lines.append("- 止损 -10%（不自动止盈）")
    lines.append("- 评级降至 C/D 自动卖出")
    lines.append("- 最多持有 10 只股票")
    lines.append("")

    lines.append("## 净值曲线（每月末）")
    lines.append("")
    lines.append("| 日期 | 组合净值 | 收益率 | 持仓数 | 现金 |")
    lines.append("|------|---------|--------|--------|------|")

    initial = strategy.initial_capital
    # 只显示每月最后一个检查点
    monthly_results = []
    last_month = None
    for r in results:
        month_key = (r["date"].year, r["date"].month)
        if month_key != last_month:
            if monthly_results:
                lines.append(_format_result_row(monthly_results[-1], initial))
            monthly_results = [r]
            last_month = month_key
        else:
            monthly_results.append(r)

    if monthly_results:
        lines.append(_format_result_row(monthly_results[-1], initial))

    lines.append("")

    # 最终收益
    final_value = results[-1]["portfolio_value"]
    total_return = (final_value / initial - 1) * 100

    # 基准收益
    idx_start = results[0]["index_price"]
    idx_end = results[-1]["index_price"]
    bench_return = (idx_end / idx_start - 1) * 100 if idx_start and idx_end else 0

    lines.append("## 收益统计")
    lines.append("")
    lines.append(f"- 初始资金: ¥{initial:,.0f}")
    lines.append(f"- 最终净值: ¥{final_value:,.0f}")
    lines.append(f"- 总收益率: {total_return:+.2f}%")
    lines.append(f"- 沪深300: {bench_return:+.2f}%")
    lines.append(f"- 超额收益: {total_return - bench_return:+.2f}%")
    lines.append(f"- 检查次数: {len(checkpoints)} 次（每周）")
    lines.append("")

    lines.append("## 与月度策略对比")
    lines.append("")
    lines.append("| 指标 | 月度检查 | 每周检查 | 差异 |")
    lines.append("|------|---------|---------|------|")
    lines.append("| 检查频率 | 每月 1 次 | 每周 1 次 | 4 倍 |")
    lines.append("| 调仓及时性 | 低 | 高 | ⬆️ |")
    lines.append("| 交易成本 | 低 | 中 | ⬆️ |")
    lines.append("| 捕捉机会 | 少 | 多 | ⬆️ |")
    lines.append("")

    return "\n".join(lines)


def _format_result_row(r, initial) -> str:
    ret_pct = (r["portfolio_value"] / initial - 1) * 100
    return (
        f"| {r['date']} "
        f"| ¥{r['portfolio_value']:,.0f} "
        f"| {ret_pct:+.2f}% "
        f"| {r['holdings_count']} "
        f"| ¥{r['cash']:,.0f} |"
    )
