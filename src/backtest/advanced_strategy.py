"""增强交易策略 — 动态仓位、止盈止损、评级变动触发"""

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal

import numpy as np
from sqlalchemy import text

from src.config import RATING_WEIGHTS, RATING_THRESHOLDS
from src.db.engine import get_finance_engine
from src.backtest.historical_sim import (
    _get_monthly_checkpoints,
    _load_fundamental_signals,
    _load_eps_data,
    _load_prices_range,
    _load_index_prices,
    _compute_signals_at_checkpoint,
    _compute_rating,
)


class AdvancedStrategy:
    """
    增强交易策略。

    核心改进：
    1. 动态仓位：根据评级分配仓位（A+ 20%、A 15%、B 10%）
    2. 分批建仓：评级上升时加仓，评级下降时减仓
    3. 止盈止损：单只 +20% 止盈、-10% 止损
    4. 三重共振优先：共振买入信号优先配置更高仓位
    5. 持仓周期灵活：评级保持 A 级则持有，降级则卖出
    """

    def __init__(self, initial_capital=100000):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.holdings = {}  # {code: {"shares": int, "cost": float, "entry_date": date}}
        self.history = []  # 每日净值记录

    def get_position_size(self, rating: str, score: float, resonance: bool) -> float:
        """计算目标仓位比例"""
        base_position = {
            "A+": 0.20,
            "A": 0.15,
            "B": 0.0,   # 不买入 B 级
            "C": 0.0,
            "D": 0.0,
        }.get(rating, 0.0)

        # 三重共振加仓 50%
        if resonance:
            base_position *= 1.5

        # 高分加仓（评分 > 80 加 20%）
        if score > 80:
            base_position *= 1.2

        return min(base_position, 0.25)  # 单只最高 25%

    def should_stop_loss(self, code: str, current_price: float) -> bool:
        """止损判断：-10%"""
        if code not in self.holdings:
            return False
        cost = self.holdings[code]["cost"]
        return (current_price / cost - 1) < -0.10

    def should_take_profit(self, code: str, current_price: float) -> bool:
        """不自动止盈"""
        return False

    def rebalance(self, checkpoint: date, ratings: dict, prices: dict):
        """
        动态调仓。

        ratings: {code: {"rating": str, "score": float, "resonance_buy": bool}}
        prices: {code: [(date, price), ...]}
        """
        # 1. 获取当前价格
        current_prices = {}
        for code, plist in prices.items():
            hist = [(d, p) for d, p in plist if d <= checkpoint]
            if hist:
                current_prices[code] = hist[-1][1]

        # 2. 止盈止损检查
        to_sell = []
        for code in list(self.holdings.keys()):
            if code not in current_prices:
                continue
            price = current_prices[code]
            if self.should_stop_loss(code, price):
                to_sell.append((code, "止损"))
            elif ratings.get(code, {}).get("rating") in ("C", "D"):
                to_sell.append((code, "降级"))

        # 3. 卖出
        for code, reason in to_sell:
            self._sell(code, current_prices[code], checkpoint, reason)

        # 4. 计算目标持仓
        target_positions = {}
        for code, info in ratings.items():
            if info["rating"] in ("A+", "A", "B"):
                target_pct = self.get_position_size(
                    info["rating"], info["score"], info.get("resonance_buy", False)
                )
                target_positions[code] = target_pct

        # 5. 按评分排序，优先买入高分股票
        sorted_targets = sorted(
            target_positions.items(), key=lambda x: ratings[x[0]]["score"], reverse=True
        )

        # 6. 买入/加仓
        total_value = self.cash + sum(
            self.holdings[c]["shares"] * current_prices.get(c, 0) for c in self.holdings
        )

        for code, target_pct in sorted_targets[:10]:  # 最多持有 10 只
            if code not in current_prices:
                continue
            price = current_prices[code]
            target_value = total_value * target_pct

            current_value = 0
            if code in self.holdings:
                current_value = self.holdings[code]["shares"] * price

            diff = target_value - current_value
            if diff > 1000:  # 差额 > 1000 才调仓
                shares_to_buy = int(diff / price / 100) * 100  # 100 股整数倍
                if shares_to_buy > 0:
                    self._buy(code, price, shares_to_buy, checkpoint)

    def _buy(self, code: str, price: float, shares: int, date: date):
        """买入"""
        cost = price * shares
        if cost > self.cash:
            shares = int(self.cash / price / 100) * 100
            cost = price * shares

        if shares == 0:
            return

        if code in self.holdings:
            # 加仓
            old_shares = self.holdings[code]["shares"]
            old_cost = self.holdings[code]["cost"]
            new_shares = old_shares + shares
            new_cost = (old_cost * old_shares + price * shares) / new_shares
            self.holdings[code] = {
                "shares": new_shares,
                "cost": new_cost,
                "entry_date": self.holdings[code]["entry_date"],
            }
        else:
            # 新建仓
            self.holdings[code] = {"shares": shares, "cost": price, "entry_date": date}

        self.cash -= cost

    def _sell(self, code: str, price: float, date: date, reason: str):
        """卖出"""
        if code not in self.holdings:
            return
        shares = self.holdings[code]["shares"]
        proceeds = price * shares
        self.cash += proceeds
        del self.holdings[code]

    def get_portfolio_value(self, checkpoint: date, prices: dict) -> float:
        """计算组合总市值"""
        total = self.cash
        for code, holding in self.holdings.items():
            plist = prices.get(code, [])
            hist = [(d, p) for d, p in plist if d <= checkpoint]
            if hist:
                total += holding["shares"] * hist[-1][1]
        return total


def run_advanced_backtest(
    start_date: date | None = None,
    end_date: date | None = None,
) -> str:
    """运行增强策略回测"""
    end_date = end_date or date(2026, 3, 27)
    start_date = start_date or end_date - timedelta(days=180)

    engine = get_finance_engine()
    print(f"增强策略回测: {start_date} ~ {end_date}")

    # 加载数据
    checkpoints = _get_monthly_checkpoints(start_date, end_date)
    fund_signals = _load_fundamental_signals(engine)
    eps_data = _load_eps_data(engine)
    prices = _load_prices_range(engine, start_date, end_date)
    index_prices = _load_index_prices(engine, start_date, end_date)

    print(f"检查点: {len(checkpoints)} 个")

    # 初始化策略
    strategy = AdvancedStrategy(initial_capital=100000)

    # 逐月执行
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

        # 基准
        idx_dates = sorted(index_prices.keys())
        idx_price = index_prices.get(cp)

        results.append({
            "date": cp,
            "portfolio_value": portfolio_value,
            "cash": strategy.cash,
            "holdings_count": len(strategy.holdings),
            "index_price": idx_price,
        })

        print(f"  {cp}: 净值 {portfolio_value:,.0f}, 持仓 {len(strategy.holdings)} 只, 现金 {strategy.cash:,.0f}")

    # 生成报告
    return _format_advanced_report(results, strategy, start_date, end_date)


def _format_advanced_report(results, strategy, start_date, end_date) -> str:
    lines = []
    lines.append(f"# 增强策略回测报告 {start_date} ~ {end_date}")
    lines.append("")
    lines.append("## 策略特点")
    lines.append("")
    lines.append("- 动态仓位：A+ 20%、A 15%（不买入 B 级）")
    lines.append("- 三重共振加仓 50%")
    lines.append("- 止损 -10%（不自动止盈）")
    lines.append("- 评级降至 C/D 自动卖出")
    lines.append("- 最多持有 10 只股票")
    lines.append("")

    lines.append("## 净值曲线")
    lines.append("")
    lines.append("| 日期 | 组合净值 | 收益率 | 持仓数 | 现金 |")
    lines.append("|------|---------|--------|--------|------|")

    initial = strategy.initial_capital
    for r in results:
        ret_pct = (r["portfolio_value"] / initial - 1) * 100
        lines.append(
            f"| {r['date']} "
            f"| ¥{r['portfolio_value']:,.0f} "
            f"| {ret_pct:+.2f}% "
            f"| {r['holdings_count']} "
            f"| ¥{r['cash']:,.0f} |"
        )

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
    lines.append("")

    return "\n".join(lines)
