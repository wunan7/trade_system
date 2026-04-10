"""增强交易策略 — 配置化，支持多策略定义与回测"""

from collections import defaultdict
from datetime import date, datetime, timedelta

import numpy as np
from sqlalchemy import text

from src.db.engine import get_finance_engine
from src.backtest.strategy_config import StrategyConfig, load_strategy
from src.backtest.historical_sim import (
    _get_monthly_checkpoints,
    _get_weekly_checkpoints,
    _load_fundamental_signals,
    _load_precomputed_signals,
    _has_precomputed_signals,
    _load_eps_data,
    _load_prices_range,
    _load_index_prices,
    _compute_signals_at_checkpoint,
)


# ─────────────────────────────────────────────────
# 评级计算（使用策略配置的权重和阈值）
# ─────────────────────────────────────────────────

_SIGNAL_SCORE = {"bullish": 100, "neutral": 50, "bearish": 0}


def _compute_rating_with_config(
    signals: dict,
    config: StrategyConfig,
    weights_override: dict | None = None,
) -> tuple[str, float]:
    """使用策略配置的权重和阈值计算综合评级

    Args:
        weights_override: 可选权重覆盖（自适应权重场景）。
                         若为 None 则使用 config.rating_weights。
    """
    weights = weights_override or config.rating_weights
    weighted_sum = 0
    total_weight = 0

    for source, weight in weights.items():
        sig = signals.get(source)
        if sig is None:
            continue
        sv = _SIGNAL_SCORE.get(sig["signal"], 50)
        blended = sv * 0.6 + sig["score"] * 0.4
        weighted_sum += weight * blended
        total_weight += weight

    if total_weight == 0:
        return "D", 0

    score = weighted_sum / total_weight
    for rating, threshold in sorted(config.rating_thresholds.items(), key=lambda x: -x[1]):
        if score >= threshold:
            return rating, round(score, 2)
    return "D", round(score, 2)


def _compute_resonance_strength(signals: dict) -> float:
    """
    计算共振强度（0-1）。

    基于三个维度的得分/置信度的几何平均：
    - 价值面：valuation/buffett/munger 中最高 score
    - 技术面：chan score
    - 情绪面：trendradar confidence

    仅当三个维度都有 bullish 信号时才计算。
    """
    # 价值面：取 valuation/buffett/munger 中最高的 bullish score
    value_scores = []
    for source in ("valuation", "buffett", "munger"):
        sig = signals.get(source)
        if sig and sig.get("signal") == "bullish":
            value_scores.append(sig.get("score", 50) / 100)
    if not value_scores:
        return 0.0
    value_strength = max(value_scores)

    # 技术面：chan 必须 bullish 且 score >= 55
    chan = signals.get("chan")
    if not chan or chan.get("signal") != "bullish" or (chan.get("score", 0)) < 55:
        return 0.0
    tech_strength = chan.get("score", 50) / 100

    # 情绪面：trendradar 必须 bullish 且 confidence >= 0.6
    tr = signals.get("trendradar")
    if not tr or tr.get("signal") != "bullish":
        return 0.0
    tr_conf = tr.get("confidence", 0)
    if isinstance(tr_conf, (int, float)) and tr_conf > 1:
        tr_conf = tr_conf / 100  # 兼容 0-100 和 0-1 格式
    if tr_conf < 0.6:
        return 0.0
    sentiment_strength = tr_conf

    # 几何平均
    strength = (value_strength * tech_strength * sentiment_strength) ** (1.0 / 3.0)
    return round(strength, 4)


# ─────────────────────────────────────────────────
# 策略类
# ─────────────────────────────────────────────────

class AdvancedStrategy:
    """配置化交易策略"""

    def __init__(self, config: StrategyConfig):
        self.config = config
        self.initial_capital = config.initial_capital
        self.cash = config.initial_capital
        self.holdings = {}
        self.peak_prices = {}  # 移动止盈：跟踪每只持仓的最高价

    def get_position_size(self, rating: str, score: float, resonance: bool,
                          regime_multiplier: float = 1.0,
                          sector_multiplier: float = 1.0,
                          consensus_mult: float = 1.0,
                          accuracy_mult: float = 1.0,
                          liquidity_mult: float = 1.0,
                          resonance_strength: float = 0.0,
                          ml_boost: float = 1.0) -> float:
        base = self.config.position_sizes.get(rating, 0.0)

        # 宏观择时仓位调整
        base *= regime_multiplier

        # 行业轮动仓位调整
        base *= sector_multiplier

        # 仓位集中度优化
        base *= consensus_mult
        base *= accuracy_mult
        base *= liquidity_mult

        if resonance:
            if self.config.resonance_strength_enabled and resonance_strength > 0:
                dynamic_boost = 1.0 + resonance_strength * self.config.resonance_max_boost
                base *= dynamic_boost
            else:
                base *= self.config.resonance_boost

        if score > self.config.high_score_threshold:
            base *= self.config.high_score_boost

        # ML 涨停预测信号加仓
        base *= ml_boost

        return min(base, self.config.max_single_position)

    def should_stop_loss(self, code: str, current_price: float) -> bool:
        if code not in self.holdings or self.config.stop_loss_pct is None:
            return False
        cost = self.holdings[code]["cost"]
        return (current_price / cost - 1) < self.config.stop_loss_pct

    def should_take_profit(self, code: str, current_price: float) -> bool:
        if code not in self.holdings or self.config.take_profit_pct is None:
            return False
        cost = self.holdings[code]["cost"]
        return (current_price / cost - 1) > self.config.take_profit_pct

    def should_atr_stop(
        self, code: str, current_price: float, atr_data: dict | None, checkpoint: date
    ) -> bool:
        """ATR 自适应止损：止损价 = 入场价 - N × ATR"""
        if not self.config.atr_stop_enabled or code not in self.holdings:
            return False
        if atr_data is None:
            return False

        from src.backtest.atr import get_atr_at_date
        code_atr = atr_data.get(code)
        if code_atr is None:
            return False

        atr_val = get_atr_at_date(code_atr, checkpoint)
        if atr_val is None:
            return False

        cost = self.holdings[code]["cost"]
        stop_price = cost - self.config.atr_stop_multiplier * atr_val
        return current_price <= stop_price

    def should_trailing_stop(self, code: str, current_price: float) -> bool:
        """移动止盈：从最高价回撤 N% 触发"""
        if not self.config.trailing_stop_enabled or code not in self.peak_prices:
            return False
        peak = self.peak_prices[code]
        cost = self.holdings[code]["cost"]
        # 仅在盈利状态下触发（避免还没赚就被 trailing 止掉）
        if current_price <= cost:
            return False
        stop_price = peak * (1 - self.config.trailing_stop_pct)
        return current_price <= stop_price

    def should_time_stop(self, code: str, current_price: float, checkpoint: date) -> bool:
        """时间止损：持仓超 N 月且仍未盈利"""
        if self.config.time_stop_months is None or code not in self.holdings:
            return False
        entry_date = self.holdings[code]["entry_date"]
        holding_months = (checkpoint - entry_date).days / 30
        if holding_months < self.config.time_stop_months:
            return False
        cost = self.holdings[code]["cost"]
        return current_price <= cost

    def _update_peak_prices(self, current_prices: dict):
        """更新持仓的历史最高价"""
        for code in self.holdings:
            price = current_prices.get(code)
            if price is None:
                continue
            if code not in self.peak_prices or price > self.peak_prices[code]:
                self.peak_prices[code] = price

    def force_liquidate(self, target_cash_needed: float, current_prices: dict, dt: date):
        """
        强制平仓以筹集指定金额的现金。
        由多策略组合 (EnsembleStrategy) 在缩减该子策略资金时调用。
        按亏损比例从大到小排序，优先平掉亏损仓位。
        """
        shortfall = target_cash_needed - self.cash
        if shortfall <= 0 or not self.holdings:
            return

        # 计算每只股票的浮动盈亏比例
        holdings_perf = []
        for code, holding in self.holdings.items():
            price = current_prices.get(code, holding["cost"])
            profit_pct = price / holding["cost"] - 1
            value = holding["shares"] * price
            holdings_perf.append((code, profit_pct, price, value))

        # 优先卖出表现最差的股票
        holdings_perf.sort(key=lambda x: x[1])

        for code, _, price, value in holdings_perf:
            if shortfall <= 0:
                break

            if value <= shortfall * 1.1:  # 加上10%缓冲，避免剩个零头
                # 全卖
                self._sell(code, price, dt, "资金抽离(全额)")
                shortfall -= value
            else:
                # 部分卖出
                ratio = shortfall / value
                self._partial_sell(code, price, ratio, dt, "资金抽离(部分)")
                shortfall = 0

    def rebalance(self, checkpoint: date, ratings: dict, prices: dict,
                  atr_data: dict | None = None, regime_multiplier: float = 1.0,
                  sector_data: dict | None = None, stock_info: dict | None = None,
                  accuracy_dict: dict | None = None, liquidity_data: dict | None = None):
        current_prices = {}
        for code, plist in prices.items():
            hist = [(d, p) for d, p in plist if d <= checkpoint]
            if hist:
                current_prices[code] = hist[-1][1]

        # 更新移动止盈跟踪的最高价
        self._update_peak_prices(current_prices)

        # 止盈止损 + 降级卖出
        to_sell = []
        to_partial_sell = []
        for code in list(self.holdings.keys()):
            if code not in current_prices:
                continue
            price = current_prices[code]
            # 优先级：ATR止损 > 固定止损 > 移动止盈 > 固定止盈 > 时间止损 > 降级
            if self.should_atr_stop(code, price, atr_data, checkpoint):
                to_sell.append((code, "ATR止损"))
            elif self.should_stop_loss(code, price):
                to_sell.append((code, "止损"))
            elif self.should_trailing_stop(code, price):
                to_sell.append((code, "移动止盈"))
            elif self.should_take_profit(code, price):
                to_sell.append((code, "止盈"))
            elif self.should_time_stop(code, price, checkpoint):
                to_sell.append((code, "时间止损"))
            elif ratings.get(code, {}).get("rating") in self.config.sell_on_downgrade_to:
                to_sell.append((code, "降级"))
            elif self.config.scaling_enabled:
                # 分批止盈：盈利达到阈值时卖出部分仓位
                cost = self.holdings[code]["cost"]
                profit_pct = price / cost - 1
                if profit_pct >= self.config.scaling_profit_threshold:
                    to_partial_sell.append((code, self.config.scaling_profit_ratio, "分批止盈"))
                # 评级渐变：A/A+ 降到 B 时部分减仓
                elif ratings.get(code, {}).get("rating") in self.config.downgrade_partial_ratings:
                    to_partial_sell.append((code, self.config.downgrade_partial_ratio, "部分降级"))

        for code, reason in to_sell:
            self._sell(code, current_prices[code], checkpoint, reason)

        for code, ratio, reason in to_partial_sell:
            if code in self.holdings:  # 可能已被 to_sell 清掉
                self._partial_sell(code, current_prices[code], ratio, checkpoint, reason)

        # 计算目标持仓
        target_positions = {}
        for code, info in ratings.items():
            if info["rating"] in self.config.buy_ratings:
                # 计算板块乘数
                sector_mult = 1.0
                if sector_data and stock_info:
                    from src.backtest.sector_rotation import get_sector_multiplier
                    sector_mult = get_sector_multiplier(
                        code,
                        sector_data,
                        stock_info,
                        self.config.sector_strong_mult,
                        self.config.sector_weak_mult,
                    )

                # 计算仓位集中度乘数
                consensus_mult = 1.0
                accuracy_mult = 1.0
                liquidity_mult = 1.0

                if self.config.position_concentration_enabled:
                    from src.backtest.position_concentration import (
                        compute_signal_consensus,
                        compute_accuracy_boost,
                        compute_liquidity_mult,
                    )

                    # 信号一致性
                    ratings_detail = info.get("detail", {})
                    consensus_mult = compute_signal_consensus(
                        ratings_detail,
                        self.config.consensus_5_mult,
                        self.config.consensus_4_mult,
                        self.config.consensus_3_mult,
                        self.config.consensus_2_mult,
                    )

                    # 历史胜率
                    if accuracy_dict:
                        signal_sources = list(ratings_detail.keys())
                        accuracy_mult = compute_accuracy_boost(
                            code,
                            signal_sources,
                            accuracy_dict,
                            self.config.accuracy_high_threshold,
                            self.config.accuracy_high_mult,
                            self.config.accuracy_mid_threshold,
                            self.config.accuracy_mid_mult,
                        )

                    # 流动性
                    if liquidity_data:
                        liquidity_mult = compute_liquidity_mult(
                            code,
                            liquidity_data,
                            self.config.liquidity_high_threshold,
                            self.config.liquidity_mid_threshold,
                            self.config.liquidity_low_threshold,
                            self.config.liquidity_mid_mult,
                            self.config.liquidity_low_mult,
                        )

                # 计算 ML 信号乘数
                ml_boost = 1.0
                if self.config.ml_signals_enabled:
                    ml_score = info.get("ml_score", 0.0)
                    if ml_score >= 0.85:
                        ml_boost = self.config.ml_boost_strong
                    elif ml_score >= 0.70:
                        ml_boost = self.config.ml_boost_mid

                target_pct = self.get_position_size(
                    info["rating"], info["score"], info.get("resonance_buy", False),
                    regime_multiplier=regime_multiplier,
                    sector_multiplier=sector_mult,
                    consensus_mult=consensus_mult,
                    accuracy_mult=accuracy_mult,
                    liquidity_mult=liquidity_mult,
                    resonance_strength=info.get("resonance_strength", 0.0),
                    ml_boost=ml_boost,
                )
                if target_pct > 0:
                    target_positions[code] = target_pct

        sorted_targets = sorted(
            target_positions.items(), key=lambda x: ratings[x[0]]["score"], reverse=True
        )

        total_value = self.cash + sum(
            self.holdings[c]["shares"] * current_prices.get(c, 0) for c in self.holdings
        )

        for code, target_pct in sorted_targets[:self.config.max_holdings]:
            if code not in current_prices:
                continue
            price = current_prices[code]
            target_value = total_value * target_pct

            current_value = 0
            if code in self.holdings:
                current_value = self.holdings[code]["shares"] * price

            diff = target_value - current_value
            if diff > self.config.min_rebalance_diff:
                # 组合优化：相关性约束 + 行业集中度上限
                adjusted_target_pct = target_pct
                if self.config.portfolio_opt_enabled and code not in self.holdings:
                    from src.backtest.portfolio_optimizer import (
                        compute_avg_correlation_with_holdings,
                        get_correlation_multiplier,
                        get_industry_remaining_capacity,
                    )

                    # 相关性约束（可选）
                    if self.config.correlation_constraint_enabled:
                        avg_corr = compute_avg_correlation_with_holdings(
                            prices, code, self.holdings, checkpoint,
                            window=self.config.correlation_window_days
                        )
                        corr_mult = get_correlation_multiplier(
                            avg_corr, self.config.max_correlation, self.config.mid_correlation
                        )
                        adjusted_target_pct *= corr_mult

                    # 行业集中度上限
                    if stock_info:
                        industry_capacity = get_industry_remaining_capacity(
                            code, self.holdings, stock_info, current_prices,
                            total_value, self.config.max_industry_pct
                        )
                        adjusted_target_pct = min(adjusted_target_pct, industry_capacity)

                # 重新计算目标价值和买入股数
                adjusted_target_value = total_value * adjusted_target_pct
                adjusted_diff = adjusted_target_value - current_value

                if adjusted_diff > self.config.min_rebalance_diff:
                    shares_to_buy = int(adjusted_diff / price / self.config.lot_size) * self.config.lot_size
                    # 分批建仓：首次买入仅建部分仓位
                    if self.config.scaling_enabled and code not in self.holdings:
                        shares_to_buy = int(
                            shares_to_buy * self.config.scaling_in_initial
                            / self.config.lot_size
                        ) * self.config.lot_size
                    if shares_to_buy > 0:
                        self._buy(code, price, shares_to_buy, checkpoint)

        # 反向信号 / 黄金坑：valuation 极度看多 + chan 看空 → 小仓位试探
        if self.config.contrarian_enabled:
            for code, info in ratings.items():
                if code in self.holdings:
                    continue  # 已持仓不重复买
                if code in target_positions:
                    continue  # 已在正常买入列表中
                if code not in current_prices:
                    continue
                if len(self.holdings) >= self.config.max_holdings:
                    break  # 已满仓

                detail = info.get("detail", {})
                val = detail.get("valuation", {})
                chan = detail.get("chan", {})

                val_score = val.get("score", 0)
                chan_sig = chan.get("signal", "")

                # 黄金坑条件：估值极度低估 + 技术面看空
                if val_score >= self.config.contrarian_valuation_min and chan_sig == "bearish":
                    # 小仓位买入
                    base_pct = self.config.position_sizes.get("A", 0.15)
                    contrarian_pct = base_pct * self.config.contrarian_position_ratio
                    target_value = total_value * contrarian_pct
                    shares_to_buy = int(
                        target_value / current_prices[code] / self.config.lot_size
                    ) * self.config.lot_size
                    if shares_to_buy > 0:
                        self._buy(code, current_prices[code], shares_to_buy, checkpoint)

    def _buy(self, code: str, price: float, shares: int, dt: date):
        cost = price * shares
        if cost > self.cash:
            shares = int(self.cash / price / self.config.lot_size) * self.config.lot_size
            cost = price * shares
        if shares == 0:
            return

        if code in self.holdings:
            old = self.holdings[code]
            new_shares = old["shares"] + shares
            new_cost = (old["cost"] * old["shares"] + price * shares) / new_shares
            self.holdings[code] = {"shares": new_shares, "cost": new_cost, "entry_date": old["entry_date"]}
        else:
            self.holdings[code] = {"shares": shares, "cost": price, "entry_date": dt}
        self.cash -= cost

    def _sell(self, code: str, price: float, dt: date, reason: str):
        if code not in self.holdings:
            return
        self.cash += price * self.holdings[code]["shares"]
        del self.holdings[code]
        self.peak_prices.pop(code, None)

    def _partial_sell(self, code: str, price: float, ratio: float, dt: date, reason: str):
        """卖出指定比例的持仓"""
        if code not in self.holdings or ratio <= 0:
            return
        holding = self.holdings[code]
        sell_shares = int(holding["shares"] * ratio / self.config.lot_size) * self.config.lot_size
        if sell_shares <= 0:
            return
        if sell_shares >= holding["shares"]:
            self._sell(code, price, dt, reason)
            return
        self.cash += price * sell_shares
        self.holdings[code] = {
            "shares": holding["shares"] - sell_shares,
            "cost": holding["cost"],
            "entry_date": holding["entry_date"],
        }

    def get_portfolio_value(self, checkpoint: date, prices: dict) -> float:
        total = self.cash
        for code, holding in self.holdings.items():
            plist = prices.get(code, [])
            hist = [(d, p) for d, p in plist if d <= checkpoint]
            if hist:
                total += holding["shares"] * hist[-1][1]
        return total

    def risk_check(self, checkpoint: date, prices: dict,
                   atr_data: dict | None = None):
        """周频风控检查：仅止损/止盈，不做买入和评级降级检查"""
        if not self.holdings:
            return

        current_prices = {}
        for code, plist in prices.items():
            hist = [(d, p) for d, p in plist if d <= checkpoint]
            if hist:
                current_prices[code] = hist[-1][1]

        self._update_peak_prices(current_prices)

        to_sell = []
        for code in list(self.holdings.keys()):
            if code not in current_prices:
                continue
            price = current_prices[code]
            if self.should_atr_stop(code, price, atr_data, checkpoint):
                to_sell.append((code, "ATR止损(周检)"))
            elif self.should_stop_loss(code, price):
                to_sell.append((code, "止损(周检)"))
            elif self.should_trailing_stop(code, price):
                to_sell.append((code, "移动止盈(周检)"))
            elif self.should_take_profit(code, price):
                to_sell.append((code, "止盈(周检)"))
            elif self.should_time_stop(code, price, checkpoint):
                to_sell.append((code, "时间止损(周检)"))

        for code, reason in to_sell:
            self._sell(code, current_prices[code], checkpoint, reason)


# ─────────────────────────────────────────────────
# 回测入口
# ─────────────────────────────────────────────────

def run_advanced_backtest(
    start_date: date | None = None,
    end_date: date | None = None,
    strategy_name: str = "default",
) -> str:
    """运行增强策略回测"""
    config = load_strategy(strategy_name)

    end_date = end_date or date(2026, 3, 27)
    start_date = start_date or end_date - timedelta(days=180)

    engine = get_finance_engine()
    print(f"策略: {config.name} — {config.description}")
    print(f"回测区间: {start_date} ~ {end_date}")

    checkpoints = _get_monthly_checkpoints(start_date, end_date)
    prices = _load_prices_range(engine, start_date, end_date)
    index_prices = _load_index_prices(engine, start_date, end_date)

    use_precomputed = _has_precomputed_signals(engine, start_date, end_date)
    if use_precomputed:
        print("使用预计算历史信号")
    else:
        print("未找到预计算信号，使用代理计算")
        fund_signals = _load_fundamental_signals(engine)
        eps_data = _load_eps_data(engine)

    print(f"检查点: {len(checkpoints)} 个")

    # 周频风控检查：加载周检查点
    weekly_checkpoints = []
    if config.weekly_risk_check:
        weekly_checkpoints = _get_weekly_checkpoints(start_date, end_date)
        # 过滤掉与月度检查点重合的周检查点
        monthly_set = set(checkpoints)
        weekly_checkpoints = [w for w in weekly_checkpoints if w not in monthly_set]
        print(f"周频风控检查: {len(weekly_checkpoints)} 个额外检查点")

    # 自适应权重：提前加载价格缓存（复用已有 prices 或独立加载）
    adaptive_weight_history = []
    if config.adaptive_weights:
        print("启用自适应权重")
        from src.backtest.adaptive_weights import compute_adaptive_weights

    # 动态止损：加载 ATR 数据
    atr_data = None
    if config.atr_stop_enabled:
        print("启用 ATR 自适应止损")
        from src.backtest.atr import compute_all_atr
        atr_data = compute_all_atr(engine, start_date, end_date, period=config.atr_period)
        print(f"ATR 数据: {len(atr_data)} 只股票")
    if config.trailing_stop_enabled:
        print(f"启用移动止盈 (回撤 {config.trailing_stop_pct*100:.0f}%)")
    if config.time_stop_months is not None:
        print(f"启用时间止损 ({config.time_stop_months} 个月)")

    # 宏观择时：预计算全部检查点的 regime
    regime_data = {}
    regime_history = []
    if config.market_regime_enabled:
        print("启用宏观择时")
        from src.backtest.market_regime import precompute_regimes
        regime_data = precompute_regimes(engine, checkpoints)
        print(f"Regime 预计算完成: {len(regime_data)} 个检查点")

    # 行业轮动：预计算全部检查点的板块强度
    sector_data = {}
    sector_history = []
    stock_info = None
    if config.sector_rotation_enabled:
        print("启用行业轮动")
        from src.backtest.sector_rotation import precompute_sector_rotation
        from src.backtest.precompute import _load_stock_info
        from src.db.engine import get_opinion_engine

        stock_info = _load_stock_info(engine)
        opinion_engine = get_opinion_engine()
        sector_data = precompute_sector_rotation(
            opinion_engine,
            checkpoints,
            stock_info,
            window=config.sector_rotation_window,
            strong_threshold=config.sector_strong_threshold,
            weak_threshold=config.sector_weak_threshold,
        )
        print(f"板块轮动预计算完成: {len(sector_data)} 个检查点")

    # 仓位集中度优化：加载流动性数据和历史准确率
    liquidity_data = None
    accuracy_dict = None
    if config.position_concentration_enabled:
        print("启用仓位集中度优化")
        from src.backtest.position_concentration import load_liquidity_data
        from src.backtest.adaptive_weights import compute_signal_accuracy

        if stock_info is None:
            from src.backtest.precompute import _load_stock_info
            stock_info = _load_stock_info(engine)

        liquidity_data = load_liquidity_data(engine, start_date, end_date)
        print(f"流动性数据加载完成: {len(liquidity_data)} 只股票")

        # 计算历史准确率（使用与 adaptive_weights 相同的逻辑）
        accuracy_dict = compute_signal_accuracy(
            engine,
            start_date,
            window_months=config.adaptive_window_months,
            forward_days=config.adaptive_forward_days,
        )
        print(f"历史准确率计算完成: {len(accuracy_dict)} 个信号源")

    # ML 涨停预测信号
    ml_signals = None
    if config.ml_signals_enabled:
        print("启用 ML 涨停预测信号")
        from src.backtest.ml_signals import load_ml_signals
        ml_signals = load_ml_signals(config.ml_signals_path, start_date, end_date)
        print(f"ML 信号加载完成: {len(ml_signals)} 个交易日")

    strategy = AdvancedStrategy(config)

    results = []
    for cp in checkpoints:
        # 计算本检查点的自适应权重（若启用）
        checkpoint_weights = None
        if config.adaptive_weights:
            adaptive_w, accuracy = compute_adaptive_weights(
                engine, cp,
                base_weights=config.rating_weights,
                window_months=config.adaptive_window_months,
                forward_days=config.adaptive_forward_days,
                exponent=config.adaptive_exponent,
                min_floor=config.adaptive_min_weight,
                prices_cache=prices,
            )
            checkpoint_weights = adaptive_w
            adaptive_weight_history.append({
                "date": cp,
                "weights": adaptive_w,
                "accuracy": accuracy,
            })

        ratings = {}

        if use_precomputed:
            precomputed = _load_precomputed_signals(engine, cp)
            for code, signals in precomputed.items():
                if code not in prices:
                    continue
                rating, score = _compute_rating_with_config(
                    signals, config, weights_override=checkpoint_weights
                )
                # 计算共振强度
                res_strength = 0.0
                if config.resonance_strength_enabled:
                    res_strength = _compute_resonance_strength(signals)

                ratings[code] = {
                    "rating": rating,
                    "score": score,
                    "resonance_buy": False,
                    "resonance_strength": res_strength,
                    "detail": signals,
                }
        else:
            from src.backtest.historical_sim import _compute_rating
            for code, plist in prices.items():
                sigs = _compute_signals_at_checkpoint(code, cp, plist, eps_data.get(code), fund_signals)
                if sigs is None:
                    continue
                rating, score = _compute_rating(sigs)
                ratings[code] = {"rating": rating, "score": score, "resonance_buy": False, "resonance_strength": 0.0}

        # 为每只股票附加 ML 分数
        if ml_signals:
            from src.backtest.ml_signals import get_ml_score_at_checkpoint
            for code in ratings:
                ratings[code]["ml_score"] = get_ml_score_at_checkpoint(ml_signals, code, cp)

        # 计算宏观择时仓位乘数（若启用）
        regime_multiplier = 1.0
        if config.market_regime_enabled and cp in regime_data:
            regime, regime_detail = regime_data[cp]
            if regime == "risk_on":
                regime_multiplier = config.regime_risk_on_mult
            elif regime == "risk_off":
                regime_multiplier = config.regime_risk_off_mult
            else:
                regime_multiplier = config.regime_neutral_mult
            regime_history.append({
                "date": cp,
                "regime": regime,
                "multiplier": regime_multiplier,
                "detail": regime_detail,
            })

        # 获取板块分类（若启用）
        sector_classification = None
        if config.sector_rotation_enabled and cp in sector_data:
            sector_classification = sector_data[cp]["classification"]
            sector_history.append({
                "date": cp,
                "classification": sector_classification,
            })

        strategy.rebalance(cp, ratings, prices, atr_data=atr_data,
                           regime_multiplier=regime_multiplier,
                           sector_data=sector_classification,
                           stock_info=stock_info,
                           accuracy_dict=accuracy_dict,
                           liquidity_data=liquidity_data)
        portfolio_value = strategy.get_portfolio_value(cp, prices)
        idx_price = index_prices.get(cp)

        results.append({
            "date": cp,
            "portfolio_value": portfolio_value,
            "cash": strategy.cash,
            "holdings_count": len(strategy.holdings),
            "index_price": idx_price,
        })

        print(f"  {cp}: 净值 {portfolio_value:,.0f}, 持仓 {len(strategy.holdings)} 只, 现金 {strategy.cash:,.0f}")

        # 周频风控检查：在本月度检查点之后、下一月度检查点之前的周检查点
        if config.weekly_risk_check and weekly_checkpoints:
            next_cp = checkpoints[checkpoints.index(cp) + 1] if cp != checkpoints[-1] else end_date
            for week_cp in weekly_checkpoints:
                if cp < week_cp < next_cp:
                    strategy.risk_check(week_cp, prices, atr_data=atr_data)

    return _format_advanced_report(
        results, strategy, config, start_date, end_date,
        adaptive_weight_history=adaptive_weight_history or None,
        regime_history=regime_history or None,
        sector_history=sector_history or None,
    )


# ─────────────────────────────────────────────────
# 报告生成
# ─────────────────────────────────────────────────

def _format_advanced_report(
    results, strategy, config: StrategyConfig, start_date, end_date,
    adaptive_weight_history=None,
    regime_history=None,
    sector_history=None,
) -> str:
    initial = config.initial_capital
    final_value = results[-1]["portfolio_value"]
    total_return = (final_value / initial - 1) * 100

    idx_start = results[0]["index_price"]
    idx_end = results[-1]["index_price"]
    bench_return = (idx_end / idx_start - 1) * 100 if idx_start and idx_end else 0

    peak = initial
    max_dd = 0
    for r in results:
        v = r["portfolio_value"]
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100
        if dd > max_dd:
            max_dd = dd

    days = (end_date - start_date).days
    annual_return = ((final_value / initial) ** (365 / days) - 1) * 100 if days > 0 else 0

    wins = sum(1 for i in range(1, len(results)) if results[i]["portfolio_value"] > results[i-1]["portfolio_value"])
    win_rate = wins / (len(results) - 1) * 100 if len(results) > 1 else 0

    lines = []
    lines.append(f"# 回测报告：{config.name}")
    lines.append("")
    lines.append(f"- 策略: {config.name} — {config.description}")
    lines.append(f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"- 回测区间: {start_date} ~ {end_date}（{days} 天）")
    lines.append(f"- 检查点数: {len(results)} 个（{config.checkpoint_frequency}）")
    lines.append("")

    # ========== 第一部分：回测结果 ==========
    lines.append("---")
    lines.append("")
    lines.append("## 第一部分：回测结果")
    lines.append("")
    lines.append("### 收益统计")
    lines.append("")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 初始资金 | ¥{initial:,.0f} |")
    lines.append(f"| 最终净值 | ¥{final_value:,.0f} |")
    lines.append(f"| **总收益率** | **{total_return:+.2f}%** |")
    lines.append(f"| **年化收益率** | **{annual_return:+.2f}%** |")
    lines.append(f"| 沪深300收益率 | {bench_return:+.2f}% |")
    lines.append(f"| **超额收益** | **{total_return - bench_return:+.2f}%** |")
    lines.append(f"| 最大回撤 | -{max_dd:.2f}% |")
    lines.append(f"| 月度胜率 | {win_rate:.0f}% |")
    lines.append("")

    lines.append("### 净值曲线")
    lines.append("")
    lines.append("| 日期 | 组合净值 | 收益率 | 持仓数 | 现金 |")
    lines.append("|------|---------|--------|--------|------|")
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

    # ========== 第二部分：策略配置（从 config 动态生成）==========
    lines.append("---")
    lines.append("")
    lines.append("## 第二部分：详细交易策略")
    lines.append("")

    # 信号权重
    lines.append("### 一、信号源与权重")
    lines.append("")
    lines.append("| 信号源 | 权重 |")
    lines.append("|--------|------|")
    for src, w in sorted(config.rating_weights.items(), key=lambda x: -x[1]):
        lines.append(f"| {src} | {w*100:.0f}% |")
    lines.append("")

    # 评级阈值
    lines.append("### 二、评级阈值")
    lines.append("")
    lines.append("```")
    lines.append("混合分 = signal方向(bullish=100/neutral=50/bearish=0) × 60% + score × 40%")
    lines.append("综合分 = Σ(权重 × 混合分) / Σ(有效权重)")
    lines.append("")
    thresholds_str = " | ".join(f"{k}: ≥{v}" for k, v in sorted(config.rating_thresholds.items(), key=lambda x: -x[1]))
    lines.append(thresholds_str + " | D: <" + str(min(config.rating_thresholds.values())))
    lines.append("```")
    lines.append("")

    # 买入策略
    lines.append("### 三、买入策略")
    lines.append("")
    lines.append(f"- 检查频率: {config.checkpoint_frequency}")
    lines.append(f"- 买入评级: {', '.join(config.buy_ratings)}")
    lines.append(f"- 最多持有: {config.max_holdings} 只")
    lines.append(f"- 三重共振加仓: ×{config.resonance_boost}")
    lines.append(f"- 高分(>{config.high_score_threshold})加仓: ×{config.high_score_boost}")
    lines.append(f"- 单只上限: {config.max_single_position*100:.0f}%")
    lines.append(f"- 最小调仓差额: ¥{config.min_rebalance_diff:,.0f}")
    lines.append(f"- 交易手数: {config.lot_size} 股")
    lines.append("")
    lines.append("**仓位分配：**")
    lines.append("")
    lines.append("| 评级 | 基础仓位 |")
    lines.append("|------|---------|")
    for rating, pos in sorted(config.position_sizes.items(), key=lambda x: -x[1]):
        lines.append(f"| {rating} | {pos*100:.0f}% |")
    lines.append("")

    # 卖出策略
    lines.append("### 四、卖出策略")
    lines.append("")
    sl = f"{config.stop_loss_pct*100:+.0f}%" if config.stop_loss_pct is not None else "无"
    tp = f"{config.take_profit_pct*100:+.0f}%" if config.take_profit_pct is not None else "不止盈"
    lines.append(f"- 止损: {sl}")
    lines.append(f"- 止盈: {tp}")
    lines.append(f"- 降级卖出: 评级降至 {'/'.join(config.sell_on_downgrade_to)}")
    lines.append("")

    # 动态止损止盈参数（仅在启用时输出）
    dynamic_exits = []
    if config.atr_stop_enabled:
        dynamic_exits.append(f"- ATR 自适应止损: {config.atr_stop_multiplier}×ATR (周期 {config.atr_period})")
    if config.trailing_stop_enabled:
        dynamic_exits.append(f"- 移动止盈: 从最高价回撤 {config.trailing_stop_pct*100:.0f}% 触发")
    if config.time_stop_months is not None:
        dynamic_exits.append(f"- 时间止损: 持仓超 {config.time_stop_months} 个月未盈利则清仓")

    if dynamic_exits:
        lines.append("**动态止损止盈：**")
        lines.append("")
        lines.extend(dynamic_exits)
        lines.append("")

    # 宏观择时参数（仅在启用时输出）
    if config.market_regime_enabled:
        lines.append("**宏观择时：**")
        lines.append("")
        lines.append(f"- risk_on 仓位乘数: {config.regime_risk_on_mult}")
        lines.append(f"- neutral 仓位乘数: {config.regime_neutral_mult}")
        lines.append(f"- risk_off 仓位乘数: {config.regime_risk_off_mult}")
        lines.append("")

    # 行业轮动参数（仅在启用时输出）
    if config.sector_rotation_enabled:
        lines.append("**行业轮动：**")
        lines.append("")
        lines.append(f"- 滚动窗口: {config.sector_rotation_window} 个检查点")
        lines.append(f"- 强势板块乘数: {config.sector_strong_mult}")
        lines.append(f"- 弱势板块乘数: {config.sector_weak_mult}")
        lines.append(f"- 强势阈值: {config.sector_strong_threshold}")
        lines.append(f"- 弱势阈值: {config.sector_weak_threshold}")
        lines.append("")

    # 仓位集中度优化参数（仅在启用时输出）
    if config.position_concentration_enabled:
        lines.append("**仓位集中度优化：**")
        lines.append("")
        lines.append("信号一致性乘数:")
        lines.append(f"- 5 个信号源看多: {config.consensus_5_mult}")
        lines.append(f"- 4 个信号源看多: {config.consensus_4_mult}")
        lines.append(f"- 3 个信号源看多: {config.consensus_3_mult} (基准)")
        lines.append(f"- 2 个或更少: {config.consensus_2_mult}")
        lines.append("")
        lines.append("历史胜率乘数:")
        lines.append(f"- 准确率 > {config.accuracy_high_threshold}: {config.accuracy_high_mult}")
        lines.append(f"- 准确率 > {config.accuracy_mid_threshold}: {config.accuracy_mid_mult}")
        lines.append("")
        lines.append("流动性乘数:")
        lines.append(f"- 日均成交额 >= {config.liquidity_high_threshold/1e8:.1f}亿: 1.0")
        lines.append(f"- 日均成交额 >= {config.liquidity_mid_threshold/1e8:.1f}亿: {config.liquidity_mid_mult}")
        lines.append(f"- 日均成交额 < {config.liquidity_low_threshold/1e8:.1f}亿: {config.liquidity_low_mult}")
        lines.append("")

    # 分批建仓/出场参数（仅在启用时输出）
    if config.scaling_enabled:
        lines.append("**分批建仓/出场：**")
        lines.append("")
        lines.append(f"- 首次建仓比例: {config.scaling_in_initial*100:.0f}%")
        lines.append(f"- 分批止盈阈值: +{config.scaling_profit_threshold*100:.0f}%")
        lines.append(f"- 分批止盈比例: {config.scaling_profit_ratio*100:.0f}%")
        lines.append(f"- 部分降级减仓比例: {config.downgrade_partial_ratio*100:.0f}%")
        lines.append(f"- 部分降级评级: {', '.join(config.downgrade_partial_ratings)}")
        lines.append("")

    # 周频风控检查参数（仅在启用时输出）
    if config.weekly_risk_check:
        lines.append("**周频风控检查：**")
        lines.append("")
        lines.append("- 评级调仓: 月度")
        lines.append("- 止损/止盈检查: 周频")
        lines.append("")

    # 组合优化参数（仅在启用时输出）
    if config.portfolio_opt_enabled:
        lines.append("**组合优化：**")
        lines.append("")
        lines.append(f"- 相关性阈值: {config.max_correlation}")
        lines.append(f"- 相关性窗口: {config.correlation_window_days} 天")
        lines.append(f"- 行业集中度上限: {config.max_industry_pct*100:.0f}%")
        lines.append("")

    # 多层级共振参数（仅在启用时输出）
    if config.resonance_strength_enabled:
        lines.append("**多层级共振：**")
        lines.append("")
        lines.append(f"- 最大加仓幅度: {config.resonance_max_boost}")
        lines.append("- 公式: boost = 1.0 + resonance_strength × max_boost")
        lines.append("- resonance_strength = (价值面 × 技术面 × 情绪面) ^ (1/3)")
        lines.append("")

    # 反向信号参数（仅在启用时输出）
    if config.contrarian_enabled:
        lines.append("**反向信号 / 黄金坑：**")
        lines.append("")
        lines.append(f"- 估值最低分: {config.contrarian_valuation_min}")
        lines.append(f"- 仓位比例: {config.contrarian_position_ratio*100:.0f}% (A级基础仓位)")
        lines.append("- 触发条件: valuation ≥ 85 AND chan = bearish")
        lines.append("")

    # ML 涨停预测信号参数（仅在启用时输出）
    if config.ml_signals_enabled:
        lines.append("**ML 涨停预测信号：**")
        lines.append("")
        lines.append(f"- 信号路径: {config.ml_signals_path}")
        lines.append(f"- 强信号加仓 (score >= 0.85): {config.ml_boost_strong}x")
        lines.append(f"- 中信号加仓 (score >= 0.70): {config.ml_boost_mid}x")
        lines.append("")

    # 自适应权重演化（仅在启用时输出）
    if adaptive_weight_history:
        sources = list(config.rating_weights.keys())

        lines.append("### 五、自适应权重演化")
        lines.append("")
        header = "| 检查点 | " + " | ".join(sources) + " |"
        sep = "|--------|" + "|".join(["--------"] * len(sources)) + "|"
        lines.append(header)
        lines.append(sep)
        for entry in adaptive_weight_history:
            cols = " | ".join(
                f"{entry['weights'].get(s, 0)*100:.1f}%" for s in sources
            )
            lines.append(f"| {entry['date']} | {cols} |")
        lines.append("")

        lines.append("### 六、信号源准确率（滚动窗口）")
        lines.append("")
        lines.append(header)
        lines.append(sep)
        for entry in adaptive_weight_history:
            cols = " | ".join(
                f"{entry['accuracy'].get(s, 0)*100:.1f}%"
                if entry["accuracy"].get(s) is not None
                else "N/A"
                for s in sources
            )
            lines.append(f"| {entry['date']} | {cols} |")
        lines.append("")

    # 宏观择时状态演化（仅在启用时输出）
    if regime_history:
        lines.append("### 七、宏观择时状态")
        lines.append("")
        lines.append("| 检查点 | Regime | 仓位乘数 | 趋势 | 宽度 | 波动率 | 总分 |")
        lines.append("|--------|--------|---------|------|------|--------|------|")
        for entry in regime_history:
            regime = entry["regime"]
            mult = entry["multiplier"]
            detail = entry["detail"]
            trend_score = detail["trend"]["score"]
            breadth_score = detail["breadth"]["score"]
            vol_score = detail["volatility"]["score"]
            total = detail["total_score"]
            lines.append(
                f"| {entry['date']} | {regime} | {mult} | "
                f"{trend_score:+d} | {breadth_score:+d} | {vol_score:+d} | {total:+d} |"
            )
        lines.append("")

    # 行业轮动状态（仅在启用时输出）
    if sector_history:
        lines.append("### 八、行业轮动状态")
        lines.append("")
        for entry in sector_history:
            classification = entry["classification"]
            strong = classification["strong"]
            weak = classification["weak"]
            scores = classification["scores"]

            lines.append(f"**{entry['date']}**")
            lines.append("")

            if strong:
                lines.append("强势板块:")
                for sector in sorted(strong, key=lambda s: scores.get(s, 0), reverse=True)[:5]:
                    score = scores.get(sector, 0)
                    lines.append(f"- {sector}: {score:+.2f}")
                lines.append("")

            if weak:
                lines.append("弱势板块:")
                for sector in sorted(weak, key=lambda s: scores.get(s, 0))[:5]:
                    score = scores.get(sector, 0)
                    lines.append(f"- {sector}: {score:+.2f}")
                lines.append("")

    return "\n".join(lines)
