"""多策略组合引擎 — 动态配置资金比例，管理多个子策略的协同运行"""

import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from src.db.engine import get_finance_engine
from src.backtest.strategy_config import load_strategy
from src.backtest.advanced_strategy import AdvancedStrategy, _compute_rating_with_config
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

ENSEMBLES_DIR = Path(__file__).resolve().parent.parent.parent / "ensembles"


@dataclass
class EnsembleConfig:
    name: str = "ensemble"
    description: str = "多策略组合"
    initial_capital: float = 100000
    sub_strategies: list[str] = field(default_factory=list)
    regime_allocations: dict[str, dict[str, float]] = field(default_factory=dict)


def load_ensemble(name: str) -> EnsembleConfig:
    """加载组合策略配置"""
    path = ENSEMBLES_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"组合配置文件不存在: {path}")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return EnsembleConfig(**data)


class EnsembleStrategy:
    """多策略组合容器"""

    def __init__(self, config: EnsembleConfig):
        self.config = config
        self.initial_capital = config.initial_capital

        # 实例化所有子策略
        self.sub_strategies = {}
        for sub_name in config.sub_strategies:
            sub_cfg = load_strategy(sub_name)
            # 覆盖初始资金为组合的默认资金，随后按比例分配
            sub_cfg.initial_capital = 0
            self.sub_strategies[sub_name] = AdvancedStrategy(sub_cfg)

        self.strategy_capital = {name: 0.0 for name in self.sub_strategies}
        self.current_regime = "neutral"

    def _get_target_allocation(self, regime: str) -> dict[str, float]:
        """获取目标资金比例"""
        if regime in self.config.regime_allocations:
            return self.config.regime_allocations[regime]

        # fallback: 平均分配
        n = len(self.sub_strategies)
        return {name: 1.0 / n for name in self.sub_strategies}

    def allocate_initial_capital(self, regime: str):
        """初始资金分配"""
        self.current_regime = regime
        allocation = self._get_target_allocation(regime)
        for name, strategy in self.sub_strategies.items():
            weight = allocation.get(name, 0.0)
            capital = self.initial_capital * weight
            self.strategy_capital[name] = capital
            strategy.initial_capital = capital
            strategy.cash = capital

    def rebalance_ensemble_capital(self, cp: date, regime: str, prices: dict):
        """
        动态资金调拨：当宏观状态变化时，调整子策略间的资金配比
        """
        if regime == self.current_regime:
            return

        self.current_regime = regime
        allocation = self._get_target_allocation(regime)

        # 计算组合总价值
        total_value = sum(
            strat.get_portfolio_value(cp, prices)
            for strat in self.sub_strategies.values()
        )

        current_prices = {}
        for code, plist in prices.items():
            hist = [(d, p) for d, p in plist if d <= cp]
            if hist:
                current_prices[code] = hist[-1][1]

        # 调拨池
        cash_pool = 0.0

        # 阶段 1：抽离超出目标比例的资金
        for name, strategy in self.sub_strategies.items():
            target_weight = allocation.get(name, 0.0)
            target_capital = total_value * target_weight
            current_value = strategy.get_portfolio_value(cp, prices)

            if current_value > target_capital:
                # 需要抽走资金
                amount_to_withdraw = current_value - target_capital
                if strategy.cash >= amount_to_withdraw:
                    # 现金足够，直接抽走
                    strategy.cash -= amount_to_withdraw
                    cash_pool += amount_to_withdraw
                else:
                    # 现金不足，需要卖股票凑钱
                    shortfall = amount_to_withdraw - strategy.cash
                    # 强平
                    strategy.force_liquidate(shortfall, current_prices, cp)

                    # 能抽多少抽多少
                    actual_withdraw = min(amount_to_withdraw, strategy.cash)
                    strategy.cash -= actual_withdraw
                    cash_pool += actual_withdraw

            # 更新目标追踪
            self.strategy_capital[name] = target_capital

        # 阶段 2：将抽离的资金注入需要增加配置的策略
        needs = []
        total_needs = 0.0
        for name, strategy in self.sub_strategies.items():
            target_weight = allocation.get(name, 0.0)
            target_capital = total_value * target_weight
            current_value = strategy.get_portfolio_value(cp, prices)

            if target_capital > current_value:
                need = target_capital - current_value
                needs.append((name, strategy, need))
                total_needs += need

        # 按需求比例分配 cash_pool
        for name, strategy, need in needs:
            if total_needs > 0:
                share = (need / total_needs) * cash_pool
                strategy.cash += share

    def get_portfolio_value(self, cp: date, prices: dict) -> float:
        return sum(strat.get_portfolio_value(cp, prices) for strat in self.sub_strategies.values())

    def get_total_cash(self) -> float:
        return sum(strat.cash for strat in self.sub_strategies.values())

    def get_total_holdings_count(self) -> int:
        return sum(len(strat.holdings) for strat in self.sub_strategies.values())


def _build_ml_ratings(ml_signals: dict, checkpoint: date, prices: dict) -> dict:
    """基于 ML 涨停预测信号构建评级，Top 50 股票直接给 A+ 评级"""
    from src.backtest.ml_signals import get_ml_score_at_checkpoint
    ratings = {}
    # 找到最近的信号日
    best_date = None
    for d in ml_signals:
        if d <= checkpoint and (best_date is None or d > best_date):
            best_date = d
    if best_date is None:
        return ratings
    for code, score in ml_signals[best_date].items():
        if code not in prices:
            continue
        if score >= 0.70:
            ratings[code] = {
                "rating": "A+" if score >= 0.85 else "A",
                "score": score * 100,
                "resonance_buy": False,
                "resonance_strength": 0.0,
                "ml_score": score,
            }
    return ratings


def run_ensemble_backtest(
    start_date: date | None = None,
    end_date: date | None = None,
    ensemble_name: str = "tactical",
) -> str:
    """运行多策略组合回测"""
    config = load_ensemble(ensemble_name)

    end_date = end_date or date(2026, 3, 27)
    start_date = start_date or end_date - timedelta(days=180)

    engine = get_finance_engine()
    print(f"组合策略: {config.name} — {config.description}")
    print(f"包含子策略: {', '.join(config.sub_strategies)}")
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

    ensemble = EnsembleStrategy(config)

    # 预计算所有可能的全局数据
    regime_data = {}
    regime_history = []
    from src.backtest.market_regime import precompute_regimes
    regime_data = precompute_regimes(engine, checkpoints)
    print(f"Regime 预计算完成: {len(regime_data)} 个检查点")

    # 预加载所有子策略可能需要的数据
    has_atr = any(s.config.atr_stop_enabled for s in ensemble.sub_strategies.values())
    atr_data = None
    if has_atr:
        print("加载 ATR 数据用于子策略风控")
        from src.backtest.atr import compute_all_atr
        atr_data = compute_all_atr(engine, start_date, end_date)

    # 加载 ML 信号（如果有 ml_momentum 子策略）
    ml_signals = None
    has_ml = any(s.config.name == "ml_momentum" for s in ensemble.sub_strategies.values())
    if has_ml:
        print("加载 ML 涨停预测信号")
        from src.backtest.ml_signals import load_ml_signals
        ml_path = None
        for s in ensemble.sub_strategies.values():
            if s.config.name == "ml_momentum":
                ml_path = r"C:\Users\wunan\GITHUB\vnpy\examples\limit_up_prediction\signals"
                break
        if ml_path:
            ml_signals = load_ml_signals(ml_path, start_date, end_date)
            print(f"ML 信号加载完成: {len(ml_signals)} 个交易日")

    # 初始化资金分配
    first_regime = regime_data.get(checkpoints[0], ("neutral", {}))[0]
    ensemble.allocate_initial_capital(first_regime)

    results = []
    sub_results = defaultdict(list)

    for cp in checkpoints:
        # 1. 判断全局 Regime
        regime, regime_detail = regime_data.get(cp, ("neutral", {}))
        regime_history.append({
            "date": cp,
            "regime": regime,
            "detail": regime_detail,
        })

        # 2. 根据 Regime 调整组合资金分配
        ensemble.rebalance_ensemble_capital(cp, regime, prices)

        # 3. 准备评级信号
        ratings = {}
        if use_precomputed:
            precomputed = _load_precomputed_signals(engine, cp)
            for code, signals in precomputed.items():
                if code not in prices:
                    continue
                # 这里只保存原始信号，具体的加权由每个子策略的 rebalance 独立计算
                # 这是一个架构权衡，为了解耦，我们把计算放入子策略
                ratings[code] = {"detail": signals, "score": 0, "rating": "D"}
        else:
            from src.backtest.historical_sim import _compute_rating
            for code, plist in prices.items():
                sigs = _compute_signals_at_checkpoint(code, cp, plist, eps_data.get(code), fund_signals)
                if sigs is None:
                    continue
                ratings[code] = {"detail": sigs, "score": 0, "rating": "D"}

        # 4. 执行各个子策略的 rebalance
        for name, strategy in ensemble.sub_strategies.items():
            # ML 动量子策略：直接用 ML 信号选股
            if ml_signals and strategy.config.name == "ml_momentum":
                sub_ratings = _build_ml_ratings(ml_signals, cp, prices)
            else:
                # 常规子策略：用基本面评级
                sub_ratings = {}
                for code, info in ratings.items():
                    signals = info["detail"]
                    rating, score = _compute_rating_with_config(signals, strategy.config)
                    sub_ratings[code] = {
                        "rating": rating,
                        "score": score,
                        "detail": signals,
                        "resonance_buy": False
                    }

            strategy.rebalance(cp, sub_ratings, prices, atr_data=atr_data)

            # 记录子策略状态
            sub_val = strategy.get_portfolio_value(cp, prices)
            sub_results[name].append({
                "date": cp,
                "value": sub_val,
                "cash": strategy.cash,
                "holdings": len(strategy.holdings)
            })

        # 5. 汇总组合净值
        portfolio_value = ensemble.get_portfolio_value(cp, prices)
        results.append({
            "date": cp,
            "portfolio_value": portfolio_value,
            "cash": ensemble.get_total_cash(),
            "holdings_count": ensemble.get_total_holdings_count(),
            "index_price": index_prices.get(cp),
            "regime": regime
        })

        # 打印日志
        sub_stats = ", ".join(f"{name}: {sub_results[name][-1]['value']/1000:.0f}k" for name in ensemble.sub_strategies)
        print(f"  {cp} [{regime[:3]}]: 净值 {portfolio_value:,.0f} | {sub_stats}")

    return _format_ensemble_report(results, sub_results, config, start_date, end_date)


def _format_ensemble_report(results, sub_results, config, start_date, end_date) -> str:
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
        if v > peak: peak = v
        dd = (peak - v) / peak * 100
        if dd > max_dd: max_dd = dd

    days = (end_date - start_date).days
    annual_return = ((final_value / initial) ** (365 / days) - 1) * 100 if days > 0 else 0

    lines = []
    lines.append(f"# 组合策略回测报告：{config.name}")
    lines.append("")
    lines.append(f"- 描述: {config.description}")
    lines.append(f"- 包含子策略: {', '.join(config.sub_strategies)}")
    lines.append(f"- 回测区间: {start_date} ~ {end_date}")
    lines.append("")

    lines.append("## 一、组合总体表现")
    lines.append("")
    lines.append("| 指标 | 数值 |")
    lines.append("|------|------|")
    lines.append(f"| 初始资金 | ¥{initial:,.0f} |")
    lines.append(f"| 最终净值 | ¥{final_value:,.0f} |")
    lines.append(f"| **总收益率** | **{total_return:+.2f}%** |")
    lines.append(f"| **年化收益率** | **{annual_return:+.2f}%** |")
    lines.append(f"| 沪深300收益 | {bench_return:+.2f}% |")
    lines.append(f"| 超额收益 | {total_return - bench_return:+.2f}% |")
    lines.append(f"| **最大回撤** | **-{max_dd:.2f}%** |")
    lines.append("")

    lines.append("## 二、资金调拨矩阵")
    lines.append("")
    lines.append("| Regime | " + " | ".join(config.sub_strategies) + " |")
    lines.append("|--------|" + "|".join(["--------"] * len(config.sub_strategies)) + "|")
    for regime, allocs in config.regime_allocations.items():
        cols = " | ".join(f"{allocs.get(s, 0)*100:.0f}%" for s in config.sub_strategies)
        lines.append(f"| {regime} | {cols} |")
    lines.append("")

    lines.append("## 三、净值与宏观状态曲线")
    lines.append("")
    lines.append("| 日期 | 状态 | 组合净值 | " + " | ".join(config.sub_strategies) + " |")
    lines.append("|------|------|----------|" + "|".join(["----------"] * len(config.sub_strategies)) + "|")

    for i, r in enumerate(results):
        date_str = r['date']
        regime = r['regime']
        total_val = r['portfolio_value']

        sub_vals = []
        for name in config.sub_strategies:
            val = sub_results[name][i]['value']
            sub_vals.append(f"¥{val:,.0f}")

        lines.append(f"| {date_str} | {regime} | ¥{total_val:,.0f} | " + " | ".join(sub_vals) + " |")

    return "\n".join(lines)
