"""策略配置管理 — 加载/保存/列出策略"""

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path

STRATEGIES_DIR = Path(__file__).resolve().parent.parent.parent / "strategies"


@dataclass
class StrategyConfig:
    name: str = "default"
    description: str = ""

    # 信号权重
    rating_weights: dict = field(default_factory=lambda: {
        "screener": 0.25, "valuation": 0.30,
        "buffett": 0.20, "munger": 0.15, "chan": 0.10,
    })

    # 评级阈值
    rating_thresholds: dict = field(default_factory=lambda: {
        "A+": 85, "A": 70, "B": 55, "C": 40,
    })

    # 买入条件
    buy_ratings: list = field(default_factory=lambda: ["A+", "A"])
    position_sizes: dict = field(default_factory=lambda: {"A+": 0.20, "A": 0.15})
    resonance_boost: float = 1.5
    high_score_boost: float = 1.2
    high_score_threshold: float = 80
    max_single_position: float = 0.25
    max_holdings: int = 10

    # 风控
    stop_loss_pct: float = -0.10
    take_profit_pct: float | None = None
    sell_on_downgrade_to: list = field(default_factory=lambda: ["C", "D"])

    # 执行
    checkpoint_frequency: str = "monthly"
    initial_capital: float = 100000
    min_rebalance_diff: float = 1000
    lot_size: int = 100

    # 自适应权重（可选，默认关闭）
    adaptive_weights: bool = False
    adaptive_window_months: int = 6
    adaptive_forward_days: int = 20
    adaptive_exponent: float = 2.0
    adaptive_min_weight: float = 0.05
    adaptive_smoothing: float = 0.3

    # 动态止损止盈（可选，默认关闭）
    atr_stop_enabled: bool = False
    atr_stop_multiplier: float = 2.0
    atr_period: int = 20
    trailing_stop_enabled: bool = False
    trailing_stop_pct: float = 0.15
    time_stop_months: int | None = None

    # 宏观择时（可选，默认关闭）
    market_regime_enabled: bool = False
    regime_risk_on_mult: float = 1.2
    regime_neutral_mult: float = 1.0
    regime_risk_off_mult: float = 0.5

    # 行业轮动（可选，默认关闭）
    sector_rotation_enabled: bool = False
    sector_rotation_window: int = 3
    sector_strong_mult: float = 1.3
    sector_weak_mult: float = 0.7
    sector_strong_threshold: float = 1.5
    sector_weak_threshold: float = -1.5

    # 仓位集中度优化（可选，默认关闭）
    position_concentration_enabled: bool = False
    consensus_5_mult: float = 1.3
    consensus_4_mult: float = 1.15
    consensus_3_mult: float = 1.0
    consensus_2_mult: float = 0.85
    accuracy_high_threshold: float = 0.70
    accuracy_high_mult: float = 1.2
    accuracy_mid_threshold: float = 0.60
    accuracy_mid_mult: float = 1.1
    liquidity_high_threshold: float = 100_000_000
    liquidity_mid_threshold: float = 50_000_000
    liquidity_low_threshold: float = 10_000_000
    liquidity_mid_mult: float = 0.85
    liquidity_low_mult: float = 0.7

    # 分批建仓/出场（可选，默认关闭）
    scaling_enabled: bool = False
    scaling_in_initial: float = 0.5
    scaling_profit_threshold: float = 0.15
    scaling_profit_ratio: float = 0.33
    downgrade_partial_ratio: float = 0.5
    downgrade_partial_ratings: list = field(default_factory=lambda: ["B"])

    # 周频风控检查（可选，默认关闭）
    weekly_risk_check: bool = False

    # 组合优化（可选，默认关闭）
    portfolio_opt_enabled: bool = False
    correlation_constraint_enabled: bool = True  # 相关性约束开关
    max_correlation: float = 0.7  # 高相关阈值
    mid_correlation: float = 0.5  # 中相关阈值
    correlation_window_days: int = 60
    max_industry_pct: float = 0.30

    # 多层级共振（可选，默认关闭）
    resonance_strength_enabled: bool = False
    resonance_max_boost: float = 0.5

    # 反向信号 / 黄金坑（可选，默认关闭）
    contrarian_enabled: bool = False
    contrarian_valuation_min: float = 85
    contrarian_position_ratio: float = 0.5


def load_strategy(name: str = "default") -> StrategyConfig:
    """从 strategies/{name}.json 加载策略配置"""
    path = STRATEGIES_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"策略文件不存在: {path}")

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    return StrategyConfig(**data)


def save_strategy(config: StrategyConfig) -> Path:
    """保存策略配置到 strategies/{name}.json"""
    STRATEGIES_DIR.mkdir(exist_ok=True)
    path = STRATEGIES_DIR / f"{config.name}.json"

    data = asdict(config)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return path


def list_strategies() -> list[dict]:
    """列出所有策略"""
    if not STRATEGIES_DIR.exists():
        return []

    result = []
    for p in sorted(STRATEGIES_DIR.glob("*.json")):
        try:
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
            result.append({
                "name": data.get("name", p.stem),
                "description": data.get("description", ""),
                "file": str(p),
            })
        except Exception:
            continue

    return result
