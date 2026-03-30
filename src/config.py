"""A股集成决策引擎 — 统一配置"""

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# 项目路径
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# 子系统路径
SUBSYSTEM_PATHS = {
    "screener": Path(os.getenv("SCREENER_PATH", r"C:\Users\wunan\projects\a-stock-screener")),
    "valuation": Path(os.getenv("VALUATION_PATH", r"C:\Users\wunan\projects\a-stock-valuation")),
    "analysis": Path(os.getenv("ANALYSIS_PATH", r"C:\Users\wunan\projects\a-stock-analysis")),
    "chan": Path(os.getenv("CHAN_PATH", r"C:\Users\wunan\projects\a-stock-chan")),
    "trendradar": Path(os.getenv("TRENDRADAR_PATH", r"C:\Users\wunan\projects\TrendRadar-master")),
    "finance_data": Path(os.getenv("FINANCE_DATA_PATH", r"C:\Users\wunan\projects\finance_data")),
}

# ---------------------------------------------------------------------------
# 数据库
# ---------------------------------------------------------------------------
FINANCE_DB_URL = os.getenv(
    "FINANCE_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/finance",
)

# TrendRadar 使用独立数据库
OPINION_DB_URL = os.getenv(
    "OPINION_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/finance_public_opinion",
)

# ---------------------------------------------------------------------------
# 综合评级权重
# ---------------------------------------------------------------------------
RATING_WEIGHTS = {
    "screener": 0.25,
    "valuation": 0.30,
    "buffett": 0.20,
    "munger": 0.15,
    "chan": 0.10,
}

# 评级阈值
RATING_THRESHOLDS = {
    "A+": 85,
    "A": 70,
    "B": 55,
    "C": 40,
    # D: < 40
}

# ---------------------------------------------------------------------------
# 三重共振参数
# ---------------------------------------------------------------------------
RESONANCE = {
    "chan_min_score": 55,       # 缠论最低分
    "chan_lookback_days": 5,    # 缠论信号有效天数
    "trendradar_min_confidence": 0.6,  # 舆情最低置信度
}

# ---------------------------------------------------------------------------
# 仓位计算
# ---------------------------------------------------------------------------
POSITION = {
    "A+": 1.0,
    "A": 0.8,
    "B": 0.5,
    "C": 0.3,
    "D": 0.0,
}

# ---------------------------------------------------------------------------
# 推送配置
# ---------------------------------------------------------------------------
PUSH = {
    "feishu_webhook_url": os.getenv("FEISHU_WEBHOOK_URL", ""),
    "dingtalk_webhook_url": os.getenv("DINGTALK_WEBHOOK_URL", ""),
}
