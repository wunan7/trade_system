"""A股集成决策引擎 — SQLAlchemy 连接管理"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from src.config import FINANCE_DB_URL, OPINION_DB_URL

# 主库（finance）— stock_signals / integrated_ratings / 财务数据
_finance_engine = None
_FinanceSession = None

# 舆情库（finance_public_opinion）— TrendRadar 数据
_opinion_engine = None
_OpinionSession = None


def get_finance_engine():
    global _finance_engine
    if _finance_engine is None:
        _finance_engine = create_engine(
            FINANCE_DB_URL, pool_pre_ping=True, pool_size=5, max_overflow=10
        )
    return _finance_engine


def get_finance_session() -> Session:
    global _FinanceSession
    if _FinanceSession is None:
        _FinanceSession = sessionmaker(bind=get_finance_engine())
    return _FinanceSession()


def get_opinion_engine():
    global _opinion_engine
    if _opinion_engine is None:
        _opinion_engine = create_engine(
            OPINION_DB_URL, pool_pre_ping=True, pool_size=2, max_overflow=5
        )
    return _opinion_engine


def get_opinion_session() -> Session:
    global _OpinionSession
    if _OpinionSession is None:
        _OpinionSession = sessionmaker(bind=get_opinion_engine())
    return _OpinionSession()
