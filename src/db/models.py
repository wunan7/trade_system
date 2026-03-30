"""A股集成决策引擎 — ORM 模型定义"""

from datetime import date, datetime

from sqlalchemy import (
    Boolean, Column, Date, Float, String, Text, DateTime,
    PrimaryKeyConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class StockSignal(Base):
    """各子系统分析信号的统一落地表"""

    __tablename__ = "stock_signals"

    code = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)
    source = Column(String(30), nullable=False)  # screener/valuation/buffett/munger/chan/trendradar
    signal = Column(String(10), nullable=False)   # bullish/bearish/neutral
    score = Column(Float)                          # 归一化 0-100
    confidence = Column(Float)                     # 0-100
    detail_json = Column(JSONB)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        PrimaryKeyConstraint("code", "date", "source"),
    )

    def __repr__(self):
        return f"<StockSignal {self.code} {self.date} {self.source}={self.signal}>"


class IntegratedRating(Base):
    """综合评级结果表"""

    __tablename__ = "integrated_ratings"

    code = Column(String(10), nullable=False)
    date = Column(Date, nullable=False)
    rating = Column(String(5))                     # A+/A/B/C/D
    weighted_score = Column(Float)                 # 0-100
    resonance_buy = Column(Boolean, default=False)
    resonance_sell = Column(Boolean, default=False)
    position_pct = Column(Float)                   # 建议仓位%
    detail_json = Column(JSONB)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        PrimaryKeyConstraint("code", "date"),
    )

    def __repr__(self):
        return f"<IntegratedRating {self.code} {self.date} {self.rating}>"
