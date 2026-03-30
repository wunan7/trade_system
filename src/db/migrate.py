"""A股集成决策引擎 — 数据库建表脚本"""

from src.db.engine import get_finance_engine
from src.db.models import Base


def migrate():
    """在 finance 库中创建 stock_signals 和 integrated_ratings 表"""
    engine = get_finance_engine()
    Base.metadata.create_all(engine)
    print("数据库迁移完成: stock_signals, integrated_ratings 表已就绪")


if __name__ == "__main__":
    migrate()
