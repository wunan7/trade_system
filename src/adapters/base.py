"""适配器基类 — 定义统一接口"""

import sys
from abc import ABC, abstractmethod
from datetime import date
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.config import SUBSYSTEM_PATHS
from src.db.engine import get_finance_session
from src.db.models import StockSignal

# finance_data 是多个子系统的公共依赖，统一注入
_fd_path = str(SUBSYSTEM_PATHS["finance_data"])
if _fd_path not in sys.path:
    sys.path.insert(0, _fd_path)


class BaseAdapter(ABC):
    """所有子系统适配器的基类"""

    source: str = ""  # 子类需指定: screener / valuation / buffett / munger / chan / trendradar

    @abstractmethod
    def run(self, analysis_date: date | None = None) -> list[dict]:
        """
        运行分析，返回信号列表。
        每个 dict 包含: code, date, source, signal, score, confidence, detail_json
        """
        ...

    def save(self, results: list[dict]) -> int:
        """批量 upsert 到 stock_signals 表，分批写入避免超大 SQL"""
        if not results:
            return 0

        BATCH_SIZE = 500
        session = get_finance_session()
        try:
            for i in range(0, len(results), BATCH_SIZE):
                batch = results[i:i + BATCH_SIZE]
                stmt = pg_insert(StockSignal).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["code", "date", "source"],
                    set_={
                        "signal": stmt.excluded.signal,
                        "score": stmt.excluded.score,
                        "confidence": stmt.excluded.confidence,
                        "detail_json": stmt.excluded.detail_json,
                    },
                )
                session.execute(stmt)
            session.commit()
            return len(results)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def collect(self, analysis_date: date | None = None) -> int:
        """运行分析并保存结果，返回写入行数"""
        results = self.run(analysis_date)
        return self.save(results)
