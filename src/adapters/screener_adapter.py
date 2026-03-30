"""Screener 适配器 — 调用 a-stock-screener 的 8 维财报评分"""

import sys
from datetime import date

from src.config import SUBSYSTEM_PATHS
from src.adapters.base import BaseAdapter

# 评级 → 信号映射
_RATING_MAP = {
    "极优": "bullish",
    "优秀": "bullish",
    "合格": "neutral",
    "观望": "bearish",
    "排除": "bearish",
}


class ScreenerAdapter(BaseAdapter):
    source = "screener"

    def run(self, analysis_date: date | None = None) -> list[dict]:
        analysis_date = analysis_date or date.today()

        # 动态导入子系统
        screener_path = str(SUBSYSTEM_PATHS["screener"])
        if screener_path not in sys.path:
            sys.path.insert(0, screener_path)

        from batch_score import batch_score, load_stock_data, load_stock_info, score_stock

        data_dir = SUBSYSTEM_PATHS["screener"] / "data"
        stocks = load_stock_data(str(data_dir))
        stock_info = load_stock_info(str(data_dir))

        if not stocks:
            print("[screener] 无数据，请先运行 batch_fetch_db.py")
            return []

        results = []
        for code, info in stocks.items():
            name = info["name"]
            df = info["df"]
            industry = stock_info.get(code, {}).get("industry", "")

            r = score_stock(code, name, df, industry)
            rating = r.get("rating", "排除")
            signal = _RATING_MAP.get(rating, "neutral")
            score = r.get("total_score", 0)

            results.append({
                "code": code,
                "date": analysis_date,
                "source": self.source,
                "signal": signal,
                "score": round(score, 2),
                "confidence": round(min(score, 100), 1),
                "detail_json": {
                    "rating": rating,
                    "industry": industry,
                    "dim2_growth": r.get("dim2_growth"),
                    "dim3_profitability": r.get("dim3_profitability"),
                    "dim4_balance_sheet": r.get("dim4_balance_sheet"),
                    "dim5_cashflow": r.get("dim5_cashflow"),
                    "dim6_capital_allocation": r.get("dim6_capital_allocation"),
                    "dim7_resilience": r.get("dim7_resilience"),
                    "dim8_competitive_advantage": r.get("dim8_competitive_advantage"),
                },
            })

        print(f"[screener] 评分完成: {len(results)} 只股票")
        return results
