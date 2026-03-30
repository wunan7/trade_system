"""Valuation 适配器 — 调用 a-stock-valuation 的 4 模型估值"""

import os
import sys
from datetime import date

from src.config import SUBSYSTEM_PATHS
from src.adapters.base import BaseAdapter


class ValuationAdapter(BaseAdapter):
    source = "valuation"

    def run(self, analysis_date: date | None = None) -> list[dict]:
        analysis_date = analysis_date or date.today()

        # 动态导入子系统
        val_path = str(SUBSYSTEM_PATHS["valuation"])
        if val_path not in sys.path:
            sys.path.insert(0, val_path)

        # 静默批量模式
        os.environ["BATCH_MODE"] = "1"

        from valuation import analyze_stock
        from finance_data.db.query import get_stock_list

        stock_list = get_stock_list(exclude_st=True)
        results = []
        errors = 0

        for i, info in enumerate(stock_list):
            code = info["code"]
            try:
                r = analyze_stock(code)
            except Exception:
                errors += 1
                continue

            if "error" in r:
                continue

            signal = r.get("signal", "neutral")
            confidence = r.get("confidence", 50)
            weighted_gap = r.get("weighted_gap", 0)

            # score: 将 weighted_gap (-100~+100) 映射到 0-100
            score = max(0, min(100, 50 + weighted_gap))

            results.append({
                "code": code,
                "date": analysis_date,
                "source": self.source,
                "signal": signal,
                "score": round(score, 2),
                "confidence": confidence,
                "detail_json": {
                    "weighted_gap": weighted_gap,
                    "wacc": r.get("wacc"),
                    "methods": r.get("methods"),
                    "dcf_scenarios": r.get("dcf_scenarios"),
                    "key_metrics": r.get("key_metrics"),
                },
            })

            if (i + 1) % 500 == 0:
                print(f"[valuation] 进度: {i + 1}/{len(stock_list)}")

        print(f"[valuation] 完成: {len(results)} 只, 跳过: {errors}")
        return results
