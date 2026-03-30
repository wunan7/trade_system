"""Munger 适配器 — 调用 a-stock-analysis 的 Munger 纯规则批量评分"""

import sys
from datetime import date

from src.config import SUBSYSTEM_PATHS
from src.adapters.base import BaseAdapter


class MungerAdapter(BaseAdapter):
    source = "munger"

    def run(self, analysis_date: date | None = None) -> list[dict]:
        analysis_date = analysis_date or date.today()

        analysis_path = str(SUBSYSTEM_PATHS["analysis"])
        if analysis_path not in sys.path:
            sys.path.insert(0, analysis_path)

        from batch_score import load_all_data, score_munger

        stock_info, valuation, financial_multi, prices, insider = load_all_data()
        codes = sorted(set(stock_info.keys()) & set(financial_multi.keys()) & set(valuation.keys()))

        results = []
        for code in codes:
            val = valuation.get(code, {})
            fin = financial_multi.get(code, [])
            insider_list = insider.get(code, [])
            if not fin:
                continue

            r = score_munger(fin, val, insider_list)
            if r is None:
                continue

            total_score = r["total_score"]
            signal = r.get("signal", "neutral")
            # Munger 的 total_score 范围是 0-10，映射到 0-100
            normalized = round(total_score * 10, 2)

            results.append({
                "code": code,
                "date": analysis_date,
                "source": self.source,
                "signal": signal,
                "score": normalized,
                "confidence": round(min(normalized, 100), 1),
                "detail_json": {
                    "total_score": total_score,
                    "moat_score": r.get("moat_score"),
                    "mgmt_score": r.get("mgmt_score"),
                    "pred_score": r.get("pred_score"),
                    "val_score": r.get("val_score"),
                    "name": stock_info[code]["name"],
                    "industry": stock_info[code]["industry"],
                },
            })

        print(f"[munger] 完成: {len(results)} 只股票")
        return results
