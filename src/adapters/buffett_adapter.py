"""Buffett 适配器 — 调用 a-stock-analysis 的 Buffett 纯规则批量评分"""

import sys
from datetime import date

from src.config import SUBSYSTEM_PATHS
from src.adapters.base import BaseAdapter


class BuffettAdapter(BaseAdapter):
    source = "buffett"

    def run(self, analysis_date: date | None = None) -> list[dict]:
        analysis_date = analysis_date or date.today()

        analysis_path = str(SUBSYSTEM_PATHS["analysis"])
        if analysis_path not in sys.path:
            sys.path.insert(0, analysis_path)

        from batch_score import load_all_data, score_buffett

        stock_info, valuation, financial_multi, prices, insider = load_all_data()
        codes = sorted(set(stock_info.keys()) & set(financial_multi.keys()) & set(valuation.keys()))

        results = []
        for code in codes:
            val = valuation.get(code, {})
            fin = financial_multi.get(code, [])
            if not fin:
                continue

            r = score_buffett(fin, val)
            if r is None:
                continue

            total_score = r["total_score"]
            max_score = r["max_score"]
            margin_of_safety = r.get("margin_of_safety")
            signal = r.get("signal", "neutral")

            # 归一化到 0-100
            normalized = round(total_score / max_score * 100, 2) if max_score > 0 else 0

            results.append({
                "code": code,
                "date": analysis_date,
                "source": self.source,
                "signal": signal,
                "score": normalized,
                "confidence": round(min(normalized, 100), 1),
                "detail_json": {
                    "total_score": total_score,
                    "max_score": max_score,
                    "margin_of_safety": margin_of_safety,
                    "fund_score": r.get("fund_score"),
                    "consist_score": r.get("consist_score"),
                    "moat_score": r.get("moat_score"),
                    "name": stock_info[code]["name"],
                    "industry": stock_info[code]["industry"],
                },
            })

        print(f"[buffett] 完成: {len(results)} 只股票")
        return results
