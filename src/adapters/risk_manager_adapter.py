"""Risk Manager 适配器 — 调用 a-stock-analysis 的波动率仓位管理"""

import sys
from datetime import date

from src.config import SUBSYSTEM_PATHS
from src.adapters.base import BaseAdapter


class RiskManagerAdapter(BaseAdapter):
    source = "risk_manager"

    def run(self, analysis_date: date | None = None) -> list[dict]:
        analysis_date = analysis_date or date.today()

        analysis_path = str(SUBSYSTEM_PATHS["analysis"])
        if analysis_path not in sys.path:
            sys.path.insert(0, analysis_path)

        from batch_score import load_all_data, score_risk

        stock_info, valuation, financial_multi, prices, insider = load_all_data()
        codes = sorted(set(stock_info.keys()) & set(prices.keys()))

        results = []
        for code in codes:
            price_list = prices.get(code, [])
            if not price_list or len(price_list) < 30:
                continue

            try:
                r = score_risk(price_list)
            except Exception:
                continue

            if r is None:
                continue

            annual_vol = float(r.get("annual_vol", 0))
            position_limit_pct = float(r.get("position_limit_pct", 0))

            # 低波动率 = bullish（适合配置），高波动率 = bearish（减少配置）
            if annual_vol < 0.20:
                signal = "bullish"
            elif annual_vol > 0.40:
                signal = "bearish"
            else:
                signal = "neutral"

            # score: position_limit_pct 范围 0.05-0.25，映射到 0-100
            score = round(min(position_limit_pct / 0.25 * 100, 100), 2)

            results.append({
                "code": code,
                "date": analysis_date,
                "source": self.source,
                "signal": signal,
                "score": score,
                "confidence": round(min(100, float(r.get("vol_percentile", 50))), 1),
                "detail_json": {
                    "daily_vol": float(r.get("daily_vol", 0)),
                    "annual_vol": round(annual_vol, 4),
                    "vol_percentile": float(r.get("vol_percentile", 0)),
                    "position_limit_pct": round(position_limit_pct, 2),
                    "latest_price": float(r.get("latest_price", 0)),
                    "name": stock_info.get(code, {}).get("name", ""),
                },
            })

        print(f"[risk_manager] 完成: {len(results)} 只股票")
        return results
