"""Chan 适配器 — 调用 a-stock-chan 的缠论分析"""

import sys
from datetime import date

from src.config import SUBSYSTEM_PATHS
from src.adapters.base import BaseAdapter

# 缠论中文信号 → 英文
_SIGNAL_MAP = {
    "强烈买入信号": "bullish",
    "买入信号": "bullish",
    "关注信号": "neutral",
    "卖出信号": "bearish",
    "观望": "neutral",
}


class ChanAdapter(BaseAdapter):
    source = "chan"

    def run(self, analysis_date: date | None = None) -> list[dict]:
        analysis_date = analysis_date or date.today()

        chan_path = str(SUBSYSTEM_PATHS["chan"])
        if chan_path not in sys.path:
            sys.path.insert(0, chan_path)

        from batch_analyze import analyze_one_from_db, score_stock
        from finance_data.db.query import get_stock_list

        stock_list = get_stock_list(exclude_st=True)
        # 排除金融行业
        stock_list = [
            s for s in stock_list
            if s.get("industry") not in ("银行", "非银金融")
        ]

        results = []
        errors = 0

        for i, info in enumerate(stock_list):
            code = info["code"]
            name = info.get("name", "")
            industry = info.get("industry", "")

            try:
                analysis = analyze_one_from_db(code)
            except Exception:
                errors += 1
                continue

            if analysis is None:
                continue

            try:
                scored = score_stock(code, name, analysis, industry)
            except Exception:
                errors += 1
                continue

            cn_signal = scored.get("signal", "观望")
            signal = _SIGNAL_MAP.get(cn_signal, "neutral")
            total_score = scored.get("total_score", 0)

            results.append({
                "code": code,
                "date": analysis_date,
                "source": self.source,
                "signal": signal,
                "score": round(total_score, 2),
                "confidence": round(min(total_score, 100), 1),
                "detail_json": {
                    "cn_signal": cn_signal,
                    "trend_type": scored.get("trend_type"),
                    "trend_desc": scored.get("trend_desc"),
                    "risk_level": scored.get("risk_level"),
                    "score_buy_signal": scored.get("score_buy_signal"),
                    "score_trend": scored.get("score_trend"),
                    "score_divergence": scored.get("score_divergence"),
                    "score_pivot_position": scored.get("score_pivot_position"),
                    "score_macd_state": scored.get("score_macd_state"),
                    "name": name,
                    "industry": industry,
                },
            })

            if (i + 1) % 500 == 0:
                print(f"[chan] 进度: {i + 1}/{len(stock_list)}")

        print(f"[chan] 完成: {len(results)} 只, 跳过: {errors}")
        return results
