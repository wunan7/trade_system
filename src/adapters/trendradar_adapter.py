"""TrendRadar 适配器 — 从舆情数据库读取 AI 板块信号，映射到个股"""

import json
from datetime import date, timedelta

from sqlalchemy import create_engine, text

from src.config import OPINION_DB_URL, FINANCE_DB_URL
from src.adapters.base import BaseAdapter


class TrendRadarAdapter(BaseAdapter):
    source = "trendradar"

    def run(self, analysis_date: date | None = None) -> list[dict]:
        analysis_date = analysis_date or date.today()

        # 1. 从舆情库读取最近一天的 AI 分析结果
        opinion_engine = create_engine(OPINION_DB_URL, pool_pre_ping=True)
        with opinion_engine.connect() as conn:
            # 查最近 3 天内有 sector_impacts_json 的记录
            rows = conn.execute(text("""
                SELECT data_date, sector_impacts_json
                FROM ai_analysis_results
                WHERE data_date >= :start AND sector_impacts_json IS NOT NULL
                    AND sector_impacts_json != '[]' AND sector_impacts_json != ''
                ORDER BY data_date DESC, id DESC
                LIMIT 1
            """), {"start": str(analysis_date - timedelta(days=3))}).fetchall()

        if not rows:
            print("[trendradar] 无近期 AI 分析结果")
            return []

        data_date = rows[0][0]
        raw = rows[0][1]
        try:
            sector_impacts = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            print("[trendradar] sector_impacts_json 解析失败")
            return []

        if not sector_impacts:
            return []

        # 2. 构建行业 → 信号映射
        # sector_impacts 格式: [{"sector": "半导体", "impact": "利多", "confidence": 0.8, "reasoning": "..."}]
        industry_signals = {}
        for item in sector_impacts:
            sector = item.get("sector", "")
            impact = item.get("impact", "中性")
            confidence = item.get("confidence", 0.5)
            reasoning = item.get("reasoning", "")

            if impact == "利多":
                signal = "bullish"
            elif impact == "利空":
                signal = "bearish"
            else:
                signal = "neutral"

            industry_signals[sector] = {
                "signal": signal,
                "confidence": confidence,
                "reasoning": reasoning,
            }

        # 3. 从 finance 库获取股票列表及行业
        finance_engine = create_engine(FINANCE_DB_URL, pool_pre_ping=True)
        with finance_engine.connect() as conn:
            stocks = conn.execute(text("""
                SELECT code, name, industry_l1
                FROM stock_info
                WHERE is_active = true AND is_st = false
            """)).fetchall()

        # 4. 按行业匹配板块信号到个股
        results = []
        matched = 0
        for code, name, industry in stocks:
            if not industry:
                continue

            # 尝试精确匹配和模糊匹配
            sig = industry_signals.get(industry)
            if sig is None:
                # 模糊匹配：行业名包含 sector 或 sector 包含行业名
                for sector, s in industry_signals.items():
                    if sector in industry or industry in sector:
                        sig = s
                        break

            if sig is None:
                continue

            matched += 1
            # score: confidence 0-1 → 0-100, 结合 signal 方向
            base_score = sig["confidence"] * 100
            if sig["signal"] == "bullish":
                score = 50 + base_score / 2  # 50-100
            elif sig["signal"] == "bearish":
                score = 50 - base_score / 2  # 0-50
            else:
                score = 50

            results.append({
                "code": code,
                "date": analysis_date,
                "source": self.source,
                "signal": sig["signal"],
                "score": round(score, 2),
                "confidence": round(sig["confidence"] * 100, 1),
                "detail_json": {
                    "matched_sector": industry,
                    "reasoning": sig["reasoning"],
                    "data_date": str(data_date),
                    "name": name,
                },
            })

        print(f"[trendradar] 板块信号: {len(industry_signals)} 个, 匹配股票: {matched}")
        return results
