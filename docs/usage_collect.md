# 适配器与信号采集

## 概述

`python -m src collect` 依次运行 7 个适配器，将各子系统的分析结果标准化后写入 `stock_signals` 表。

## 7 个适配器

### screener — 财报质量筛选

- **来源**: a-stock-screener `batch_score.score_stock()`
- **输入**: PostgreSQL 财务三表（利润表、资产负债表、现金流量表）
- **评分维度**: 成长性、盈利能力、资产负债质量、现金流、资本配置、韧性、竞争优势（共 8 维）
- **信号映射**:
  - 极优/优秀 → bullish
  - 合格 → neutral
  - 观望/排除 → bearish
- **score**: total_score 直接使用（0-100）

### valuation — 多模型估值

- **来源**: a-stock-valuation `valuation.analyze_stock()`
- **估值模型**: 所有者盈余、DCF、EV/EBITDA、剩余收益（4 种加权）
- **信号映射**: weighted_gap > 15% → bullish; < -15% → bearish
- **score**: 50 + weighted_gap（映射到 0-100）

### buffett — 巴菲特视角

- **来源**: a-stock-analysis `batch_score.score_buffett()`
- **评分维度**: 基本面(7分)、一致性(3分)、护城河(5分)、定价权(5分)、账面价值增长(5分)、管理层(2分)
- **信号映射**: 函数内部已计算 signal（基于安全边际和得分）
- **score**: total_score / max_score * 100

### munger — 芒格视角

- **来源**: a-stock-analysis `batch_score.score_munger()`
- **评分维度**: 护城河(10分)、管理层(10分)、可预测性(10分)、估值(10分)
- **信号映射**: total >= 7.5 → bullish; <= 5.5 → bearish
- **score**: total_score * 10（0-10 映射到 0-100）

### chan — 缠论择时

- **来源**: a-stock-chan `batch_analyze.analyze_one_from_db()` + `score_stock()`
- **分析内容**: 分型→笔→线段→中枢→走势类型→背驰→买卖点
- **信号映射**:
  - 强烈买入信号/买入信号 → bullish
  - 卖出信号 → bearish
  - 关注信号/观望 → neutral
- **score**: total_score 直接使用

### trendradar — 舆情板块信号

- **来源**: finance_public_opinion 库 `ai_analysis_results.sector_impacts_json`
- **数据**: TrendRadar 的 AI 板块分析结果（11 平台热榜 + RSS）
- **映射逻辑**: 读取板块信号 → 按行业匹配到个股
  - 板块利多 → bullish; 板块利空 → bearish
- **score**: 基于 confidence 和信号方向映射

### risk_manager — 波动率风控

- **来源**: a-stock-analysis `batch_score.score_risk()`
- **计算**: 60 日日收益率标准差 → 年化波动率 → 仓位上限
- **仓位上限范围**: 5%（高波动）~ 25%（低波动）
- **信号映射**: 年化波动率 < 20% → bullish; > 40% → bearish
- **特殊用途**: `position_limit_pct` 供仓位计算器读取

## stock_signals 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| code | VARCHAR(10) | 股票代码 |
| date | DATE | 信号日期 |
| source | VARCHAR(30) | screener/valuation/buffett/munger/chan/trendradar/risk_manager |
| signal | VARCHAR(10) | bullish/bearish/neutral |
| score | FLOAT | 归一化评分 0-100 |
| confidence | FLOAT | 置信度 0-100 |
| detail_json | JSONB | 各系统的详细输出（维度明细、原始分数等） |

## 常用操作

```bash
# 全部采集（约 10 分钟）
python -m src collect

# 只采集单个 source（调试用）
python -m src collect --source screener
python -m src collect --source valuation
python -m src collect --source chan

# 指定日期
python -m src collect --date 2026-03-28

# 查看采集结果
psql -d finance -c "SELECT source, count(*) FROM stock_signals WHERE date = CURRENT_DATE GROUP BY source ORDER BY source;"
```
