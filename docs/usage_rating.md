# 评级与共振

## 综合评级系统

`python -m src rate` 从 `stock_signals` 读取当日全部信号，计算综合评级并写入 `integrated_ratings`。

### 评级算法

**5 模型加权投票**（risk_manager 不参与评级投票，仅供仓位计算）：

| 投票者 | 权重 | 投票内容 |
|--------|------|---------|
| screener | 25% | 财务质量分 |
| valuation | 30% | 估值偏差 |
| buffett | 20% | 价值投资视角 |
| munger | 15% | 质量优先视角 |
| chan | 10% | 技术面加减分 |

**混合评分公式**:
```
blended = signal_value × 0.6 + score × 0.4

signal_value: bullish=100, neutral=50, bearish=0
score: 各系统的归一化评分 0-100
```

**评级映射**:

| 评级 | 加权分数 | 含义 |
|------|---------|------|
| A+ | >= 85 | 所有模型一致看多（极稀缺） |
| A | >= 70 | 多数模型看多 |
| B | >= 55 | 基本面优秀但估值中性 |
| C | >= 40 | 信号混杂 |
| D | < 40 | 多数模型看空 |

### 修改权重

编辑 `src/config.py` 中的 `RATING_WEIGHTS` 和 `RATING_THRESHOLDS`。

## 三重共振检测

在评级流程中自动运行，检测价值+技术+情绪三维度同时发出信号的时刻。

### 买入共振条件（同时满足）

1. **价值面看多**: valuation=bullish 或 buffett/munger 任一=bullish
2. **技术面确认**: 缠论 signal=bullish 且 score >= 55（5 天内）
3. **情绪面催化**: trendradar 对应行业 signal=bullish 且 confidence >= 60%

### 卖出共振条件（同时满足）

1. **价值面看空**: valuation=bearish
2. **技术面确认**: 缠论 signal=bearish
3. **情绪面利空**: trendradar 对应行业 signal=bearish

### 调整共振参数

编辑 `src/config.py` 中的 `RESONANCE`:

```python
RESONANCE = {
    "chan_min_score": 55,             # 缠论最低分
    "chan_lookback_days": 5,          # 缠论信号有效天数
    "trendradar_min_confidence": 0.6, # 舆情最低置信度
}
```

## 仓位计算

**公式**: `最终仓位 = Risk Manager 仓位上限 × 信号强度系数`

| 评级 | 信号强度系数 | 示例（Risk Manager 上限 20%） |
|------|------------|------|
| A+ | 1.0 | 20.0% |
| A | 0.8 | 16.0% |
| B | 0.5 | 10.0% |
| C | 0.3 | 6.0% |
| D | 0.0 | 0.0% |

Risk Manager 仓位上限根据个股波动率计算（5%-25%），低波动股票允许更大仓位。

## integrated_ratings 表结构

| 字段 | 类型 | 说明 |
|------|------|------|
| code | VARCHAR(10) | 股票代码 |
| date | DATE | 评级日期 |
| rating | VARCHAR(5) | A+/A/B/C/D |
| weighted_score | FLOAT | 加权综合分 0-100 |
| resonance_buy | BOOLEAN | 三重共振买入 |
| resonance_sell | BOOLEAN | 三重共振卖出 |
| position_pct | FLOAT | 建议仓位百分比 |
| detail_json | JSONB | 各 source 投票明细 |

## 常用操作

```bash
# 运行评级
python -m src rate

# 指定日期
python -m src rate --date 2026-03-28

# 查看评级分布
psql -d finance -c "SELECT rating, count(*) FROM integrated_ratings WHERE date = CURRENT_DATE GROUP BY rating ORDER BY rating;"

# 查看三重共振买入信号
psql -d finance -c "SELECT code, weighted_score, position_pct FROM integrated_ratings WHERE date = CURRENT_DATE AND resonance_buy = true ORDER BY weighted_score DESC;"
```
