# 集成决策引擎 — 实施计划

## Context

用户有 6 个独立的 A 股分析子系统（screener、valuation、Buffett/Munger 分析、缠论、TrendRadar），各自输出 CSV 或 dict，互不相通。目标是构建一个集成层，将各系统信号汇入统一数据库，计算综合评级和三重共振信号，并通过 TrendRadar 推送通道发送每日简报。

## 项目结构

```
legacy_solution_integration/
├── docs/design/                         # 已有方案设计文档
├── src/
│   ├── __init__.py
│   ├── config.py                        # 统一配置（DB URL、子系统路径、权重参数）
│   ├── db/
│   │   ├── __init__.py
│   │   ├── engine.py                    # SQLAlchemy engine/session（复用 finance DB）
│   │   ├── models.py                    # stock_signals + integrated_ratings 表 ORM
│   │   └── migrate.py                   # 建表脚本
│   ├── adapters/                        # 各子系统适配器（统一输出格式写入 stock_signals）
│   │   ├── __init__.py
│   │   ├── base.py                      # BaseAdapter 抽象类
│   │   ├── screener_adapter.py          # 调用 a-stock-screener
│   │   ├── valuation_adapter.py         # 调用 a-stock-valuation
│   │   ├── buffett_adapter.py           # 调用 a-stock-analysis Buffett batch
│   │   ├── munger_adapter.py            # 调用 a-stock-analysis Munger batch
│   │   ├── chan_adapter.py              # 调用 a-stock-chan
│   │   └── trendradar_adapter.py        # 读取 TrendRadar AI分析结果
│   ├── engine/                          # 决策融合引擎
│   │   ├── __init__.py
│   │   ├── rating.py                    # 综合评级系统（多模型投票）
│   │   ├── resonance.py                 # 三重共振检测器
│   │   └── position.py                  # 仓位计算器
│   ├── output/                          # 输出层
│   │   ├── __init__.py
│   │   ├── briefing.py                  # 每日简报生成
│   │   └── push.py                      # 推送（复用 TrendRadar senders）
│   └── cli.py                           # 主入口 CLI
├── tests/
│   ├── test_adapters.py
│   ├── test_rating.py
│   └── test_resonance.py
└── requirements.txt
```

## Phase 1：统一信号表 + 适配器

### 1.1 数据库（`src/db/`）

**`engine.py`** — 复用 finance DB 连接
```python
# 使用 env FINANCE_DATABASE_URL 或 fallback postgresql://postgres:postgres@localhost:5432/finance
```

**`models.py`** — 两张新表
```sql
CREATE TABLE stock_signals (
    code        VARCHAR(10),
    date        DATE,
    source      VARCHAR(30),  -- screener/valuation/buffett/munger/chan/trendradar
    signal      VARCHAR(10),  -- bullish/bearish/neutral
    score       FLOAT,        -- 归一化 0-100
    confidence  FLOAT,        -- 0-100
    detail_json JSONB,
    created_at  TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (code, date, source)
);

CREATE TABLE integrated_ratings (
    code           VARCHAR(10),
    date           DATE,
    rating         VARCHAR(5),   -- A+/A/B/C/D
    weighted_score FLOAT,        -- 0-100
    resonance_buy  BOOLEAN,      -- 三重共振买入
    resonance_sell BOOLEAN,      -- 三重共振卖出
    position_pct   FLOAT,        -- 建议仓位%
    detail_json    JSONB,
    created_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (code, date)
);
```

### 1.2 适配器（`src/adapters/`）

**BaseAdapter 接口**:
```python
class BaseAdapter(ABC):
    def run(self) -> list[dict]:
        """运行分析，返回 [{code, date, source, signal, score, confidence, detail_json}]"""
    def save(self, results: list[dict]):
        """批量 upsert 到 stock_signals 表"""
```

**6 个适配器的调用方式**:

| 适配器 | 调用方式 | 信号映射 |
|--------|---------|---------|
| screener | `sys.path` 加入项目路径，import `batch_score.score_stock()` | total_score → score; rating 映射 → signal |
| valuation | import `valuation.analyze_stock(ticker)` | signal 直用; confidence 直用; weighted_gap → detail |
| buffett | import `batch_score.score_buffett()` | total/max → 归一化 score; >70%=bullish, <40%=bearish |
| munger | import `batch_score.score_munger()` | weighted_score → score; ≥7.5=bullish, ≤5.5=bearish |
| chan | import `batch_analyze.analyze_one_from_db()` + `score_stock()` | total_score → score; signal 中文 → 英文映射 |
| trendradar | 直接读 `finance_public_opinion.ai_analysis_results` 表的 `sector_impacts_json` | 按股票所属行业匹配板块信号 |

### 1.3 CLI 入口

```bash
python -m src.cli collect           # 运行所有适配器，写入 stock_signals
python -m src.cli collect --source screener  # 只运行某个适配器
python -m src.cli rate              # 运行综合评级
python -m src.cli briefing          # 生成并推送每日简报
python -m src.cli run               # 全流程：collect → rate → briefing
```

## Phase 2：综合评级系统

**`src/engine/rating.py`**

1. 从 `stock_signals` 读取当日各 source 信号
2. 按权重加权投票：screener 25% + valuation 30% + buffett 20% + munger 15% + chan 10%
3. signal 转数值：bullish=100, neutral=50, bearish=0
4. 计算 weighted_score，映射评级：A+(≥85) / A(≥70) / B(≥55) / C(≥40) / D(<40)
5. 写入 `integrated_ratings` 表

## Phase 3：三重共振检测器

**`src/engine/resonance.py`**

三重共振买入条件（同时满足）：
1. 价值面：valuation=bullish 或 buffett/munger 任一=bullish
2. 技术面：chan signal 含"买入"且 score≥55，5天内
3. 情绪面：trendradar 对应行业 impact=利多 且 confidence≥0.6

三重共振卖出条件（同时满足）：
1. 价值面：valuation=bearish
2. 技术面：chan signal="卖出信号"
3. 情绪面：trendradar 对应行业 impact=利空

## Phase 4：每日简报 + 推送

**`src/output/briefing.py`** — 生成 Markdown 格式简报
**`src/output/push.py`** — 独立实现飞书/钉钉 webhook 推送（不依赖 TrendRadar 代码）

推送实现方式：
- 飞书：POST 到 webhook URL，发送 Markdown 卡片消息
- 钉钉：POST 到 webhook URL，发送 Markdown 消息
- 配置：`config.py` 中配置 webhook URL
- 简洁实现，仅 requests 库依赖

## Phase 5：回测验证系统（详细设计）

### Context

Phase 1-4 已全部通过端到端验证。`stock_signals` 表存有 7 个 source 的 31,000+ 条信号，`integrated_ratings` 表存有 5,012 条综合评级（含三重共振标记）。现在需要衡量这些信号的历史预测能力，为后续权重调优提供数据支撑。

已有 `C:\Users\wunan\projects\backtesting\` 项目（策略级回测引擎），但 Phase 5 需要的是**信号级评估**——对 `stock_signals` 和 `integrated_ratings` 中的每条记录，查看未来 N 天的涨跌表现，不涉及持仓模拟。

### 项目结构

```
src/backtest/
├── __init__.py
├── price_loader.py      # 批量加载价格数据（复用 finance_data）
├── signal_eval.py       # 各模型信号胜率评估
├── resonance_eval.py    # 三重共振收益率评估
├── alpha_decomp.py      # 综合评级 Alpha 分解
└── report.py            # 回测报告生成（Markdown）
```

### 5.1 价格数据加载 (`price_loader.py`)

批量从 `stock_daily` 读取前复权 close 价格，构建 `{code: {date: close}}` 字典。

```python
def load_prices(start_date, end_date) -> dict[str, dict[date, float]]:
    """批量加载价格数据，SQL 一次性读取，内存中索引"""
```

关键依赖：`finance` 库 `stock_daily` 表（665 万行，前复权，字段 code/trade_date/close）。

### 5.2 各模型信号胜率 (`signal_eval.py`)

**逻辑：**
1. 读取指定日期范围内 `stock_signals` 的全部记录
2. 对每条 signal=bullish 的记录，查找 T+5/T+10/T+20 收盘价
3. 计算前向收益率 = (close_T+N - close_T) / close_T
4. bullish 胜出 = 收益率 > 0；bearish 胜出 = 收益率 < 0
5. 按 source 分组统计

**输出表：**

| source | signal | count | win_5d | win_10d | win_20d | avg_ret_5d | avg_ret_10d | avg_ret_20d |
|--------|--------|-------|--------|---------|---------|------------|-------------|-------------|
| screener | bullish | 1200 | 58.3% | 61.2% | 64.5% | +1.2% | +2.1% | +3.8% |
| valuation | bullish | 980 | ... | ... | ... | ... | ... | ... |

### 5.3 三重共振收益率 (`resonance_eval.py`)

**逻辑：**
1. 读取 `integrated_ratings` 中 `resonance_buy=true` 的记录
2. 入场价 = T+1 开盘价（模拟次日买入）
3. 计算持仓 5/10/20/60 天后的收益率
4. 对比同期沪深300（sh000300）涨跌幅
5. 同理处理 `resonance_sell=true`

**输出：**
- 三重共振买入：平均 N 日收益率、胜率、跑赢基准比例
- 三重共振卖出：信号后 N 日跌幅统计

### 5.4 综合评级 Alpha 分解 (`alpha_decomp.py`)

**逻辑：**
1. 读取 `integrated_ratings`，按 rating 分组
2. 每组计算前向 5/10/20 日平均收益率
3. 与市场基准（全 A 等权平均或沪深300）做差 = Alpha
4. 验证：A+ > A > B > C > D 的单调性

**输出：**

| rating | count | avg_ret_5d | avg_ret_10d | alpha_5d | alpha_10d |
|--------|-------|------------|-------------|----------|-----------|
| A+ | 0 | - | - | - | - |
| A | 42 | +2.1% | +3.5% | +1.5% | +2.8% |
| B | 189 | +0.8% | +1.2% | +0.2% | +0.5% |
| market | 5012 | +0.6% | +0.7% | 0 | 0 |

### 5.5 报告生成 (`report.py`)

合并三部分结果，输出 Markdown 报告文件到 `reports/backtest_{date}.md`。

### 5.6 CLI

```bash
python -m src backtest                           # 默认回测当日信号
python -m src backtest --start 2026-03-01 --end 2026-03-29  # 日期范围
python -m src backtest --holding-days 5,10,20    # 自定义持仓天数
```

### 实施顺序

1. `src/backtest/__init__.py` + `price_loader.py`
2. `signal_eval.py`
3. `resonance_eval.py`
4. `alpha_decomp.py`
5. `report.py` + CLI `backtest` 命令
6. 验证：`python -m src backtest --dry-run`

### 注意事项

- 当前只有 1 天的 `stock_signals` 数据（2026-03-29），回测需要多日累积数据才有统计意义
- 首次运行时输出的胜率仅供参考，需连续运行 collect+rate 积累 20+ 天数据后才有可靠结论
- 前视偏差防护：使用 T+1 开盘价作为入场价，不使用 T 日收盘价

## 实施顺序

1. **Phase 1a**: `db/` — engine、models、migrate（建表）
2. **Phase 1b**: `adapters/` — 6 个适配器 + CLI `collect` 命令
3. **Phase 2**: `engine/rating.py` + CLI `rate` 命令
4. **Phase 3**: `engine/resonance.py`（在 rating 流程中调用）
5. **Phase 4**: `output/` — briefing + push + CLI `briefing` 命令
6. **验证**: 端到端运行 `python -m src.cli run`

## 关键文件依赖

| 集成项目文件 | 依赖的子系统文件 |
|------------|----------------|
| screener_adapter | `C:\Users\wunan\projects\a-stock-screener\batch_score.py` → `score_stock()` |
| valuation_adapter | `C:\Users\wunan\projects\a-stock-valuation\valuation.py` → `analyze_stock()` |
| buffett_adapter | `C:\Users\wunan\projects\a-stock-analysis\batch_score.py` → `score_buffett()` |
| munger_adapter | `C:\Users\wunan\projects\a-stock-analysis\batch_score.py` → `score_munger()` |
| chan_adapter | `C:\Users\wunan\projects\a-stock-chan\batch_analyze.py` → `analyze_one_from_db()` + `score_stock()` |
| trendradar_adapter | PostgreSQL `finance_public_opinion.ai_analysis_results` 表直接查询 |
| push.py | 独立实现，仅依赖 requests 库 |

## 验证方式

- Phase 1: `python -m src.cli collect --source screener` → 检查 `stock_signals` 表有数据
- Phase 2: `python -m src.cli rate` → 检查 `integrated_ratings` 表，验证评级分布合理
- Phase 3: 检查 `integrated_ratings.resonance_buy` 字段，A+ 级股票数量应 <30
- Phase 4: `python -m src.cli briefing --dry-run` → 输出简报文本不实际推送
- 全流程: `python -m src.cli run` → 从采集到推送端到端完成
