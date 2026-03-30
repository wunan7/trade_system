# A股集成决策引擎 — 使用说明

## 系统简介

本系统将 7 个独立的 A 股分析子系统（财报筛选、多模型估值、巴菲特评分、芒格评分、缠论择时、舆情分析、波动率风控）的信号汇入统一数据库，计算综合评级和三重共振信号，生成每日投资决策简报。

## 前置条件

- Python 3.11+
- PostgreSQL（finance 库和 finance_public_opinion 库已有数据）
- 各子系统代码已部署在 `C:\Users\wunan\projects\` 下

## 安装

```bash
cd C:\Users\wunan\projects\legacy_solution_integration
pip install -r requirements.txt
```

依赖：sqlalchemy、psycopg2-binary、pandas、numpy、requests

## 首次初始化

```bash
# 在 finance 库中创建 stock_signals 和 integrated_ratings 两张表
python -m src migrate
```

## 日常使用（一键全流程）

```bash
# 采集全部信号 → 综合评级 → 生成简报（预览模式）
python -m src run --dry-run

# 采集全部信号 → 综合评级 → 推送简报（需配置 webhook）
python -m src run
```

全流程耗时约 10-15 分钟（主要是 valuation 和 chan 的全量计算）。

## 分步执行

| 命令 | 作用 | 耗时 |
|------|------|------|
| `python -m src collect` | 运行 7 个适配器，写入 stock_signals | ~10 分钟 |
| `python -m src collect --source screener` | 只运行单个适配器 | ~10 秒 |
| `python -m src rate` | 综合评级 + 三重共振 + 仓位计算 | ~5 秒 |
| `python -m src briefing --dry-run` | 生成简报输出到终端 | ~2 秒 |
| `python -m src briefing` | 生成简报并推送 | ~3 秒 |
| `python -m src backtest --dry-run` | 回测验证输出到终端 | ~10 秒 |
| `python -m src backtest` | 回测验证保存到 reports/ | ~10 秒 |

所有命令支持 `--date YYYY-MM-DD` 指定分析日期（默认今天）。

## 详细文档

- [适配器与信号采集](usage_collect.md) — 7 个适配器的输入输出和信号映射
- [评级与共振](usage_rating.md) — 综合评级算法、三重共振条件、仓位计算
- [简报与推送](usage_output.md) — 简报 6 大板块、持仓风控、推送配置
- [回测验证](usage_backtest.md) — 信号胜率、共振收益率、Alpha 分解

## 项目结构

```
legacy_solution_integration/
├── src/
│   ├── config.py              # 统一配置
│   ├── cli.py                 # CLI 入口
│   ├── db/                    # 数据库层
│   │   ├── engine.py          #   连接管理
│   │   ├── models.py          #   ORM 模型
│   │   └── migrate.py         #   建表脚本
│   ├── adapters/              # 7 个子系统适配器
│   │   ├── base.py            #   基类（upsert 逻辑）
│   │   ├── screener_adapter.py
│   │   ├── valuation_adapter.py
│   │   ├── buffett_adapter.py
│   │   ├── munger_adapter.py
│   │   ├── chan_adapter.py
│   │   ├── trendradar_adapter.py
│   │   └── risk_manager_adapter.py
│   ├── engine/                # 决策融合引擎
│   │   ├── rating.py          #   综合评级
│   │   ├── resonance.py       #   三重共振检测
│   │   └── position.py        #   仓位计算
│   ├── output/                # 输出层
│   │   ├── briefing.py        #   简报生成
│   │   └── push.py            #   飞书/钉钉推送
│   └── backtest/              # 回测验证
│       ├── price_loader.py    #   价格数据加载
│       ├── signal_eval.py     #   信号胜率
│       ├── resonance_eval.py  #   共振收益率
│       ├── alpha_decomp.py    #   Alpha 分解
│       └── report.py          #   报告生成
├── reports/                   # 回测报告输出
├── portfolio.txt              # 当前持仓列表（手动维护）
├── requirements.txt
└── docs/design/               # 设计文档
```

## 数据库表

| 表名 | 库 | 主键 | 用途 |
|------|------|------|------|
| stock_signals | finance | (code, date, source) | 各子系统信号统一落地 |
| integrated_ratings | finance | (code, date) | 综合评级结果 |
| stock_daily | finance | (code, trade_date) | 日K线价格（已有） |
| stock_info | finance | (code) | 股票基本信息（已有） |
| ai_analysis_results | finance_public_opinion | (id) | TrendRadar AI 分析（已有） |
