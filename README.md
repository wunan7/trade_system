# A股智能交易系统 (Trade System)

> 基于多维度信号融合与量化回测的 A 股智能交易决策系统

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📋 项目简介

本系统是一个完整的 A 股量化交易解决方案，集成了**信号采集、多因子评级、风险控制、策略回测、每日简报**等核心模块。通过融合基本面、技术面、舆情面等多维度信号，实现智能化的股票评级与交易决策。

### 核心特性

- **7 大信号源融合**：估值面、财务面、技术面（缠论）、舆情面、行业轮动等
- **动态评级系统**：A+/A/B/C/D 五级评级，支持三重共振检测
- **宏观择时**：基于市场宽度、趋势、波动率的 Risk On/Off 判断
- **多策略回测引擎**：支持 15+ 种策略配置，月度/周度检查点
- **多策略组合框架**：Ensemble 架构，支持动态资金调拨与风险对冲
- **每日简报生成**：自动化生成持仓分析、风险预警、市场洞察

---

## 🚀 快速开始

### 1. 环境准备

```bash
# 克隆仓库
git clone https://github.com/wunan7/trade_system.git
cd trade_system

# 安装依赖
pip install -r requirements.txt

# 配置数据库连接（修改 src/db/engine.py 中的连接字符串）
```

### 2. 数据库初始化

```bash
# 创建数据库表结构
python -m src migrate

# 采集股票基础信息
python -m src collect
```

### 3. 运行核心功能

```bash
# 生成每日简报
python -m src briefing

# 执行股票评级
python -m src rate

# 运行策略回测（推荐使用 concentrated_v2）
python -m src advanced-backtest --strategy concentrated_v2 --start 2023-03-31 --end 2026-03-31

# 运行多策略组合回测
python -m src ensemble-backtest --ensemble tactical --start 2023-03-31 --end 2026-03-31
```

---

## 📂 项目结构

```
trade_system/
├── src/
│   ├── adapters/          # 信号源适配器（7个）
│   │   ├── screener.py    # 财务筛选器
│   │   ├── valuation.py   # 估值分析
│   │   ├── buffett.py     # 巴菲特指标
│   │   ├── munger.py      # 芒格指标
│   │   ├── chan.py        # 缠论技术分析
│   │   ├── opinion.py     # 舆情分析
│   │   └── trendradar.py  # 板块趋势雷达
│   ├── backtest/          # 回测引擎
│   │   ├── advanced_strategy.py      # 高级策略引擎
│   │   ├── ensemble_strategy.py      # 多策略组合引擎
│   │   ├── market_regime.py          # 宏观择时模块
│   │   ├── atr.py                    # ATR 动态止损
│   │   ├── position_concentration.py # 仓位集中度优化
│   │   └── portfolio_optimizer.py    # 组合优化
│   ├── engine/            # 核心引擎
│   │   ├── rating.py      # 评级引擎
│   │   └── resonance.py   # 三重共振检测
│   ├── output/            # 输出模块
│   │   └── briefing.py    # 每日简报生成
│   ├── db/                # 数据库层
│   └── cli.py             # 命令行接口
├── strategies/            # 策略配置文件（15+）
│   ├── concentrated_v2.json   # 🏆 最优单策略
│   ├── regime_aware.json      # 宏观择时策略
│   ├── defensive.json         # 防御型策略
│   └── ...
├── ensembles/             # 多策略组合配置
│   └── tactical.json      # 🛡️ 进攻+防御动态切换
├── docs/                  # 完整文档
│   ├── 01_信号采集.md
│   ├── 02_评级与共振.md
│   ├── 03_每日简报与风控.md
│   ├── 04_策略回测.md
│   ├── 05_使用手册.md
│   └── 策略优化探索/      # 策略研究报告
│       ├── 01_总结报告.md
│       ├── 02_策略详细分析_上篇.md
│       └── 03_策略详细分析_下篇.md
└── reports/               # 回测报告输出目录
```

---

## 🎯 核心模块说明

### 1. 信号采集层（Adapters）

| 信号源 | 维度 | 核心指标 | 权重建议 |
|--------|------|---------|---------|
| Screener | 财务筛选 | ROE、营收增长、负债率 | 25% |
| Valuation | 估值面 | PE/PB 百分位、DCF | 30% |
| Buffett | 价值投资 | 股息率、自由现金流 | 10% |
| Munger | 质量因子 | 护城河、管理层 | 10% |
| Chan | 技术面 | 缠论笔段、买卖点 | 15% |
| Opinion | 舆情面 | 新闻情绪、社交媒体 | 5% |
| TrendRadar | 板块轮动 | 行业资金流向 | 5% |

### 2. 评级引擎（Rating Engine）

**评级逻辑**：
- 加权综合得分 → A+/A/B/C/D 五级评级
- 三重共振检测：估值面 + 技术面 + 舆情面同时看多
- 动态权重调整（可选）：基于历史准确率自适应

**输出**：
```json
{
  "code": "000001.SZ",
  "rating": "A+",
  "score": 87.5,
  "resonance_buy": true,
  "detail": {
    "valuation": {"signal": "bullish", "score": 85},
    "chan": {"signal": "bullish", "score": 78},
    "opinion": {"signal": "bullish", "score": 92}
  }
}
```

### 3. 策略回测引擎（Backtest Engine）

**支持的优化模块**：
- ✅ 动态止损止盈（ATR 自适应 + 移动止盈 + 时间止损）
- ✅ 宏观择时（Market Regime Detector）
- ✅ 仓位集中度优化（信号一致性加权）
- ✅ 多策略组合（Ensemble 资金动态调拨）
- ✅ 多层级共振（强度评分替代二元触发）
- ⏳ 行业轮动（需积累更多历史数据）

**回测命令**：
```bash
# 单策略回测
python -m src advanced-backtest --strategy concentrated_v2 --start 2023-03-31 --end 2026-03-31

# 多策略组合回测
python -m src ensemble-backtest --ensemble tactical --start 2023-03-31 --end 2026-03-31

# 列出所有可用策略
python -m src list-strategies
```

### 4. 多策略组合框架（Ensemble）

**核心机制**：
- 顶层资金池管理多个独立子策略
- 根据宏观状态（risk_on/neutral/risk_off）动态调拨资金
- 子策略间完全隔离，各自独立执行买卖决策

**示例配置（tactical.json）**：
```json
{
  "name": "tactical",
  "initial_capital": 100000,
  "sub_strategies": ["concentrated_v2", "defensive"],
  "regime_allocations": {
    "risk_on":  {"concentrated_v2": 0.80, "defensive": 0.20},
    "neutral":  {"concentrated_v2": 0.60, "defensive": 0.40},
    "risk_off": {"concentrated_v2": 0.20, "defensive": 0.80}
  }
}
```

---

## 📊 每日简报示例

系统每日自动生成 Markdown 格式简报，包含：

1. **持仓概览**：当前持仓、浮动盈亏、仓位分布
2. **评级变化**：新晋 A 级股票、降级预警
3. **三重共振机会**：多维度信号一致看多的标的
4. **风险预警**：触及止损线、流动性不足、行业集中度过高
5. **市场洞察**：宏观状态判断、板块轮动趋势

---

## 🔧 配置说明

### 策略配置文件（strategies/*.json）

```json
{
  "name": "concentrated_v2",
  "rating_weights": {
    "screener": 0.25,
    "valuation": 0.30,
    "buffett": 0.10,
    "munger": 0.10,
    "chan": 0.15,
    "opinion": 0.05,
    "trendradar": 0.05
  },
  "buy_ratings": ["A+", "A"],
  "position_sizes": {"A+": 0.15, "A": 0.12},
  "stop_loss_pct": -0.10,
  "take_profit_pct": 0.30,
  
  "atr_stop_enabled": true,
  "atr_stop_multiplier": 2.0,
  "trailing_stop_enabled": true,
  "trailing_stop_pct": 0.15,
  "time_stop_months": 6,
  
  "market_regime_enabled": true,
  "regime_risk_on_mult": 1.2,
  "regime_risk_off_mult": 0.5,
  
  "position_concentration_enabled": true,
  "consensus_strong_mult": 1.2,
  "consensus_weak_mult": 0.9
}
```

### 数据库配置（src/db/engine.py）

```python
# MySQL 连接配置
FINANCE_DB_URL = "mysql+pymysql://user:password@localhost:3306/finance_db"
OPINION_DB_URL = "mysql+pymysql://user:password@localhost:3306/opinion_db"
```

---

## 📈 策略选择指南

### 场景 1：个人投资者，追求高收益
**推荐**：`concentrated_v2`
- 年化收益 +8.85%，超额收益 +12.26%
- 最大回撤 -23.11%（可接受范围）
- 适合风险承受能力较强的投资者

### 场景 2：机构资金，严格风控要求
**推荐**：`tactical` 多策略组合
- 最大回撤仅 -15.83%（所有策略中最低）
- 通过动态资金调拨实现熊市防守
- 适合追求绝对收益、回撤敏感的机构

### 场景 3：稳健型投资者
**推荐**：`regime_aware`
- 年化收益 +8.58%，回撤 -21.75%
- 不使用激进的仓位集中度优化
- 逻辑简洁，易于理解和监控

---

## 🛠️ 开发与扩展

### 添加新的信号源

1. 在 `src/adapters/` 下创建新的适配器
2. 实现 `collect()` 和 `rate()` 方法
3. 在 `src/engine/rating.py` 中注册新信号源
4. 更新策略配置文件中的 `rating_weights`

### 创建新的策略

1. 复制 `strategies/concentrated_v2.json` 作为模板
2. 修改参数（权重、止损、仓位等）
3. 运行回测验证效果
4. 对比现有策略，评估是否采用

### 扩展回测引擎

- 修改 `src/backtest/advanced_strategy.py` 添加新的风控逻辑
- 在 `src/backtest/strategy_config.py` 中添加新的配置字段
- 更新 `run_advanced_backtest()` 函数集成新模块

---

## 📚 文档索引

- [01_信号采集.md](docs/01_信号采集.md) - 7 大信号源详细说明
- [02_评级与共振.md](docs/02_评级与共振.md) - 评级算法与三重共振
- [03_每日简报与风控.md](docs/03_每日简报与风控.md) - 简报生成与风险管理
- [04_策略回测.md](docs/04_策略回测.md) - 回测引擎使用指南
- [05_使用手册.md](docs/05_使用手册.md) - 完整操作手册
- [策略优化探索/](docs/策略优化探索/) - 15 个策略的深度回测分析

---

## ⚠️ 免责声明

本系统仅供学习研究使用，不构成任何投资建议。股市有风险，投资需谨慎。使用本系统进行实盘交易的一切后果由使用者自行承担。

---

## 📝 License

MIT License - 详见 [LICENSE](LICENSE) 文件

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

## 📧 联系方式

如有问题或建议，请通过 GitHub Issues 联系。
