# 简报与推送

## 每日简报

`python -m src briefing --dry-run` 生成 Markdown 格式的投资决策简报。

### 简报 6 大板块

#### 1. 三重共振买入预警

价值+技术+情绪同时看多的股票，附评分、建议仓位和三因子理由。

```
- **688581 安杰思** (评分 82.9, 建议仓位 12.8%)
  - 价值面看多, 缠论买入(score=75.5), 板块利多(conf=65.0)
```

#### 2. 三重共振卖出预警

三维度同时看空的股票，提示风险。

#### 3. 综合评级变动

与前一交易日对比的评级升降（如 B→A、A→C），帮助发现评级趋势变化。

#### 4. 候选池监控 (A+/A级)

A+/A 级候选池的新入池和出池变化，跟踪优质股票池的流动。

#### 5. 持仓风控提醒

对当前持仓中出现 bearish 信号的股票发出预警，按来源分类（缠论卖点、板块利空、估值高估等）。

**前提**: 需要创建 `portfolio.txt` 文件。

#### 6. 综合评级概览 + Top 20

各级别数量分布和 A+/A 级高分股票排名表。

### 持仓文件

在项目根目录创建 `portfolio.txt`，每行一个持仓代码：

```
# 当前持仓
600519
000858
002415
300750
```

空行和 `#` 开头的行会被忽略。更新持仓后，下次生成简报会自动包含风控提醒。

## 推送配置

### 飞书

设置环境变量：
```bash
export FEISHU_WEBHOOK_URL="https://open.feishu.cn/open-apis/bot/v2/hook/xxx"
```

### 钉钉

设置环境变量：
```bash
export DINGTALK_WEBHOOK_URL="https://oapi.dingtalk.com/robot/send?access_token=xxx"
```

可同时配置两者，简报会推送到所有已配置的渠道。

### Windows 永久设置环境变量

```powershell
[Environment]::SetEnvironmentVariable("FEISHU_WEBHOOK_URL", "https://open.feishu.cn/...", "User")
[Environment]::SetEnvironmentVariable("DINGTALK_WEBHOOK_URL", "https://oapi.dingtalk.com/...", "User")
```

## 常用操作

```bash
# 预览简报（不推送）
python -m src briefing --dry-run

# 推送简报
python -m src briefing

# 指定日期
python -m src briefing --date 2026-03-28 --dry-run

# 保存简报到文件
python -c "
from src.output.briefing import generate_briefing
content = generate_briefing()
with open('briefing.md', 'w', encoding='utf-8') as f:
    f.write(content)
"
```

## 定时运行

### Windows 任务计划

每个交易日 18:30 自动运行全流程：

```powershell
# 创建任务计划（PowerShell 管理员）
$action = New-ScheduledTaskAction -Execute "python" -Argument "-m src run --dry-run" -WorkingDirectory "C:\Users\wunan\projects\legacy_solution_integration"
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At 18:30
Register-ScheduledTask -TaskName "IntegratedDecisionEngine" -Action $action -Trigger $trigger -Description "A股集成决策引擎每日运行"
```

去掉 `--dry-run` 即可启用推送。
