"""A股集成决策引擎 — CLI 主入口"""

import argparse
import sys
from datetime import date


def cmd_migrate(args):
    """建表"""
    from src.db.migrate import migrate
    migrate()


def cmd_collect(args):
    """运行适配器采集信号"""
    from src.adapters.screener_adapter import ScreenerAdapter
    from src.adapters.valuation_adapter import ValuationAdapter
    from src.adapters.buffett_adapter import BuffettAdapter
    from src.adapters.munger_adapter import MungerAdapter
    from src.adapters.chan_adapter import ChanAdapter
    from src.adapters.trendradar_adapter import TrendRadarAdapter
    from src.adapters.risk_manager_adapter import RiskManagerAdapter

    adapters = {
        "screener": ScreenerAdapter,
        "valuation": ValuationAdapter,
        "buffett": BuffettAdapter,
        "munger": MungerAdapter,
        "chan": ChanAdapter,
        "trendradar": TrendRadarAdapter,
        "risk_manager": RiskManagerAdapter,
    }

    analysis_date = date.fromisoformat(args.date) if args.date else date.today()
    sources = [args.source] if args.source else list(adapters.keys())

    total = 0
    for name in sources:
        cls = adapters.get(name)
        if cls is None:
            print(f"未知 source: {name}, 可选: {list(adapters.keys())}")
            continue
        print(f"\n{'='*50}")
        print(f"  采集: {name}")
        print(f"{'='*50}")
        adapter = cls()
        count = adapter.collect(analysis_date)
        print(f"  写入 stock_signals: {count} 行")
        total += count

    print(f"\n采集完成, 共写入 {total} 行")


def cmd_rate(args):
    """运行综合评级"""
    from src.engine.rating import run_rating

    analysis_date = date.fromisoformat(args.date) if args.date else date.today()
    count = run_rating(analysis_date, use_adaptive=getattr(args, "adaptive", False))
    print(f"\n评级完成, 写入 integrated_ratings: {count} 行")


def cmd_briefing(args):
    """生成并推送每日简报"""
    from src.output.briefing import generate_briefing
    from src.output.push import push_briefing
    from pathlib import Path

    analysis_date = date.fromisoformat(args.date) if args.date else date.today()
    content = generate_briefing(analysis_date)

    # 始终保存到文件
    out = Path("reports") / f"briefing_{analysis_date}.md"
    out.parent.mkdir(exist_ok=True)
    out.write_text(content, encoding="utf-8")
    print(f"简报已保存: {out}")

    if args.dry_run:
        pass  # 仅保存，不推送
    else:
        push_briefing(content)
        print("简报已推送")


def cmd_run(args):
    """全流程: collect → rate → briefing"""
    cmd_collect(args)
    cmd_rate(args)
    cmd_briefing(args)


def cmd_backtest(args):
    """运行回测验证"""
    from src.backtest.signal_eval import evaluate_signals
    from src.backtest.resonance_eval import evaluate_resonance
    from src.backtest.alpha_decomp import evaluate_alpha
    from src.backtest.report import generate_backtest_report, save_report

    end_date = date.fromisoformat(args.end) if args.end else date.today()
    start_date = date.fromisoformat(args.start) if args.start else end_date
    holding_days = [int(x) for x in args.holding_days.split(",")]

    print(f"回测区间: {start_date} ~ {end_date}, 持仓天数: {holding_days}")

    print("\n[1/3] 各模型信号胜率...")
    signal_results = evaluate_signals(start_date, end_date, holding_days)

    print("[2/3] 三重共振收益率...")
    resonance_results = evaluate_resonance(start_date, end_date, holding_days)

    print("[3/3] 综合评级 Alpha 分解...")
    alpha_results = evaluate_alpha(start_date, end_date, holding_days)

    content = generate_backtest_report(
        signal_results, resonance_results, alpha_results,
        start_date, end_date, holding_days,
    )

    if args.dry_run:
        print(content)
    else:
        path = save_report(content, end_date)
        print(f"\n报告已保存: {path}")


def cmd_hist_backtest(args):
    """历史回测模拟"""
    from src.backtest.historical_sim import run_historical_backtest
    from pathlib import Path

    end = date.fromisoformat(args.end) if args.end else date(2026, 3, 27)
    start = date.fromisoformat(args.start) if args.start else end - __import__("datetime").timedelta(days=180)
    hold = int(args.hold_days)

    content = run_historical_backtest(start, end, hold)

    if args.dry_run:
        print(content)
    else:
        out = Path("reports") / f"hist_backtest_{end}.md"
        out.parent.mkdir(exist_ok=True)
        out.write_text(content, encoding="utf-8")
        print(f"\n报告已保存: {out}")


def cmd_advanced_backtest(args):
    """增强策略回测"""
    from src.backtest.advanced_strategy import run_advanced_backtest
    from pathlib import Path
    from datetime import datetime

    end = date.fromisoformat(args.end) if args.end else date(2026, 3, 27)
    start = date.fromisoformat(args.start) if args.start else end - __import__("datetime").timedelta(days=365*3)
    strategy_name = args.strategy or "default"

    content = run_advanced_backtest(start, end, strategy_name=strategy_name)

    now = datetime.now().strftime("%Y%m%d_%H%M")
    out = Path("reports") / f"backtest_{strategy_name}_{now}.md"
    out.parent.mkdir(exist_ok=True)
    out.write_text(content, encoding="utf-8")
    print(f"\n报告已保存: {out}")


def cmd_ensemble_backtest(args):
    """多策略组合回测"""
    from src.backtest.ensemble_strategy import run_ensemble_backtest
    from pathlib import Path
    from datetime import datetime

    end = date.fromisoformat(args.end) if args.end else date(2026, 3, 27)
    start = date.fromisoformat(args.start) if args.start else end - __import__("datetime").timedelta(days=365*3)
    ensemble_name = args.ensemble or "tactical"

    content = run_ensemble_backtest(start, end, ensemble_name=ensemble_name)

    now = datetime.now().strftime("%Y%m%d_%H%M")
    out = Path("reports") / f"ensemble_backtest_{ensemble_name}_{now}.md"
    out.parent.mkdir(exist_ok=True)
    out.write_text(content, encoding="utf-8")
    print(f"\n报告已保存: {out}")


def cmd_weekly_backtest(args):
    """每周检查策略回测"""
    from src.backtest.weekly_strategy import run_weekly_backtest
    from pathlib import Path

    end = date.fromisoformat(args.end) if args.end else date(2026, 3, 27)
    start = date.fromisoformat(args.start) if args.start else end - __import__("datetime").timedelta(days=180)

    content = run_weekly_backtest(start, end)

    if args.dry_run:
        print(content)
    else:
        out = Path("reports") / f"weekly_backtest_{end}.md"
        out.parent.mkdir(exist_ok=True)
        out.write_text(content, encoding="utf-8")
        print(f"\n报告已保存: {out}")


def cmd_precompute(args):
    """预计算历史信号"""
    from src.backtest.precompute import run_precompute

    end = date.fromisoformat(args.end) if args.end else date(2026, 3, 27)
    start = date.fromisoformat(args.start) if args.start else end - __import__("datetime").timedelta(days=365*3)
    sources = [args.source] if args.source else None

    run_precompute(start, end, sources=sources, skip_chan=args.skip_chan)


def cmd_list_strategies(args):
    """列出所有策略"""
    from src.backtest.strategy_config import list_strategies
    strategies = list_strategies()
    if not strategies:
        print("暂无策略文件（请在 strategies/ 目录下创建 .json 文件）")
        return
    print(f"共 {len(strategies)} 个策略:\n")
    for s in strategies:
        print(f"  {s['name']:20s} {s['description']}")
        print(f"  {'':20s} {s['file']}")
        print()


def main():
    parser = argparse.ArgumentParser(description="A股集成决策引擎")
    sub = parser.add_subparsers(dest="command")

    # migrate
    sub.add_parser("migrate", help="创建数据库表")

    # collect
    p_collect = sub.add_parser("collect", help="采集子系统信号")
    p_collect.add_argument("--source", type=str, default=None, help="指定单个 source")
    p_collect.add_argument("--date", type=str, default=None, help="分析日期 YYYY-MM-DD")

    # rate
    p_rate = sub.add_parser("rate", help="运行综合评级")
    p_rate.add_argument("--date", type=str, default=None, help="分析日期 YYYY-MM-DD")
    p_rate.add_argument("--adaptive", action="store_true",
                        help="启用自适应权重（基于历史信号准确率）")

    # briefing
    p_brief = sub.add_parser("briefing", help="生成并推送每日简报")
    p_brief.add_argument("--date", type=str, default=None, help="分析日期 YYYY-MM-DD")
    p_brief.add_argument("--dry-run", action="store_true", help="只输出不推送")

    # run
    p_run = sub.add_parser("run", help="全流程 (collect → rate → briefing)")
    p_run.add_argument("--source", type=str, default=None)
    p_run.add_argument("--date", type=str, default=None)
    p_run.add_argument("--dry-run", action="store_true")

    # backtest
    p_bt = sub.add_parser("backtest", help="回测验证")
    p_bt.add_argument("--start", type=str, default=None, help="回测起始日期")
    p_bt.add_argument("--end", type=str, default=None, help="回测结束日期")
    p_bt.add_argument("--holding-days", type=str, default="5,10,20", help="持仓天数,逗号分隔")
    p_bt.add_argument("--dry-run", action="store_true", help="输出到终端不保存文件")

    # hist-backtest
    p_hbt = sub.add_parser("hist-backtest", help="历史回测模拟(6个月)")
    p_hbt.add_argument("--start", type=str, default=None, help="起始日期")
    p_hbt.add_argument("--end", type=str, default=None, help="结束日期")
    p_hbt.add_argument("--hold-days", type=str, default="20", help="持仓交易日数")
    p_hbt.add_argument("--dry-run", action="store_true")

    # advanced-backtest
    p_adv = sub.add_parser("advanced-backtest", help="增强策略回测")
    p_adv.add_argument("--start", type=str, default=None, help="起始日期")
    p_adv.add_argument("--end", type=str, default=None, help="结束日期")
    p_adv.add_argument("--strategy", type=str, default=None, help="策略名称（对应 strategies/*.json）")

    # ensemble-backtest
    p_ens = sub.add_parser("ensemble-backtest", help="多策略组合回测")
    p_ens.add_argument("--start", type=str, default=None, help="起始日期")
    p_ens.add_argument("--end", type=str, default=None, help="结束日期")
    p_ens.add_argument("--ensemble", type=str, default=None, help="组合名称（对应 ensembles/*.json）")

    # list-strategies
    sub.add_parser("list-strategies", help="列出所有策略")

    # weekly-backtest
    p_wk = sub.add_parser("weekly-backtest", help="每周检查策略回测")
    p_wk.add_argument("--start", type=str, default=None, help="起始日期")
    p_wk.add_argument("--end", type=str, default=None, help="结束日期")
    p_wk.add_argument("--dry-run", action="store_true")

    # precompute
    p_pc = sub.add_parser("precompute", help="预计算历史信号")
    p_pc.add_argument("--start", type=str, default=None, help="起始日期")
    p_pc.add_argument("--end", type=str, default=None, help="结束日期")
    p_pc.add_argument("--source", type=str, default=None, help="指定单个 source")
    p_pc.add_argument("--skip-chan", action="store_true", help="跳过缠论计算")

    args = parser.parse_args()

    commands = {
        "migrate": cmd_migrate,
        "collect": cmd_collect,
        "rate": cmd_rate,
        "briefing": cmd_briefing,
        "run": cmd_run,
        "backtest": cmd_backtest,
        "hist-backtest": cmd_hist_backtest,
        "advanced-backtest": cmd_advanced_backtest,
        "ensemble-backtest": cmd_ensemble_backtest,
        "weekly-backtest": cmd_weekly_backtest,
        "precompute": cmd_precompute,
        "list-strategies": cmd_list_strategies,
    }

    handler = commands.get(args.command)
    if handler is None:
        parser.print_help()
        sys.exit(1)

    handler(args)


if __name__ == "__main__":
    main()
