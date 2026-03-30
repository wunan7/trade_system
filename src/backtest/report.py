"""回测报告生成 — 合并信号胜率、共振收益、Alpha 分解为 Markdown"""

from datetime import date
from pathlib import Path

from src.config import PROJECT_ROOT

REPORTS_DIR = PROJECT_ROOT / "reports"


def generate_backtest_report(
    signal_results: list[dict],
    resonance_results: dict,
    alpha_results: list[dict],
    start_date: date,
    end_date: date,
    holding_days: list[int],
) -> str:
    """生成完整回测报告 Markdown"""
    lines = []
    lines.append(f"# 回测验证报告 {start_date} ~ {end_date}")
    lines.append(f"\n持仓天数: {holding_days}")
    lines.append("")

    # === 1. 各模型信号胜率 ===
    lines.append("## 1. 各模型信号胜率")
    lines.append("")
    if signal_results:
        # 表头
        hd_cols = []
        for n in holding_days:
            hd_cols.extend([f"胜率{n}d", f"均值{n}d"])
        header = "| source | signal | count | " + " | ".join(hd_cols) + " |"
        sep = "|" + "------|" * (3 + len(hd_cols))
        lines.append(header)
        lines.append(sep)

        for r in signal_results:
            cols = [r["source"], r["signal"], str(r["count"])]
            for n in holding_days:
                win = r.get(f"win_{n}d")
                avg = r.get(f"avg_ret_{n}d")
                cols.append(f"{win}%" if win is not None else "-")
                cols.append(f"{avg:+.2f}%" if avg is not None else "-")
            lines.append("| " + " | ".join(cols) + " |")
    else:
        lines.append("*无信号数据*")
    lines.append("")

    # === 2. 三重共振收益率 ===
    lines.append("## 2. 三重共振收益率")
    lines.append("")

    for direction in ["buy", "sell"]:
        label = "买入" if direction == "buy" else "卖出"
        data = resonance_results.get(direction)
        if data is None:
            lines.append(f"### 三重共振{label}: 无数据")
            lines.append("")
            continue

        lines.append(f"### 三重共振{label} ({data['count']} 次)")
        lines.append("")
        lines.append("| 持仓天数 | 样本数 | 胜率 | 平均收益 | 中位收益 |")
        lines.append("|---------|--------|------|---------|---------|")
        for n in holding_days:
            count_n = data.get(f"count_{n}d", 0)
            win = data.get(f"win_{n}d")
            avg = data.get(f"avg_ret_{n}d")
            median = data.get(f"median_ret_{n}d")
            lines.append(
                f"| {n}d | {count_n} | "
                f"{win}% | " if win is not None else "- | "
                f"{avg:+.2f}% | " if avg is not None else "- | "
                f"{median:+.2f}% |" if median is not None else "- |"
            )
        lines.append("")

    # === 3. 综合评级 Alpha 分解 ===
    lines.append("## 3. 综合评级 Alpha 分解")
    lines.append("")
    if alpha_results:
        hd_cols = []
        for n in holding_days:
            hd_cols.extend([f"均值{n}d", f"Alpha{n}d", f"胜率{n}d"])
        header = "| 评级 | count | " + " | ".join(hd_cols) + " |"
        sep = "|" + "------|" * (2 + len(hd_cols))
        lines.append(header)
        lines.append(sep)

        for r in alpha_results:
            cols = [r["rating"], str(r["count"])]
            for n in holding_days:
                avg = r.get(f"avg_ret_{n}d")
                alpha = r.get(f"alpha_{n}d")
                win = r.get(f"win_{n}d")
                cols.append(f"{avg:+.2f}%" if avg is not None else "-")
                cols.append(f"{alpha:+.2f}%" if alpha is not None else "-")
                cols.append(f"{win}%" if win is not None else "-")
            lines.append("| " + " | ".join(cols) + " |")
    else:
        lines.append("*无评级数据*")
    lines.append("")

    lines.append("---")
    lines.append("*注: 当前仅有少量天数信号数据，胜率统计仅供参考。需累积 20+ 天数据后结论才可靠。*")

    return "\n".join(lines)


def save_report(content: str, end_date: date) -> Path:
    """保存报告到 reports/ 目录"""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / f"backtest_{end_date}.md"
    path.write_text(content, encoding="utf-8")
    return path
