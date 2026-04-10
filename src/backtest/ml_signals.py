"""ML 涨停预测信号加载器 — 读取每日 CSV 信号文件"""

import csv
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path


def load_ml_signals(
    signals_path: str,
    start: date,
    end: date,
) -> dict[date, dict[str, float]]:
    """
    加载 ML 涨停预测信号。

    Args:
        signals_path: 信号 CSV 文件目录路径
        start: 回测开始日期
        end: 回测结束日期

    Returns:
        {date: {code: composite_score}}
        code 已从 vnpy 格式（600488.SSE）转换为系统格式（600488）
    """
    signals_dir = Path(signals_path)
    if not signals_dir.exists():
        print(f"[ml_signals] 信号目录不存在: {signals_dir}")
        return {}

    result = {}
    padded_start = start - timedelta(days=10)

    for csv_file in sorted(signals_dir.glob("*.csv")):
        try:
            file_date = date.fromisoformat(csv_file.stem)
        except ValueError:
            continue

        if not (padded_start <= file_date <= end):
            continue

        day_signals = {}
        with open(csv_file, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                vt_symbol = row.get("vt_symbol", "")
                composite_score = float(row.get("composite_score", 0))

                # 转换代码格式: "600488.SSE" → "600488"
                code = _convert_vnpy_code(vt_symbol)
                if code:
                    day_signals[code] = composite_score

        if day_signals:
            result[file_date] = day_signals

    return result


def get_ml_score_at_checkpoint(
    ml_signals: dict[date, dict[str, float]],
    code: str,
    checkpoint: date,
) -> float:
    """
    获取指定股票在检查点当天或之前最近的 ML 信号分数。

    Args:
        ml_signals: load_ml_signals() 的返回值
        code: 股票代码
        checkpoint: 检查点日期

    Returns:
        composite_score（0-1），无信号则返回 0.0
    """
    # 找到 checkpoint 当天或之前最近的信号日期
    best_date = None
    for sig_date in ml_signals:
        if sig_date <= checkpoint:
            if best_date is None or sig_date > best_date:
                best_date = sig_date

    if best_date is None:
        return 0.0

    return ml_signals[best_date].get(code, 0.0)


def _convert_vnpy_code(vt_symbol: str) -> str:
    """将 vnpy 代码格式转换为系统格式: '600488.SSE' → '600488'"""
    if not vt_symbol or "." not in vt_symbol:
        return ""
    return vt_symbol.split(".")[0]
