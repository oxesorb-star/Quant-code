import argparse
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
TRADE_LOG_FILE = DATA_DIR / "trade_log.jsonl"
WATCH_CONFIRM_LOG_FILE = DATA_DIR / "watch_confirm_log.jsonl"
EXECUTION_GATE_LOG_FILE = DATA_DIR / "execution_gate_log.jsonl"


def _load_jsonl(path):
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _parse_time(value):
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _filter_recent(rows, *, days):
    if days <= 0:
        return list(rows)
    cutoff = datetime.now() - timedelta(days=days)
    filtered = []
    for row in rows:
        dt = _parse_time(row.get("time"))
        if dt is None or dt >= cutoff:
            filtered.append(row)
    return filtered


def _fmt_pct(value):
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return "-"


def _fmt_num(value, digits=2):
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "-"


def _trade_stats(rows):
    completed = [
        r for r in rows
        if ("卖出" in str(r.get("action", "")) or "清仓" in str(r.get("action", "")))
        and r.get("pnl_pct") is not None
    ]
    wins = [r for r in completed if float(r.get("pnl_pct", 0) or 0) > 0]
    losses = [r for r in completed if float(r.get("pnl_pct", 0) or 0) <= 0]
    avg_win = sum(float(r.get("pnl_pct", 0) or 0) for r in wins) / len(wins) if wins else 0.0
    avg_loss = sum(float(r.get("pnl_pct", 0) or 0) for r in losses) / len(losses) if losses else 0.0
    profit_ratio = abs(avg_win / avg_loss) if wins and losses and avg_loss != 0 else None
    exit_counter = Counter(
        str(r.get("exit_reason_tag") or "unlabeled").strip() or "unlabeled"
        for r in completed
    )
    linked_signal_count = sum(1 for r in rows if str(r.get("entry_signal_id") or "").strip())
    linked_watch_count = sum(1 for r in rows if str(r.get("watch_id") or "").strip())
    return {
        "total_rows": len(rows),
        "completed": len(completed),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / len(completed)) if completed else 0.0,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "profit_ratio": profit_ratio,
        "exit_counter": exit_counter,
        "linked_signal_count": linked_signal_count,
        "linked_watch_count": linked_watch_count,
    }


def _watch_stats(rows):
    decisions = Counter(str(r.get("decision") or "unknown") for r in rows)
    confirm_buy = decisions.get("confirm_buy", 0)
    return {
        "total": len(rows),
        "decisions": decisions,
        "confirm_buy_rate": (confirm_buy / len(rows)) if rows else 0.0,
    }


def _gate_stats(rows):
    allow_true = [r for r in rows if bool(r.get("allow", False))]
    decision_sources = Counter(str(r.get("decision_source") or "unknown") for r in rows)
    return {
        "total": len(rows),
        "allow_true": len(allow_true),
        "allow_rate": (len(allow_true) / len(rows)) if rows else 0.0,
        "decision_sources": decision_sources,
    }


def _print_counter(title, counter):
    print(title)
    if not counter:
        print("  - 无")
        return
    for key, count in counter.most_common():
        print(f"  - {key}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Phase2 基础回测/归因报告")
    parser.add_argument("--days", type=int, default=30, help="统计最近 N 天，默认 30")
    args = parser.parse_args()

    trade_rows = _filter_recent(_load_jsonl(TRADE_LOG_FILE), days=args.days)
    watch_rows = _filter_recent(_load_jsonl(WATCH_CONFIRM_LOG_FILE), days=args.days)
    gate_rows = _filter_recent(_load_jsonl(EXECUTION_GATE_LOG_FILE), days=args.days)

    trade = _trade_stats(trade_rows)
    watch = _watch_stats(watch_rows)
    gate = _gate_stats(gate_rows)

    print("=== Phase2 基础回测报告 ===")
    print(f"统计区间：最近 {args.days} 天")
    print("")

    print("[交易结果]")
    print(f"总交易日志数：{trade['total_rows']}")
    print(f"已完成卖出样本：{trade['completed']}")
    print(f"胜率：{trade['win_rate']:.1%}")
    print(f"平均盈利：{_fmt_pct(trade['avg_win'])}")
    print(f"平均亏损：{_fmt_pct(trade['avg_loss'])}")
    print(f"盈亏比：{_fmt_num(trade['profit_ratio']) if trade['profit_ratio'] is not None else '-'}")
    print(f"带 entry_signal_id 的记录：{trade['linked_signal_count']}")
    print(f"带 watch_id 的记录：{trade['linked_watch_count']}")
    _print_counter("退出标签分布：", trade["exit_counter"])
    print("")

    print("[第三层观察确认]")
    if watch_rows:
        print(f"样本数：{watch['total']}")
        print(f"confirm_buy 占比：{watch['confirm_buy_rate']:.1%}")
        _print_counter("决策分布：", watch["decisions"])
    else:
        print("样本数：0")
        print("说明：watch_confirm_log.jsonl 还没有正式样本")
    print("")

    print("[第四层执行闸门]")
    if gate_rows:
        print(f"样本数：{gate['total']}")
        print(f"allow=true 占比：{gate['allow_rate']:.1%}")
        _print_counter("来源分布：", gate["decision_sources"])
    else:
        print("样本数：0")
        print("说明：execution_gate_log.jsonl 还没有正式样本")


if __name__ == "__main__":
    main()
