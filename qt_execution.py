import copy
import collections
import json
import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime

from qt_config import (
    ORDER_EVENTS_FILE,
    POSITIONS_FILE,
    REAL_BALANCE_FILE,
    SIM_BALANCE_FILE,
    SIM_POSITIONS_FILE,
    SIM_TOTAL_CAPITAL,
    TOTAL_CAPITAL,
    TRADE_LOG_FILE,
)
import qt_push_templates as push_templates


logger = logging.getLogger(__name__)

# 执行域状态只归本模块所有：订单状态、幂等、防重、执行锁都在这里维护。
# 主文件只保留薄包装和高层编排，不应再长第二套执行状态。
_TRADE_LOG_LOCK = threading.Lock()
_ORDER_EVENTS_LOCK = threading.Lock()
_execution_meta_lock = threading.Lock()
_symbol_execution_locks = {}
_account_execution_locks = {}
_inflight_execution_keys = set()
_recent_execution_results = {}
_order_latest_status = {}

ORDER_STATUS_TRANSITIONS = {
    "__new__": {"pending", "processing", "rejected", "failed", "done"},
    "pending": {"processing", "rejected", "failed"},
    "processing": {"partial", "done", "rejected", "failed"},
    "partial": {"processing", "done", "rejected", "failed"},
    "done": set(),
    "rejected": set(),
    "failed": {"processing", "done", "rejected"},
}


def bind_trade_log_lock(shared_lock):
    """绑定主进程共享的交易日志锁。

    临时例外：交易日志读侧仍在主文件，因此这把锁暂时采用共享模式。
    等 read_trade_log 相关调用完全收口后，可再评估是否取消共享。
    """
    global _TRADE_LOG_LOCK
    if shared_lock is not None:
        _TRADE_LOG_LOCK = shared_lock


def _make_tracking_id(prefix):
    return f"{prefix}-{datetime.now().strftime('%Y%m%d%H%M%S%f')}-{uuid.uuid4().hex[:6]}"


def _get_named_lock(pool, name):
    with _execution_meta_lock:
        lock = pool.get(name)
        if lock is None:
            lock = threading.RLock()
            pool[name] = lock
        return lock


def get_symbol_execution_lock(acc_type, code):
    return _get_named_lock(_symbol_execution_locks, f"{acc_type}|{str(code).zfill(6)}")


def get_account_execution_lock(acc_type):
    return _get_named_lock(_account_execution_locks, str(acc_type))


def get_account_paths(acc_type):
    clean_acc = 'sim' if acc_type == 'sim' else 'real'
    return (
        SIM_POSITIONS_FILE if clean_acc == 'sim' else POSITIONS_FILE,
        SIM_BALANCE_FILE if clean_acc == 'sim' else REAL_BALANCE_FILE,
    )


def _prune_recent_execution_results():
    now = time.time()
    expired = [k for k, v in _recent_execution_results.items() if now - v.get("timestamp", 0) > 7200]
    for key in expired:
        _recent_execution_results.pop(key, None)


def build_idempotency_key(side, acc_type, code, price=None, volume=None, strategy_batch=None, trade_day=None):
    # strategy_batch 只表示幂等分组/策略批次，不表示信号主键；真正的业务信号统一看 signal_id。
    trade_day = trade_day or datetime.now().strftime("%Y-%m-%d")
    clean_code = str(code or "").strip().zfill(6)
    clean_acc = 'sim' if acc_type == 'sim' else 'real'
    clean_side = str(side or "").strip().lower()
    batch = str(strategy_batch or "default").strip() or "default"
    try:
        price_part = "" if price in (None, "", 0, "0") else f"{float(price):.3f}"
    except Exception:
        price_part = str(price or "").strip()
    try:
        volume_part = "" if volume in (None, "", 0, "0") else str(int(float(volume)))
    except Exception:
        volume_part = str(volume or "").strip()
    return "|".join([trade_day, clean_acc, clean_code, clean_side, price_part, volume_part, batch])


def claim_execution_idempotency(idempotency_key, order_id):
    key = str(idempotency_key or "").strip()
    if not key:
        return True, ""
    with _execution_meta_lock:
        _prune_recent_execution_results()
        if key in _inflight_execution_keys:
            return False, "同一指令正在执行"
        prev = _recent_execution_results.get(key)
        if prev and prev.get("status") == "done":
            return False, f"同一指令已执行成功 ({prev.get('order_id', '')})"
        _inflight_execution_keys.add(key)
    return True, ""


def release_execution_idempotency(idempotency_key, status, order_id, message=""):
    key = str(idempotency_key or "").strip()
    if not key:
        return
    with _execution_meta_lock:
        _inflight_execution_keys.discard(key)
        _recent_execution_results[key] = {
            "status": status,
            "order_id": order_id,
            "message": str(message or "")[:200],
            "timestamp": time.time(),
        }


def append_order_event(order_id, status, side, code, acc_type, **extra):
    with _ORDER_EVENTS_LOCK:
        prev_status = _order_latest_status.get(order_id, "__new__")
        allowed = ORDER_STATUS_TRANSITIONS.get(prev_status, set())
        if status not in allowed:
            logger.warning("订单状态跳转被拦截: %s %s -> %s", order_id, prev_status, status)
            return {
                "blocked": True,
                "order_id": order_id,
                "prev_status": prev_status,
                "attempt_status": status,
            }
        entry = {
            "event_id": _make_tracking_id("OE"),
            "order_id": order_id,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "prev_status": None if prev_status == "__new__" else prev_status,
            "side": side,
            "code": str(code or "").zfill(6),
            "account": acc_type,
            **extra,
        }
        with open(ORDER_EVENTS_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        _order_latest_status[order_id] = status
    return entry


def read_account_state(acc_type, *, get_cached_holdings_fn, safe_load_json_fn, use_cache=True):
    clean_acc = 'sim' if acc_type == 'sim' else 'real'
    pos_file, bal_file = get_account_paths(clean_acc)
    initial_capital = SIM_TOTAL_CAPITAL if clean_acc == 'sim' else TOTAL_CAPITAL
    holdings = None
    if use_cache:
        real_holdings, sim_holdings = get_cached_holdings_fn()
        holdings = sim_holdings if clean_acc == 'sim' else real_holdings
    if holdings is None:
        holdings = safe_load_json_fn(pos_file)
    holdings = holdings or {}
    balance = safe_load_json_fn(bal_file)
    try:
        cash = float(balance.get('cash', float(initial_capital)))
    except Exception:
        cash = float(initial_capital)
    try:
        account_initial_capital = float(balance.get('initial_capital', float(initial_capital)))
    except Exception:
        account_initial_capital = float(initial_capital)
    return {
        "account": clean_acc,
        "positions_file": pos_file,
        "balance_file": bal_file,
        "holdings": holdings,
        "balance": balance,
        "cash": cash,
        "initial_capital": account_initial_capital,
        "configured_capital": float(initial_capital),
    }


def update_balance(acc_type, amount_change, *, atomic_write_json_fn, log_terminal_fn, reason=""):
    balance_file = REAL_BALANCE_FILE if acc_type == 'real' else SIM_BALANCE_FILE
    initial_capital = TOTAL_CAPITAL if acc_type == 'real' else SIM_TOTAL_CAPITAL
    try:
        if os.path.exists(balance_file):
            with open(balance_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            cash = data.get('cash', float(initial_capital))
        else:
            cash = float(initial_capital)

        new_cash = cash + amount_change
        data = {
            'cash': new_cash,
            'initial_capital': initial_capital,
            'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        atomic_write_json_fn(balance_file, data)
        log_terminal_fn("余额更新", f"{'💰' if acc_type == 'real' else '🧪'} {acc_type} 余额: {cash:+.2f} -> {new_cash:+.2f} | 原因: {reason}")
        return new_cash
    except Exception as e:
        log_terminal_fn("余额更新", f"❌ 更新失败: {e}")
        return None


def commit_account_ledger(
    acc_type,
    previous_holdings,
    next_holdings,
    amount_change,
    *,
    safe_load_json_fn,
    atomic_write_json_fn,
    log_terminal_fn,
    logger_instance,
    critical_alert_fn=None,
    reason="",
):
    positions_file, balance_file = get_account_paths(acc_type)
    initial_capital = TOTAL_CAPITAL if acc_type == 'real' else SIM_TOTAL_CAPITAL
    balance_data = safe_load_json_fn(balance_file)
    try:
        pre_cash = float(balance_data.get('cash', float(initial_capital)))
    except Exception:
        pre_cash = float(initial_capital)
    try:
        account_initial_capital = float(balance_data.get('initial_capital', float(initial_capital)))
    except Exception:
        account_initial_capital = float(initial_capital)

    post_cash = pre_cash + amount_change
    if post_cash < -0.01:
        raise RuntimeError(f"余额更新后将为负数: {post_cash:.2f}")

    next_balance = {
        'cash': post_cash,
        'initial_capital': account_initial_capital,
        'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    previous_balance = {
        'cash': pre_cash,
        'initial_capital': account_initial_capital,
        'last_update': str(balance_data.get('last_update', "")) if isinstance(balance_data, dict) else "",
    }

    atomic_write_json_fn(positions_file, next_holdings)
    try:
        atomic_write_json_fn(balance_file, next_balance)
    except Exception as e:
        rollback_errors = []
        try:
            atomic_write_json_fn(positions_file, previous_holdings)
        except Exception as rollback_e:
            rollback_errors.append(f"positions:{rollback_e}")
            logger_instance.error(f"账户账本回滚失败 {acc_type}: {rollback_e}", exc_info=True)
        try:
            atomic_write_json_fn(balance_file, previous_balance)
        except Exception as rollback_e:
            rollback_errors.append(f"balance:{rollback_e}")
            logger_instance.error(f"账户余额回滚失败 {acc_type}: {rollback_e}", exc_info=True)
        if rollback_errors:
            log_terminal_fn("账本异常", f"❌ {acc_type} 账本写入失败且回滚异常，可能出现状态分裂")
            if callable(critical_alert_fn):
                try:
                    critical_alert_fn(
                        acc_type=acc_type,
                        event="账户账本写入失败且回滚异常",
                        fallback="请立即核对 positions/balance 文件",
                        detail=" | ".join(rollback_errors),
                    )
                except Exception:
                    pass
            raise RuntimeError(f"余额写入失败且账本回滚异常，账本可能已分裂: {e}")
        raise RuntimeError(f"余额写入失败，已回滚账本: {e}")

    log_terminal_fn("余额更新", f"{'💰' if acc_type == 'real' else '🧪'} {acc_type} 余额: {pre_cash:+.2f} -> {post_cash:+.2f} | 原因: {reason}")
    return {
        "pre_cash": pre_cash,
        "post_cash": post_cash,
        "balance": next_balance,
    }


def append_trade_log(action, code, name, acc_type, price, volume, *, log_terminal_fn, cloud_sync_add_fn, **extra):
    trade_id = str(extra.pop("trade_id", "") or _make_tracking_id("TRD"))
    entry = {
        "trade_id": trade_id,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "action": action,
        "code": code,
        "name": name,
        "account": acc_type,
        "price": price,
        "volume": volume,
        "amount": round(price * volume, 2),
        **extra
    }
    with _TRADE_LOG_LOCK:
        with open(TRADE_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    log_terminal_fn("交易记录", f"📝 {entry['time']} | {action} | {name}({code}) | {price}×{volume} | ID:{trade_id}")
    cloud_sync_add_fn("trade_logs", entry, trade_id)
    return trade_id


def _standardize_exit_reason_tag(source):
    # exit_reason_tag 只表达“为什么退出”，不要混用为动作来源或策略来源。
    src = str(source or "").strip().lower()
    if any(token in src for token in ("tp1", "first_take_profit")):
        return "tp1"
    if any(token in src for token in ("tp2", "second_take_profit")):
        return "tp2"
    if any(token in src for token in ("tp3", "third_take_profit")):
        return "tp3"
    if "dynamic_stop" in src:
        return "dynamic_stop"
    if "stop" in src:
        return "stop_loss"
    if "risk" in src:
        return "risk_exit"
    if "switch" in src:
        return "switch_exit"
    if "timeout" in src:
        return "timeout_exit"
    if src in {"web_panel", "remote_command", "manual"}:
        return "manual_exit"
    return "other_exit"


def stable_trade_log_id(record):
    """为历史交易日志生成稳定 trade_id，避免启动补同步时重复入云。"""
    if not isinstance(record, dict):
        return ""
    existing = str(record.get("trade_id", "")).strip()
    if existing:
        return existing
    payload = {
        "time": str(record.get("time", "")).strip(),
        "action": str(record.get("action", "")).strip(),
        "code": str(record.get("code", "")).strip().zfill(6),
        "account": str(record.get("account", "")).strip(),
        "price": str(record.get("price", "")).strip(),
        "volume": str(record.get("volume", "")).strip(),
        "amount": str(record.get("amount", "")).strip(),
        "pnl": str(record.get("pnl", "")).strip(),
    }
    digest = uuid.uuid5(uuid.NAMESPACE_DNS, json.dumps(payload, ensure_ascii=False, sort_keys=True)).hex[:16]
    return f"TRDLEGACY-{digest}"


def read_trade_log(limit=50):
    """读取最近 N 条交易记录（deque 方式，内存只保留最近 N 条）"""
    if not os.path.exists(TRADE_LOG_FILE):
        return []
    with _TRADE_LOG_LOCK:
        with open(TRADE_LOG_FILE, 'r', encoding='utf-8') as f:
            recent = collections.deque(f, maxlen=limit)
    records = []
    for line in recent:
        try:
            records.append(json.loads(line.strip()))
        except (json.JSONDecodeError, ValueError):
            continue
    records.reverse()
    return records


def execute_buy(
    code,
    buy_price,
    volume,
    acc_type,
    *,
    precheck_buy_order_fn,
    read_account_state_fn,
    commit_account_ledger_fn,
    cache_reset_fn,
    get_cached_name_fn,
    append_trade_log_fn,
    link_buy_to_decision_fn,
    mark_decision_linked_fn,
    sync_heartbeat_fn,
    log_terminal_fn,
    logger_instance,
    native_exception_cls,
    push_fn=None,
    source="core",
    command_id=None,
    signal_id=None,
    idempotency_key=None,
    strategy_batch=None,
    mode=None,
    watch_id=None,
    gate_decision_source=None,
):
    # source: 动作入口来源（web_panel / remote_command / watch_confirm 等）
    # strategy_batch: 幂等批次键，用于同一轮动作去重
    # gate_decision_source: 第四层闸门结论来源，不等于 source
    order_id = _make_tracking_id("ORD")
    raw_code = str(code or "").strip()
    notify_name = raw_code or "未知标的"

    def _emit_push(title, content, *, cooldown_key=None):
        if not callable(push_fn):
            return
        try:
            push_fn(title, content, code=cooldown_key, urgent=False)
        except Exception:
            logger_instance.warning("买入推送发送失败", exc_info=True)

    def _emit_failure(reason):
        title, content = push_templates.build_execution_fail_message(
            name=notify_name,
            code=raw_code or notify_name,
            account=acc_type,
            action="buy",
            reason=reason,
        )
        _emit_push(title, content, cooldown_key=f"exec_fail:buy:{raw_code or notify_name}")

    if acc_type not in ['sim', 'real']:
        _emit_failure("账户类型无效")
        return False, "账户类型无效"
    if not code:
        _emit_failure("请输入股票代码")
        return False, "请输入股票代码"
    clean_code = str(code).strip().zfill(6)
    notify_name = clean_code
    if not re.match(r'^\d{6}$', clean_code):
        _emit_failure("代码格式无效")
        return False, "代码格式无效（需为6位数字）"
    try:
        buy_price_val = float(buy_price)
        if buy_price_val <= 0:
            _emit_failure("买入价格必须大于0")
            return False, "买入价格必须大于 0"
    except (ValueError, TypeError):
        _emit_failure("买入价格格式无效")
        return False, "买入价格格式无效"
    try:
        volume_val = int(volume)
        if volume_val <= 0:
            _emit_failure("股数必须大于0")
            return False, "股数必须大于 0"
        if volume_val % 100 != 0:
            _emit_failure("A股买入必须是100股整数倍")
            return False, "A股买入必须是 100 股的整数倍"
    except (ValueError, TypeError):
        _emit_failure("股数格式无效")
        return False, "股数格式无效"

    idempotency_key = idempotency_key or build_idempotency_key(
        "buy", acc_type, clean_code, buy_price_val, volume_val, strategy_batch or source
    )
    risk_msg = precheck_buy_order_fn(acc_type, clean_code, buy_price_val, volume_val)
    if risk_msg:
        append_order_event(order_id, "rejected", "buy", clean_code, acc_type,
                           source=source, command_id=command_id, signal_id=signal_id,
                           idempotency_key=idempotency_key, message=risk_msg)
        _emit_failure(f"风控拦截: {risk_msg}")
        return False, f"风控拦截: {risk_msg}"

    claimed, claimed_msg = claim_execution_idempotency(idempotency_key, order_id)
    if not claimed:
        append_order_event(order_id, "rejected", "buy", clean_code, acc_type,
                           source=source, command_id=command_id, signal_id=signal_id,
                           idempotency_key=idempotency_key, message=claimed_msg)
        _emit_failure(claimed_msg)
        return False, claimed_msg

    symbol_lock = get_symbol_execution_lock(acc_type, clean_code)
    account_lock = get_account_execution_lock(acc_type)
    disk_name = "🧪 模拟盘" if acc_type == 'sim' else "💰 实盘"
    stock_name = clean_code

    try:
        append_order_event(order_id, "pending", "buy", clean_code, acc_type,
                           source=source, command_id=command_id, signal_id=signal_id,
                           idempotency_key=idempotency_key, price=buy_price_val, volume=volume_val)
        with symbol_lock:
            with account_lock:
                pre_state = read_account_state_fn(acc_type, use_cache=False)
                pre_cash = pre_state["cash"]
                locked_risk_msg = precheck_buy_order_fn(acc_type, clean_code, buy_price_val, volume_val, use_cache=False)
                if locked_risk_msg:
                    append_order_event(order_id, "rejected", "buy", clean_code, acc_type,
                                       source=source, command_id=command_id, signal_id=signal_id,
                                       idempotency_key=idempotency_key, message=f"锁内复检失败: {locked_risk_msg}")
                    release_execution_idempotency(idempotency_key, "rejected", order_id, locked_risk_msg)
                    _emit_failure(f"风控拦截: {locked_risk_msg}")
                    return False, f"风控拦截: {locked_risk_msg}"

                previous_data = copy.deepcopy(pre_state["holdings"])
                data = copy.deepcopy(pre_state["holdings"])
                if clean_code in data:
                    old = data[clean_code]
                    old_vol = old.get("volume", 0)
                    old_price = old.get("buy_price", 0)
                    new_vol = old_vol + volume_val
                    avg_price = (old_price * old_vol + buy_price_val * volume_val) / new_vol
                    data[clean_code] = {
                        "buy_price": round(avg_price, 3),
                        "volume": new_vol,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")
                    }
                else:
                    data[clean_code] = {
                        "buy_price": buy_price_val,
                        "volume": volume_val,
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")
                    }

                buy_amount = buy_price_val * volume_val
                append_order_event(order_id, "processing", "buy", clean_code, acc_type,
                                   source=source, command_id=command_id, signal_id=signal_id,
                                   idempotency_key=idempotency_key, price=buy_price_val, volume=volume_val,
                                   pre_cash=round(pre_cash, 2), pre_volume=previous_data.get(clean_code, {}).get("volume", 0))
                ledger = commit_account_ledger_fn(
                    acc_type,
                    previous_data,
                    data,
                    -buy_amount,
                    reason=f"买入{clean_code} {volume_val}股@{buy_price_val}"
                )
                post_cash = ledger["post_cash"]
                cache_reset_fn()
                stock_name = get_cached_name_fn(clean_code)
                trade_id = append_trade_log_fn(
                    "买入", clean_code, stock_name, acc_type, buy_price_val, volume_val,
                    order_id=order_id, command_id=command_id, signal_id=signal_id,
                    idempotency_key=idempotency_key, source=source,
                    entry_signal_id=signal_id or "",
                    watch_id=watch_id or "",
                    gate_decision_source=gate_decision_source or "",
                    entry_mode=mode or ("manual" if source == "web_panel" else ""),
                )

        try:
            did = link_buy_to_decision_fn(clean_code, buy_price_val)
            if did:
                mark_decision_linked_fn(did)
                log_terminal_fn("决策关联", f"🔗 {stock_name} 已关联 AI 决策 {did}")
        except native_exception_cls:
            pass

        append_order_event(order_id, "done", "buy", clean_code, acc_type,
                           source=source, command_id=command_id, signal_id=signal_id,
                           idempotency_key=idempotency_key, trade_id=trade_id,
                           pre_cash=round(pre_cash, 2), post_cash=round(post_cash or 0, 2),
                           post_volume=data.get(clean_code, {}).get("volume", volume_val),
                           message=f"{disk_name} 买入成功")
        try:
            sync_heartbeat_fn({
                "event": "trade_buy",
                "last_command": "buy",
                "last_code": clean_code,
                "last_account": acc_type,
                "last_order_id": order_id,
            })
        except Exception:
            pass
        log_terminal_fn("远程买入", f"✅ {disk_name} {stock_name}({clean_code}) {volume_val}股@{buy_price_val} | 订单:{order_id}")
        title, content = push_templates.build_buy_success_message(
            name=stock_name,
            code=clean_code,
            account=acc_type,
            shares=volume_val,
            price=buy_price_val,
            mode=mode or ("manual" if source == "web_panel" else "-"),
            order_id=order_id,
        )
        _emit_push(title, content, cooldown_key=f"buy:{clean_code}")
        release_execution_idempotency(idempotency_key, "done", order_id, "buy ok")
        return True, f"{disk_name} 买入成功: {clean_code} {volume_val}股@{buy_price_val}"
    except Exception as e:
        append_order_event(order_id, "failed", "buy", clean_code, acc_type,
                           source=source, command_id=command_id, signal_id=signal_id,
                           idempotency_key=idempotency_key, message=str(e)[:200])
        release_execution_idempotency(idempotency_key, "failed", order_id, str(e))
        logger_instance.error(f"买入执行失败 {clean_code}: {e}", exc_info=True)
        _emit_failure(str(e))
        return False, f"买入执行失败: {e}"


def execute_sell(
    code,
    volume,
    sell_price_input,
    acc_type,
    *,
    read_account_state_fn,
    commit_account_ledger_fn,
    cache_reset_fn,
    get_cached_name_fn,
    get_shared_market_data_fn,
    get_cached_price_fn,
    append_trade_log_fn,
    link_sell_to_decision_fn,
    mark_decision_linked_fn,
    resolve_risk_events_fn,
    sync_heartbeat_fn,
    log_terminal_fn,
    logger_instance,
    push_fn=None,
    source="core",
    command_id=None,
    signal_id=None,
    idempotency_key=None,
    strategy_batch=None,
    exit_reason_tag=None,
):
    # source: 动作入口来源；exit_reason_tag: 标准化退出原因标签，两者不要混用。
    order_id = _make_tracking_id("ORD")
    raw_code = str(code or "").strip()
    notify_name = raw_code or "未知标的"

    def _emit_push(title, content, *, cooldown_key=None):
        if not callable(push_fn):
            return
        try:
            push_fn(title, content, code=cooldown_key, urgent=False)
        except Exception:
            logger_instance.warning("卖出推送发送失败", exc_info=True)

    def _emit_failure(reason):
        title, content = push_templates.build_execution_fail_message(
            name=notify_name,
            code=raw_code or notify_name,
            account=acc_type,
            action="sell",
            reason=reason,
        )
        _emit_push(title, content, cooldown_key=f"exec_fail:sell:{raw_code or notify_name}")

    if acc_type not in ['sim', 'real']:
        _emit_failure("账户类型无效")
        return False, "账户类型无效"
    if not code:
        _emit_failure("请选择要卖出的股票")
        return False, "请选择要卖出的股票"
    clean_code = str(code).strip().zfill(6)
    notify_name = clean_code
    if not re.match(r'^\d{6}$', clean_code):
        _emit_failure("代码格式无效")
        return False, "代码格式无效"

    try:
        raw_sell_volume = int(str(volume).strip()) if str(volume).strip() else 0
    except ValueError:
        _emit_failure("股数格式无效")
        return False, "股数格式无效"

    idempotency_key = idempotency_key or build_idempotency_key(
        "sell", acc_type, clean_code, sell_price_input, raw_sell_volume or volume, strategy_batch or source
    )
    claimed, claimed_msg = claim_execution_idempotency(idempotency_key, order_id)
    if not claimed:
        append_order_event(order_id, "rejected", "sell", clean_code, acc_type,
                           source=source, command_id=command_id, signal_id=signal_id,
                           idempotency_key=idempotency_key, message=claimed_msg)
        _emit_failure(claimed_msg)
        return False, claimed_msg

    symbol_lock = get_symbol_execution_lock(acc_type, clean_code)
    account_lock = get_account_execution_lock(acc_type)
    disk_name = "🧪 模拟盘" if acc_type == 'sim' else "💰 实盘"
    name = clean_code

    try:
        append_order_event(order_id, "pending", "sell", clean_code, acc_type,
                           source=source, command_id=command_id, signal_id=signal_id,
                           idempotency_key=idempotency_key, price=sell_price_input, volume=raw_sell_volume or "")
        with symbol_lock:
            with account_lock:
                pre_state = read_account_state_fn(acc_type, use_cache=False)
                pre_cash = pre_state["cash"]
                previous_holdings = copy.deepcopy(pre_state["holdings"])
                holdings = copy.deepcopy(pre_state["holdings"])
                if not holdings or clean_code not in holdings:
                    append_order_event(order_id, "rejected", "sell", clean_code, acc_type,
                                       source=source, command_id=command_id, signal_id=signal_id,
                                       idempotency_key=idempotency_key, message="not in holdings")
                    release_execution_idempotency(idempotency_key, "rejected", order_id, "not in holdings")
                    _emit_failure(f"{clean_code} 不在持仓中")
                    return False, f"{clean_code} 不在持仓中"

                info = holdings[clean_code]
                current_volume = int(info.get('volume', 0) or 0)
                buy_price = float(info.get('buy_price', 0) or 0)

                sell_volume = 0
                if volume and str(volume).strip() and str(volume).strip() != '0':
                    try:
                        sell_volume = int(volume)
                    except ValueError:
                        append_order_event(order_id, "rejected", "sell", clean_code, acc_type,
                                           source=source, command_id=command_id, signal_id=signal_id,
                                           idempotency_key=idempotency_key, message="股数格式无效")
                        release_execution_idempotency(idempotency_key, "rejected", order_id, "invalid volume")
                        _emit_failure("股数格式无效")
                        return False, "股数格式无效"
                    if sell_volume < 0:
                        append_order_event(order_id, "rejected", "sell", clean_code, acc_type,
                                           source=source, command_id=command_id, signal_id=signal_id,
                                           idempotency_key=idempotency_key, message="卖出股数不能为负数")
                        release_execution_idempotency(idempotency_key, "rejected", order_id, "negative volume")
                        _emit_failure("卖出股数不能为负数")
                        return False, "卖出股数不能为负数"

                is_full_sell = (sell_volume == 0 or sell_volume >= current_volume)
                if not is_full_sell and sell_volume % 100 != 0:
                    append_order_event(order_id, "rejected", "sell", clean_code, acc_type,
                                       source=source, command_id=command_id, signal_id=signal_id,
                                       idempotency_key=idempotency_key, message="A股部分卖出必须是100股整数倍")
                    release_execution_idempotency(idempotency_key, "rejected", order_id, "lot size invalid")
                    _emit_failure("A股部分卖出必须是100股整数倍")
                    return False, "A股部分卖出必须是 100 股的整数倍"
                if sell_volume > current_volume:
                    append_order_event(order_id, "rejected", "sell", clean_code, acc_type,
                                       source=source, command_id=command_id, signal_id=signal_id,
                                       idempotency_key=idempotency_key, message=f"卖出股数超过持仓({current_volume})")
                    release_execution_idempotency(idempotency_key, "rejected", order_id, "volume exceeds holdings")
                    _emit_failure(f"卖出股数超过持仓({current_volume})")
                    return False, f"卖出股数({sell_volume})超过持仓({current_volume})"

                sell_price = 0.0
                if sell_price_input and str(sell_price_input).strip():
                    try:
                        sell_price = float(sell_price_input)
                    except ValueError:
                        append_order_event(order_id, "rejected", "sell", clean_code, acc_type,
                                           source=source, command_id=command_id, signal_id=signal_id,
                                           idempotency_key=idempotency_key, message="卖出价格格式无效")
                        release_execution_idempotency(idempotency_key, "rejected", order_id, "sell price invalid")
                        _emit_failure("卖出价格格式无效")
                        return False, "卖出价格格式无效"
                    if sell_price <= 0:
                        append_order_event(order_id, "rejected", "sell", clean_code, acc_type,
                                           source=source, command_id=command_id, signal_id=signal_id,
                                           idempotency_key=idempotency_key, message="卖出价格必须大于0")
                        release_execution_idempotency(idempotency_key, "rejected", order_id, "invalid sell price")
                        _emit_failure("卖出价格必须大于0")
                        return False, "卖出价格必须大于 0"
                else:
                    df_spot = get_shared_market_data_fn()
                    cached_price = get_cached_price_fn(clean_code)
                    if cached_price > 0:
                        sell_price = cached_price
                    if df_spot is not None and not getattr(df_spot, "empty", True):
                        row = df_spot[df_spot['代码'] == clean_code]
                        if not row.empty:
                            sell_price = float(row['最新价'].values[0])
                if sell_price <= 0:
                    append_order_event(order_id, "rejected", "sell", clean_code, acc_type,
                                       source=source, command_id=command_id, signal_id=signal_id,
                                       idempotency_key=idempotency_key, message="无法获取有效卖出价")
                    release_execution_idempotency(idempotency_key, "rejected", order_id, "missing sell price")
                    _emit_failure("无法获取有效卖出价")
                    return False, "未提供卖出价格，且无法获取实时行情"

                sold_vol = current_volume if is_full_sell else sell_volume
                append_order_event(order_id, "processing", "sell", clean_code, acc_type,
                                   source=source, command_id=command_id, signal_id=signal_id,
                                   idempotency_key=idempotency_key, price=sell_price, volume=sold_vol,
                                   pre_cash=round(pre_cash, 2), pre_volume=current_volume)

                if is_full_sell:
                    del holdings[clean_code]
                    post_volume = 0
                else:
                    holdings[clean_code]['volume'] = current_volume - sell_volume
                    post_volume = holdings[clean_code]['volume']

                sell_amount = sell_price * sold_vol
                ledger = commit_account_ledger_fn(
                    acc_type,
                    previous_holdings,
                    holdings,
                    sell_amount,
                    reason=f"卖出{clean_code} {sold_vol}股@{sell_price}"
                )
                post_cash = ledger["post_cash"]
                cache_reset_fn()

                pnl = (sell_price - buy_price) * sold_vol
                pnl_pct = ((sell_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
                action = "全部清仓" if is_full_sell else f"部分卖出({sold_vol}股)"
                name = get_cached_name_fn(clean_code)
                trade_id = append_trade_log_fn(
                    action, clean_code, name, acc_type, sell_price, sold_vol,
                    buy_price=buy_price, pnl=round(pnl, 2), pnl_pct=round(pnl_pct, 2),
                    order_id=order_id, command_id=command_id, signal_id=signal_id,
                    idempotency_key=idempotency_key, source=source,
                    exit_reason_tag=exit_reason_tag or _standardize_exit_reason_tag(source),
                )

        try:
            did = link_sell_to_decision_fn(clean_code, is_full_sell)
            if did:
                mark_decision_linked_fn(did)
                log_terminal_fn("决策关联", f"🔗 {name} 已关联 AI 卖出决策 {did}")
        except Exception:
            pass

        resolved_count = resolve_risk_events_fn(acc_type, clean_code, "sell_confirmed", order_id=order_id, trade_id=trade_id)
        append_order_event(order_id, "done", "sell", clean_code, acc_type,
                           source=source, command_id=command_id, signal_id=signal_id,
                           idempotency_key=idempotency_key, trade_id=trade_id,
                           pre_cash=round(pre_cash, 2), post_cash=round(post_cash or 0, 2),
                           post_volume=post_volume, message=f"{action}成功",
                           resolved_risk_events=resolved_count)
        try:
            sync_heartbeat_fn({
                "event": "trade_sell",
                "last_command": "sell",
                "last_code": clean_code,
                "last_account": acc_type,
                "last_order_id": order_id,
            })
        except Exception:
            pass
        log_terminal_fn("远程卖出", f"✅ {disk_name} {name}({clean_code}) {action} {sold_vol}股@{sell_price} 盈亏:{pnl:+.2f} | 订单:{order_id}")
        title, content = push_templates.build_sell_success_message(
            name=name,
            code=clean_code,
            account=acc_type,
            shares=sold_vol,
            price=sell_price,
            pnl_text=f"{pnl_pct:+.1f}% ({pnl:+.2f})",
            reason=push_templates.describe_execution_reason(source, "卖出"),
        )
        _emit_push(title, content, cooldown_key=f"sell:{clean_code}")
        release_execution_idempotency(idempotency_key, "done", order_id, "sell ok")
        return True, f"{disk_name} {action}成功: {clean_code} {sold_vol}股@{sell_price} 盈亏:{pnl:+.2f}"
    except Exception as e:
        append_order_event(order_id, "failed", "sell", clean_code, acc_type,
                           source=source, command_id=command_id, signal_id=signal_id,
                           idempotency_key=idempotency_key, message=str(e)[:200])
        release_execution_idempotency(idempotency_key, "failed", order_id, str(e))
        logger_instance.error(f"卖出执行失败 {clean_code}: {e}", exc_info=True)
        _emit_failure(str(e))
        return False, f"卖出执行失败: {e}"
