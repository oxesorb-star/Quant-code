import json
import threading
import uuid
from datetime import datetime, timedelta

from qt_ai_gateway import ask_ai
from qt_config import EXECUTION_GATE_LOG_FILE


_ALLOWED_ACTIONS = {"buy"}
_ALLOWED_MODES = {"trial", "normal", "blocked"}
_EXECUTION_GATE_LOG_LOCK = threading.RLock()
EXECUTION_GATE_SYSTEM_PROMPT = (
    "你是量化交易系统的第四层执行闸门模型。"
    "你的职责是把第三层观察确认结果转成标准化执行许可。"
    "你不能放宽规则约束：如果第三层不是 confirm_buy 或存在风险标记，就不能返回 allow=true。"
    "如果 allow=false，请输出 shares=0，mode=blocked。"
    "如果 allow=true，shares 必须是 A 股 100 股整数倍。"
    "你必须只返回 JSON，字段固定为 allow/action/account/shares/mode/reason/expires_at。"
)


def _now():
    return datetime.now()


def _iso(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_lot_size(shares, default_shares=100):
    try:
        value = int(float(shares))
    except Exception:
        value = int(default_shares)
    if value <= 0:
        value = int(default_shares)
    value = (value // 100) * 100
    if value <= 0:
        value = 100
    return value


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _max_lot_shares(amount, price):
    amount = max(0.0, _safe_float(amount, 0.0))
    price = max(0.0, _safe_float(price, 0.0))
    if amount <= 0 or price <= 0:
        return 0
    return max(0, int(amount / price) // 100 * 100)


def _normalize_account_context(account_context, *, price=0.0, default_shares=100):
    ctx = dict(account_context or {})
    normalized = {
        "cash_available": max(0.0, _safe_float(ctx.get("cash_available"), 0.0)),
        "can_buy_amt": max(0.0, _safe_float(ctx.get("can_buy_amt"), 0.0)),
        "current_total_ratio": max(0.0, _safe_float(ctx.get("current_total_ratio"), 0.0)),
        "single_ratio": max(0.0, _safe_float(ctx.get("single_ratio"), 0.0)),
        "trial_shares_cap": _normalize_lot_size(ctx.get("trial_shares_cap", default_shares), default_shares=default_shares),
        "max_total_ratio": max(0.0, _safe_float(ctx.get("max_total_ratio"), 0.70)),
        "max_single_ratio": max(0.0, _safe_float(ctx.get("max_single_ratio"), 0.40)),
        "blocking_reason": str(ctx.get("blocking_reason", "")).strip(),
    }
    legal_max_shares = _max_lot_shares(
        min(
            normalized["cash_available"] if normalized["cash_available"] > 0 else normalized["can_buy_amt"],
            normalized["can_buy_amt"] if normalized["can_buy_amt"] > 0 else normalized["cash_available"],
        ),
        price,
    )
    if legal_max_shares <= 0:
        legal_max_shares = _max_lot_shares(normalized["cash_available"], price)
    normalized["legal_max_shares"] = legal_max_shares
    return normalized


def append_execution_gate_log(
    *,
    watch_result,
    gate_result,
    account,
    mode,
    account_context,
):
    normalized_context = _normalize_account_context(
        account_context,
        price=float(watch_result.get("price") or 0.0),
        default_shares=int(gate_result.get("shares") or 100 or 100),
    )
    payload = {
        "log_id": f"eglog-{uuid.uuid4().hex[:12]}",
        "time": _iso(_now()),
        "watch_id": str(watch_result.get("watch_id") or ""),
        "signal_id": str(
            watch_result.get("signal_id")
            or watch_result.get("decision_signal_id")
            or ""
        ),
        "code": str(watch_result.get("code", "")).zfill(6),
        "name": str(watch_result.get("name") or watch_result.get("code") or ""),
        "price": float(watch_result.get("price") or 0.0),
        "account": str(gate_result.get("account") or account or "sim"),
        "mode": str(gate_result.get("mode") or mode or "trial"),
        "allow": bool(gate_result.get("allow", False)),
        "shares": int(gate_result.get("shares") or 0),
        "reason": str(gate_result.get("reason") or ""),
        "decision_source": str(gate_result.get("decision_source") or "baseline"),
        "ai_allow": gate_result.get("ai_allow"),
        "ai_reason": gate_result.get("ai_reason"),
        "cash_available": normalized_context["cash_available"],
        "can_buy_amt": normalized_context["can_buy_amt"],
        "current_total_ratio": normalized_context["current_total_ratio"],
        "single_ratio": normalized_context["single_ratio"],
        "legal_max_shares": int(normalized_context["legal_max_shares"] or 0),
        "market_health_score": int(_safe_float((account_context or {}).get("market_health_score"), 0)),
        "halt_new_buys": bool((account_context or {}).get("halt_new_buys", False)),
        "guard_reason": str((account_context or {}).get("guard_reason") or ""),
    }
    with _EXECUTION_GATE_LOG_LOCK:
        with open(EXECUTION_GATE_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return payload


def build_execution_gate_prompt(
    watch_result,
    *,
    account="real",
    default_shares=100,
    mode="trial",
    ttl_minutes=10,
    account_context=None,
):
    watch_price = float(watch_result.get("price") or 0.0)
    normalized_context = _normalize_account_context(
        account_context,
        price=watch_price,
        default_shares=default_shares,
    )
    payload = {
        "watch_result": {
            "code": str(watch_result.get("code", "")).zfill(6),
            "name": watch_result.get("name", ""),
            "price": float(watch_result.get("price") or 0.0),
            "decision": watch_result.get("decision"),
            "confidence": watch_result.get("confidence"),
            "reason": watch_result.get("reason") or watch_result.get("decision_reason", ""),
            "risk_flag": bool(watch_result.get("risk_flag", False)),
            "observed_at": watch_result.get("observed_at"),
            "evaluated_at": watch_result.get("evaluated_at"),
            "rule_decision": watch_result.get("rule_decision"),
            "ai_decision": watch_result.get("ai_decision"),
            "decision_source": watch_result.get("decision_source", "rule"),
        },
        "execution_context": {
            "requested_action": "buy",
            "account": str(account).strip().lower() or "real",
            "default_shares": _normalize_lot_size(default_shares),
            "mode": mode if mode in _ALLOWED_MODES else "trial",
            "ttl_minutes": int(ttl_minutes or 10),
            "cash_available": normalized_context["cash_available"],
            "can_buy_amt": normalized_context["can_buy_amt"],
            "current_total_ratio": normalized_context["current_total_ratio"],
            "single_ratio": normalized_context["single_ratio"],
            "trial_shares_cap": normalized_context["trial_shares_cap"],
            "legal_max_shares": normalized_context["legal_max_shares"],
            "max_total_ratio": normalized_context["max_total_ratio"],
            "max_single_ratio": normalized_context["max_single_ratio"],
            "blocking_reason": normalized_context["blocking_reason"],
        },
    }
    return (
        "请基于第三层观察确认结果输出执行许可。"
        "你不能越权放行：只有第三层 decision=confirm_buy 且 risk_flag=false 时，才可能 allow=true。\n"
        "你不能突破 execution_context 中给出的现金、仓位和 legal_max_shares 约束；如果不满足，就必须 allow=false。\n"
        "只返回 JSON："
        '{"allow":true,"action":"buy","account":"real","shares":100,"mode":"trial","reason":"...","expires_at":"YYYY-MM-DD HH:MM:SS"}\n\n'
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )


def _normalize_ai_execution_gate(raw_text, *, now=None, default_account="real", default_shares=100, ttl_minutes=10):
    if not raw_text:
        return None
    try:
        payload = json.loads(raw_text)
    except Exception:
        return None

    now = now or _now()
    allow = bool(payload.get("allow", False))
    action = str(payload.get("action", "buy")).strip().lower()
    account = str(payload.get("account", default_account)).strip().lower() or default_account
    mode = str(payload.get("mode", "trial")).strip().lower()
    reason = str(payload.get("reason", "")).strip()
    expires_at = str(payload.get("expires_at", "")).strip()

    if action not in _ALLOWED_ACTIONS:
        action = "buy"
    if mode not in _ALLOWED_MODES:
        mode = "trial" if allow else "blocked"
    if not reason:
        reason = "执行闸门模型未提供明确理由"

    if allow:
        shares = _normalize_lot_size(payload.get("shares", default_shares), default_shares=default_shares)
        if not expires_at:
            expires_at = _iso(now + timedelta(minutes=int(ttl_minutes or 10)))
    else:
        shares = 0
        mode = "blocked"
        if not expires_at:
            expires_at = _iso(now + timedelta(minutes=int(ttl_minutes or 10)))

    return {
        "allow": allow,
        "action": action,
        "account": account,
        "shares": shares,
        "mode": mode,
        "reason": reason[:180],
        "expires_at": expires_at,
    }


def baseline_execution_gate(
    watch_result,
    *,
    now=None,
    account="real",
    default_shares=100,
    mode="trial",
    ttl_minutes=10,
    account_context=None,
):
    now = now or _now()
    decision = str(watch_result.get("decision", "")).strip().lower()
    risk_flag = bool(watch_result.get("risk_flag", False))
    code = str(watch_result.get("code", "")).zfill(6)
    price = max(0.0, _safe_float(watch_result.get("price"), 0.0))
    normalized_context = _normalize_account_context(
        account_context,
        price=price,
        default_shares=default_shares,
    )

    if decision != "confirm_buy":
        return {
            "allow": False,
            "action": "buy",
            "account": account,
            "shares": 0,
            "mode": "blocked",
            "reason": f"第三层未确认买入（{decision or 'unknown'}），执行闸门拒绝放行",
            "expires_at": _iso(now + timedelta(minutes=int(ttl_minutes or 10))),
            "decision_source": "baseline",
            "watch_code": code,
        }

    if risk_flag:
        return {
            "allow": False,
            "action": "buy",
            "account": account,
            "shares": 0,
            "mode": "blocked",
            "reason": "第三层带有风险标记，执行闸门拒绝放行",
            "expires_at": _iso(now + timedelta(minutes=int(ttl_minutes or 10))),
            "decision_source": "baseline",
            "watch_code": code,
        }

    if price <= 0:
        return {
            "allow": False,
            "action": "buy",
            "account": account,
            "shares": 0,
            "mode": "blocked",
            "reason": "缺少有效价格，执行闸门拒绝放行",
            "expires_at": _iso(now + timedelta(minutes=int(ttl_minutes or 10))),
            "decision_source": "baseline",
            "watch_code": code,
        }

    if normalized_context["blocking_reason"]:
        return {
            "allow": False,
            "action": "buy",
            "account": account,
            "shares": 0,
            "mode": "blocked",
            "reason": normalized_context["blocking_reason"],
            "expires_at": _iso(now + timedelta(minutes=int(ttl_minutes or 10))),
            "decision_source": "baseline",
            "watch_code": code,
        }

    legal_max_shares = normalized_context["legal_max_shares"]
    if mode == "trial":
        legal_max_shares = min(legal_max_shares, normalized_context["trial_shares_cap"])

    requested_shares = _normalize_lot_size(default_shares, default_shares=default_shares)
    final_shares = min(requested_shares, legal_max_shares) if legal_max_shares > 0 else 0
    if final_shares <= 0:
        reason = (
            f"账户上下文不允许新开仓：现金{normalized_context['cash_available']:.0f}元，"
            f"可买金额{normalized_context['can_buy_amt']:.0f}元，总仓{normalized_context['current_total_ratio']:.1%}，"
            f"单票{normalized_context['single_ratio']:.1%}"
        )
        return {
            "allow": False,
            "action": "buy",
            "account": account,
            "shares": 0,
            "mode": "blocked",
            "reason": reason,
            "expires_at": _iso(now + timedelta(minutes=int(ttl_minutes or 10))),
            "decision_source": "baseline",
            "watch_code": code,
        }

    return {
        "allow": True,
        "action": "buy",
        "account": account,
        "shares": final_shares,
        "mode": mode if mode in _ALLOWED_MODES and mode != "blocked" else "trial",
        "reason": (
            watch_result.get("reason")
            or watch_result.get("decision_reason")
            or "第三层确认通过，允许试错仓"
        ),
        "expires_at": _iso(now + timedelta(minutes=int(ttl_minutes or 10))),
        "decision_source": "baseline",
        "watch_code": code,
        "cash_available": normalized_context["cash_available"],
        "can_buy_amt": normalized_context["can_buy_amt"],
        "current_total_ratio": normalized_context["current_total_ratio"],
        "single_ratio": normalized_context["single_ratio"],
        "legal_max_shares": legal_max_shares,
    }


def _merge_gate_with_ai(baseline, ai_gate):
    final = dict(baseline)
    final["ai_allow"] = ai_gate["allow"]
    final["ai_reason"] = ai_gate["reason"]

    if not baseline["allow"]:
        final["decision_source"] = "baseline"
        return final

    if not ai_gate["allow"]:
        final.update({
            "allow": False,
            "shares": 0,
            "mode": "blocked",
            "reason": f"执行闸门模型否决：{ai_gate['reason']}",
            "expires_at": ai_gate["expires_at"],
            "decision_source": "ai_veto",
        })
        return final

    baseline_cap = int(baseline.get("shares", 0))
    ai_shares = int(ai_gate.get("shares", 0))
    merged_shares = min(x for x in (baseline_cap, ai_shares) if x > 0) if baseline_cap > 0 and ai_shares > 0 else 0
    if merged_shares <= 0:
        final.update({
            "allow": False,
            "shares": 0,
            "mode": "blocked",
            "reason": "执行闸门未获得合法股数，拒绝放行",
            "decision_source": "baseline_clip",
        })
        return final

    final.update({
        "allow": True,
        "action": ai_gate["action"],
        "account": baseline.get("account", ai_gate["account"]),
        "shares": merged_shares,
        "mode": baseline.get("mode", ai_gate["mode"]),
        "reason": ai_gate["reason"],
        "expires_at": ai_gate["expires_at"],
        "decision_source": "baseline+ai",
    })
    return final


async def evaluate_execution_gate(
    watch_result,
    *,
    now=None,
    account="real",
    default_shares=100,
    mode="trial",
    ttl_minutes=10,
    account_context=None,
    ask_ai_fn=ask_ai,
    log_terminal_fn=None,
    logger_instance=None,
):
    now = now or _now()
    baseline = baseline_execution_gate(
        watch_result,
        now=now,
        account=account,
        default_shares=default_shares,
        mode=mode,
        ttl_minutes=ttl_minutes,
        account_context=account_context,
    )

    if not baseline["allow"]:
        baseline["ai_allow"] = None
        baseline["ai_reason"] = None
        try:
            append_execution_gate_log(
                watch_result=watch_result,
                gate_result=baseline,
                account=account,
                mode=mode,
                account_context=account_context,
            )
        except Exception:
            pass
        return baseline

    prompt = build_execution_gate_prompt(
        watch_result,
        account=account,
        default_shares=default_shares,
        mode=mode,
        ttl_minutes=ttl_minutes,
        account_context=account_context,
    )
    ai_raw = await ask_ai_fn(
        stage="execution_gate",
        prompt=prompt,
        system_prompt=EXECUTION_GATE_SYSTEM_PROMPT,
        temperature=0.1,
        force_json=True,
        timeout=45.0,
        log_terminal_fn=log_terminal_fn,
        logger_instance=logger_instance,
    )
    ai_gate = _normalize_ai_execution_gate(
        ai_raw,
        now=now,
        default_account=account,
        default_shares=default_shares,
        ttl_minutes=ttl_minutes,
    )
    if not ai_gate:
        baseline["decision_source"] = "baseline_fallback"
        baseline["ai_allow"] = None
        baseline["ai_reason"] = None
        try:
            append_execution_gate_log(
                watch_result=watch_result,
                gate_result=baseline,
                account=account,
                mode=mode,
                account_context=account_context,
            )
        except Exception:
            pass
        return baseline

    final = _merge_gate_with_ai(baseline, ai_gate)
    try:
        append_execution_gate_log(
            watch_result=watch_result,
            gate_result=final,
            account=account,
            mode=mode,
            account_context=account_context,
        )
    except Exception:
        pass
    return final
