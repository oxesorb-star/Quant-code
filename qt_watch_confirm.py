import os
import json
import threading
import uuid
from datetime import datetime, timedelta

from qt_ai_gateway import ask_ai
from qt_config import DATA_DIR, WATCH_CONFIRM_LOG_FILE
from qt_system_io import atomic_write_json, safe_load_json


WATCH_POOL_FILE = os.path.join(DATA_DIR, "watch_pool.json")
_WATCH_POOL_LOCK = threading.RLock()
_WATCH_LOG_LOCK = threading.RLock()
_ALLOWED_DECISIONS = {"confirm_buy", "wait", "reject", "expired"}
_ALLOWED_CONFIDENCE = {"low", "medium", "high"}
WATCH_CONFIRM_SYSTEM_PROMPT = (
    "你是量化交易系统的第三层观察确认模型。"
    "只能在 confirm_buy / wait / reject 中选择，不得输出其他决策。"
    "规则优先，模型只做辅助确认，不能把规则未达标的候选直接升级成 confirm_buy。"
    "你必须只返回 JSON，字段固定为 decision/confidence/reason/risk_flag。"
)


def _now():
    return datetime.now()


def _iso(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_items(raw):
    def _normalize_signal_id(item):
        if not isinstance(item, dict):
            return item
        normalized = dict(item)
        signal_id = (
            normalized.get("signal_id")
            or normalized.get("decision_signal_id")
            or normalized.get("id")
            or ""
        )
        normalized["signal_id"] = str(signal_id or "")
        return normalized

    if isinstance(raw, list):
        return [_normalize_signal_id(item) for item in raw]
    if isinstance(raw, dict):
        items = raw.get("items", [])
        return [_normalize_signal_id(item) for item in items] if isinstance(items, list) else []
    return []


def _decision_to_status(decision):
    decision = str(decision or "").strip().lower()
    if decision == "wait":
        return "pending"
    if decision == "confirm_buy":
        return "confirmed"
    if decision == "reject":
        return "rejected"
    if decision == "expired":
        return "expired"
    return "pending"


def _apply_refresh_payload(item, refreshed, *, now=None):
    if not isinstance(refreshed, dict):
        return item

    merged = dict(item)
    features = dict(merged.get("features", {}))
    features_update = refreshed.get("features")
    if isinstance(features_update, dict):
        features.update(features_update)
    merged["features"] = features

    if "price" in refreshed:
        try:
            merged["price"] = float(refreshed.get("price") or 0.0)
        except Exception:
            pass

    if "name" in refreshed and refreshed.get("name"):
        merged["name"] = str(refreshed["name"])

    refresh_mode = refreshed.get("refresh_mode")
    if refresh_mode:
        merged["refresh_mode"] = str(refresh_mode)

    refresh_reason = refreshed.get("refresh_reason")
    if refresh_reason:
        merged["refresh_reason"] = str(refresh_reason)

    refreshed_at = refreshed.get("refreshed_at")
    if refreshed_at:
        merged["last_refreshed_at"] = str(refreshed_at)
    elif now is not None:
        merged["last_refreshed_at"] = _iso(now)

    return merged


def _snapshot_watch_features(features):
    features = dict(features or {})
    return {
        "made_new_low": bool(features.get("made_new_low")),
        "higher_low": bool(features.get("higher_low")),
        "reclaimed_ma5_or_ma10": bool(features.get("reclaimed_ma5_or_ma10")),
        "volume_recovered": bool(features.get("volume_recovered")),
        "blowoff_reversal": bool(features.get("blowoff_reversal")),
        "market_worsened": bool(features.get("market_worsened")),
        "sector_strengthening": bool(features.get("sector_strengthening")),
    }


def append_watch_confirm_log(
    *,
    watch_id,
    signal_id,
    code,
    name,
    price,
    decision,
    confidence,
    reason,
    risk_flag,
    rule_decision,
    ai_decision,
    ai_confidence,
    decision_source,
    target_account,
    target_mode,
    default_shares,
    features_snapshot,
    observed_at,
    evaluated_at,
):
    payload = {
        "log_id": f"wclog-{uuid.uuid4().hex[:12]}",
        "time": evaluated_at or _iso(_now()),
        "watch_id": str(watch_id or ""),
        "signal_id": str(signal_id or ""),
        "code": str(code or "").zfill(6),
        "name": str(name or code or ""),
        "price": float(price or 0.0),
        "decision": str(decision or ""),
        "confidence": str(confidence or ""),
        "reason": str(reason or ""),
        "risk_flag": bool(risk_flag),
        "rule_decision": str(rule_decision or ""),
        "ai_decision": ai_decision if ai_decision is None else str(ai_decision),
        "ai_confidence": ai_confidence if ai_confidence is None else str(ai_confidence),
        "decision_source": str(decision_source or "rule"),
        "target_account": str(target_account or "sim"),
        "target_mode": str(target_mode or "trial"),
        "default_shares": int(default_shares or 0),
        "features_snapshot": dict(features_snapshot or {}),
        "observed_at": str(observed_at or ""),
        "evaluated_at": str(evaluated_at or ""),
    }
    with _WATCH_LOG_LOCK:
        with open(WATCH_CONFIRM_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return payload


def load_watch_pool(*, safe_load_json_fn=safe_load_json):
    with _WATCH_POOL_LOCK:
        raw = safe_load_json_fn(WATCH_POOL_FILE)
        return _normalize_items(raw)


def save_watch_pool(items, *, atomic_write_json_fn=atomic_write_json):
    with _WATCH_POOL_LOCK:
        atomic_write_json_fn(WATCH_POOL_FILE, {"items": items})


def evaluate_watch_decision(features, *, now=None, ttl_minutes=None):
    now = now or _now()
    observed_at = features.get("observed_at")
    if observed_at:
        try:
            observed_dt = datetime.strptime(observed_at, "%Y-%m-%d %H:%M:%S")
            effective_ttl = ttl_minutes if ttl_minutes is not None else int(features.get("ttl_minutes", 30) or 30)
            if now > observed_dt + timedelta(minutes=effective_ttl):
                return {
                    "decision": "expired",
                    "confidence": "high",
                    "reason": "观察窗口已过期",
                    "risk_flag": True,
                }
        except Exception:
            pass

    if bool(features.get("made_new_low")):
        return {
            "decision": "reject",
            "confidence": "high",
            "reason": "再次创出新低，放弃确认",
            "risk_flag": True,
        }

    if bool(features.get("blowoff_reversal")):
        return {
            "decision": "reject",
            "confidence": "high",
            "reason": "出现冲高回落，疑似承接不足",
            "risk_flag": True,
        }

    if bool(features.get("market_worsened")):
        return {
            "decision": "wait",
            "confidence": "medium",
            "reason": "大盘环境转弱，先继续观察",
            "risk_flag": True,
        }

    if (
        bool(features.get("higher_low"))
        and bool(features.get("reclaimed_ma5_or_ma10"))
        and bool(features.get("volume_recovered"))
    ):
        reason = "低点抬高且站回短均线，量能回暖"
        if bool(features.get("sector_strengthening")):
            reason += "，板块同步转强"
        return {
            "decision": "confirm_buy",
            "confidence": "medium",
            "reason": reason,
            "risk_flag": False,
        }

    return {
        "decision": "wait",
        "confidence": "low",
        "reason": "结构尚未确认，继续观察",
        "risk_flag": False,
    }


def _build_watch_confirm_prompt(item, rule_outcome):
    features = dict(item.get("features", {}))
    payload = {
        "code": str(item.get("code", "")).zfill(6),
        "name": item.get("name", ""),
        "price": float(item.get("price") or 0.0),
        "observed_at": item.get("observed_at"),
        "ttl_minutes": int(item.get("ttl_minutes") or 30),
        "rule_decision": rule_outcome.get("decision"),
        "rule_confidence": rule_outcome.get("confidence"),
        "rule_reason": rule_outcome.get("reason"),
        "features": {
            "made_new_low": bool(features.get("made_new_low")),
            "higher_low": bool(features.get("higher_low")),
            "reclaimed_ma5_or_ma10": bool(features.get("reclaimed_ma5_or_ma10")),
            "volume_recovered": bool(features.get("volume_recovered")),
            "blowoff_reversal": bool(features.get("blowoff_reversal")),
            "market_worsened": bool(features.get("market_worsened")),
            "sector_strengthening": bool(features.get("sector_strengthening")),
        },
    }
    return (
        "请对以下观察池候选做辅助确认。规则结果优先，你只能辅助否决或补充理由，"
        "不能把规则未达标的 wait 直接升级为 confirm_buy。\n"
        "请只返回 JSON："
        '{"decision":"confirm_buy|wait|reject","confidence":"low|medium|high","reason":"...","risk_flag":false}\n\n'
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )


def _normalize_ai_watch_decision(raw_text):
    if not raw_text:
        return None
    try:
        payload = json.loads(raw_text)
    except Exception:
        return None

    decision = str(payload.get("decision", "")).strip().lower()
    confidence = str(payload.get("confidence", "medium")).strip().lower()
    reason = str(payload.get("reason", "")).strip()
    risk_flag = bool(payload.get("risk_flag", False))

    if decision not in {"confirm_buy", "wait", "reject"}:
        return None
    if confidence not in _ALLOWED_CONFIDENCE:
        confidence = "medium"
    if not reason:
        reason = "Gemini 未提供明确理由"
    return {
        "decision": decision,
        "confidence": confidence,
        "reason": reason[:160],
        "risk_flag": risk_flag,
    }


def _merge_rule_and_ai(rule_outcome, ai_outcome):
    rule_decision = rule_outcome["decision"]
    ai_decision = ai_outcome["decision"]

    final = dict(rule_outcome)
    decision_source = "rule"

    if rule_decision == "confirm_buy":
        if ai_decision == "confirm_buy":
            final = {
                "decision": "confirm_buy",
                "confidence": ai_outcome["confidence"],
                "reason": f"规则确认通过；Gemini复核通过：{ai_outcome['reason']}",
                "risk_flag": bool(ai_outcome["risk_flag"]),
            }
            decision_source = "rule+ai"
        elif ai_decision == "wait":
            final = {
                "decision": "wait",
                "confidence": ai_outcome["confidence"],
                "reason": f"规则通过，但 Gemini 建议继续观察：{ai_outcome['reason']}",
                "risk_flag": bool(ai_outcome["risk_flag"]),
            }
            decision_source = "ai_veto"
        elif ai_decision == "reject":
            final = {
                "decision": "reject",
                "confidence": ai_outcome["confidence"],
                "reason": f"规则通过，但 Gemini 否决：{ai_outcome['reason']}",
                "risk_flag": True,
            }
            decision_source = "ai_veto"
    elif rule_decision == "wait":
        if ai_decision == "reject" and ai_outcome["risk_flag"]:
            final = {
                "decision": "reject",
                "confidence": ai_outcome["confidence"],
                "reason": f"规则建议继续观察，但 Gemini 识别出风险：{ai_outcome['reason']}",
                "risk_flag": True,
            }
            decision_source = "ai_risk_veto"
        elif ai_decision == "confirm_buy":
            final = {
                "decision": "wait",
                "confidence": rule_outcome["confidence"],
                "reason": f"{rule_outcome['reason']}；Gemini 倾向确认，但规则未达标",
                "risk_flag": bool(ai_outcome["risk_flag"]),
            }
            decision_source = "rule_dominant"
        elif ai_decision == "wait":
            final = {
                "decision": "wait",
                "confidence": ai_outcome["confidence"],
                "reason": f"{rule_outcome['reason']}；Gemini 也建议继续观察：{ai_outcome['reason']}",
                "risk_flag": bool(ai_outcome["risk_flag"]) or bool(rule_outcome["risk_flag"]),
            }
            decision_source = "rule+ai_wait"

    final["rule_decision"] = rule_decision
    final["ai_decision"] = ai_decision
    final["ai_confidence"] = ai_outcome["confidence"]
    final["decision_source"] = decision_source
    return final


async def evaluate_watch_decision_with_ai(
    item,
    *,
    now=None,
    ask_ai_fn=ask_ai,
    log_terminal_fn=None,
    logger_instance=None,
):
    now = now or _now()
    rule_outcome = evaluate_watch_decision(
        item.get("features", {}),
        now=now,
        ttl_minutes=item.get("ttl_minutes"),
    )

    base = dict(rule_outcome)
    base["rule_decision"] = rule_outcome["decision"]
    base["ai_decision"] = None
    base["ai_confidence"] = None
    base["decision_source"] = "rule"

    if rule_outcome["decision"] in {"expired", "reject"}:
        return base

    prompt = _build_watch_confirm_prompt(item, rule_outcome)
    ai_raw = await ask_ai_fn(
        stage="watch_confirm",
        prompt=prompt,
        system_prompt=WATCH_CONFIRM_SYSTEM_PROMPT,
        temperature=0.1,
        force_json=True,
        timeout=45.0,
        log_terminal_fn=log_terminal_fn,
        logger_instance=logger_instance,
    )
    ai_outcome = _normalize_ai_watch_decision(ai_raw)
    if not ai_outcome:
        base["decision_source"] = "rule_fallback"
        return base
    return _merge_rule_and_ai(rule_outcome, ai_outcome)


def add_watch_candidate(
    *,
    code,
    name="",
    price=0.0,
    features=None,
    observed_at=None,
    ttl_minutes=30,
    source_stage="deep_audit",
    source_reason="",
    signal_id=None,
    decision_signal_id=None,
    target_account="sim",
    target_mode="trial",
    default_shares=100,
    default_sim_shares=None,
    default_real_shares=None,
):
    features = dict(features or {})
    observed_at = observed_at or _iso(_now())
    features["observed_at"] = observed_at
    features["ttl_minutes"] = ttl_minutes

    item = {
        "watch_id": f"watch-{uuid.uuid4().hex[:12]}",
        "code": str(code).zfill(6),
        "name": str(name or code),
        "price": float(price or 0.0),
        "status": "pending",
        "observed_at": observed_at,
        "ttl_minutes": int(ttl_minutes),
        "source_stage": source_stage,
        "source_reason": source_reason,
        "signal_id": str(signal_id or decision_signal_id or ""),
        "target_account": str(target_account or "sim").strip().lower() or "sim",
        "target_mode": str(target_mode or "trial").strip().lower() or "trial",
        "default_shares": int(default_shares or 100),
        "default_sim_shares": int(default_sim_shares or default_shares or 100),
        "default_real_shares": int(default_real_shares or default_shares or 100),
        "features": features,
        "decision": None,
        "confidence": None,
        "decision_reason": "",
        "risk_flag": False,
        "evaluated_at": None,
    }

    with _WATCH_POOL_LOCK:
        items = load_watch_pool()
        items = [
            x for x in items
            if not (
                x.get("status") == "pending"
                and str(x.get("code", "")).zfill(6) == item["code"]
            )
        ]
        items.append(item)
        save_watch_pool(items)
    return item


def scan_watch_pool(*, now=None):
    now = now or _now()
    with _WATCH_POOL_LOCK:
        items = load_watch_pool()
        results = []
        changed = False

        for item in items:
            if item.get("status") != "pending":
                continue
            item["target_account"] = str(item.get("target_account") or "sim").strip().lower() or "sim"
            if item["target_account"] not in ("sim", "real", "auto"):
                item["target_account"] = "sim"
            item["target_mode"] = str(item.get("target_mode") or "trial").strip().lower() or "trial"
            item["default_shares"] = int(item.get("default_shares") or item.get("default_sim_shares") or 100)
            item["default_sim_shares"] = int(item.get("default_sim_shares") or item.get("default_shares") or 100)
            item["default_real_shares"] = int(item.get("default_real_shares") or item.get("default_shares") or 100)
            outcome = evaluate_watch_decision(
                item.get("features", {}),
                now=now,
                ttl_minutes=item.get("ttl_minutes"),
            )
            decision = outcome["decision"]
            item["status"] = _decision_to_status(decision)
            item["decision"] = decision
            item["confidence"] = outcome["confidence"]
            item["decision_reason"] = outcome["reason"]
            item["risk_flag"] = bool(outcome["risk_flag"])
            item["evaluated_at"] = _iso(now)
            item["rule_decision"] = decision
            item["ai_decision"] = None
            item["ai_confidence"] = None
            item["decision_source"] = "rule"
            changed = True
            result = {
                "watch_id": item["watch_id"],
                "code": item["code"],
                "name": item["name"],
                "price": item["price"],
                "status": item["status"],
                "decision": decision,
                "confidence": outcome["confidence"],
                "reason": outcome["reason"],
                "risk_flag": bool(outcome["risk_flag"]),
                "observed_at": item["observed_at"],
                "evaluated_at": item["evaluated_at"],
                "target_account": item["target_account"],
                "target_mode": item["target_mode"],
                "default_shares": item["default_shares"],
                "default_sim_shares": item["default_sim_shares"],
                "default_real_shares": item["default_real_shares"],
                "signal_id": item.get("signal_id") or item.get("decision_signal_id"),
                "rule_decision": item.get("rule_decision"),
                "ai_decision": item.get("ai_decision"),
                "ai_confidence": item.get("ai_confidence"),
                "decision_source": item.get("decision_source", "rule"),
                "features": dict(item.get("features", {})),
            }
            results.append(result)
            try:
                append_watch_confirm_log(
                    watch_id=result["watch_id"],
                    signal_id=result.get("signal_id"),
                    code=result["code"],
                    name=result["name"],
                    price=result["price"],
                    decision=result["decision"],
                    confidence=result["confidence"],
                    reason=result["reason"],
                    risk_flag=result["risk_flag"],
                    rule_decision=result.get("rule_decision"),
                    ai_decision=result.get("ai_decision"),
                    ai_confidence=result.get("ai_confidence"),
                    decision_source=result.get("decision_source"),
                    target_account=result.get("target_account"),
                    target_mode=result.get("target_mode"),
                    default_shares=result.get("default_shares"),
                    features_snapshot=_snapshot_watch_features(result.get("features", {})),
                    observed_at=result.get("observed_at"),
                    evaluated_at=result.get("evaluated_at"),
                )
            except Exception:
                pass

        if changed:
            save_watch_pool(items)
        return results


async def scan_watch_pool_with_ai(
    *,
    now=None,
    ask_ai_fn=ask_ai,
    refresh_item_fn=None,
    log_terminal_fn=None,
    logger_instance=None,
):
    now = now or _now()
    with _WATCH_POOL_LOCK:
        items_snapshot = load_watch_pool()
        pending_items = [dict(item) for item in items_snapshot if item.get("status") == "pending"]

    evaluated = []
    for item in pending_items:
        refreshed_item = dict(item)
        if refresh_item_fn is not None:
            try:
                refreshed_payload = await refresh_item_fn(dict(refreshed_item))
                refreshed_item = _apply_refresh_payload(refreshed_item, refreshed_payload, now=now)
            except Exception as e:
                if log_terminal_fn:
                    log_terminal_fn("观察池", f"{item.get('code')} 特征刷新失败：{type(e).__name__}")
                elif logger_instance:
                    logger_instance.warning(f"观察池特征刷新失败 {item.get('code')}: {type(e).__name__} - {e}")
        outcome = await evaluate_watch_decision_with_ai(
            refreshed_item,
            now=now,
            ask_ai_fn=ask_ai_fn,
            log_terminal_fn=log_terminal_fn,
            logger_instance=logger_instance,
        )
        evaluated.append((item.get("watch_id"), refreshed_item, outcome))

    with _WATCH_POOL_LOCK:
        items = load_watch_pool()
        by_id = {str(item.get("watch_id")): item for item in items}
        results = []
        changed = False

        for watch_id, refreshed_item, outcome in evaluated:
            item = by_id.get(str(watch_id))
            if not item or item.get("status") != "pending":
                continue
            item["target_account"] = str(item.get("target_account") or "sim").strip().lower() or "sim"
            if item["target_account"] not in ("sim", "real", "auto"):
                item["target_account"] = "sim"
            item["target_mode"] = str(item.get("target_mode") or "trial").strip().lower() or "trial"
            item["default_shares"] = int(item.get("default_shares") or item.get("default_sim_shares") or 100)
            item["default_sim_shares"] = int(item.get("default_sim_shares") or item.get("default_shares") or 100)
            item["default_real_shares"] = int(item.get("default_real_shares") or item.get("default_shares") or 100)
            decision = outcome["decision"]
            item["status"] = _decision_to_status(decision)
            item["decision"] = decision
            item["confidence"] = outcome["confidence"]
            item["decision_reason"] = outcome["reason"]
            item["risk_flag"] = bool(outcome["risk_flag"])
            item["evaluated_at"] = _iso(now)
            item["price"] = float(refreshed_item.get("price") or item.get("price") or 0.0)
            item["name"] = refreshed_item.get("name") or item.get("name")
            item["features"] = dict(refreshed_item.get("features", item.get("features", {})))
            if refreshed_item.get("last_refreshed_at"):
                item["last_refreshed_at"] = refreshed_item.get("last_refreshed_at")
            if refreshed_item.get("refresh_mode"):
                item["refresh_mode"] = refreshed_item.get("refresh_mode")
            if refreshed_item.get("refresh_reason"):
                item["refresh_reason"] = refreshed_item.get("refresh_reason")
            item["rule_decision"] = outcome.get("rule_decision")
            item["ai_decision"] = outcome.get("ai_decision")
            item["ai_confidence"] = outcome.get("ai_confidence")
            item["decision_source"] = outcome.get("decision_source", "rule")
            changed = True
            result = {
                "watch_id": item["watch_id"],
                "code": item["code"],
                "name": item["name"],
                "price": item["price"],
                "status": item["status"],
                "decision": decision,
                "confidence": outcome["confidence"],
                "reason": outcome["reason"],
                "risk_flag": bool(outcome["risk_flag"]),
                "observed_at": item["observed_at"],
                "evaluated_at": item["evaluated_at"],
                "rule_decision": item.get("rule_decision"),
                "ai_decision": item.get("ai_decision"),
                "ai_confidence": item.get("ai_confidence"),
                "decision_source": item.get("decision_source", "rule"),
                "signal_id": item.get("signal_id") or item.get("decision_signal_id"),
                "target_account": item.get("target_account", "sim"),
                "target_mode": item.get("target_mode", "trial"),
                "default_shares": int(item.get("default_shares") or item.get("default_sim_shares") or 100),
                "default_sim_shares": int(item.get("default_sim_shares") or item.get("default_shares") or 100),
                "default_real_shares": int(item.get("default_real_shares") or item.get("default_shares") or 100),
                "refresh_mode": item.get("refresh_mode"),
                "features": dict(item.get("features", {})),
            }
            results.append(result)
            try:
                append_watch_confirm_log(
                    watch_id=result["watch_id"],
                    signal_id=result.get("signal_id"),
                    code=result["code"],
                    name=result["name"],
                    price=result["price"],
                    decision=result["decision"],
                    confidence=result["confidence"],
                    reason=result["reason"],
                    risk_flag=result["risk_flag"],
                    rule_decision=result.get("rule_decision"),
                    ai_decision=result.get("ai_decision"),
                    ai_confidence=result.get("ai_confidence"),
                    decision_source=result.get("decision_source"),
                    target_account=result.get("target_account"),
                    target_mode=result.get("target_mode"),
                    default_shares=result.get("default_shares"),
                    features_snapshot=_snapshot_watch_features(result.get("features", {})),
                    observed_at=result.get("observed_at"),
                    evaluated_at=result.get("evaluated_at"),
                )
            except Exception:
                pass

        if changed:
            save_watch_pool(items)
        return results
