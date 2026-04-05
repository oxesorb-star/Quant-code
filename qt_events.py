import json
import threading
from datetime import datetime


_RISK_EVENTS_LOCK = threading.Lock()
_system_meta_lock = threading.Lock()
_active_risk_events = {}
_provider_health = {}


def export_runtime_state():
    with _system_meta_lock:
        return {
            "active_risk_events": {
                str(key): dict(value or {})
                for key, value in _active_risk_events.items()
            },
            "provider_health": {
                str(key): dict(value or {})
                for key, value in _provider_health.items()
            },
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }


def import_runtime_state(payload):
    payload = payload if isinstance(payload, dict) else {}
    active_raw = payload.get("active_risk_events", {})
    provider_raw = payload.get("provider_health", {})

    active_items = {}
    if isinstance(active_raw, dict):
        for key, value in active_raw.items():
            if isinstance(value, dict):
                active_items[str(key)] = dict(value)

    provider_items = {}
    if isinstance(provider_raw, dict):
        for key, value in provider_raw.items():
            if isinstance(value, dict):
                provider_items[str(key)] = dict(value)

    with _system_meta_lock:
        _active_risk_events.clear()
        _active_risk_events.update(active_items)
        _provider_health.clear()
        _provider_health.update(provider_items)

    return {
        "active_risk_events": len(active_items),
        "provider_health": len(provider_items),
    }


def append_risk_event(event_type, status, code, acc_type, *, risk_events_file, make_tracking_id_fn, **extra):
    entry = {
        "event_id": make_tracking_id_fn("RE"),
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "event_type": event_type,
        "status": status,
        "code": str(code or "").zfill(6),
        "account": acc_type,
        **extra,
    }
    with _RISK_EVENTS_LOCK:
        with open(risk_events_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    return entry


def mark_risk_event_open(event_type, acc_type, code, *, append_risk_event_fn, persist_state_fn=None, **extra):
    key = f"{acc_type}|{str(code).zfill(6)}|{event_type}"
    with _system_meta_lock:
        if key in _active_risk_events:
            return False
        _active_risk_events[key] = {
            "opened_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            **extra,
        }
    if callable(persist_state_fn):
        persist_state_fn()
    append_risk_event_fn(event_type, "triggered", code, acc_type, **extra)
    return True


def resolve_risk_events(acc_type, code, reason, *, append_risk_event_fn, persist_state_fn=None, **extra):
    clean_code = str(code or "").zfill(6)
    resolved = []
    with _system_meta_lock:
        keys = [k for k in _active_risk_events.keys() if k.startswith(f"{acc_type}|{clean_code}|")]
        for key in keys:
            payload = _active_risk_events.pop(key, {})
            event_type = key.split("|", 2)[-1]
            resolved.append((event_type, payload))
    if resolved and callable(persist_state_fn):
        persist_state_fn()
    for event_type, payload in resolved:
        append_risk_event_fn(event_type, "resolved", clean_code, acc_type, reason=reason, **payload, **extra)
    return len(resolved)


def record_provider_result(name, ok, sample_count=0, error="", *, persist_state_fn=None):
    with _system_meta_lock:
        state = _provider_health.setdefault(name, {
            "score": 70,
            "ok_count": 0,
            "fail_count": 0,
            "last_ok": "",
            "last_error": "",
            "last_count": 0,
        })
        if ok:
            state["ok_count"] += 1
            state["last_ok"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            state["last_count"] = int(sample_count or 0)
            state["score"] = min(100, state["score"] + 8)
        else:
            state["fail_count"] += 1
            state["last_error"] = str(error or "")[:200]
            state["score"] = max(0, state["score"] - 15)
        result = dict(state)
    if callable(persist_state_fn):
        persist_state_fn()
    return result


def get_provider_state(name):
    with _system_meta_lock:
        return dict(_provider_health.get(name, {}))


def has_open_risk_events(acc_type=None, code=None):
    clean_account = str(acc_type or "").strip().lower()
    clean_code = str(code or "").zfill(6) if code is not None else ""
    with _system_meta_lock:
        for key in _active_risk_events.keys():
            parts = key.split("|", 2)
            if len(parts) < 3:
                continue
            key_account, key_code, _ = parts
            if clean_account and key_account != clean_account:
                continue
            if clean_code and key_code != clean_code:
                continue
            return True
    return False
