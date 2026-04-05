import json
import os
from datetime import datetime

import httpx


def _consume_jsonl_lines(lines, start_line, on_record, *, warn_fn=None, source_name=""):
    next_line = int(start_line or 0)
    total = len(lines or [])
    for idx in range(next_line, total):
        raw_line = lines[idx]
        line = raw_line.strip()
        if not line:
            next_line = idx + 1
            continue
        try:
            record = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            # 最后一行若还没有换行符，说明可能正处于写入中；本轮不推进游标，避免丢样本。
            if idx == total - 1 and not raw_line.endswith(("\n", "\r")):
                break
            if callable(warn_fn):
                try:
                    warn_fn(
                        source=source_name or "jsonl",
                        event=f"检测到损坏记录，已跳过第 {idx + 1} 行",
                        detail=line[:160],
                    )
                except Exception:
                    pass
            next_line = idx + 1
            continue
        on_record(record)
        next_line = idx + 1
    return next_line


def cloud_sync_headers(*, sync_token, publishable_key):
    return {
        "Authorization": f"Bearer {publishable_key}",
        "X-Sync-Token": sync_token,
        "Content-Type": "application/json",
    }


def cloud_sync_payload(action, collection, *, sync_token, **extra):
    payload = {
        "action": action,
        "collection": collection,
        "syncToken": sync_token,
    }
    payload.update(extra)
    return payload


def cloud_sync_add(
    *,
    sync_queue,
    sync_queue_lock,
    sync_queue_replace_index,
    collection,
    data,
    record_id=None,
    replace_key=None,
    local_marker=None,
):
    if record_id is None:
        record_id = f"{collection}_{data.get('time', '')}_{data.get('id', data.get('code', ''))}"
    with sync_queue_lock:
        if replace_key:
            old_rid = sync_queue_replace_index.get(replace_key)
            old_item = sync_queue.pop(old_rid, None) if old_rid else None
            retry = old_item.get("retry", 0) if old_item else 0
            sync_queue[record_id] = {
                "collection": collection,
                "data": data,
                "retry": retry,
                "replace_key": replace_key,
                "local_marker": local_marker,
            }
            sync_queue_replace_index[replace_key] = record_id
            return
        if record_id not in sync_queue:
            sync_queue[record_id] = {
                "collection": collection,
                "data": data,
                "retry": 0,
                "local_marker": local_marker,
            }


async def cloud_sync_worker(
    *,
    sync_token,
    sync_queue,
    sync_queue_lock,
    sync_queue_replace_index,
    local_sync_markers,
    persist_local_sync_markers_fn,
    tcb_function_url,
    build_payload_fn,
    build_headers_fn,
    logger_instance,
    log_terminal_fn,
    alert_fn=None,
):
    if not sync_token:
        return

    with sync_queue_lock:
        if not sync_queue:
            return
        batch = []
        for rid, item in list(sync_queue.items()):
            batch.append({
                "rid": rid,
                "collection": item["collection"],
                "data": item["data"],
                "retry": item.get("retry", 0),
            })
            if len(batch) >= 50:
                break

    if not batch:
        return

    def _mark_done(record_ids):
        dirty_markers = False
        with sync_queue_lock:
            for rid in record_ids:
                item = sync_queue.pop(rid, None)
                replace_key = item.get("replace_key") if item else None
                if replace_key and sync_queue_replace_index.get(replace_key) == rid:
                    sync_queue_replace_index.pop(replace_key, None)
                local_marker = item.get("local_marker") if item else None
                if isinstance(local_marker, dict):
                    bucket = local_marker.get("bucket")
                    key = local_marker.get("key")
                    token = local_marker.get("token")
                    if bucket in local_sync_markers and key and token:
                        local_sync_markers[bucket][key] = token
                        dirty_markers = True
        if dirty_markers:
            persist_local_sync_markers_fn()

    def _mark_retry(record_ids):
        with sync_queue_lock:
            for rid in record_ids:
                if rid in sync_queue:
                    sync_queue[rid]["retry"] = sync_queue[rid].get("retry", 0) + 1

    by_collection = {}
    for item in batch:
        by_collection.setdefault(item["collection"], []).append(item)

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            for col, items in by_collection.items():
                records = [item["data"] for item in items]
                record_ids = [item["rid"] for item in items]
                next_retry = max((int(item.get("retry", 0) or 0) + 1) for item in items) if items else 0
                dedup_fields = {
                    "trade_logs": ["trade_id"],
                    "ai_decisions": ["signal_id"],
                    "watch_confirm_logs": ["log_id"],
                    "execution_gate_logs": ["log_id"],
                    "daily_reviews": ["date"],
                    "strategy_audits": ["date", "filename"],
                    "account_snapshots": ["snapshot_type", "account", "code"],
                }.get(col, [])
                try:
                    resp = await client.post(
                        tcb_function_url,
                        json=build_payload_fn("add", col, data=records, dedupFields=dedup_fields),
                        headers=build_headers_fn(),
                    )
                    if resp.status_code == 200:
                        result = resp.json()
                        if result.get("code") == 0:
                            _mark_done(record_ids)
                            log_terminal_fn("云端同步", f"☁️ {col} +{len(records)} 条")
                        else:
                            _mark_retry(record_ids)
                            logger_instance.warning(f"云端同步失败 [{col}]: {result}")
                            if callable(alert_fn) and next_retry == 3:
                                try:
                                    alert_fn(
                                        collection=col,
                                        event=f"云端返回失败 code={result.get('code')}",
                                        fallback="本地队列保留并继续重试",
                                    )
                                except Exception:
                                    pass
                    else:
                        _mark_retry(record_ids)
                        logger_instance.warning(f"云端同步HTTP错误 [{col}]: {resp.status_code}")
                        if callable(alert_fn) and next_retry == 3:
                            try:
                                alert_fn(
                                    collection=col,
                                    event=f"HTTP {resp.status_code}",
                                    fallback="本地队列保留并继续重试",
                                )
                            except Exception:
                                pass
                except Exception as e:
                    _mark_retry(record_ids)
                    logger_instance.warning(f"云端同步异常 [{col}]: {e}")
                    if callable(alert_fn) and next_retry == 3:
                        try:
                            alert_fn(
                                collection=col,
                                event=f"{type(e).__name__}: {e}",
                                fallback="本地队列保留并继续重试",
                            )
                        except Exception:
                            pass
    except Exception as e:
        logger_instance.warning(f"云端同步连接异常: {e}")


async def flush_cloud_sync_now(*, sync_pending_data_fn, sync_heartbeat_fn, cloud_sync_worker_fn, status_info=None, include_heartbeat=True):
    await __import__('asyncio').to_thread(sync_pending_data_fn)
    if include_heartbeat:
        if status_info is None:
            await __import__('asyncio').to_thread(sync_heartbeat_fn)
        else:
            await __import__('asyncio').to_thread(sync_heartbeat_fn, status_info)
    await cloud_sync_worker_fn()


def sync_pending_data(
    *,
    load_local_sync_markers_fn,
    trade_log_file,
    trade_log_lock,
    stable_trade_log_id_fn,
    cloud_sync_add_fn,
    ai_decisions_file,
    ai_decisions_lock,
    watch_confirm_log_file,
    execution_gate_log_file,
    daily_review_dir,
    review_dir,
    normalize_cloud_date_fn,
    artifact_sync_token_fn,
    sync_queue,
    sync_queue_lock,
    local_sync_markers,
    sync_last_trade_line,
    sync_last_decision_line,
    sync_last_watch_confirm_line,
    sync_last_execution_gate_line,
    warn_fn=None,
):
    load_local_sync_markers_fn()

    next_trade_line = sync_last_trade_line
    next_decision_line = sync_last_decision_line
    next_watch_confirm_line = sync_last_watch_confirm_line
    next_execution_gate_line = sync_last_execution_gate_line

    if os.path.exists(trade_log_file):
        with trade_log_lock:
            with open(trade_log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        def _handle_trade(record):
            stable_trade_id = stable_trade_log_id_fn(record)
            if stable_trade_id:
                record["trade_id"] = stable_trade_id
            cloud_sync_add_fn(
                "trade_logs",
                record,
                stable_trade_id or f"trade_{record.get('time', '')}_{record.get('code', '')}_{record.get('action', '')}",
            )
        next_trade_line = _consume_jsonl_lines(
            lines,
            sync_last_trade_line,
            _handle_trade,
            warn_fn=warn_fn,
            source_name="trade_log.jsonl",
        )

    if os.path.exists(ai_decisions_file):
        with ai_decisions_lock:
            with open(ai_decisions_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        next_decision_line = _consume_jsonl_lines(
            lines,
            sync_last_decision_line,
            lambda record: cloud_sync_add_fn(
                "ai_decisions",
                {
                    **record,
                    "signal_id": str(record.get("signal_id") or record.get("id") or ""),
                },
                str(record.get("signal_id") or record.get("id") or ""),
            ),
            warn_fn=warn_fn,
            source_name="ai_decisions.jsonl",
        )

    if os.path.exists(watch_confirm_log_file):
        with open(watch_confirm_log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        next_watch_confirm_line = _consume_jsonl_lines(
            lines,
            sync_last_watch_confirm_line,
            lambda record: cloud_sync_add_fn("watch_confirm_logs", record, record.get("log_id")),
            warn_fn=warn_fn,
            source_name="watch_confirm_log.jsonl",
        )

    if os.path.exists(execution_gate_log_file):
        with open(execution_gate_log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        next_execution_gate_line = _consume_jsonl_lines(
            lines,
            sync_last_execution_gate_line,
            lambda record: cloud_sync_add_fn("execution_gate_logs", record, record.get("log_id")),
            warn_fn=warn_fn,
            source_name="execution_gate_log.jsonl",
        )

    if os.path.exists(daily_review_dir):
        for fname in os.listdir(daily_review_dir):
            if fname.startswith("daily_") and fname.endswith(".txt"):
                date_str = fname.replace("daily_", "").replace(".txt", "")
                normalized_date = normalize_cloud_date_fn(date_str)
                rid = f"review_{normalized_date}"
                file_path = os.path.join(daily_review_dir, fname)
                local_token = artifact_sync_token_fn(file_path)
                if local_sync_markers["daily_reviews"].get(normalized_date) == local_token:
                    continue
                with sync_queue_lock:
                    if rid not in sync_queue:
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                            sync_queue[rid] = {
                                "collection": "daily_reviews",
                                "data": {
                                    "date": normalized_date,
                                    "content": content,
                                    "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                },
                                "retry": 0,
                                "local_marker": {
                                    "bucket": "daily_reviews",
                                    "key": normalized_date,
                                    "token": local_token,
                                },
                            }
                        except Exception as e:
                            if callable(warn_fn):
                                try:
                                    warn_fn(
                                        source="daily_reviews",
                                        event=f"读取复盘文件失败: {fname}",
                                        detail=str(e),
                                    )
                                except Exception:
                                    pass

    if os.path.exists(review_dir):
        for fname in os.listdir(review_dir):
            if fname.startswith("audit_") and fname.endswith(".txt"):
                date_str = fname.replace("audit_", "").replace(".txt", "")
                normalized_date = normalize_cloud_date_fn(date_str)
                rid = f"audit_{fname}"
                file_path = os.path.join(review_dir, fname)
                local_token = artifact_sync_token_fn(file_path)
                if local_sync_markers["strategy_audits"].get(fname) == local_token:
                    continue
                with sync_queue_lock:
                    if rid not in sync_queue:
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                            sync_queue[rid] = {
                                "collection": "strategy_audits",
                                "data": {
                                    "date": normalized_date,
                                    "filename": fname,
                                    "content": content,
                                    "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                },
                                "retry": 0,
                                "local_marker": {
                                    "bucket": "strategy_audits",
                                    "key": fname,
                                    "token": local_token,
                                },
                            }
                        except Exception as e:
                            if callable(warn_fn):
                                try:
                                    warn_fn(
                                        source="strategy_audits",
                                        event=f"读取审计文件失败: {fname}",
                                        detail=str(e),
                                    )
                                except Exception:
                                    pass

    return next_trade_line, next_decision_line, next_watch_confirm_line, next_execution_gate_line


def sync_daily_review_to_cloud(*, date_str, report_text, daily_review_dir, normalize_cloud_date_fn, artifact_sync_token_fn, cloud_sync_add_fn):
    normalized_date = normalize_cloud_date_fn(date_str)
    report_file = os.path.join(daily_review_dir, f"daily_{normalized_date.replace('-', '')}.txt")
    local_token = artifact_sync_token_fn(report_file)
    cloud_sync_add_fn(
        "daily_reviews",
        {
            "date": normalized_date,
            "content": report_text,
            "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        f"review_{normalized_date}",
        local_marker={
            "bucket": "daily_reviews",
            "key": normalized_date,
            "token": local_token,
        },
    )


async def claim_remote_command(*, client, doc_id, tcb_function_url, build_payload_fn, build_headers_fn):
    if not doc_id:
        return False
    try:
        resp = await client.post(
            tcb_function_url,
            json=build_payload_fn("claim_command", "trade_commands", docId=doc_id),
            headers=build_headers_fn(),
        )
        if resp.status_code != 200:
            return False
        result = resp.json()
        return result.get("code") == 0
    except Exception:
        return False


async def update_command_status(*, client, doc_id, status, result_msg, tcb_function_url, build_payload_fn, build_headers_fn):
    try:
        await client.post(
            tcb_function_url,
            json=build_payload_fn(
                "update_status",
                "trade_commands",
                docId=doc_id,
                data={
                    "status": status,
                    "result": result_msg,
                    "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
            ),
            headers=build_headers_fn(),
        )
    except Exception:
        pass
