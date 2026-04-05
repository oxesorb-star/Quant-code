import json
import os
import shutil
import time


def safe_load_json(file_path, *, native_exception=Exception, log_terminal_fn=None, alert_fn=None):
    """安全读取 JSON，文件缺失/损坏时返回空字典。"""
    if not os.path.exists(file_path) or os.path.getsize(file_path) < 2:
        return {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except native_exception as e:
        backup_path = ""
        try:
            backup_path = f"{file_path}.bak.{int(time.time())}"
            shutil.copy2(file_path, backup_path)
            if log_terminal_fn:
                log_terminal_fn("JSON 损坏", f"已备份损坏文件 → {os.path.basename(backup_path)}")
        except Exception:
            pass
        if log_terminal_fn:
            log_terminal_fn("JSON 损坏", f"正在重置异常文件 {file_path}: {e}")
        if callable(alert_fn):
            try:
                alert_fn(file_path=file_path, error=e, backup_path=backup_path)
            except Exception:
                pass
        return {}


def atomic_write_json(filepath, data):
    """尽量原子地写入 JSON 文件，环境不支持 replace 时降级为直写。"""
    dirpath = os.path.dirname(filepath)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    temp_filepath = filepath + '.tmp'
    bak_filepath = filepath + '.bak'

    try:
        with open(temp_filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())

        if os.path.exists(filepath):
            try:
                shutil.copy2(filepath, bak_filepath)
            except Exception:
                pass

        try:
            os.replace(temp_filepath, filepath)
        except PermissionError:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            try:
                os.remove(temp_filepath)
            except Exception:
                pass
    except Exception:
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except Exception:
                pass
        raise


def load_local_sync_markers(state_file, *, safe_load_json_fn):
    raw = safe_load_json_fn(state_file)
    markers = {
        "daily_reviews": {},
        "strategy_audits": {},
    }
    if isinstance(raw, dict):
        daily = raw.get("daily_reviews", {})
        audits = raw.get("strategy_audits", {})
        markers["daily_reviews"] = daily if isinstance(daily, dict) else {}
        markers["strategy_audits"] = audits if isinstance(audits, dict) else {}
    return markers


def persist_local_sync_markers(state_file, markers, *, atomic_write_json_fn):
    atomic_write_json_fn(state_file, markers)


def artifact_sync_token(file_path):
    try:
        st = os.stat(file_path)
        return f"{int(st.st_mtime_ns)}:{int(st.st_size)}"
    except OSError:
        return ""
