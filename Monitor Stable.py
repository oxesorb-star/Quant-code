import os
import re
import time
import json
import copy
import random
import asyncio
import sys
import subprocess
import tempfile
import requests
import collections
import html
import pandas as pd
from flask import Flask, request, jsonify, redirect, session, url_for
import threading
import akshare as ak
import pandas_ta
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import shutil
import httpx  # 【新增：现代化异步 HTTP 库，更稳定】
import uuid
from functools import wraps
import qt_audit_steps as audit_steps
import qt_ai_gateway as ai_gateway
import qt_events as runtime_events
import qt_execution as execution_mod
import qt_execution_gate as execution_gate_mod
import qt_market_data as market_data
import qt_push_templates as push_templates
import qt_risk_rules as risk_rules
import qt_sync as sync_mod
import qt_system_io as system_io
import qt_watch_confirm as watch_confirm_mod
from qt_config import (
    AI_DECISIONS_FILE,
    AI_DECISION_LINKS_FILE,
    BASE_DIR,
    DATA_DIR,
    FLASK_SECRET_KEY,
    LOG_DIR,
    MARKET_CACHE_FILE,
    MAX_POSITION_RATIO,
    MODEL_GEMMA,
    MODEL_R1,
    OLLAMA_API,
    ORDER_EVENTS_FILE,
    POSITIONS_FILE,
    QUANT_SYSTEM_LOG_FILE,
    REAL_BALANCE_FILE,
    REVIEW_DIR,
    RISK_EVENTS_FILE,
    RUNTIME_EVENTS_STATE_FILE,
    SCT_KEY,
    SIM_BALANCE_FILE,
    SIM_POSITIONS_FILE,
    SIM_TARGET_PROFIT,
    SIM_TOTAL_CAPITAL,
    SINA_SPOT_MODE,
    STOCKS,
    SUMMARY_FILE,
    SYNC_TOKEN,
    TARGET_PROFIT,
    TCB_ENV_ID,
    TCB_FUNCTION_URL,
    TCB_PUBLISHABLE_KEY,
    TOTAL_CAPITAL,
    TRADE_LOG_FILE,
    WATCH_CONFIRM_LOG_FILE,
    EXECUTION_GATE_LOG_FILE,
    AUTO_REAL_ENABLED,
    AUTO_REAL_FORCE_TRIAL,
    AUTO_REAL_MIN_CONFIDENCE,
    AUTO_REAL_MIN_MARKET_SCORE,
    AUTO_REAL_MIN_SHARES,
    ALLOW_PYRAMID_ADD,
    KEEPER_MIN_RETAIN_RATIO,
    PYRAMID_MAX_SINGLE_RATIO,
    PYRAMID_MAX_TOTAL_RATIO,
    PYRAMID_MIN_PNL,
    STRONG_MARKET_SCORE,
    TP1_PCT,
    TP1_SELL_RATIO,
    TP2_PCT,
    TP2_SELL_RATIO,
    TP3_PCT,
    TP3_SELL_RATIO,
    WEAK_MARKET_SCORE,
    WATCH_CONFIRM_TARGET_ACCOUNT,
    WATCH_CONFIRM_TARGET_MODE,
    WEB_ADMIN_PASSWORD,
    WEB_ADMIN_USER,
    _DAILY_REVIEW_DIR,
    _DAILY_REVIEW_FLAG_FILE,
    get_env_optional,
    get_env_required,
    mask_secret,
)

# 【关键修复：保存原生 Exception 引用，防止被异步库覆盖】
import builtins
_NativeException = builtins.Exception


# 全局缓存：仪表盘实时价格缓存
DASHBOARD_CACHE = {}
# 线程锁：防止多线程同时修改缓存造成数据错乱
cache_lock = threading.Lock()

log_format = '%(asctime)s - %(levelname)s - %(message)s'
date_format = '%Y-%m-%d %H:%M:%S'

file_handler = RotatingFileHandler(
    QUANT_SYSTEM_LOG_FILE,
    maxBytes=5 * 1024 * 1024,
    backupCount=5,
    encoding='utf-8'
)
stream_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    datefmt=date_format,
    handlers=[file_handler, stream_handler]
)
logger = logging.getLogger(__name__)

# 压低底层 HTTP 库的常规请求日志，只保留 warning/error。
for noisy_logger_name in ("httpx", "httpcore", "httpcore.connection", "httpcore.http11"):
    logging.getLogger(noisy_logger_name).setLevel(logging.WARNING)


# ==================== 【全局伪装浏览器请求头】====================
# 随机UA池，避免固定UA被网站识别拦截
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.97",
]

# 反爬：monkey-patch requests，让 akshare 内部请求也自动带随机 UA + Referer
# 只在 headers 中没有 UA 时注入，不覆盖已有设置
_referer_map = {
    'sina.com': 'https://finance.sina.com.cn/',
    'eastmoney.com': 'https://quote.eastmoney.com/',
}
_orig_session_request = requests.Session.request

def _patched_request(self, method, url, **kwargs):
    headers = kwargs.get('headers', {})
    if 'User-Agent' not in headers:
        headers['User-Agent'] = random.choice(USER_AGENTS)
    for domain, referer in _referer_map.items():
        if domain in url and 'Referer' not in headers:
            headers['Referer'] = referer
            break
    kwargs['headers'] = headers
    return _orig_session_request(self, method, url, **kwargs)

requests.Session.request = _patched_request

# ==================== 核心配置 =====================
# ==========================================
# 1. 核心配置与环境变量净化
# ==========================================
# 交易变更日志（轻量 JSONL，append-only）
# 临时例外：交易日志读侧仍在主文件，因此主文件持有这把锁，并共享给执行模块。
_TRADE_LOG_LOCK = threading.Lock()
execution_mod.bind_trade_log_lock(_TRADE_LOG_LOCK)

# AI 决策快照（用于交易复盘 · 自动关联买卖与 AI 推理）
_AI_DECISIONS_LOCK = threading.Lock()
_decision_counter = 0  # 决策唯一编号计数器
# 已关联决策ID集合（append-only，避免全文件重写）
_LINKED_IDS_CACHE = None  # None = 尚未从磁盘加载
# SYSTEM_GUARD 是系统级硬风控状态，只回答“当前能不能开新仓”。
# 它不是盈利管理节奏标签；后者统一由 qt_risk_rules.get_market_regime() 给出。
SYSTEM_GUARD = {
    "halt_new_buys": False,
    "reasons": [],
    "updated_at": "",
    "market_health_score": 100,
}
_STARTUP_CHECK_STATUS = {"ok": True, "messages": []}


def _get_linked_ids():
    """获取已关联的决策ID集合（带内存缓存，只读一次磁盘）"""
    global _LINKED_IDS_CACHE
    if _LINKED_IDS_CACHE is None:
        _LINKED_IDS_CACHE = set()
        if os.path.exists(AI_DECISION_LINKS_FILE):
            try:
                with open(AI_DECISION_LINKS_FILE, 'r', encoding='utf-8') as f:
                    _LINKED_IDS_CACHE = set(line.strip() for line in f if line.strip())
            except Exception:
                pass
    return _LINKED_IDS_CACHE


def make_tracking_id(prefix):
    """生成统一追踪ID，贯穿信号、订单、风控、推送等链路。"""
    return f"{prefix}-{datetime.now().strftime('%Y%m%d%H%M%S%f')}-{uuid.uuid4().hex[:6]}"


def get_symbol_execution_lock(acc_type, code):
    return execution_mod.get_symbol_execution_lock(acc_type, code)


def get_account_execution_lock(acc_type):
    return execution_mod.get_account_execution_lock(acc_type)


def get_account_paths(acc_type):
    return execution_mod.get_account_paths(acc_type)


def build_idempotency_key(side, acc_type, code, price=None, volume=None, strategy_batch=None, trade_day=None):
    return execution_mod.build_idempotency_key(side, acc_type, code, price, volume, strategy_batch, trade_day)


def claim_execution_idempotency(idempotency_key, order_id):
    return execution_mod.claim_execution_idempotency(idempotency_key, order_id)


def release_execution_idempotency(idempotency_key, status, order_id, message=""):
    execution_mod.release_execution_idempotency(idempotency_key, status, order_id, message)


def append_order_event(order_id, status, side, code, acc_type, **extra):
    return execution_mod.append_order_event(order_id, status, side, code, acc_type, **extra)


def append_risk_event(event_type, status, code, acc_type, **extra):
    return runtime_events.append_risk_event(
        event_type,
        status,
        code,
        acc_type,
        risk_events_file=RISK_EVENTS_FILE,
        make_tracking_id_fn=make_tracking_id,
        **extra,
    )


def _persist_runtime_event_state():
    system_io.atomic_write_json(RUNTIME_EVENTS_STATE_FILE, runtime_events.export_runtime_state())


def _load_runtime_event_state():
    payload = safe_load_json(RUNTIME_EVENTS_STATE_FILE)
    return runtime_events.import_runtime_state(payload)


def mark_risk_event_open(event_type, acc_type, code, **extra):
    return runtime_events.mark_risk_event_open(
        event_type,
        acc_type,
        code,
        append_risk_event_fn=append_risk_event,
        persist_state_fn=_persist_runtime_event_state,
        **extra,
    )


def resolve_risk_events(acc_type, code, reason, **extra):
    return runtime_events.resolve_risk_events(
        acc_type,
        code,
        reason,
        append_risk_event_fn=append_risk_event,
        persist_state_fn=_persist_runtime_event_state,
        **extra,
    )


def record_provider_result(name, ok, sample_count=0, error=""):
    return runtime_events.record_provider_result(
        name,
        ok,
        sample_count,
        error,
        persist_state_fn=_persist_runtime_event_state,
    )


def get_market_health():
    provider_state = runtime_events.get_provider_state(MARKET_SOURCE)
    return risk_rules.get_market_health(
        last_cache_time=LAST_CACHE_TIME,
        market_source=MARKET_SOURCE,
        provider_state=provider_state,
        consecutive_data_failures=consecutive_data_failures,
        now_ts=time.time(),
        weak_market_score=WEAK_MARKET_SCORE,
    )


def refresh_system_guard():
    health = get_market_health()
    return risk_rules.refresh_system_guard(
        health=health,
        startup_check_ok=_STARTUP_CHECK_STATUS.get("ok", True),
        is_trade_time_now=_is_trade_time_now(),
        system_guard=SYSTEM_GUARD,
        now_text=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


def check_new_buy_guard():
    return risk_rules.check_new_buy_guard(guard=refresh_system_guard())


def startup_self_check():
    messages = []
    ok = True
    for path in (DATA_DIR, LOG_DIR, REVIEW_DIR):
        if not os.path.exists(path):
            ok = False
            messages.append(f"目录缺失: {path}")
            continue
        try:
            probe = os.path.join(path, f".probe_{int(time.time())}.tmp")
            with open(probe, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(probe)
        except Exception as e:
            ok = False
            messages.append(f"目录不可写: {os.path.basename(path)} ({e})")
    for label, path in (
        ("实盘持仓", POSITIONS_FILE),
        ("模拟持仓", SIM_POSITIONS_FILE),
        ("实盘余额", REAL_BALANCE_FILE),
        ("模拟余额", SIM_BALANCE_FILE),
    ):
        try:
            if os.path.exists(path):
                data = safe_load_json(path)
                if not isinstance(data, dict):
                    ok = False
                    messages.append(f"{label} 文件结构异常")
        except Exception as e:
            ok = False
            messages.append(f"{label} 文件校验失败 ({e})")
    if not TCB_FUNCTION_URL.startswith("http"):
        ok = False
        messages.append("TCB_FUNCTION_URL 格式异常")
    if not OLLAMA_API.startswith("http"):
        ok = False
        messages.append("OLLAMA_API 格式异常")
    openrouter_stages = []
    if ai_gateway.resolve_stage_provider("watch_confirm") == "openrouter":
        openrouter_stages.append("watch_confirm")
    if ai_gateway.resolve_stage_provider("execution_gate") == "openrouter":
        openrouter_stages.append("execution_gate")
    if openrouter_stages:
        if not ai_gateway.OPENROUTER_BASE_URL.startswith("http"):
            ok = False
            messages.append("OPENROUTER_BASE_URL 格式异常")
        if not str(ai_gateway.OPENROUTER_API_KEY or "").strip():
            ok = False
            messages.append(f"缺少 OPENROUTER_API_KEY（阶段: {','.join(openrouter_stages)}）")
    if len(str(FLASK_SECRET_KEY or "")) < 16:
        ok = False
        messages.append("FLASK_SECRET_KEY 过短")
    if SYNC_TOKEN == TCB_PUBLISHABLE_KEY:
        messages.append("警告: SYNC_TOKEN 与 Publishable Key 相同，不建议继续共用")
    if SINA_SPOT_MODE == "direct" and os.name == "nt" and sys.version_info >= (3, 13):
        messages.append("警告: Windows/Python3.13 下 direct 新浪链路可能触发 py_mini_racer 崩溃")
    if not messages:
        messages.append("启动自检通过")
    _STARTUP_CHECK_STATUS["ok"] = ok
    _STARTUP_CHECK_STATUS["messages"] = messages
    return dict(_STARTUP_CHECK_STATUS)


# ==================== 异步 AI 请求核心（httpx 稳定版）====================


# 异步 AI 调用（httpx 稳定版）
async def httpx_ask_ollama(model, prompt, temperature=0.2, force_json=False):
    """Legacy helper：保留兼容入口，真实调用已统一收口到 qt_ai_gateway。"""
    return await ai_gateway.ask_ollama(
        model,
        prompt,
        temperature=temperature,
        force_json=force_json,
        timeout=90.0,
    )




def _calculate_holding_stats(holdings, price_map, total_capital=None):
    """
    内部工具函数：统一计算持仓的现值、盈亏和占比
    """
    stats = {}
    total_market_val = 0

    for code, h in holdings.items():
        # 1. 价格获取逻辑统一
        # 优先实时价 -> 其次买入价 -> 最后 0
        p = price_map.get(code, h.get('buy_price', 0))
        p = max(float(p), 0)

        volume = h.get('volume', 0)
        buy_price = h.get('buy_price', 0)

        # 2. 基础数值计算
        market_val = p * volume
        total_market_val += market_val

        # 3. 盈亏计算
        pnl = (p - buy_price) * volume if buy_price > 0 else 0
        pnl_pct = ((p - buy_price) / buy_price * 100) if buy_price > 0 else 0

        stats[code] = {
            "price": p,
            "buy_price": buy_price,
            "market_val": market_val,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "volume": volume
        }

    # 4. 如果提供了总本金，额外计算权重
    if total_capital and total_capital > 0:
        for code in stats:
            stats[code]["weight"] = (stats[code]["market_val"] / total_capital * 100)

    return stats, total_market_val


def safe_load_json(file_path):
    return system_io.safe_load_json(
        file_path,
        native_exception=_NativeException,
        log_terminal_fn=log_terminal,
        alert_fn=lambda *, file_path, error, backup_path="": _push_system_notice(
            module="JSON状态",
            event=f"{os.path.basename(file_path)} 损坏并已重置",
            fallback=(f"已备份到 {os.path.basename(backup_path)}" if backup_path else "已重置为空状态，请尽快核查"),
            code=f"json:corrupt:{os.path.basename(file_path)}",
        ),
    )


def read_account_state(acc_type, use_cache=True):
    return execution_mod.read_account_state(
        acc_type,
        get_cached_holdings_fn=get_cached_holdings,
        safe_load_json_fn=safe_load_json,
        use_cache=use_cache,
    )


# ===================== 【仓位风控计算函数】 =====================
def get_risk_info(holdings, total_capital, price_map):
    return risk_rules.get_risk_info(
        holdings,
        total_capital,
        price_map,
        calculate_holding_stats_fn=_calculate_holding_stats,
    )


# ===================== 【持仓强弱评分 + 换票判断】 =====================
def score_stock(code, price, j_val, rsi, bias_20, vol_ratio):
    """个股评分模型：超跌+量能+趋势综合打分（满分100）"""
    score = 0
    # J值处于低位 加分
    if 0 < j_val < 30: score += 25
    # RSI处于健康区间 加分
    if 20 < rsi < 50: score += 20
    # MA20乖离率合理超跌 加分
    if -15 < bias_20 < -5: score += 25
    # 量比健康 加分
    if 0.8 < vol_ratio < 2.5: score += 15
    # J值极致超跌 额外加分
    if j_val < 10: score += 15
    return score


def get_weakest_holding(holdings, risk, price_map, m_ratio):
    """找出持仓里最弱的一只，用于换票"""
    if not holdings: return None
    min_score = 999
    weak_code = None
    for code, h in holdings.items():
        p = price_map.get(code, h.get('buy_price', 0))
        buy_price = h.get('buy_price', 0)
        # 估算技术指标替代全零参数：
        # j_val: 用价格相对买入价的变化率作为代理（价格越低 j_val 越低）
        j_val = (p - buy_price) / buy_price * 50 - 30 if buy_price > 0 else 0
        # rsi: 用持仓亏损比例代理（亏损越大 rsi 越低）
        rsi = 50 + (p - buy_price) / buy_price * 100 if buy_price > 0 else 50
        # bias_20: 用持仓亏损百分比代理
        bias_20 = (p - buy_price) / buy_price * 100 if buy_price > 0 else 0
        # vol_ratio: 默认 1.0（无 5 分钟数据可用）
        sc = score_stock(code, p, j_val, rsi, bias_20, 1.0)
        if sc < min_score:
            min_score = sc
            weak_code = code
    return weak_code


def get_dashboard_asset_stats(acc_type='real'):
    """
    仪表盘专用：统一计算总资产、市值、总盈亏、各票明细
    返回：(total_assets, total_stock_value, holdings_items, total_pnl)
    total_pnl 现在是"账户权益口径"= 总资产 - 初始本金
    """
    account_state = read_account_state(acc_type, use_cache=False)
    holdings = account_state["holdings"]
    initial_capital = account_state["configured_capital"]
    cash = account_state["cash"]
    balance_file = account_state["balance_file"]

    # 空仓分支：直接用余额文件里的现金作为总资产
    if not holdings:
        total_pnl = cash - initial_capital  # 权益口径：真实现金 - 初始本金
        return cash, 0.0, [], total_pnl

    with cache_lock:
        price_map_full = DASHBOARD_CACHE.copy()
    price_map = get_price_map()

    stats, total_stock = _calculate_holding_stats(holdings, price_map, initial_capital)

    items = []
    floating_pnl = 0.0  # 仅浮盈亏（用于明细展示）
    total_cost = 0.0
    for code, data in stats.items():
        name = price_map_full.get(code, {}).get("name", code)
        floating_pnl += data['pnl']
        total_cost += data['buy_price'] * data['volume']
        items.append({
            "code": code,
            "name": name,
            "price": data['price'],
            "buy_price": data['buy_price'],
            "value": data['market_val'],
            "volume": data['volume'],
            "pnl": data['pnl'],
            "pnl_pct": data['pnl_pct']
        })

    # 首次运行无余额文件时，用 初始本金-持仓成本 估算现金并初始化
    if not os.path.exists(balance_file):
        cash = float(initial_capital) - total_cost
        try:
            atomic_write_json(balance_file, {'cash': cash, 'initial_capital': initial_capital})
        except Exception:
            pass

    total_assets = total_stock + cash
    # 权益口径总盈亏 = 总资产 - 初始本金（包含已实现盈亏 + 浮盈亏）
    total_pnl = total_assets - initial_capital

    return total_assets, total_stock, items, total_pnl


def update_balance(acc_type, amount_change, reason=""):
    return execution_mod.update_balance(
        acc_type,
        amount_change,
        atomic_write_json_fn=atomic_write_json,
        log_terminal_fn=log_terminal,
        reason=reason,
    )


def commit_account_ledger(acc_type, previous_holdings, next_holdings, amount_change, reason=""):
    return execution_mod.commit_account_ledger(
        acc_type,
        previous_holdings,
        next_holdings,
        amount_change,
        safe_load_json_fn=safe_load_json,
        atomic_write_json_fn=atomic_write_json,
        log_terminal_fn=log_terminal,
        logger_instance=logger,
        critical_alert_fn=lambda *, acc_type, event, fallback, detail="": _push_system_notice(
            title="【系统异常】",
            module=f"账本/{acc_type}",
            event=(f"{event} | {detail}" if detail else event),
            fallback=fallback,
            code=f"ledger:{acc_type}",
            urgent=True,
        ),
        reason=reason,
    )


def get_price_map():
    """线程安全地从 DASHBOARD_CACHE 提取 {code: price} 映射表"""
    with cache_lock:
        return {c: data.get("price", 0) for c, data in DASHBOARD_CACHE.items()}


def _reset_holdings_cache():
    global _HOLDINGS_CACHE
    with _holdings_cache_lock:
        _HOLDINGS_CACHE['time'] = 0


def _get_dashboard_cached_name(code):
    with cache_lock:
        return DASHBOARD_CACHE.get(str(code).zfill(6), {}).get('name', str(code).zfill(6))


def _get_dashboard_cached_price(code):
    with cache_lock:
        try:
            return float(DASHBOARD_CACHE.get(str(code).zfill(6), {}).get('price', 0) or 0)
        except Exception:
            return 0.0


def atomic_write_json(filepath, data):
    return system_io.atomic_write_json(filepath, data)


def _load_local_sync_markers():
    global _local_sync_markers_loaded, _local_sync_markers
    if _local_sync_markers_loaded:
        return
    _local_sync_markers = system_io.load_local_sync_markers(
        _LOCAL_SYNC_STATE_FILE,
        safe_load_json_fn=safe_load_json,
    )
    _local_sync_markers_loaded = True


def _persist_local_sync_markers():
    system_io.persist_local_sync_markers(
        _LOCAL_SYNC_STATE_FILE,
        _local_sync_markers,
        atomic_write_json_fn=atomic_write_json,
    )


def _artifact_sync_token(file_path):
    return system_io.artifact_sync_token(file_path)




# 引入磁盘持久化文件名，用于休盘期强制分析

# 股票冷却黑名单：防止短时间内重复审计
blacklist_cooldown = {}
# 本地请求禁用代理，保证Ollama/行情访问速度
os.environ['NO_PROXY'] = '127.0.0.1,localhost'

# 全局股票名称缓存（每4小时刷新，避免每只股票都全量拉取）
_STOCK_NAME_CACHE = {}
_STOCK_NAME_CACHE_TIME = 0
_STOCK_NAME_REFRESHING = False


# 行业板块涨跌数据缓存（交易时段内30分钟刷新）
_SECTOR_BOARD_CACHE = {}       # {板块名称: 涨跌幅}
_SECTOR_BOARD_CACHE_TIME = 0

# 个股→板块映射缓存（避免每只股票都调用 ak.stock_individual_info_em）
_STOCK_INDUSTRY_CACHE = {}      # {股票代码: 板块名称}


async def get_stock_name(code):
    """异步获取股票名称，带全局缓存（4小时过期）+ 线程锁防并发刷新"""
    global _STOCK_NAME_CACHE, _STOCK_NAME_CACHE_TIME, _STOCK_NAME_REFRESHING
    if not _STOCK_NAME_CACHE or time.time() - _STOCK_NAME_CACHE_TIME > 14400:
        if not _STOCK_NAME_REFRESHING:
            _STOCK_NAME_REFRESHING = True
            try:
                name_map = await asyncio.to_thread(ak.stock_info_a_code_name)
                _STOCK_NAME_CACHE = dict(zip(name_map['code'], name_map['name']))
                _STOCK_NAME_CACHE_TIME = time.time()
                logger.info(f"[名称缓存] 已刷新，共 {len(_STOCK_NAME_CACHE)} 只股票")
            except Exception:
                pass
            finally:
                _STOCK_NAME_REFRESHING = False
        else:
            # 另一个协程正在刷新，等它完成
            await asyncio.sleep(0.5)
    return _STOCK_NAME_CACHE.get(code, code)


# 持仓文件缓存（30秒内复用，避免高频磁盘读取）
_HOLDINGS_CACHE = {'real': {}, 'sim': {}, 'time': 0}
_holdings_cache_lock = threading.Lock()


def get_cached_holdings():
    """持仓缓存：30秒内复用，避免高频磁盘读取"""
    global _HOLDINGS_CACHE
    with _holdings_cache_lock:
        if time.time() - _HOLDINGS_CACHE['time'] > 30:
            _HOLDINGS_CACHE['real'] = safe_load_json(POSITIONS_FILE)
            _HOLDINGS_CACHE['sim'] = safe_load_json(SIM_POSITIONS_FILE)
            _HOLDINGS_CACHE['time'] = time.time()
        return _HOLDINGS_CACHE['real'], _HOLDINGS_CACHE['sim']


# 审计日志文件锁（防止并发写入交错损坏）
_log_lock = threading.Lock()

# 盈亏推送冷却机制：同一只票 1 小时内不重复推送
_alert_cooldown = {}
ALERT_COOLDOWN_SECONDS = 3600

# 全局推送冷却：同一只票 30 分钟内不重复推送（覆盖所有推送类型）
_push_cooldown = {}  # {code: last_push_timestamp}
PUSH_COOLDOWN_SECONDS = 1800  # 30 分钟
PUSH_RETRY_MAX_ATTEMPTS = 3
PUSH_RETRY_BACKOFF_SECONDS = 1.5

# 同日首次买入记录：仅追踪"首次买入"，用于T+1拦截卖出
# 加仓不记录（因为已有持仓可随时卖），卖出也不记录（卖后资金回笼可再买）
_daily_new_buy = {}  # {code: date_str}

# AI 置信度缓存：{code: {"gemma_score": 0-40, "ds_confidence": "高/中/低", "timestamp": ...}}
_ai_confidence_cache = {}


def _prune_confidence_cache():
    """清理过期的 AI 置信度缓存（超过200条时清除2小时以上的旧条目）"""
    if len(_ai_confidence_cache) <= 200:
        return
    now = time.time()
    expired = [k for k, v in _ai_confidence_cache.items() if now - v.get('timestamp', 0) > 7200]
    for k in expired:
        del _ai_confidence_cache[k]

def append_trade_log(action, code, name, acc_type, price, volume, **extra):
    return execution_mod.append_trade_log(
        action,
        code,
        name,
        acc_type,
        price,
        volume,
        log_terminal_fn=log_terminal,
        cloud_sync_add_fn=_cloud_sync_add,
        **extra
    )


def _stable_trade_log_id(record):
    return execution_mod.stable_trade_log_id(record)

def read_trade_log(limit=50):
    return execution_mod.read_trade_log(limit)


# ==================== AI 决策快照 & 交易复盘系统 ====================

def save_ai_decision(code, name, decision, confidence, price,
                     j_val, rsi, vol_ratio, bias_20, market_sentiment, reasoning,
                     mode=None, suggested_vol=None, real_vol=None, sim_vol=None,
                     target_stop=None, target_tp1=None, target_tp2=None, target_tp3=None):
    """保存 AI 决策快照，用于后续交易复盘（结构化增强版）"""
    global _decision_counter
    _decision_counter += 1
    decision_id = f"D{datetime.now().strftime('%Y%m%d')}-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}"
    entry = {
        "id": decision_id,
        "signal_id": decision_id,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "code": code,
        "name": name,
        "decision": decision,
        "confidence": confidence,
        "price": price,
        "j_val": round(j_val, 1),
        "rsi": round(rsi, 1),
        "vol_ratio": round(vol_ratio, 1),
        "bias_20": round(bias_20, 2),
        "market_sentiment": market_sentiment,
        "reasoning": reasoning[:500],
        "linked": False,
        "mode": mode,
        "suggested_vol": suggested_vol,
        "real_vol": real_vol,
        "sim_vol": sim_vol,
        "stop_loss": target_stop if target_stop else round(price * 0.975, 2),
        "tp1": target_tp1 if target_tp1 else round(price * 1.035, 2),
        "tp2": target_tp2 if target_tp2 else round(price * 1.05, 2),
        "tp3": target_tp3 if target_tp3 else round(price * 1.10, 2)
    }
    with _AI_DECISIONS_LOCK:
        with open(AI_DECISIONS_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    # 触发云端同步
    _cloud_sync_add("ai_decisions", entry, entry["signal_id"])
    return entry["signal_id"]


def _get_decision_signal_id(record):
    if not isinstance(record, dict):
        return ""
    return str(
        record.get("signal_id")
        or record.get("decision_signal_id")
        or record.get("id")
        or ""
    )


def read_ai_decisions(limit=5):
    """读取最近 N 条 AI 决策快照（deque 方式，用于 Web 展示）"""
    if not os.path.exists(AI_DECISIONS_FILE):
        return []
    linked_ids = _get_linked_ids()
    with _AI_DECISIONS_LOCK:
        with open(AI_DECISIONS_FILE, 'r', encoding='utf-8') as f:
            recent = collections.deque(f, maxlen=limit)
    records = []
    for line in reversed(recent):
        try:
            d = json.loads(line.strip())
            d['_linked'] = _get_decision_signal_id(d) in linked_ids
            records.append(d)
        except (json.JSONDecodeError, ValueError):
            continue
    return records


def link_buy_to_decision(code, buy_price):
    """买入时自动关联最近的 AI 决策（同一代码 + 30 分钟内）"""
    if not os.path.exists(AI_DECISIONS_FILE):
        return None
    now = datetime.now()
    with _AI_DECISIONS_LOCK:
        with open(AI_DECISIONS_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    # 从最新往前找，找最近的匹配决策
    for line in reversed(lines[-100:]):  # 只看最近100条
        try:
            d = json.loads(line.strip())
            signal_id = _get_decision_signal_id(d)
            if d.get("code") == code and signal_id not in _get_linked_ids():
                # 检查时间是否在30分钟内，且决策类型为买入类
                if d.get("decision") not in ("轻仓买入", "买入", "加仓"):
                    continue
                d_time = datetime.strptime(d["time"], "%Y-%m-%d %H:%M")
                if (now - d_time).total_seconds() < 1800:
                    return signal_id
        except (json.JSONDecodeError, ValueError, KeyError):
            continue
    return None


def link_sell_to_decision(code, is_full_sell):
    """卖出时自动关联最近的 AI 卖出类决策（同一代码 + 30 分钟内 + 动作类型匹配）

    Args:
        code: 股票代码（6位）
        is_full_sell: True=全部清仓, False=部分卖出

    Returns:
        匹配到的决策 ID，未匹配返回 None
    """
    if not os.path.exists(AI_DECISIONS_FILE):
        return None
    now = datetime.now()
    with _AI_DECISIONS_LOCK:
        with open(AI_DECISIONS_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    linked = _get_linked_ids()
    # 根据卖出类型确定优先匹配和降级匹配的决策类型
    if is_full_sell:
        primary_types = ("卖出", "止损", "换股")
        fallback_types = ("减仓",)
    else:
        primary_types = ("减仓",)
        fallback_types = ("卖出", "止损", "换股")
    # 第一轮：精确匹配（动作类型 + 同代码 + 30分钟内 + 未关联）
    for line in reversed(lines[-100:]):
        try:
            d = json.loads(line.strip())
            signal_id = _get_decision_signal_id(d)
            if d.get("code") == code and signal_id not in linked:
                d_time = datetime.strptime(d["time"], "%Y-%m-%d %H:%M")
                if (now - d_time).total_seconds() < 1800:
                    if d.get("decision") in primary_types:
                        return signal_id
        except (json.JSONDecodeError, ValueError, KeyError):
            continue
    # 第二轮：降级匹配（动作类型放宽，避免因 AI 措辞偏差漏匹配）
    for line in reversed(lines[-100:]):
        try:
            d = json.loads(line.strip())
            signal_id = _get_decision_signal_id(d)
            if d.get("code") == code and signal_id not in linked:
                d_time = datetime.strptime(d["time"], "%Y-%m-%d %H:%M")
                if (now - d_time).total_seconds() < 1800:
                    if d.get("decision") in fallback_types:
                        return signal_id
        except (json.JSONDecodeError, ValueError, KeyError):
            continue
    return None


def mark_decision_linked(signal_id):
    """标记决策已被实际交易关联（append-only，不重写原文件）"""
    if not signal_id:
        return
    linked = _get_linked_ids()
    if signal_id not in linked:
        linked.add(signal_id)
        with _AI_DECISIONS_LOCK:
            try:
                with open(AI_DECISION_LINKS_FILE, 'a', encoding='utf-8') as f:
                    f.write(signal_id + '\n')
            except Exception as e:
                logger.warning(f"决策关联写入失败: {signal_id} | {e}")
                log_terminal("决策关联", f"⚠️ 决策关联写入失败: {signal_id} | {e}")


def get_trade_lessons_breakdown(limit=5, missed_limit=None):
    """
    生成交易复盘拆分结果：
    - trade_lessons: 已完成交易对账
    - missed_opportunities: 未被采纳的推荐回溯
    """
    trade_lessons = []
    missed_opportunities = []
    decisions = []

    if os.path.exists(AI_DECISIONS_FILE):
        with _AI_DECISIONS_LOCK:
            with open(AI_DECISIONS_FILE, 'r', encoding='utf-8') as f:
                for line in f.readlines()[-200:]:
                    try:
                        decisions.append(json.loads(line.strip()))
                    except (json.JSONDecodeError, ValueError):
                        continue

    # --- 1. 已完成的交易对账 ---
    trade_records = read_trade_log(limit=50)
    sells = [r for r in trade_records if r.get("action") in ("全部清仓", "部分卖出")]
    for sell in sells:
        code = sell.get("code")
        pnl_pct = sell.get("pnl_pct", 0)
        for d in reversed(decisions):
            if (d.get("code") == code
                    and _get_decision_signal_id(d) in _get_linked_ids()
                    and d.get("decision") in ("轻仓买入", "买入", "加仓", "持有", "减仓", "止损")):
                tag = "✅" if pnl_pct >= 0 else "❌"
                outcome = "盈利" if pnl_pct >= 0 else "亏损"
                stock_name = sell.get("name") or d.get("name") or code
                trade_lessons.append(
                    f"{tag} {stock_name}({code}) | "
                    f"[{d['decision']}|J={d.get('j_val','?')}|RSI={d.get('rsi','?')}|"
                    f"量比{d.get('vol_ratio','?')}|大盘{d.get('market_sentiment','?')}] "
                    f"→ {outcome}{pnl_pct:+.1f}%"
                )
                break
        if len(trade_lessons) >= limit:
            break

    # --- 2. 未被采纳的推荐（回溯当天及近3天） ---
    if decisions:
        now = datetime.now()
        seen_codes = set()
        missed_cap = missed_limit if missed_limit is not None else (limit + 3)
        for d in reversed(decisions[-50:]):
            if _get_decision_signal_id(d) in _get_linked_ids() or d.get("decision") not in ("轻仓买入", "买入"):
                continue
            try:
                d_time = datetime.strptime(d["time"], "%Y-%m-%d %H:%M")
                hours_passed = (now - d_time).total_seconds() / 3600
            except ValueError:
                continue
            if hours_passed < 0 or hours_passed > 72:
                continue
            try:
                info_df = ak.stock_individual_info_em(symbol=d["code"])
                if info_df is not None and not info_df.empty:
                    row_p = info_df[info_df['item'] == '最新']
                    current_price = float(row_p['value'].values[0]) if not row_p.empty else 0
                else:
                    current_price = 0
            except Exception:
                current_price = get_price_map().get(d["code"], 0)
            if current_price and d.get("price", 0) > 0:
                change_pct = (current_price - d["price"]) / d["price"] * 100
                if d["code"] in seen_codes:
                    continue
                seen_codes.add(d["code"])
                if abs(change_pct) > 1:
                    stock_name = d.get('name', d['code'])
                    if change_pct > 0:
                        missed_opportunities.append(
                            f"💡 {stock_name}({d['code']}) | 未采纳 | "
                            f"推荐价{d['price']} → 现价{current_price:.2f} ({change_pct:+.1f}%) | 错失机会"
                        )
                    else:
                        missed_opportunities.append(
                            f"✅ {stock_name}({d['code']}) | 未采纳 | "
                            f"推荐价{d['price']} → 现价{current_price:.2f} ({change_pct:+.1f}%) | 幸运躲过"
                        )
            if len(missed_opportunities) >= missed_cap:
                break

    return {
        "trade_lessons": trade_lessons,
        "missed_opportunities": missed_opportunities,
    }


def get_trade_lessons(limit=5):
    """
    生成交易复盘教训摘要，注入到后续 AI 审计 prompt 中。
    来源：已关联且已卖出的交易（对账）+ 未被采纳的推荐（回溯）
    """
    breakdown = get_trade_lessons_breakdown(limit=limit, missed_limit=limit + 3)
    return (breakdown["trade_lessons"] + breakdown["missed_opportunities"])[:limit + 3]


def get_trade_stats_text():
    """从交易记录中计算近期量化胜率，返回结构化文本注入 prompt"""
    records = read_trade_log(limit=50)
    completed = [r for r in records if r.get("pnl_pct") is not None and r.get("action") in ("全部清仓", "部分卖出")]
    if len(completed) < 3:
        return ""
    wins = [r for r in completed if r["pnl_pct"] > 0]
    losses = [r for r in completed if r["pnl_pct"] <= 0]
    win_rate = len(wins) / len(completed)
    avg_win = sum(r["pnl_pct"] for r in wins) / len(wins) if wins else 0
    avg_loss = sum(r["pnl_pct"] for r in losses) / len(losses) if losses else 0
    ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
    return (
        f"📌 【历史绩效量化（近{len(completed)}笔）】"
        f"\n- 胜率：{win_rate:.0%} | 平均盈利：{avg_win:+.1f}% | 平均亏损：{avg_loss:+.1f}%"
        f"\n- 盈亏比：{min(ratio, 99):.1f}:1"
        f"\n- 注意：若当前信号特征与亏损样本高度相似，请主动降低置信度\n"
    )


# 行情数据线程锁（data_refresher 写 / 主循环+Flask 读）
_market_data_lock = threading.Lock()

# --- 每日自动复盘 ---
def _has_daily_review(date_str):
    """检查某天是否已生成过每日复盘"""
    if not os.path.exists(_DAILY_REVIEW_FLAG_FILE):
        return False
    try:
        with open(_DAILY_REVIEW_FLAG_FILE, 'r') as f:
            return date_str in f.read()
    except Exception:
        return False


def _mark_daily_review_done(date_str):
    """标记某天已生成复盘"""
    try:
        existing = set()
        if os.path.exists(_DAILY_REVIEW_FLAG_FILE):
            with open(_DAILY_REVIEW_FLAG_FILE, 'r', encoding='utf-8') as f:
                existing = {line.strip() for line in f if line.strip()}
        if date_str not in existing:
            with open(_DAILY_REVIEW_FLAG_FILE, 'a', encoding='utf-8') as f:
                f.write(date_str + '\n')
    except Exception:
        pass


def generate_daily_review(report_date=None, force=False):
    """生成每日自动复盘报告，写入 Strategy_Review/daily_YYYYMMDD.txt"""
    if report_date is None:
        report_date = datetime.now().strftime('%Y-%m-%d')
    date_str = report_date

    # 防重复
    if not force and _has_daily_review(date_str):
        log_terminal("每日复盘", f"📅 {date_str} 复盘已生成，跳过")
        return

    try:
        # 1. 统计今日 AI 决策
        ai_count = 0
        ai_buy_count = 0
        ai_sell_count = 0
        high_conf_count = 0
        decisions_today = []
        if os.path.exists(AI_DECISIONS_FILE):
            with _AI_DECISIONS_LOCK:
                with open(AI_DECISIONS_FILE, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            d = json.loads(line.strip())
                            if d.get('time', '').startswith(date_str):
                                ai_count += 1
                                decisions_today.append(d)
                                if d.get('confidence') in ('高', '中'):
                                    high_conf_count += 1
                                dec = d.get('decision', '')
                                if any(w in dec for w in ['买入', '轻仓买入', '加仓']):
                                    ai_buy_count += 1
                                elif any(w in dec for w in ['减仓', '止损', '换股']):
                                    ai_sell_count += 1
                        except (json.JSONDecodeError, ValueError):
                            continue

        # 2. 统计今日实际交易（实盘/模拟盘分开）
        real_buys = 0
        real_sells = 0
        real_pnl = 0.0
        sim_buys = 0
        sim_sells = 0
        sim_pnl = 0.0
        if os.path.exists(TRADE_LOG_FILE):
            with _TRADE_LOG_LOCK:
                with open(TRADE_LOG_FILE, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            t = json.loads(line.strip())
                            if t.get('time', '').startswith(date_str):
                                action = t.get('action', '')
                                acc = t.get('account', '')
                                if '买入' in action:
                                    if acc == 'sim':
                                        sim_buys += 1
                                    else:
                                        real_buys += 1
                                elif '卖出' in action or '清仓' in action:
                                    if acc == 'sim':
                                        sim_sells += 1
                                    else:
                                        real_sells += 1
                                pnl = t.get('pnl', 0)
                                if isinstance(pnl, (int, float)):
                                    if acc == 'sim':
                                        sim_pnl += pnl
                                    else:
                                        real_pnl += pnl
                        except (json.JSONDecodeError, ValueError):
                            continue

        # 3. 交易教训 / 错失机会摘要
        lesson_breakdown = get_trade_lessons_breakdown(limit=3, missed_limit=3)
        trade_lessons = lesson_breakdown["trade_lessons"]
        missed_opportunities = lesson_breakdown["missed_opportunities"]
        trade_lesson_text = '\n'.join(f"  - {l}" for l in trade_lessons[:3]) if trade_lessons else "  无"
        missed_text = '\n'.join(f"  - {l}" for l in missed_opportunities[:3]) if missed_opportunities else "  无"

        # 4. 计算当前持仓盈亏（实盘/模拟盘分开）
        real_holdings, sim_holdings = get_cached_holdings()

        def _fallback_stock_name(code):
            code = str(code).zfill(6)
            cached_name = str(DASHBOARD_CACHE.get(code, {}).get('name') or "").strip()
            if cached_name and cached_name != code:
                return cached_name
            cached_name = str(_STOCK_NAME_CACHE.get(code, "") or "").strip()
            if cached_name and cached_name != code:
                return cached_name
            for file_path in (TRADE_LOG_FILE, AI_DECISIONS_FILE):
                if not os.path.exists(file_path):
                    continue
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    for line in reversed(lines[-200:]):
                        try:
                            record = json.loads(line.strip())
                        except (json.JSONDecodeError, ValueError):
                            continue
                        if str(record.get('code', '')).zfill(6) != code:
                            continue
                        candidate = str(record.get('name') or "").strip()
                        if candidate and candidate != code:
                            return candidate
                except Exception:
                    continue
            return code

        def _get_stock_price(code):
            """三级回退获取股价：缓存 → 共享行情 → 个股信息接口"""
            code = str(code).zfill(6)
            name = _fallback_stock_name(code)
            # 1. 缓存
            with cache_lock:
                cached = DASHBOARD_CACHE.get(code, {})
            if cached.get('price', 0) > 0:
                cached_name = str(cached.get('name') or "").strip()
                return cached.get('price', 0), (cached_name or name or code)
            name = str(cached.get('name') or "").strip() or name
            # 2. 共享行情
            try:
                df = get_shared_market_data()
                if df is not None and not df.empty:
                    row = df[df['代码'] == code]
                    if not row.empty:
                        p = float(row['最新价'].values[0])
                        if p > 0:
                            n = str(row['名称'].values[0]) if '名称' in row.columns else name
                            return p, n
            except Exception:
                pass
            # 3. 个股信息接口（同时取名称+最新价，非交易时间也能用）
            try:
                info_df = ak.stock_individual_info_em(symbol=code)
                if info_df is not None and not info_df.empty:
                    row_name = info_df[info_df['item'] == '股票简称']
                    if not row_name.empty:
                        name = str(row_name['value'].values[0])
                    row_price = info_df[info_df['item'] == '最新']
                    if not row_price.empty:
                        p = float(row_price['value'].values[0])
                        if p > 0:
                            return p, name
            except Exception:
                pass
            return 0, (name or code)

        def _build_holding_summary(holdings, label):
            if not holdings:
                return f"  {label}: 无持仓\n"
            lines = [f"  {label}:"]
            for code, info in holdings.items():
                pnl = 0
                cur_price, stock_name = _get_stock_price(code)
                if cur_price <= 0:
                    cur_price = info.get('buy_price', 0)
                if info.get('buy_price', 0) > 0:
                    pnl = (cur_price - info['buy_price']) * info['volume']
                    pnl_pct = (cur_price / info['buy_price'] - 1) * 100
                    sign = '+' if pnl >= 0 else ''
                    lines.append(f"    {code} {stock_name}: {sign}{pnl:.0f}元 ({sign}{pnl_pct:.1f}%)")
            return '\n'.join(lines) + '\n'

        holding_text = _build_holding_summary(real_holdings, "实盘") + _build_holding_summary(sim_holdings, "模拟盘")

        # 5. 写入报告
        report_file = os.path.join(_DAILY_REVIEW_DIR, f"daily_{date_str.replace('-', '')}.txt")
        real_pnl_sign = '+' if real_pnl >= 0 else ''
        sim_pnl_sign = '+' if sim_pnl >= 0 else ''
        report = f"""═══════════════════════════════════════
📊 每日复盘报告 | {date_str}
═══════════════════════════════════════

【AI 审计概况】
  审计总数: {ai_count}
  买入建议: {ai_buy_count}
  卖出建议: {ai_sell_count}
  高/中置信: {high_conf_count}

【实盘交易】
  买入: {real_buys} 次 | 卖出: {real_sells} 次
  当日已实现盈亏: {real_pnl_sign}{real_pnl:.2f} 元

【模拟盘交易】
  买入: {sim_buys} 次 | 卖出: {sim_sells} 次
  当日已实现盈亏: {sim_pnl_sign}{sim_pnl:.2f} 元

【当前持仓浮盈】
{holding_text}
【交易教训】
{trade_lesson_text}

【错失机会】
{missed_text}

【生成时间】{datetime.now().strftime('%Y-%m-%d %H:%M')}
"""
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)

        _mark_daily_review_done(date_str)
        action_word = "已覆盖生成" if force else "已生成"
        log_terminal("每日复盘", f"✅ {date_str} 复盘{action_word}: {report_file}")
        # 上传到云端
        _sync_daily_review_to_cloud(date_str, report)

    except Exception as e:
        log_terminal("每日复盘", f"❌ {date_str} 生成失败: {e}")


# ==================== CloudBase 云端同步层 ====================
# 本地异步同步器：队列 + 重试，不阻塞主交易链路
logger.info(
    "敏感配置已从环境变量加载: SCT_KEY=%s | TCB_PUBLISHABLE_KEY=%s | SYNC_TOKEN=%s",
    mask_secret(SCT_KEY),
    mask_secret(TCB_PUBLISHABLE_KEY),
    mask_secret(SYNC_TOKEN),
)
logger.info(
    "Web 面板认证配置已加载: WEB_ADMIN_USER=%s | FLASK_SECRET_KEY=%s",
    WEB_ADMIN_USER,
    mask_secret(FLASK_SECRET_KEY),
)

# 同步队列：{record_id: {"collection": str, "data": dict, "retry": int, "replace_key": str?}}
_sync_queue = {}
_sync_queue_lock = threading.Lock()
_sync_queue_replace_index = {}
_sync_last_trade_line = 0  # 追踪已同步的交易日志行数
_sync_last_decision_line = 0  # 追踪已同步的AI决策行数
_sync_last_watch_confirm_line = 0  # 追踪已同步的第三层观察日志行数
_sync_last_execution_gate_line = 0  # 追踪已同步的第四层闸门日志行数
_LOCAL_SYNC_STATE_FILE = os.path.join(DATA_DIR, "cloud_sync_state.json")
_local_sync_markers = {"daily_reviews": {}, "strategy_audits": {}}
_local_sync_markers_loaded = False


def _normalize_cloud_date(date_str):
    """统一云端日期键：YYYYMMDD / YYYY-MM-DD -> YYYY-MM-DD"""
    s = str(date_str or "").strip()
    if not s:
        return ""
    if re.fullmatch(r"\d{8}", s):
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _build_snapshot_key(snapshot_type, account, code=""):
    return f"{snapshot_type}|{account}|{code or ''}"


def _new_sync_record_id(prefix):
    return f"{prefix}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"


def _cloud_sync_headers():
    return sync_mod.cloud_sync_headers(sync_token=SYNC_TOKEN, publishable_key=TCB_PUBLISHABLE_KEY)


def _cloud_sync_payload(action, collection, **extra):
    return sync_mod.cloud_sync_payload(action, collection, sync_token=SYNC_TOKEN, **extra)


def _cloud_sync_add(collection, data, record_id=None, replace_key=None, local_marker=None):
    if record_id is None:
        record_id = f"{collection}_{data.get('time', '')}_{data.get('id', data.get('code', ''))}_{time.time()}"
    return sync_mod.cloud_sync_add(
        sync_queue=_sync_queue,
        sync_queue_lock=_sync_queue_lock,
        sync_queue_replace_index=_sync_queue_replace_index,
        collection=collection,
        data=data,
        record_id=record_id,
        replace_key=replace_key,
        local_marker=local_marker,
    )




async def _cloud_sync_worker():
    return await sync_mod.cloud_sync_worker(
        sync_token=SYNC_TOKEN,
        sync_queue=_sync_queue,
        sync_queue_lock=_sync_queue_lock,
        sync_queue_replace_index=_sync_queue_replace_index,
        local_sync_markers=_local_sync_markers,
        persist_local_sync_markers_fn=_persist_local_sync_markers,
        tcb_function_url=TCB_FUNCTION_URL,
        build_payload_fn=_cloud_sync_payload,
        build_headers_fn=_cloud_sync_headers,
        logger_instance=logger,
        log_terminal_fn=log_terminal,
        alert_fn=lambda *, collection, event, fallback: _push_system_notice(
            title="【同步异常】",
            module=f"CloudBase/{collection}",
            event=event,
            fallback=fallback,
            code=f"sync:{collection}",
        ),
    )


async def _flush_cloud_sync_now(status_info=None, include_heartbeat=True):
    return await sync_mod.flush_cloud_sync_now(
        sync_pending_data_fn=_sync_pending_data,
        sync_heartbeat_fn=_sync_heartbeat,
        cloud_sync_worker_fn=_cloud_sync_worker,
        status_info=status_info,
        include_heartbeat=include_heartbeat,
    )


def _sync_pending_data():
    global _sync_last_trade_line, _sync_last_decision_line
    global _sync_last_watch_confirm_line, _sync_last_execution_gate_line
    next_trade, next_decision, next_watch_confirm, next_execution_gate = sync_mod.sync_pending_data(
        load_local_sync_markers_fn=_load_local_sync_markers,
        trade_log_file=TRADE_LOG_FILE,
        trade_log_lock=_TRADE_LOG_LOCK,
        stable_trade_log_id_fn=_stable_trade_log_id,
        cloud_sync_add_fn=_cloud_sync_add,
        ai_decisions_file=AI_DECISIONS_FILE,
        ai_decisions_lock=_AI_DECISIONS_LOCK,
        watch_confirm_log_file=WATCH_CONFIRM_LOG_FILE,
        execution_gate_log_file=EXECUTION_GATE_LOG_FILE,
        daily_review_dir=_DAILY_REVIEW_DIR,
        review_dir=REVIEW_DIR,
        normalize_cloud_date_fn=_normalize_cloud_date,
        artifact_sync_token_fn=_artifact_sync_token,
        sync_queue=_sync_queue,
        sync_queue_lock=_sync_queue_lock,
        local_sync_markers=_local_sync_markers,
        sync_last_trade_line=_sync_last_trade_line,
        sync_last_decision_line=_sync_last_decision_line,
        sync_last_watch_confirm_line=_sync_last_watch_confirm_line,
        sync_last_execution_gate_line=_sync_last_execution_gate_line,
        warn_fn=lambda *, source, event, detail="": _push_system_notice(
            title="【同步异常】",
            module=f"LocalSync/{source}",
            event=event if not detail else f"{event} | {detail[:120]}",
            fallback="本地文件已跳过异常记录，请检查对应日志文件",
            code=f"localsync:{source}",
        ),
    )
    _sync_last_trade_line = next_trade
    _sync_last_decision_line = next_decision
    _sync_last_watch_confirm_line = next_watch_confirm
    _sync_last_execution_gate_line = next_execution_gate


def _sync_daily_review_to_cloud(date_str, report_text):
    return sync_mod.sync_daily_review_to_cloud(
        date_str=date_str,
        report_text=report_text,
        daily_review_dir=_DAILY_REVIEW_DIR,
        normalize_cloud_date_fn=_normalize_cloud_date,
        artifact_sync_token_fn=_artifact_sync_token,
        cloud_sync_add_fn=_cloud_sync_add,
    )


def _sync_heartbeat(status_info=None):
    """上报系统心跳 + 当前持仓/余额快照"""
    import platform
    health = get_market_health()
    guard = refresh_system_guard()
    heartbeat_now = datetime.now()
    heartbeat = {
        "timestamp": heartbeat_now.strftime("%Y-%m-%d %H:%M:%S"),
        "hostname": platform.node(),
        "is_trade_time": _is_trade_time_now(),
        "market_source": MARKET_SOURCE,
        "status": "running",
        "market_health_score": health.get("score", 0),
        "market_age_sec": health.get("age_sec", 0),
        "halt_new_buys": guard.get("halt_new_buys", False),
        "guard_reasons": "；".join(guard.get("reasons", []))[:200],
    }
    if status_info:
        heartbeat.update(status_info)
    _cloud_sync_add("system_heartbeat", heartbeat, _new_sync_record_id("heartbeat"))

    # 同步账户快照（模拟盘 + 实盘）
    for acc_type, capital, pos_file, bal_file in [
        ('sim', SIM_TOTAL_CAPITAL, SIM_POSITIONS_FILE, SIM_BALANCE_FILE),
        ('real', TOTAL_CAPITAL, POSITIONS_FILE, REAL_BALANCE_FILE),
    ]:
        snapshot_now = datetime.now()
        snapshot_upload_time = snapshot_now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_nonce = snapshot_now.strftime("%Y%m%d%H%M%S%f")
        position_count = 0
        position_market_value = 0.0
        cash = 0.0
        total_capital = capital
        # 持仓快照
        if os.path.exists(pos_file):
            try:
                with open(pos_file, 'r', encoding='utf-8') as f:
                    positions = json.load(f)
                position_count = len(positions)
                for code, info in positions.items():
                    buy_price = info.get("buy_price", 0)
                    volume = info.get("volume", 0)
                    # 获取现价+名称，优先复用本地行情缓存，避免名称退化成代码
                    cached = DASHBOARD_CACHE.get(code, {})
                    stock_name = str(cached.get("name") or "").strip() or _STOCK_NAME_CACHE.get(code, code)
                    try:
                        cur_price = float(cached.get("price", 0) or 0)
                    except Exception:
                        cur_price = 0
                    try:
                        if cur_price <= 0 or stock_name == code:
                            info_df = ak.stock_individual_info_em(symbol=code)
                            if info_df is not None and not info_df.empty:
                                row_p = info_df[info_df['item'] == '最新']
                                if not row_p.empty:
                                    cur_price = float(row_p['value'].values[0])
                                row_n = info_df[info_df['item'] == '股票简称']
                                if not row_n.empty:
                                    stock_name = str(row_n['value'].values[0])
                    except Exception:
                        pass
                    if stock_name == code:
                        stock_name = _STOCK_NAME_CACHE.get(code, stock_name)
                    if cur_price <= 0:
                        cur_price = buy_price
                    pnl = (cur_price - buy_price) * volume
                    pnl_pct = ((cur_price / buy_price) - 1) * 100 if buy_price > 0 else 0
                    position_market_value += round(cur_price * volume, 2)
                    snapshot_key = _build_snapshot_key("position", acc_type, code)
                    _cloud_sync_add("account_snapshots", {
                        "snapshot_key": snapshot_key,
                        "snapshot_type": "position",
                        "account": acc_type,
                        "code": code,
                        "name": stock_name,
                        "buy_price": buy_price,
                        "current_price": round(cur_price, 2),
                        "volume": volume,
                        "cost_amount": round(buy_price * volume, 2),
                        "pnl": round(pnl, 2),
                        "pnl_pct": round(pnl_pct, 2),
                        "timestamp": info.get("timestamp", ""),
                        "upload_time": snapshot_upload_time
                    }, f"pos_{acc_type}_{code}_{snapshot_nonce}",
                       replace_key=f"account_snapshots:{snapshot_key}")
            except Exception:
                pass
        # 余额快照
        if os.path.exists(bal_file):
            try:
                with open(bal_file, 'r', encoding='utf-8') as f:
                    balance = json.load(f)
                cash = balance.get("cash", 0)
                total_capital = balance.get("initial_capital", capital)
                total_assets = cash + position_market_value
                account_pnl = round(total_assets - total_capital, 2)
                account_pnl_pct = round((account_pnl / total_capital) * 100, 2) if total_capital > 0 else 0
                balance_snapshot_key = _build_snapshot_key("balance", acc_type, "_balance_")
                _cloud_sync_add("account_snapshots", {
                    "snapshot_key": balance_snapshot_key,
                    "snapshot_type": "balance",
                    "account": acc_type,
                    "code": "_balance_",
                    "cash": cash,
                    "initial_capital": total_capital,
                    "total_capital": capital,
                    "position_market_value": round(position_market_value, 2),
                    "total_assets": round(total_assets, 2),
                    "pnl": account_pnl,
                    "pnl_pct": account_pnl_pct,
                    "last_update": balance.get("last_update", ""),
                    "upload_time": snapshot_upload_time
                }, f"bal_{acc_type}_{snapshot_nonce}",
                   replace_key=f"account_snapshots:{balance_snapshot_key}")
                summary_snapshot_key = _build_snapshot_key("position_summary", acc_type, "_summary_")
                _cloud_sync_add("account_snapshots", {
                    "snapshot_key": summary_snapshot_key,
                    "snapshot_type": "position_summary",
                    "account": acc_type,
                    "code": "_summary_",
                    "stock_count": position_count,
                    "cash": cash,
                    "total_capital": total_capital,
                    "total_assets": round(total_assets, 2),
                    "position_market_value": round(position_market_value, 2),
                    "pnl": account_pnl,
                    "pnl_pct": account_pnl_pct,
                    "cash_ratio": round(cash / (cash + position_market_value) * 100, 2) if (cash + position_market_value) > 0 else 0,
                    "upload_time": snapshot_upload_time
                }, f"summary_{acc_type}_{snapshot_nonce}",
                   replace_key=f"account_snapshots:{summary_snapshot_key}")
            except Exception:
                pass


async def _poll_remote_commands():
    """轮询 CloudBase trade_commands 集合，执行待处理的远程买卖命令"""
    if not SYNC_TOKEN:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                TCB_FUNCTION_URL,
                json=_cloud_sync_payload("query_pending", "trade_commands", limit=10),
                headers=_cloud_sync_headers()
            )
            if resp.status_code != 200:
                return
            result = resp.json()
            if result.get("code") != 0:
                return
            commands = result.get("data", [])
            if not commands:
                return

            for cmd in commands:
                cmd_id = cmd.get("_id", "")
                cmd_type = cmd.get("type", "")
                code = cmd.get("code", "")
                price = cmd.get("price", "")
                volume = cmd.get("volume", "")
                acc_type = cmd.get("acc_type", "sim")
                signal_id = cmd.get("signal_id") or cmd.get("source_signal_id") or ""
                idem_key = cmd.get("idempotency_key") or build_idempotency_key(
                    cmd_type, acc_type, code, price, volume, "remote_command"
                )

                claimed = await _claim_remote_command(client, cmd_id)
                if not claimed:
                    continue

                # 价格偏离校验（±2%）
                try:
                    cur_price = 0.0
                    df_spot = get_shared_market_data()
                    with cache_lock:
                        cached = DASHBOARD_CACHE.get(code, {})
                    if cached.get('price', 0) > 0:
                        cur_price = cached['price']
                    if df_spot is not None and not df_spot.empty:
                        row = df_spot[df_spot['代码'] == code]
                        if not row.empty:
                            cur_price = float(row['最新价'].values[0])
                    if cur_price > 0 and price:
                        deviation = abs(float(price) - cur_price) / cur_price
                        if deviation > 0.02:
                            await _update_command_status(client, cmd_id, "rejected", f"价格偏离现价 {deviation:.1%}，超过2%限制")
                            log_terminal("远程命令", f"❌ 拒绝 {cmd_type} {code}：价格偏离 {deviation:.1%}")
                            continue
                except Exception:
                    pass  # 偏离校验失败不阻止执行

                success, msg = False, ""
                if cmd_type == "buy":
                    success, msg = await asyncio.to_thread(
                        _execute_buy, code, price, volume, acc_type,
                        "remote_command", cmd_id, signal_id, idem_key, "remote_command"
                    )
                elif cmd_type == "sell":
                    success, msg = await asyncio.to_thread(
                        _execute_sell, code, volume, price, acc_type,
                        "remote_command", cmd_id, signal_id, idem_key, "remote_command"
                    )
                else:
                    msg = f"未知命令类型: {cmd_type}"

                status = "done" if success else "rejected"
                await _update_command_status(client, cmd_id, status, msg)
                log_terminal("远程命令", f"{'✅' if success else '❌'} {cmd_type} {code} → {status}: {msg}")
                # 立即同步交易记录到云端（不等5分钟心跳）
                if success:
                    try:
                        await _flush_cloud_sync_now({
                            "event": f"remote_{cmd_type}",
                            "last_command": cmd_type,
                            "last_code": code,
                            "last_account": acc_type,
                        })
                    except Exception:
                        pass

    except Exception as e:
        logger.warning(f"远程命令轮询异常: {e}")
        title, content = push_templates.build_system_error_message(
            module="远程命令",
            event=str(e),
            fallback="稍后重试",
        )
        push_decision(title, content, code="sys:remote_poll")


async def _claim_remote_command(client, doc_id):
    return await sync_mod.claim_remote_command(
        client=client,
        doc_id=doc_id,
        tcb_function_url=TCB_FUNCTION_URL,
        build_payload_fn=_cloud_sync_payload,
        build_headers_fn=_cloud_sync_headers,
    )


async def _update_command_status(client, doc_id, status, result_msg):
    return await sync_mod.update_command_status(
        client=client,
        doc_id=doc_id,
        status=status,
        result_msg=result_msg,
        tcb_function_url=TCB_FUNCTION_URL,
        build_payload_fn=_cloud_sync_payload,
        build_headers_fn=_cloud_sync_headers,
    )


# --- 全局行情数据区 ---
# 全市场行情缓存（主数据）
GLOBAL_MARKET_DATA = None
# 当前行情数据源
MARKET_SOURCE = "None"
# 上次缓存更新时间
LAST_CACHE_TIME = 0

# 行情连续失败计数器（用于健康检查）
consecutive_data_failures = 0
_START_TIME = time.time()

async def get_daily_kline(full_code, code):
    """行情模块包装：带缓存的日线获取。"""
    return await market_data.get_daily_kline(full_code, code)


async def get_30m_kline(full_code, code, df_5m_backup=None):
    """行情模块包装：带缓存的30分钟K线获取。"""
    return await market_data.get_30m_kline(
        full_code,
        code,
        df_5m_backup=df_5m_backup,
        log_terminal=log_terminal,
        native_exception=_NativeException,
    )


def check_30m_filter(df_30m):
    """30分钟中周期过滤器 —— 排雷器，不生成独立买卖信号。
    仅在新仓（hunter模式）中使用，不作用于 keeper 持仓管理。

    返回: (grade, reason)
        grade: 'allow' | 'watch' | 'reject'
        reason: 简短中文说明
    """
    if df_30m is None or len(df_30m) < 5:
        return 'allow', '30分钟数据不足，跳过过滤'

    try:
        df_30m.columns = [c.lower() for c in df_30m.columns]
        col_map = {'day': 'datetime', 'date': 'datetime', 'open': 'open', 'high': 'high',
                    'low': 'low', 'close': 'close', 'volume': 'volume'}
        df_30m = df_30m.rename(columns=col_map)
        for c in ['open', 'high', 'low', 'close', 'volume']:
            if c in df_30m.columns:
                df_30m[c] = pd.to_numeric(df_30m[c], errors='coerce')
        df_30m = df_30m.dropna(subset=['close'])
    except Exception:
        return 'allow', '30分钟数据格式异常，跳过过滤'

    if len(df_30m) < 5:
        return 'allow', '30分钟有效K线不足5根，跳过过滤'

    closes = df_30m['close'].values
    lows = df_30m['low'].values if 'low' in df_30m.columns else closes

    # ── 检查项1：最近2~3根K线是否持续创新低（下跌中继） ──
    recent = closes[-3:] if len(closes) >= 3 else closes
    making_new_lows = all(recent[i] < lows[-(len(recent)-i) if i < len(recent)-1 else -1]
                          for i in range(len(recent)-1)) if len(recent) >= 2 else False
    # 简化判断：最近2根K线的低点是否都在创新低
    if len(lows) >= 2:
        making_new_lows = lows[-1] < lows[-2] and closes[-1] < closes[-2]
    else:
        making_new_lows = False

    # 连续3根收盘价是否递减
    consecutive_decline = False
    if len(closes) >= 3:
        consecutive_decline = (closes[-1] < closes[-2] < closes[-3])

    # ── 检查项2：30分钟RSI/J值（可选增强，仅在有足够数据时计算） ──
    rsi_30m = None
    j_30m = None
    try:
        if len(df_30m) >= 27:
            kd_30m = df_30m.ta.kdj()
            if kd_30m is not None and not kd_30m.empty and 'J_9_3' in kd_30m.columns:
                j_30m = float(kd_30m['J_9_3'].iloc[-1])
        if len(df_30m) >= 15:
            rsi_30m_s = df_30m.ta.rsi(length=14)
            if rsi_30m_s is not None and not rsi_30m_s.empty:
                rsi_30m = float(rsi_30m_s.iloc[-1])
    except Exception:
        pass

    # ── 检查项3：价格是否重新站回短均线/中轨附近（下跌减速信号） ──
    ma5_30m = None
    try:
        if len(df_30m) >= 5:
            ma5_30m = float(df_30m['close'].rolling(5).mean().iloc[-1])
    except Exception:
        pass

    price_near_ma = False
    if ma5_30m is not None and ma5_30m > 0:
        price_30m = float(closes[-1])
        # 价格在MA5的±1.5%范围内，视为"站回短均线附近"
        price_near_ma = abs(price_30m - ma5_30m) / ma5_30m < 0.015

    # ── 综合判定 ──

    # REJECT: 30分钟明显下跌中继
    # 条件：连续下跌 + 创新低，且没有减速信号
    if (consecutive_decline or making_new_lows) and not price_near_ma:
        tag = []
        if consecutive_decline:
            tag.append("连3根收跌")
        if making_new_lows:
            tag.append("创新低")
        if j_30m is not None and j_30m < 20:
            tag.append(f"J={j_30m:.0f}")
        if rsi_30m is not None and rsi_30m < 30:
            tag.append(f"RSI={rsi_30m:.0f}")
        reason = "30分钟" + "+".join(tag) + "，下跌中继"
        return 'reject', reason

    # REJECT: 30分钟RSI/J值极端超卖且仍在恶化（创新低）
    if (j_30m is not None and j_30m < 15 and making_new_lows):
        return 'reject', f"30分钟J={j_30m:.1f}极度超卖且仍在恶化"

    # WATCH: 30分钟不够理想但不至于reject
    # 条件：有下跌迹象但已出现减速信号，或指标处于中间地带
    if making_new_lows and price_near_ma:
        return 'watch', "30分钟仍在创新低但开始靠近均线(减速信号)"
    if j_30m is not None and 15 <= j_30m < 30 and not price_near_ma:
        return 'watch', f"30分钟J={j_30m:.1f}偏弱"
    if rsi_30m is not None and 25 <= rsi_30m < 35 and making_new_lows:
        return 'watch', f"30分钟RSI={rsi_30m:.1f}偏弱且仍在下跌"

    # ALLOW: 30分钟结构不差
    if price_near_ma:
        return 'allow', "30分钟价格回靠均线，下跌减速"
    if not consecutive_decline and not making_new_lows:
        return 'allow', "30分钟未持续创新低，结构尚可"
    if j_30m is not None and j_30m >= 30:
        return 'allow', f"30分钟J={j_30m:.1f}已脱离超卖区"

    # 默认 allow（安全降级）
    return 'allow', "30分钟无明显恶化信号"


# 标记本次运行是否已完成首次网络同步
_first_sync_done = False


def _is_trade_time_now():
    """判断当前是否在活跃审计窗口内（排除周末、午休、无效时段）
    窗口① 09:20-11:35  集合竞价预热 + 早盘 + 5分钟缓冲
    窗口② 12:30-15:05  午休分析上午 + 下午盘 + 收盘缓冲
    其余时间完全待机
    """
    now = datetime.now()
    if now.weekday() >= 5:
        return False
    t = now.hour * 100 + now.minute  # 例如 9:25 → 925
    return (920 <= t <= 1135) or (1230 <= t <= 1505)


def _do_force_sync():
    """执行一次强制网络同步，更新内存缓存 + 写盘。成功返回 True"""
    global GLOBAL_MARKET_DATA, LAST_CACHE_TIME, _first_sync_done, MARKET_SOURCE, consecutive_data_failures
    try:
        data, source = fetch_robust_spot_data(STOCKS)
        if is_market_data_valid(data, strict=False):
            with _market_data_lock:
                GLOBAL_MARKET_DATA = data
            MARKET_SOURCE = source
            LAST_CACHE_TIME = time.time()
            consecutive_data_failures = 0
            _first_sync_done = True
            try:
                data.to_pickle(MARKET_CACHE_FILE)
            except Exception:
                pass
            log_terminal("网络同步", f"✅ 首次同步完成：{source} 获取 {len(data)} 条")
            return True
        else:
            log_terminal("同步失败", "网络返回数据格式非法或完全为空")
            return False
    except _NativeException as e:
        log_terminal("网络异常", f"同步过程中发生错误：{e}")
        return False


def is_market_data_valid(df, strict=False):
    return market_data.is_market_data_valid(df, strict=strict)


# ====================== 【新增】统一持仓查询接口 ======================
def get_holdings_info(code):
    """
    统一持仓查询接口：同时检查实盘和模拟盘（使用缓存，避免高频磁盘 I/O）
    返回：(是否持仓，持仓信息，账户类型)
    """
    real_holdings, sim_holdings = get_cached_holdings()

    if code in real_holdings:
        log_terminal("持仓识别", f"✅ {code} 在实盘中找到")
        return True, real_holdings[code], 'real'
    if code in sim_holdings:
        log_terminal("持仓识别", f"✅ {code} 在模拟盘中找到")
        return True, sim_holdings[code], 'sim'

    log_terminal("持仓识别", f"😶 {code} 空仓")
    return False, None, None


def get_audit_universe():
    """审计池 = 猎人模式监控池 + 当前实盘/模拟盘持仓（供管家模式使用）"""
    seen = set()
    universe = []

    def _add(code):
        code = str(code).zfill(6)
        if code and code not in seen:
            seen.add(code)
            universe.append(code)

    for code in STOCKS:
        _add(code)

    real_holdings, sim_holdings = get_cached_holdings()
    for code in real_holdings.keys():
        _add(code)
    for code in sim_holdings.keys():
        _add(code)

    return universe


def _load_overlay_market_base():
    """为重点票补价提供底板：优先当前内存，其次最近磁盘快照。"""
    with _market_data_lock:
        if GLOBAL_MARKET_DATA is not None and is_market_data_valid(GLOBAL_MARKET_DATA, strict=False):
            return GLOBAL_MARKET_DATA.copy()
    if os.path.exists(MARKET_CACHE_FILE):
        try:
            disk_data = pd.read_pickle(MARKET_CACHE_FILE)
            if is_market_data_valid(disk_data, strict=False):
                return disk_data.copy()
        except Exception:
            pass
    return None


# ==============================================================================
# ====================== 行情模块：主文件保留薄包装，实现在 qt_market_data ======================
# ==============================================================================
def normalize_df(df, source):
    return market_data.normalize_df(df, source, logger_instance=logger)


def fetch_sina_spot_subprocess():
    return market_data.fetch_sina_spot_subprocess()


def _to_tencent_symbol(code):
    return market_data._to_tencent_symbol(code)


def fetch_tencent_focus_quotes(codes):
    return market_data.fetch_tencent_focus_quotes(codes, logger_instance=logger)


def merge_market_snapshot(base_df, overlay_df, source_label="Overlay"):
    return market_data.merge_market_snapshot(base_df, overlay_df, source_label, logger_instance=logger)


def fetch_robust_spot_data(monitor_stocks):
    return market_data.fetch_robust_spot_data(
        monitor_stocks,
        sina_spot_mode=SINA_SPOT_MODE,
        load_overlay_market_base=_load_overlay_market_base,
        get_audit_universe=get_audit_universe,
        record_provider_result=record_provider_result,
        log_terminal=log_terminal,
        native_exception=_NativeException,
        logger_instance=logger,
    )


def _get_market_data_snapshot():
    return GLOBAL_MARKET_DATA


def _set_market_data_snapshot(data):
    global GLOBAL_MARKET_DATA
    with _market_data_lock:
        GLOBAL_MARKET_DATA = data


def _mark_disk_market_fallback(data=None):
    global LAST_CACHE_TIME, MARKET_SOURCE
    try:
        LAST_CACHE_TIME = os.path.getmtime(MARKET_CACHE_FILE)
    except Exception:
        LAST_CACHE_TIME = time.time()
    MARKET_SOURCE = "DiskSnapshot"


def get_shared_market_data():
    return market_data.get_shared_market_data(
        current_data_getter=_get_market_data_snapshot,
        current_data_setter=_set_market_data_snapshot,
        data_lock=_market_data_lock,
        do_force_sync=_do_force_sync,
        market_cache_file=MARKET_CACHE_FILE,
        log_terminal=log_terminal,
        native_exception=_NativeException,
        on_disk_fallback=_mark_disk_market_fallback,
    )


# ==========================================
# 2. 系统核心组件
# ==========================================

def log_terminal(stage, msg):
    """专业日志输出（兼容原有所有调用）"""
    icons = {
        "系统": "⚙️", "行情": "📈", "审计": "🔍", "报错": "🚨",
        "交易": "💰", "推送": "📤", "风控": "🛡️", "缓存": "💾",
        "静默": "😶", "完成": "✅", "警告": "⚠️"
    }
    icon = icons.get(stage, "🔔")
    logger.info(f"{icon} 【{stage}】: {msg}")


def push_decision(title, content, code=None, urgent=False):
    """Server 酱推送：交易信号/预警通知（带30分钟统一冷却，urgent=True 跳过冷却）"""
    # 统一冷却：同一只票 30 分钟内不重复推送（止损/止盈等紧急推送不受限）
    if not urgent and code:
        last = _push_cooldown.get(code, 0)
        if time.time() - last < PUSH_COOLDOWN_SECONDS:
            log_terminal("推送冷却", f"⏳ {code} 距上次推送不足30分钟，跳过")
            return False

    # 提取摘要用于手机通知栏（加粗置顶）
    summary_parts = []
    dm = re.search(r'决策[：:]\s*(.+)', content)
    if dm:
        summary_parts.append(f"决策: {dm.group(1).strip()[:20]}")
    cm = re.search(r'置信度[：:]\s*(高|中|低)', content)
    if cm:
        summary_parts.append(f"置信: {cm.group(1)}")
    sm = re.search(r'止损位[：:]\s*([^\n]+)', content)
    if sm:
        summary_parts.append(f"止损: {sm.group(1).strip()}")
    tm = re.search(r'止盈位[：:]\s*([^\n]+)', content)
    if tm:
        summary_parts.append(f"止盈: {tm.group(1).strip()}")
    summary = " | ".join(summary_parts) if summary_parts else title

    # 安全检查：未设置密钥则跳过推送
    if not SCT_KEY:
        log_terminal("推送", "⚠️ 未设置SCT_KEY环境变量，跳过Server酱推送")
        return

    url = f"https://sctapi.ftqq.com/{SCT_KEY}.send"
    desp = f"**{summary}**\n\n---\n\n{content}"
    session = requests.Session()
    session.trust_env = False
    last_error = ""
    for attempt in range(1, PUSH_RETRY_MAX_ATTEMPTS + 1):
        try:
            resp = session.post(url, data={"title": title, "desp": desp}, timeout=8)
            if resp.status_code == 200:
                if not urgent and code:
                    _push_cooldown[code] = time.time()
                log_terminal("推送", "消息推送成功")
                return True
            last_error = f"HTTP {resp.status_code}"
            should_retry = attempt < PUSH_RETRY_MAX_ATTEMPTS and (resp.status_code >= 500 or resp.status_code == 429)
            if should_retry:
                log_terminal("推送重试", f"第{attempt}/{PUSH_RETRY_MAX_ATTEMPTS}次失败({last_error})，准备重试")
                time.sleep(PUSH_RETRY_BACKOFF_SECONDS * attempt)
                continue
            break
        except _NativeException as e:
            last_error = str(e)
            if attempt < PUSH_RETRY_MAX_ATTEMPTS:
                log_terminal("推送重试", f"第{attempt}/{PUSH_RETRY_MAX_ATTEMPTS}次连接失败，准备重试：{e}")
                time.sleep(PUSH_RETRY_BACKOFF_SECONDS * attempt)
                continue
            log_terminal("推送异常", f"无法连接到 Server 酱服务器：{e}")
            return False
    if last_error:
        log_terminal("推送异常", f"Server 酱推送失败：{last_error}")
    return False


def _push_system_notice(*, title="【系统异常】", module, event, fallback, code, urgent=False):
    notice_title, notice_content = push_templates.build_system_error_message(
        module=module,
        event=event,
        fallback=fallback,
        title=title,
    )
    return push_decision(notice_title, notice_content, code=code, urgent=urgent)


def _push_ai_notice(stage, code, event, fallback):
    return _push_system_notice(
        title="【AI异常】",
        module=f"AI/{stage}",
        event=event,
        fallback=fallback,
        code=f"ai:{stage}:{str(code or '').zfill(6) or stage}",
    )


def write_review_log(text):
    """写入审计日志文件：永久记录交易决策（线程安全）"""
    with _log_lock:
        with open(SUMMARY_FILE, "a", encoding='utf-8') as f:
            f.write(f"\n{'-' * 60}\n时间: {datetime.now()}\n{text}\n")


# ==========================================
# 3. 数据层：大盘行情 & 舆情采集
# ==========================================

# ====================== 【增强版】大盘数据轮询 + 失败容错 ======================


def get_market_analysis():
    return market_data.get_market_analysis(
        get_shared_market_data_fn=get_shared_market_data,
        log_terminal=log_terminal,
    )


# ========================================================================

def _clip_log_text(text, limit=48):
    """压缩日志文本，避免多行/超长内容冲散终端输出。"""
    s = re.sub(r'\s+', ' ', str(text or '')).strip()
    if len(s) <= limit:
        return s
    return s[: max(0, limit - 3)] + "..."


def fetch_web_news(code):
    return market_data.fetch_web_news(
        code,
        log_terminal=log_terminal,
        native_exception=_NativeException,
    )

def _refresh_sector_boards():
    """刷新行业板块涨跌数据（30分钟缓存，交易时段外不刷新）"""
    global _SECTOR_BOARD_CACHE, _SECTOR_BOARD_CACHE_TIME
    if time.time() - _SECTOR_BOARD_CACHE_TIME < 1800:
        return
    try:
        boards = ak.stock_board_industry_name_em()
        if boards is not None and not boards.empty:
            _SECTOR_BOARD_CACHE.clear()
            for _, row in boards.iterrows():
                name = str(row.get('板块名称', ''))
                change = row.get('涨跌幅', None)
                if name and change is not None:
                    try:
                        _SECTOR_BOARD_CACHE[name] = float(change)
                    except (ValueError, TypeError):
                        continue
            _SECTOR_BOARD_CACHE_TIME = time.time()
            log_terminal("板块缓存", f"✅ 刷新 {len(_SECTOR_BOARD_CACHE)} 个行业板块涨跌数据")
    except Exception as e:
        log_terminal("板块缓存", f"⚠️ 板块数据刷新失败：{e}")


def check_sector_resonance(code):
    """个股板块共振分析：获取行业板块 + 板块实际涨跌幅计算共振强度
    返回：(板块名称, 共振分数0-100)
    分数含义：越低=板块越超跌，共振反弹潜力越大
      < 20  板块暴跌（跌幅>3%）
      20-35 板块超跌（跌幅1.5%-3%）
      35-65 板块中性
      > 65  板块强势
    """
    _refresh_sector_boards()

    # 1. 获取该股票所属板块（优先走缓存，30分钟过期）
    industry = _STOCK_INDUSTRY_CACHE.get(code)
    if not industry:
        try:
            info = ak.stock_individual_info_em(symbol=code)
            filtered = info[info['item'] == '板块']
            if not filtered.empty:
                industry = str(filtered['value'].values[0])
                _STOCK_INDUSTRY_CACHE[code] = industry
            else:
                return "通用板块", 50.0
        except Exception:
            return "通用板块", 50.0

    # 2. 查找该板块的实际涨跌幅
    sector_change = _SECTOR_BOARD_CACHE.get(industry)
    if sector_change is None:
        # 精确匹配失败，尝试模糊匹配（板块名称可能略有差异）
        for cached_name, cached_change in _SECTOR_BOARD_CACHE.items():
            if industry in cached_name or cached_name in industry:
                sector_change = cached_change
                break
    if sector_change is None:
        return industry, 50.0

    # 3. 计算共振强度分数（0-100）
    # 板块跌幅越大 → 分数越低 → 表示板块超跌共振越强
    # 映射：-5% → 0, 0% → 50, +5% → 100
    resonance_score = max(0.0, min(100.0, 50.0 + sector_change * 10.0))

    return industry, resonance_score


def analyze_order_trap(code):
    """委比陷阱检测：高位涨不动 + 委比极低 → 判定为诱多陷阱"""
    try:
        spot = get_shared_market_data()
        if spot is None or spot.empty:
            return False, 0.0
        row = spot[spot['代码'] == code]
        if not row.empty:
            weibi_series = row.get('委比', 0)
            weibi = float(weibi_series.values[0]) if not weibi_series.empty else 0.0
            change_series = row.get('涨跌幅', 0)
            change = float(change_series.values[0]) if not change_series.empty else 0.0
            is_trap = (change > 1.0 and weibi < -20)
            return is_trap, weibi
        return False, 0.0
    except _NativeException:
        return False, 0.0


# ===================== Flask Web 仪表盘 =====================

# Flask应用实例（全局唯一）
app = Flask(__name__)
app.config.update(
    SECRET_KEY=FLASK_SECRET_KEY,
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)


def is_web_authenticated():
    """当前请求是否已通过 Web 面板登录认证"""
    return session.get("web_authenticated") is True and session.get("web_user") == WEB_ADMIN_USER


def _sanitize_next_url(next_url):
    """限制登录后跳转目标，避免开放重定向"""
    target = (next_url or "").strip()
    if not target or not target.startswith("/") or target.startswith("//"):
        return url_for("dashboard")
    return target


def _login_redirect_target():
    next_url = request.full_path.rstrip("?") if request.query_string else request.path
    return url_for("login", next=_sanitize_next_url(next_url))


def login_required(view_func=None, *, api=False):
    """统一登录保护：页面跳登录，接口返回 401"""
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            if not is_web_authenticated():
                if api:
                    return jsonify({"code": 401, "message": "Unauthorized"}), 401
                return redirect(_login_redirect_target())
            return func(*args, **kwargs)
        return wrapped

    if view_func is None:
        return decorator
    return decorator(view_func)


def _render_login_page(error_msg="", next_url=""):
    safe_next = html.escape(_sanitize_next_url(next_url), quote=True)
    error_html = f'<div style="background:#4a1f1f;color:#ffb3b3;padding:10px 12px;border-radius:8px;margin-bottom:14px;">{html.escape(error_msg)}</div>' if error_msg else ""
    return f'''
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>面板登录</title>
            <style>
                body {{ font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; min-height: 100vh; margin: 0;
                        display: flex; align-items: center; justify-content: center; padding: 20px; }}
                .card {{ width: 100%; max-width: 380px; background: #1e1e1e; border-radius: 16px; padding: 24px; box-shadow: 0 10px 40px rgba(0,0,0,.35); }}
                h2 {{ margin: 0 0 8px 0; text-align: center; color: #28a745; }}
                p {{ margin: 0 0 18px 0; text-align: center; color: #9aa0a6; font-size: 14px; }}
                label {{ display: block; margin-bottom: 6px; color: #b0b0b0; font-size: 13px; }}
                input {{ width: 100%; box-sizing: border-box; margin-bottom: 14px; padding: 12px; border-radius: 10px; border: 1px solid #333;
                        background: #0f0f0f; color: #fff; font-size: 15px; }}
                button {{ width: 100%; border: none; padding: 12px; border-radius: 10px; background: #28a745; color: #fff; font-size: 16px;
                         font-weight: bold; cursor: pointer; }}
            </style>
        </head>
        <body>
            <div class="card">
                <h2>面板登录</h2>
                <p>登录后可访问交易台、看板、持仓和日志页面</p>
                {error_html}
                <form method="post" action="/login">
                    <input type="hidden" name="next" value="{safe_next}">
                    <label for="username">用户名</label>
                    <input id="username" name="username" type="text" autocomplete="username" required>
                    <label for="password">密码</label>
                    <input id="password" name="password" type="password" autocomplete="current-password" required>
                    <button type="submit">登录</button>
                </form>
            </div>
        </body>
    </html>
    '''


@app.route('/login', methods=['GET', 'POST'])
def login():
    """最小登录页：单用户 + Flask Session"""
    next_url = _sanitize_next_url(request.values.get('next', ''))
    if is_web_authenticated():
        return redirect(next_url)

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')
        if username == WEB_ADMIN_USER and password == WEB_ADMIN_PASSWORD:
            session.clear()
            session.permanent = True
            session["web_authenticated"] = True
            session["web_user"] = WEB_ADMIN_USER
            session["login_at"] = int(time.time())
            logger.info("Web 面板登录成功: user=%s", username)
            return redirect(next_url)
        logger.warning("Web 面板登录失败")
        return _render_login_page("用户名或密码错误", next_url)

    return _render_login_page(next_url=next_url)


@app.route('/logout')
def logout():
    """退出登录并清理 session"""
    session.clear()
    return redirect(url_for("login"))


# 根路由：重定向到买入界面
@app.route('/')
def index():
    """主页：已登录进看板，未登录进登录页"""
    return redirect(url_for("dashboard" if is_web_authenticated() else "login"))


# Web路由：买入/操作主界面
def _build_trade_ui(acc_type):
    """构建交易台界面（统一实盘/模拟盘，消除重复代码）"""
    is_sim = (acc_type == 'sim')
    theme = "#9c27b0" if is_sim else "#28a745"
    label = "🧪 模拟盘交易台" if is_sim else "💰 实盘交易台"
    other_label = "💰 切换到实盘" if is_sim else "🧪 切换到模拟盘"
    dash_url = '/sim-dashboard' if is_sim else '/dashboard'

    real_h, sim_h = get_cached_holdings()
    holdings = sim_h if is_sim else real_h

    # 构建持仓卡片
    holdings_html = ""
    if holdings:
        df_spot = get_shared_market_data()
        for code, info in holdings.items():
            name = code
            price = info.get('buy_price', 0)
            buy_price = info.get('buy_price', 0)
            volume = info.get('volume', 0)

            with cache_lock:
                cached = DASHBOARD_CACHE.get(code, {})
            name = cached.get('name', code)
            price = cached.get('price', buy_price)

            if df_spot is not None and not df_spot.empty:
                row = df_spot[df_spot['代码'] == code]
                if not row.empty:
                    price = float(row['最新价'].values[0])
                    name = row['名称'].values[0] if not row['名称'].empty else name

            pnl_pct = ((price - buy_price) / buy_price * 100) if buy_price > 0 else 0
            pnl_color = '#00c851' if pnl_pct >= 0 else '#ff4444'

            holdings_html += f'''
            <div style="background:#2a2a2a; border-radius:10px; padding:12px; margin-bottom:10px; border-left:4px solid {theme};">
                <div style="display:flex; justify-content:space-between;">
                    <span style="font-size:16px; font-weight:bold;">{name}({code})</span>
                    <span style="font-size:14px;">持股 {volume}股</span>
                </div>
                <div style="font-size:13px; color:#aaa; margin-top:4px;">
                    成本:{buy_price:.2f} | 现价:{price:.2f} | 盈亏:<span style="color:{pnl_color};">{'+' if pnl_pct >= 0 else ''}{pnl_pct:.1f}%</span>
                </div>
            </div>
            '''
    else:
        holdings_html = '<div style="text-align:center; color:#666; padding:20px;">当前无持仓</div>'

    # AI 最近决策快照卡片
    ai_records = read_ai_decisions(limit=5)
    ai_card_html = ""
    if ai_records:
        rows = ""
        for d in ai_records:
            code = d.get('code', '?')
            name = d.get('name', code)
            decision = d.get('decision', '?')
            confidence = d.get('confidence', '?')
            conf_color = '#69f0ae' if confidence == '高' else '#ffab40' if confidence == '中' else '#ff5252'
            j_val = d.get('j_val', '?')
            rsi = d.get('rsi', '?')
            sent = d.get('market_sentiment', '?')
            time_str = d.get('time', '?')[-5:]  # 只取 HH:MM
            linked = d.get('_linked', False)
            link_icon = '🔗' if linked else '💬'
            # 决策颜色
            if '买入' in decision or '加仓' in decision:
                dec_color = '#00c851'
            elif '卖出' in decision or '止损' in decision or '减仓' in decision:
                dec_color = '#ff5252'
            elif '换股' in decision:
                dec_color = '#ffab40'
            else:
                dec_color = '#90caf9'
            # 一键操作按钮（根据当前账户类型动态设置）
            action_btn = ''
            if '买入' in decision or '加仓' in decision:
                action_btn = f'<a href="/buy_page?acc_type={acc_type}&code={code}&price={d.get("price","")}&vol={d.get("real_vol",d.get("suggested_vol",""))}" style="color:#00c851;font-size:11px;text-decoration:none;background:#1a2a1a;padding:2px 8px;border-radius:4px;white-space:nowrap;">买入</a>'
            elif '卖出' in decision or '止损' in decision or '减仓' in decision:
                action_btn = f'<a href="/sell_page?acc_type={acc_type}&code={code}" style="color:#ff5252;font-size:11px;text-decoration:none;background:#2a1a1a;padding:2px 8px;border-radius:4px;white-space:nowrap;">卖出</a>'
            rows += f'''
            <tr style="border-bottom:1px solid #333;">
                <td style="padding:8px 6px; color:#888; font-size:12px; white-space:nowrap;">{time_str}</td>
                <td style="padding:8px 6px; font-size:13px;">
                    <b>{name}</b>
                    <span style="color:#666; font-size:11px;">{code}</span>
                </td>
                <td style="padding:8px 6px;">
                    <span style="color:{dec_color}; font-weight:bold; font-size:13px;">{decision}</span>
                    <span style="background:{conf_color}22; color:{conf_color}; font-size:11px; padding:1px 6px; border-radius:4px; margin-left:4px;">{confidence}</span>
                </td>
                <td style="padding:8px 6px; color:#aaa; font-size:12px; white-space:nowrap;">J:{j_val} RSI:{rsi}</td>
                <td style="padding:8px 6px; font-size:12px;">{sent}</td>
                <td style="padding:8px 6px; text-align:center;">{link_icon}</td>
                <td style="padding:8px 4px; text-align:center;">{action_btn}</td>
            </tr>'''
        ai_card_html = f'''
        <div class="card" style="margin-top:20px; padding:12px; overflow-x:auto;">
            <h3 style="margin:0 0 10px 0; font-size:14px;">🤖 AI 最近决策 <span style="color:#666; font-size:12px;">（最近5条）</span></h3>
            <table style="width:100%; border-collapse:collapse; font-size:13px;">
                <tr style="border-bottom:2px solid #444;">
                    <th style="padding:6px; text-align:left; color:#888; font-size:11px;">时间</th>
                    <th style="padding:6px; text-align:left; color:#888; font-size:11px;">标的</th>
                    <th style="padding:6px; text-align:left; color:#888; font-size:11px;">决策</th>
                    <th style="padding:6px; text-align:left; color:#888; font-size:11px;">指标</th>
                    <th style="padding:6px; text-align:left; color:#888; font-size:11px;">大盘</th>
                    <th style="padding:6px; text-align:center; color:#888; font-size:11px;">状态</th>
                    <th style="padding:6px; text-align:center; color:#888; font-size:11px;">操作</th>
                </tr>
                {rows}
            </table>
            <div style="margin-top:8px; color:#555; font-size:11px;">🔗=已关联交易 💬=待采纳</div>
        </div>
        '''

    return f'''
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 15px; }}
                h2 {{ text-align: center; color: {theme}; }}
                .tab-bar {{ display: flex; gap: 10px; margin-bottom: 20px; }}
                .tab {{ flex: 1; padding: 12px; text-align: center; background: #2a2a2a; color: #888; text-decoration: none; border-radius: 8px; }}
                .tab.active {{ background: {theme}; color: white; font-weight: bold; }}
                .card {{ background: #1e1e1e; border-radius: 12px; padding: 15px; margin-bottom: 15px; }}
                .btn {{ display: block; width: 100%; padding: 15px; background: {theme}; color: white; text-align: center;
                        text-decoration: none; border-radius: 8px; margin-bottom: 10px; font-weight: bold; font-size: 18px; border: none; cursor: pointer; }}
                .btn-sell {{ background: #e53935; }}
                .btn-view {{ background: #2196f3; }}
                .back-link {{ display: block; text-align: center; color: #888; text-decoration: none; margin-top: 15px; }}
            </style>
        </head>
        <body>
            <h2>{label}</h2>
            <div style="text-align:right; margin-bottom:12px; color:#888; font-size:12px;">
                已登录 {WEB_ADMIN_USER} | <a href="/logout" style="color:{theme}; text-decoration:none;">退出登录</a>
            </div>

            <div class="tab-bar">
                <a href="/buy_ui" class="tab {'active' if not is_sim else ''}">💰 实盘</a>
                <a href="/sim-buy-ui" class="tab {'active' if is_sim else ''}">🧪 模拟盘</a>
            </div>

            <div class="card">
                <h3 style="margin:0 0 10px 0; font-size:16px;">📋 当前持仓</h3>
                {holdings_html}
            </div>

            <a href="/buy_page?acc_type={acc_type}" class="btn">➕ 买入新股</a>
            <a href="/sell_page?acc_type={acc_type}" class="btn btn-sell">🔻 卖出持仓</a>
            <a href="{dash_url}" class="btn btn-view">📊 查看看板</a>
            <a href="/trade_log" class="btn" style="background:#555;">📋 交易历史</a>

            {ai_card_html}

            <a href="/{'buy_ui' if is_sim else 'sim-buy-ui'}" class="back-link">{other_label}</a>
        </body>
    </html>
    '''


@app.route('/buy_ui')
@login_required
def buy_interface():
    """实盘交易台"""
    return _build_trade_ui('real')


@app.route('/buy_page')
@login_required
def buy_page():
    """买入录入页面 - 暗色风格，带买入价格输入"""
    acc_type = request.args.get('acc_type', 'real')
    acc_label = "🧪 模拟盘" if acc_type == 'sim' else "💰 实盘"
    initial_capital_val = SIM_TOTAL_CAPITAL if acc_type == 'sim' else TOTAL_CAPITAL
    # 支持 AI 决策卡片一键带入参数
    pre_code = request.args.get('code', '')
    pre_price = request.args.get('price', '')
    pre_vol = request.args.get('vol', '')
    
    return f'''
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 15px; }}
                h2 {{ text-align: center; color: #28a745; }}
                form {{ text-align: left; width: 100%; }}
                select, input[type=text], input[type=number] {{ font-size: 18px; width: 100%; margin-bottom: 15px; padding: 12px; background: #2a2a2a; color: #e0e0e0; border: 1px solid #444; border-radius: 8px; box-sizing: border-box; }}
                label {{ display: block; margin-bottom: 5px; color: #aaa; font-size: 14px; }}
                .submit-btn {{ display: block; width: 100%; padding: 15px; background: #28a745; color: white; text-align: center; text-decoration: none; border-radius: 8px; margin-bottom: 15px; font-weight: bold; font-size: 18px; border: none; cursor: pointer; }}
                .back-link {{ display: block; text-align: center; color: #888; text-decoration: none; margin-top: 15px; }}
                .price-hint {{ color: #4fc3f7; font-size: 13px; margin-top: -10px; margin-bottom: 10px; }}
                .max-hint {{ color: #ffb74d; font-size: 13px; margin-top: -10px; margin-bottom: 10px; }}
                .info-card {{ background: #1e1e1e; border: 1px solid #333; border-radius: 8px; padding: 12px; margin-bottom: 15px; font-size: 13px; color: #bbb; }}
                .quick-btns {{ display: flex; gap: 8px; margin-bottom: 10px; }}
                .quick-btns button {{ flex:1; padding:8px; background:#2a2a2a; color:#4fc3f7; border:1px solid #444; border-radius:6px; cursor:pointer; font-size:13px; }}
                .quick-btns button:hover {{ background:#333; }}
            </style>
        </head>
        <body>
            <h2>{acc_label} ➕ 买入录入</h2>
            <div class="info-card" id="infoCard">输入代码后将自动查询现价和可买数量</div>
            <form action="/buy" method="get">
                <input type="hidden" name="acc_type" value="{acc_type}">
                
                <label>股票代码 (6位数字)</label>
                <input type="text" name="code" id="codeInput" placeholder="如: 600519" value="{pre_code}" required>
                <div class="price-hint" id="priceHint"></div>
                
                <label>买入价格 (元)</label>
                <input type="number" step="0.01" name="buy_price" id="priceInput" placeholder="买入成本价" value="{pre_price}" required>
                <div class="max-hint" id="maxHint"></div>
                
                <label>买入股数 (股)</label>
                <div class="quick-btns" id="buyQuickArea" style="display:none;">
                    <button type="button" onclick="document.getElementById('volInput').value=100">100股</button>
                    <button type="button" onclick="fillHalfBuy()">半仓</button>
                    <button type="button" onclick="fillMaxBuy()">最大</button>
                </div>
                <input type="number" name="volume" id="volInput" placeholder="买入股数" value="{pre_vol}" required>
                
                <button type="submit" class="submit-btn">✅ 确认买入</button>
            </form>
            <a href="/{'sim-buy-ui' if acc_type == 'sim' else 'buy_ui'}" class="back-link">⬅️ 返回交易台</a>
            <script>
            var _lastMaxVol = 0;
            function fillHalfBuy() {{ var v = Math.floor(_lastMaxVol / 2 / 100) * 100; if (v < 100) v = 100; document.getElementById('volInput').value = v; }}
            function fillMaxBuy() {{ document.getElementById('volInput').value = _lastMaxVol; }}
            var _debounceTimer = null;
            document.getElementById('codeInput').addEventListener('input', function() {{
                var code = this.value.trim();
                if (code.length < 6) return;
                clearTimeout(_debounceTimer);
                _debounceTimer = setTimeout(function() {{
                    code = code.padStart(6, '0');
                    fetch('/api/stock_info?code=' + code + '&acc_type={acc_type}')
                    .then(r => r.json())
                    .then(data => {{
                        if (data.error) {{
                            document.getElementById('infoCard').textContent = data.error;
                            return;
                        }}
                        document.getElementById('infoCard').innerHTML =
                            '<b>' + (data.name || code) + '</b> | 现价: <b>' + data.price + '</b> | 可用现金: ' + data.cash +
                            ' | 当前总仓位: ' + data.total_ratio_now + '%' + (data.single_ratio_now > 0 ? ' | 当前单票: ' + data.single_ratio_now + '%' : '');
                        document.getElementById('priceInput').value = data.price;
                        if (data.max_vol > 0) {{
                            var max100 = Math.floor(data.max_vol / 100) * 100;
                            _lastMaxVol = max100;
                            var totalAfter = (data.total_ratio_now + max100 * data.price / {initial_capital_val}).toFixed(1);
                            var singleAfter = (parseFloat(data.single_ratio_now) + max100 * data.price / {initial_capital_val} * 100).toFixed(1);
                            document.getElementById('maxHint').innerHTML =
                                '最大可买: <b>' + max100 + '股</b> (' + (max100*data.price).toFixed(0) + '元)' +
                                ' | 买入后总仓位: ' + totalAfter + '% | 单票: ' + singleAfter + '%';
                            if (!document.getElementById('volInput').value) document.getElementById('volInput').value = max100;
                            document.getElementById('buyQuickArea').style.display = 'flex';
                        }}
                    }})
                    .catch(() => {{ document.getElementById('infoCard').textContent = '查询失败'; }});
                }}, 500);
            }});
            </script>
        </body>
    </html>
    '''


# Web路由：交易数据录入页面 (兼容旧入口)
@app.route('/input_page')
@login_required
def input_page():
    """兼容旧入口 - 跳转到新的买入页面"""
    return '''
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body { font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 15px; text-align: center; }
                h2 { color: #28a745; }
                .btn { display: block; width: 80%; margin: 20px auto; padding: 20px; background: #2a2a2a; color: #e0e0e0; text-decoration: none; border-radius: 10px; font-size: 18px; }
                .btn:hover { background: #333; }
            </style>
        </head>
        <body>
            <h2>📋 选择账户类型</h2>
            <a href="/buy_page?acc_type=real" class="btn">💰 实盘买入</a>
            <a href="/buy_page?acc_type=sim" class="btn">🧪 模拟盘买入</a>
        </body>
    </html>
    '''


def _execute_buy(code, buy_price, volume, acc_type, source="core", command_id=None, signal_id=None,
                 idempotency_key=None, strategy_batch=None, mode=None,
                 watch_id=None, gate_decision_source=None):
    # source: 动作来源，如 web_panel / remote_command / watch_confirm
    # strategy_batch: 幂等批次或策略分组键，用于把同一轮动作归到同一组
    # gate_decision_source: 第四层执行闸门的结论来源，如 baseline / baseline+ai / ai_veto
    return execution_mod.execute_buy(
        code,
        buy_price,
        volume,
        acc_type,
        precheck_buy_order_fn=precheck_buy_order,
        read_account_state_fn=read_account_state,
        commit_account_ledger_fn=commit_account_ledger,
        cache_reset_fn=_reset_holdings_cache,
        get_cached_name_fn=_get_dashboard_cached_name,
        append_trade_log_fn=append_trade_log,
        link_buy_to_decision_fn=link_buy_to_decision,
        mark_decision_linked_fn=mark_decision_linked,
        sync_heartbeat_fn=_sync_heartbeat,
        log_terminal_fn=log_terminal,
        logger_instance=logger,
        native_exception_cls=_NativeException,
        push_fn=push_decision,
        source=source,
        command_id=command_id,
        signal_id=signal_id,
        idempotency_key=idempotency_key,
        strategy_batch=strategy_batch,
        mode=mode,
        watch_id=watch_id,
        gate_decision_source=gate_decision_source,
    )


def _execute_sell(code, volume, sell_price_input, acc_type, source="core", command_id=None, signal_id=None,
                  idempotency_key=None, strategy_batch=None, exit_reason_tag=None):
    # source: 动作来源，如 web_panel / remote_command / risk_exit
    # strategy_batch: 幂等批次或策略分组键，不等于信号ID
    # exit_reason_tag: 标准化退出标签，如 stop_loss / dynamic_stop / tp1 / manual_exit
    return execution_mod.execute_sell(
        code,
        volume,
        sell_price_input,
        acc_type,
        read_account_state_fn=read_account_state,
        commit_account_ledger_fn=commit_account_ledger,
        cache_reset_fn=_reset_holdings_cache,
        get_cached_name_fn=_get_dashboard_cached_name,
        get_shared_market_data_fn=get_shared_market_data,
        get_cached_price_fn=_get_dashboard_cached_price,
        append_trade_log_fn=append_trade_log,
        link_sell_to_decision_fn=link_sell_to_decision,
        mark_decision_linked_fn=mark_decision_linked,
        resolve_risk_events_fn=resolve_risk_events,
        sync_heartbeat_fn=_sync_heartbeat,
        log_terminal_fn=log_terminal,
        logger_instance=logger,
        push_fn=push_decision,
        source=source,
        command_id=command_id,
        signal_id=signal_id,
        idempotency_key=idempotency_key,
        strategy_batch=strategy_batch,
        exit_reason_tag=exit_reason_tag,
    )


# Web 路由：买入处理
@app.route('/buy')
@login_required(api=True)
def buy_stock():
    code = request.args.get('code', '')
    buy_price = request.args.get('buy_price', '')
    volume = request.args.get('volume', '')
    acc_type = request.args.get('acc_type', 'real')
    idem = request.args.get('idempotency_key', '') or build_idempotency_key(
        "buy", acc_type, code, buy_price, volume, f"web_panel_{datetime.now().strftime('%Y%m%d%H%M')}"
    )
    success, msg = _execute_buy(
        code, buy_price, volume, acc_type,
        source="web_panel", idempotency_key=idem
    )
    disk_name = "🧪 模拟盘" if acc_type == 'sim' else "💰 实盘"
    back_url = '/sim-buy-ui' if acc_type == 'sim' else '/buy_ui'
    dashboard_url = '/sim-dashboard' if acc_type == 'sim' else '/dashboard'
    title = f"{disk_name} 买入成功" if success else f"{disk_name} 买入失败"
    color = "#28a745" if success else "#ff5252"
    
    return f'''
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 50px; text-align: center; }}
                h1 {{ color: {color}; }}
                .info {{ background: #2a2a2a; padding: 20px; border-radius: 10px; margin: 20px auto; max-width: 560px; white-space: pre-wrap; }}
                .btn {{ display: inline-block; padding: 12px 30px; background: #28a745; color: white; text-decoration: none; border-radius: 8px; margin: 10px; }}
            </style>
        </head>
        <body>
            <h1>{title}</h1>
            <div class="info">
                {html.escape(msg)}
            </div>
            <a href="{dashboard_url}" class="btn">📊 查看看板</a>
            <a href="{back_url}" class="btn">⬅️ 继续交易</a>
        </body>
    </html>
    <script>setTimeout(function(){{ window.location.href = '{dashboard_url if success else back_url}'; }}, 1500);</script>
    '''


@app.route('/sim-buy-ui')
@login_required
def sim_buy_ui():
    """模拟盘交易台"""
    return _build_trade_ui('sim')


# Web 路由：卖出表单页面（根据 acc_type 动态展示持仓列表）
@app.route('/sell_page')
@login_required
def sell_page():
    acc_type = request.args.get('acc_type', 'real')
    target_file = SIM_POSITIONS_FILE if acc_type == 'sim' else POSITIONS_FILE
    acc_label = "🧪 模拟盘" if acc_type == 'sim' else "💰 实盘"
    acc_color = "#9c27b0" if acc_type == 'sim' else "#28a745"

    # 读取当前持仓（使用缓存）
    if acc_type == 'sim':
        _, holdings = get_cached_holdings()
    else:
        holdings, _ = get_cached_holdings()
    if not holdings:
        return f"<h1 style='text-align:center;padding-top:100px;background:#121212;color:#e0e0e0;'>{acc_label} 当前无持仓</h1><p style='text-align:center;'><a href='/{'sim-buy-ui' if acc_type == 'sim' else 'buy_ui'}'>返回交易台</a></p>"

    # 获取实时价格构建持仓选项
    df_spot = get_shared_market_data()
    options_html = ""
    price_hints = {}  # 用于JS自动填充当前价格
    
    for code, info in holdings.items():
        name = code
        # 尝试从行情或缓存获取名称和现价
        with cache_lock:
            cached = DASHBOARD_CACHE.get(code, {})
        name = cached.get('name', code)
        price = cached.get('price', info.get('buy_price', 0))
        if df_spot is not None and not df_spot.empty:
            row = df_spot[df_spot['代码'] == code]
            if not row.empty:
                price = float(row['最新价'].values[0])
                name = row['名称'].values[0] if not row['名称'].empty else name
        buy_price = info.get('buy_price', 0)
        volume = info.get('volume', 0)
        pnl_pct = ((price - buy_price) / buy_price * 100) if buy_price > 0 else 0
        pnl_color = '#00c851' if pnl_pct >= 0 else '#ff4444'
        
        price_hints[code] = price
        
        options_html += f'''
        <div style="background:#2a2a2a; border-radius:10px; padding:12px; margin-bottom:10px; border-left:4px solid #e53935;">
            <div style="display:flex; justify-content:space-between;">
                <span style="font-size:16px; font-weight:bold;">{name}({code})</span>
                <span style="font-size:14px;">持股 {volume}股</span>
            </div>
            <div style="font-size:13px; color:#aaa; margin-top:4px;">
                成本:{buy_price:.2f} | 现价:{price:.2f} | 盈亏:<span style="color:{pnl_color};">{'+' if pnl_pct >= 0 else ''}{pnl_pct:.1f}%</span>
            </div>
        </div>
        '''

    back_url = '/sim-buy-ui' if acc_type == 'sim' else '/buy_ui'
    
    # 生成JS数据：价格 + 持仓详情（供快捷按钮使用）
    holdings_json = {}
    for code, info in holdings.items():
        with cache_lock:
            cached_name = DASHBOARD_CACHE.get(code, {}).get('name', code)
            cached_price = DASHBOARD_CACHE.get(code, {}).get('price', info.get('buy_price', 0))
        holdings_json[code] = {
            "vol": info.get('volume', 0),
            "buy_price": info.get('buy_price', 0),
            "price": cached_price,
            "name": cached_name
        }

    price_json = json.dumps({k: v['price'] for k, v in holdings_json.items()})
    holdings_js = json.dumps(holdings_json)

    # 生成下拉选项（避免在f-string中使用walrus operator）
    select_options = []
    for code, info in holdings.items():
        with cache_lock:
            cached_name = DASHBOARD_CACHE.get(code, {}).get('name', code)
        select_options.append(f'<option value="{code}">{cached_name}({code}) 持{info["volume"]}股</option>')
    select_options_html = " ".join(select_options)
    
    return f'''
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 15px; }}
                h2 {{ text-align: center; color: #e53935; }}
                .tab-bar {{ display: flex; gap: 10px; margin-bottom: 20px; }}
                .tab {{ flex: 1; padding: 12px; text-align: center; background: #2a2a2a; color: #888; text-decoration: none; border-radius: 8px; }}
                .tab.active {{ background: #e53935; color: white; font-weight: bold; }}
                .stock-list {{ margin-bottom: 20px; }}
                form {{ text-align: left; width: 100%; }}
                select {{ font-size: 18px; width: 100%; margin-bottom: 15px; padding: 10px; background: #2a2a2a; color: #e0e0e0; border: 1px solid #444; border-radius: 8px; }}
                input[type=text], input[type=number] {{ font-size: 18px; width: 100%; margin-bottom: 15px; padding: 10px; background: #2a2a2a; color: #e0e0e0; border: 1px solid #444; border-radius: 8px; box-sizing: border-box; }}
                label {{ display: block; margin-bottom: 5px; color: #aaa; font-size: 14px; }}
                .submit-btn {{ display: block; width: 100%; padding: 15px; background: #e53935; color: white; text-align: center;
                               text-decoration: none; border-radius: 8px; margin-bottom: 15px; font-weight: bold; font-size: 18px; border: none; cursor: pointer; }}
                .back-link {{ display: block; text-align: center; color: #888; text-decoration: none; margin-top: 15px; }}
                .hint {{ font-size: 13px; color: #888; margin-bottom: 15px; }}
            </style>
            <script>
                var priceData = {price_json};
                var holdingsData = {holdings_js};
                function updatePriceHint() {{
                    var code = document.getElementById('code_select').value;
                    var priceInput = document.getElementById('sell_price');
                    var volInput = document.getElementById('sell_vol');
                    var quickArea = document.getElementById('quick_area');
                    if (code && priceData[code]) {{
                        priceInput.placeholder = '当前价: ' + priceData[code].toFixed(2);
                        var h = holdingsData[code];
                        if (h) {{
                            var pnl = h.price > 0 ? ((h.price - h.buy_price) / h.buy_price * 100).toFixed(1) : '0.0';
                            var pnlColor = pnl >= 0 ? '#00c851' : '#ff4444';
                            var stopPrice = (h.buy_price * 0.975).toFixed(2);
                            quickArea.innerHTML =
                                '<div style="background:#1e1e1e;border:1px solid #333;border-radius:8px;padding:10px;margin-bottom:15px;font-size:13px;">' +
                                '<b>' + h.name + '</b> | 持仓 <b>' + h.vol + '股</b> | 成本 ' + h.buy_price.toFixed(2) +
                                ' | 现价 ' + h.price.toFixed(2) +
                                ' | 盈亏 <span style="color:' + pnlColor + ';">' + (pnl >= 0 ? '+' : '') + pnl + '%</span>' +
                                '</div>' +
                                '<div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap;">' +
                                '<button type="button" onclick="fillQuick(1/3)" style="flex:1;min-width:60px;padding:10px;background:#2a2a2a;color:#4fc3f7;border:1px solid #444;border-radius:6px;cursor:pointer;font-size:13px;">1/3仓</button>' +
                                '<button type="button" onclick="fillQuick(1/2)" style="flex:1;min-width:60px;padding:10px;background:#2a2a2a;color:#4fc3f7;border:1px solid #444;border-radius:6px;cursor:pointer;font-size:13px;">1/2仓</button>' +
                                '<button type="button" onclick="fillQuick(1)" style="flex:1;min-width:60px;padding:10px;background:#2a2a2a;color:#ff7043;border:1px solid #444;border-radius:6px;cursor:pointer;font-size:13px;">全部清仓</button>' +
                                '<button type="button" onclick="fillPrice()" style="flex:1;min-width:60px;padding:10px;background:#2a2a2a;color:#ffb74d;border:1px solid #444;border-radius:6px;cursor:pointer;font-size:13px;">按现价填</button>' +
                                '</div>' +
                                '<div id="targetBtns" style="display:flex;gap:8px;margin-bottom:15px;flex-wrap:wrap;"></div>';
                            if (Math.abs(parseFloat(pnl)) >= 3.5) {{
                                quickArea.innerHTML += '<div style="color:#ffb74d;font-size:12px;margin-bottom:10px;">' + (pnl > 0 ? '当前盈利超过3.5%，注意止盈纪律' : '当前浮亏较深，关注止损位') + '</div>';
                            }}
                        }}
                    }} else {{
                        priceInput.placeholder = '输入卖出价格';
                        quickArea.innerHTML = '';
                    }}
                    // 获取 AI 决策止损/止盈位并生成按钮
                    var targetBtns = document.getElementById('targetBtns');
                    if (code && targetBtns) {{
                        fetch('/api/decision_targets?code=' + code).then(r=>r.json()).then(t => {{
                            if (t.stop_loss || t.tp1 || t.dyn_stop) {{
                                var btns = '';
                                if (t.dyn_stop) btns += '<button type="button" onclick="document.getElementById(\'sell_price\').value=' + t.dyn_stop + '" style="flex:1;min-width:60px;padding:8px;background:#3a1a1a;color:#ff5252;border:2px solid #ff5252;border-radius:6px;cursor:pointer;font-size:11px;font-weight:bold;">保护止损 ' + t.dyn_stop + ' (' + t.dyn_label + ')</button>';
                                else if (t.stop_loss) btns += '<button type="button" onclick="document.getElementById(\'sell_price\').value=' + t.stop_loss + '" style="flex:1;min-width:50px;padding:8px;background:#2a1a1a;color:#ff5252;border:1px solid #444;border-radius:6px;cursor:pointer;font-size:12px;">止损' + t.stop_loss + '</button>';
                                if (t.tp1) btns += '<button type="button" onclick="document.getElementById(\'sell_price\').value=' + t.tp1 + '" style="flex:1;min-width:50px;padding:8px;background:#1a2a1a;color:#69f0ae;border:1px solid #444;border-radius:6px;cursor:pointer;font-size:12px;">止盈1 ' + t.tp1 + '</button>';
                                if (t.tp2) btns += '<button type="button" onclick="document.getElementById(\'sell_price\').value=' + t.tp2 + '" style="flex:1;min-width:50px;padding:8px;background:#1a2a1a;color:#00e676;border:1px solid #444;border-radius:6px;cursor:pointer;font-size:12px;">止盈2 ' + t.tp2 + '</button>';
                                if (t.tp3) btns += '<button type="button" onclick="document.getElementById(\'sell_price\').value=' + t.tp3 + '" style="flex:1;min-width:50px;padding:8px;background:#1a2a1a;color:#76ff03;border:1px solid #444;border-radius:6px;cursor:pointer;font-size:12px;">止盈3 ' + t.tp3 + '</button>';
                                targetBtns.innerHTML = btns;
                            }}
                        }}).catch(function(){{}});
                    }}
                }}
                function fillQuick(ratio) {{
                    var code = document.getElementById('code_select').value;
                    var h = holdingsData[code];
                    if (!h) return;
                    var vol = Math.floor(h.vol * ratio / 100) * 100;
                    if (ratio >= 1) vol = h.vol;
                    document.getElementById('sell_vol').value = vol;
                }}
                function fillPrice() {{
                    var code = document.getElementById('code_select').value;
                    if (code && priceData[code]) {{
                        document.getElementById('sell_price').value = priceData[code].toFixed(2);
                    }}
                }}
            </script>
        </head>
        <body>
            <h2>{acc_label} 🔻 卖出操作</h2>
            
            <!-- Tab 切换栏 -->
            <div class="tab-bar">
                <a href="/sell_page?acc_type=real" class="tab {'active' if acc_type == 'real' else ''}">💰 实盘</a>
                <a href="/sell_page?acc_type=sim" class="tab {'active' if acc_type == 'sim' else ''}">🧪 模拟盘</a>
            </div>
            
            <div class="stock-list">{options_html}</div>
            <form action="/sell" method="get">
                <input type="hidden" name="acc_type" value="{acc_type}">
                
                <label>选择股票</label>
                <select name="code" id="code_select" onchange="updatePriceHint()">
                    <option value="">-- 请选择要卖出的股票 --</option>
                    {select_options_html}
                </select>

                <div id="quick_area"></div>

                <label>卖出价格 (元)</label>
                <input type="number" step="0.01" name="sell_price" id="sell_price" placeholder="输入卖出价格">

                <p class="hint">留空股数或填0 = 全部清仓该股</p>
                <label>卖出股数</label>
                <input type="number" name="volume" id="sell_vol" placeholder="留空=全部清仓">
                
                <button type="submit" class="submit-btn">🔻 确认卖出</button>
            </form>
            <a href="{back_url}" class="back-link">⬅️ 返回交易台</a>
        </body>
    </html>
    '''


# Web 路由：卖出处理（部分卖出 or 全部清仓）
@app.route('/sell')
@login_required(api=True)
def sell_stock():
    code = request.args.get('code', '')
    volume = request.args.get('volume', '')
    sell_price_input = request.args.get('sell_price', '')
    acc_type = request.args.get('acc_type', 'real')
    idem = request.args.get('idempotency_key', '') or build_idempotency_key(
        "sell", acc_type, code, sell_price_input, volume, f"web_panel_{datetime.now().strftime('%Y%m%d%H%M')}"
    )
    success, msg = _execute_sell(
        code, volume, sell_price_input, acc_type,
        source="web_panel", idempotency_key=idem
    )
    disk_name = "🧪 模拟盘" if acc_type == 'sim' else "💰 实战盘"
    back_url = '/sim-dashboard' if acc_type == 'sim' else '/dashboard'
    buy_url = '/sim-buy-ui' if acc_type == 'sim' else '/buy_ui'
    title = f"{disk_name} 卖出成功" if success else f"{disk_name} 卖出失败"
    color = "#28a745" if success else "#ff5252"

    return f'''
    <html>
        <head><meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; text-align: center; padding: 50px 15px; }}
                h2 {{ color: {color}; }}
                .result-box {{ background: #1e1e1e; border-radius: 14px; padding: 25px; margin: 20px auto; max-width: 560px; white-space: pre-wrap; }}
                a.btn {{ display: block; padding: 15px; background: #333; color: white; text-decoration: none; border-radius: 10px;
                         margin: 10px auto; max-width: 300px; font-weight: bold; border: 1px solid #555; }}
            </style>
        </head>
        <body>
            <h2>{title}</h2>
            <div class="result-box">
                {html.escape(msg)}
            </div>
            <a href="{back_url}" class="btn">📊 查看看板</a>
            <a href="{buy_url}" class="btn">⬅️ 返回交易台</a>
        </body>
    </html>
    <script>setTimeout(function(){{ window.location.href = '{back_url if success else buy_url}'; }}, 1500);</script>
    '''


# Web 路由：实盘数据看板
@app.route('/health')
def health_check():
    """健康检查接口，用于外部监控"""
    from flask import jsonify
    health = get_market_health()
    guard = refresh_system_guard()
    return jsonify({
        "status": "ok",
        "uptime_min": round((time.time() - _START_TIME) / 60),
        "last_data_update": datetime.fromtimestamp(LAST_CACHE_TIME).strftime("%H:%M:%S") if LAST_CACHE_TIME else "never",
        "data_failures": consecutive_data_failures,
        "stocks_monitored": len(STOCKS),
        "is_trade_time": _is_trade_time_now(),
        "market_source": health.get("source"),
        "market_health_score": health.get("score"),
        "market_age_sec": health.get("age_sec"),
        "halt_new_buys": guard.get("halt_new_buys"),
        "guard_reasons": guard.get("reasons"),
        "startup_check_ok": _STARTUP_CHECK_STATUS.get("ok", True),
    })


@app.route('/trade_log')
@login_required
def trade_log_page():
    """交易变更历史（轻量版本控制）"""
    records = read_trade_log(limit=100)
    
    rows_html = ""
    for r in records:
        act = r.get('action', '')
        acc = "🧪模拟" if r.get('account') == 'sim' else "💰实盘"
        color = '#ff5252' if '卖出' in act or '清仓' in act else '#00c851'
        pnl_info = ""
        if 'pnl' in r:
            p = r['pnl']
            pnl_color = '#00c851' if p >= 0 else '#ff5252'
            pnl_info = f' | 盈亏: <span style="color:{pnl_color};">{"+" if p >= 0 else ""}{p:.2f}元 ({r.get("pnl_pct", 0):+.1f}%)</span>'
        rows_html += f'''
        <tr style="border-bottom:1px solid #333;">
            <td style="padding:8px; color:#888; font-size:13px;">{r.get('time', '')}</td>
            <td style="padding:8px;"><span style="color:{color}; font-weight:bold;">{act}</span></td>
            <td style="padding:8px;">{acc}</td>
            <td style="padding:8px;">{r.get('name', '')}({r.get('code', '')})</td>
            <td style="padding:8px;">{r.get('price', 0):.2f} × {r.get('volume', 0)}</td>
            <td style="padding:8px;">{r.get('amount', 0):,.2f}元{pnl_info}</td>
        </tr>'''
    
    if not rows_html:
        rows_html = '<tr><td colspan="6" style="padding:40px; color:#666; text-align:center;">暂无交易记录</td></tr>'
    
    return f'''
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 20px; }}
                h2 {{ text-align: center; color: #fff; }}
                table {{ width: 100%; max-width: 900px; margin: 20px auto; border-collapse: collapse; background: #1e1e1e; border-radius: 10px; overflow: hidden; }}
                th {{ padding: 12px; text-align: left; color: #aaa; font-size: 13px; border-bottom: 2px solid #333; }}
                a.btn {{ display: block; max-width: 300px; margin: 20px auto; padding: 12px; background: #333; color: #fff; text-decoration: none; border-radius: 8px; text-align: center; }}
                a.btn:hover {{ background: #444; }}
                .summary {{ text-align: center; color: #888; font-size: 13px; }}
            </style>
        </head>
        <body>
            <h2>📋 交易变更历史</h2>
            <p class="summary">最近 {len(records)} 条记录 · append-only 日志</p>
            <table>
                <tr><th>时间</th><th>操作</th><th>账户</th><th>标的</th><th>价格×数量</th><th>金额</th></tr>
                {rows_html}
            </table>
            <a href="/buy_ui" class="btn">⬅️ 返回交易台</a>
        </body>
    </html>
    '''


# Web 路由：数据看板（统一实盘/模拟盘）
def _build_dashboard(acc_type):
    """构建设看板界面（统一实盘/模拟盘，消除重复代码）"""
    is_sim = (acc_type == 'sim')
    capital = SIM_TOTAL_CAPITAL if is_sim else TOTAL_CAPITAL
    label = "🧪 模拟盘" if is_sim else "💰 实盘"
    pos_file = SIM_POSITIONS_FILE if is_sim else POSITIONS_FILE

    total_assets, total_stock_value, holdings_items, total_pnl = get_dashboard_asset_stats(acc_type=acc_type)
    cash_ratio = (total_assets - total_stock_value) / total_assets * 100 if total_assets > 0 else 0

    total_pnl_pct = (total_pnl / capital) * 100 if capital > 0 else 0
    stock_ratio = (total_stock_value / capital) * 100 if capital > 0 else 0
    cash_value = total_assets - total_stock_value
    pnl_color = '#00c851' if total_pnl >= 0 else '#ff4444'

    def is_valid(f):
        return os.path.exists(f) and os.path.getsize(f) > 2

    if not is_valid(pos_file):
        empty_msg = "🧪 模拟盘空仓" if is_sim else "⚠️ 未录入任何持仓"
        link = '/sim-buy-ui' if is_sim else '/buy_ui'
        return f"<h1 style='color:white; background:#121212; text-align:center; padding-top:50px;'>{empty_msg}</h1><p style='text-align:center;'><a href='{link}'>前往买入</a></p>"

    df_spot = get_shared_market_data()
    if df_spot is None:
        return f'''
        <body style="background:#121212; color:white; text-align:center; padding-top:100px;">
            <h1>📡 行情初始化中...</h1>
            <p style="color:#888;">请稍候刷新</p>
        </body>
        '''

    try:
        page_html = f'''
        <html><head>
            <title>{label}看板</title>
            <meta http-equiv="refresh" content="60">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 15px; }}
                .card {{ background: #1e1e1e; border-radius: 12px; padding: 15px; margin-bottom: 15px; }}
                .progress-bg {{ background: #333; border-radius: 20px; height: 35px; width: 100%; margin: 15px 0; overflow: hidden; }}
                .progress-fill {{ background: linear-gradient(90deg, #28a745, #b2ff59); height: 100%; text-align: center; color: #000; line-height: 35px; font-weight: bold; }}
                .btn-refresh {{ display: block; width: 100%; padding: 12px; background: #333; color: white; text-align: center;
                               text-decoration: none; border-radius: 8px; margin-bottom: 15px; font-weight: bold; border: 1px solid #444; }}
                .asset-box {{ background:#1a1a1a; padding:15px; border-radius:14px; margin-bottom:20px; text-align:center; }}
                .pie-wrap {{ display:flex; align-items:center; justify-content:center; gap:15px; margin:10px 0; }}
                .pie {{ width:110px; height:110px; border-radius:50%;
                        background: conic-gradient(#4caf50 {cash_ratio}%, #2196f3 0%);
                        display:grid; place-content:center; }}
                .pie-inner {{ width:80px; height:80px; background:#121212; border-radius:50%;
                             display:grid; place-content:center; }}
                .red {{ color: #ff4444; }}
                .green {{ color: #00c851; }}
                .stat-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; margin: 15px 0; }}
                .stat-item {{ background: #2a2a2a; padding: 12px; border-radius: 8px; text-align: center; }}
                .stat-label {{ font-size: 12px; color: #888; margin-bottom: 4px; }}
                .stat-value {{ font-size: 18px; font-weight: bold; }}
            </style>
        </head><body>

        <!-- 资金概览卡片 -->
        <div class="asset-box">
            <h3 style="margin:0 0 10px 0;">{label} 资金概览</h3>
            <div class="stat-grid">
                <div class="stat-item">
                    <div class="stat-label">本金</div>
                    <div class="stat-value">¥{capital:,.0f}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">仓内市值</div>
                    <div class="stat-value">¥{total_stock_value:,.2f}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">仓位占比</div>
                    <div class="stat-value">{stock_ratio:.1f}%</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">可用现金</div>
                    <div class="stat-value">¥{cash_value:,.2f}</div>
                </div>
            </div>
            <div style="background:#2a2a2a; padding:15px; border-radius:10px; margin-top:10px;">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <span style="color:#888;">总盈亏</span>
                    <span style="font-size:24px; font-weight:bold; color:{pnl_color};">
                        {'+' if total_pnl >= 0 else ''}¥{total_pnl:,.2f} ({'+' if total_pnl_pct >= 0 else ''}{total_pnl_pct:.2f}%)
                    </span>
                </div>
            </div>
        </div>

        <!-- 资产分布（仅实盘显示饼图）'''
        if not is_sim:
            page_html += f'''
        <div class="asset-box">
            <h3>📊 资产分布</h3>
            <div class="pie-wrap">
                <div class="pie">
                    <div class="pie-inner">
                        <span style="font-size:12px;color:#888;">持仓</span>
                        <b style="font-size:18px;">{100 - cash_ratio:.1f}%</b>
                    </div>
                </div>
                <div style="text-align:left;font-size:14px;">
                    <p>🟢 现金 {cash_ratio:.1f}%</p>
                    <p>🔵 股票 {100 - cash_ratio:.1f}%</p>
                </div>
            </div>
        </div>'''

        other_dash = '/dashboard' if is_sim else '/sim-dashboard'
        other_dash_label = '💰 查看实盘' if is_sim else '🧪 查看模拟盘'
        trade_ui = '/sim-buy-ui' if is_sim else '/buy_ui'

        page_html += f'''
        <h2 style="text-align:center;">{label} 持仓</h2>
        <div style="text-align:right; margin-bottom:12px; color:#888; font-size:12px;">
            已登录 {WEB_ADMIN_USER} | <a href="/logout" style="color:#4fc3f7; text-decoration:none;">退出登录</a>
        </div>
        <a href="{trade_ui}" class="btn-refresh">📈 交易台</a>
        <a href="{other_dash}" class="btn-refresh" style="background:#444;">{other_dash_label}</a>
        '''

        # 持仓卡片列表
        for item in holdings_items:
            try:
                code = item.get('code', '')
                name = item.get('name', '')
                val = float(item.get('value', 0))
                item_pnl_pct = float(item.get('pnl_pct', 0))
                item_pnl_color = '#69f0ae' if item_pnl_pct >= 0 else '#ff5252'

                if is_sim:
                    # 模拟盘：固定蓝色边框，显示详细信息
                    page_html += f'''
                <div class="card" style="border-left:6px solid #2196f3;">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span style="font-size:18px; font-weight:bold;">{name}</span>
                        <a href="/sell_page?acc_type=sim&code={code}" style="color:#e53935; font-size:13px; text-decoration:none; background:#2a1a1a; padding:3px 10px; border-radius:6px;">🔻 卖出</a>
                    </div>
                    <div style="margin-top:8px; font-size:14px; color:#aaa;">
                        现价：¥{item.get('price', 0):.2f} | 成本：¥{item.get('buy_price', 0):.2f} | 市值：¥{val:.2f}
                    </div>
                    <div style="margin-top:8px; font-size:14px;">
                        盈亏：<span style="color:{item_pnl_color};">
                        {'+' if item_pnl_pct >= 0 else ''}{item_pnl_pct:.1f}% ({'+' if item.get('pnl', 0) >= 0 else ''}¥{item.get('pnl', 0):.2f})</span>
                    </div>
                </div>
                    '''
                else:
                    # 实盘：权重风险颜色预警
                    weight = (val / total_assets) * 100 if total_assets > 0 else 0
                    if weight < 10:
                        border = "#4caf50"
                    elif weight <= 20:
                        border = "#ff9800"
                    else:
                        border = "#ff4444"

                    page_html += f'''
                <div class="card" style="border-left:6px solid {border};">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <span style="font-size:18px; font-weight:bold;">{name}</span>
                        <a href="/sell_page?acc_type=real&code={code}" style="color:#e53935; font-size:13px; text-decoration:none; background:#2a1a1a; padding:3px 10px; border-radius:6px;">🔻 卖出</a>
                    </div>
                    <div style="margin-top:8px; font-size:14px; color:#aaa;">
                        盈亏: <span style="color:{item_pnl_color};">
                        {'+' if item_pnl_pct >= 0 else ''}{item_pnl_pct:.1f}%</span>
                    </div>
                    <div style="font-size:12px; color:#888; margin-top:4px;">
                        市值: ¥{val:.2f}
                    </div>
                </div>
                    '''
            except Exception:
                continue

        # 回本进度条（仅实盘）
        if not is_sim:
            progress = 0.0
            remaining = 0.0
            try:
                target_profit = SIM_TARGET_PROFIT if is_sim else TARGET_PROFIT
                if target_profit > 0:
                    progress = min(max(total_pnl / target_profit * 100, 0), 100)
                    remaining = max(target_profit - total_pnl, 0)
            except Exception:
                pass

            page_html += f'''
            <div style="text-align:center; margin-top:25px; padding:20px; background:#1a1a1a; border-radius:15px;">
                <h3>🎯 回本进度 {progress:.1f}%</h3>
                <div class="progress-bg">
                    <div class="progress-fill" style="width:{progress}%;">{progress:.1f}%</div>
                </div>
                <p style="font-size:16px;">还差 ¥{remaining:.2f}</p>
            </div>
            '''

        back_link = '/sim-buy-ui' if is_sim else '/buy_ui'
        back_label = '模拟盘交易台' if is_sim else '主菜单'
        uptime_min = round((time.time() - _START_TIME) / 60)
        uptime_str = f"{uptime_min // 60}h{uptime_min % 60}m" if uptime_min >= 60 else f"{uptime_min}m"
        last_update = datetime.fromtimestamp(LAST_CACHE_TIME).strftime("%H:%M:%S") if LAST_CACHE_TIME else "尚未更新"
        source_display = MARKET_SOURCE if MARKET_SOURCE not in ("None", "") else "未知"
        is_trade = _is_trade_time_now()
        status_color = "#00c851" if is_trade else "#ff9800"
        status_text = "交易中" if is_trade else "休盘中"
        fail_display = consecutive_data_failures
        fail_color = "#ff4444" if fail_display > 0 else "#00c851"
        page_html += f'''
            <!-- 系统健康卡片 -->
            <div class="card" style="margin-top:15px; padding:12px;">
                <h3 style="margin:0 0 8px 0; font-size:13px; color:#888;">📡 系统状态</h3>
                <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:8px; font-size:12px;">
                    <div style="text-align:center;">
                        <div style="color:#888;">数据源</div>
                        <div style="font-weight:bold; color:#4fc3f7;">{source_display}</div>
                    </div>
                    <div style="text-align:center;">
                        <div style="color:#888;">上次更新</div>
                        <div style="font-weight:bold;">{last_update}</div>
                    </div>
                    <div style="text-align:center;">
                        <div style="color:#888;">状态</div>
                        <div style="font-weight:bold; color:{status_color};">{status_text}</div>
                    </div>
                    <div style="text-align:center;">
                        <div style="color:#888;">运行时长</div>
                        <div style="font-weight:bold;">{uptime_str}</div>
                    </div>
                    <div style="text-align:center;">
                        <div style="color:#888;">连续失败</div>
                        <div style="font-weight:bold; color:{fail_color};">{fail_display}次</div>
                    </div>
                    <div style="text-align:center;">
                        <div style="color:#888;">监控数</div>
                        <div style="font-weight:bold;">{len(STOCKS)}只</div>
                    </div>
                </div>
            </div>
            <div style="text-align:center; margin-top:20px;">
                <a href="{back_link}" style="color:#888; text-decoration:none;">⬅️ 返回{back_label}</a>
            </div>
        </body></html>
        '''
        return page_html

    except _NativeException as e:
        log_terminal("看板异常", str(e))
        return f"<body style='background:black;color:white;'><h3>渲染失败：{e}</h3></body>"


@app.route('/dashboard')
@login_required
def dashboard():
    """实盘看板"""
    return _build_dashboard('real')


@app.route('/sim-dashboard')
@login_required
def sim_dashboard():
    """模拟盘看板"""
    return _build_dashboard('sim')


@app.route('/api/decision_targets')
@login_required(api=True)
def api_decision_targets():
    """API: 返回某票最近的 AI 决策止损/止盈位（供卖出页JS调用）"""
    code = request.args.get('code', '').strip().zfill(6)
    if not os.path.exists(AI_DECISIONS_FILE):
        return jsonify({})
    result = {}
    try:
        with open(AI_DECISIONS_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for line in reversed(lines[-50:]):
            d = json.loads(line.strip())
            if d.get('code') == code:
                result = {
                    'stop_loss': d.get('stop_loss'),
                    'tp1': d.get('tp1'),
                    'tp2': d.get('tp2'),
                    'tp3': d.get('tp3')
                }
                break
    except Exception:
        pass
    # 计算动态保护止损位（基于实盘或模拟盘持仓）
    try:
        holdings_real, holdings_sim = get_cached_holdings()
        h = (holdings_real or {}).get(code) or (holdings_sim or {}).get(code)
        if h:
            buy_price = h.get('buy_price', 0)
            df_spot = get_shared_market_data()
            if buy_price > 0 and df_spot is not None and not df_spot.empty:
                row = df_spot[df_spot['代码'] == code]
                if not row.empty:
                    curr_price = float(row['最新价'].values[0])
                    pnl_pct = (curr_price - buy_price) / buy_price * 100
                    dyn_stop, dyn_label = get_dynamic_stop_loss(buy_price, pnl_pct)
                    result['dyn_stop'] = round(dyn_stop, 2)
                    result['dyn_label'] = dyn_label
    except Exception:
        pass
    return jsonify(result)


@app.route('/api/stock_info')
@login_required(api=True)
def api_stock_info():
    """API: 查询股票现价和可买空间（供买入页JS调用）"""
    code = request.args.get('code', '').strip().zfill(6)
    acc_type = request.args.get('acc_type', 'real')

    # 查现价
    price = 0
    name = code
    with cache_lock:
        info = DASHBOARD_CACHE.get(code)
        if info:
            price = info.get('price', 0)
            name = info.get('name', code)

    # 如果缓存没有，尝试从共享行情查
    if price == 0:
        df = get_shared_market_data()
        if df is not None and not df.empty:
            try:
                code_series = df['代码'].astype(str).str.extract(r'(\d{6})', expand=False).fillna('')
                row = df[code_series == code]
                if not row.empty:
                    price = float(row['最新价'].values[0])
                    name = str(row['名称'].values[0]) if '名称' in row.columns else name
            except Exception:
                pass

    # 如果共享行情也没有，单独拉这只股票的日线（非交易时间也能查到最近收盘价）
    if price == 0:
        try:
            full_code = 'sh' + code if code.startswith(('6', '9')) else 'sz' + code
            df_single = ak.stock_zh_a_hist_tx(symbol=full_code, start_date="20240101", adjust="qfq")
            if df_single is not None and not df_single.empty:
                last_row = df_single.iloc[-1]
                if '收盘' in last_row:
                    price = float(last_row['收盘'])
                elif '收盘价' in last_row:
                    price = float(last_row['收盘价'])
                elif 'close' in last_row:
                    price = float(last_row['close'])
                name = str(last_row.get('名称') or last_row.get('name') or code)
                log_terminal("单票查询", f"✅ 从网络获取 {code} 最新价: {price}")
        except Exception as e:
            log_terminal("单票查询失败", f"❌ {code}: {e}")

    if price <= 0:
        return jsonify({"error": f"未找到 {code} 的行情数据，请检查代码是否正确"})

    # 查可用现金
    account_state = read_account_state(acc_type, use_cache=True)
    initial_capital = account_state["configured_capital"]
    cash = account_state["cash"]

    max_vol = cash / price if price > 0 else 0

    # 仓位限制：总仓位70%、单票40%（和 precheck_buy_order 同口径）
    total_ratio_now = 0.0
    single_ratio_now = 0.0
    try:
        holdings = account_state["holdings"]
        if holdings:
            current_spent = sum(h['buy_price'] * h['volume'] for h in holdings.values())
            total_ratio_now = current_spent / initial_capital if initial_capital > 0 else 0
            # 总仓位上限
            remaining_ratio = 0.70 - total_ratio_now
            max_by_total_pos = remaining_ratio * initial_capital / price if price > 0 else 0
            max_vol = min(max_vol, max_by_total_pos)
            # 单票上限
            old_cost = holdings.get(code, {}).get('buy_price', 0) * holdings.get(code, {}).get('volume', 0)
            single_ratio_now = old_cost / initial_capital if initial_capital > 0 else 0
            max_single_cost = initial_capital * 0.40 - old_cost
            max_by_single = max_single_cost / price if price > 0 else 0
            max_vol = min(max_vol, max_by_single)
    except Exception:
        pass

    return jsonify({
        "code": code, "name": name, "price": price,
        "cash": round(cash, 0), "max_vol": max_vol,
        "total_ratio_now": round(total_ratio_now * 100, 1),
        "single_ratio_now": round(single_ratio_now * 100, 1)
    })


# ===================== 盈亏提醒 =====================


def get_dynamic_stop_loss(buy_price, pnl_pct):
    return risk_rules.get_dynamic_stop_loss(buy_price, pnl_pct)


def check_pnl_alerts():
    """持仓盈亏监控：止盈 & 止损自动推送提醒（带冷却机制）
    同时监控实盘和模拟盘持仓。

    规则结论统一由 qt_risk_rules 提供，主文件只负责行情读取、风险事件编排、
    冷却控制和推送输出，避免这里继续长出第二套盈亏判断树。"""
    try:
        real_holdings, sim_holdings = get_cached_holdings()

        if not real_holdings and not sim_holdings:
            log_terminal("盈亏检查", "😶 实盘+模拟盘均空仓，无需检查")
            return

        df_spot = get_shared_market_data()
        if df_spot is None or df_spot.empty:
            log_terminal("盈亏检查", "❌ 行情数据为空，无法计算盈亏")
            return

        def _push_risk_notice(acc_type, name, code, event, status_or_pnl, reason):
            title, content = push_templates.build_risk_warning_message(
                name=name,
                code=code,
                account=acc_type,
                event=event,
                status_or_pnl=status_or_pnl,
                reason=reason,
                title="【风控预警】",
            )
            push_decision(title, content, code=code, urgent=True)

        market_health = get_market_health()
        guard = refresh_system_guard()
        market_health_score = market_health.get("score", 0)
        halt_new_buys = guard.get("halt_new_buys", False)

        def _handle_profit_stage(acc_type, code, name, buy_price, curr_price, pnl, volume):
            conf = _ai_confidence_cache.get(code, {})
            plan = risk_rules.get_profit_alert_plan(
                pnl_pct=pnl,
                volume=volume,
                buy_price=buy_price,
                market_health_score=market_health_score,
                halt_new_buys=halt_new_buys,
                gemma_score=conf.get("gemma_score", 0),
                ds_confidence=conf.get("ds_confidence", ""),
                tp1_pct=TP1_PCT,
                tp2_pct=TP2_PCT,
                tp3_pct=TP3_PCT,
                tp1_sell_ratio=TP1_SELL_RATIO,
                tp2_sell_ratio=TP2_SELL_RATIO,
                tp3_sell_ratio=TP3_SELL_RATIO,
                keeper_min_retain_ratio=KEEPER_MIN_RETAIN_RATIO,
                weak_market_score=WEAK_MARKET_SCORE,
                strong_market_score=STRONG_MARKET_SCORE,
            )
            stage = plan.get("stage")
            if stage == "none":
                return

            event_key = f"{stage}_{acc_type}"
            mark_risk_event_open(
                event_key,
                acc_type,
                code,
                name=name,
                current_price=curr_price,
                buy_price=buy_price,
                pnl_pct=round(pnl, 2),
                threshold_price=round(plan.get("stop_price", buy_price), 2),
            )

            if not plan.get("should_alert", False):
                log_terminal(
                    "盈亏检查",
                    f"📈 {acc_type.upper()} {name} 盈利{pnl:.2f}%已触发{plan['stage_label']}，但{plan.get('suppress_reason', '当前不推送')}",
                )
                return

            cooldown_key = f"{stage}_{acc_type}_{code}"
            last_alert = _alert_cooldown.get(cooldown_key, 0)
            if time.time() - last_alert <= ALERT_COOLDOWN_SECONDS:
                log_terminal("盈亏检查", f"⏳ {acc_type.upper()} {name} 触发{plan['stage_label']}({pnl:.1f}%)，但冷却中")
                return

            _push_risk_notice(
                acc_type,
                name,
                code,
                plan["event"],
                plan["status_text"],
                plan["reason_text"],
            )
            _alert_cooldown[cooldown_key] = time.time()

        def _handle_stop_loss(acc_type, code, name, buy_price, curr_price, pnl):
            stop_plan = risk_rules.get_stop_loss_alert_plan(
                pnl_pct=pnl,
                buy_price=buy_price,
                curr_price=curr_price,
            )
            if not stop_plan["triggered"]:
                return

            mark_risk_event_open(
                f"stop_{acc_type}_{stop_plan['stop_label']}",
                acc_type,
                code,
                name=name,
                current_price=curr_price,
                buy_price=buy_price,
                pnl_pct=round(pnl, 2),
                threshold_price=stop_plan["stop_price"],
            )

            cooldown_key = f"stop_{acc_type}_{code}"
            last_alert = _alert_cooldown.get(cooldown_key, 0)
            if time.time() - last_alert > ALERT_COOLDOWN_SECONDS:
                _push_risk_notice(
                    acc_type,
                    name,
                    code,
                    stop_plan["event"],
                    stop_plan["status_text"],
                    stop_plan["reason_text"],
                )
                _alert_cooldown[cooldown_key] = time.time()
                _alert_cooldown[f"stop_recheck_{acc_type}_{code}"] = time.time() + 1800
            else:
                remain_min = int((ALERT_COOLDOWN_SECONDS - (time.time() - last_alert)) / 60)
                log_terminal(
                    "盈亏检查",
                    f"⏳ {acc_type.upper()} {name} 触发止损({stop_plan['stop_label']})，但冷却中({remain_min}分钟后可再推)",
                )

            recheck_key = f"stop_recheck_{acc_type}_{code}"
            if recheck_key in _alert_cooldown and time.time() > _alert_cooldown[recheck_key]:
                _push_risk_notice(
                    acc_type,
                    name,
                    code,
                    stop_plan["recheck_event"],
                    stop_plan["status_text"],
                    stop_plan["recheck_reason"],
                )
                del _alert_cooldown[recheck_key]

        def _process_holdings(acc_type, holdings):
            account_label = "实盘" if acc_type == "real" else "模拟盘"
            for code, info in holdings.items():
                try:
                    buy_price = info['buy_price']
                    volume = info.get('volume', 0)
                    row = df_spot[df_spot['代码'] == code]
                    if row.empty:
                        log_terminal("盈亏检查", f"⚠️ {account_label} {code} 行情数据缺失")
                        continue
                    curr_price_series = row['最新价']
                    if curr_price_series.empty:
                        log_terminal("盈亏检查", f"⚠️ {account_label} {code} 价格数据缺失")
                        continue
                    curr_price = float(curr_price_series.values[0])
                    name_series = row['名称']
                    name = name_series.values[0] if not name_series.empty else code
                    pnl = (curr_price - buy_price) / buy_price * 100

                    _handle_stop_loss(acc_type, code, name, buy_price, curr_price, pnl)
                    _handle_profit_stage(acc_type, code, name, buy_price, curr_price, pnl, volume)
                except KeyError as e:
                    log_terminal("字段缺失", f"❌ {account_label} {code} 持仓信息缺少字段：{str(e)}")
                except ZeroDivisionError as e:
                    log_terminal("计算错误", f"❌ {account_label} {code} 除零错误（成本价为 0）: {str(e)}")
                except Exception as e:
                    log_terminal("个股计算失败", f"❌ {account_label} {code} 盈亏计算错误：{type(e).__name__} - {str(e)[:100]}")
                    logger.error(f"{account_label} {code} 详细错误：{str(e)}")

        _process_holdings("real", real_holdings)
        _process_holdings("sim", sim_holdings)

    except _NativeException as e:
        log_terminal("监控异常", f"盯盘逻辑运行失败：{e}")


def get_current_occupancy(file_path):
    """计算指定账本当前总持仓占用的资金（使用缓存，避免磁盘读取）"""
    try:
        if file_path == POSITIONS_FILE:
            holdings, _ = get_cached_holdings()
        else:
            _, holdings = get_cached_holdings()
        total_spent = sum(info['buy_price'] * info['volume'] for info in holdings.values()) if holdings else 0
        return total_spent
    except _NativeException as e:
        log_terminal("风控读取异常", f"持仓缓存解析失败：{e}")
        return TOTAL_CAPITAL * 0.5


def precheck_buy_order(acc_type, code, buy_price_val, volume_val, use_cache=True):
    return risk_rules.precheck_buy_order(
        acc_type,
        code,
        buy_price_val,
        volume_val,
        guard_reason=check_new_buy_guard(),
        is_trade_time_now=_is_trade_time_now(),
        now_provider=datetime.now,
        read_account_state_fn=read_account_state,
        logger_instance=logger,
        use_cache=use_cache,
    )


def get_strategy_constraints(code, price, m_ratio, holdings):
    return risk_rules.get_strategy_constraints(
        code,
        price,
        m_ratio,
        holdings,
        get_shared_market_data_fn=get_shared_market_data,
        total_capital=TOTAL_CAPITAL,
        target_profit=TARGET_PROFIT,
    )


def check_multi_gates(df, j_val, rsi, price, lower_band, vol_ratio):
    return risk_rules.check_multi_gates(df, j_val, rsi, price, lower_band, vol_ratio)


# ==========================================
# 5. 核心逻辑执行体 (全方位指标合并版)
# ==========================================

# ====================== 异步批量审计（真正并发 + Semaphore 限流）======================
async def async_execute_audits(stocks, m_ratio, m_vol):
    """异步批量审计：并发处理所有股票，Semaphore 限制同时请求数防封 IP"""
    _prune_confidence_cache()
    semaphore = asyncio.Semaphore(5)  # 最多5个股票同时审计

    async def _run_with_sem(code):
        async with semaphore:
            try:
                await async_execute_audit(code, m_ratio, m_vol)
            except BaseException as e:
                logger.error(f"审计 {code} 异常：{type(e).__name__} - {e}", exc_info=True)

    await asyncio.gather(*[_run_with_sem(s) for s in stocks])


# ====================== 异步单股票审计（100%保留你的逻辑）======================
def _build_watch_features(df_5m, price, ma5, ma10, vol_ratio, vol_rising, m_ratio, s_rsi):
    recent_lows = []
    if 'low' in df_5m.columns:
        recent_lows = [float(x) for x in df_5m['low'].tail(3).tolist() if pd.notna(x)]
    last_low = recent_lows[-1] if recent_lows else float(price)
    prev_low = recent_lows[-2] if len(recent_lows) >= 2 else last_low
    rolling_low = min(recent_lows) if recent_lows else float(price)
    last_open = float(df_5m['open'].iloc[-1]) if 'open' in df_5m.columns and not df_5m.empty else float(price)
    last_high = float(df_5m['high'].iloc[-1]) if 'high' in df_5m.columns and not df_5m.empty else float(price)
    return {
        "made_new_low": bool(last_low <= rolling_low),
        "higher_low": bool(last_low > prev_low),
        "reclaimed_ma5_or_ma10": bool(price >= ma5 or price >= ma10),
        "volume_recovered": bool(vol_ratio >= 1.0 or vol_rising),
        "blowoff_reversal": bool(last_high >= float(price) * 1.01 and float(price) < last_open),
        "market_worsened": bool(m_ratio is not None and m_ratio < 0.45),
        "sector_strengthening": bool(s_rsi <= 35),
    }


def _normalize_watch_refresh_frame(df):
    if df is None or df.empty:
        return None
    df = df.copy()
    if isinstance(df.index, pd.DatetimeIndex) and 'datetime' not in df.columns:
        df = df.reset_index()
        first_col = df.columns[0]
        if first_col != 'datetime':
            df = df.rename(columns={first_col: 'datetime'})

    rename_pairs = {
        '时间': 'datetime',
        '日期': 'datetime',
        'day': 'datetime',
        'date': 'datetime',
        '开盘': 'open',
        '最高': 'high',
        '最低': 'low',
        '收盘': 'close',
        '收盘价': 'close',
        '成交量': 'volume',
        '成交额': 'amount',
    }
    for source_col, target_col in rename_pairs.items():
        if source_col in df.columns and target_col not in df.columns:
            df = df.rename(columns={source_col: target_col})

    if 'datetime' in df.columns:
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        df = df.dropna(subset=['datetime']).sort_values('datetime')

    for col in ('open', 'high', 'low', 'close', 'volume', 'amount'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'close' not in df.columns:
        return None
    return df.dropna(subset=['close']).reset_index(drop=True)


def _get_watch_market_ratio():
    try:
        df = get_shared_market_data()
        if df is None or df.empty or '涨跌幅' not in df.columns:
            return None
        changes = pd.to_numeric(df['涨跌幅'], errors='coerce').dropna()
        if len(changes) < 10:
            return None
        return float((changes > 0).sum() / len(changes))
    except _NativeException:
        return None
    except Exception:
        return None


async def _refresh_watch_candidate_features(item):
    code = str(item.get("code", "")).zfill(6)
    name = item.get("name") or code
    full_code = f"sh{code}" if code.startswith('6') else f"sz{code}"
    shared_price = float(item.get("price") or 0.0)
    shared_name = str(name)

    try:
        shared_df = get_shared_market_data()
        if shared_df is not None and not shared_df.empty and '代码' in shared_df.columns:
            row = shared_df[shared_df['代码'].astype(str).str.extract(r'(\d{6})', expand=False) == code]
            if not row.empty:
                if '最新价' in row.columns:
                    shared_price = float(pd.to_numeric(row['最新价'], errors='coerce').dropna().iloc[-1])
                if '名称' in row.columns:
                    shared_name = str(row['名称'].iloc[-1])
    except Exception:
        pass

    em_start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d 09:30:00")
    em_end = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d 15:30:00")
    df_refresh = None
    refresh_mode = "5分钟"
    refresh_reason = "已刷新最新短周期特征"

    for attempt in range(1, 3):
        try:
            try:
                df_refresh = await asyncio.to_thread(ak.stock_zh_a_minute, symbol=full_code, period='5')
                refresh_mode = "5分钟(Sina)"
            except _NativeException:
                df_refresh = await asyncio.to_thread(
                    ak.stock_zh_a_hist_min_em,
                    symbol=code,
                    start_date=em_start,
                    end_date=em_end,
                    period='5',
                    adjust='',
                )
                refresh_mode = "5分钟(EastMoney)"
            if df_refresh is not None and not df_refresh.empty:
                break
        except _NativeException:
            df_refresh = None
        if attempt < 2:
            await asyncio.sleep(0.4)

    if df_refresh is None or df_refresh.empty:
        try:
            df_refresh = await market_data.get_30m_kline(
                full_code,
                code,
                df_5m_backup=None,
                log_terminal=log_terminal,
                native_exception=_NativeException,
            )
            if df_refresh is not None and not df_refresh.empty:
                refresh_mode = "30分钟"
                refresh_reason = "5分钟线不可用，已降级到30分钟"
        except _NativeException:
            df_refresh = None

    if df_refresh is None or df_refresh.empty:
        try:
            df_refresh = await market_data.get_daily_kline(full_code, code)
            if df_refresh is not None and not df_refresh.empty:
                refresh_mode = "日线"
                refresh_reason = "短周期不可用，已降级到日线"
        except _NativeException:
            df_refresh = None

    normalized = _normalize_watch_refresh_frame(df_refresh)
    if normalized is None or normalized.empty:
        return {
            "price": shared_price,
            "name": shared_name,
            "features": dict(item.get("features", {})),
            "refresh_mode": "quote_only",
            "refresh_reason": "刷新失败，保留原观察特征",
            "refreshed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    close_series = pd.to_numeric(normalized['close'], errors='coerce').dropna()
    if close_series.empty:
        return {
            "price": shared_price,
            "name": shared_name,
            "features": dict(item.get("features", {})),
            "refresh_mode": "quote_only",
            "refresh_reason": "无有效收盘价，保留原观察特征",
            "refreshed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    price = float(close_series.iloc[-1])
    ma5 = float(close_series.tail(5).mean()) if len(close_series) >= 1 else price
    ma10 = float(close_series.tail(10).mean()) if len(close_series) >= 1 else price
    vol_ratio = 1.0
    vol_rising = False
    if 'volume' in normalized.columns:
        volumes = pd.to_numeric(normalized['volume'], errors='coerce').fillna(0)
        if not volumes.empty:
            last_vol = float(volumes.iloc[-1])
            prev_vol = float(volumes.iloc[-2]) if len(volumes) >= 2 else last_vol
            base_vol = float(volumes.tail(6).iloc[:-1].mean()) if len(volumes) >= 3 else prev_vol
            vol_ratio = (last_vol / base_vol) if base_vol > 0 else 1.0
            vol_rising = last_vol >= prev_vol

    s_rsi = 50.0
    if len(close_series) >= 6:
        try:
            rsi_series = pandas_ta.rsi(close_series, length=6)
            if rsi_series is not None and not rsi_series.dropna().empty:
                s_rsi = float(rsi_series.dropna().iloc[-1])
        except Exception:
            pass

    m_ratio = _get_watch_market_ratio()
    features = _build_watch_features(
        df_5m=normalized,
        price=price,
        ma5=ma5,
        ma10=ma10,
        vol_ratio=vol_ratio,
        vol_rising=vol_rising,
        m_ratio=m_ratio,
        s_rsi=s_rsi,
    )
    return {
        "price": price,
        "name": shared_name,
        "features": features,
        "refresh_mode": refresh_mode,
        "refresh_reason": refresh_reason,
        "refreshed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

def _confidence_rank(confidence):
    mapping = {"low": 1, "medium": 2, "high": 3}
    return mapping.get(str(confidence or "").strip().lower(), 0)


def _build_watch_account_context(account, code, price, default_shares):
    account = "real" if str(account).strip().lower() == "real" else "sim"
    default_capital = SIM_TOTAL_CAPITAL if account == "sim" else TOTAL_CAPITAL
    account_state = read_account_state(account, use_cache=False)
    capital = float(account_state.get("configured_capital") or default_capital or 0.0)
    cash_available = float(account_state.get("cash") or 0.0)
    holdings = dict(account_state.get("holdings") or {})
    clean_code = str(code or "").zfill(6)
    current_spent = sum(
        float(info.get("buy_price", 0) or 0) * float(info.get("volume", 0) or 0)
        for info in holdings.values()
    )
    existing_single_cost = 0.0
    if clean_code in holdings:
        existing_single_cost = float(holdings[clean_code].get("buy_price", 0) or 0) * float(
            holdings[clean_code].get("volume", 0) or 0
        )
    current_total_ratio = (current_spent / capital) if capital > 0 else 0.0
    single_ratio = (existing_single_cost / capital) if capital > 0 else 0.0
    remaining_total_amt = max(0.0, capital * 0.70 - current_spent)
    remaining_single_amt = max(0.0, capital * MAX_POSITION_RATIO - existing_single_cost)
    can_buy_amt = min(cash_available, remaining_total_amt, remaining_single_amt)
    trial_shares_cap = int(default_shares or 100)
    blocking_reason = ""
    if capital <= 0 or price <= 0:
        blocking_reason = "账户资本或价格无效，执行闸门拒绝放行"
    elif can_buy_amt <= 0:
        blocking_reason = (
            f"仓位或现金不足：现金{cash_available:.0f}元，总仓{current_total_ratio:.1%}，单票{single_ratio:.1%}"
        )

    return account, {
        "cash_available": cash_available,
        "can_buy_amt": can_buy_amt,
        "current_total_ratio": current_total_ratio,
        "single_ratio": single_ratio,
        "trial_shares_cap": trial_shares_cap,
        "max_total_ratio": 0.70,
        "max_single_ratio": MAX_POSITION_RATIO,
        "blocking_reason": blocking_reason,
    }


def _enrich_gate_runtime_context(account_context):
    enriched = dict(account_context or {})
    health = get_market_health()
    guard = refresh_system_guard()
    enriched["market_health_score"] = int(float(health.get("score", 0) or 0))
    enriched["halt_new_buys"] = bool(guard.get("halt_new_buys", False))
    enriched["guard_reason"] = "；".join(str(x) for x in (guard.get("reasons") or []) if str(x).strip())
    return enriched


def _auto_watch_real_eligible(watch_result, real_context):
    if not AUTO_REAL_ENABLED:
        return False, "AUTO_REAL_ENABLED 未开启"
    if str(watch_result.get("decision", "")).strip().lower() != "confirm_buy":
        return False, "第三层未确认买入"
    if bool(watch_result.get("risk_flag", False)):
        return False, "第三层存在风险标记"
    if _confidence_rank(watch_result.get("confidence")) < _confidence_rank(AUTO_REAL_MIN_CONFIDENCE):
        return False, f"置信度未达到自动实盘阈值({AUTO_REAL_MIN_CONFIDENCE})"
    health = get_market_health()
    if float(health.get("score", 0) or 0) < float(AUTO_REAL_MIN_MARKET_SCORE):
        return False, f"市场健康分低于阈值({AUTO_REAL_MIN_MARKET_SCORE})"
    if runtime_events.has_open_risk_events("real"):
        return False, "实盘仍有未关闭风险事件"
    price = float(watch_result.get("price") or 0.0)
    legal_max_shares = 0
    if price > 0:
        legal_max_shares = max(0, int(float(real_context.get("can_buy_amt", 0.0) or 0.0) / price) // 100 * 100)
    if int(legal_max_shares or 0) < int(AUTO_REAL_MIN_SHARES):
        return False, f"实盘合法股数不足 {AUTO_REAL_MIN_SHARES} 股"
    return True, "自动实盘条件满足"


def _should_push_watch_result(watch_result):
    decision = str(watch_result.get("decision") or "").strip().lower()
    confidence = str(watch_result.get("confidence") or "").strip().lower()
    decision_source = str(watch_result.get("decision_source") or "").strip().lower()
    risk_flag = bool(watch_result.get("risk_flag"))
    features = dict(watch_result.get("features") or {})
    if decision == "confirm_buy":
        return True
    if decision == "wait":
        return (
            confidence == "high"
            or decision_source in {"ai_veto", "rule+ai_wait", "ai_risk_veto"}
            or bool(features.get("sector_strengthening"))
        )
    if decision == "reject":
        return (
            risk_flag
            or bool(features.get("made_new_low"))
            or bool(features.get("blowoff_reversal"))
        )
    return False


def _push_watch_result_notice(watch_result):
    decision_source = str(watch_result.get("decision_source") or "").strip().lower()
    code = str(watch_result.get("code") or "").zfill(6)
    if decision_source == "rule_fallback":
        _push_ai_notice(
            "watch_confirm",
            code,
            "第三层 AI 复核失败，已回退规则结果",
            "当前继续使用规则结果，请检查 OpenRouter/Gemini 状态",
        )
    if not _should_push_watch_result(watch_result):
        return False
    decision = str(watch_result.get("decision") or "").strip().lower()
    name = watch_result.get("name") or code
    price = watch_result.get("price")
    reason = watch_result.get("reason")
    if decision == "confirm_buy":
        title, content = push_templates.build_watch_confirm_pass_message(
            name=name,
            code=code,
            confidence=watch_result.get("confidence"),
            price=price,
            reason=reason,
        )
    elif decision == "wait":
        title, content = push_templates.build_watch_confirm_wait_message(
            name=name,
            code=code,
            price=price,
            reason=reason,
        )
    else:
        title, content = push_templates.build_watch_confirm_reject_message(
            name=name,
            code=code,
            price=price,
            reason=reason,
        )
    return push_decision(title, content, code=f"watch:{decision}:{code}")


async def _process_watch_confirm_result(watch_result):
    name = watch_result.get("name") or watch_result.get("code")
    if watch_result.get("decision") != "confirm_buy":
        return False

    price = float(watch_result.get("price") or 0.0)
    code = str(watch_result.get("code", "")).zfill(6)
    target_account = str(watch_result.get("target_account") or WATCH_CONFIRM_TARGET_ACCOUNT or "sim").strip().lower()
    if target_account not in ("sim", "real", "auto"):
        target_account = "sim"
    requested_mode = str(watch_result.get("target_mode") or WATCH_CONFIRM_TARGET_MODE or "trial").strip().lower()
    if requested_mode not in ("trial", "normal"):
        requested_mode = "trial"

    real_default_shares = int(
        watch_result.get("default_real_shares")
        or watch_result.get("default_shares")
        or watch_result.get("default_sim_shares")
        or 100
    )
    sim_default_shares = int(
        watch_result.get("default_sim_shares")
        or watch_result.get("default_shares")
        or real_default_shares
        or 100
    )

    account = "sim"
    mode = requested_mode
    default_shares = sim_default_shares
    account_context = {}
    gate_result = None

    if target_account == "auto":
        real_gate_default_shares = min(real_default_shares, sim_default_shares) if AUTO_REAL_FORCE_TRIAL else real_default_shares
        _, real_context = _build_watch_account_context("real", code, price, real_gate_default_shares)
        real_context = _enrich_gate_runtime_context(real_context)
        auto_real_ok, auto_real_reason = _auto_watch_real_eligible(watch_result, real_context)
        if auto_real_ok:
            real_mode = "trial" if AUTO_REAL_FORCE_TRIAL else requested_mode
            real_gate_result = await execution_gate_mod.evaluate_execution_gate(
                watch_result,
                account="real",
                default_shares=real_gate_default_shares,
                mode=real_mode,
                account_context=real_context,
                log_terminal_fn=log_terminal,
                logger_instance=logger,
            )
            if real_gate_result.get("decision_source") == "baseline_fallback":
                _push_ai_notice(
                    "execution_gate",
                    code,
                    "第四层 AI 闸门失败，实盘候选已回退 baseline",
                    "当前继续使用 baseline 结果，请检查 OpenRouter/GPT 状态",
                )
            if real_gate_result.get("allow"):
                auto_title, auto_content = push_templates.build_auto_account_message(
                    name=name,
                    code=code,
                    target_account="real",
                    mode=real_mode,
                    shares=real_gate_result.get("shares"),
                    reason="满足 AUTO_REAL 条件",
                )
                push_decision(auto_title, auto_content, code=f"auto:real:{code}")
                account = "real"
                mode = real_mode
                default_shares = real_gate_default_shares
                account_context = real_context
                gate_result = real_gate_result
            else:
                log_terminal("执行闸门", f"{name} auto->real 未放行，回落模拟盘 | {real_gate_result.get('reason', auto_real_reason)}")
                auto_title, auto_content = push_templates.build_auto_account_message(
                    name=name,
                    code=code,
                    target_account="sim",
                    shares=sim_default_shares,
                    reason=real_gate_result.get("reason") or auto_real_reason,
                )
                push_decision(auto_title, auto_content, code=f"auto:sim:{code}")
        else:
            log_terminal("执行闸门", f"{name} auto->real 条件未满足，回落模拟盘 | {auto_real_reason}")
            auto_title, auto_content = push_templates.build_auto_account_message(
                name=name,
                code=code,
                target_account="sim",
                shares=sim_default_shares,
                reason=auto_real_reason,
            )
            push_decision(auto_title, auto_content, code=f"auto:sim:{code}")
    elif target_account == "real":
        account = "real"
        default_shares = real_default_shares

    if gate_result is None:
        account, account_context = _build_watch_account_context(account, code, price, default_shares)
        account_context = _enrich_gate_runtime_context(account_context)
        gate_result = await execution_gate_mod.evaluate_execution_gate(
            watch_result,
            account=account,
            default_shares=default_shares,
            mode=mode,
            account_context=account_context,
            log_terminal_fn=log_terminal,
            logger_instance=logger,
        )
    if gate_result.get("decision_source") == "baseline_fallback":
        _push_ai_notice(
            "execution_gate",
            code,
            "第四层 AI 闸门失败，已回退 baseline",
            "当前继续使用 baseline 结果，请检查 OpenRouter/GPT 状态",
        )

    log_terminal(
        "执行闸门",
        f"{name} account={account} allow={gate_result['allow']} | shares={gate_result['shares']} | source={gate_result.get('decision_source', 'baseline')}"
    )
    if not gate_result.get("allow"):
        title, content = push_templates.build_execution_gate_block_message(
            name=name,
            code=code,
            account=account,
            price=price,
            reason=gate_result.get("reason"),
        )
        push_decision(title, content, code=f"gate:block:{code}")
        return False

    title, content = push_templates.build_execution_gate_allow_message(
        name=name,
        code=code,
        account=account,
        mode=gate_result.get("mode"),
        shares=gate_result.get("shares"),
        price=price,
        reason=gate_result.get("reason"),
    )
    push_decision(title, content, code=f"gate:allow:{code}")

    gate_token = (
        watch_result.get("watch_id")
        or watch_result.get("signal_id")
        or watch_result.get("decision_signal_id")
        or datetime.now().strftime("%Y%m%d%H%M%S")
    )
    gate_batch = f"watch_gate_{gate_token}"
    gate_idem = build_idempotency_key("buy", account, watch_result["code"], None, None, gate_batch)
    success, msg = _execute_buy(
        watch_result["code"],
        watch_result["price"],
        gate_result["shares"],
        account,
        source="watch_confirm",
        signal_id=watch_result.get("signal_id") or watch_result.get("decision_signal_id"),
        idempotency_key=gate_idem,
        strategy_batch=gate_batch,
        mode=gate_result.get("mode"),
        watch_id=watch_result.get("watch_id"),
        gate_decision_source=gate_result.get("decision_source"),
    )
    if success:
        log_terminal("观察买入", f"✅ {name} 已按观察确认链路买入[{account}] | {msg}")
    else:
        log_terminal("观察买入", f"⚠️ {name} 观察确认通过但执行未完成[{account}] | {msg}")
    return success


async def _scan_pending_watch_pool_execution_chain():
    watch_results = await watch_confirm_mod.scan_watch_pool_with_ai(
        now=datetime.now(),
        refresh_item_fn=_refresh_watch_candidate_features,
        log_terminal_fn=log_terminal,
        logger_instance=logger,
    )
    for watch_result in watch_results:
        log_terminal(
            "观察池",
            f"{watch_result['name']} 观察确认={watch_result['decision']} | 状态={watch_result.get('status', 'pending')} | 来源={watch_result.get('decision_source', 'rule')}"
        )
        _push_watch_result_notice(watch_result)
        if watch_result.get("decision") == "confirm_buy":
            await _process_watch_confirm_result(watch_result)
    return watch_results


async def _run_watch_confirm_execution_chain(
    *,
    code,
    name,
    price,
    features,
    signal_id,
    source_reason,
    default_sim_shares,
    default_real_shares,
):
    target_account = WATCH_CONFIRM_TARGET_ACCOUNT if WATCH_CONFIRM_TARGET_ACCOUNT in ("sim", "real", "auto") else "sim"
    target_mode = WATCH_CONFIRM_TARGET_MODE if WATCH_CONFIRM_TARGET_MODE in ("trial", "normal") else "trial"
    default_watch_shares = default_real_shares if target_account == "real" else default_sim_shares
    default_watch_shares = int(default_watch_shares or 100)

    candidate = watch_confirm_mod.add_watch_candidate(
        code=code,
        name=name,
        price=price,
        features=features,
        source_stage="deep_audit",
        source_reason=source_reason,
        signal_id=signal_id,
        target_account=target_account,
        target_mode=target_mode,
        default_shares=default_watch_shares,
        default_sim_shares=default_sim_shares,
        default_real_shares=default_real_shares,
    )
    log_terminal(
        "观察池",
        f"{name} 已入观察池 | watch_id={candidate['watch_id']} | target={target_account}/{target_mode}"
    )

    if not _is_trade_time_now():
        log_terminal("观察池", f"{name} 非交易时段，已录入观察池，跳过执行闸门")
        return

    watch_results = await watch_confirm_mod.scan_watch_pool_with_ai(
        now=datetime.now(),
        refresh_item_fn=_refresh_watch_candidate_features,
        log_terminal_fn=log_terminal,
        logger_instance=logger,
    )
    watch_result = next((x for x in watch_results if x.get("watch_id") == candidate["watch_id"]), None)
    if not watch_result:
        log_terminal("观察池", f"{name} 本轮未得到观察确认结果")
        return

    log_terminal("观察池", f"{name} 观察确认={watch_result['decision']} | 来源={watch_result.get('decision_source', 'rule')}")
    _push_watch_result_notice(watch_result)
    await _process_watch_confirm_result(watch_result)


async def async_execute_audit(code, m_ratio, m_vol):
    # 替换为异步延迟
    await asyncio.sleep(random.uniform(0.1, 0.8))

    """
    核心审计引擎 v13.6 (仓位熔断 + J值负数击发版)
    【异步优化版 · 所有业务逻辑完全不变】
    """
    if code in blacklist_cooldown:
        if time.time() < blacklist_cooldown[code]:
            return
        else:
            del blacklist_cooldown[code]

    full_code = f"sh{code}" if code.startswith('6') else f"sz{code}"
    log_terminal("名称转换", f"正在检索 {code} 的企业登记信息...")
    try:
        await asyncio.sleep(random.uniform(0.1, 0.3))

        # 使用全局名称缓存，避免每只股票都全量拉取5000行名称表
        name = await get_stock_name(code)
    except _NativeException as e:
        log_terminal("名称转换警告", f"{code} 名称获取失败：{e}")
        name = code

    log_terminal("进度", f">>> [1/5] 开始扫描 {name}({code})")
    r1_res = None

    try:
        await asyncio.sleep(random.uniform(0.1, 0.3))

        # 5分钟线主链先走新浪，失败再退东财，最后再走 30m / 日线降级
        em_start = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d 09:30:00")
        em_end = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d 15:30:00")
        df_5m = None
        prefetched_30m = None
        base_kline_mode = "5分钟"
        minute_err = None
        for attempt in range(1, 3):
            try:
                try:
                    df_5m = await asyncio.to_thread(ak.stock_zh_a_minute, symbol=full_code, period='5')
                except _NativeException as sina_err:
                    log_terminal("5分钟数据源", f"{name} 新浪5分钟失败({type(sina_err).__name__})，切东财备用...")
                    df_5m = await asyncio.to_thread(
                        ak.stock_zh_a_hist_min_em,
                        symbol=code,
                        start_date=em_start,
                        end_date=em_end,
                        period='5',
                        adjust=''
                    )
                if df_5m is not None and not df_5m.empty:
                    if attempt > 1:
                        log_terminal("5分钟数据源", f"{name} 5分钟线重试成功（第{attempt}次）")
                    break
                minute_err = RuntimeError("empty_5m_data")
                if attempt < 2:
                    log_terminal("5分钟数据源", f"{name} 5分钟线为空，准备重试...")
                    await asyncio.sleep(0.4)
            except _NativeException as e:
                minute_err = e
                if attempt < 2:
                    log_terminal("5分钟数据源", f"{name} 5分钟线异常({type(e).__name__})，准备重试...")
                    await asyncio.sleep(0.6)

        if df_5m is None or df_5m.empty:
            if minute_err is not None:
                log_terminal("模式切换", f"{name} 5分钟线失败({type(minute_err).__name__})，尝试切换到30分钟线")
            else:
                log_terminal("模式切换", f"{name} 实时数据为空，尝试切换到30分钟线")
            if df_5m is None or df_5m.empty:
                try:
                    prefetched_30m = await get_30m_kline(full_code, code, df_5m_backup=None)
                except _NativeException as e:
                    log_terminal("30分钟数据源", f"{name} 30分钟线异常：{type(e).__name__}")
                    prefetched_30m = None
                if prefetched_30m is not None and not prefetched_30m.empty:
                    df_5m = prefetched_30m.copy()
                    base_kline_mode = "30分钟"
                    log_terminal("模式切换", f"{name} 已切换到30分钟降级模式")
                else:
                    log_terminal("模式切换", f"{name} 30分钟线也不可用，尝试调取最近交易日日线")
                    try:
                        df_5m = await asyncio.to_thread(ak.stock_zh_a_hist_tx, symbol=full_code, start_date="20260101", adjust="qfq")
                    except _NativeException as e:
                        log_terminal("数据死点", f"{name} 日线回退失败：{type(e).__name__}")
                        return
                    if df_5m is None or df_5m.empty:
                        log_terminal("数据死点", f"无法获取 {name} 的任何行情，跳过")
                        return
                    base_kline_mode = "日线"
                    log_terminal("模式切换", f"{name} 已切换到日线降级模式")

        log_terminal("进度", f">>> [2/5] {name} {base_kline_mode}行情加载成功，计算技术指标...")

        # ==========================
        # ✅ 你的数据处理完全不动
        # ==========================
        df_5m.columns = [c.lower() for c in df_5m.columns]
        column_map = {
            'day': 'datetime', 'date': 'datetime', 'time': 'datetime',
            'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume',
            '时间': 'datetime', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume',
            '成交额': 'amount'
        }
        df_5m = df_5m.rename(columns=column_map)
        if 'volume' not in df_5m.columns:
            if 'amount' in df_5m.columns:
                df_5m['volume'] = pd.to_numeric(df_5m['amount'], errors='coerce').fillna(0.0)
            else:
                df_5m['volume'] = 0.0
        cols_to_fix = ['open', 'high', 'low', 'close', 'volume']
        for col in cols_to_fix:
            if col in df_5m.columns:
                df_5m[col] = pd.to_numeric(df_5m[col], errors='coerce')
        df_5m = df_5m.dropna(subset=['close'])

        # ==========================
        # ✅ 指标计算完全不动
        # ==========================
        df_5m['vol_ma5'] = df_5m['volume'].rolling(window=5).mean()
        curr_vol = float(df_5m['volume'].iloc[-1])
        avg_vol_5 = float(df_5m['vol_ma5'].iloc[-1])
        vol_ratio = curr_vol / avg_vol_5 if avg_vol_5 > 0 else 1.0
        vol_status = "量平"
        if vol_ratio < 0.6:
            vol_status = "缩量止跌"
        elif vol_ratio > 1.5:
            vol_status = "放量反抽"

        # ====================== 【完全安全、永不崩溃】技术指标计算 ======================
        kd_data = df_5m.ta.kdj()

        # 安全取 J 值
        if kd_data is None or kd_data.empty or 'J_9_3' not in kd_data.columns:
            log_terminal("指标异常", f"{name} KDJ 计算失败，跳过")
            return
        j_val = kd_data['J_9_3'].iloc[-1]

        # 安全取 RSI
        rsi_series = df_5m.ta.rsi(length=14)
        if rsi_series is None or rsi_series.empty:
            log_terminal("指标异常", f"{name} RSI 计算失败，跳过")
            return
        rsi = rsi_series.iloc[-1]

        # 安全取布林带
        bb = df_5m.ta.bbands(length=20, std=2)
        if bb is None or bb.empty:
            log_terminal("指标异常", f"{name} 布林带计算失败，跳过")
            return

        bb_cols = bb.columns.tolist()
        lower_band_col = next((c for c in bb_cols if 'BBL' in c), None)
        upper_band_col = next((c for c in bb_cols if 'BBU' in c), None)

        if not lower_band_col or not upper_band_col:
            log_terminal("指标异常", f"{name} 布林带列不存在，跳过")
            return

        # 最后安全取值
        lower_band = float(bb[lower_band_col].iloc[-1])
        upper_band = float(bb[upper_band_col].iloc[-1])
        bb_width = (upper_band - lower_band) / lower_band * 100 if lower_band != 0 else 0
        price = float(df_5m['close'].iloc[-1])

        # 使用带缓存的日线获取（同一只票1小时内不重复拉取）
        df_d = await get_daily_kline(full_code, code)
        if df_d is None or df_d.empty:
            return
        df_d['close'] = pd.to_numeric(df_d['close'], errors='coerce')
        ma20 = float(df_d['close'].tail(20).mean())
        bias_20 = (price - ma20) / ma20 * 100

        # 日线 KDJ 多周期确认（需要至少 27 根日线数据）
        daily_j = None
        daily_rsi_d = None
        if len(df_d) >= 27:
            try:
                df_d_for_ta = df_d.copy()
                df_d_for_ta.columns = [c.lower() for c in df_d_for_ta.columns]
                for _col in ['high', 'low', 'open']:
                    if _col not in df_d_for_ta.columns:
                        df_d_for_ta[_col] = df_d_for_ta['close']
                daily_kdj = df_d_for_ta.ta.kdj()
                if daily_kdj is not None and not daily_kdj.empty and 'J_9_3' in daily_kdj.columns:
                    daily_j = float(daily_kdj['J_9_3'].iloc[-1])
                daily_rsi_s = df_d_for_ta.ta.rsi(length=14)
                if daily_rsi_s is not None and not daily_rsi_s.empty:
                    daily_rsi_d = float(daily_rsi_s.iloc[-1])
            except _NativeException:
                pass
        mtf_score = 0
        mtf_reasons = []
        if daily_j is not None and daily_j < 20:
            mtf_score += 1
            mtf_reasons.append(f"日线J={daily_j:.1f}超卖")
        if daily_rsi_d is not None and daily_rsi_d < 35:
            mtf_score += 1
            mtf_reasons.append(f"日线RSI={daily_rsi_d:.1f}低位")
        if mtf_score == 0:
            mtf_tag = "⚠️仅5分钟信号(日线无确认)"
        elif mtf_score == 1:
            mtf_tag = "弱共振(日线单维确认)"
        else:
            mtf_tag = f"🔥强共振({', '.join(mtf_reasons)})"

        # ==========================
        # ✅ 趋势结构完全不动
        # ==========================
        if len(df_5m) >= 5:
            ma5 = df_5m['close'].rolling(5).mean().iloc[-1]
        else:
            ma5 = df_5m['close'].mean() if not df_5m.empty else price
        if len(df_5m) >= 10:
            ma10 = df_5m['close'].rolling(10).mean().iloc[-1]
        else:
            ma10 = df_5m['close'].mean() if not df_5m.empty else price

        trend_up = (ma5 > ma10) and (price > ma5)
        trend_down = (ma5 < ma10) and (price < ma5)
        vol_rising = False
        if len(df_5m) >= 3:
            vol_rising = (df_5m['volume'].iloc[-1] > df_5m['volume'].iloc[-2] > df_5m['volume'].iloc[-3])
        vol_falling = False
        if len(df_5m) >= 2:
            vol_falling = (df_5m['volume'].iloc[-1] < df_5m['volume'].iloc[-2])

        # ==========================
        # ✅ 缓存不动
        # ==========================
        with cache_lock:
            DASHBOARD_CACHE[code] = {
                'price': price, 'name': name, 'time': time.strftime("%H:%M:%S")
            }

        # ==========================
        # ✅ 仓位、风控、策略完全不动
        # ==========================
        # 先做持仓识别，再根据账户类型计算仓位
        is_held = False
        h_info = None
        acc_type_audit = 'real'
        holdings_real, holdings_sim = get_cached_holdings()
        if code in holdings_real:
            is_held = True
            h_info = holdings_real[code]
            acc_type_audit = 'real'
        elif code in holdings_sim:
            is_held = True
            h_info = holdings_sim[code]
            acc_type_audit = 'sim'

        current_file = SIM_POSITIONS_FILE if acc_type_audit == 'sim' else POSITIONS_FILE
        target_capital = SIM_TOTAL_CAPITAL if acc_type_audit == 'sim' else TOTAL_CAPITAL
        current_spent = get_current_occupancy(current_file)
        current_ratio = current_spent / target_capital

        is_triggered = check_multi_gates(df_5m, j_val, rsi, price, lower_band, vol_ratio)
        if not (is_triggered or is_held):
            if random.random() < 0.15:
                log_terminal("监控", f"{name} | J:{j_val:.1f} RSI:{rsi:.1f} | 未达技术卡口阈值，跳过")
            return

        log_terminal("进度", f">>> [3/5] 🎯 {name} 触发技术卡口！准备策略审计...")

        # 【增强版：统一持仓识别 + 双账户数据融合】
        is_held, h_info, acc_type = get_holdings_info(code)
        
        is_j_trigger = (j_val < 10)
        is_oversold_assist = (rsi < 28) or (price <= lower_band * 1.002)

        # 猎人模式
        hunter_allow = False
        if not is_held:
            hunter_condition1 = (j_val < 12) and (rsi < 33) and (bias_20 < -8)
            hunter_condition2 = (j_val < 20) and (price < lower_band) and vol_falling
            hunter_condition3 = (j_val < 0) and trend_down and (vol_ratio < 0.7)
            hunter_reject = (price > ma20 * 1.12) or (j_val > 60) or (rsi > 65)
            hunter_allow = (hunter_condition1 or hunter_condition2 or hunter_condition3) and not hunter_reject and mtf_score >= 1

        # 管家模式（支持实盘 + 模拟盘）
        keeper_allow = False
        keeper_action = "hold_steady"
        keeper_label = "持仓平稳"
        keeper_reason = "未进入尾仓管理区间"
        pnl_amt = 0.0
        pnl_pct = 0.0
        if is_held and h_info:
            cost = h_info['buy_price']
            vol = h_info['volume']
            pnl_amt = (price - cost) * vol
            pnl_pct = (price - cost) / cost * 100
            dyn_stop_keeper, _ = get_dynamic_stop_loss(cost, pnl_pct)
            keeper_plan = risk_rules.get_keeper_action(
                pnl_pct=pnl_pct,
                vol_ratio=vol_ratio,
                j_val=j_val,
                trend_up=trend_up,
                vol_falling=vol_falling,
                curr_price=price,
                dyn_stop=dyn_stop_keeper,
                keeper_min_retain_ratio=KEEPER_MIN_RETAIN_RATIO,
            )
            keeper_action = keeper_plan.get("action", "hold_steady")
            keeper_label = keeper_plan.get("label", "持仓平稳")
            keeper_reason = keeper_plan.get("reason", "未进入尾仓管理区间")
            keeper_allow = bool(keeper_plan.get("audit_needed"))
        
        # 账户类型标签
        acc_label = "💰 实盘" if acc_type == 'real' else "🧪 模拟盘" if acc_type == 'sim' else ""

        # 30分钟中周期过滤状态（默认值，新仓分支内可能被更新）
        m30m_tag = "未检测（已持仓/未触发）"

        if not is_held:
            # ── 30分钟中周期过滤器（排雷器，仅新仓，不影响keeper） ──
            try:
                df_30m = prefetched_30m if prefetched_30m is not None else await get_30m_kline(
                    full_code, code, df_5m_backup=df_5m if base_kline_mode == "5分钟" else None
                )
                m30m_grade, m30m_reason = check_30m_filter(df_30m)
                m30m_tag = f"{m30m_grade}({m30m_reason})"
                if m30m_grade == 'reject':
                    log_terminal("🚫 30分钟过滤", f"{name} | {m30m_reason}，静默跳过")
                    write_review_log(f"30m过滤 reject {name}: {m30m_reason}")
                    return
                elif m30m_grade == 'watch':
                    log_terminal("⏳ 30分钟观察", f"{name} | {m30m_reason}，继续但降级处理")
            except _NativeException:
                pass  # 安全降级：30分钟过滤失败不影响主流程

            guard_reason = check_new_buy_guard()
            if guard_reason:
                log_terminal("🛡️ 系统熔断", f"{name} | {guard_reason}，拦截新仓审计")
                return

            # ── 硬拦截层：弱市禁新仓（仅自动审计，手动录单不受限） ──
            if m_ratio is not None and m_ratio < 0.4:
                log_terminal("🛡️ 弱市熔断", f"大盘上涨仅 {m_ratio:.1%}，拦截 {name} 的新仓审计")
                return
            # ── 硬拦截层：尾盘 14:20 后不新开仓（自动审计仅限，手动录单不受限） ──
            now_hour_min = datetime.now().hour * 100 + datetime.now().minute
            if now_hour_min >= 1420:
                log_terminal("🛡️ 尾盘熔断", f"当前 {now_hour_min//100}:{now_hour_min%100:02d}，拦截 {name} 的新仓审计")
                return
            # ── 技术门槛收紧：is_j_trigger / is_oversold_assist 也需要 mtf_score >= 1 ──
            if hunter_allow:
                pass  # 猎人条件本身已要求 mtf_score >= 1
            elif (is_j_trigger or is_oversold_assist) and mtf_score >= 1:
                pass  # 5分钟信号 + 日线确认，放行
            else:
                if random.random() < 0.15:
                    log_terminal("猎人筛查", f"{name} | 结构不满足条件（需日线确认），跳过")
                return
        else:
            if not keeper_allow:
                log_terminal(
                    "管家筛查",
                    f"{acc_label} {name} | 盈亏:{pnl_pct:+.1f}% | {keeper_label}：{keeper_reason}",
                )
                return

        if not is_held and current_ratio >= MAX_POSITION_RATIO:
            if random.random() < 0.2:
                log_terminal("🛡️ 仓位熔断", f"总仓位已达 {current_ratio:.1%}, 拦截 {name} 的买入审计。")
            return

        # ==========================
        # ✅ 板块、盘口、策略完全不动
        # ==========================
        sector_name, s_rsi = await asyncio.to_thread(check_sector_resonance, code)
        is_trap = analyze_order_trap(code)[0]
        if s_rsi < 20:
            resonance_tag = "🔥🔥 板块深度超跌"
        elif s_rsi < 35:
            resonance_tag = "🔥 板块超跌(共振反弹潜力)"
        elif s_rsi > 65:
            resonance_tag = "📈 板块强势(无超跌共振)"
        else:
            resonance_tag = "➡️ 板块中性"
        trap_tag = "⚠️ 发现主力撤单诱多" if is_trap else "✅ 盘口稳健"
        # 传入与当前审计账户匹配的持仓数据（供 R1 prompt 使用）
        if is_held and acc_type_audit == 'sim':
            _, holdings_data = get_cached_holdings()
        else:
            holdings_data, _ = get_cached_holdings()
        strategy_context = get_strategy_constraints(code, price, m_ratio, holdings_data)
        log_terminal("进度", f">>> [4/5] 策略注入: {strategy_context} | 调用 Ollama 生成报告...")

        if is_j_trigger:
            log_terminal("🎯 击发", f"{name} J值杀入负数区 ({j_val:.2f})，启动 AI 审计！")
        elif is_held:
            log_terminal("🛡️ 管家", f"持仓标的 {name} 触发风险扫描 (RSI:{rsi:.1f})")

        gemma_model = ai_gateway.resolve_stage_model("screen")
        log_terminal("Gemma 初筛", f"启动 {gemma_model} 评估多维参数...")
        gemma_start_ts = time.time()
        
        gemma_p = audit_steps.build_gemma_prompt(
            name=name,
            code=code,
            price=price,
            j_val=j_val,
            rsi=rsi,
            bias_20=bias_20,
            vol_status=vol_status,
            vol_ratio=vol_ratio,
            m_ratio=m_ratio,
            sector_name=sector_name,
            resonance_tag=resonance_tag,
            trap_tag=trap_tag,
        )
        
        # ==========================
        # 🔹 AI 调用（统一走 AI 网关）
        # ==========================
        g_res = await ai_gateway.ask_ai(
            stage="screen",
            prompt=gemma_p,
            temperature=0.3,
            force_json=False,
            timeout=90.0,
            log_terminal_fn=log_terminal,
            logger_instance=logger,
        )
        gemma_elapsed = time.time() - gemma_start_ts

        gemma_meta = audit_steps.parse_gemma_result(g_res) if g_res else None
        # 缓存 Gemma 评分供止盈推送使用
        if g_res:
            gemma_score = gemma_meta["score"]
            if gemma_score:
                _ai_confidence_cache[code] = _ai_confidence_cache.get(code, {})
                _ai_confidence_cache[code]['gemma_score'] = gemma_score
                _ai_confidence_cache[code]['timestamp'] = time.time()

        if not g_res:
            log_terminal("Gemma 初筛", f"{name} 未返回有效结果 | 耗时{gemma_elapsed:.1f}s")
            log_terminal("初筛失败", f"Gemma 调用超时或失败，{name} 跳过")
            _push_ai_notice(
                "screen",
                code,
                f"{name} 初筛未返回有效结果",
                "本轮直接跳过，请检查 Ollama/Gemma 状态",
            )
            return
        
        gemma_score = gemma_meta["score"]
        decision = gemma_meta["decision"]
        reason = gemma_meta["reason"]
        log_terminal("Gemma 初筛", f"{name} 初筛完成 | 评分:{gemma_score}/40 | 决策:{decision} | 耗时{gemma_elapsed:.1f}s")
        
        if decision not in ["强烈通过", "通过"]:
            log_terminal("初筛拦截", f"Gemma 拦截了 {name} | 评分:{gemma_score}/40 | 决策:{decision} | {reason}")
            return
        
        log_terminal("初筛通过", f"✅ {name} | 评分:{gemma_score}/40 | 决策:{decision} | {reason}")

        log_terminal("AI 审计", f"标的 {name} 触发卡口，正在根据回本策略进行深度审计...")
        news_stage_start = time.time()
        news_data = await asyncio.to_thread(fetch_web_news, code)
        news_elapsed = time.time() - news_stage_start
        log_terminal("AI 审计", f"{name} 舆情已注入 | 摘要:{_clip_log_text(news_data, 40)} | 耗时{news_elapsed:.1f}s")
                
        personal_context = audit_steps.build_personal_context(
            is_held=is_held,
            h_info=h_info,
            acc_type=acc_type,
            pnl_pct=pnl_pct,
            keeper_action=keeper_action,
            keeper_reason=keeper_reason,
            vol_ratio=vol_ratio,
            j_val=j_val,
            price=price,
        )


        total_assets, _, _, _ = get_dashboard_asset_stats()
        holdings, _ = get_cached_holdings()
        # DASHBOARD_CACHE 是 {code: {"price":..., "name":...}} 嵌套结构
        # 展开为 {code: price} 以适配 get_risk_info 和 get_weakest_holding 的 price_map 参数
        price_map = get_price_map()
        risk = get_risk_info(holdings, TOTAL_CAPITAL, price_map)
        weak_code = get_weakest_holding(holdings, risk, price_map, m_ratio)

        # 计算实盘和模拟盘的可用买入空间（基于余额文件的真实数据）
        # 实盘
        real_cash = float(TOTAL_CAPITAL)
        if os.path.exists(REAL_BALANCE_FILE):
            try:
                with open(REAL_BALANCE_FILE, 'r', encoding='utf-8') as f:
                    real_cash = json.load(f).get('cash', float(TOTAL_CAPITAL))
            except Exception:
                pass
        real_spent = sum(h['buy_price'] * h['volume'] for h in holdings.values()) if holdings else 0
        real_ratio = real_spent / TOTAL_CAPITAL if TOTAL_CAPITAL > 0 else 0
        real_can_vol = max(0, min(40, 70 - real_ratio * 100))
        real_can_buy_amt_by_ratio = real_can_vol / 100 * TOTAL_CAPITAL
        real_can_buy_amt = min(real_cash, real_can_buy_amt_by_ratio)  # 取仓位限制和现金余额的较小值

        # 模拟盘
        sim_cash = float(SIM_TOTAL_CAPITAL)
        if os.path.exists(SIM_BALANCE_FILE):
            try:
                with open(SIM_BALANCE_FILE, 'r', encoding='utf-8') as f:
                    sim_cash = json.load(f).get('cash', float(SIM_TOTAL_CAPITAL))
            except Exception:
                pass
        sim_capital = SIM_TOTAL_CAPITAL
        _, sim_holdings = get_cached_holdings()
        sim_spent = sum(h['buy_price'] * h['volume'] for h in sim_holdings.values()) if sim_holdings else 0
        sim_ratio = sim_spent / sim_capital if sim_capital > 0 else 0
        sim_can_vol = max(0, min(40, 70 - sim_ratio * 100))
        sim_can_buy_amt_by_ratio = sim_can_vol / 100 * sim_capital
        sim_can_buy_amt = min(sim_cash, sim_can_buy_amt_by_ratio)  # 取仓位限制和现金余额的较小值

        pyramid_plan_text = ""
        if is_held and h_info:
            market_health_now = get_market_health()
            guard_now = refresh_system_guard()
            current_capital_base = SIM_TOTAL_CAPITAL if acc_type_audit == 'sim' else TOTAL_CAPITAL
            current_cash_available = sim_cash if acc_type_audit == 'sim' else real_cash
            current_can_buy_amt = sim_can_buy_amt if acc_type_audit == 'sim' else real_can_buy_amt
            current_total_ratio_for_add = sim_ratio if acc_type_audit == 'sim' else real_ratio
            current_single_ratio_for_add = ((price * h_info['volume']) / current_capital_base) if current_capital_base > 0 else 0.0
            pyramid_plan = risk_rules.get_pyramid_add_plan(
                pnl_pct=pnl_pct,
                current_volume=h_info['volume'],
                price=price,
                market_health_score=market_health_now.get("score", 0),
                halt_new_buys=guard_now.get("halt_new_buys", False),
                has_open_risk_event=runtime_events.has_open_risk_events(acc_type_audit, code),
                current_single_ratio=current_single_ratio_for_add,
                current_total_ratio=current_total_ratio_for_add,
                cash_available=current_cash_available,
                can_buy_amt=current_can_buy_amt,
                capital_base=current_capital_base,
                allow_pyramid_add=ALLOW_PYRAMID_ADD,
                pyramid_min_pnl=PYRAMID_MIN_PNL,
                pyramid_max_single_ratio=PYRAMID_MAX_SINGLE_RATIO,
                pyramid_max_total_ratio=PYRAMID_MAX_TOTAL_RATIO,
                weak_market_score=WEAK_MARKET_SCORE,
                strong_market_score=STRONG_MARKET_SCORE,
            )
            if pyramid_plan.get("allow_add"):
                pyramid_plan_text = (
                    f"\n📌 【盈利加仓计划】"
                    f"\n- 状态：可考虑盈利加仓（{pyramid_plan['regime_label']}）"
                    f"\n- 建议：最多新增 {pyramid_plan['suggested_shares']}股（约{pyramid_plan['suggested_amount']:.0f}元）"
                    f"\n- 说明：{pyramid_plan['reason']}\n"
                )
                log_terminal(
                    "加仓计划",
                    f"{acc_label} {name} | 可考虑盈利加仓 {pyramid_plan['suggested_shares']}股 | {pyramid_plan['reason']}",
                )
            else:
                pyramid_plan_text = (
                    f"\n📌 【盈利加仓计划】"
                    f"\n- 状态：当前不建议加仓（{pyramid_plan['regime_label']}）"
                    f"\n- 说明：{pyramid_plan['reason']}\n"
                )

        mode_detail = audit_steps.build_mode_detail(
            is_held=is_held,
            acc_label=acc_label,
            j_val=j_val,
            rsi=rsi,
            bias_20=bias_20,
            price=price,
            lower_band=lower_band,
            vol_falling=vol_falling,
            trend_down=trend_down,
            vol_ratio=vol_ratio,
            pnl_pct=pnl_pct,
            trend_up=trend_up,
            ma10=ma10,
            keeper_action=keeper_action,
        )

        weak_info = audit_steps.build_weak_info(
            weak_code=weak_code,
            holdings=holdings,
            dashboard_cache=DASHBOARD_CACHE,
        )


        # 生成交易复盘教训文本（注入 prompt，让 AI 从历史对账中学习）
        lessons_text = ""
        try:
            lessons = get_trade_lessons(limit=5)
            stats_text = get_trade_stats_text()
            lessons_text = audit_steps.build_lessons_text(lessons, stats_text)
        except _NativeException:
            pass

        # 账户健康度（锚定 AI 的激进/保守判断）
        try:
            _, _, _, total_pnl = get_dashboard_asset_stats()
            account_health = audit_steps.build_account_health(
                total_pnl=total_pnl,
                total_capital=TOTAL_CAPITAL,
                target_profit=TARGET_PROFIT,
            )
        except _NativeException:
            account_health = ""


        r1_p = audit_steps.build_r1_prompt(
            name=name,
            code=code,
            price=price,
            mode_detail=mode_detail,
            m_ratio=m_ratio,
            m_vol=m_vol,
            lessons_text=lessons_text,
            account_health=account_health,
            pyramid_plan_text=pyramid_plan_text,
            total_capital=TOTAL_CAPITAL,
            real_ratio=real_ratio,
            real_cash=real_cash,
            real_can_buy_amt=real_can_buy_amt,
            sim_total_capital=SIM_TOTAL_CAPITAL,
            sim_ratio=sim_ratio,
            sim_cash=sim_cash,
            sim_can_buy_amt=sim_can_buy_amt,
            weak_info=weak_info,
            j_val=j_val,
            rsi=rsi,
            lower_band=lower_band,
            upper_band=upper_band,
            bb_width=bb_width,
            bias_20=bias_20,
            vol_ratio=vol_ratio,
            vol_status=vol_status,
            trend_down=trend_down,
            trend_up=trend_up,
            mtf_tag=mtf_tag,
            m30m_tag=m30m_tag,
            sector_name=sector_name,
            s_rsi=s_rsi,
            resonance_tag=resonance_tag,
            is_trap=is_trap,
            news_data=news_data,
            personal_context=personal_context,
            strategy_context=strategy_context,
        )

        # ==========================
        # 🔹 AI 调用（使用 httpx 异步版，超稳定）
        # ==========================
        r1_start_ts = time.time()
        r1_model = ai_gateway.resolve_stage_model("deep_audit")
        log_terminal("深度审计", f"启动 {r1_model} 生成三段式报告...")
        try:
            r1_res = await ai_gateway.ask_ai(
                stage="deep_audit",
                prompt=r1_p,
                temperature=0.3,
                force_json=False,
                timeout=90.0,
                log_terminal_fn=log_terminal,
                logger_instance=logger,
            )
            if r1_res:
                parsed_r1 = audit_steps.parse_r1_result(r1_res)
                decision = parsed_r1["decision"]
                is_buy = parsed_r1["is_buy"]
                is_sell = parsed_r1["is_sell"]
                is_hold = parsed_r1["is_hold"]
                ds_conf = parsed_r1["ds_conf"]
                has_high_conf = parsed_r1["has_high_conf"]
                has_low_conf = parsed_r1["has_low_conf"]
                action_tag = parsed_r1["action_tag"]
                _ai_confidence_cache[code] = _ai_confidence_cache.get(code, {})
                _ai_confidence_cache[code]['ds_confidence'] = ds_conf
                _ai_confidence_cache[code]['timestamp'] = time.time()
                log_terminal("深度审计", f"{name} 深审完成 | 动作:{action_tag} | 置信度:{ds_conf} | 耗时{time.time() - r1_start_ts:.1f}s")
                log_terminal("进度", f">>> [5/5] AI 分析完成")
                should_record_ai = (is_buy or is_sell or is_hold) and not has_low_conf
                should_push_ai = False
                silent_reason = ""

                if should_record_ai:
                    if not is_held:
                        # 猎人模式只推送未持仓标的的买入类信号
                        should_push_ai = is_buy
                        if not should_push_ai:
                            silent_reason = f"猎人模式仅推送未持仓买入信号，当前决策[{action_tag}]仅记录不推送"
                    else:
                        # 管家模式提醒统一走半动态止盈止损 + keeper 尾仓管理
                        silent_reason = "管家模式提醒统一走半动态止盈止损与尾仓管理，AI审计仅记录不推送"
                
                if should_record_ai:
                    is_new_buy = parsed_r1["is_new_buy"]
                    is_add_pos = parsed_r1["is_add_pos"]
                    conf_level = parsed_r1["conf_level"]
                    title = f"📊 {name}({code}) | {action_tag} | {conf_level}"
                    parsed_targets = audit_steps.extract_trade_targets(r1_res)
                    saved_signal_id = None

                    # T+1 拦截：仅当"今日首次推荐买入"→ 拦截同日卖出
                    # 加仓不拦截（已有持仓可随时卖），卖出不拦截（卖后回笼可再买）
                    blocked = False
                    if should_push_ai and is_sell:
                        today = datetime.now().strftime("%Y-%m-%d")
                        if _daily_new_buy.get(code) == today:
                            log_terminal("T+1拦截", f"🚫 {name} 今日已推荐首次买入，T+1限制无法卖出，跳过推送")
                            write_review_log(f"T+1拦截 {name}: 今日首次买入，拒绝卖出")
                            blocked = True

                    if should_push_ai and not blocked:
                        push_decision(title, r1_res, code=code)
                        log_terminal("完成", f"✅ 已推送 {name} ({action_tag}/{conf_level})")
                    elif silent_reason:
                        log_terminal("推送", f"ℹ️ {name} {silent_reason}")

                    # 仅"首次买入"记录（加仓不记录）
                    if should_push_ai and is_new_buy and not is_add_pos:
                        _daily_new_buy[code] = datetime.now().strftime("%Y-%m-%d")

                    # 保存 AI 决策快照（用于交易复盘）
                    try:
                        m_sent = f"{m_ratio:.0%}{'强势' if m_ratio > 0.65 else '弱势' if m_ratio < 0.35 else '中性'}"
                        saved_signal_id = save_ai_decision(
                            code=code, name=name,
                            decision=decision or action_tag,
                            confidence=ds_conf, price=price,
                            j_val=j_val, rsi=rsi, vol_ratio=vol_ratio,
                            bias_20=bias_20, market_sentiment=m_sent,
                            reasoning=r1_res, mode=mode_detail,
                            suggested_vol=parsed_targets["parsed_vol"],
                            real_vol=parsed_targets["parsed_real_vol"],
                            sim_vol=parsed_targets["parsed_sim_vol"],
                            target_stop=parsed_targets["parsed_sl"],
                            target_tp1=parsed_targets["parsed_tp1"],
                            target_tp2=parsed_targets["parsed_tp2"],
                            target_tp3=parsed_targets["parsed_tp3"]
                        )
                    except _NativeException:
                        pass

                    if should_push_ai and is_buy and not blocked and not is_held:
                        watch_features = _build_watch_features(
                            df_5m=df_5m,
                            price=price,
                            ma5=ma5,
                            ma10=ma10,
                            vol_ratio=vol_ratio,
                            vol_rising=vol_rising,
                            m_ratio=m_ratio,
                            s_rsi=s_rsi,
                        )
                        await _run_watch_confirm_execution_chain(
                            code=code,
                            name=name,
                            price=price,
                            features=watch_features,
                            signal_id=saved_signal_id,
                            source_reason=f"{action_tag}/{conf_level}",
                            default_sim_shares=parsed_targets["parsed_sim_vol"] or 100,
                            default_real_shares=parsed_targets["parsed_real_vol"] or 100,
                        )
                else:
                    if has_low_conf:
                        log_terminal("置信度不足", f"😶 {name} 置信度过低，放弃推送")
                    else:
                        log_terminal("静默", f"😶 {name} 观望")
                write_review_log(f"审计报告 {name}: {r1_res}")
            else:
                log_terminal("深度审计", f"{name} 深审未返回有效报告 | 耗时{time.time() - r1_start_ts:.1f}s")
                log_terminal("警告", f"⚠️ {name} AI 返回空")
                _push_ai_notice(
                    "deep_audit",
                    code,
                    f"{name} 深审未返回有效报告",
                    "本轮跳过深审结果，请检查模型链路状态",
                )
        except _NativeException as e:
            log_terminal("深度审计", f"{name} 深审异常 | {type(e).__name__} | 耗时{time.time() - r1_start_ts:.1f}s")
            log_terminal("审计崩溃", f"❌ {name} 异常：{e}")
            _push_ai_notice(
                "deep_audit",
                code,
                f"{name} 深审异常: {type(e).__name__}",
                "本轮跳过深审结果，请检查模型链路状态",
            )
            blacklist_cooldown[code] = time.time() + min(600 * m_vol, 1800)  # 冷却上限 30 分钟

    except _NativeException as e:
        log_terminal("运行错误", f"🔥 {code} 核心链路中断：{type(e).__name__} - {str(e)} | 数据源：{MARKET_SOURCE}")


def _sync_dashboard_from_market(data):
    """从大盘行情数据同步更新 DASHBOARD_CACHE（价格+名称）。
    覆盖范围：STOCKS 监控列表 + 实盘持仓 + 模拟盘持仓。
    """
    if data is None or data.empty:
        return
    # 收集所有需要更新的代码
    codes_to_update = set()
    for s in STOCKS:
        codes_to_update.add(str(s).zfill(6))
    real_h, sim_h = get_cached_holdings()
    codes_to_update.update(real_h.keys())
    codes_to_update.update(sim_h.keys())

    updated = 0
    with cache_lock:
        for code in codes_to_update:
            row = data[data['代码'] == code]
            if not row.empty:
                price_val = float(row['最新价'].values[0])
                name_val = str(row['名称'].values[0])
                DASHBOARD_CACHE[code] = {
                    'price': price_val,
                    'name': name_val,
                    'time': time.strftime("%H:%M:%S")
                }
                updated += 1
    if updated > 0:
        log_terminal("看板同步", f"✅ 已同步 {updated} 只个股现价到看板缓存")


def data_refresher():
    """后台数据刷新线程：活跃窗口内5分钟轮询，非活跃时段30分钟保活"""
    global GLOBAL_MARKET_DATA, consecutive_data_failures, _first_sync_done, MARKET_SOURCE, LAST_CACHE_TIME
    while True:
        try:
            if _is_trade_time_now():
                # 活跃交易窗口：每5分钟刷新一次
                data, source = fetch_robust_spot_data(STOCKS)
                if is_market_data_valid(data, strict=False):
                    if len(data) < 100:
                        log_terminal("数据警告", f"⚠️ 行情样本仅{len(data)}只，大盘情绪计算不可靠")
                    with _market_data_lock:
                        GLOBAL_MARKET_DATA = data
                    MARKET_SOURCE = source
                    LAST_CACHE_TIME = time.time()
                    consecutive_data_failures = 0
                    _first_sync_done = True
                    try:
                        data.to_pickle(MARKET_CACHE_FILE)
                    except Exception:
                        pass
                    _sync_dashboard_from_market(data)
                    log_terminal("缓存同步", f"✅ {source} 更新{len(data)}条并写入磁盘")
                    sleep_time = 300
                else:
                    consecutive_data_failures += 1
                    sleep_time = min(60 * (2 ** min(consecutive_data_failures, 3)), 300)
            else:
                # 非活跃时段：仅当内存缓存为空时尝试补一次，否则30分钟静默
                with _market_data_lock:
                    has_cache = GLOBAL_MARKET_DATA is not None and not GLOBAL_MARKET_DATA.empty
                if has_cache:
                    sleep_time = 1800
                else:
                    data, source = fetch_robust_spot_data(STOCKS)
                    if is_market_data_valid(data, strict=False):
                        with _market_data_lock:
                            GLOBAL_MARKET_DATA = data
                        MARKET_SOURCE = source
                        LAST_CACHE_TIME = time.time()
                        _first_sync_done = True
                        try:
                            data.to_pickle(MARKET_CACHE_FILE)
                        except Exception:
                            pass
                        _sync_dashboard_from_market(data)
                        log_terminal("离线补缓", f"✅ 非活跃时段补充缓存：{source} {len(data)}条")
                    sleep_time = 1800

        except _NativeException as e:
            log_terminal("刷新线程异常", str(e))
            consecutive_data_failures += 1
            sleep_time = min(60 * (2 ** min(consecutive_data_failures, 3)), 300)

        time.sleep(sleep_time)


# 异步定时任务：每日复盘（独立于主循环，不受数据源阻塞影响）
async def daily_review_scheduler():
    """独立协程：每个交易日 15:10 后触发一次复盘，不会被主循环的数据请求阻塞"""
    while True:
        try:
            now = datetime.now()
            # 工作日 15:10~15:59
            if now.hour == 15 and now.minute >= 10 and now.weekday() < 5:
                try:
                    await asyncio.to_thread(generate_daily_review, None, True)
                except BaseException as e:
                    logger.error(f"每日复盘异常：{type(e).__name__} - {e}", exc_info=True)
                # 等到下一个整点再检查，避免重复触发
                await asyncio.sleep(3600)
            else:
                # 每分钟检查一次是否到了触发时间
                await asyncio.sleep(60)
        except BaseException as e:
            logger.error(f"复盘定时器异常：{type(e).__name__} - {e}", exc_info=True)
            await asyncio.sleep(60)


async def remote_command_scheduler():
    """独立协程：每 10 秒轮询远程交易命令"""
    while True:
        try:
            await _poll_remote_commands()
        except BaseException as e:
            logger.warning(f"远程命令调度异常：{type(e).__name__} - {e}")
        await asyncio.sleep(10)


# 异步主循环（简化版：20 分钟轮询大盘数据）
async def async_main_loop():
    consecutive_empty_cycles = 0  # 完全保留你的计数器
    while True:
        try:
            # 1.5 每日自动复盘 → 已移到独立协程 daily_review_scheduler()，不再阻塞主循环

            # 1.6 云端同步 & 心跳（每 3 分钟执行一次，用时间戳防重复触发）
            try:
                now_ts = time.time()
                if not hasattr(async_main_loop, '_last_cloud_sync') or (now_ts - async_main_loop._last_cloud_sync) >= 180:
                    async_main_loop._last_cloud_sync = now_ts
                    await _flush_cloud_sync_now()
            except BaseException as e:
                logger.warning(f"云端同步异常：{type(e).__name__} - {e}")
                title, content = push_templates.build_system_error_message(
                    module="CloudBase",
                    event=f"{type(e).__name__}: {e}",
                    fallback="稍后重试",
                    title="【同步异常】",
                )
                push_decision(title, content, code="sys:cloud_sync")

            # 0. 非交易时间直接待机（避免夜间/周末空跑审计浪费 Ollama 资源）
            if not _is_trade_time_now():
                log_terminal("系统待机", "当前非交易时间，60秒后重试...")
                await asyncio.sleep(60)
                continue

            # 1. 异步检查盈亏（不阻塞）
            try:
                await asyncio.to_thread(check_pnl_alerts)
            except BaseException as e:
                logger.error(f"盈亏检查异常：{type(e).__name__} - {e}", exc_info=True)

            # 2. 获取大盘数据（20 分钟轮询 + 失败容错）
            try:
                m_ratio, m_vol = await asyncio.to_thread(get_market_analysis)
            except BaseException as e:
                logger.error(f"市场分析异常：{type(e).__name__} - {e}", exc_info=True)
                m_ratio, m_vol = None, None

            # 3. 【完全保留你的空盘/非交易时间逻辑】
            if m_ratio is None:
                consecutive_empty_cycles += 1
                if consecutive_empty_cycles >= 10:
                    global GLOBAL_MARKET_DATA
                    GLOBAL_MARKET_DATA = None
                    consecutive_empty_cycles = 0
                log_terminal("系统待机", "当前非交易时间或数据未就绪，60秒后重试...")
                await asyncio.sleep(60)  # 异步休眠
                continue

            # 4. 重置计数器（原版逻辑）
            consecutive_empty_cycles = 0
            audit_universe = get_audit_universe()
            log_terminal("系统", f"🚀 开启新一轮【异步】审计（监控池+持仓，共 {len(audit_universe)} 只）...")

            # 5. 【核心：异步批量审计（超级快）】
            await async_execute_audits(audit_universe, m_ratio, m_vol)

            # 5.5 观察池续扫：wait 保持待观察，每轮根据最新特征重新确认
            try:
                await _scan_pending_watch_pool_execution_chain()
            except BaseException as e:
                logger.error(f"观察池扫描异常：{type(e).__name__} - {e}", exc_info=True)

            # 6. 【保留你的随机休眠逻辑】
            long_sleep = random.randint(60, 120)
            log_terminal("轮询完毕", f"全名单扫描完成，休眠 {long_sleep} 秒")
            await asyncio.sleep(long_sleep)

        # 7.【完全保留你的异常捕获】
        # 【终极修复：使用 BaseException 捕获所有异常，包括非标准异常】
        except BaseException as e:
            logger.error(f"🚨 异步主循环异常：{type(e).__name__} - {e}", exc_info=True)
            await asyncio.sleep(25)


# ==========================================
# 7. 主控入口（最终稳定版 · 自动启动Waitress）
# ==========================================
def start_async_main():
    async def _run_all():
        """同时运行主策略循环、复盘定时器和远程命令轮询"""
        await asyncio.gather(
            async_main_loop(),
            daily_review_scheduler(),
            remote_command_scheduler(),
        )
    asyncio.run(_run_all())


if __name__ == "__main__":
    # --- 1. 初始化系统状态 ---
    startup_status = startup_self_check()
    for item in startup_status.get("messages", []):
        log_terminal("启动自检", ("✅ " if startup_status.get("ok") else "⚠️ ") + item)
    try:
        runtime_state = _load_runtime_event_state()
        log_terminal(
            "运行态恢复",
            f"♻️ 已恢复 risk_events={runtime_state.get('active_risk_events', 0)} | "
            f"provider_states={runtime_state.get('provider_health', 0)}"
        )
    except Exception as e:
        log_terminal("运行态恢复", f"⚠️ 运行态恢复失败: {e}")
    refresh_system_guard()
    log_terminal("系统启动", "Monitor Stable 1.0 已就绪...")
    push_decision("📊 Quant System Online", f"监控已开启\n当前目标: 5万 -> 20万\n监控列表: {len(STOCKS)} 支个股")

    # --- 1.5 首次强制网络同步（主线程阻塞，确保数据就绪后再启动子线程） ---
    log_terminal("启动同步", "🚀 正在从网络获取最新行情...")
    _do_force_sync()
    if not _first_sync_done:
        log_terminal("启动同步", "⚠️ 首次同步失败，将在交易时间内自动重试")
    else:
        # 首次同步成功后，立即同步所有个股价格到看板缓存
        _sync_dashboard_from_market(GLOBAL_MARKET_DATA)

    # --- 1.7 启动时补漏：只补昨天起最近3个工作日，避免把“今天”的盘中快照误当最终复盘 ---
    from datetime import timedelta
    for days_ago in range(1, 4):
        check_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        check_dt = datetime.now() - timedelta(days=days_ago)
        # 只补工作日
        if check_dt.weekday() >= 5:
            continue
        if not _has_daily_review(check_date):
            generate_daily_review(check_date)

    # --- 1.8 启动时：同步本地历史数据到 CloudBase ---
    try:
        asyncio.run(_flush_cloud_sync_now({"event": "startup"}))
        async_main_loop._last_cloud_sync = time.time()
        log_terminal("云端同步", "☁️ 启动数据同步完成")
    except Exception as e:
        log_terminal("云端同步", f"⚠️ 启动同步跳过: {e}")

    # --- 2. 启动数据刷新线程 (后台数据源) ---
    t_refresh = threading.Thread(target=data_refresher, daemon=True)
    t_refresh.start()
    log_terminal("数据引擎", "📡 qt_market_data 行情模块已接管主链路（新浪/东财/腾讯补价）")
    log_terminal("数据引擎", "✅ 后台行情轮询线程已启动")

    # --- 3. 启动【异步主策略循环】（后台运行 · 极速版）---
    t_main = threading.Thread(target=start_async_main, daemon=True)
    t_main.start()
    log_terminal("策略引擎", "✅ 异步审计循环已启动（猎人=监控池，管家=持仓池）")

    # --- 4. ✅ 前台启动 Waitress 服务器 ---
    print("\n" + "=" * 50)
    print("✅ Waitress 启动成功！访问看板：")
    print("🔐 http://127.0.0.1:5000/login")
    print("🔗 http://127.0.0.1:5000/dashboard")
    print("🔗 http://127.0.0.1:5000/buy_ui")
    print("\n🧪 模拟盘专属：")
    print("🔗 http://127.0.0.1:5000/sim-dashboard")
    print("🔗 http://127.0.0.1:5000/sim-buy-ui")
    print("=" * 50 + "\n")

    # 直接在这里启动 Waitress，替代 Flask 自带 run
    from waitress import serve

    serve(app, host="0.0.0.0", port=5000)
