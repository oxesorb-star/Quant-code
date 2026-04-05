import os
import re
import time
import json
import random
import asyncio
import requests
import collections
import html
import pandas as pd
from flask import Flask, request
import threading
import akshare as ak
import pandas_ta
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import shutil
import httpx  # 【新增：现代化异步 HTTP 库，更稳定】

# 【关键修复：保存原生 Exception 引用，防止被异步库覆盖】
import builtins

_NativeException = builtins.Exception

# 【固定：全局唯一基准目录，解决文件找不到大坑】
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 子目录：数据、日志
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
# 实盘持仓文件（绝对路径，永不迷路）
POSITIONS_FILE = os.path.join(DATA_DIR, "positions.json")
# 模拟持仓文件
SIM_POSITIONS_FILE = os.path.join(DATA_DIR, "positions_sim.json")
# 行情缓存
MARKET_CACHE_FILE = os.path.join(DATA_DIR, "market_last_snapshot.pkl")
# 账户余额文件（跟踪已实现盈亏后的实际可用现金）
REAL_BALANCE_FILE = os.path.join(DATA_DIR, "balance_real.json")
SIM_BALANCE_FILE = os.path.join(DATA_DIR, "balance_sim.json")
# 全局缓存：仪表盘实时价格缓存
DASHBOARD_CACHE = {}
# 线程锁：防止多线程同时修改缓存造成数据错乱
cache_lock = threading.Lock()

log_format = '%(asctime)s - %(levelname)s - %(message)s'
date_format = '%Y-%m-%d %H:%M:%S'

file_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, "quant_system.log"),
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
# 【安全：优先从环境变量读取密钥，兼容硬编码兜底】
SCT_KEY = os.getenv("SCT_KEY", "sctp18409toonkxqh5hkznke8cq9uxpx").strip()
if not SCT_KEY:
    logger.warning("⚠️ 未设置环境变量 SCT_KEY，Server酱推送功能将不可用")

# 监控股票池（完全保留你的原代码）
STOCKS = ['000831', '601728', '000807', '601600', '601668', '300015',
          '600031', '002594', '000333', '600900', '601138', '601398',
          '601615', '000938', '600879', '000063', '601607', '600989',
          '600150', '000737', '600030', '002714', '600048', '600905']

# AI 模型配置（双模型协作：Gemma 初筛 + DeepSeek-R1 决策）
MODEL_R1 = "deepseek-r1:8b"  # 深度分析模型（推理速度快，审计任务够用）
MODEL_GEMMA = "gemma3:4b"  # 股票初筛模型
OLLAMA_API = "http://127.0.0.1:11434/api/generate"

# 审计日志目录（完全保留）
REVIEW_DIR = os.path.join(BASE_DIR, "Strategy_Review")
if not os.path.exists(REVIEW_DIR):
    os.makedirs(REVIEW_DIR)
SUMMARY_FILE = os.path.join(REVIEW_DIR, f"audit_{datetime.now().strftime('%Y%m%d')}.txt")

# 交易变更日志（轻量 JSONL，append-only）
TRADE_LOG_FILE = os.path.join(DATA_DIR, "trade_log.jsonl")
_TRADE_LOG_LOCK = threading.Lock()

# AI 决策快照（用于交易复盘 · 自动关联买卖与 AI 推理）
AI_DECISIONS_FILE = os.path.join(DATA_DIR, "ai_decisions.jsonl")
_AI_DECISIONS_LOCK = threading.Lock()
_decision_counter = 0  # 决策唯一编号计数器
# 已关联决策ID集合（append-only，避免全文件重写）
AI_DECISION_LINKS_FILE = os.path.join(DATA_DIR, "ai_decision_links.txt")
_LINKED_IDS_CACHE = None  # None = 尚未从磁盘加载


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


# ==================== 异步 AI 请求核心（httpx 稳定版）====================


# 异步 AI 调用（httpx 稳定版）
async def httpx_ask_ollama(model, prompt, temperature=0.2, force_json=False):
    """使用 httpx 异步调用 Ollama，更稳定，不易崩溃"""
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature, "num_ctx": 4096}
    }

    try:
        # 【关键修复】每次创建新客户端，避免重复关闭问题
        async with httpx.AsyncClient(timeout=90.0, trust_env=False, follow_redirects=True) as client:
            resp = await client.post(OLLAMA_API, json=payload)

            if resp.status_code != 200:
                resp.raise_for_status()

            full_response = resp.json().get('response', '').strip()
            if not full_response:
                log_terminal("AI 异常", "模型返回内容为空")
                return None

            if force_json:
                json_match = re.search(r'(\{.*\}|\[.*\])', full_response, re.DOTALL)
                if json_match:
                    clean_json = json_match.group(0)
                    try:
                        json.loads(clean_json)
                        return clean_json
                    except Exception:
                        log_terminal("AI 格式损坏", "无法解析为标准 JSON")
                        return None
                else:
                    log_terminal("AI 逻辑异常", "未匹配到 JSON 结构")
                    return None

            return full_response

    except httpx.TimeoutException as e:
        log_terminal("AI 超时",
                     f"模型 {model} 在 90s 内未完成推理 | 请求参数：temperature={temperature}, force_json={force_json}")
        logger.error(f"详细超时信息：{str(e)}")
    except httpx.ConnectError as e:
        log_terminal("AI 链路断开", f"无法连接本地 Ollama 服务 (http://127.0.0.1:11434) | 请检查 Ollama 是否运行")
        logger.error(f"详细连接错误：{str(e)}")
    except httpx.HTTPStatusError as e:
        log_terminal("AI HTTP 错误", f"服务器返回错误状态码：{e.response.status_code} | 模型：{model}")
        logger.error(f"详细 HTTP 错误：{str(e)} - 响应内容：{e.response.text[:200]}")
    except json.JSONDecodeError as e:
        log_terminal("AI JSON 解析失败", f"无法解析响应为 JSON | 模型：{model}, force_json={force_json}")
        logger.error(f"JSON 解析错误详情：{str(e)}")
    except Exception as e:
        log_terminal("AI 未知错误", f"调用异常 | 类型：{type(e).__name__} | 模型：{model} | 详情：{str(e)}")
        logger.error(f"完整堆栈跟踪：", exc_info=True)

    return None


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
    """安全读取JSON，彻底解决 Expecting value 报错，兼容多异常场景"""
    # 文件不存在 / 为空文件 → 直接返回空字典
    if not os.path.exists(file_path) or os.path.getsize(file_path) < 2:
        return {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except _NativeException as e:
        # 先备份损坏文件，再重置
        try:
            backup_path = f"{file_path}.bak.{int(time.time())}"
            shutil.copy2(file_path, backup_path)
            log_terminal("JSON 损坏", f"已备份损坏文件 → {os.path.basename(backup_path)}")
        except Exception:
            pass
        log_terminal("JSON 损坏", f"正在重置异常文件 {file_path}: {e}")
        return {}


# ===================== 【仓位风控计算函数】 =====================
def get_risk_info(holdings, total_capital, price_map):
    """
    计算总仓位、单票仓位、可调空间 (优化合并版)
    功能：通过核心引擎计算持仓状态，并根据风控规则返回汇总指标
    """
    # 1. 基础安全检查：空持仓或无效本金直接返回默认安全值
    if not holdings or total_capital <= 0:
        # 默认可调空间为 5.0%
        return {"total": 0, "single": {}, "max_single": 0, "can_vol": 5.0}

    # 2. 直接调用核心计算引擎 (保持逻辑唯一性，避免重复循环)
    # 传入 total_capital 自动在 stats 中生成每个标的的 'weight'
    stats, total_val = _calculate_holding_stats(holdings, price_map, total_capital)

    # 3. 汇总风控指标计算
    # 总仓位占比 (%)
    total_pct = (total_val / total_capital * 100)

    # 提取所有单票权重，计算最大单票仓位 (%)
    single_weights = {k: v['weight'] for k, v in stats.items()}
    max_single = max(single_weights.values()) if single_weights else 0

    # 4. 核心风控逻辑：可开仓空间
    # 规则：单次上限 40.0%，且总仓位受 70.0% 硬限制
    # 使用 max(0, ...) 确保超标时不会返回负数导致下单异常
    can_vol = max(0, min(40, 70 - total_pct))

    # 5. 返回规范化结果：统一对数值进行 round(1) 处理，保持 UI 洁净
    return {
        "total": round(total_pct, 1),
        "single": {k: round(v, 1) for k, v in single_weights.items()},
        "max_single": round(max_single, 1),
        "can_vol": round(can_vol, 1)
    }


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
    if acc_type == 'sim':
        holdings = safe_load_json(SIM_POSITIONS_FILE)
        initial_capital = SIM_TOTAL_CAPITAL
    else:
        holdings = safe_load_json(POSITIONS_FILE)
        initial_capital = TOTAL_CAPITAL

    # 从余额文件读取实际可用现金（包含已实现盈亏）
    balance_file = REAL_BALANCE_FILE if acc_type == 'real' else SIM_BALANCE_FILE

    if os.path.exists(balance_file):
        try:
            with open(balance_file, 'r', encoding='utf-8') as f:
                cash_data = json.load(f)
                cash = cash_data.get('cash', float(initial_capital))
        except Exception:
            cash = float(initial_capital)
    else:
        # 首次运行，无余额文件，现金=初始本金
        cash = float(initial_capital)

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
    """更新账户余额（买入减少，卖出增加），支持已实现盈亏"""
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
        data = {'cash': new_cash, 'initial_capital': initial_capital,
                'last_update': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

        atomic_write_json(balance_file, data)

        log_terminal("余额更新",
                     f"{'💰' if acc_type == 'real' else '🧪'} {acc_type} 余额: {cash:+.2f} -> {new_cash:+.2f} | 原因: {reason}")
        return new_cash
    except Exception as e:
        log_terminal("余额更新", f"❌ 更新失败: {e}")
        return None


def get_price_map():
    """线程安全地从 DASHBOARD_CACHE 提取 {code: price} 映射表"""
    with cache_lock:
        return {c: data.get("price", 0) for c, data in DASHBOARD_CACHE.items()}


def atomic_write_json(filepath, data):
    """原子写入JSON文件（防止写入过程中中断导致文件损坏）"""
    temp_filepath = filepath + '.tmp'
    bak_filepath = filepath + '.bak'
    try:
        # 先写入临时文件，fsync必须在文件句柄打开时调用
        fd = os.open(temp_filepath, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                f.flush()
                os.fsync(fd)
        except Exception:
            os.close(fd)
            raise
    except Exception as e:
        # 如果写入失败，删除临时文件
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except Exception:
                pass
        raise e

    # 原子替换：os.replace() 本身就是原子操作（跨平台安全）
    try:
        # 保留一份备份（可选，失败不影响主逻辑）
        if os.path.exists(filepath):
            try:
                os.replace(filepath, bak_filepath)
            except Exception:
                pass  # 备份失败不影响主流程
        os.replace(temp_filepath, filepath)
    except Exception as e:
        # 清理临时文件
        if os.path.exists(temp_filepath):
            try:
                os.remove(temp_filepath)
            except Exception:
                pass
        raise e


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
_SECTOR_BOARD_CACHE = {}  # {板块名称: 涨跌幅}
_SECTOR_BOARD_CACHE_TIME = 0

# 个股→板块映射缓存（避免每只股票都调用 ak.stock_individual_info_em）
_STOCK_INDUSTRY_CACHE = {}  # {股票代码: 板块名称}


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
    """追加一条交易记录到 JSONL 日志（append-only，无需读取整个文件）"""
    entry = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "action": action,  # "买入" / "全部清仓" / "部分卖出"
        "code": code,
        "name": name,
        "account": acc_type,  # "real" / "sim"
        "price": price,
        "volume": volume,
        "amount": round(price * volume, 2),
        **extra
    }
    with _TRADE_LOG_LOCK:
        with open(TRADE_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    log_terminal("交易记录", f"📝 {entry['time']} | {action} | {name}({code}) | {price}×{volume}")
    # 触发云端同步
    _cloud_sync_add("trade_logs", entry,
                    f"trade_{entry['time']}_{code}_{action}")


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
    records.reverse()  # 最新的排前面
    return records


# ==================== AI 决策快照 & 交易复盘系统 ====================

def save_ai_decision(code, name, decision, confidence, price,
                     j_val, rsi, vol_ratio, bias_20, market_sentiment, reasoning,
                     mode=None, suggested_vol=None, real_vol=None, sim_vol=None,
                     target_stop=None, target_tp1=None, target_tp2=None, target_tp3=None):
    """保存 AI 决策快照，用于后续交易复盘（结构化增强版）"""
    global _decision_counter
    entry = {
        "id": f"D{datetime.now().strftime('%Y%m%d')}-{_decision_counter:04d}-{int(time.time()) % 1000}",
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
    _cloud_sync_add("ai_decisions", entry, entry["id"])
    return entry["id"]


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
            d['_linked'] = d.get("id", "") in linked_ids
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
            if d.get("code") == code and d.get("id") not in _get_linked_ids():
                # 检查时间是否在30分钟内，且决策类型为买入类
                if d.get("decision") not in ("轻仓买入", "买入", "加仓"):
                    continue
                d_time = datetime.strptime(d["time"], "%Y-%m-%d %H:%M")
                if (now - d_time).total_seconds() < 1800:
                    return d["id"]
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
            if d.get("code") == code and d.get("id") not in linked:
                d_time = datetime.strptime(d["time"], "%Y-%m-%d %H:%M")
                if (now - d_time).total_seconds() < 1800:
                    if d.get("decision") in primary_types:
                        return d["id"]
        except (json.JSONDecodeError, ValueError, KeyError):
            continue
    # 第二轮：降级匹配（动作类型放宽，避免因 AI 措辞偏差漏匹配）
    for line in reversed(lines[-100:]):
        try:
            d = json.loads(line.strip())
            if d.get("code") == code and d.get("id") not in linked:
                d_time = datetime.strptime(d["time"], "%Y-%m-%d %H:%M")
                if (now - d_time).total_seconds() < 1800:
                    if d.get("decision") in fallback_types:
                        return d["id"]
        except (json.JSONDecodeError, ValueError, KeyError):
            continue
    return None


def mark_decision_linked(decision_id):
    """标记决策已被实际交易关联（append-only，不重写原文件）"""
    if not decision_id:
        return
    linked = _get_linked_ids()
    if decision_id not in linked:
        linked.add(decision_id)
        with _AI_DECISIONS_LOCK:
            try:
                with open(AI_DECISION_LINKS_FILE, 'a', encoding='utf-8') as f:
                    f.write(decision_id + '\n')
            except Exception:
                pass


def get_trade_lessons(limit=5):
    """
    生成交易复盘教训摘要，注入到后续 AI 审计 prompt 中。
    来源：已关联且已卖出的交易（对账）+ 未被采纳的推荐（回溯）
    """
    lessons = []

    # --- 1. 已完成的交易对账 ---
    trade_records = read_trade_log(limit=50)
    # 收集所有卖出记录
    sells = [r for r in trade_records if r.get("action") in ("全部清仓", "部分卖出")]
    if os.path.exists(AI_DECISIONS_FILE):
        with _AI_DECISIONS_LOCK:
            with open(AI_DECISIONS_FILE, 'r', encoding='utf-8') as f:
                decisions = []
                for line in f.readlines()[-200:]:
                    try:
                        decisions.append(json.loads(line.strip()))
                    except (json.JSONDecodeError, ValueError):
                        continue

        for sell in sells:
            code = sell.get("code")
            pnl_pct = sell.get("pnl_pct", 0)
            for d in reversed(decisions):
                if (d.get("code") == code
                        and d.get("id") in _get_linked_ids()
                        and d.get("decision") in ("轻仓买入", "买入", "加仓", "持有", "减仓", "止损")):
                    tag = "✅" if pnl_pct >= 0 else "❌"
                    outcome = "盈利" if pnl_pct >= 0 else "亏损"
                    lessons.append(
                        f"{tag} [{d['decision']}|J={d.get('j_val', '?')}|RSI={d.get('rsi', '?')}|"
                        f"量比{d.get('vol_ratio', '?')}|大盘{d.get('market_sentiment', '?')}] "
                        f"→ {outcome}{pnl_pct:+.1f}%"
                    )
                    break
            if len(lessons) >= limit:
                break

    # --- 2. 未被采纳的推荐（回溯当天及近3天） ---
    if os.path.exists(AI_DECISIONS_FILE):
        now = datetime.now()
        seen_codes = set()
        for d in reversed(decisions[-50:]):
            if d.get("id") in _get_linked_ids() or d.get("decision") not in ("轻仓买入", "买入"):
                continue
            try:
                d_time = datetime.strptime(d["time"], "%Y-%m-%d %H:%M")
                hours_passed = (now - d_time).total_seconds() / 3600
            except ValueError:
                continue
            if hours_passed < 0 or hours_passed > 72:
                continue
            # 通过实时价格回溯
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
                    if change_pct > 0:
                        lessons.append(
                            f"💡 {d.get('name', d['code'])} {d['code']} | 未采纳 | "
                            f"推荐价{d['price']} → 现价{current_price:.2f} ({change_pct:+.1f}%) | 错失机会"
                        )
                    else:
                        lessons.append(
                            f"✅ {d.get('name', d['code'])} {d['code']} | 未采纳 | "
                            f"推荐价{d['price']} → 现价{current_price:.2f} ({change_pct:+.1f}%) | 幸运躲过"
                        )
            if len(lessons) >= limit + 3:
                break

    return lessons[:limit + 3]


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
_DAILY_REVIEW_DIR = os.path.join(BASE_DIR, "Daily_Review")
if not os.path.exists(_DAILY_REVIEW_DIR):
    os.makedirs(_DAILY_REVIEW_DIR)
_DAILY_REVIEW_FLAG_FILE = os.path.join(_DAILY_REVIEW_DIR, ".daily_review_flags")


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
        with open(_DAILY_REVIEW_FLAG_FILE, 'a', encoding='utf-8') as f:
            f.write(date_str + '\n')
    except Exception:
        pass


def generate_daily_review(report_date=None):
    """生成每日自动复盘报告，写入 Strategy_Review/daily_YYYYMMDD.txt"""
    if report_date is None:
        report_date = datetime.now().strftime('%Y-%m-%d')
    date_str = report_date

    # 防重复
    if _has_daily_review(date_str):
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

        # 3. 错失机会摘要（复用 get_trade_lessons）
        lessons = get_trade_lessons(limit=3)
        missed_text = ""
        if lessons:
            missed_text = '\n'.join(f"  - {l}" for l in lessons[:3])

        # 4. 计算当前持仓盈亏（实盘/模拟盘分开）
        real_holdings, sim_holdings = get_cached_holdings()

        def _get_stock_price(code):
            """三级回退获取股价：缓存 → 共享行情 → 个股信息接口"""
            name = code
            # 1. 缓存
            with cache_lock:
                cached = DASHBOARD_CACHE.get(code, {})
            if cached.get('price', 0) > 0:
                return cached.get('price', 0), cached.get('name', code)
            name = cached.get('name', code)
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
            return 0, name

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
【错失机会】
{missed_text if missed_text else '  无'}

【生成时间】{datetime.now().strftime('%Y-%m-%d %H:%M')}
"""
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)

        _mark_daily_review_done(date_str)
        log_terminal("每日复盘", f"✅ {date_str} 复盘已生成: {report_file}")
        # 上传到云端
        _sync_daily_review_to_cloud(date_str, report)

    except Exception as e:
        log_terminal("每日复盘", f"❌ {date_str} 生成失败: {e}")


# ==================== CloudBase 云端同步层 ====================
# 本地异步同步器：队列 + 重试，不阻塞主交易链路

TCB_ENV_ID = "osuvox-5g8bq6qif05a757f"
TCB_FUNCTION_URL = f"https://{TCB_ENV_ID}.api.tcloudbasegateway.com/v1/functions/quantSync"
# Publishable Key 从控制台获取: https://tcb.cloud.tencent.com/dev?#/identity/token-management
TCB_PUBLISHABLE_KEY = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjlkMWRjMzFlLWI0ZDAtNDQ4Yi1hNzZmLWIwY2M2M2Q4MTQ5OCJ9.eyJpc3MiOiJodHRwczovL29zdXZveC01ZzhicTZxaWYwNWE3NTdmLmFwLXNoYW5naGFpLnRjYi1hcGkudGVuY2VudGNsb3VkYXBpLmNvbSIsInN1YiI6ImFub24iLCJhdWQiOiJvc3V2b3gtNWc4YnE2cWlmMDVhNzU3ZiIsImV4cCI6NDA3ODU1ODI3NiwiaWF0IjoxNzc0ODc1MDc2LCJub25jZSI6IlI4SkFXaUdOUngtTGwzbVd6YzNqZ1EiLCJhdF9oYXNoIjoiUjhKQVdpR05SeC1MbDNtV3pjM2pnUSIsIm5hbWUiOiJBbm9ueW1vdXMiLCJzY29wZSI6ImFub255bW91cyIsInByb2plY3RfaWQiOiJvc3V2b3gtNWc4YnE2cWlmMDVhNzU3ZiIsInVzZXJfdHlwZSI6IiIsImNsaWVudF90eXBlIjoiY2xpZW50X3VzZXIiLCJpc19zeXN0ZW1fYWRtaW4iOmZhbHNlfQ.VXAcCM3Gv4U1OvxhEO14xk1504KaLaDxPSnPVyu6XQmGZcyePPL5ZlVm1Im43ZGvwVn0HJzqv44cGJdfP5NmxMj0ksSm_e9YqZO3W-tStb-OlNw8R8zPkRcYZGl65ryGesdJ7-lycaiybl0H8BUm8Ha3NugaMtDT4R1FrU0c_7BRWdzIleCGZaC-v_kqbfXGikCxg5pdvKb3ofstSFL2wv6DJtcp7AU-8ui2kumdYYshL5nIO7OdZHCUO5N0iNy0hvOyv0EfceoyaB_Mmsy5e5HWdjc7tUb2lZ3yZUujxhqdzAPyYBdikyw0NUgCiSag6JIfwOpmv7ksGxUAa286Yg"

# 同步队列：{(collection, record_id): {"collection": str, "data": dict, "retry": int}}
_sync_queue = {}
_sync_queue_lock = threading.Lock()
_sync_last_trade_line = 0  # 追踪已同步的交易日志行数
_sync_last_decision_line = 0  # 追踪已同步的AI决策行数


def _normalize_cloud_date(date_str):
    """统一云端日期键：YYYYMMDD / YYYY-MM-DD -> YYYY-MM-DD"""
    s = str(date_str or "").strip()
    if not s:
        return ""
    if re.fullmatch(r"\d{8}", s):
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _cloud_sync_add(collection, data, record_id=None):
    """向同步队列添加一条记录（线程安全）"""
    if record_id is None:
        record_id = f"{collection}_{data.get('time', '')}_{data.get('id', data.get('code', ''))}_{time.time()}"
    with _sync_queue_lock:
        if record_id not in _sync_queue:
            _sync_queue[record_id] = {"collection": collection, "data": data, "retry": 0}


async def _cloud_sync_worker():
    """异步同步工作器：通过云函数网关写入数据库"""
    if not TCB_PUBLISHABLE_KEY:
        return

    with _sync_queue_lock:
        if not _sync_queue:
            return
        batch = []
        for rid, item in list(_sync_queue.items()):
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
        with _sync_queue_lock:
            for rid in record_ids:
                _sync_queue.pop(rid, None)

    def _mark_retry(record_ids):
        with _sync_queue_lock:
            for rid in record_ids:
                if rid in _sync_queue:
                    _sync_queue[rid]["retry"] = _sync_queue[rid].get("retry", 0) + 1

    # 按集合分组
    by_collection = {}
    for item in batch:
        col = item["collection"]
        if col not in by_collection:
            by_collection[col] = []
        by_collection[col].append(item)

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            for col, items in by_collection.items():
                records = [item["data"] for item in items]
                record_ids = [item["rid"] for item in items]
                # 根据集合类型选择去重字段
                dedup_fields = {
                    "trade_logs": ["time", "code", "action"],
                    "ai_decisions": ["id"],
                    "daily_reviews": ["date"],
                    "strategy_audits": ["date", "filename"],
                    "account_snapshots": ["snapshot_type", "account", "code"],
                }.get(col, [])

                try:
                    resp = await client.post(
                        TCB_FUNCTION_URL,
                        json={
                            "action": "add",
                            "collection": col,
                            "data": records,
                            "dedupFields": dedup_fields
                        },
                        headers={
                            "Authorization": f"Bearer {TCB_PUBLISHABLE_KEY}",
                            "Content-Type": "application/json"
                        }
                    )
                    if resp.status_code == 200:
                        result = resp.json()
                        if result.get("code") == 0:
                            _mark_done(record_ids)
                            log_terminal("云端同步", f"☁️ {col} +{len(records)} 条")
                        else:
                            _mark_retry(record_ids)
                            logger.warning(f"云端同步失败 [{col}]: {result}")
                    else:
                        _mark_retry(record_ids)
                        logger.warning(f"云端同步HTTP错误 [{col}]: {resp.status_code}")
                except Exception as e:
                    _mark_retry(record_ids)
                    logger.warning(f"云端同步异常 [{col}]: {e}")

    except Exception as e:
        logger.warning(f"云端同步连接异常: {e}")


async def _flush_cloud_sync_now(status_info=None, include_heartbeat=True):
    """立即刷出本地待同步数据，并可选附带一次心跳/账户快照。"""
    await asyncio.to_thread(_sync_pending_data)
    if include_heartbeat:
        if status_info is None:
            await asyncio.to_thread(_sync_heartbeat)
        else:
            await asyncio.to_thread(_sync_heartbeat, status_info)
    await _cloud_sync_worker()


def _sync_pending_data():
    """同步本地尚未上传的数据到云端（启动时和定时调用）"""
    global _sync_last_trade_line, _sync_last_decision_line

    # 1. 同步交易日志
    if os.path.exists(TRADE_LOG_FILE):
        with _TRADE_LOG_LOCK:
            with open(TRADE_LOG_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        new_lines = lines[_sync_last_trade_line:]
        for line in new_lines:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                _cloud_sync_add("trade_logs", record,
                                f"trade_{record.get('time', '')}_{record.get('code', '')}_{record.get('action', '')}")
            except (json.JSONDecodeError, ValueError):
                continue
        _sync_last_trade_line = len(lines)

    # 2. 同步 AI 决策
    if os.path.exists(AI_DECISIONS_FILE):
        with _AI_DECISIONS_LOCK:
            with open(AI_DECISIONS_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        new_lines = lines[_sync_last_decision_line:]
        for line in new_lines:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                _cloud_sync_add("ai_decisions", record, record.get("id"))
            except (json.JSONDecodeError, ValueError):
                continue
        _sync_last_decision_line = len(lines)

    # 3. 同步每日复盘（检查本地文件是否已在云端）
    if os.path.exists(_DAILY_REVIEW_DIR):
        for fname in os.listdir(_DAILY_REVIEW_DIR):
            if fname.startswith("daily_") and fname.endswith(".txt"):
                date_str = fname.replace("daily_", "").replace(".txt", "")
                normalized_date = _normalize_cloud_date(date_str)
                rid = f"review_{normalized_date}"
                # 如果队列里没有这条，重新入队（兜底丢失情况）
                with _sync_queue_lock:
                    if rid not in _sync_queue:
                        try:
                            with open(os.path.join(_DAILY_REVIEW_DIR, fname), 'r', encoding='utf-8') as f:
                                content = f.read()
                            _sync_queue[rid] = {
                                "collection": "daily_reviews",
                                "data": {
                                    "date": normalized_date,
                                    "content": content,
                                    "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                },
                                "retry": 0
                            }
                        except Exception:
                            pass

    # 4. 同步策略审计报告
    if os.path.exists(REVIEW_DIR):
        for fname in os.listdir(REVIEW_DIR):
            if fname.startswith("audit_") and fname.endswith(".txt"):
                date_str = fname.replace("audit_", "").replace(".txt", "")
                normalized_date = _normalize_cloud_date(date_str)
                rid = f"audit_{fname}"
                with _sync_queue_lock:
                    if rid not in _sync_queue:
                        try:
                            with open(os.path.join(REVIEW_DIR, fname), 'r', encoding='utf-8') as f:
                                content = f.read()
                            _sync_queue[rid] = {
                                "collection": "strategy_audits",
                                "data": {
                                    "date": normalized_date,
                                    "filename": fname,
                                    "content": content,
                                    "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                },
                                "retry": 0
                            }
                        except Exception:
                            pass


def _sync_daily_review_to_cloud(date_str, report_text):
    """上传每日复盘到云端"""
    normalized_date = _normalize_cloud_date(date_str)
    _cloud_sync_add("daily_reviews", {
        "date": normalized_date,
        "content": report_text,
        "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }, f"review_{normalized_date}")


def _sync_heartbeat(status_info=None):
    """上报系统心跳 + 当前持仓/余额快照"""
    import platform
    heartbeat = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "hostname": platform.node(),
        "is_trade_time": _is_trade_time_now(),
        "market_source": MARKET_SOURCE,
        "status": "running"
    }
    if status_info:
        heartbeat.update(status_info)
    _cloud_sync_add("system_heartbeat", heartbeat, f"heartbeat_{datetime.now().strftime('%Y%m%d%H%M')}")

    # 同步账户快照（模拟盘 + 实盘）
    for acc_type, capital, pos_file, bal_file in [
        ('sim', SIM_TOTAL_CAPITAL, SIM_POSITIONS_FILE, SIM_BALANCE_FILE),
        ('real', TOTAL_CAPITAL, POSITIONS_FILE, REAL_BALANCE_FILE),
    ]:
        position_count = 0
        position_market_value = 0.0
        # 持仓快照
        if os.path.exists(pos_file):
            try:
                with open(pos_file, 'r', encoding='utf-8') as f:
                    positions = json.load(f)
                position_count = len(positions)
                for code, info in positions.items():
                    buy_price = info.get("buy_price", 0)
                    volume = info.get("volume", 0)
                    # 获取现价+名称
                    cur_price = 0
                    stock_name = code
                    try:
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
                    if cur_price <= 0:
                        cur_price = buy_price
                    pnl = (cur_price - buy_price) * volume
                    pnl_pct = ((cur_price / buy_price) - 1) * 100 if buy_price > 0 else 0
                    position_market_value += round(cur_price * volume, 2)
                    _cloud_sync_add("account_snapshots", {
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
                        "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }, f"pos_{acc_type}_{code}_{datetime.now().strftime('%Y%m%d%H%M')}")
            except Exception:
                pass
        # 余额快照
        if os.path.exists(bal_file):
            try:
                with open(bal_file, 'r', encoding='utf-8') as f:
                    balance = json.load(f)
                cash = balance.get("cash", 0)
                total_capital = balance.get("initial_capital", capital)
                _cloud_sync_add("account_snapshots", {
                    "snapshot_type": "balance",
                    "account": acc_type,
                    "code": "_balance_",
                    "cash": cash,
                    "initial_capital": total_capital,
                    "total_capital": capital,
                    "pnl": round(cash - capital, 2),
                    "pnl_pct": round((cash - capital) / capital * 100, 2) if capital > 0 else 0,
                    "last_update": balance.get("last_update", ""),
                    "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }, f"bal_{acc_type}_{datetime.now().strftime('%Y%m%d%H%M')}")
                _cloud_sync_add("account_snapshots", {
                    "snapshot_type": "position_summary",
                    "account": acc_type,
                    "code": "_summary_",
                    "stock_count": position_count,
                    "cash": cash,
                    "total_capital": total_capital,
                    "position_market_value": round(position_market_value, 2),
                    "cash_ratio": round(cash / total_capital * 100, 2) if total_capital > 0 else 0,
                    "upload_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }, f"summary_{acc_type}_{datetime.now().strftime('%Y%m%d%H%M')}")
            except Exception:
                pass


async def _poll_remote_commands():
    """轮询 CloudBase trade_commands 集合，执行待处理的远程买卖命令"""
    if not TCB_PUBLISHABLE_KEY:
        return
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                TCB_FUNCTION_URL,
                json={"action": "query", "collection": "trade_commands", "query": {"status": "pending"}, "limit": 10,
                      "orderBy": {"field": "created_at", "direction": "asc"}},
                headers={"Authorization": f"Bearer {TCB_PUBLISHABLE_KEY}", "Content-Type": "application/json"}
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
                            await _update_command_status(client, cmd_id, "rejected",
                                                         f"价格偏离现价 {deviation:.1%}，超过2%限制")
                            log_terminal("远程命令", f"❌ 拒绝 {cmd_type} {code}：价格偏离 {deviation:.1%}")
                            continue
                except Exception:
                    pass  # 偏离校验失败不阻止执行

                success, msg = False, ""
                if cmd_type == "buy":
                    success, msg = await asyncio.to_thread(_execute_buy, code, price, volume, acc_type)
                elif cmd_type == "sell":
                    success, msg = await asyncio.to_thread(_execute_sell, code, volume, price, acc_type)
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


async def _update_command_status(client, doc_id, status, result_msg):
    """更新命令状态"""
    try:
        await client.post(
            TCB_FUNCTION_URL,
            json={"action": "update", "collection": "trade_commands", "docId": doc_id,
                  "data": {"status": status, "result": result_msg,
                           "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
            headers={"Authorization": f"Bearer {TCB_PUBLISHABLE_KEY}", "Content-Type": "application/json"}
        )
    except Exception:
        pass


# --- 全局行情数据区 ---
# 全市场行情缓存（主数据）
GLOBAL_MARKET_DATA = None
# 当前行情数据源
MARKET_SOURCE = "None"
# 上次缓存更新时间
LAST_CACHE_TIME = 0
# 实盘盈利目标（回本10000 + 再赚20000 = 30000元）
TARGET_PROFIT = 30000.0

# --- 仓位风控配置 ---
# 实盘总本金
TOTAL_CAPITAL = 50084.0
# 总仓位上限 70%
MAX_POSITION_RATIO = 0.7

# 模拟盘总本金
SIM_TOTAL_CAPITAL = 186727.0
# 模拟盘目标收益
SIM_TARGET_PROFIT = 12734.0

# 行情连续失败计数器（用于健康检查）
consecutive_data_failures = 0
_START_TIME = time.time()

# 日线 K 线缓存（同一只票 1 小时内不重复拉取）
_daily_kline_cache = {}
_DAILY_CACHE_TTL = 3600


async def get_daily_kline(full_code, code):
    """带缓存的日线获取，同一只票1小时内复用"""
    now = time.time()
    if code in _daily_kline_cache:
        ts, df = _daily_kline_cache[code]
        if now - ts < _DAILY_CACHE_TTL:
            return df
    df = await asyncio.to_thread(ak.stock_zh_a_hist_tx, symbol=full_code, start_date="20240101", adjust="qfq")
    if df is not None and not df.empty:
        _daily_kline_cache[code] = (now, df)
    return df


# 30分钟 K 线缓存（同一只票 10 分钟内复用，盘内变化快所以 TTL 较短）
_30m_kline_cache = {}
_30M_CACHE_TTL = 600


async def get_30m_kline(full_code, code, df_5m_backup=None):
    """带缓存的30分钟K线获取，同一只票10分钟内复用。失败返回 None。

    三重回退策略（与5分钟双源对齐）：
      1. ak.stock_zh_a_minute(period='30')        — 新浪实时
      2. ak.stock_zh_a_hist_min_em(period='30')   — 东财历史分钟（盘后可用）
      3. 从 df_5m_backup 合成30分钟（每6根5分钟聚合1根）
    """
    now = time.time()
    if code in _30m_kline_cache:
        ts, df = _30m_kline_cache[code]
        if now - ts < _30M_CACHE_TTL:
            return df

    # 源1：新浪实时分钟数据
    try:
        await asyncio.sleep(random.uniform(0.05, 0.2))
        df_30m = await asyncio.to_thread(ak.stock_zh_a_minute, symbol=full_code, period='30')
        if df_30m is not None and not df_30m.empty:
            _30m_kline_cache[code] = (now, df_30m)
            return df_30m
    except _NativeException:
        pass

    # 源2：东财历史分钟数据（盘后也可用）
    try:
        await asyncio.sleep(random.uniform(0.1, 0.3))
        raw_code = full_code[2:]  # "sh600031" → "600031"
        df_30m = await asyncio.to_thread(
            ak.stock_zh_a_hist_min_em, symbol=raw_code, period='30', adjust=''
        )
        if df_30m is not None and not df_30m.empty:
            _30m_kline_cache[code] = (now, df_30m)
            log_terminal("30分钟数据源", f"{code} 新浪失败，已从东财历史分钟获取")
            return df_30m
    except _NativeException:
        pass

    # 源3：从5分钟K线合成30分钟（A股每30分钟 = 6根5分钟K线）
    if df_5m_backup is not None and len(df_5m_backup) >= 6:
        try:
            df_tmp = df_5m_backup.copy()
            if 'datetime' not in df_tmp.columns:
                for c in ['day', 'date']:
                    if c in df_tmp.columns:
                        df_tmp = df_tmp.rename(columns={c: 'datetime'})
                        break
            agg_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
            if 'volume' in df_tmp.columns:
                agg_dict['volume'] = 'sum'
            df_30m_synthetic = df_tmp.groupby(
                pd.Grouper(key='datetime', freq='30min') if 'datetime' in df_tmp.columns else None,
            ).agg(agg_dict).dropna(subset=['close'])
            if df_30m_synthetic is not None and not df_30m_synthetic.empty:
                df_30m_synthetic = df_30m_synthetic.tail(20)
                _30m_kline_cache[code] = (now, df_30m_synthetic)
                log_terminal("30分钟数据源", f"{code} 新浪+东财均失败，已从5分钟合成")
                return df_30m_synthetic
        except _NativeException:
            pass

    return None


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
    making_new_lows = all(recent[i] < lows[-(len(recent) - i) if i < len(recent) - 1 else -1]
                          for i in range(len(recent) - 1)) if len(recent) >= 2 else False
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
    global GLOBAL_MARKET_DATA, LAST_CACHE_TIME, _first_sync_done, MARKET_SOURCE
    try:
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
            log_terminal("网络同步", f"✅ 首次同步完成：{source} 获取 {len(data)} 条")
            return True
        else:
            log_terminal("同步失败", "网络返回数据格式非法或完全为空")
            return False
    except _NativeException as e:
        log_terminal("网络异常", f"同步过程中发生错误：{e}")
        return False


def is_market_data_valid(df, strict=False):
    """
    弹性行情校验引擎：
    1. 基础校验：非空且包含核心列（代码、最新价）。
    2. 严格校验 (strict=True)：用于大盘情绪计算，需满足 500 只样本。
    3. 宽松校验 (strict=False)：用于休盘期/个股审计，只要有数据就放行。
    """
    # --- 1. 基础物理检查 ---
    if df is None or df.empty:
        return False

    # --- 2. 结构检查 (必须包含核心字段) ---
    required_cols = ['代码', '最新价']
    if not all(col in df.columns for col in required_cols):
        return False

    # --- 3. 数量逻辑检查 ---
    count = len(df)

    if strict:
        # 严格模式：用于判断是否能代表全市场情绪（通常 > 500 只）
        if count < 500:
            return False
    else:
        # 宽松模式：只要有 1 条数据，就允许存入缓存或执行个股审计
        if count < 1:
            return False

    return True


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


# ==============================================================================
# ====================== 你原版五层防御 + 异步新浪 融合最终版 ======================
# ==============================================================================
def normalize_df(df, source):
    """
    统一适配层：把三个数据源的列名统一成标准格式
    标准输出列：代码 / 最新价 / 名称 / 涨跌幅 / 委比
    """
    if df is None or df.empty:
        return df

    df = df.copy()
    cols = df.columns.tolist()

    # ---- 1. 股票代码（正则提取6位数字，兼容各种格式）----
    if '代码' not in cols:
        for c in ['股票代码', 'symbol', 'code', '证券代码']:
            if c in cols:
                extracted = df[c].astype(str).str.extract(r'(\d{6})')
                if extracted is not None and not extracted.empty:
                    df['代码'] = extracted[0]
                break

    # ---- 2. 最新价 ----
    if '最新价' not in cols:
        for c in ['trade', '现价', 'price', '今收盘']:
            if c in cols:
                df['最新价'] = pd.to_numeric(df[c], errors='coerce')
                break
    else:
        df['最新价'] = pd.to_numeric(df['最新价'], errors='coerce')

    # ---- 3. 名称 ----
    if '名称' not in cols:
        for c in ['name', '股票名称', '简称']:
            if c in cols:
                df['名称'] = df[c]
                break

    # ---- 4. 涨跌幅 ----
    if '涨跌幅' not in cols:
        for c in ['change_percent', 'pcnt', 'percent', 'changepercent']:
            if c in cols:
                df['涨跌幅'] = pd.to_numeric(df[c], errors='coerce')
                break
    else:
        df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce')

    # ---- 5. 委比（可选）----
    if '委比' not in cols:
        df['委比'] = 0.0

    # ---- 6. 代码补零（6位）----
    if '代码' in df.columns:
        df['代码'] = df['代码'].astype(str).str.zfill(6)

    # ---- 7. 只保留必要列，去掉 NaN 行 ----
    required = ['代码', '最新价', '名称', '涨跌幅', '委比']
    existing = [c for c in required if c in df.columns]
    df = df[existing].dropna(subset=['代码', '最新价'])

    logger.info(f"[normalize_df] {source} → {len(df)} 条有效数据")
    return df


def fetch_robust_spot_data(monitor_stocks):
    """双层防御行情引擎 (新浪→东财，2026 实战版 - 反爬增强)"""
    monitor_stocks = [str(s).zfill(6) for s in monitor_stocks]

    # 1. 新浪 (主链路 - 全量数据)
    try:
        df_sina = ak.stock_zh_a_spot()
        if df_sina is not None and not df_sina.empty:
            df_sina = normalize_df(df_sina, "Sina")
            if all(c in df_sina.columns for c in ['代码', '最新价', '名称', '涨跌幅']):
                valid_rows = df_sina[df_sina['最新价'] > 0]
                if len(valid_rows) < 100:
                    log_terminal("数据质量", f"⚠️ 新浪数据质量差：仅{len(valid_rows)}条有效价格，降级")
                else:
                    log_terminal("数据源选择", f"✅ 使用新浪 ({len(valid_rows)}只股票)")
                    return df_sina, "Sina"
            else:
                log_terminal("字段缺失", "⚠️ 新浪数据缺少必需字段")
    except Exception as e:
        err_msg = str(e)
        if '<' in err_msg:
            log_terminal("反爬拦截", "⚠️ 新浪触发反爬（返回HTML页面），切换备用数据源")
        else:
            log_terminal("数据源崩溃", f"❌ 新浪异常：{type(e).__name__}")
        logger.error(f"新浪详细错误：{err_msg}")

    time.sleep(random.uniform(0.5, 1.5))

    # 2. 东财 (备选链路)
    try:
        df_em = ak.stock_zh_a_spot_em()
        if df_em is not None and not df_em.empty:
            df_em = normalize_df(df_em, "EastMoney")
            log_terminal("数据源选择", f"✅ 使用东方财富 ({len(df_em)}只股票)")
            return df_em, "EastMoney"
    except Exception as e:
        err_msg = str(e)
        if '<' in err_msg:
            log_terminal("反爬拦截", "⚠️ 东财触发反爬")
        else:
            log_terminal("数据源崩溃", f"❌ 东方财富异常：{type(e).__name__}")
        logger.error(f"东财详细错误：{err_msg}")

    # 新浪+东财全部失效，返回空壳
    log_terminal("数据源耗尽", "❌ 新浪与东方财富均失效，请检查网络")
    return pd.DataFrame(), "FAILED"


def get_shared_market_data():
    """
    看板和主循环调用的统一接口：
    核心逻辑：内存(严格) -> 网络(首次) -> 磁盘(兜底) -> 网络(强制)
    优先走网络确保数据最新，磁盘仅作为网络失败时的兜底
    """
    global GLOBAL_MARKET_DATA, STOCKS, LAST_CACHE_TIME

    # 1. 第一层：内存优先
    #   大盘分析需要 strict(>500条)；个股查询用宽松校验即可
    with _market_data_lock:
        if GLOBAL_MARKET_DATA is not None and is_market_data_valid(GLOBAL_MARKET_DATA, strict=True):
            return GLOBAL_MARKET_DATA

    # 2. 第二层：网络优先（本次运行首次同步成功后缓存内存，后续直接命中第一层）
    log_terminal("同步中", "内存缓存无效，正在从网络获取最新行情...")
    if _do_force_sync():
        return GLOBAL_MARKET_DATA

    # 3. 第三层：磁盘快照兜底（网络失败时使用上次交易日的收盘数据）
    if os.path.exists(MARKET_CACHE_FILE):
        try:
            disk_data = pd.read_pickle(MARKET_CACHE_FILE)
            if is_market_data_valid(disk_data, strict=False):
                with _market_data_lock:
                    GLOBAL_MARKET_DATA = disk_data
                log_terminal("离线兜底", f"网络失败，加载历史快照 ({len(GLOBAL_MARKET_DATA)}条)")
                return GLOBAL_MARKET_DATA
        except _NativeException as e:
            log_terminal("快照损坏", f"无法读取磁盘文件：{e}")

    # 4. 第四层：降级兜底（同步失败但内存有旧数据 → 强行复用）
    if GLOBAL_MARKET_DATA is not None and not GLOBAL_MARKET_DATA.empty:
        log_terminal("缓存降级", "同步失败，强行复用现有内存数据运行")
        return GLOBAL_MARKET_DATA

    # 5. 彻底失败：返回最小空壳，保证程序不崩溃
    log_terminal("严重错误", "全链路无法获取行情，请检查网络设置")
    return pd.DataFrame({
        "代码": [],
        "最新价": [],
        "名称": [],
        "涨跌幅": [],
        "委比": []
    })


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
    _push_cooldown[code] = time.time()

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
    try:
        desp = f"**{summary}**\n\n---\n\n{content}"
        resp = requests.post(url, data={"title": title, "desp": desp}, timeout=15)
        if resp.status_code == 200:
            log_terminal("推送", "消息推送成功")
            return True
    except _NativeException as e:
        log_terminal("推送异常", f"无法连接到 Server 酱服务器：{e}")
    return False


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
    """市场情绪分析：上涨家数占比 + 平均波动率，用于策略风控（读共享缓存，不重复请求网络）"""
    log_terminal("市场环境", "正在从共享缓存评估全 A 股情绪...")

    try:
        # 直接读共享缓存，避免与 data_refresher 线程重复请求网络
        df = get_shared_market_data()

        if df is None or df.empty or '涨跌幅' not in df.columns:
            return None, None

        df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce')
        valid = df['涨跌幅'].dropna()
        if len(valid) < 10:
            return None, None

        up_ratio = (valid > 0).sum() / len(valid)
        avg_vol = valid.abs().mean()

        log_terminal("环境数据", f"上涨占比：{up_ratio:.2%}, 平均波动：{avg_vol:.2f}")
        return float(up_ratio), float(avg_vol)
    except Exception as e:
        log_terminal("链路中断", f"市场分析异常：{e}")
        return None, None


# ========================================================================

def fetch_web_news(code):
    """个股新闻抓取：从东方财富获取前3条重要标题"""
    log_terminal("舆情采集", f"正在抓取 {code} 实时深度资讯...")
    try:
        news_df = ak.stock_news_em(symbol=code)
        if news_df.empty:
            return "暂无重大新闻"
        top_news = news_df['新闻标题'].head(3).tolist()
        return " | ".join(top_news)
    except _NativeException:
        return "网络数据源连接超时"


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


# 根路由：重定向到买入界面
@app.route('/')
def index():
    """主页 - 跳转到实盘交易台"""
    return '''
    <html>
        <head>
            <meta http-equiv="refresh" content="0;url=/buy_ui">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body { font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 50px; text-align: center; }
                h1 { color: #28a745; }
                a { color: #28a745; text-decoration: none; font-size: 24px; }
            </style>
        </head>
        <body>
            <h1>📈 量化交易系统</h1>
            <p>正在跳转...</p>
            <p><a href="/buy_ui">💰 实盘交易台</a> | <a href="/sim-buy-ui">🧪 模拟盘交易台</a></p>
        </body>
    </html>
    '''


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
                action_btn = f'<a href="/buy_page?acc_type={acc_type}&code={code}&price={d.get("price", "")}&vol={d.get("real_vol", d.get("suggested_vol", ""))}" style="color:#00c851;font-size:11px;text-decoration:none;background:#1a2a1a;padding:2px 8px;border-radius:4px;white-space:nowrap;">买入</a>'
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
def buy_interface():
    """实盘交易台"""
    return _build_trade_ui('real')


@app.route('/buy_page')
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


def _execute_buy(code, buy_price, volume, acc_type):
    """核心买入执行逻辑（不依赖 Flask），返回 (success: bool, message: str)"""
    if acc_type not in ['sim', 'real']:
        return False, "账户类型无效"
    if not code:
        return False, "请输入股票代码"
    clean_code = code.strip().zfill(6)
    if not re.match(r'^\d{6}$', clean_code):
        return False, "代码格式无效（需为6位数字）"
    try:
        buy_price_val = float(buy_price)
        if buy_price_val <= 0:
            return False, "买入价格必须大于 0"
    except (ValueError, TypeError):
        return False, "买入价格格式无效"
    try:
        volume_val = int(volume)
        if volume_val <= 0:
            return False, "股数必须大于 0"
        if volume_val % 100 != 0:
            return False, "A股买入必须是 100 股的整数倍"
    except (ValueError, TypeError):
        return False, "股数格式无效"

    risk_msg = precheck_buy_order(acc_type, clean_code, buy_price_val, volume_val)
    if risk_msg:
        return False, f"风控拦截: {risk_msg}"

    target_file = SIM_POSITIONS_FILE if acc_type == 'sim' else POSITIONS_FILE
    data = {}
    if os.path.exists(target_file):
        try:
            with open(target_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, ValueError, IOError):
            data = {}

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

    atomic_write_json(target_file, data)
    buy_amount = buy_price_val * volume_val
    update_balance(acc_type, -buy_amount, f"买入{clean_code} {volume_val}股@{buy_price_val}")

    global _HOLDINGS_CACHE
    with _holdings_cache_lock:
        _HOLDINGS_CACHE['time'] = 0

    with cache_lock:
        stock_name = DASHBOARD_CACHE.get(clean_code, {}).get('name', clean_code)
    append_trade_log("买入", clean_code, stock_name, acc_type, buy_price_val, volume_val)
    try:
        did = link_buy_to_decision(clean_code, buy_price_val)
        if did:
            mark_decision_linked(did)
            log_terminal("决策关联", f"🔗 {stock_name} 已关联 AI 决策 {did}")
    except _NativeException:
        pass

    disk_name = "🧪 模拟盘" if acc_type == 'sim' else "💰 实盘"
    log_terminal("远程买入", f"✅ {disk_name} {stock_name}({clean_code}) {volume_val}股@{buy_price_val}")
    return True, f"{disk_name} 买入成功: {clean_code} {volume_val}股@{buy_price_val}"


def _execute_sell(code, volume, sell_price_input, acc_type):
    """核心卖出执行逻辑（不依赖 Flask），返回 (success: bool, message: str)"""
    if acc_type not in ['sim', 'real']:
        return False, "账户类型无效"
    if not code:
        return False, "请选择要卖出的股票"
    clean_code = code.strip().zfill(6)
    if not re.match(r'^\d{6}$', clean_code):
        return False, "代码格式无效"

    target_file = SIM_POSITIONS_FILE if acc_type == 'sim' else POSITIONS_FILE

    holdings = safe_load_json(target_file)
    if not holdings or clean_code not in holdings:
        return False, f"{clean_code} 不在持仓中"

    info = holdings[clean_code]
    current_volume = info.get('volume', 0)
    buy_price = info.get('buy_price', 0)

    sell_volume = 0
    if volume and str(volume).strip() and str(volume).strip() != '0':
        try:
            sell_volume = int(volume)
            if sell_volume < 0:
                return False, "卖出股数不能为负数"
        except ValueError:
            return False, "股数格式无效"

    is_full_sell = (sell_volume == 0 or sell_volume >= current_volume)
    if not is_full_sell and sell_volume % 100 != 0:
        return False, "A股部分卖出必须是 100 股的整数倍"

    sell_price = buy_price
    if sell_price_input and str(sell_price_input).strip():
        try:
            sell_price = float(sell_price_input)
            if sell_price <= 0:
                return False, "卖出价格必须大于 0"
        except ValueError:
            return False, "卖出价格格式无效"
    else:
        df_spot = get_shared_market_data()
        with cache_lock:
            cached = DASHBOARD_CACHE.get(clean_code, {})
        if cached.get('price', 0) > 0:
            sell_price = cached['price']
        if df_spot is not None and not df_spot.empty:
            row = df_spot[df_spot['代码'] == clean_code]
            if not row.empty:
                sell_price = float(row['最新价'].values[0])

    if is_full_sell:
        del holdings[clean_code]
        sold_vol = current_volume
    else:
        if sell_volume > current_volume:
            return False, f"卖出股数({sell_volume})超过持仓({current_volume})"
        holdings[clean_code]['volume'] = current_volume - sell_volume
        sold_vol = sell_volume

    atomic_write_json(target_file, holdings)
    sell_amount = sell_price * sold_vol
    update_balance(acc_type, sell_amount, f"卖出{clean_code} {sold_vol}股@{sell_price}")

    with _holdings_cache_lock:
        _HOLDINGS_CACHE['time'] = 0

    pnl = (sell_price - buy_price) * sold_vol
    pnl_pct = ((sell_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
    action = "全部清仓" if is_full_sell else f"部分卖出({sold_vol}股)"

    with cache_lock:
        name = DASHBOARD_CACHE.get(clean_code, {}).get('name', clean_code)

    append_trade_log(action, clean_code, name, acc_type, sell_price, sold_vol,
                     buy_price=buy_price, pnl=round(pnl, 2), pnl_pct=round(pnl_pct, 2))
    try:
        did = link_sell_to_decision(clean_code, is_full_sell)
        if did:
            mark_decision_linked(did)
            log_terminal("决策关联", f"🔗 {name} 已关联 AI 卖出决策 {did}")
    except Exception:
        pass

    disk_name = "🧪 模拟盘" if acc_type == 'sim' else "💰 实盘"
    log_terminal("远程卖出", f"✅ {disk_name} {name}({clean_code}) {action} {sold_vol}股@{sell_price} 盈亏:{pnl:+.2f}")
    return True, f"{disk_name} {action}成功: {clean_code} {sold_vol}股@{sell_price} 盈亏:{pnl:+.2f}"


# Web 路由：买入处理
@app.route('/buy')
def buy_stock():
    code = request.args.get('code', '')
    buy_price = request.args.get('buy_price', '')
    volume = request.args.get('volume', '')
    acc_type = request.args.get('acc_type', 'real')

    # 校验账户类型
    if acc_type not in ['sim', 'real']:
        return "<h1>❌ 操作失败：账户类型无效</h1>"

    # 校验代码
    if not code:
        return "<h1>❌ 操作失败：请输入股票代码</h1>"

    clean_code = code.strip().zfill(6)
    if not re.match(r'^\d{6}$', clean_code):
        return "<h1>❌ 操作失败：代码格式无效（需为6位数字）</h1>"

    # 校验价格
    try:
        buy_price_val = float(buy_price)
        if buy_price_val <= 0:
            return "<h1>❌ 操作失败：买入价格必须大于 0</h1>"
    except (ValueError, TypeError):
        return "<h1>❌ 操作失败：买入价格格式无效</h1>"

    # 校验股数
    try:
        volume_val = int(volume)
        if volume_val <= 0:
            return "<h1>❌ 操作失败：股数必须大于 0</h1>"
        if volume_val % 100 != 0:
            return "<h1>❌ 操作失败：A股买入必须是 100 股的整数倍</h1>"
    except (ValueError, TypeError):
        return "<h1>❌ 操作失败：股数格式无效</h1>"

    # 风控预检（交易时段 + 总仓位 + 单票仓位）
    risk_msg = precheck_buy_order(acc_type, clean_code, buy_price_val, volume_val)
    if risk_msg:
        return f"<h1>🛡️ 风控拦截</h1><p>{risk_msg}</p>"

    # 写入持仓文件
    target_file = SIM_POSITIONS_FILE if acc_type == 'sim' else POSITIONS_FILE
    data = {}
    if os.path.exists(target_file):
        try:
            with open(target_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, ValueError, IOError):
            data = {}

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

    atomic_write_json(target_file, data)

    # 更新余额（买入减少现金）
    buy_amount = buy_price_val * volume_val
    update_balance(acc_type, -buy_amount, f"买入{clean_code} {volume_val}股@{buy_price_val}")

    # 清除持仓缓存
    with _holdings_cache_lock:
        _HOLDINGS_CACHE['time'] = 0  # 只重置时间戳，触发下次重新读取

    # 写入交易变更日志
    with cache_lock:
        stock_name = DASHBOARD_CACHE.get(clean_code, {}).get('name', clean_code)
    append_trade_log("买入", clean_code, stock_name, acc_type, buy_price_val, volume_val)
    # 自动关联最近的 AI 决策快照
    try:
        did = link_buy_to_decision(clean_code, buy_price_val)
        if did:
            mark_decision_linked(did)
            log_terminal("决策关联", f"🔗 {stock_name} 已关联 AI 决策 {did}")
    except _NativeException:
        pass

    disk_name = "🧪 模拟盘" if acc_type == 'sim' else "💰 实盘"
    safe_code = html.escape(clean_code)
    back_url = '/sim-buy-ui' if acc_type == 'sim' else '/buy_ui'
    dashboard_url = '/sim-dashboard' if acc_type == 'sim' else '/dashboard'

    return f'''
    <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; padding: 50px; text-align: center; }}
                h1 {{ color: #28a745; }}
                .info {{ background: #2a2a2a; padding: 20px; border-radius: 10px; margin: 20px auto; max-width: 400px; }}
                .btn {{ display: inline-block; padding: 12px 30px; background: #28a745; color: white; text-decoration: none; border-radius: 8px; margin: 10px; }}
            </style>
        </head>
        <body>
            <h1>✅ {disk_name} 买入成功</h1>
            <div class="info">
                <p>股票: {safe_code}</p>
                <p>买入价: ¥{buy_price_val:.2f}</p>
                <p>股数: {volume_val}股</p>
                <p>金额: ¥{buy_price_val * volume_val:.2f}</p>
            </div>
            <a href="{dashboard_url}" class="btn">📊 查看看板</a>
            <a href="{back_url}" class="btn">⬅️ 继续交易</a>
        </body>
    </html>
    <script>setTimeout(function(){{ window.location.href = '{back_url}'; }}, 1500);</script>
    '''


@app.route('/sim-buy-ui')
def sim_buy_ui():
    """模拟盘交易台"""
    return _build_trade_ui('sim')


# Web 路由：卖出表单页面（根据 acc_type 动态展示持仓列表）
@app.route('/sell_page')
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
def sell_stock():
    code = request.args.get('code', '')
    volume = request.args.get('volume', '')
    sell_price_input = request.args.get('sell_price', '')
    acc_type = request.args.get('acc_type', 'real')

    # 校验账户类型
    if acc_type not in ['sim', 'real']:
        return "<h1>❌ 操作失败：账户类型无效</h1>"

    if not code:
        return "<h1>❌ 操作失败：请选择要卖出的股票</h1>"

    clean_code = code.strip().zfill(6)
    if not re.match(r'^\d{6}$', clean_code):
        return "<h1>❌ 操作失败：代码格式无效</h1>"

    target_file = SIM_POSITIONS_FILE if acc_type == 'sim' else POSITIONS_FILE
    disk_name = "🧪 模拟盘" if acc_type == 'sim' else "💰 实战盘"

    # 读取持仓（卖出需要最新数据，仍直读磁盘保证一致性）
    holdings = safe_load_json(target_file)
    if not holdings or clean_code not in holdings:
        return f"<h1>❌ 操作失败：{html.escape(clean_code)} 不在{disk_name}持仓中</h1>"

    info = holdings[clean_code]
    current_volume = info.get('volume', 0)
    buy_price = info.get('buy_price', 0)

    # 解析卖出股数（留空/0 = 全部卖出）
    sell_volume = 0
    if volume and volume.strip() and volume.strip() != '0':
        try:
            sell_volume = int(volume)
            if sell_volume < 0:
                return "<h1>❌ 操作失败：卖出股数不能为负数</h1>"
        except ValueError:
            return "<h1>❌ 操作失败：股数格式无效</h1>"

    is_full_sell = (sell_volume == 0 or sell_volume >= current_volume)

    # 部分卖出时校验 100 股整数倍（全部清仓不受限）
    if not is_full_sell and sell_volume % 100 != 0:
        return "<h1>❌ 操作失败：A股部分卖出必须是 100 股的整数倍（全部清仓不限）</h1>"

    # 获取卖出价格：优先使用用户输入，否则使用实时行情
    sell_price = buy_price  # 兜底用成本价

    # 如果用户输入了卖出价格，使用用户输入的
    if sell_price_input and sell_price_input.strip():
        try:
            sell_price = float(sell_price_input)
            if sell_price <= 0:
                return "<h1>❌ 操作失败：卖出价格必须大于 0</h1>"
        except ValueError:
            return "<h1>❌ 操作失败：卖出价格格式无效</h1>"
    else:
        # 否则尝试获取实时行情价格
        df_spot = get_shared_market_data()
        with cache_lock:
            cached = DASHBOARD_CACHE.get(clean_code, {})
        if cached.get('price', 0) > 0:
            sell_price = cached['price']
        if df_spot is not None and not df_spot.empty:
            row = df_spot[df_spot['代码'] == clean_code]
            if not row.empty:
                sell_price = float(row['最新价'].values[0])

    if is_full_sell:
        # 全部清仓：删除该条目
        del holdings[clean_code]
        sold_vol = current_volume
    else:
        # 部分卖出：减少股数
        if sell_volume > current_volume:
            return f"<h1>❌ 操作失败：卖出股数({sell_volume})超过持仓({current_volume})</h1>"
        holdings[clean_code]['volume'] = current_volume - sell_volume
        sold_vol = sell_volume

    # 写回文件
    atomic_write_json(target_file, holdings)

    # 更新余额（卖出增加现金，包含已实现盈亏）
    sell_amount = sell_price * sold_vol
    update_balance(acc_type, sell_amount, f"卖出{clean_code} {sold_vol}股@{sell_price}")

    # 清除持仓缓存，确保盈亏计算实时更新
    with _holdings_cache_lock:
        _HOLDINGS_CACHE['time'] = 0

    # 计算本次卖出盈亏
    pnl = (sell_price - buy_price) * sold_vol
    pnl_pct = ((sell_price - buy_price) / buy_price * 100) if buy_price > 0 else 0
    sell_amount = sell_price * sold_vol
    action = "全部清仓" if is_full_sell else f"部分卖出({sold_vol}股)"

    # 获取股票名称
    with cache_lock:
        name = DASHBOARD_CACHE.get(clean_code, {}).get('name', clean_code)

    # 写入交易变更日志
    append_trade_log(action, clean_code, name, acc_type, sell_price, sold_vol,
                     buy_price=buy_price, pnl=round(pnl, 2), pnl_pct=round(pnl_pct, 2))

    # 自动关联最近的 AI 卖出类决策（卖出成功后执行）
    try:
        did = link_sell_to_decision(clean_code, is_full_sell)
        if did:
            mark_decision_linked(did)
            log_terminal("决策关联", f"🔗 {name} 已关联 AI 卖出决策 {did}")
    except Exception:
        pass

    pnl_color = '#00c851' if pnl >= 0 else '#ff4444'

    safe_code = html.escape(clean_code)
    safe_name = html.escape(name)
    back_url = '/sim-dashboard' if acc_type == 'sim' else '/dashboard'
    buy_url = '/sim-buy-ui' if acc_type == 'sim' else '/buy_ui'

    return f'''
    <html>
        <head><meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, sans-serif; background: #121212; color: #e0e0e0; text-align: center; padding: 50px 15px; }}
                .result-box {{ background: #1e1e1e; border-radius: 14px; padding: 25px; margin: 20px auto; max-width: 400px; }}
                .pnl {{ font-size: 28px; font-weight: bold; margin: 15px 0; }}
                .detail {{ font-size: 16px; color: #aaa; margin: 8px 0; }}
                a.btn {{ display: block; padding: 15px; background: #333; color: white; text-decoration: none; border-radius: 10px;
                         margin: 10px auto; max-width: 300px; font-weight: bold; border: 1px solid #555; }}
            </style>
        </head>
        <body>
            <h2 style="color: #e53935;">🔻 {disk_name} 卖出成功</h2>
            <div class="result-box">
                <div style="font-size: 22px; font-weight: bold;">{safe_name}({safe_code})</div>
                <div class="detail">操作：{action}</div>
                <div class="detail">买入价格：{buy_price:.2f} 元</div>
                <div class="detail">卖出价格：{sell_price:.2f} 元</div>
                <div class="detail">卖出数量：{sold_vol} 股</div>
                <div class="detail">回收资金：{sell_amount:.2f} 元</div>
                <div class="pnl" style="color:{pnl_color};">
                    {"盈利" if pnl >= 0 else "亏损"} {'+' if pnl >= 0 else ''}{pnl:.2f} 元 ({'+' if pnl_pct >= 0 else ''}{pnl_pct:.1f}%)
                </div>
            </div>
            <a href="{back_url}" class="btn">📊 查看看板</a>
            <a href="{buy_url}" class="btn">⬅️ 返回交易台</a>
        </body>
    </html>
    <script>setTimeout(function(){{ window.location.href = '{buy_url}'; }}, 1500);</script>
    '''


# Web 路由：实盘数据看板
@app.route('/health')
def health_check():
    """健康检查接口，用于外部监控"""
    from flask import jsonify
    return jsonify({
        "status": "ok",
        "uptime_min": round((time.time() - _START_TIME) / 60),
        "last_data_update": datetime.fromtimestamp(LAST_CACHE_TIME).strftime(
            "%H:%M:%S") if LAST_CACHE_TIME else "never",
        "data_failures": consecutive_data_failures,
        "stocks_monitored": len(STOCKS),
        "is_trade_time": _is_trade_time_now()
    })


@app.route('/trade_log')
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
def dashboard():
    """实盘看板"""
    return _build_dashboard('real')


@app.route('/sim-dashboard')
def sim_dashboard():
    """模拟盘看板"""
    return _build_dashboard('sim')


@app.route('/api/decision_targets')
def api_decision_targets():
    """API: 返回某票最近的 AI 决策止损/止盈位（供卖出页JS调用）"""
    from flask import jsonify
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
    from flask import jsonify
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
def api_stock_info():
    """API: 查询股票现价和可买空间（供买入页JS调用）"""
    from flask import jsonify
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
            row = df[df['代码'] == code]
            if not row.empty:
                price = float(row['最新价'].values[0])
                name = str(row['名称'].values[0]) if '名称' in row.columns else name

    # 如果共享行情也没有，单独拉这只股票的日线（非交易时间也能查到最近收盘价）
    if price == 0:
        try:
            full_code = 'sh' + code if code.startswith(('6', '9')) else 'sz' + code
            df_single = ak.stock_zh_a_hist_tx(symbol=full_code, start_date="20240101", adjust="qfq")
            if df_single is not None and not df_single.empty:
                last_row = df_single.iloc[-1]
                price = float(last_row['收盘']) if '收盘' in last_row else float(last_row['收盘价'])
                name = str(last_row.get('名称', code))
                log_terminal("单票查询", f"✅ 从网络获取 {code} 最新价: {price}")
        except Exception as e:
            log_terminal("单票查询失败", f"❌ {code}: {e}")

    if price <= 0:
        return jsonify({"error": f"未找到 {code} 的行情数据，请检查代码是否正确"})

    # 查可用现金
    balance_file = REAL_BALANCE_FILE if acc_type == 'real' else SIM_BALANCE_FILE
    initial_capital = TOTAL_CAPITAL if acc_type == 'real' else SIM_TOTAL_CAPITAL
    cash = float(initial_capital)
    if os.path.exists(balance_file):
        try:
            with open(balance_file, 'r', encoding='utf-8') as f:
                cash = json.load(f).get('cash', cash)
        except Exception:
            pass

    max_vol = cash / price if price > 0 else 0

    # 仓位限制：总仓位70%、单票40%（和 precheck_buy_order 同口径）
    target_file = POSITIONS_FILE if acc_type == 'real' else SIM_POSITIONS_FILE
    total_ratio_now = 0.0
    single_ratio_now = 0.0
    try:
        holdings = safe_load_json(target_file)
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
    """分段上移保护止损：盈利达到各阶段时，止损位阶梯式上移（只升不降）

    阶段设计：
      pnl < 3.5%  → 固定止损 = 成本 × 0.975（-2.5%）
      pnl >= 3.5% → 保本止损 = 成本价（不再亏损）
      pnl >= 5%   → 锁利止损 = 成本 × 1.01（至少 +1%）
      pnl >= 10%  → 强锁止损 = 成本 × 1.03（至少 +3%）

    Args:
        buy_price: 买入成本价
        pnl_pct: 当前浮盈百分比（如 4.2 表示 +4.2%）

    Returns:
        (stop_price, stage_label)
    """
    if pnl_pct >= 10.0:
        return buy_price * 1.03, "强锁(+3%)"
    elif pnl_pct >= 5.0:
        return buy_price * 1.01, "锁利(+1%)"
    elif pnl_pct >= 3.5:
        return buy_price, "保本"
    else:
        return buy_price * 0.975, "固定(-2.5%)"


def check_pnl_alerts():
    """持仓盈亏监控：止盈 & 止损自动推送提醒（带冷却机制）
    同时监控实盘和模拟盘持仓"""
    try:
        real_holdings, sim_holdings = get_cached_holdings()

        if not real_holdings and not sim_holdings:
            log_terminal("盈亏检查", "😶 实盘+模拟盘均空仓，无需检查")
            return

        df_spot = get_shared_market_data()
        if df_spot is None or df_spot.empty:
            log_terminal("盈亏检查", "❌ 行情数据为空，无法计算盈亏")
            return

        # ── 检查实盘持仓 ──
        for code, info in real_holdings.items():
            try:
                buy_price = info['buy_price']
                row = df_spot[df_spot['代码'] == code]
                if row.empty:
                    log_terminal("盈亏检查", f"⚠️ 实盘 {code} 行情数据缺失")
                    continue
                curr_price_series = row['最新价']
                if curr_price_series.empty:
                    log_terminal("盈亏检查", f"⚠️ 实盘 {code} 价格数据缺失")
                    continue
                curr_price = float(curr_price_series.values[0])
                name_series = row['名称']
                name = name_series.values[0] if not name_series.empty else code
                pnl = (curr_price - buy_price) / buy_price * 100

                # ── 动态保护止损位（分段上移，只升不降） ──
                dyn_stop, dyn_label = get_dynamic_stop_loss(buy_price, pnl)

                # ── 止损推送：使用动态止损阈值（含二次提醒机制） ──
                if curr_price <= dyn_stop:
                    last_alert = _alert_cooldown.get(f"stop_real_{code}", 0)
                    if time.time() - last_alert > ALERT_COOLDOWN_SECONDS:
                        msg = (
                            f"🚨 实盘止损提醒（{dyn_label}）！\n"
                            f"标单: {name}({code})\n"
                            f"现价: {curr_price} | 成本: {buy_price}\n"
                            f"盈亏: {pnl:.2f}%\n"
                            f"保护止损位: ¥{dyn_stop:.2f}\n"
                            f"⏱ 建议在下一个整点前执行\n"
                            f"若未执行，30分钟后将二次提醒"
                        )
                        push_decision(f"💰实盘触发止损保护({dyn_label})", msg, code=code, urgent=True)
                        _alert_cooldown[f"stop_real_{code}"] = time.time()
                        _alert_cooldown[f"stop_recheck_real_{code}"] = time.time() + 1800
                    else:
                        log_terminal("盈亏检查",
                                     f"⏳ 实盘 {name} 触发止损({dyn_label})，但冷却中({int((ALERT_COOLDOWN_SECONDS - (time.time() - last_alert)) / 60)}分钟后可再推)")

                    recheck_key = f"stop_recheck_real_{code}"
                    if recheck_key in _alert_cooldown and time.time() > _alert_cooldown[recheck_key]:
                        msg2 = (
                            f"⚠️ 实盘止损未执行二次提醒！\n"
                            f"标单: {name}({code})\n"
                            f"30分钟前已提醒，当前仍跌破保护止损位\n"
                            f"保护止损: ¥{dyn_stop:.2f} | 当前盈亏: {pnl:.2f}%\n"
                            f"请立即执行止损或明确持有理由"
                        )
                        push_decision("⚠️ 实盘止损延迟警告", msg2, code=code, urgent=True)
                        del _alert_cooldown[recheck_key]

                # ── 止盈推送：elif 链从高到低，只触发最高级别 ──
                if pnl >= 10.0:
                    conf = _ai_confidence_cache.get(code, {})
                    gemma_score = conf.get("gemma_score", 0)
                    ds_conf = conf.get("ds_confidence", "")
                    if gemma_score >= 36 and ds_conf == "高":
                        last_alert = _alert_cooldown.get(f"tp3_real_{code}", 0)
                        if time.time() - last_alert > ALERT_COOLDOWN_SECONDS:
                            msg = f"🔥 实盘第三止盈提醒（极高确信）！\n标单: {name}({code})\n现价: {curr_price}\n成本: {buy_price}\n盈亏: {pnl:.2f}%\n🛡️ 保护止损已上移至: ¥{dyn_stop:.2f}({dyn_label})\n📊 DS+Gemma 双模型高置信共识\n⚠️ 建议评估是否继续持有"
                            push_decision("💰实盘触发第三止盈(双模型确认)", msg, code=code, urgent=True)
                            _alert_cooldown[f"tp3_real_{code}"] = time.time()
                        else:
                            log_terminal("盈亏检查", f"⏳ 实盘 {name} 触发tp3({pnl:.1f}%)，但冷却中")
                    else:
                        log_terminal("盈亏检查",
                                     f"📈 实盘 {name} 盈利{pnl:.2f}%超过10%，但AI置信度不足(Gemma:{gemma_score}/40, DS:{ds_conf})，跳过第三止盈推送")
                elif pnl >= 5.0:
                    last_alert = _alert_cooldown.get(f"tp2_real_{code}", 0)
                    if time.time() - last_alert > ALERT_COOLDOWN_SECONDS:
                        msg = f"🏆 实盘第二止盈提醒！\n标单: {name}({code})\n现价: {curr_price}\n成本: {buy_price}\n盈亏: {pnl:.2f}%\n🛡️ 保护止损已上移至: ¥{dyn_stop:.2f}({dyn_label})\n⚠️ 建议大幅减仓或清仓"
                        push_decision("💰实盘触发第二止盈", msg, code=code, urgent=True)
                        _alert_cooldown[f"tp2_real_{code}"] = time.time()
                    else:
                        log_terminal("盈亏检查", f"⏳ 实盘 {name} 触发tp2({pnl:.1f}%)，但冷却中")
                elif pnl >= 3.5:
                    last_alert = _alert_cooldown.get(f"tp1_real_{code}", 0)
                    if time.time() - last_alert > ALERT_COOLDOWN_SECONDS:
                        msg = f"💰 实盘第一止盈提醒！\n标单: {name}({code})\n现价: {curr_price}\n成本: {buy_price}\n盈亏: {pnl:.2f}%\n🛡️ 保护止损已上移至: ¥{dyn_stop:.2f}({dyn_label})\n⚠️ 建议减仓锁定利润"
                        push_decision("💰实盘触发第一止盈", msg, code=code, urgent=True)
                        _alert_cooldown[f"tp1_real_{code}"] = time.time()
                    else:
                        log_terminal("盈亏检查", f"⏳ 实盘 {name} 触发tp1({pnl:.1f}%)，但冷却中")
            except KeyError as e:
                log_terminal("字段缺失", f"❌ 实盘 {code} 持仓信息缺少字段：{str(e)}")
            except ZeroDivisionError as e:
                log_terminal("计算错误", f"❌ 实盘 {code} 除零错误（成本价为 0）: {str(e)}")
            except Exception as e:
                log_terminal("个股计算失败", f"❌ 实盘 {code} 盈亏计算错误：{type(e).__name__} - {str(e)[:100]}")
                logger.error(f"实盘 {code} 详细错误：{str(e)}")

        # ── 检查模拟盘持仓 ──
        for code, info in sim_holdings.items():
            try:
                buy_price = info['buy_price']
                row = df_spot[df_spot['代码'] == code]
                if row.empty:
                    log_terminal("盈亏检查", f"⚠️ 模拟盘 {code} 行情数据缺失")
                    continue
                curr_price_series = row['最新价']
                if curr_price_series.empty:
                    log_terminal("盈亏检查", f"⚠️ 模拟盘 {code} 价格数据缺失")
                    continue
                curr_price = float(curr_price_series.values[0])
                name_series = row['名称']
                name = name_series.values[0] if not name_series.empty else code
                pnl = (curr_price - buy_price) / buy_price * 100

                dyn_stop, dyn_label = get_dynamic_stop_loss(buy_price, pnl)

                # ── 止损推送 ──
                if curr_price <= dyn_stop:
                    last_alert = _alert_cooldown.get(f"stop_sim_{code}", 0)
                    if time.time() - last_alert > ALERT_COOLDOWN_SECONDS:
                        msg = (
                            f"🚨 模拟盘止损提醒（{dyn_label}）！\n"
                            f"标单: {name}({code})\n"
                            f"现价: {curr_price} | 成本: {buy_price}\n"
                            f"盈亏: {pnl:.2f}%\n"
                            f"保护止损位: ¥{dyn_stop:.2f}\n"
                            f"⏱ 建议在下一个整点前执行\n"
                            f"若未执行，30分钟后将二次提醒"
                        )
                        push_decision(f"🧪模拟盘触发止损保护({dyn_label})", msg, code=code, urgent=True)
                        _alert_cooldown[f"stop_sim_{code}"] = time.time()
                        _alert_cooldown[f"stop_recheck_sim_{code}"] = time.time() + 1800
                    else:
                        log_terminal("盈亏检查",
                                     f"⏳ 模拟盘 {name} 触发止损({dyn_label})，但冷却中({int((ALERT_COOLDOWN_SECONDS - (time.time() - last_alert)) / 60)}分钟后可再推)")

                    recheck_key = f"stop_recheck_sim_{code}"
                    if recheck_key in _alert_cooldown and time.time() > _alert_cooldown[recheck_key]:
                        msg2 = (
                            f"⚠️ 模拟盘止损未执行二次提醒！\n"
                            f"标单: {name}({code})\n"
                            f"30分钟前已提醒，当前仍跌破保护止损位\n"
                            f"保护止损: ¥{dyn_stop:.2f} | 当前盈亏: {pnl:.2f}%\n"
                            f"请立即执行止损或明确持有理由"
                        )
                        push_decision("⚠️ 模拟盘止损延迟警告", msg2, code=code, urgent=True)
                        del _alert_cooldown[recheck_key]

                # ── 止盈推送：elif 链从高到低，只触发最高级别 ──
                if pnl >= 10.0:
                    # 第三止盈：10%（需 DS+Gemma 双高置信）
                    conf = _ai_confidence_cache.get(code, {})
                    gemma_score = conf.get("gemma_score", 0)
                    ds_conf = conf.get("ds_confidence", "")
                    if gemma_score >= 36 and ds_conf == "高":
                        last_alert = _alert_cooldown.get(f"tp3_sim_{code}", 0)
                        if time.time() - last_alert > ALERT_COOLDOWN_SECONDS:
                            msg = f"🔥 模拟盘第三止盈提醒（极高确信）！\n标单: {name}({code})\n现价: {curr_price}\n成本: {buy_price}\n盈亏: {pnl:.2f}%\n🛡️ 保护止损已上移至: ¥{dyn_stop:.2f}({dyn_label})\n📊 DS+Gemma 双模型高置信共识\n⚠️ 建议评估是否继续持有"
                            push_decision("🧪模拟盘触发第三止盈(双模型确认)", msg, code=code, urgent=True)
                            _alert_cooldown[f"tp3_sim_{code}"] = time.time()
                        else:
                            log_terminal("盈亏检查", f"⏳ 模拟盘 {name} 触发tp3({pnl:.1f}%)，但冷却中")
                    else:
                        log_terminal("盈亏检查",
                                     f"📈 模拟盘 {name} 盈利{pnl:.2f}%超过10%，但AI置信度不足(Gemma:{gemma_score}/40, DS:{ds_conf})，跳过第三止盈推送")
                elif pnl >= 5.0:
                    # 第二止盈：5%
                    last_alert = _alert_cooldown.get(f"tp2_sim_{code}", 0)
                    if time.time() - last_alert > ALERT_COOLDOWN_SECONDS:
                        msg = f"🏆 模拟盘第二止盈提醒！\n标单: {name}({code})\n现价: {curr_price}\n成本: {buy_price}\n盈亏: {pnl:.2f}%\n🛡️ 保护止损已上移至: ¥{dyn_stop:.2f}({dyn_label})\n⚠️ 建议大幅减仓或清仓"
                        push_decision("🧪模拟盘触发第二止盈", msg, code=code, urgent=True)
                        _alert_cooldown[f"tp2_sim_{code}"] = time.time()
                    else:
                        log_terminal("盈亏检查", f"⏳ 模拟盘 {name} 触发tp2({pnl:.1f}%)，但冷却中")
                elif pnl >= 3.5:
                    # 第一止盈：3.5%
                    last_alert = _alert_cooldown.get(f"tp1_sim_{code}", 0)
                    if time.time() - last_alert > ALERT_COOLDOWN_SECONDS:
                        msg = f"💰 模拟盘止盈提醒！\n标单: {name}({code})\n现价: {curr_price}\n成本: {buy_price}\n盈亏: {pnl:.2f}%\n🛡️ 保护止损已上移至: ¥{dyn_stop:.2f}({dyn_label})\n⚠️ 建议减仓锁定利润"
                        push_decision("🧪模拟盘触发止盈", msg, code=code, urgent=True)
                        _alert_cooldown[f"tp1_sim_{code}"] = time.time()
                    else:
                        log_terminal("盈亏检查", f"⏳ 模拟盘 {name} 触发tp1({pnl:.1f}%)，但冷却中")
            except KeyError as e:
                log_terminal("字段缺失", f"❌ 模拟盘 {code} 持仓信息缺少字段：{str(e)}")
            except ZeroDivisionError as e:
                log_terminal("计算错误", f"❌ 模拟盘 {code} 除零错误（成本价为 0）: {str(e)}")
            except Exception as e:
                log_terminal("个股计算失败", f"❌ 模拟盘 {code} 盈亏计算错误：{type(e).__name__} - {str(e)[:100]}")
                logger.error(f"模拟盘 {code} 详细错误：{str(e)}")

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


def precheck_buy_order(acc_type, code, buy_price_val, volume_val):
    """买入前风控预检：现金余额 + 总仓位70% + 单票40%
    返回 None 表示通过，返回 str 表示拒绝原因
    """
    # 1. 交易时段检查（宽松模式：仅提示不拦截，手动录单允许盘后）
    # 原因：手动录单是补录成交，而非实时下单，应允许盘后、周末操作
    if not _is_trade_time_now():
        # 只记录警告，不拦截
        now = datetime.now()
        logger.warning(
            f"⚠️ 非交易时段录单：{now.strftime('%Y-%m-%d %H:%M:%S')} | {acc_type} | {code} | {volume_val}股@{buy_price_val}")

    # 本次买入金额
    this_amount = buy_price_val * volume_val

    # 2. 现金余额校验（基于余额文件的真实数据）
    balance_file = REAL_BALANCE_FILE if acc_type == 'real' else SIM_BALANCE_FILE
    initial_capital = TOTAL_CAPITAL if acc_type == 'real' else SIM_TOTAL_CAPITAL

    if os.path.exists(balance_file):
        try:
            with open(balance_file, 'r', encoding='utf-8') as f:
                cash_data = json.load(f)
                current_cash = cash_data.get('cash', float(initial_capital))
        except Exception:
            current_cash = float(initial_capital)
    else:
        current_cash = float(initial_capital)

    if current_cash < this_amount:
        return (f"现金不足：当前可用现金 {current_cash:.0f}元，"
                f"本次买入需要 {this_amount:.0f}元（差额 {this_amount - current_cash:.0f}元）")

    # 3. 获取本金和持仓
    capital = SIM_TOTAL_CAPITAL if acc_type == 'sim' else TOTAL_CAPITAL
    target_file = SIM_POSITIONS_FILE if acc_type == 'sim' else POSITIONS_FILE
    current_spent = get_current_occupancy(target_file)

    # 4. 总仓位检查：买入后是否超过70%
    new_total_ratio = (current_spent + this_amount) / capital if capital > 0 else 0
    if new_total_ratio > 0.70:
        return (f"总仓位超限：买入后总仓位将达到 {new_total_ratio:.1%}，"
                f"超过上限 70%（当前已用 {current_spent / capital:.1%}，本次 {this_amount:.0f}元）")

    # 5. 单票仓位检查：买入后该票是否超过40%
    holdings_data = safe_load_json(target_file)
    if code in holdings_data:
        old_cost = holdings_data[code].get('buy_price', 0) * holdings_data[code].get('volume', 0)
        new_single_cost = old_cost + this_amount
    else:
        new_single_cost = this_amount
    single_ratio = new_single_cost / capital if capital > 0 else 0
    if single_ratio > 0.40:
        return (f"单票仓位超限：{code} 买入后占比将达到 {single_ratio:.1%}，"
                f"超过上限 40%（本次 {this_amount:.0f}元）")

    return None


def get_strategy_constraints(code, price, m_ratio, holdings):
    """根据回本进度和环境生成硬性指令"""
    # 环境过滤
    env_tag = "弱市(严禁新仓)" if m_ratio < 0.4 else "强市(可操作)"

    # 回本进度联动 (目标 12734)
    total_pnl = 0
    single_pos_ratio = 0
    if holdings:
        # 计算当前实战盘总盈亏
        df_s = get_shared_market_data()
        for c, info in holdings.items():
            row = df_s[df_s['代码'] == c]
            if not row.empty:
                curr = float(row['最新价'].values[0]) if not row['最新价'].empty else 0.0
                total_pnl += (curr - info['buy_price']) * info['volume']

        # 计算单票持仓占比
        if code in holdings:
            info = holdings[code]  # 明确取 code 对应的持仓，而非循环变量
            cost = info['buy_price'] * info['volume']
            single_pos_ratio = cost / TOTAL_CAPITAL if TOTAL_CAPITAL > 0 else 0

    # 防止除零错误，保护计算
    recovery_rate = (total_pnl / TARGET_PROFIT * 100) if TARGET_PROFIT > 0 else 0
    if recovery_rate < 30:
        strategy = "回本初期: 允许对超跌优质标的小幅补仓"
    elif 30 <= recovery_rate < 70:
        strategy = "稳健期: 严禁加仓，以持有或逢高减仓为主"
    else:
        strategy = "冲刺期: 保护利润，分批止盈"

    # 单票风控 (40%上限)
    risk_tag = "❗超仓预警" if single_pos_ratio > 0.4 else "仓位正常"

    return f"指令:{env_tag} | {strategy} | {risk_tag}({single_pos_ratio:.1%})"


def check_multi_gates(df, j_val, rsi, price, lower_band, vol_ratio):
    """四重卡口检测"""
    g1 = (j_val < 10)  # 极致超跌
    g2 = (rsi < 30) or (price <= lower_band * 1.002)  # 布林/RSI超卖
    g3 = False
    if len(df) >= 2:
        g3 = (vol_ratio > 2.2) and (price > df['close'].iloc[-2])
    g4 = False
    if len(df) >= 5:
        g4 = (price > df['close'].rolling(5).mean().iloc[-1]) and (j_val > -10)
    elif len(df) >= 1:
        g4 = (price > df['close'].iloc[0]) and (j_val > -10)
    return g1 or g2 or g3 or g4


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

        # 使用 asyncio.to_thread 包裹同步 akshare 调用，不阻塞 event loop
        df_5m = await asyncio.to_thread(ak.stock_zh_a_minute, symbol=full_code, period='5')
        if df_5m is None or df_5m.empty:
            log_terminal("模式切换", f"{name} 实时数据为空，尝试调取最后交易日数据")
            df_5m = await asyncio.to_thread(ak.stock_zh_a_hist_tx, symbol=full_code, start_date="20260101",
                                            adjust="qfq")
            if df_5m is None or df_5m.empty:
                log_terminal("数据死点", f"无法获取 {name} 的任何行情，跳过")
                return

        log_terminal("进度", f">>> [2/5] {name} 行情加载成功，计算技术指标...")

        # ==========================
        # ✅ 你的数据处理完全不动
        # ==========================
        df_5m.columns = [c.lower() for c in df_5m.columns]
        column_map = {
            'day': 'datetime', 'date': 'datetime', 'open': 'open', 'high': 'high',
            'low': 'low', 'close': 'close', 'volume': 'volume'
        }
        df_5m = df_5m.rename(columns=column_map)
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
            hunter_allow = (
                                       hunter_condition1 or hunter_condition2 or hunter_condition3) and not hunter_reject and mtf_score >= 1

        # 管家模式（支持实盘 + 模拟盘）
        keeper_allow = False
        pnl_amt = 0.0
        pnl_pct = 0.0
        if is_held and h_info:
            cost = h_info['buy_price']
            vol = h_info['volume']
            pnl_amt = (price - cost) * vol
            pnl_pct = (price - cost) / cost * 100
            keeper_profit = (pnl_pct >= 2.5) and (not trend_up or vol_falling)
            keeper_loss = (pnl_pct <= -2.5)
            keeper_add_ok = (pnl_pct <= -6) and (j_val < 5) and (price < lower_band)
            # 去掉 keeper_hold_ok：正常持仓（不亏且趋势向上）不需要 AI 审计，节省算力
            keeper_allow = keeper_profit or keeper_loss or keeper_add_ok

        # 账户类型标签
        acc_label = "💰 实盘" if acc_type == 'real' else "🧪 模拟盘" if acc_type == 'sim' else ""

        # 30分钟中周期过滤状态（默认值，新仓分支内可能被更新）
        m30m_tag = "未检测（已持仓/未触发）"

        if not is_held:
            # ── 30分钟中周期过滤器（排雷器，仅新仓，不影响keeper） ──
            try:
                df_30m = await get_30m_kline(full_code, code, df_5m_backup=df_5m)
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

            # ── 硬拦截层：弱市禁新仓（仅自动审计，手动录单不受限） ──
            if m_ratio is not None and m_ratio < 0.4:
                log_terminal("🛡️ 弱市熔断", f"大盘上涨仅 {m_ratio:.1%}，拦截 {name} 的新仓审计")
                return
            # ── 硬拦截层：尾盘 14:20 后不新开仓（自动审计仅限，手动录单不受限） ──
            now_hour_min = datetime.now().hour * 100 + datetime.now().minute
            if now_hour_min >= 1420:
                log_terminal("🛡️ 尾盘熔断",
                             f"当前 {now_hour_min // 100}:{now_hour_min % 100:02d}，拦截 {name} 的新仓审计")
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
                log_terminal("管家筛查", f"{acc_label} {name} | 盈亏:{pnl_pct:+.1f}% | 持仓状态平稳，无需AI审计")
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

        log_terminal("Gemma 初筛", f"启动 {MODEL_GEMMA} 评估多维参数...")

        # 【增强版：Gemma 3 多维评估 + 置信度评分】
        gemma_p = f"""
【股票】{name}({code})
【价格】{price:.2f} 元
【技术面】
  - J 值：{j_val:.1f} {'(严重超卖)' if j_val < 10 else '(超卖)' if j_val < 20 else '(中性)'}
  - RSI: {rsi:.1f} {'(超卖区)' if rsi < 30 else '(弱势区)' if rsi < 45 else '(中性区)'}
  - MA20 偏离：{bias_20:.2f}% {'(深度超跌)' if bias_20 < -10 else '(超跌)' if bias_20 < -5 else '(正常)'}
  - 量能：{vol_status} (量比{vol_ratio:.1f})
【环境】
  - 大盘：上涨占比{m_ratio:.1%} {'(强势)' if m_ratio > 0.65 else '(弱势)' if m_ratio < 0.35 else '(中性)'}
  - 板块：{sector_name} {resonance_tag}
  - 盘口：{trap_tag}
【任务】
请从以下 4 个维度评分（每项 0-10 分）：
1. 超跌程度：J 值、RSI、乖离率的综合超卖严重性
2. 反弹潜力：趋势结构、量价配合、板块共振
3. 安全边际：是否有主力诱多、大盘环境配合度
4. 时机成熟度：是否立即具备反抽条件

【输出格式】
超跌评分：X/10
潜力评分：X/10
安全评分：X/10
时机评分：X/10
综合评分：X/40
决策：【强烈通过】|【通过】|【观望】|【拒绝】
理由：一句话说明核心逻辑（20 字以内）
"""

        # ==========================
        # 🔹 AI 调用（使用 httpx 异步版，超稳定）
        # ==========================
        g_res = await httpx_ask_ollama(MODEL_GEMMA, gemma_p, temperature=0.3)

        # 缓存 Gemma 评分供止盈推送使用
        if g_res:
            score_match = re.search(r'综合评分[：:]\s*(\d+)/40', g_res)
            if score_match:
                gemma_score = int(score_match.group(1))
                _ai_confidence_cache[code] = _ai_confidence_cache.get(code, {})
                _ai_confidence_cache[code]['gemma_score'] = gemma_score
                _ai_confidence_cache[code]['timestamp'] = time.time()

        if not g_res:
            log_terminal("初筛失败", f"Gemma 调用超时或失败，{name} 跳过")
            return

        # 提取评分和决策
        score_match = re.search(r'综合评分[：:]\s*(\d+)/40', g_res)
        decision_match = re.search(r'决策[：:]\s*【?(强烈通过|通过|观望|拒绝)】?', g_res)
        reason_match = re.search(r'理由[：:]\s*(.+?)(?:\n|$)', g_res)

        gemma_score = int(score_match.group(1)) if score_match else 0
        decision = decision_match.group(1) if decision_match else "未知"
        reason = reason_match.group(1).strip() if reason_match else "无理由"

        if decision not in ["强烈通过", "通过"]:
            log_terminal("初筛拦截", f"Gemma 拦截了 {name} | 评分:{gemma_score}/40 | 决策:{decision} | {reason}")
            return

        log_terminal("初筛通过", f"✅ {name} | 评分:{gemma_score}/40 | 决策:{decision} | {reason}")

        log_terminal("AI 审计", f"标的 {name} 触发卡口，正在根据回本策略进行深度审计...")
        news_data = await asyncio.to_thread(fetch_web_news, code)

        # 【增强版：双账户持仓信息融合】
        if is_held and h_info:
            acc_name = "实盘" if acc_type == 'real' else "模拟盘"
            personal_context = f"[个人实战数据]:\n- 持仓状态：已入场 ({acc_name})\n- 成本:{h_info['buy_price']:.3f}\n- 股数:{h_info['volume']}\n- 盈亏:{pnl_pct:.2f}%"
            # ── tp1/tp2 后轻量追撤判断（分级） ──
            if keeper_profit:
                vol_label = "放量" if vol_ratio > 1.3 else "缩量" if vol_ratio < 0.8 else "平量"
                j_trend = "上行" if j_val > 50 else "拐头向下" if j_val < 30 else "中性"
                day_change = (price - h_info['buy_price']) / h_info['buy_price'] * 100
                stagnation = vol_ratio > 1.0 and day_change < 1.0 and pnl_pct > 2.5
                stag_tag = "⚠️放量滞涨(疑似出货)" if stagnation else ""

                if pnl_pct >= 5.0:
                    # tp2：更保守，除非极强否则应离场
                    personal_context += (
                        f"\n\n【tp2止盈后追撤判断】(当前盈利{pnl_pct:.1f}%，已过第二止盈线，获利丰厚)"
                        f"\n- 量能：{vol_label}(量比{vol_ratio:.1f}) | J值方向：{j_trend}(J={j_val:.1f}) {stag_tag}"
                        f"\n- 已过第二止盈线，建议偏向落袋。请根据以下规则给出追撤建议（必须三选一，写进决策理由）："
                        f"\n  · 继续拿：量比>1.5 且 J>60 且无滞涨（极强趋势才值得拿）"
                        f"\n  · 减半：量比>1.0 但<1.5 或 J在30~60（动能边际减弱，先锁大部分利润）"
                        f"\n  · 清仓：量比<1.0 或 J<30 或 放量滞涨 或 趋势转空（风险大于收益，全部离场）"
                    )
                else:
                    # tp1：标准判断
                    personal_context += (
                        f"\n\n【tp1止盈后追撤判断】(当前盈利{pnl_pct:.1f}%，已过第一止盈线)"
                        f"\n- 量能：{vol_label}(量比{vol_ratio:.1f}) | J值方向：{j_trend}(J={j_val:.1f}) {stag_tag}"
                        f"\n- 请根据以下规则给出追撤建议（必须三选一，写进决策理由）："
                        f"\n  · 继续拿：量比>1.3 且 J>50（动能仍在，可追第二止盈）"
                        f"\n  · 减半：量比<0.8 或 J拐头向下（动能衰竭，先锁一半利润）"
                        f"\n  · 清仓：放量滞涨 或 趋势由多转空（主力出货风险，全部离场）"
                    )
        else:
            personal_context = "[个人实战数据]: 未持仓，处于观察期"

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

        # 猎人/管家模式详细状态
        if not is_held:
            mode_detail = "猎人模式"
            hunter_tags = []
            if (j_val < 12) and (rsi < 33) and (bias_20 < -8):
                hunter_tags.append("三低共振")
            if (j_val < 20) and (price < lower_band) and vol_falling:
                hunter_tags.append("跌穿布林+缩量")
            if (j_val < 0) and trend_down and (vol_ratio < 0.7):
                hunter_tags.append("J负数+极缩量")
            mode_detail += f" | 触发:{'+'.join(hunter_tags) if hunter_tags else '常规'}"
        else:
            keeper_actions = []
            if (pnl_pct >= 2.5) and (not trend_up or vol_falling):
                keeper_actions.append("减仓止盈")
            if (pnl_pct <= -4.0) and (price < ma10) and (j_val > 30):
                keeper_actions.append("止损离场")
            if (pnl_pct > -2.5) and trend_up and (j_val < 45):
                keeper_actions.append("持有待涨")
            if (pnl_pct <= -6) and (j_val < 5) and (price < lower_band):
                keeper_actions.append("加仓摊薄")
            mode_detail = f"管家模式({acc_label}) | 触发:{'+'.join(keeper_actions) if keeper_actions else '常规扫描'}"

        # 最弱持仓信息
        weak_info = ""
        if weak_code and weak_code in holdings:
            wh = holdings[weak_code]
            wn = DASHBOARD_CACHE.get(weak_code, {}).get('name', weak_code)
            weak_info = f"最弱持仓:{wn}({weak_code}) 成本{wh['buy_price']:.2f} 股数{wh['volume']}"

        # 生成交易复盘教训文本（注入 prompt，让 AI 从历史对账中学习）
        lessons_text = ""
        try:
            lessons = get_trade_lessons(limit=5)
            if lessons:
                lessons_text = "\n📌 【系统近期交易复盘】\n" + "\n".join(lessons) + "\n（请参考以上真实交易结果调整当前判断）\n"
            stats_text = get_trade_stats_text()
            if stats_text:
                lessons_text += "\n" + stats_text
        except _NativeException:
            pass

        # 账户健康度（锚定 AI 的激进/保守判断）
        try:
            _, _, _, total_pnl = get_dashboard_asset_stats()
            pnl_pct = (total_pnl / TOTAL_CAPITAL * 100) if TOTAL_CAPITAL > 0 else 0
            recovery = min(max(total_pnl / TARGET_PROFIT * 100, 0), 100) if TARGET_PROFIT > 0 else 0
            if recovery >= 70:
                risk_mode = "保守——接近目标，优先保护利润"
            elif total_pnl < 0:
                risk_mode = "激进——仍需追回，可承受适度风险"
            else:
                risk_mode = "均衡"
            account_health = (
                f"\n📌 【账户健康度】"
                f"\n- 实盘总浮盈亏：{total_pnl:+.0f}元（{pnl_pct:+.1f}%）"
                f"\n- 回本进度：{recovery:.0f}%（目标{TARGET_PROFIT:.0f}元）"
                f"\n- 当前风险承受：{risk_mode}\n"
            )
        except _NativeException:
            account_health = ""

        r1_p = f"""
你是一位专业的A股量化交易分析师。请严格按照以下【三段式】格式输出，不要遗漏任何段落，不要添加额外内容。

═══════════════════════════
【一、核心结论】
═══════════════════════════

📌 {name}({code}) | 现价 {price:.2f} 元
📌 模式：{mode_detail}
📌 大盘：上涨{m_ratio:.1%} {'强势' if m_ratio > 0.65 else '弱势' if m_ratio < 0.35 else '中性'} | 波动{m_vol:.2f}%
{lessons_text}{account_health}

【操作指令】（从以下选择一项：轻仓买入 / 加仓 / 持有 / 减仓 / 止损 / 换股 / 观望）
决策：[填写决策]
置信度：[高/中/低]

【仓位计算】（严格基于以下可用资金计算具体买入股数，必须是100的整数倍）
- 实盘本金：{TOTAL_CAPITAL:.0f}元 | 已用仓位：{real_ratio * 100:.1f}% | 可用现金：{real_cash:.0f}元 | 可买空间：{real_can_buy_amt:.0f}元
- 模拟盘本金：{SIM_TOTAL_CAPITAL:.0f}元 | 已用仓位：{sim_ratio * 100:.1f}% | 可用现金：{sim_cash:.0f}元 | 可买空间：{sim_can_buy_amt:.0f}元

如果决策是买入/加仓，请填写：
实盘建议：买入[XXX]股（约[XXXXX]元，占实盘[X]%）
模拟盘建议：买入[XXX]股（约[XXXXX]元，占模拟盘[X]%）

如果决策是减仓/止损，请填写：
实盘建议：卖出[XXX]股
模拟盘建议：卖出[XXX]股

【止损止盈】（必须严格遵循系统风控规则，不可随意填写）
⚠️ 本系统为超跌反弹策略，止损止盈必须紧凑：
- 止损位：现价×0.975（固定亏损-2.5%，抄底失败立即离场）
- 第一止盈：现价×1.035（盈利+3.5%，建议减仓锁定利润）
- 第二止盈：现价×1.05（盈利+5%，建议大幅减仓）
- 第三止盈：现价×1.10（盈利+10%，需双模型高置信确认）

请填写具体价格：
止损位：[X.XX]元（-2.5%）
目标止盈位：[X.XX]元（+3.5%/+5%/+10% 三选一，根据置信度选择）

{weak_info}

───────────────────────────

【二、技术分析详情】
───────────────────────────

【五维技术指标】
J值：{j_val:.1f} {'⚠️严重超卖' if j_val < 10 else '超卖' if j_val < 20 else '中性'} | RSI：{rsi:.1f} {'⚠️超卖区' if rsi < 30 else '弱势' if rsi < 45 else '中性'}
布林带：下轨{lower_band:.2f} / 上轨{upper_band:.2f} | 带宽{bb_width:.2f}% | 价格位置：{'跌破下轨' if price < lower_band else '中轨附近' if price < upper_band else '突破上轨'}
MA20乖离：{bias_20:.2f}% {'⚠️深度超跌' if bias_20 < -10 else '超跌' if bias_20 < -5 else '正常'}
量比：{vol_ratio:.1f} ({vol_status}) | 趋势通道：{'🔴下降通道' if trend_down else '🟢上升通道' if trend_up else '🔶震荡'} | 多周期：{mtf_tag} | 30分钟：{m30m_tag}

【大盘情绪】上涨占比{m_ratio:.1%}，平均波动{m_vol:.2f}%
{'大盘弱势，新仓需谨慎' if m_ratio < 0.4 else '大盘中性，可适度操作' if m_ratio < 0.65 else '大盘强势，有利于反弹'}

【板块共振】{sector_name} {resonance_tag}
{'该板块处于超跌区，可能与大盘形成共振反弹' if s_rsi < 20 else '板块小幅下跌，有一定共振反弹空间' if s_rsi < 35 else '板块强势，该股下跌属于个股独立行情' if s_rsi > 65 else '板块整体中性，无明显共振方向'}

【盘口分析】{'⚠️ 发现主力撤单诱多迹象！委比极低，需警惕' if is_trap else '✅ 盘口稳健，未见明显诱多诱空'}
布林带位置：{'价格已跌破下轨，极端超卖区域' if price < lower_band else '价格处于布林带中轨附近' if price < (lower_band + upper_band) / 2 else '价格接近上轨'}

【舆情摘要】{news_data[:80]}{'...' if len(news_data) > 80 else ''}

───────────────────────────

【三、总结与风险提示】
───────────────────────────

【一句话总结】（30字以内，直白说明核心逻辑）

【风险提示】（列举当前面临的主要风险，至少1条。注意：A股T+1制度下，若当前为午后拉升且无量，需警惕冲高回落导致明日低开被套的风险）

【持仓状态】{personal_context}
{f"策略约束：{strategy_context}" if strategy_context else ""}
"""

        # ==========================
        # 🔹 AI 调用（使用 httpx 异步版，超稳定）
        # ==========================
        try:
            r1_res = await httpx_ask_ollama(MODEL_R1, r1_p, temperature=0.3, force_json=False)
            if r1_res:
                log_terminal("进度", f">>> [5/5] AI 分析完成")

                # 从三段式报告中用正则提取决策行，防误判（如"不建议买入"）
                decision_match = re.search(r'决策[:：]\s*(.+)', r1_res)
                decision = decision_match.group(1).strip() if decision_match else ""

                if decision:
                    is_buy = any(w in decision for w in ["轻仓买入", "买入", "加仓"])
                    is_sell = any(w in decision for w in ["减仓", "止损", "换股"])
                    is_hold = "持有" in decision and not is_sell
                else:
                    # 兜底：决策行未提取到，用全文匹配
                    is_buy = any(w in r1_res for w in ["轻仓买入", "加仓"])
                    is_sell = any(w in r1_res for w in ["减仓", "止损", "换股"])
                    is_hold = "持有" in r1_res and not is_sell

                # 验证置信度（精确匹配"置信度：X"行，避免"风险较高"等误判）
                conf_match = re.search(r'置信度[：:]\s*(高|中|低)', r1_res)
                ds_conf = conf_match.group(1) if conf_match else "低"
                has_high_conf = ds_conf in ("高", "中")
                has_low_conf = ds_conf == "低"
                _ai_confidence_cache[code] = _ai_confidence_cache.get(code, {})
                _ai_confidence_cache[code]['ds_confidence'] = ds_conf
                _ai_confidence_cache[code]['timestamp'] = time.time()
                action_tag = "买入" if is_buy else "卖出" if is_sell else "持有" if is_hold else "观望"
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
                        # 管家模式只负责持仓分析，真正提醒统一交给半动态止盈止损
                        silent_reason = "管家模式持仓提醒统一走半动态止盈止损，AI审计仅记录不推送"

                if should_record_ai:
                    # 区分"首次买入"和"加仓"
                    is_new_buy = any(w in decision for w in ["轻仓买入", "买入"]) if decision else any(
                        w in r1_res for w in ["轻仓买入", "买入"])
                    is_add_pos = "加仓" in (decision if decision else r1_res)

                    # 提取置信度和决策类型用于标题
                    # 精确匹配置信度（避免"创新高""高风险"等误判）
                    conf_match = re.search(r'置信度[：:]\s*(高|中|低)', r1_res)
                    conf_level = f"{conf_match.group(1)}置信" if conf_match else "中置信"
                    title = f"📊 {name}({code}) | {action_tag} | {conf_level}"

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
                        # 从AI回复中解析建议股数和目标位
                        parsed_vol = None
                        parsed_real_vol = None
                        parsed_sim_vol = None
                        parsed_sl = None
                        parsed_tp1 = None
                        parsed_tp2 = None
                        parsed_tp3 = None
                        try:
                            vol_match = re.search(r'买入\s*(\d+)\s*股', r1_res)
                            if vol_match:
                                parsed_vol = int(vol_match.group(1))
                            # 区分实盘/模拟盘建议股数
                            real_vol_match = re.search(r'实盘建议[：:]*买入\s*(\d+)\s*股', r1_res)
                            if real_vol_match:
                                parsed_real_vol = int(real_vol_match.group(1))
                            else:
                                parsed_real_vol = parsed_vol
                            sim_vol_match = re.search(r'模拟盘建议[：:]*买入\s*(\d+)\s*股', r1_res)
                            if sim_vol_match:
                                parsed_sim_vol = int(sim_vol_match.group(1))
                            else:
                                parsed_sim_vol = parsed_vol
                            sl_match = re.search(r'止损位[：:]\s*([\d.]+)', r1_res)
                            if sl_match:
                                parsed_sl = float(sl_match.group(1))
                            tp_match = re.search(r'第一止盈[：:]\s*([\d.]+)', r1_res)
                            if tp_match:
                                parsed_tp1 = float(tp_match.group(1))
                            tp2_match = re.search(r'第二止盈[：:]\s*([\d.]+)', r1_res)
                            if tp2_match:
                                parsed_tp2 = float(tp2_match.group(1))
                            tp3_match = re.search(r'第三止盈[：:]\s*([\d.]+)', r1_res)
                            if tp3_match:
                                parsed_tp3 = float(tp3_match.group(1))
                        except Exception:
                            pass
                        save_ai_decision(
                            code=code, name=name,
                            decision=decision or action_tag,
                            confidence=ds_conf, price=price,
                            j_val=j_val, rsi=rsi, vol_ratio=vol_ratio,
                            bias_20=bias_20, market_sentiment=m_sent,
                            reasoning=r1_res, mode=mode_detail,
                            suggested_vol=parsed_vol,
                            real_vol=parsed_real_vol,
                            sim_vol=parsed_sim_vol,
                            target_stop=parsed_sl,
                            target_tp1=parsed_tp1,
                            target_tp2=parsed_tp2,
                            target_tp3=parsed_tp3
                        )
                    except _NativeException:
                        pass
                else:
                    if has_low_conf:
                        log_terminal("置信度不足", f"😶 {name} 置信度过低，放弃推送")
                    else:
                        log_terminal("静默", f"😶 {name} 观望")
                write_review_log(f"审计报告 {name}: {r1_res}")
            else:
                log_terminal("警告", f"⚠️ {name} AI 返回空")
        except _NativeException as e:
            log_terminal("审计崩溃", f"❌ {name} 异常：{e}")
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
    global GLOBAL_MARKET_DATA, consecutive_data_failures, _first_sync_done, MARKET_SOURCE
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
                    await asyncio.to_thread(generate_daily_review)
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
                if not hasattr(async_main_loop, '_last_cloud_sync') or (
                        now_ts - async_main_loop._last_cloud_sync) >= 180:
                    async_main_loop._last_cloud_sync = now_ts
                    await _flush_cloud_sync_now()
            except BaseException as e:
                logger.warning(f"云端同步异常：{type(e).__name__} - {e}")

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

    # --- 1.7 启动时补漏：检查最近3个工作日是否有缺失的每日复盘 ---
    from datetime import timedelta

    for days_ago in range(0, 4):
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
    log_terminal("数据引擎", "✅ 后台行情轮询线程已启动")

    # --- 3. 启动【异步主策略循环】（后台运行 · 极速版）---
    t_main = threading.Thread(target=start_async_main, daemon=True)
    t_main.start()
    log_terminal("策略引擎", "✅ 异步审计循环已启动（猎人=监控池，管家=持仓池）")

    # --- 4. ✅ 前台启动 Waitress 服务器 ---
    print("\n" + "=" * 50)
    print("✅ Waitress 启动成功！访问看板：")
    print("🔗 http://127.0.0.1:5000/dashboard")
    print("🔗 http://127.0.0.1:5000/buy_ui")
    print("\n🧪 模拟盘专属：")
    print("🔗 http://127.0.0.1:5000/sim-dashboard")
    print("🔗 http://127.0.0.1:5000/sim-buy-ui")
    print("=" * 50 + "\n")

    # 直接在这里启动 Waitress，替代 Flask 自带 run
    from waitress import serve

    serve(app, host="0.0.0.0", port=5000)
