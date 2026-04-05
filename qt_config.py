import os
import sys
from datetime import datetime


def mask_secret(value, keep_start=4, keep_end=4):
    """脱敏展示敏感值，避免日志泄露完整 key/token"""
    text = str(value or "").strip()
    if not text:
        return "<empty>"
    if len(text) <= keep_start + keep_end:
        return "*" * len(text)
    return f"{text[:keep_start]}...{text[-keep_end:]}"


def get_env_required(name):
    """读取必填环境变量，缺失时在启动阶段直接报错"""
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_env_optional(name, default):
    """读取可选环境变量，未设置时使用安全默认值"""
    value = os.getenv(name, "").strip()
    return value if value else default


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")
REVIEW_DIR = os.path.join(BASE_DIR, "Strategy_Review")
_DAILY_REVIEW_DIR = os.path.join(BASE_DIR, "Daily_Review")

for path in (DATA_DIR, LOG_DIR, REVIEW_DIR, _DAILY_REVIEW_DIR):
    os.makedirs(path, exist_ok=True)

POSITIONS_FILE = os.path.join(DATA_DIR, "positions.json")
SIM_POSITIONS_FILE = os.path.join(DATA_DIR, "positions_sim.json")
MARKET_CACHE_FILE = os.path.join(DATA_DIR, "market_last_snapshot.pkl")
REAL_BALANCE_FILE = os.path.join(DATA_DIR, "balance_real.json")
SIM_BALANCE_FILE = os.path.join(DATA_DIR, "balance_sim.json")
TRADE_LOG_FILE = os.path.join(DATA_DIR, "trade_log.jsonl")
AI_DECISIONS_FILE = os.path.join(DATA_DIR, "ai_decisions.jsonl")
AI_DECISION_LINKS_FILE = os.path.join(DATA_DIR, "ai_decision_links.txt")
WATCH_CONFIRM_LOG_FILE = os.path.join(DATA_DIR, "watch_confirm_log.jsonl")
EXECUTION_GATE_LOG_FILE = os.path.join(DATA_DIR, "execution_gate_log.jsonl")
ORDER_EVENTS_FILE = os.path.join(DATA_DIR, "order_events.jsonl")
RISK_EVENTS_FILE = os.path.join(DATA_DIR, "risk_events.jsonl")
RUNTIME_EVENTS_STATE_FILE = os.path.join(DATA_DIR, "runtime_events_state.json")
SUMMARY_FILE = os.path.join(REVIEW_DIR, f"audit_{datetime.now().strftime('%Y%m%d')}.txt")
QUANT_SYSTEM_LOG_FILE = os.path.join(LOG_DIR, "quant_system.log")
_DAILY_REVIEW_FLAG_FILE = os.path.join(_DAILY_REVIEW_DIR, ".daily_review_flags")

SCT_KEY = get_env_required("SCT_KEY")
TCB_ENV_ID = get_env_required("TCB_ENV_ID")
TCB_FUNCTION_URL = get_env_required("TCB_FUNCTION_URL")
TCB_PUBLISHABLE_KEY = get_env_required("TCB_PUBLISHABLE_KEY")
SYNC_TOKEN = get_env_required("SYNC_TOKEN")
FLASK_SECRET_KEY = get_env_required("FLASK_SECRET_KEY")
WEB_ADMIN_USER = get_env_required("WEB_ADMIN_USER")
WEB_ADMIN_PASSWORD = get_env_required("WEB_ADMIN_PASSWORD")

STOCKS = [
    '000831', '601728', '000807', '601600', '601668', '300015',
    '600031', '002594', '000333', '600900', '601138', '601398',
    '601615', '000938', '600879', '000063', '601607', '600989',
    '600150', '000737', '600030', '002714', '600048', '600905'
]

MODEL_R1 = get_env_optional("MODEL_R1", "deepseek-r1:8b")
MODEL_GEMMA = get_env_optional("MODEL_GEMMA", "gemma3:4b")
OLLAMA_API = get_env_optional("OLLAMA_API", "http://127.0.0.1:11434/api/generate")

_LEGACY_DISABLE_SINA_SPOT = get_env_optional("DISABLE_SINA_SPOT", "0").lower() in ("1", "true", "yes", "on")
SINA_SPOT_MODE = get_env_optional(
    "SINA_SPOT_MODE",
    "subprocess" if (os.name == "nt" and sys.version_info >= (3, 13)) else "direct"
).strip().lower()
if _LEGACY_DISABLE_SINA_SPOT:
    SINA_SPOT_MODE = "disable"
if SINA_SPOT_MODE not in ("direct", "subprocess", "disable"):
    SINA_SPOT_MODE = "subprocess" if (os.name == "nt" and sys.version_info >= (3, 13)) else "direct"

TARGET_PROFIT = 30000.0
TOTAL_CAPITAL = 50084.0
MAX_POSITION_RATIO = 0.7
SIM_TOTAL_CAPITAL = 186727.0
SIM_TARGET_PROFIT = 12734.0

WATCH_CONFIRM_TARGET_ACCOUNT = get_env_optional("WATCH_CONFIRM_TARGET_ACCOUNT", "sim").strip().lower()
if WATCH_CONFIRM_TARGET_ACCOUNT not in ("sim", "real", "auto"):
    raise RuntimeError(
        f"Invalid WATCH_CONFIRM_TARGET_ACCOUNT: {WATCH_CONFIRM_TARGET_ACCOUNT}. Expected sim, real or auto."
    )

WATCH_CONFIRM_TARGET_MODE = get_env_optional("WATCH_CONFIRM_TARGET_MODE", "trial").strip().lower()
if WATCH_CONFIRM_TARGET_MODE not in ("trial", "normal"):
    raise RuntimeError(
        f"Invalid WATCH_CONFIRM_TARGET_MODE: {WATCH_CONFIRM_TARGET_MODE}. Expected trial or normal."
    )

AUTO_REAL_ENABLED = get_env_optional("AUTO_REAL_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
AUTO_REAL_MIN_CONFIDENCE = get_env_optional("AUTO_REAL_MIN_CONFIDENCE", "high").strip().lower()
if AUTO_REAL_MIN_CONFIDENCE not in ("low", "medium", "high"):
    raise RuntimeError(
        f"Invalid AUTO_REAL_MIN_CONFIDENCE: {AUTO_REAL_MIN_CONFIDENCE}. Expected low, medium or high."
    )

try:
    AUTO_REAL_MIN_MARKET_SCORE = float(get_env_optional("AUTO_REAL_MIN_MARKET_SCORE", "60"))
except Exception:
    raise RuntimeError("Invalid AUTO_REAL_MIN_MARKET_SCORE. Expected a numeric value.")

AUTO_REAL_FORCE_TRIAL = get_env_optional("AUTO_REAL_FORCE_TRIAL", "1").strip().lower() in ("1", "true", "yes", "on")

try:
    AUTO_REAL_MIN_SHARES = int(float(get_env_optional("AUTO_REAL_MIN_SHARES", "100")))
except Exception:
    raise RuntimeError("Invalid AUTO_REAL_MIN_SHARES. Expected an integer value.")
if AUTO_REAL_MIN_SHARES <= 0:
    raise RuntimeError("Invalid AUTO_REAL_MIN_SHARES. Expected a positive integer value.")

try:
    TP1_PCT = float(get_env_optional("TP1_PCT", "3.5"))
    TP2_PCT = float(get_env_optional("TP2_PCT", "5.0"))
    TP3_PCT = float(get_env_optional("TP3_PCT", "9.5"))
    TP1_SELL_RATIO = float(get_env_optional("TP1_SELL_RATIO", "0.30"))
    TP2_SELL_RATIO = float(get_env_optional("TP2_SELL_RATIO", "0.35"))
    TP3_SELL_RATIO = float(get_env_optional("TP3_SELL_RATIO", "0.20"))
    KEEPER_MIN_RETAIN_RATIO = float(get_env_optional("KEEPER_MIN_RETAIN_RATIO", "0.15"))
    WEAK_MARKET_SCORE = float(get_env_optional("WEAK_MARKET_SCORE", "40"))
    STRONG_MARKET_SCORE = float(get_env_optional("STRONG_MARKET_SCORE", "70"))
except Exception:
    raise RuntimeError("Invalid profit/risk config. Expected numeric TP/market regime values.")

ALLOW_PYRAMID_ADD = get_env_optional("ALLOW_PYRAMID_ADD", "1").strip().lower() in ("1", "true", "yes", "on")

try:
    PYRAMID_MIN_PNL = float(get_env_optional("PYRAMID_MIN_PNL", "2.0"))
    PYRAMID_MAX_SINGLE_RATIO = float(get_env_optional("PYRAMID_MAX_SINGLE_RATIO", "0.25"))
    PYRAMID_MAX_TOTAL_RATIO = float(get_env_optional("PYRAMID_MAX_TOTAL_RATIO", "0.55"))
except Exception:
    raise RuntimeError("Invalid pyramid add config. Expected numeric pyramid values.")

for _ratio_name, _ratio_value in (
    ("TP1_SELL_RATIO", TP1_SELL_RATIO),
    ("TP2_SELL_RATIO", TP2_SELL_RATIO),
    ("TP3_SELL_RATIO", TP3_SELL_RATIO),
    ("KEEPER_MIN_RETAIN_RATIO", KEEPER_MIN_RETAIN_RATIO),
    ("PYRAMID_MAX_SINGLE_RATIO", PYRAMID_MAX_SINGLE_RATIO),
    ("PYRAMID_MAX_TOTAL_RATIO", PYRAMID_MAX_TOTAL_RATIO),
):
    if not 0 < _ratio_value < 1:
        raise RuntimeError(f"Invalid {_ratio_name}. Expected a ratio between 0 and 1.")

if not (0 < TP1_PCT < TP2_PCT < TP3_PCT):
    raise RuntimeError("Invalid TP stage thresholds. Expected TP1_PCT < TP2_PCT < TP3_PCT.")

if WEAK_MARKET_SCORE >= STRONG_MARKET_SCORE:
    raise RuntimeError("Invalid market regime thresholds. Expected WEAK_MARKET_SCORE < STRONG_MARKET_SCORE.")

if PYRAMID_MIN_PNL <= 0:
    raise RuntimeError("Invalid PYRAMID_MIN_PNL. Expected a positive numeric value.")

if PYRAMID_MAX_SINGLE_RATIO >= PYRAMID_MAX_TOTAL_RATIO:
    raise RuntimeError("Invalid pyramid add limits. Expected PYRAMID_MAX_SINGLE_RATIO < PYRAMID_MAX_TOTAL_RATIO.")
