import asyncio
import json
import logging
import os
import random
import re
import subprocess
import sys
import tempfile
import time

import akshare as ak
import pandas as pd
import requests


logger = logging.getLogger(__name__)

_daily_kline_cache = {}
_DAILY_CACHE_TTL = 3600
_30m_kline_cache = {}
_30M_CACHE_TTL = 600


def is_market_data_valid(df, strict=False):
    """
    弹性行情校验引擎：
    1. 基础校验：非空且包含核心列（代码、最新价）。
    2. 严格校验 (strict=True)：用于大盘情绪计算，需满足 500 只样本。
    3. 宽松校验 (strict=False)：用于休盘期/个股审计，只要有数据就放行。
    """
    if df is None or df.empty:
        return False

    required_cols = ['代码', '最新价']
    if not all(col in df.columns for col in required_cols):
        return False
    latest = pd.to_numeric(df['最新价'], errors='coerce')
    if latest.isna().all() or (latest <= 0).all():
        return False

    count = len(df)
    if strict:
        if count < 500:
            return False
    else:
        if count < 1:
            return False
    return True


def normalize_df(df, source, logger_instance=None):
    """
    统一适配层：把三个数据源的列名统一成标准格式
    标准输出列：代码 / 最新价 / 名称 / 涨跌幅 / 委比
    """
    if df is None or df.empty:
        return df

    logger_instance = logger_instance or logger
    df = df.copy()
    cols = df.columns.tolist()

    if '代码' not in cols:
        for c in ['股票代码', 'symbol', 'code', '证券代码']:
            if c in cols:
                extracted = df[c].astype(str).str.extract(r'(\d{6})')
                if extracted is not None and not extracted.empty:
                    df['代码'] = extracted[0]
                break

    if '最新价' not in cols:
        for c in ['trade', '现价', 'price', '今收盘']:
            if c in cols:
                df['最新价'] = pd.to_numeric(df[c], errors='coerce')
                break
    else:
        df['最新价'] = pd.to_numeric(df['最新价'], errors='coerce')

    if '名称' not in cols:
        for c in ['name', '股票名称', '简称']:
            if c in cols:
                df['名称'] = df[c]
                break

    if '涨跌幅' not in cols:
        for c in ['change_percent', 'pcnt', 'percent', 'changepercent']:
            if c in cols:
                df['涨跌幅'] = pd.to_numeric(df[c], errors='coerce')
                break
    else:
        df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce')

    if '委比' not in cols:
        df['委比'] = 0.0

    if '代码' in df.columns:
        df['代码'] = df['代码'].astype(str).str.zfill(6)

    required = ['代码', '最新价', '名称', '涨跌幅', '委比']
    existing = [c for c in required if c in df.columns]
    df = df[existing].dropna(subset=['代码', '最新价'])

    logger_instance.info(f"[normalize_df] {source} -> {len(df)} 条有效数据")
    return df


def fetch_sina_spot_subprocess():
    """在隔离子进程中拉取新浪全市场现货，防止底层原生崩溃拖垮主进程。"""
    child_script = r"""
import json
import random
import requests
import sys

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]
REFERER_MAP = {
    "sina.com": "https://finance.sina.com.cn/",
    "eastmoney.com": "https://quote.eastmoney.com/",
}
orig_request = requests.Session.request

def patched_request(self, method, url, **kwargs):
    headers = kwargs.get("headers", {})
    if "User-Agent" not in headers:
        headers["User-Agent"] = random.choice(USER_AGENTS)
    for domain, referer in REFERER_MAP.items():
        if domain in url and "Referer" not in headers:
            headers["Referer"] = referer
            break
    kwargs["headers"] = headers
    return orig_request(self, method, url, **kwargs)

requests.Session.request = patched_request

import akshare as ak

out_path = sys.argv[1]
rows = []
df = ak.stock_zh_a_spot()
if df is not None and not df.empty:
    if "委比" not in df.columns:
        df["委比"] = 0.0
    required = [c for c in ["代码", "名称", "最新价", "涨跌幅", "委比"] if c in df.columns]
    if "代码" in required and "最新价" in required:
        rows = df[required].to_dict(orient="records")
with open(out_path, "w", encoding="utf-8") as fh:
    json.dump(rows, fh, ensure_ascii=False)
"""
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            temp_path = tmp.name
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        proc = subprocess.run(
            [sys.executable, "-c", child_script, temp_path],
            capture_output=True,
            text=True,
            timeout=35,
            creationflags=creationflags
        )
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip()
            raise RuntimeError(f"新浪隔离进程退出码={proc.returncode} | {err[:200]}")
        if not temp_path or not os.path.exists(temp_path):
            raise RuntimeError("新浪隔离进程未生成结果文件")
        with open(temp_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if not payload:
            return pd.DataFrame()
        return normalize_df(pd.DataFrame(payload), "SinaSubprocess")
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass


def _to_tencent_symbol(code):
    clean = str(code).strip().zfill(6)
    if not re.match(r'^\d{6}$', clean):
        return ""
    if clean.startswith(('5', '6', '9')):
        return f"sh{clean}"
    if clean.startswith(('4', '8')):
        return f"bj{clean}"
    return f"sz{clean}"


def fetch_tencent_focus_quotes(codes, logger_instance=None):
    """腾讯重点票补价链路：只抓监控池/持仓票，避免全市场接口缺失时完全失明。"""
    logger_instance = logger_instance or logger
    unique_codes = []
    seen = set()
    for code in codes or []:
        clean = str(code).strip().zfill(6)
        if re.match(r'^\d{6}$', clean) and clean not in seen:
            seen.add(clean)
            unique_codes.append(clean)
    if not unique_codes:
        return pd.DataFrame()

    rows = []
    for i in range(0, len(unique_codes), 60):
        batch = unique_codes[i:i + 60]
        symbols = [_to_tencent_symbol(code) for code in batch]
        symbols = [symbol for symbol in symbols if symbol]
        if not symbols:
            continue
        try:
            resp = requests.get(f"https://qt.gtimg.cn/q={','.join(symbols)}", timeout=15)
            resp.encoding = 'gbk'
            for line in resp.text.split(';'):
                line = line.strip()
                if not line or '=' not in line:
                    continue
                try:
                    payload = line.split('"', 2)[1]
                except Exception:
                    continue
                fields = payload.split('~')
                if len(fields) < 33:
                    continue
                code = str(fields[2]).strip().zfill(6)
                name = str(fields[1]).strip() or code
                try:
                    price = float(fields[3])
                except Exception:
                    price = 0.0
                try:
                    pct = float(fields[32])
                except Exception:
                    pct = 0.0
                if price <= 0:
                    continue
                rows.append({
                    "代码": code,
                    "名称": name,
                    "最新价": price,
                    "涨跌幅": pct,
                    "委比": 0.0
                })
        except Exception as e:
            logger_instance.error(f"腾讯重点补价失败(batch={i // 60 + 1}): {e}")

    if not rows:
        return pd.DataFrame()
    df_tx = pd.DataFrame(rows).drop_duplicates(subset=['代码'], keep='last')
    return normalize_df(df_tx, "TencentFocus", logger_instance=logger_instance)


def merge_market_snapshot(base_df, overlay_df, source_label="Overlay", logger_instance=None):
    """把重点票实时价格叠加到全市场底板上，兼顾大盘宽度和关键票实时性。"""
    logger_instance = logger_instance or logger
    if overlay_df is None or overlay_df.empty:
        return base_df
    overlay_df = normalize_df(overlay_df, source_label, logger_instance=logger_instance)
    if base_df is None or base_df.empty:
        return overlay_df

    merged = base_df.copy()
    if '代码' not in merged.columns:
        return overlay_df

    merged['代码'] = merged['代码'].astype(str).str.zfill(6)
    merged = merged.set_index('代码', drop=False)

    for _, row in overlay_df.iterrows():
        code = str(row.get('代码', '')).zfill(6)
        if not code:
            continue
        if code in merged.index:
            for col in ['最新价', '涨跌幅', '名称', '委比']:
                if col in row and pd.notna(row[col]) and row[col] != '':
                    merged.at[code, col] = row[col]
        else:
            merged.loc[code] = row

    merged = merged.reset_index(drop=True)
    return normalize_df(merged, f"Merged-{source_label}", logger_instance=logger_instance)


def fetch_robust_spot_data(
    monitor_stocks,
    *,
    sina_spot_mode,
    load_overlay_market_base,
    get_audit_universe,
    record_provider_result,
    log_terminal,
    native_exception,
    logger_instance=None,
):
    """双层防御行情引擎 (新浪→东财→腾讯重点票补价，2026 实战版)"""
    logger_instance = logger_instance or logger
    monitor_stocks = [str(s).zfill(6) for s in monitor_stocks]

    if sina_spot_mode == "disable":
        log_terminal("数据源策略", "⚠️ 已禁用新浪主链路，直接使用备用链路")
    else:
        try:
            if sina_spot_mode == "subprocess":
                log_terminal("数据源策略", "🧯 新浪主链路以隔离子进程模式运行，崩溃不会拖垮主进程")
                df_sina = fetch_sina_spot_subprocess()
                sina_source = "SinaSubprocess"
            else:
                df_sina = ak.stock_zh_a_spot()
                sina_source = "Sina"
            if df_sina is not None and not df_sina.empty:
                if all(c in df_sina.columns for c in ['代码', '最新价', '名称', '涨跌幅']):
                    valid_rows = df_sina[df_sina['最新价'] > 0]
                    if len(valid_rows) < 100:
                        log_terminal("数据质量", f"⚠️ 新浪数据质量差：仅{len(valid_rows)}条有效价格，降级")
                    else:
                        health = record_provider_result(sina_source, True, len(valid_rows))
                        log_terminal("数据源选择", f"✅ 使用新浪{'隔离进程' if sina_spot_mode == 'subprocess' else ''} ({len(valid_rows)}只股票)")
                        log_terminal("数据源健康", f"{sina_source} 健康分 {health['score']}")
                        return df_sina, sina_source
                else:
                    log_terminal("字段缺失", "⚠️ 新浪数据缺少必需字段")
        except Exception as e:
            err_msg = str(e)
            record_provider_result("SinaSubprocess" if sina_spot_mode == "subprocess" else "Sina", False, error=err_msg)
            if '<' in err_msg:
                log_terminal("反爬拦截", "⚠️ 新浪触发反爬（返回HTML页面），切换备用数据源")
            else:
                log_terminal("数据源崩溃", f"❌ 新浪异常：{type(e).__name__}")
            logger_instance.error(f"新浪详细错误：{err_msg}")

    time.sleep(random.uniform(0.5, 1.5))

    try:
        df_em = ak.stock_zh_a_spot_em()
        if df_em is not None and not df_em.empty:
            df_em = normalize_df(df_em, "EastMoney", logger_instance=logger_instance)
            health = record_provider_result("EastMoney", True, len(df_em))
            log_terminal("数据源选择", f"✅ 使用东方财富 ({len(df_em)}只股票)")
            log_terminal("数据源健康", f"EastMoney 健康分 {health['score']}")
            return df_em, "EastMoney"
    except Exception as e:
        err_msg = str(e)
        record_provider_result("EastMoney", False, error=err_msg)
        if '<' in err_msg:
            log_terminal("反爬拦截", "⚠️ 东财触发反爬")
        else:
            log_terminal("数据源崩溃", f"❌ 东方财富异常：{type(e).__name__}")
        logger_instance.error(f"东财详细错误：{err_msg}")

    try:
        focus_codes = []
        seen = set()
        for code in (monitor_stocks or []):
            clean = str(code).strip().zfill(6)
            if clean and clean not in seen:
                seen.add(clean)
                focus_codes.append(clean)
        for code in get_audit_universe():
            clean = str(code).strip().zfill(6)
            if clean and clean not in seen:
                seen.add(clean)
                focus_codes.append(clean)

        df_tx = fetch_tencent_focus_quotes(focus_codes, logger_instance=logger_instance)
        if is_market_data_valid(df_tx, strict=False):
            health = record_provider_result("TencentFocus", True, len(df_tx))
            base_df = load_overlay_market_base()
            if is_market_data_valid(base_df, strict=True):
                merged_df = merge_market_snapshot(base_df, df_tx, "TencentFocus", logger_instance=logger_instance)
                log_terminal("数据源选择", f"✅ 使用腾讯重点补价 + 全市场快照 ({len(df_tx)}只重点票覆盖)")
                log_terminal("数据源健康", f"TencentFocus 健康分 {health['score']}")
                return merged_df, "TencentOverlay"
            log_terminal("数据源选择", f"⚠️ 使用腾讯重点行情 ({len(df_tx)}只)，大盘宽度将退化为局部样本")
            log_terminal("数据源健康", f"TencentFocus 健康分 {health['score']}")
            return df_tx, "TencentFocus"
    except Exception as e:
        record_provider_result("TencentFocus", False, error=str(e))
        logger_instance.error(f"腾讯重点补价详细错误：{e}")

    log_terminal("数据源耗尽", "❌ 东财与腾讯补价均失效，请检查网络")
    return pd.DataFrame(), "FAILED"


def get_shared_market_data(
    *,
    current_data_getter,
    current_data_setter,
    data_lock,
    do_force_sync,
    market_cache_file,
    log_terminal,
    native_exception,
    on_disk_fallback=None,
):
    """
    看板和主循环调用的统一接口：
    核心逻辑：内存(严格) -> 网络(首次) -> 磁盘(兜底) -> 旧内存(降级)
    """
    with data_lock:
        current_data = current_data_getter()
        if current_data is not None and is_market_data_valid(current_data, strict=True):
            return current_data

    log_terminal("同步中", "内存缓存无效，正在从网络获取最新行情...")
    if do_force_sync():
        return current_data_getter()

    if os.path.exists(market_cache_file):
        try:
            disk_data = pd.read_pickle(market_cache_file)
            if is_market_data_valid(disk_data, strict=False):
                current_data_setter(disk_data)
                if callable(on_disk_fallback):
                    on_disk_fallback(disk_data)
                log_terminal("离线兜底", f"网络失败，加载历史快照 ({len(disk_data)}条)")
                return current_data_getter()
        except native_exception as e:
            log_terminal("快照损坏", f"无法读取磁盘文件：{e}")

    current_data = current_data_getter()
    if current_data is not None and not current_data.empty:
        log_terminal("缓存降级", "同步失败，强行复用现有内存数据运行")
        return current_data

    log_terminal("严重错误", "全链路无法获取行情，请检查网络设置")
    return pd.DataFrame({
        "代码": [],
        "最新价": [],
        "名称": [],
        "涨跌幅": [],
        "委比": []
    })


def get_market_analysis(*, get_shared_market_data_fn, log_terminal):
    """市场情绪分析：上涨家数占比 + 平均波动率，用于策略风控。"""
    log_terminal("市场环境", "正在从共享缓存评估全 A 股情绪...")
    try:
        df = get_shared_market_data_fn()
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


def _clip_log_text(text, limit=48):
    s = re.sub(r'\s+', ' ', str(text or '')).strip()
    if len(s) <= limit:
        return s
    return s[: max(0, limit - 3)] + "..."


def fetch_web_news(code, *, log_terminal, native_exception):
    """个股新闻抓取：从东方财富获取前3条重要标题"""
    start_ts = time.time()
    log_terminal("舆情采集", f"正在抓取 {code} 实时深度资讯...")
    try:
        news_df = ak.stock_news_em(symbol=code)
        if news_df.empty:
            log_terminal("舆情采集", f"{code} 未检索到重大新闻 | 耗时{time.time() - start_ts:.1f}s")
            return "暂无重大新闻"
        top_news = news_df['新闻标题'].head(3).tolist()
        head = _clip_log_text(top_news[0], 28) if top_news else "暂无标题"
        log_terminal("舆情采集", f"{code} 抓取完成 | {len(top_news)}条 | 耗时{time.time() - start_ts:.1f}s | 头条:{head}")
        return " | ".join(top_news)
    except native_exception:
        log_terminal("舆情采集", f"{code} 抓取超时 | 耗时{time.time() - start_ts:.1f}s")
        return "网络数据源连接超时"
    except Exception as e:
        log_terminal("舆情采集", f"{code} 抓取失败 | {type(e).__name__} | 耗时{time.time() - start_ts:.1f}s")
        return "舆情抓取失败"


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


async def get_30m_kline(full_code, code, df_5m_backup=None, *, log_terminal, native_exception):
    """带缓存的30分钟K线获取，同一只票10分钟内复用。失败返回 None。"""
    now = time.time()
    if code in _30m_kline_cache:
        ts, df = _30m_kline_cache[code]
        if now - ts < _30M_CACHE_TTL:
            return df

    try:
        await asyncio.sleep(random.uniform(0.05, 0.2))
        df_30m = await asyncio.to_thread(ak.stock_zh_a_minute, symbol=full_code, period='30')
        if df_30m is not None and not df_30m.empty:
            _30m_kline_cache[code] = (now, df_30m)
            return df_30m
    except native_exception:
        pass

    try:
        await asyncio.sleep(random.uniform(0.1, 0.3))
        raw_code = full_code[2:]
        df_30m = await asyncio.to_thread(
            ak.stock_zh_a_hist_min_em, symbol=raw_code, period='30', adjust=''
        )
        if df_30m is not None and not df_30m.empty:
            _30m_kline_cache[code] = (now, df_30m)
            log_terminal("30分钟数据源", f"{code} 新浪失败，已从东财历史分钟获取")
            return df_30m
    except native_exception:
        pass

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
        except native_exception:
            pass

    return None
