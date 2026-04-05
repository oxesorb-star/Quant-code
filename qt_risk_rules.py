import time
from datetime import datetime


def get_market_health(
    *,
    last_cache_time,
    market_source,
    provider_state,
    consecutive_data_failures,
    now_ts=None,
    weak_market_score=40,
):
    # trade_safe 是系统级硬风控口径：给 SYSTEM_GUARD 判断“当前能否开新仓”使用。
    # 它不等于后续盈利管理里的 market_regime。
    age_sec = int((now_ts if now_ts is not None else time.time()) - last_cache_time) if last_cache_time else 10 ** 9
    provider_score = int(provider_state.get("score", 60 if market_source not in ("None", "FAILED") else 20))
    score = provider_score
    if age_sec > 900:
        score -= 35
    elif age_sec > 300:
        score -= 15
    score -= min(consecutive_data_failures * 10, 40)
    score = max(0, min(100, score))
    reasons = []
    if age_sec > 900:
        reasons.append(f"行情缓存过旧({age_sec}s)")
    if consecutive_data_failures >= 3:
        reasons.append(f"连续失败{consecutive_data_failures}次")
    if market_source in ("None", "FAILED"):
        reasons.append("无有效主数据源")
    return {
        "source": market_source,
        "age_sec": age_sec,
        "score": score,
        "reasons": reasons,
        "provider": dict(provider_state),
        "trade_safe": score >= weak_market_score and age_sec <= 900 and market_source not in ("None", "FAILED"),
    }


def refresh_system_guard(*, health, startup_check_ok, is_trade_time_now, system_guard, now_text=None):
    reasons = []
    if not startup_check_ok:
        reasons.append("启动自检失败")
    reasons.extend(health.get("reasons", []))
    halt_new_buys = bool(is_trade_time_now) and not health.get("trade_safe", False)
    system_guard.update({
        "halt_new_buys": halt_new_buys,
        "reasons": reasons,
        "updated_at": now_text or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "market_health_score": health.get("score", 0),
    })
    return system_guard


def check_new_buy_guard(*, guard):
    if guard["halt_new_buys"]:
        reason = "；".join(guard["reasons"]) if guard["reasons"] else "系统健康度不足"
        return reason
    return None


def get_risk_info(holdings, total_capital, price_map, *, calculate_holding_stats_fn):
    """
    计算总仓位、单票仓位、可调空间 (优化合并版)
    功能：通过核心引擎计算持仓状态，并根据风控规则返回汇总指标
    """
    if not holdings or total_capital <= 0:
        return {"total": 0, "single": {}, "max_single": 0, "can_vol": 5.0}

    stats, total_val = calculate_holding_stats_fn(holdings, price_map, total_capital)
    total_pct = (total_val / total_capital * 100)
    single_weights = {k: v['weight'] for k, v in stats.items()}
    max_single = max(single_weights.values()) if single_weights else 0
    can_vol = max(0, min(40, 70 - total_pct))

    return {
        "total": round(total_pct, 1),
        "single": {k: round(v, 1) for k, v in single_weights.items()},
        "max_single": round(max_single, 1),
        "can_vol": round(can_vol, 1)
    }


def get_dynamic_stop_loss(buy_price, pnl_pct):
    """分段上移保护止损：盈利达到各阶段时，止损位阶梯式上移（只升不降）"""
    if pnl_pct >= 10.0:
        return buy_price * 1.03, "强锁(+3%)"
    if pnl_pct >= 5.0:
        return buy_price * 1.01, "锁利(+1%)"
    if pnl_pct >= 3.5:
        return buy_price, "保本"
    return buy_price * 0.975, "固定(-2.5%)"


def get_stop_loss_alert_plan(*, pnl_pct, buy_price, curr_price):
    """输出止损提醒所需的结构化结果，主文件只负责冷却和推送。"""
    stop_price, stop_label = get_dynamic_stop_loss(buy_price, pnl_pct)
    triggered = curr_price <= stop_price
    return {
        "triggered": triggered,
        "stop_price": round(stop_price, 2),
        "stop_label": stop_label,
        "event": f"止损触发({stop_label})",
        "status_text": f"盈亏{pnl_pct:.2f}% | 保护位{stop_price:.2f}",
        "reason_text": "跌破保护止损位，建议立即检查",
        "recheck_event": "止损未执行",
        "recheck_reason": "30分钟前已提醒，当前仍未处理",
    }


def get_profit_stage(pnl_pct, *, tp1_pct=3.5, tp2_pct=5.0, tp3_pct=9.5):
    if pnl_pct >= tp3_pct:
        return "tp3"
    if pnl_pct >= tp2_pct:
        return "tp2"
    if pnl_pct >= tp1_pct:
        return "tp1"
    return "none"


def calc_reduce_shares(volume, ratio):
    try:
        volume_int = int(float(volume or 0))
        ratio_val = float(ratio or 0)
    except (TypeError, ValueError):
        return 0

    if volume_int <= 0 or ratio_val <= 0:
        return 0
    if volume_int < 100:
        return volume_int

    max_sellable_lot = (volume_int // 100) * 100
    if max_sellable_lot <= 0:
        return 0

    raw_target = int(volume_int * ratio_val)
    rounded = (raw_target // 100) * 100
    if rounded <= 0:
        rounded = 100
    return min(rounded, max_sellable_lot)


def get_market_regime(score, halt_new_buys, *, weak_market_score=40, strong_market_score=70):
    """盈利管理节奏标签：weak / neutral / strong，不直接替代系统硬熔断判断。"""
    try:
        score_val = float(score or 0)
    except (TypeError, ValueError):
        score_val = 0.0

    if halt_new_buys or score_val < weak_market_score:
        return {"regime": "weak", "label": "弱市", "score": score_val}
    if score_val >= strong_market_score:
        return {"regime": "strong", "label": "强市", "score": score_val}
    return {"regime": "neutral", "label": "中性", "score": score_val}


def get_profit_action_plan(
    *,
    pnl_pct,
    volume,
    buy_price,
    market_health_score,
    halt_new_buys,
    tp1_pct=3.5,
    tp2_pct=5.0,
    tp3_pct=9.5,
    tp1_sell_ratio=0.30,
    tp2_sell_ratio=0.35,
    tp3_sell_ratio=0.20,
    keeper_min_retain_ratio=0.15,
    weak_market_score=40,
    strong_market_score=70,
):
    dyn_stop_price, dyn_stop_label = get_dynamic_stop_loss(buy_price, pnl_pct)
    regime_info = get_market_regime(
        market_health_score,
        halt_new_buys,
        weak_market_score=weak_market_score,
        strong_market_score=strong_market_score,
    )
    stage = get_profit_stage(
        pnl_pct,
        tp1_pct=tp1_pct,
        tp2_pct=tp2_pct,
        tp3_pct=tp3_pct,
    )

    if stage == "none":
        return {
            "stage": "none",
            "stage_label": "未触发止盈",
            "regime": regime_info["regime"],
            "regime_label": regime_info["label"],
            "action": "hold",
            "reduce_ratio": 0.0,
            "reduce_shares": 0,
            "keeper_min_retain_ratio": keeper_min_retain_ratio,
            "reason": "未到分级止盈区间，继续观察",
            "stop_hint": f"维持当前保护位({dyn_stop_label})",
            "stop_price": round(dyn_stop_price, 2),
        }

    stage_map = {
        "tp1": {
            "label": "第一止盈",
            "ratio": tp1_sell_ratio,
            "neutral_reason": "建议先落袋一部分，保护位抬到保本",
            "weak_reason": "弱市先落袋，保护位抬到保本",
            "strong_reason": "强市先轻减仓，保护位抬到保本",
        },
        "tp2": {
            "label": "第二止盈",
            "ratio": tp2_sell_ratio,
            "neutral_reason": "标准锁利，保护位抬到+1%",
            "weak_reason": "弱市加快锁利，保护位抬到+1%",
            "strong_reason": "强市保留更多利润单，保护位抬到+1%",
        },
        "tp3": {
            "label": "第三止盈",
            "ratio": tp3_sell_ratio,
            "neutral_reason": "建议再减仓，剩余仓位交给keeper",
            "weak_reason": "弱市优先兑现，尾仓从严管理",
            "strong_reason": "强市保留趋势尾仓，继续跟踪",
        },
    }
    stage_cfg = stage_map[stage]

    adjusted_ratio = float(stage_cfg["ratio"])
    if regime_info["regime"] == "weak":
        adjusted_ratio = min(0.90, adjusted_ratio + 0.10)
        reason = stage_cfg["weak_reason"]
    elif regime_info["regime"] == "strong":
        adjusted_ratio = max(0.10, adjusted_ratio - 0.05)
        reason = stage_cfg["strong_reason"]
    else:
        reason = stage_cfg["neutral_reason"]

    reduce_shares = calc_reduce_shares(volume, adjusted_ratio)
    return {
        "stage": stage,
        "stage_label": stage_cfg["label"],
        "regime": regime_info["regime"],
        "regime_label": regime_info["label"],
        "action": "reduce",
        "reduce_ratio": round(adjusted_ratio, 2),
        "reduce_shares": reduce_shares,
        "keeper_min_retain_ratio": keeper_min_retain_ratio,
        "reason": reason,
        "stop_hint": f"当前保护位{dyn_stop_label}",
        "stop_price": round(dyn_stop_price, 2),
    }


def get_profit_alert_plan(
    *,
    pnl_pct,
    volume,
    buy_price,
    market_health_score,
    halt_new_buys,
    gemma_score=0,
    ds_confidence="",
    tp1_pct=3.5,
    tp2_pct=5.0,
    tp3_pct=9.5,
    tp1_sell_ratio=0.30,
    tp2_sell_ratio=0.35,
    tp3_sell_ratio=0.20,
    keeper_min_retain_ratio=0.15,
    weak_market_score=40,
    strong_market_score=70,
):
    """输出分级止盈提醒所需的结构化结果，包含 TP3 置信门槛。"""
    plan = get_profit_action_plan(
        pnl_pct=pnl_pct,
        volume=volume,
        buy_price=buy_price,
        market_health_score=market_health_score,
        halt_new_buys=halt_new_buys,
        tp1_pct=tp1_pct,
        tp2_pct=tp2_pct,
        tp3_pct=tp3_pct,
        tp1_sell_ratio=tp1_sell_ratio,
        tp2_sell_ratio=tp2_sell_ratio,
        tp3_sell_ratio=tp3_sell_ratio,
        keeper_min_retain_ratio=keeper_min_retain_ratio,
        weak_market_score=weak_market_score,
        strong_market_score=strong_market_score,
    )
    if plan.get("stage") == "none":
        return {
            **plan,
            "should_alert": False,
            "suppress_reason": "未触发止盈阶段",
            "event": "",
            "status_text": "",
            "reason_text": "",
        }

    should_alert = True
    suppress_reason = ""
    if plan.get("stage") == "tp3" and (float(gemma_score or 0) < 36 or ds_confidence != "高"):
        should_alert = False
        suppress_reason = (
            f"达到TP3但AI置信度不足(Gemma:{int(float(gemma_score or 0))}/40, DS:{ds_confidence or '未知'})"
        )

    reduce_ratio_pct = int(round(float(plan.get("reduce_ratio", 0) or 0) * 100))
    reduce_shares = int(plan.get("reduce_shares", 0) or 0)
    return {
        **plan,
        "should_alert": should_alert,
        "suppress_reason": suppress_reason,
        "event": f"{plan['stage_label']}({reduce_ratio_pct}%减仓)",
        "status_text": f"盈亏{pnl_pct:.2f}% | 建议减{reduce_shares}股",
        "reason_text": f"{plan['regime_label']}{plan['reason']}，{plan['stop_hint']}",
    }


def get_keeper_action(
    *,
    pnl_pct,
    vol_ratio,
    j_val,
    trend_up,
    vol_falling,
    curr_price,
    dyn_stop,
    keeper_min_retain_ratio=0.15,
):
    if curr_price <= dyn_stop:
        return {
            "action": "exit_tail",
            "label": "清尾仓",
            "reason": "跌破动态保护位，尾仓应优先退出",
            "retain_ratio": 0.0,
            "audit_needed": True,
        }

    if pnl_pct >= 5.0 and trend_up and vol_ratio > 1.3 and j_val > 55 and not vol_falling:
        return {
            "action": "hold_tail",
            "label": "继续拿尾仓",
            "reason": "趋势与量能仍强，尾仓继续持有",
            "retain_ratio": keeper_min_retain_ratio,
            "audit_needed": False,
        }

    if pnl_pct >= 3.5 and (vol_ratio < 0.9 or j_val < 40 or vol_falling):
        return {
            "action": "trim_tail",
            "label": "减尾仓",
            "reason": "动能边际减弱，建议继续锁利",
            "retain_ratio": keeper_min_retain_ratio,
            "audit_needed": True,
        }

    if pnl_pct >= 3.5:
        return {
            "action": "watch_tail",
            "label": "观察尾仓",
            "reason": "已过止盈线但趋势未坏，先观察尾仓",
            "retain_ratio": keeper_min_retain_ratio,
            "audit_needed": False,
        }

    return {
        "action": "hold_steady",
        "label": "持仓平稳",
        "reason": "未进入尾仓管理区间",
        "retain_ratio": 1.0,
        "audit_needed": False,
    }


def get_pyramid_add_plan(
    *,
    pnl_pct,
    current_volume,
    price,
    market_health_score,
    halt_new_buys,
    has_open_risk_event,
    current_single_ratio,
    current_total_ratio,
    cash_available,
    can_buy_amt,
    capital_base,
    allow_pyramid_add=True,
    pyramid_min_pnl=2.0,
    pyramid_max_single_ratio=0.25,
    pyramid_max_total_ratio=0.55,
    weak_market_score=40,
    strong_market_score=70,
):
    regime_info = get_market_regime(
        market_health_score,
        halt_new_buys,
        weak_market_score=weak_market_score,
        strong_market_score=strong_market_score,
    )
    base_result = {
        "action": "no_add",
        "allow_add": False,
        "requires_reconfirm": False,
        "regime": regime_info["regime"],
        "regime_label": regime_info["label"],
        "suggested_ratio": 0.0,
        "suggested_shares": 0,
        "suggested_amount": 0.0,
        "reason": "",
    }

    try:
        price_val = float(price or 0)
        current_volume_val = int(float(current_volume or 0))
        single_ratio_val = float(current_single_ratio or 0)
        total_ratio_val = float(current_total_ratio or 0)
        cash_val = float(cash_available or 0)
        can_buy_amt_val = float(can_buy_amt or 0)
        capital_val = float(capital_base or 0)
    except (TypeError, ValueError):
        base_result["reason"] = "账户上下文异常，无法生成加仓计划"
        return base_result

    if not allow_pyramid_add:
        base_result["reason"] = "盈利加仓功能未启用"
        return base_result
    if pnl_pct < pyramid_min_pnl:
        base_result["reason"] = f"当前盈利未达到加仓门槛({pyramid_min_pnl:.1f}%)"
        return base_result
    if halt_new_buys or regime_info["regime"] == "weak":
        base_result["reason"] = "弱市或系统熔断中，禁止盈利加仓"
        return base_result
    if has_open_risk_event:
        base_result["reason"] = "存在未关闭风险事件，禁止盈利加仓"
        return base_result
    if price_val <= 0 or current_volume_val <= 0 or capital_val <= 0:
        base_result["reason"] = "持仓或价格信息不足，无法生成加仓计划"
        return base_result
    if single_ratio_val >= pyramid_max_single_ratio:
        base_result["reason"] = f"单票仓位已达上限({pyramid_max_single_ratio:.0%})"
        return base_result
    if total_ratio_val >= pyramid_max_total_ratio:
        base_result["reason"] = f"总仓位已达上限({pyramid_max_total_ratio:.0%})"
        return base_result

    headroom_single_amt = max(0.0, (pyramid_max_single_ratio - single_ratio_val) * capital_val)
    headroom_total_amt = max(0.0, (pyramid_max_total_ratio - total_ratio_val) * capital_val)
    affordable_amt = min(cash_val, can_buy_amt_val, headroom_single_amt, headroom_total_amt)
    affordable_shares = int(affordable_amt // price_val // 100) * 100 if price_val > 0 else 0
    target_shares = calc_reduce_shares(current_volume_val, 0.5)
    suggested_shares = min(target_shares, affordable_shares)

    if suggested_shares < 100:
        base_result["reason"] = "可用加仓空间不足一个整手"
        return base_result

    suggested_amount = suggested_shares * price_val
    return {
        "action": "watch_add",
        "allow_add": True,
        "requires_reconfirm": True,
        "regime": regime_info["regime"],
        "regime_label": regime_info["label"],
        "suggested_ratio": 0.5,
        "suggested_shares": suggested_shares,
        "suggested_amount": round(suggested_amount, 2),
        "reason": "满足盈利加仓前置条件，但仍需第三层与第四层再次确认",
    }


def precheck_buy_order(
    acc_type,
    code,
    buy_price_val,
    volume_val,
    *,
    guard_reason,
    is_trade_time_now,
    now_provider,
    read_account_state_fn,
    logger_instance,
    use_cache=True,
):
    """买入前风控预检：现金余额 + 总仓位70% + 单票40%"""
    if guard_reason:
        return f"系统熔断：{guard_reason}"

    if not is_trade_time_now:
        now = now_provider()
        logger_instance.warning(
            f"⚠️ 非交易时段录单：{now.strftime('%Y-%m-%d %H:%M:%S')} | {acc_type} | {code} | {volume_val}股@{buy_price_val}"
        )

    this_amount = buy_price_val * volume_val
    account_state = read_account_state_fn(acc_type, use_cache=use_cache)
    current_cash = account_state["cash"]

    if current_cash < this_amount:
        return (
            f"现金不足：当前可用现金 {current_cash:.0f}元，"
            f"本次买入需要 {this_amount:.0f}元（差额 {this_amount - current_cash:.0f}元）"
        )

    capital = account_state["configured_capital"]
    holdings_data = account_state["holdings"]
    current_spent = sum(info.get('buy_price', 0) * info.get('volume', 0) for info in holdings_data.values())

    new_total_ratio = (current_spent + this_amount) / capital if capital > 0 else 0
    if new_total_ratio > 0.70:
        return (
            f"总仓位超限：买入后总仓位将达到 {new_total_ratio:.1%}，"
            f"超过上限 70%（当前已用 {current_spent/capital:.1%}，本次 {this_amount:.0f}元）"
        )

    if code in holdings_data:
        old_cost = holdings_data[code].get('buy_price', 0) * holdings_data[code].get('volume', 0)
        new_single_cost = old_cost + this_amount
    else:
        new_single_cost = this_amount

    single_ratio = new_single_cost / capital if capital > 0 else 0
    if single_ratio > 0.40:
        return (
            f"单票仓位超限：{code} 买入后占比将达到 {single_ratio:.1%}，"
            f"超过上限 40%（本次 {this_amount:.0f}元）"
        )

    return None


def get_strategy_constraints(code, price, m_ratio, holdings, *, get_shared_market_data_fn, total_capital, target_profit):
    """根据回本进度和环境生成硬性指令"""
    env_tag = "弱市(严禁新仓)" if m_ratio < 0.4 else "强市(可操作)"

    total_pnl = 0
    single_pos_ratio = 0
    if holdings:
        df_s = get_shared_market_data_fn()
        for c, info in holdings.items():
            row = df_s[df_s['代码'] == c]
            if not row.empty:
                curr = float(row['最新价'].values[0]) if not row['最新价'].empty else 0.0
                total_pnl += (curr - info['buy_price']) * info['volume']

        if code in holdings:
            info = holdings[code]
            cost = info['buy_price'] * info['volume']
            single_pos_ratio = cost / total_capital if total_capital > 0 else 0

    recovery_rate = (total_pnl / target_profit * 100) if target_profit > 0 else 0
    if recovery_rate < 30:
        strategy = "回本初期: 允许对超跌优质标的小幅补仓"
    elif recovery_rate < 70:
        strategy = "稳健期: 严禁加仓，以持有或逢高减仓为主"
    else:
        strategy = "冲刺期: 保护利润，分批止盈"

    risk_tag = "❗超仓预警" if single_pos_ratio > 0.4 else "仓位正常"
    return f"指令:{env_tag} | {strategy} | {risk_tag}({single_pos_ratio:.1%})"


def check_multi_gates(df, j_val, rsi, price, lower_band, vol_ratio):
    """四重卡口检测"""
    g1 = (j_val < 10)
    g2 = (rsi < 30) or (price <= lower_band * 1.002)
    g3 = False
    if len(df) >= 2:
        g3 = (vol_ratio > 2.2) and (price > df['close'].iloc[-2])
    g4 = False
    if len(df) >= 5:
        g4 = (price > df['close'].rolling(5).mean().iloc[-1]) and (j_val > -10)
    elif len(df) >= 1:
        g4 = (price > df['close'].iloc[0]) and (j_val > -10)
    return g1 or g2 or g3 or g4
