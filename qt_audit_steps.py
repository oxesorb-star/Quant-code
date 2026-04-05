import re


def build_gemma_prompt(
    *,
    name,
    code,
    price,
    j_val,
    rsi,
    bias_20,
    vol_status,
    vol_ratio,
    m_ratio,
    sector_name,
    resonance_tag,
    trap_tag,
):
    return f"""
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


def parse_gemma_result(text):
    if not text:
        return {"score": 0, "decision": "未知", "reason": "无理由"}
    score_match = re.search(r'综合评分[：:]\s*(\d+)/40', text)
    decision_match = re.search(r'决策[：:]\s*【?(强烈通过|通过|观望|拒绝)】?', text)
    reason_match = re.search(r'理由[：:]\s*(.+?)(?:\n|$)', text)
    return {
        "score": int(score_match.group(1)) if score_match else 0,
        "decision": decision_match.group(1) if decision_match else "未知",
        "reason": reason_match.group(1).strip() if reason_match else "无理由",
    }


def build_personal_context(
    *,
    is_held,
    h_info,
    acc_type,
    pnl_pct,
    keeper_action,
    keeper_reason,
    vol_ratio,
    j_val,
    price,
):
    if not is_held or not h_info:
        return "[个人实战数据]: 未持仓，处于观察期"

    acc_name = "实盘" if acc_type == 'real' else "模拟盘"
    personal_context = (
        f"[个人实战数据]:\n- 持仓状态：已入场 ({acc_name})\n"
        f"- 成本:{h_info['buy_price']:.3f}\n- 股数:{h_info['volume']}\n- 盈亏:{pnl_pct:.2f}%"
    )

    if keeper_action and keeper_action not in ("hold_steady",):
        vol_label = "放量" if vol_ratio > 1.3 else "缩量" if vol_ratio < 0.8 else "平量"
        j_trend = "上行" if j_val > 50 else "拐头向下" if j_val < 30 else "中性"
        day_change = (price - h_info['buy_price']) / h_info['buy_price'] * 100
        stagnation = vol_ratio > 1.0 and day_change < 1.0 and pnl_pct > 2.5
        stag_tag = "⚠️放量滞涨(疑似出货)" if stagnation else ""
        keeper_label_map = {
            "hold_tail": "继续拿尾仓",
            "trim_tail": "减尾仓锁利",
            "exit_tail": "清尾仓",
            "watch_tail": "观察尾仓",
        }
        keeper_label = keeper_label_map.get(keeper_action, keeper_action)

        if pnl_pct >= 5.0:
            personal_context += (
                f"\n\n【tp2止盈后追撤判断】(当前盈利{pnl_pct:.1f}%，已过第二止盈线，获利丰厚)"
                f"\n- 量能：{vol_label}(量比{vol_ratio:.1f}) | J值方向：{j_trend}(J={j_val:.1f}) {stag_tag}"
                f"\n- 当前keeper建议：{keeper_label}（{keeper_reason}）"
                f"\n- 已过第二止盈线，建议偏向落袋。请根据以下规则给出追撤建议（必须三选一，写进决策理由）："
                f"\n  · 继续拿：量比>1.5 且 J>60 且无滞涨（极强趋势才值得拿）"
                f"\n  · 减半：量比>1.0 但<1.5 或 J在30~60（动能边际减弱，先锁大部分利润）"
                f"\n  · 清仓：量比<1.0 或 J<30 或 放量滞涨 或 趋势转空（风险大于收益，全部离场）"
            )
        else:
            personal_context += (
                f"\n\n【tp1止盈后追撤判断】(当前盈利{pnl_pct:.1f}%，已过第一止盈线)"
                f"\n- 量能：{vol_label}(量比{vol_ratio:.1f}) | J值方向：{j_trend}(J={j_val:.1f}) {stag_tag}"
                f"\n- 当前keeper建议：{keeper_label}（{keeper_reason}）"
                f"\n- 请根据以下规则给出追撤建议（必须三选一，写进决策理由）："
                f"\n  · 继续拿：量比>1.3 且 J>50（动能仍在，可追第二止盈）"
                f"\n  · 减半：量比<0.8 或 J拐头向下（动能衰竭，先锁一半利润）"
                f"\n  · 清仓：放量滞涨 或 趋势由多转空（主力出货风险，全部离场）"
            )

    return personal_context


def build_mode_detail(
    *,
    is_held,
    acc_label,
    j_val,
    rsi,
    bias_20,
    price,
    lower_band,
    vol_falling,
    trend_down,
    vol_ratio,
    pnl_pct,
    trend_up,
    ma10,
    keeper_action="",
):
    if not is_held:
        hunter_tags = []
        if (j_val < 12) and (rsi < 33) and (bias_20 < -8):
            hunter_tags.append("三低共振")
        if (j_val < 20) and (price < lower_band) and vol_falling:
            hunter_tags.append("跌穿布林+缩量")
        if (j_val < 0) and trend_down and (vol_ratio < 0.7):
            hunter_tags.append("J负数+极缩量")
        return f"猎人模式 | 触发:{'+'.join(hunter_tags) if hunter_tags else '常规'}"

    keeper_label_map = {
        "hold_tail": "继续拿尾仓",
        "trim_tail": "减尾仓锁利",
        "exit_tail": "清尾仓",
        "watch_tail": "观察尾仓",
        "hold_steady": "持仓平稳",
    }
    keeper_tag = keeper_label_map.get(keeper_action, "")
    if not keeper_tag:
        if (pnl_pct > -2.5) and trend_up and (j_val < 45):
            keeper_tag = "持仓待涨"
        elif (pnl_pct <= -4.0) and (price < ma10) and (j_val > 30):
            keeper_tag = "止损离场"
        elif (pnl_pct >= 2.5) and (not trend_up or vol_falling):
            keeper_tag = "减仓止盈"
        else:
            keeper_tag = "常规扫描"
    return f"管家模式({acc_label}) | 触发:{keeper_tag}"


def build_weak_info(*, weak_code, holdings, dashboard_cache):
    if weak_code and weak_code in holdings:
        wh = holdings[weak_code]
        wn = dashboard_cache.get(weak_code, {}).get('name', weak_code)
        return f"最弱持仓:{wn}({weak_code}) 成本{wh['buy_price']:.2f} 股数{wh['volume']}"
    return ""


def build_lessons_text(lessons, stats_text):
    parts = []
    if lessons:
        parts.append("\n📌 【系统近期交易复盘】\n" + "\n".join(lessons) + "\n（请参考以上真实交易结果调整当前判断）\n")
    if stats_text:
        parts.append("\n" + stats_text)
    return "".join(parts)


def build_account_health(*, total_pnl, total_capital, target_profit):
    pnl_pct = (total_pnl / total_capital * 100) if total_capital > 0 else 0
    recovery = min(max(total_pnl / target_profit * 100, 0), 100) if target_profit > 0 else 0
    if recovery >= 70:
        risk_mode = "保守——接近目标，优先保护利润"
    elif total_pnl < 0:
        risk_mode = "激进——仍需追回，可承受适度风险"
    else:
        risk_mode = "均衡"
    return (
        f"\n📌 【账户健康度】"
        f"\n- 实盘总浮盈亏：{total_pnl:+.0f}元（{pnl_pct:+.1f}%）"
        f"\n- 回本进度：{recovery:.0f}%（目标{target_profit:.0f}元）"
        f"\n- 当前风险承受：{risk_mode}\n"
    )


def build_r1_prompt(
    *,
    name,
    code,
    price,
    mode_detail,
    m_ratio,
    m_vol,
    lessons_text,
    account_health,
    pyramid_plan_text,
    total_capital,
    real_ratio,
    real_cash,
    real_can_buy_amt,
    sim_total_capital,
    sim_ratio,
    sim_cash,
    sim_can_buy_amt,
    weak_info,
    j_val,
    rsi,
    lower_band,
    upper_band,
    bb_width,
    bias_20,
    vol_ratio,
    vol_status,
    trend_down,
    trend_up,
    mtf_tag,
    m30m_tag,
    sector_name,
    s_rsi,
    resonance_tag,
    is_trap,
    news_data,
    personal_context,
    strategy_context,
):
    return f"""
你是一位专业的A股量化交易分析师。请严格按照以下【三段式】格式输出，不要遗漏任何段落，不要添加额外内容。

═══════════════════════════
【一、核心结论】
═══════════════════════════

📌 {name}({code}) | 现价 {price:.2f} 元
📌 模式：{mode_detail}
📌 大盘：上涨{m_ratio:.1%} {'强势' if m_ratio > 0.65 else '弱势' if m_ratio < 0.35 else '中性'} | 波动{m_vol:.2f}%
{lessons_text}{account_health}{pyramid_plan_text}

【操作指令】（从以下选择一项：轻仓买入 / 加仓 / 持有 / 减仓 / 止损 / 换股 / 观望）
决策：[填写决策]
置信度：[高/中/低]

【仓位计算】（严格基于以下可用资金计算具体买入股数，必须是100的整数倍）
- 实盘本金：{total_capital:.0f}元 | 已用仓位：{real_ratio*100:.1f}% | 可用现金：{real_cash:.0f}元 | 可买空间：{real_can_buy_amt:.0f}元
- 模拟盘本金：{sim_total_capital:.0f}元 | 已用仓位：{sim_ratio*100:.1f}% | 可用现金：{sim_cash:.0f}元 | 可买空间：{sim_can_buy_amt:.0f}元

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


def parse_r1_result(text):
    decision_match = re.search(r'决策[:：]\s*(.+)', text)
    decision = decision_match.group(1).strip() if decision_match else ""

    if decision:
        is_buy = any(w in decision for w in ["轻仓买入", "买入", "加仓"])
        is_sell = any(w in decision for w in ["减仓", "止损", "换股"])
        is_hold = "持有" in decision and not is_sell
    else:
        is_buy = any(w in text for w in ["轻仓买入", "加仓"])
        is_sell = any(w in text for w in ["减仓", "止损", "换股"])
        is_hold = "持有" in text and not is_sell

    conf_match = re.search(r'置信度[：:]\s*(高|中|低)', text)
    ds_conf = conf_match.group(1) if conf_match else "低"
    action_tag = "买入" if is_buy else "卖出" if is_sell else "持有" if is_hold else "观望"

    return {
        "decision": decision,
        "is_buy": is_buy,
        "is_sell": is_sell,
        "is_hold": is_hold,
        "ds_conf": ds_conf,
        "has_high_conf": ds_conf in ("高", "中"),
        "has_low_conf": ds_conf == "低",
        "action_tag": action_tag,
        "conf_level": f"{conf_match.group(1)}置信" if conf_match else "中置信",
        "is_new_buy": any(w in decision for w in ["轻仓买入", "买入"]) if decision else any(w in text for w in ["轻仓买入", "买入"]),
        "is_add_pos": "加仓" in (decision if decision else text),
    }


def extract_trade_targets(text):
    parsed = {
        "parsed_vol": None,
        "parsed_real_vol": None,
        "parsed_sim_vol": None,
        "parsed_sl": None,
        "parsed_tp1": None,
        "parsed_tp2": None,
        "parsed_tp3": None,
    }
    try:
        vol_match = re.search(r'买入\s*(\d+)\s*股', text)
        if vol_match:
            parsed["parsed_vol"] = int(vol_match.group(1))

        real_vol_match = re.search(r'实盘建议[：:]*买入\s*(\d+)\s*股', text)
        if real_vol_match:
            parsed["parsed_real_vol"] = int(real_vol_match.group(1))
        else:
            parsed["parsed_real_vol"] = parsed["parsed_vol"]

        sim_vol_match = re.search(r'模拟盘建议[：:]*买入\s*(\d+)\s*股', text)
        if sim_vol_match:
            parsed["parsed_sim_vol"] = int(sim_vol_match.group(1))
        else:
            parsed["parsed_sim_vol"] = parsed["parsed_vol"]

        sl_match = re.search(r'止损位[：:]\s*([\d.]+)', text)
        if sl_match:
            parsed["parsed_sl"] = float(sl_match.group(1))

        tp_match = re.search(r'第一止盈[：:]\s*([\d.]+)', text)
        if tp_match:
            parsed["parsed_tp1"] = float(tp_match.group(1))

        tp2_match = re.search(r'第二止盈[：:]\s*([\d.]+)', text)
        if tp2_match:
            parsed["parsed_tp2"] = float(tp2_match.group(1))

        tp3_match = re.search(r'第三止盈[：:]\s*([\d.]+)', text)
        if tp3_match:
            parsed["parsed_tp3"] = float(tp3_match.group(1))
    except Exception:
        pass
    return parsed
