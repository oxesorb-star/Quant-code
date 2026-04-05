import re


def _clean_text(value, fallback="-", limit=28):
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        text = fallback
    return text[:limit]


def _fmt_account(account):
    value = str(account or "").strip().lower()
    if value == "real":
        return "REAL"
    if value == "sim":
        return "SIM"
    return (value or "-").upper()


def _fmt_mode(mode):
    value = str(mode or "").strip().lower()
    return value or "-"


def _fmt_price(price):
    try:
        number = float(price)
        if number > 0:
            return f"{number:.2f}"
    except (TypeError, ValueError):
        pass
    return "-"


def _fmt_name(name, code):
    label = _clean_text(name or code, fallback=str(code or "-"), limit=18)
    return f"{label}({str(code or '').zfill(6)})"


def _source_reason(source, action):
    source = str(source or "").strip().lower()
    mapping = {
        "watch_confirm": "观察确认链路执行",
        "web_panel": "手动提交",
        "remote_command": "远程命令执行",
        "core": f"{action}执行完成",
    }
    return mapping.get(source, _clean_text(source or f"{action}执行完成", fallback=f"{action}执行完成"))


def describe_execution_reason(source, action):
    return _source_reason(source, action)


def build_execution_gate_allow_message(*, name, code, account, mode, shares, price, reason):
    title = "【第四层放行】"
    content = "\n".join([
        f"标的：{_fmt_name(name, code)}",
        f"目标账户：{_fmt_account(account)}",
        f"模式：{_fmt_mode(mode)}",
        f"建议仓位：{int(shares or 0)}股",
        f"价格：{_fmt_price(price)}",
        f"原因：{_clean_text(reason, fallback='账户上下文允许执行')}",
    ])
    return title, content


def build_execution_gate_block_message(*, name, code, account, price, reason):
    title = "【第四层拦截】"
    content = "\n".join([
        f"标的：{_fmt_name(name, code)}",
        f"目标账户：{_fmt_account(account)}",
        "结果：block",
        f"价格：{_fmt_price(price)}",
        f"原因：{_clean_text(reason, fallback='执行闸门拒绝放行')}",
    ])
    return title, content


def build_buy_success_message(*, name, code, account, shares, price, mode, order_id):
    title = "【买入成功】"
    content = "\n".join([
        f"标的：{_fmt_name(name, code)}",
        f"账户：{_fmt_account(account)}",
        f"股数：{int(shares or 0)}股",
        f"价格：{_fmt_price(price)}",
        f"模式：{_fmt_mode(mode)}",
        f"订单号：{_clean_text(order_id, fallback='-', limit=36)}",
    ])
    return title, content


def build_sell_success_message(*, name, code, account, shares, price, pnl_text, reason):
    title = "【卖出成功】"
    content = "\n".join([
        f"标的：{_fmt_name(name, code)}",
        f"账户：{_fmt_account(account)}",
        f"股数：{int(shares or 0)}股",
        f"价格：{_fmt_price(price)}",
        f"盈亏：{_clean_text(pnl_text, fallback='-')}",
        f"原因：{_clean_text(reason, fallback='执行完成')}",
    ])
    return title, content


def build_execution_fail_message(*, name, code, account, action, reason):
    title = "【执行失败】"
    content = "\n".join([
        f"标的：{_fmt_name(name, code)}",
        f"账户：{_fmt_account(account)}",
        f"动作：{_clean_text(action, fallback='unknown')}",
        f"原因：{_clean_text(reason, fallback='执行未完成', limit=42)}",
    ])
    return title, content


def build_risk_warning_message(*, name, code, account, event, status_or_pnl, reason, title="【风控预警】"):
    content = "\n".join([
        f"标的：{_fmt_name(name, code)}",
        f"账户：{_fmt_account(account)}",
        f"事件：{_clean_text(event, fallback='风险事件')}",
        f"状态：{_clean_text(status_or_pnl, fallback='-')}",
        f"原因：{_clean_text(reason, fallback='请立即检查', limit=42)}",
    ])
    return title, content


def build_watch_confirm_pass_message(*, name, code, confidence, price, reason):
    title = "【第三层确认通过】"
    content = "\n".join([
        f"标的：{_fmt_name(name, code)}",
        "结果：confirm_buy",
        f"置信度：{_clean_text(confidence, fallback='medium')}",
        f"价格：{_fmt_price(price)}",
        f"原因：{_clean_text(reason, fallback='结构确认通过', limit=42)}",
    ])
    return title, content


def build_watch_confirm_wait_message(*, name, code, price, reason):
    title = "【第三层继续观察】"
    content = "\n".join([
        f"标的：{_fmt_name(name, code)}",
        "结果：wait",
        f"价格：{_fmt_price(price)}",
        f"原因：{_clean_text(reason, fallback='结构尚未确认，继续观察', limit=42)}",
    ])
    return title, content


def build_watch_confirm_reject_message(*, name, code, price, reason):
    title = "【第三层否决】"
    content = "\n".join([
        f"标的：{_fmt_name(name, code)}",
        "结果：reject",
        f"价格：{_fmt_price(price)}",
        f"原因：{_clean_text(reason, fallback='观察确认否决', limit=42)}",
    ])
    return title, content


def build_auto_account_message(*, name, code, target_account, shares, reason, mode=None):
    target = str(target_account or "").strip().lower()
    if target == "real":
        title = "【账户判定：允许实盘】"
        content = "\n".join([
            f"标的：{_fmt_name(name, code)}",
            "目标账户：REAL",
            f"模式：{_fmt_mode(mode or 'trial')}",
            f"建议仓位：{int(shares or 0)}股",
            f"原因：{_clean_text(reason, fallback='满足 AUTO_REAL 条件', limit=42)}",
        ])
        return title, content
    title = "【账户判定：继续模拟盘】"
    content = "\n".join([
        f"标的：{_fmt_name(name, code)}",
        "目标账户：SIM",
        f"建议仓位：{int(shares or 0)}股",
        f"原因：{_clean_text(reason, fallback='未满足 AUTO_REAL 条件', limit=42)}",
    ])
    return title, content


def build_system_error_message(*, module, event, fallback, title="【系统异常】"):
    content = "\n".join([
        f"模块：{_clean_text(module, fallback='system')}",
        f"事件：{_clean_text(event, fallback='unknown', limit=42)}",
        f"处理：{_clean_text(fallback, fallback='稍后重试', limit=42)}",
    ])
    return title, content
