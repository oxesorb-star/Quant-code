# Quant Trade 运行逻辑全说明（2026-04-02）

## 文档定位
本说明基于当前工作区代码、真实运行日志、append-only 数据文件和沙箱隔离烟测整理而成。目标不是画一张简单架构图，而是把系统从启动、拉数、AI 审计、第三层观察确认、第四层执行闸门、执行层、推送、云同步，到最终留档和回测的整条链路完整讲清楚。

这份文档重点覆盖四类对象：
- 代码主逻辑：系统如何从启动一路跑到买卖执行。
- 模块作用：每个模块负责什么、输入输出是什么、与谁耦合。
- 关键变量：配置常量、主控全局状态、跨模块上下文字段，以及它们变化后会影响哪些环节。
- 落地结果：推送消息长什么样、日志长什么样、真实文件里目前已经有什么样本。

当前版本可以定义为：Phase 2（三四层 + 推送 + 回测留档）基础闭环稳定版。

## 一、项目总体架构
系统当前不是单体大脚本硬堆，而是“主控文件 + 领域模块”的结构。主控文件仍然是调度中心，但核心能力已经拆开。

### 1. 主控文件
- `E:\Python\Quant Trade\Monitor Stable.py`：总编排器。负责启动、自检、日志初始化、反爬 monkey patch、主循环、AI 审计编排、第三层/第四层接线、Flask 路由、推送、部分风控主流程、云同步编排。

### 2. 配置与基础设施
- `E:\Python\Quant Trade\qt_config.py`：统一环境变量、目录、文件路径、资金参数、AI 模型路由开关、watch confirm / auto real 开关。
- `E:\Python\Quant Trade\qt_system_io.py`：JSON 安全读取、写入、同步标记文件读写、artifact token 计算。
- `E:\Python\Quant Trade\qt_events.py`：风险事件、provider 健康状态、事件状态锁。
- `E:\Python\Quant Trade\qt_sync.py`：云同步 headers/payload/queue/worker/pending scan/daily review upload/remote command claim/update。

### 3. 市场、风控、执行、审计
- `E:\Python\Quant Trade\qt_market_data.py`：全市场现货、分钟线/日线、新闻、市场分析、共享行情缓存。
- `E:\Python\Quant Trade\qt_risk_rules.py`：市场健康度、系统 guard、买入预检、动态止损规则、多条件闸门。
- `E:\Python\Quant Trade\qt_execution.py`：账户账本、订单状态机、执行锁、幂等、交易日志、订单事件、买卖真实执行。
- `E:\Python\Quant Trade\qt_audit_steps.py`：Gemma / R1 prompt 构造、解析、账户上下文文案、持仓复盘教训文本。

### 4. Phase 2 新链路模块
- `E:\Python\Quant Trade\qt_ai_gateway.py`：统一模型调用入口，支持 Ollama 和 OpenRouter，按 stage 路由。
- `E:\Python\Quant Trade\qt_watch_confirm.py`：第三层观察确认，包含 watch pool、规则判断、Gemini 辅助确认、观察日志。
- `E:\Python\Quant Trade\qt_execution_gate.py`：第四层执行闸门，包含 baseline 账户约束、GPT-5.4 mini 辅助裁决、execution gate 日志。
- `E:\Python\Quant Trade\qt_push_templates.py`：统一推送模板构造。

### 5. 当前总链路
```text
watchdog.bat
 -> Monitor Stable.py 启动
 -> qt_config 读取环境与路径
 -> startup_self_check()
 -> 行情线程 / 云同步 / Flask 启动
 -> async_main_loop()
 -> async_execute_audits() 批量异步审计
 -> async_execute_audit() 单票审计
 -> qt_ai_gateway(screen/deep_audit)
 -> save_ai_decision()
 -> _run_watch_confirm_execution_chain()
 -> qt_watch_confirm.scan_watch_pool_with_ai()
 -> qt_execution_gate.evaluate_execution_gate()
 -> _execute_buy() / _execute_sell()
 -> qt_execution.execute_buy() / execute_sell()
 -> order_events / trade_log / risk_events / push / sync
```

## 二、启动到稳定运行全过程
### 1. watchdog 层
- `E:\Python\Quant Trade\watchdog.bat` 是桌面快捷方式和常规启动入口。
- 现在它会先清理坏代理环境变量（HTTP_PROXY / HTTPS_PROXY / ALL_PROXY），再拉起主程序。
- 这样可以避免东财、腾讯、CloudBase、远程命令轮询被 `127.0.0.1:9` 之类的坏代理直接拦死。

### 2. qt_config 初始化
主程序一 import `qt_config.py`，就会完成以下动作：
- 校验必填环境变量：`SCT_KEY / TCB_ENV_ID / TCB_FUNCTION_URL / TCB_PUBLISHABLE_KEY / SYNC_TOKEN / FLASK_SECRET_KEY / WEB_ADMIN_USER / WEB_ADMIN_PASSWORD`。
- 建立目录：`data / logs / Strategy_Review / Daily_Review`。
- 生成路径常量：`TRADE_LOG_FILE / AI_DECISIONS_FILE / ORDER_EVENTS_FILE / RISK_EVENTS_FILE / SIM_BALANCE_FILE / SIM_POSITIONS_FILE / MARKET_CACHE_FILE` 等。
- 解析资金参数：`TOTAL_CAPITAL / SIM_TOTAL_CAPITAL / TARGET_PROFIT / MAX_POSITION_RATIO`。
- 解析 Phase 2 配置：`WATCH_CONFIRM_TARGET_ACCOUNT / WATCH_CONFIRM_TARGET_MODE / AUTO_REAL_ENABLED / AUTO_REAL_MIN_CONFIDENCE / AUTO_REAL_MIN_MARKET_SCORE / AUTO_REAL_FORCE_TRIAL / AUTO_REAL_MIN_SHARES`。

### 3. 启动自检 startup_self_check()
主文件 `startup_self_check()` 会检查：
- 目录是否存在。
- 关键账本文件是否可读。
- CloudBase URL / Flask secret / Ollama API 等基础配置是否存在。
- 如果 `watch_confirm` 或 `execution_gate` 使用 `openrouter`，还会额外检查 `OPENROUTER_API_KEY` 和 `OPENROUTER_BASE_URL`。

当前真实启动日志样例：
```text
2026-04-02 18:12:01 - INFO - 🔔 【启动自检】: ✅ 启动自检通过
2026-04-02 18:12:01 - INFO - 🔔 【系统启动】: Monitor Stable 1.0 已就绪...
2026-04-02 18:12:27 - INFO - 🔔 【数据源选择】: ✅ 使用新浪隔离进程 (5490只股票)
2026-04-02 18:12:37 - INFO - 🔔 【数据引擎】: 📡 qt_market_data 行情模块已接管主链路（新浪/东财/腾讯补价）
2026-04-02 18:12:37 - INFO - 🔔 【数据引擎】: ✅ 后台行情轮询线程已启动
2026-04-02 18:12:37 - INFO - Serving on http://0.0.0.0:5000
```

### 4. 启动后常驻的运行部件
- Flask 服务：提供登录、dashboard、buy/sell 页面和 API。
- 后台行情线程：由 `qt_market_data` 接管主链路。
- 云同步 worker：把本地 append-only 文件同步到 CloudBase。
- 主循环：定时做 AI 审计、观察池扫描、风控检查、心跳上报。

## 三、关键模块详解
### 1. qt_config.py
作用：所有运行参数与路径的单一真相源。

关键变量及其后果：
- `DATA_DIR / LOG_DIR / REVIEW_DIR / _DAILY_REVIEW_DIR`：所有运行时状态文件的落盘目录。改路径会影响日志、账本、缓存、云同步扫描。
- `TRADE_LOG_FILE / AI_DECISIONS_FILE / ORDER_EVENTS_FILE / RISK_EVENTS_FILE`：append-only 核心文件路径。改了会直接影响推送、回测、同步。
- `MODEL_GEMMA / MODEL_R1 / OLLAMA_API`：screen 与 deep_audit 的默认模型和入口。
- `WATCH_CONFIRM_TARGET_ACCOUNT`：第三层候选的目标账户，支持 `sim / real / auto`。会影响 watch item、第四层账户、最终 `_execute_buy` 账户以及 auto 推送。
- `WATCH_CONFIRM_TARGET_MODE`：默认 `trial / normal`。在 `auto -> real` 时还会被 `AUTO_REAL_FORCE_TRIAL` 进一步覆盖。
- `AUTO_REAL_ENABLED`：决定 `auto` 能不能真正升到实盘；为 0 时一律回落模拟盘。
- `AUTO_REAL_MIN_CONFIDENCE / AUTO_REAL_MIN_MARKET_SCORE / AUTO_REAL_MIN_SHARES`：控制自动升实盘门槛。

### 2. qt_ai_gateway.py
作用：把系统中所有模型调用统一到一层，不再让主链直接散落旧的 AI 入口。

当前 stage 路由：
- `screen -> ollama -> gemma3:4b`
- `deep_audit -> ollama -> deepseek-r1:8b`
- `watch_confirm -> openrouter -> google/gemini-3.1-flash-lite-preview`
- `execution_gate -> openrouter -> openai/gpt-5.4-mini`

关键点：
- `ask_ai(stage=...)` 会统一处理 provider/model 选择。
- `ask_ollama()` 与 `ask_openrouter()` 都使用 `httpx.AsyncClient(..., trust_env=False)`，避免系统代理污染。
- `force_json=True` 时会做 JSON 抽取和校验。
- 对 timeout / connect error / HTTPStatusError / JSONDecodeError 会写统一运行日志。

### 3. qt_market_data.py
作用：行情抓取和降级策略。

主链路：
- 全市场主链：新浪隔离子进程 `SinaSubprocess`。
- 主链失败时：腾讯重点补价 + 市场快照兜底。
- 单票审计链：5 分钟 -> 30 分钟 -> 日线。
- 单票 5 分钟当前优先新浪；失败再切东财；再失败切 30 分钟和日线。

关键运行态变量（在主文件中持有）：
- `GLOBAL_MARKET_DATA`：共享行情总表，买卖页、dashboard、审计、stock_info API 都依赖它。
- `MARKET_SOURCE`：当前主数据源名称。会进入市场健康分、系统 guard、日志、异常归因。
- `LAST_CACHE_TIME`：最近一次主行情更新时间；过旧会拉低市场健康分并触发 guard。
- `_market_data_lock`：保护共享行情快照写入。

### 4. qt_risk_rules.py
作用：规则层，不负责执行，只负责判断。

关键函数：
- `get_market_health()`：市场健康分与结构说明。
- `refresh_system_guard()`：判断是否暂停新开仓，例如行情缓存过旧或无有效主数据源。
- `precheck_buy_order()`：买入前最后一道规则闸门，检查整手、现金、仓位、交易时段等。
- `get_dynamic_stop_loss()`：动态止损/锁利线。
- `check_multi_gates()`：多条件拦截。

### 5. qt_watch_confirm.py（第三层）
作用：把 R1 深审后的“可疑买点”放进观察池，不直接买，而是让它继续观察并复核。

核心对象是 watch item，主要字段有：
- `watch_id`：观察项唯一 ID。
- `decision_signal_id`：来源 AI 决策 ID。
- `code / name / price`：基础标的信息。
- `target_account / target_mode`：预期目标账户与模式。
- `default_shares / default_sim_shares / default_real_shares`：默认股数建议。
- `features`：结构化特征快照。
- `status / decision / confidence / decision_reason / risk_flag / evaluated_at`：本次观察结果。

第三层规则：
- `made_new_low=True` -> `reject`。
- `blowoff_reversal=True` -> `reject`。
- `market_worsened=True` -> `wait` 且带风险标记。
- `higher_low + reclaimed_ma5_or_ma10 + volume_recovered=True` -> `confirm_buy`。
- 其他情况 -> `wait`。

关键修复：
- `wait` 不再终结观察，而是映射成 `status=pending`，下一轮还能继续扫。
- 扫描前会通过主文件回调刷新动态特征，不再只吃入池时旧快照。
- AI 只能辅助，不能把规则未达标的 `wait` 直接升级成 `confirm_buy`。

第三层日志：`watch_confirm_log.jsonl`。它记录的是“每次评估结果”，不是观察池当前状态。

### 6. qt_execution_gate.py（第四层）
作用：把第三层结果转成“是否允许执行、用什么账户、多少股、什么模式”的标准化执行许可。

这层分两段：
- `baseline_execution_gate()`：不用 AI，也能先做硬约束。
- `evaluate_execution_gate()`：在 baseline 允许的边界内，再用 GPT-5.4 mini 做辅助裁决。

关键输入：
- `watch_result`：第三层结果。
- `account_context`：账户可用现金、可买额度、总仓位、单票仓位、最小试错仓、合法最大股数、市场健康分、是否熔断等。

关键约束：
- 第三层不是 `confirm_buy`，直接 block。
- 有 `risk_flag`，直接 block。
- 现金不足、仓位受限、单票过高、合法整手不足，直接 block。
- AI 给出的 shares 也不能超过 `legal_max_shares`。
- `allow=true` 时 shares 会强制收敛为 A 股 100 股整数倍。

第四层日志：`execution_gate_log.jsonl`。它记录的是“每次闸门判断”，不管最终有没有买。

## 四、关键运行变量与它们牵动的环节
这部分只覆盖“跨模块、跨阶段、会落文件、会影响最终结果”的变量，不枚举每个局部循环临时变量。

### 1. 主控全局状态（Monitor Stable.py）
- `DASHBOARD_CACHE`：dashboard / sell_page / 资产展示缓存。变更会影响页面显示与价格/名称兜底。
- `GLOBAL_MARKET_DATA`：共享行情总表。直接影响 `api/stock_info`、AI 审计行情特征、dashboard 实时数据。
- `MARKET_SOURCE`：当前行情来源。会进入市场健康分、系统 guard、日志、异常归因。
- `LAST_CACHE_TIME`：最近行情更新时间。过旧会导致 `market_health_score` 下降，可能触发停止新开仓。
- `SYSTEM_GUARD`：系统熔断状态摘要。`halt_new_buys=True` 时第四层和风控链都可能拦截。
- `_sync_queue / _sync_queue_replace_index`：CloudBase 待同步队列和 replace-key 索引。影响 trade_logs / ai_decisions / account_snapshots 等是否被推上云。
- `_push_cooldown`：推送冷却。相同 code 的普通推送在冷却窗内会跳过。
- `_daily_new_buy`：记录当日已新开仓标的，防止过度加票。
- `_HOLDINGS_CACHE`：实盘/模拟盘持仓缓存，影响页面读取与风控扫描效率。

### 2. 第三层观察上下文字段
- `watch_id`：连接第三层、第四层、trade_log 的主桥梁之一。
- `decision_signal_id`：连接 AI 决策与观察项的主桥梁之一。
- `target_account / target_mode`：后续自动账户选择、第四层、执行推送都依赖它。
- `default_shares / default_sim_shares / default_real_shares`：会影响第四层默认股数和 auto->real 时的试错仓大小。
- `features`：第三层规则判断与日志留档的依据。
- `decision_source`：决定是 `rule`、`rule+ai`、`rule_dominant`、`ai_risk_veto` 还是 fallback；它会进入第三层日志、第四层上下文、trade_log extra 字段。

### 3. 第四层执行上下文字段
- `cash_available`：可用现金。直接决定能否买以及 legal_max_shares 上限。
- `can_buy_amt`：在风控/仓位限制下真实可买额度。
- `current_total_ratio`：当前总仓位。会触发总仓上限 block。
- `single_ratio`：当前单票仓位。会触发单票上限 block。
- `trial_shares_cap`：试错仓上限。即便 AI 想给更大股数，也会被裁到这个范围。
- `legal_max_shares`：第四层最终合法股数上限，是“现金 + 可买额度 + 价格 + 整手制”的综合结果。
- `market_health_score / halt_new_buys / guard_reason`：由主文件透传进第四层和日志，不由 gate 模块自己全局反查。

### 4. 交易日志关键串联字段
- `entry_signal_id`：买入对应哪条 AI 决策。
- `watch_id`：买入是否来自第三层观察项。
- `gate_decision_source`：第四层放行来源，例如 `baseline` 或 `baseline+ai`。
- `entry_mode`：`trial / normal`。
- `exit_reason_tag`：标准化退出标签，用于后续报表。

## 五、主循环与主审计链的完整运行逻辑
### 1. async_main_loop() 的职责
主循环在交易日/非交易日都常驻，但行为不同：
- 交易时段内：拉行情、批量异步审计、观察池再扫描、风控检查、心跳/快照同步。
- 非交易时段：不做真实新仓执行，但可以保留观察池、做同步、做复盘、维持服务。

### 2. async_execute_audits()
- 这是“批量审计入口”。
- 内部使用 `asyncio.Semaphore(5)` 限制并发，避免分钟线/新闻/API 过度并发。
- 每只股票走 `async_execute_audit(code, m_ratio, m_vol)`。

### 3. async_execute_audit() 单票链路
单票主链可以拆成 5 步：

#### 第 1 步：行情与结构读取
- 先准备名称、5m/30m/日线、最新价、量比、J 值、RSI、Bias20、市场环境。
- 这里大量依赖 `qt_market_data.py`。
- 如果 5 分钟线失败，会按 `5分钟 -> 30分钟 -> 日线` 降级。

#### 第 2 步：Gemma 初筛（screen）
- 现在统一走 `qt_ai_gateway.ask_ai(stage="screen", ...)`。
- 作用不是做最后决策，而是做轻量初筛、卡口判断。
- 典型日志格式会包括 `Gemma 初筛完成 | 评分:xx/40 | 决策:...`。

#### 第 3 步：R1 深审（deep_audit）
- 现在统一走 `qt_ai_gateway.ask_ai(stage="deep_audit", ...)`。
- 作用是给出“轻仓买入 / 持有 / 卖出 / 换股 / 观望”等方向与置信度、建议仓位、止损止盈。
- 典型日志样例：
```text
2026-04-02 13:17:38 - INFO - 🔔 【AI 审计】: 标的 明阳智能 触发卡口，正在根据回本策略进行深度审计...
2026-04-02 13:17:43 - INFO - 🔔 【深度审计】: 启动 deepseek-r1:8b 生成三段式报告...
2026-04-02 13:18:08 - INFO - 🔔 【深度审计】: 明阳智能 深审完成 | 动作:卖出 | 置信度:低 | 耗时25.0s
2026-04-02 13:18:08 - INFO - 🔔 【进度】: >>> [5/5] AI 分析完成
2026-04-02 13:54:58 - INFO - 🔔 【执行闸门】: 中国船舶 allow=True | shares=100 | source=baseline
2026-04-02 19:37:25 - INFO - 🔔 【执行闸门】: 中国船舶 account=sim allow=False | shares=0 | source=baseline
```

#### 第 4 步：save_ai_decision() 落地
- 深审结果会写入 `ai_decisions.jsonl`。
- 这里已经有 `id / signal_id / decision / confidence / price / j_val / rsi / vol_ratio / bias_20 / reasoning / stop_loss / tp1 / tp2 / tp3` 等字段。
- 当前真实样本摘录：
```json
[
  {
    "id": "D20260401-0000-576",
    "time": "2026-04-01 09:39",
    "code": "601615",
    "name": "明阳智能",
    "decision": "轻仓买入",
    "confidence": "中",
    "price": 16.65,
    "j_val": 0.3,
    "rsi": 21.7,
    "vol_ratio": 1.6,
    "bias_20": -15.92,
    "mode": "猎人模式 | 触发:三低共振+跌穿布林+缩量",
    "real_vol": 100,
    "sim_vol": 500,
    "stop_loss": 16.4,
    "tp1": 17.23,
    "tp2": 17.48,
    "tp3": 18.32
  },
  {
    "id": "D20260401-0000-777",
    "time": "2026-04-01 14:09",
    "code": "601728",
    "name": "中国电信",
    "decision": "持有",
    "confidence": "中",
    "price": 5.69,
    "j_val": -10.1,
    "rsi": 47.0,
    "vol_ratio": 0.8,
    "bias_20": -3.95,
    "mode": "猎人模式 | 触发:常规",
    "real_vol": null,
    "sim_vol": null,
    "stop_loss": 5.54,
    "tp1": 5.89,
    "tp2": 5.97,
    "tp3": 6.26
  }
]
```

#### 第 5 步：进入 Phase 2 观察确认链
- 深审若给出可观察买点，不直接下单。
- 主文件会进入 `_run_watch_confirm_execution_chain(...)`。
- 这一步先入观察池，再扫观察池，再把 `confirm_buy` 送去第四层。
- 非交易时段当前也会先录入观察池，只是跳过执行闸门。

## 六、第三层观察确认的详细逻辑
### 1. add_watch_candidate()
深审链把候选写成 watch item：
- `watch_id` 是观察实例 ID。
- `decision_signal_id` 是来源 AI 决策 ID。
- `target_account / target_mode / default_shares` 已经在这一层落进去。
- 所以第三层不是只看行情，它同时承载“这票原本想去哪种账户、多少股”的上下文。

### 2. scan_watch_pool() / scan_watch_pool_with_ai()
- 只有 `status=pending` 的项才会继续被扫。
- `wait` 会被映射回 `pending`，所以能继续观察。
- `confirm_buy -> confirmed`，`reject -> rejected`，`expired -> expired`。
- `scan_watch_pool_with_ai()` 会在规则判断基础上再调用 Gemini，但规则优先。

### 3. 为什么“规则优先”很重要
如果规则还只是 `wait`，Gemini 就算想确认，也不能直接把它升级成 `confirm_buy`。当前系统会把这种情况记成：
- `decision=wait`
- `ai_decision=confirm_buy`
- `decision_source=rule_dominant`

这意味着：模型可以表达“我倾向想买”，但执行权仍然服从规则层。

### 4. 第三层历史日志
每次第三层评估都会写 `watch_confirm_log.jsonl`，字段包括：
- `watch_id / decision_signal_id / code / name / price`
- `decision / confidence / reason / risk_flag`
- `rule_decision / ai_decision / ai_confidence / decision_source`
- `target_account / target_mode / default_shares`
- `features_snapshot / observed_at / evaluated_at`

当前真实运行里，这个文件还没有正式样本；下面是我在当前环境做的隔离烟测样本：
```json
{
  "watch_id": "watch-a56c26cffc60",
  "decision_signal_id": "D20260402-TEST-001",
  "code": "600150",
  "name": "中国船舶",
  "price": 31.56,
  "decision": "wait",
  "confidence": "low",
  "reason": "结构尚未确认，继续观察；Gemini 倾向确认，但规则未达标",
  "risk_flag": false,
  "rule_decision": "wait",
  "ai_decision": "confirm_buy",
  "ai_confidence": "high",
  "decision_source": "rule_dominant",
  "target_account": "auto",
  "target_mode": "normal"
}
```

## 七、第四层执行闸门的详细逻辑
### 1. baseline_execution_gate() 先做硬约束
第四层不是“AI 说买就买”，而是：
- 先检查第三层是不是 `confirm_buy`。
- 再检查 `risk_flag`。
- 再检查现金、总仓、单票仓位、合法整手、可买额度。
- 这些都没问题时，才允许 AI 在边界内补充判断。

### 2. evaluate_execution_gate() 的最终输出
第四层最终会返回标准 JSON：
- `allow`
- `action`
- `account`
- `shares`
- `mode`
- `reason`
- `expires_at`
- `decision_source`

### 3. legal_max_shares 的意义
这是第四层最重要的实盘约束字段之一。它不是简单的默认 100 股，而是综合了：
- 当前 `price`
- `cash_available`
- `can_buy_amt`
- A 股整手规则
- 试错仓上限

所以即便 AI 想放大到 500 股，只要合法最大股数只有 200，最终也会被裁成 200。

### 4. 第四层历史日志
每次第四层判断都会写 `execution_gate_log.jsonl`，无论 allow 还是 block。

隔离烟测样本 A：第三层仍是 wait，所以 baseline 直接拦截
```json
{
  "watch_id": "watch-a56c26cffc60",
  "decision_signal_id": "D20260402-TEST-001",
  "code": "600150",
  "name": "中国船舶",
  "account": "real",
  "mode": "blocked",
  "allow": false,
  "shares": 0,
  "reason": "第三层未确认买入（wait），执行闸门拒绝放行",
  "decision_source": "baseline",
  "cash_available": 50000.0,
  "can_buy_amt": 24000.0,
  "current_total_ratio": 0.32,
  "single_ratio": 0.05,
  "legal_max_shares": 700,
  "market_health_score": 78
}
```

隔离烟测样本 B：第三层 confirm_buy，AI 允许更大仓位，但最终仍被裁到 legal_max_shares=200
```json
{
  "watch_id": "watch-allow-001",
  "decision_signal_id": "D20260402-TEST-002",
  "code": "600150",
  "name": "中国船舶",
  "account": "real",
  "mode": "trial",
  "allow": true,
  "shares": 200,
  "reason": "第三层确认通过，账户上下文允许试错仓",
  "decision_source": "baseline+ai",
  "ai_allow": true,
  "cash_available": 50000.0,
  "can_buy_amt": 24000.0,
  "current_total_ratio": 0.32,
  "single_ratio": 0.05,
  "legal_max_shares": 200,
  "market_health_score": 78
}
```

## 八、账户选择：sim / real / auto
### 1. 三态账户设计
- `sim`：固定模拟盘。
- `real`：固定实盘。
- `auto`：默认回落模拟盘，满足严苛条件时才升到实盘。

### 2. 当前默认配置
- `WATCH_CONFIRM_TARGET_ACCOUNT=auto`
- `WATCH_CONFIRM_TARGET_MODE=trial`
- `AUTO_REAL_ENABLED=0`
- `AUTO_REAL_MIN_CONFIDENCE=high`
- `AUTO_REAL_MIN_MARKET_SCORE=60`
- `AUTO_REAL_FORCE_TRIAL=1`
- `AUTO_REAL_MIN_SHARES=100`

当前虽然是 auto，但默认仍然安全回落到模拟盘。

### 3. auto 升实盘规则
`_auto_watch_real_eligible(...)` 至少会要求：
- `AUTO_REAL_ENABLED=1`
- 第三层 `decision=confirm_buy`
- 第三层 `risk_flag=false`
- 第三层置信度达到 `AUTO_REAL_MIN_CONFIDENCE`
- 市场健康分 >= `AUTO_REAL_MIN_MARKET_SCORE`
- 实盘无未关闭风险事件
- 实盘合法股数 >= `AUTO_REAL_MIN_SHARES`
- 第四层在 real 上重新评估后仍然 `allow=true`

即便满足这些条件：
- 如果 `AUTO_REAL_FORCE_TRIAL=1`，也只会升到 `real + trial`，不会自动重仓。

## 九、执行层：从允许执行到真正写账本
### 1. 主文件只做包装，真实执行在 qt_execution.py
主文件 `_execute_buy()` / `_execute_sell()` 负责把上下文收集完整，然后透传给执行层。真实的：
- 锁
- 幂等
- 订单事件
- 账本提交
- 交易日志
都在 `qt_execution.py`。

### 2. 买入大致顺序
```text
build_idempotency_key
 -> claim_execution_idempotency
 -> append_order_event(status=pending)
 -> append_order_event(status=processing)
 -> read_account_state
 -> commit_account_ledger
 -> append_trade_log
 -> append_order_event(status=done)
 -> release_execution_idempotency
 -> push 买入成功
```

### 3. 卖出大致顺序
卖出和买入类似，但会额外处理：
- 持仓数量检查
- `pnl / pnl_pct` 计算
- `exit_reason_tag` 标准化
- `resolve_risk_events()` 回收已触发风险事件

### 4. 订单状态机
当前允许的状态跳转是：
- `__new__ -> pending / processing / rejected / failed / done`
- `pending -> processing / rejected / failed`
- `processing -> partial / done / rejected / failed`
- `partial -> processing / done / rejected / failed`
- `done / rejected / failed` 之后不允许乱跳

真实订单事件尾样：
```json
[
  {
    "event_id": "OE-20260402202448481873-f9a30c",
    "order_id": "ORD-20260402202448481845-59429b",
    "time": "2026-04-02 20:24:48",
    "status": "pending",
    "prev_status": null,
    "side": "sell",
    "code": "600030",
    "account": "sim",
    "source": "web_panel",
    "signal_id": null,
    "idempotency_key": "2026-04-02|sim|600030|sell|25.200|300|web_panel",
    "price": 25.2,
    "volume": 300
  },
  {
    "event_id": "OE-20260402202448482182-ba1435",
    "order_id": "ORD-20260402202448481845-59429b",
    "time": "2026-04-02 20:24:48",
    "status": "processing",
    "prev_status": "pending",
    "side": "sell",
    "code": "600030",
    "account": "sim",
    "source": "web_panel",
    "signal_id": null,
    "idempotency_key": "2026-04-02|sim|600030|sell|25.200|300|web_panel",
    "price": 25.2,
    "volume": 300,
    "pre_cash": 80000.0
  },
  {
    "event_id": "OE-20260402202448482441-456b96",
    "order_id": "ORD-20260402202448481845-59429b",
    "time": "2026-04-02 20:24:48",
    "status": "done",
    "prev_status": "processing",
    "side": "sell",
    "code": "600030",
    "account": "sim",
    "source": "web_panel",
    "signal_id": null,
    "idempotency_key": "2026-04-02|sim|600030|sell|25.200|300|web_panel",
    "pre_cash": 80000.0,
    "post_cash": 90000.0,
    "post_volume": 0,
    "message": "全部清仓成功"
  }
]
```

### 5. 交易日志现状
`trade_log.jsonl` 当前历史样本大多还是旧结构；新代码路径已经支持写入：
- `entry_signal_id`
- `watch_id`
- `gate_decision_source`
- `entry_mode`
- `exit_reason_tag`

但截至当前真实样本，这些新字段还没有在正式交易数据里沉淀出来，所以回测脚本暂时仍看到 0。

真实交易日志尾样：
```json
[
  {
    "time": "2026-04-01 09:44:32",
    "action": "买入",
    "code": "002714",
    "name": "牧原股份",
    "account": "sim",
    "price": 41.58,
    "volume": 200,
    "amount": 8316.0
  },
  {
    "trade_id": "TRD-20260402113643483122-d0b848",
    "time": "2026-04-02 11:36:43",
    "action": "全部清仓",
    "code": "002714",
    "name": "牧原股份",
    "account": "sim",
    "price": 43.88,
    "volume": 200,
    "amount": 8776.0,
    "buy_price": 41.58,
    "pnl": 460.0,
    "pnl_pct": 5.53,
    "order_id": "ORD-20260402113643439844-82f1cb",
    "signal_id": "WEB_20260402_113634_sim_sell_002714",
    "source": "remote_command"
  },
  {
    "trade_id": "TRD-20260402132945998594-eb140f",
    "time": "2026-04-02 13:29:45",
    "action": "全部清仓",
    "code": "601615",
    "name": "明阳智能",
    "account": "sim",
    "price": 16.36,
    "volume": 500,
    "amount": 8180.0,
    "buy_price": 16.84,
    "pnl": -240.0,
    "pnl_pct": -2.85,
    "order_id": "ORD-20260402132945963308-df8ff5",
    "signal_id": "WEB_20260402_132942_sim_sell_601615",
    "source": "remote_command"
  }
]
```

## 十、风控链、风险事件与退出标签
### 1. 风控主流程
主文件 `check_pnl_alerts()` 会同时监控实盘和模拟盘持仓。主要行为包括：
- 固定止损
- 动态止损/锁利
- 第一止盈
- 第二止盈
- 第三止盈
- 止损延迟警告

### 2. 风险事件日志
所有重要风控事件都会落到 `risk_events.jsonl`。典型字段有：
- `event_type`：如 `tp2_sim`、`stop_sim_固定(-2.5%)`
- `status`：`triggered / resolved`
- `code / account / current_price / buy_price / pnl_pct / threshold_price`
- `order_id / trade_id`：在 resolved 时补充

真实风险事件尾样：
```json
[
  {
    "event_id": "RE-20260402131720644920-5c51b0",
    "time": "2026-04-02 13:17:20",
    "event_type": "stop_sim_固定(-2.5%)",
    "status": "triggered",
    "code": "601615",
    "account": "sim",
    "name": "明阳智能",
    "current_price": 16.37,
    "buy_price": 16.84,
    "pnl_pct": -2.79,
    "threshold_price": 16.42
  },
  {
    "event_id": "RE-20260402132946000296-07569d",
    "time": "2026-04-02 13:29:46",
    "event_type": "stop_sim_固定(-2.5%)",
    "status": "resolved",
    "code": "601615",
    "account": "sim",
    "name": "明阳智能",
    "current_price": 16.37,
    "buy_price": 16.84,
    "pnl_pct": -2.79,
    "threshold_price": 16.42,
    "reason": "sell_confirmed",
    "opened_at": "2026-04-02 13:17:20",
    "order_id": "ORD-20260402132945963308-df8ff5",
    "trade_id": "TRD-20260402132945998594-eb140f"
  },
  {
    "event_id": "RE-20260402194239870173-13d292",
    "time": "2026-04-02 19:42:39",
    "event_type": "stop_real_固定(-2.5%)",
    "status": "triggered",
    "code": "600030",
    "account": "real",
    "name": "????",
    "current_price": 9.6,
    "buy_price": 10.0,
    "pnl_pct": -4.0,
    "threshold_price": 9.75
  },
  {
    "event_id": "RE-20260402194239870816-e4bd7f",
    "time": "2026-04-02 19:42:39",
    "event_type": "tp3_sim",
    "status": "triggered",
    "code": "000001",
    "account": "sim",
    "name": "????",
    "current_price": 11.1,
    "buy_price": 10.0,
    "pnl_pct": 11.0,
    "threshold_price": 10.3
  }
]
```

### 3. 退出原因标准化
当前执行层已经开始支持标准化退出标签，建议长期使用这些值：
- `stop_loss`
- `dynamic_stop`
- `tp1`
- `tp2`
- `tp3`
- `risk_exit`
- `manual_exit`
- `switch_exit`
- `timeout_exit`
- `other_exit`

## 十一、推送系统：什么时候推、推什么、内容长什么样
### 1. push_decision() 不负责判断，只负责发送
`push_decision(title, content, code=None, urgent=False)` 继续只做：
- 冷却控制
- 重试
- Server 酱发送
- 统一日志记录

当前普通推送按 code 冷却；紧急推送可 `urgent=True` 跳过冷却。

### 2. 当前会推送给你的所有情况
- 第三层：`confirm_buy` 默认推；`wait` 只推高价值；`reject` 只推风险型拒绝。
- auto 账户判定：`继续模拟盘` 或 `允许实盘`。
- 第四层：放行 / 拦截。
- 最终执行：买入成功 / 卖出成功 / 执行失败。
- 风控：止损、止盈、止损延迟、系统熔断等。
- 系统/同步异常：远程命令轮询异常、CloudBase 同步异常等。

### 3. 推送正文样例
#### 第四层放行
```text
【第四层放行】
标的：中国船舶(600150)
目标账户：SIM
模式：trial
建议仓位：100股
价格：31.56
原因：第三层确认通过，账户上下文允许试错仓
```

#### 第四层拦截
```text
【第四层拦截】
标的：中国中铁(601390)
目标账户：SIM
结果：block
价格：6.84
原因：现金不足，总仓位受限
```

#### 买入成功
```text
【买入成功】
标的：中航西飞(000768)
账户：SIM
股数：100股
价格：24.18
模式：trial
订单号：ORD-20260402-xxxxxx
```

#### 卖出成功
```text
【卖出成功】
标的：中航西飞(000768)
账户：SIM
股数：100股
价格：25.03
盈亏：+3.5%
原因：达到第一止盈位
```

#### 风控预警
```text
【风控预警】
标的：牧原股份(002714)
账户：SIM
事件：第二止盈触发
状态：+6.28%
原因：达到 tp2 保护位
```

#### 第三层确认通过
```text
【第三层确认通过】
标的：中兴通讯(000063)
结果：confirm_buy
置信度：high
价格：38.42
原因：低点抬高，站回短均线，量能回暖
```

#### 第三层继续观察
```text
【第三层继续观察】
标的：三一重工(600031)
结果：wait
价格：16.88
原因：结构尚未确认，继续观察
```

#### 第三层否决
```text
【第三层否决】
标的：中国铝业(601600)
结果：reject
价格：7.95
原因：再次创出新低，放弃确认
```

#### 账户判定：继续模拟盘
```text
【账户判定：继续模拟盘】
标的：中国船舶(600150)
目标账户：SIM
建议仓位：100股
原因：未满足 AUTO_REAL 条件
```

#### 账户判定：允许实盘
```text
【账户判定：允许实盘】
标的：中国船舶(600150)
目标账户：REAL
模式：trial
建议仓位：100股
原因：满足 AUTO_REAL 条件
```

#### 系统异常
```text
【系统异常】
模块：remote_command
事件：轮询异常
处理：稍后重试
```

#### 同步异常
```text
【同步异常】
模块：CloudBase
事件：All connection attempts failed
处理：稍后重试
```

## 十二、云同步、远程命令与状态文件
### 1. 同步对象
当前会同步到云端的主要集合包括：
- `trade_logs`
- `ai_decisions`
- `daily_reviews`
- `strategy_audits`
- `system_heartbeat`
- `account_snapshots`

### 2. 本地同步机制
- `_cloud_sync_add()`：把新内容放进队列。
- `_cloud_sync_worker()`：后台消费队列。
- `_sync_pending_data()`：启动或周期扫描本地 append-only 文件，把尚未同步的部分补上。
- `_sync_heartbeat()`：定时上传系统心跳和账户快照。
- `_claim_remote_command()` / `_update_command_status()`：远程命令轮询与状态回写。

### 3. cloud_sync_state.json 的作用
这个文件记录“哪些 review/audit 文件已经同步过”，避免每轮都重复入队。当前内容摘录：
```json
{
  "daily_review_keys": [
    "2026-03-30",
    "2026-03-31",
    "2026-04-01",
    "2026-04-02"
  ],
  "strategy_audit_keys": [
    "audit_20260328.txt",
    "audit_20260330.txt",
    "audit_20260331.txt",
    "audit_20260401.txt",
    "audit_20260402.txt"
  ]
}
```

## 十三、当前真实数据快照（2026-04-02）
### 1. 模拟盘余额
```json
{
  "cash": 176142.0,
  "initial_capital": 186727.0,
  "last_update": "2026-04-02 13:29:45"
}
```

### 2. 模拟盘持仓
```json
{
  "position_count": 1,
  "codes": [
    "600030"
  ],
  "detail": {
    "600030": {
      "buy_price": 24.05,
      "volume": 400,
      "timestamp": "2026-03-30 13:27"
    }
  }
}
```

## 十四、沙箱隔离烟测：我这次实际验证了什么
- 样本 A：规则是 `wait`，Gemini 想 `confirm_buy`，最终结果仍然是 `wait`，`decision_source=rule_dominant`。
- 样本 B：第三层未确认买入，第四层直接 `allow=false`。
- 样本 C：AI 想给更大股数，最终仍被裁到 `legal_max_shares=200`。

当前 `tools/backtest_report.py --days 30` 输出为：
```text
=== Phase2 基础回测报告 ===
统计区间：最近 30 天

[交易结果]
总交易日志数：10
已完成卖出样本：5
胜率：20.0%
平均盈利：+5.53%
平均亏损：-2.06%
盈亏比：2.69
带 entry_signal_id 的记录：0
带 watch_id 的记录：0
退出标签分布：unlabeled: 5

[第三层观察确认]
样本数：0（真实正式样本尚未生成）

[第四层执行闸门]
样本数：0（真实正式样本尚未生成）
```

这个结果有一个非常重要的解释：
- `watch_confirm_log.jsonl` 和 `execution_gate_log.jsonl` 目前还没有正式运行样本；因此报表里显示 0。
- `trade_log.jsonl` 里也还没有新字段贯通后的正式样本，所以 `entry_signal_id` 和 `watch_id` 目前也还是 0。
- 这不是代码没接好，而是“真实运行尚未沉淀出新链路样本”。

## 十五、主文件全局变量索引（自动抽取）
```text
_NativeException, DASHBOARD_CACHE, logger, USER_AGENTS, _referer_map, _orig_session_request, _TRADE_LOG_LOCK, _AI_DECISIONS_LOCK, _decision_counter, _LINKED_IDS_CACHE, SYSTEM_GUARD, _STARTUP_CHECK_STATUS, _STOCK_NAME_CACHE, _STOCK_NAME_CACHE_TIME, _STOCK_NAME_REFRESHING, _SECTOR_BOARD_CACHE, _SECTOR_BOARD_CACHE_TIME, _STOCK_INDUSTRY_CACHE, _HOLDINGS_CACHE, _holdings_cache_lock, _log_lock, _alert_cooldown, ALERT_COOLDOWN_SECONDS, _push_cooldown, PUSH_COOLDOWN_SECONDS, PUSH_RETRY_MAX_ATTEMPTS, PUSH_RETRY_BACKOFF_SECONDS, _daily_new_buy, _ai_confidence_cache, _market_data_lock, _sync_queue, _sync_queue_lock, _sync_queue_replace_index, _sync_last_trade_line, _sync_last_decision_line, _LOCAL_SYNC_STATE_FILE, _local_sync_markers, _local_sync_markers_loaded, GLOBAL_MARKET_DATA, MARKET_SOURCE, LAST_CACHE_TIME, _START_TIME, _first_sync_done, app
```

## 十六、主要函数索引（自动抽取）
### Monitor Stable.py
```text
_patched_request, _get_linked_ids, make_tracking_id, get_symbol_execution_lock, get_account_execution_lock, get_account_paths, build_idempotency_key, claim_execution_idempotency, release_execution_idempotency, append_order_event, append_risk_event, mark_risk_event_open, resolve_risk_events, record_provider_result, get_market_health, refresh_system_guard, check_new_buy_guard, startup_self_check, httpx_ask_ollama, _calculate_holding_stats, safe_load_json, read_account_state, get_risk_info, score_stock, get_weakest_holding, get_dashboard_asset_stats, update_balance, commit_account_ledger, get_price_map, _reset_holdings_cache, _get_dashboard_cached_name, _get_dashboard_cached_price, atomic_write_json, _load_local_sync_markers, _persist_local_sync_markers, _artifact_sync_token, get_stock_name, get_cached_holdings, _prune_confidence_cache, append_trade_log
```

### qt_ai_gateway.py
```text
_extract_json_string, resolve_stage_provider, resolve_stage_model, ask_ollama, ask_openrouter, ask_ai
```

### qt_watch_confirm.py
```text
_now, _iso, _normalize_items, _decision_to_status, _apply_refresh_payload, _snapshot_watch_features, append_watch_confirm_log, load_watch_pool, save_watch_pool, evaluate_watch_decision, _build_watch_confirm_prompt, _normalize_ai_watch_decision, _merge_rule_and_ai, evaluate_watch_decision_with_ai, add_watch_candidate, scan_watch_pool, scan_watch_pool_with_ai
```

### qt_execution_gate.py
```text
_now, _iso, _normalize_lot_size, _safe_float, _max_lot_shares, _normalize_account_context, append_execution_gate_log, build_execution_gate_prompt, _normalize_ai_execution_gate, baseline_execution_gate, _merge_gate_with_ai, evaluate_execution_gate
```

### qt_execution.py
```text
bind_trade_log_lock, _make_tracking_id, _get_named_lock, get_symbol_execution_lock, get_account_execution_lock, get_account_paths, _prune_recent_execution_results, build_idempotency_key, claim_execution_idempotency, release_execution_idempotency, append_order_event, read_account_state, update_balance, commit_account_ledger, append_trade_log, _standardize_exit_reason_tag, stable_trade_log_id, read_trade_log, execute_buy, execute_sell
```

### qt_push_templates.py
```text
_clean_text, _fmt_account, _fmt_mode, _fmt_price, _fmt_name, _source_reason, describe_execution_reason, build_execution_gate_allow_message, build_execution_gate_block_message, build_buy_success_message, build_sell_success_message, build_execution_fail_message, build_risk_warning_message, build_watch_confirm_pass_message, build_watch_confirm_wait_message, build_watch_confirm_reject_message, build_auto_account_message, build_system_error_message
```

## 十七、当前已知限制与下一步建议
- 决策关联目前仍偏模糊：买入与 AI 决策的关联还不是完全 signal_id 强绑定。
- `watch_confirm_log.jsonl` 和 `execution_gate_log.jsonl` 的正式样本尚未在真实运行里沉淀。
- 当前 `.env` 是 `WATCH_CONFIRM_TARGET_ACCOUNT=auto + AUTO_REAL_ENABLED=0`，所以 auto 会安全回落模拟盘。
- 旧样本不会自动回填 `entry_signal_id / watch_id / gate_decision_source / entry_mode / exit_reason_tag`。

## 十八、最终结论
```text
这套系统已经从“单层 AI 判断 + 直接执行”演进成了“主审计 -> 第三层观察确认 -> 第四层执行闸门 -> 执行底座 -> 推送 -> 留档 -> 回测”的完整闭环。
```

这意味着：
- 你现在不只是能交易，还能回答“为什么买、为什么没买、为什么卖、哪一层拦了、哪一层放了”。
- 你现在不只是能看日志，还能把一笔交易从 `ai_decision -> watch_confirm -> execution_gate -> trade_log -> risk_event -> push` 串成一条链。
- 你现在不只是能凭感觉优化系统，而是已经具备了继续做统计、回测和归因的基础。

建议把当前版本封存为：
`Phase 2 - Watch Confirm & Execution Gate 基础闭环稳定版`
