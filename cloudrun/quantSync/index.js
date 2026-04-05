const cloudbase = require("@cloudbase/node-sdk");

function getRequiredEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

const ENV_ID = getRequiredEnv("ENV_ID");
const SYNC_TOKEN = getRequiredEnv("SYNC_TOKEN");
const COMMAND_TOKEN = getRequiredEnv("COMMAND_TOKEN");
const QUERY_TOKEN = String(process.env.QUERY_TOKEN || COMMAND_TOKEN).trim();

const app = cloudbase.init({
  env: ENV_ID
});

const db = app.database();
const STATUS_VALUES = new Set(["pending", "processing", "done", "rejected"]);
const DEFAULT_ALLOWED_ORIGINS = [
  "https://oxesorb-star.github.io",
  "http://localhost:5000",
  "http://127.0.0.1:5000",
];
const COMMAND_STATUS_TRANSITIONS = {
  __new__: new Set(["pending", "processing", "done", "rejected"]),
  pending: new Set(["processing", "rejected"]),
  processing: new Set(["processing", "done", "rejected"]),
  done: new Set([]),
  rejected: new Set([]),
};
const TRADE_TYPE_VALUES = new Set(["buy", "sell"]);
const ACCOUNT_TYPE_VALUES = new Set(["sim", "real"]);
const ACTION_COLLECTION_ALLOWLIST = {
  add: new Set([
    "system_heartbeat",
    "account_snapshots",
    "daily_reviews",
    "strategy_audits",
    "ai_decisions",
    "trade_logs",
    "watch_confirm_logs",
    "execution_gate_logs",
  ]),
  query: new Set([
    "system_heartbeat",
    "account_snapshots",
    "daily_reviews",
    "trade_logs",
  ]),
  secure_query: new Set([
    "ai_decisions",
    "watch_confirm_logs",
    "execution_gate_logs",
    "strategy_audits",
  ]),
  query_trade_commands: new Set(["trade_commands"]),
  query_pending: new Set(["trade_commands"]),
  claim_command: new Set(["trade_commands"]),
  update_status: new Set(["trade_commands"]),
  submit_trade_command: new Set(["trade_commands"]),
  validate_command_token: new Set(["trade_commands"]),
  validate_query_token: new Set(["trade_commands"]),
};
const AUTH_REQUIRED_ACTIONS = new Set(["add", "query_pending", "claim_command", "update_status"]);
const COMMAND_AUTH_REQUIRED_ACTIONS = new Set(["submit_trade_command", "validate_command_token", "query_trade_commands"]);
const QUERY_AUTH_REQUIRED_ACTIONS = new Set(["secure_query", "validate_query_token"]);

function normalizeDateKey(value) {
  const s = String(value || "").trim();
  if (/^\d{8}$/.test(s)) {
    return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  }
  return s;
}

function dedupeLatestByDate(rows, dateField = "date") {
  const map = new Map();
  for (const row of rows || []) {
    if (row?.archived) continue;
    const key = normalizeDateKey(row?.[dateField]);
    if (!key) continue;
    const normalized = { ...row, [dateField]: key };
    const prev = map.get(key);
    const curTs = String(normalized.upload_time || "");
    const prevTs = String(prev?.upload_time || "");
    if (!prev || curTs >= prevTs) {
      map.set(key, normalized);
    }
  }
  return Array.from(map.values()).sort((a, b) => {
    const t = String(b.upload_time || "").localeCompare(String(a.upload_time || ""));
    if (t !== 0) return t;
    return String(b[dateField] || "").localeCompare(String(a[dateField] || ""));
  });
}

function tradeLogDedupKey(row) {
  const source = row && typeof row === "object" ? row : {};
  const semanticKey = [
    String(source.time || "").trim(),
    String(source.action || "").trim(),
    String(source.code || "").trim(),
    String(source.account || "").trim(),
    String(source.price ?? "").trim(),
    String(source.volume ?? "").trim(),
    String(source.amount ?? "").trim(),
    String(source.pnl ?? "").trim(),
  ].join("|");
  if (semanticKey.replace(/\|/g, "").trim()) return semanticKey;
  return String(source.trade_id || "").trim();
}

function dedupeTradeLogs(rows) {
  const map = new Map();
  for (const row of rows || []) {
    if (row?.archived) continue;
    const key = tradeLogDedupKey(row);
    if (!key) continue;
    const prev = map.get(key);
    const curTs = String(row?.upload_time || row?.time || "");
    const prevTs = String(prev?.upload_time || prev?.time || "");
    if (!prev || curTs >= prevTs) {
      map.set(key, row);
    }
  }
  return Array.from(map.values()).sort((a, b) => {
    const t = String(b.time || "").localeCompare(String(a.time || ""));
    if (t !== 0) return t;
    return String(b.upload_time || "").localeCompare(String(a.upload_time || ""));
  });
}

function checkAuth(headers, body) {
  const token =
    headers["x-sync-token"] ||
    headers["X-Sync-Token"] ||
    body?.syncToken ||
    "";
  return String(token || "").trim() === SYNC_TOKEN;
}

function isCollectionAllowed(action, collection) {
  return Boolean(ACTION_COLLECTION_ALLOWLIST[action] && ACTION_COLLECTION_ALLOWLIST[action].has(collection));
}

function sanitizeStatusUpdate(data) {
  const source = data && typeof data === "object" ? data : {};
  const status = String(source.status || "").trim();
  if (!STATUS_VALUES.has(status)) {
    return { error: "invalid status" };
  }
  const cleaned = { status };
  if (source.result !== undefined) cleaned.result = String(source.result);
  if (source.executed_at !== undefined) cleaned.executed_at = String(source.executed_at);
  return { data: cleaned };
}

function canTransitionCommandStatus(prevStatus, nextStatus) {
  const prev = String(prevStatus || "__new__").trim() || "__new__";
  return Boolean(COMMAND_STATUS_TRANSITIONS[prev] && COMMAND_STATUS_TRANSITIONS[prev].has(String(nextStatus || "").trim()));
}

function normalizeIdempotencyKey(value) {
  return String(value || "").trim().slice(0, 200);
}

function isStaleProcessing(value, maxAgeSec = 180) {
  if (!value) return true;
  const ts = Date.parse(String(value));
  if (!Number.isFinite(ts)) return true;
  return (Date.now() - ts) / 1000 > maxAgeSec;
}

function checkCommandAuth(headers, body) {
  const token =
    headers["x-command-token"] ||
    headers["X-Command-Token"] ||
    body?.commandToken ||
    "";
  return String(token || "").trim() === COMMAND_TOKEN;
}

function checkQueryAuth(headers, body) {
  const token =
    headers["x-query-token"] ||
    headers["X-Query-Token"] ||
    body?.queryToken ||
    "";
  return String(token || "").trim() === QUERY_TOKEN;
}

function validateTradeCommand(data) {
  const source = data && typeof data === "object" ? data : {};
  const type = String(source.type || "").trim();
  const code = String(source.code || "").trim();
  const accountType = String(source.acc_type || "").trim();
  const priceRaw = String(source.price ?? "").trim();
  const volumeRaw = String(source.volume ?? "").trim();

  if (!TRADE_TYPE_VALUES.has(type)) return { error: "invalid trade type" };
  if (!/^\d{6}$/.test(code)) return { error: "invalid stock code" };
  if (!ACCOUNT_TYPE_VALUES.has(accountType)) return { error: "invalid account type" };

  const cleaned = {
    type,
    code,
    acc_type: accountType,
    status: "pending",
    created_at: new Date().toISOString(),
    source: "controlled_web_app",
    idempotency_key: normalizeIdempotencyKey(source.idempotency_key) || `${new Date().toISOString().slice(0, 16)}|${accountType}|${code}|${type}|${priceRaw || ""}|${volumeRaw || ""}|web`,
  };
  const signalId = String(source.signal_id || source.request_id || "").trim().slice(0, 80);
  if (signalId) cleaned.signal_id = signalId;

  if (type === "buy") {
    const price = Number(priceRaw);
    const volume = Number(volumeRaw);
    if (!Number.isFinite(price) || price <= 0) return { error: "invalid buy price" };
    if (!Number.isInteger(volume) || volume <= 0) return { error: "invalid buy volume" };
    if (volume % 100 !== 0) return { error: "buy volume must be a multiple of 100" };
    cleaned.price = Number(price.toFixed(3));
    cleaned.volume = volume;
    return { data: cleaned };
  }

  if (priceRaw) {
    const sellPrice = Number(priceRaw);
    if (!Number.isFinite(sellPrice) || sellPrice <= 0) return { error: "invalid sell price" };
    cleaned.price = Number(sellPrice.toFixed(3));
  } else {
    cleaned.price = "";
  }

  if (!volumeRaw || Number(volumeRaw) === 0) {
    cleaned.volume = "";
    return { data: cleaned };
  }

  const sellVolume = Number(volumeRaw);
  if (!Number.isInteger(sellVolume) || sellVolume < 0) return { error: "invalid sell volume" };
  if (sellVolume % 100 !== 0) return { error: "sell volume must be a multiple of 100" };
  cleaned.volume = sellVolume;
  return { data: cleaned };
}

function sanitizeTradeCommandView(row) {
  const source = row && typeof row === "object" ? row : {};
  return {
    _id: source._id,
    type: source.type,
    code: source.code,
    price: source.price,
    volume: source.volume,
    acc_type: source.acc_type,
    status: source.status,
    created_at: source.created_at,
    claimed_at: source.claimed_at,
    executed_at: source.executed_at,
    result: source.result,
    signal_id: source.signal_id,
    source: source.source,
  };
}

function getAllowedOrigins() {
  const extra = String(process.env.CORS_ALLOWED_ORIGINS || "").trim();
  const merged = [
    ...DEFAULT_ALLOWED_ORIGINS,
    ...extra.split(",").map(item => item.trim()).filter(Boolean),
  ];
  return Array.from(new Set(merged));
}

function buildCorsHeaders(origin) {
  const allowedOrigins = getAllowedOrigins();
  const normalizedOrigin = String(origin || "").trim();
  const allowOrigin = !normalizedOrigin || allowedOrigins.includes(normalizedOrigin)
    ? (normalizedOrigin || allowedOrigins[0])
    : allowedOrigins[0];
  return {
    "Access-Control-Allow-Origin": allowOrigin,
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Sync-Token,X-Command-Token",
    "Access-Control-Allow-Credentials": "false",
    "Access-Control-Max-Age": "86400",
    Vary: "Origin, Access-Control-Request-Headers, Access-Control-Request-Method",
    "Content-Type": "application/json; charset=utf-8",
  };
}

function withCors(body, origin) {
  return {
    statusCode: 200,
    headers: buildCorsHeaders(origin),
    body: JSON.stringify(body),
  };
}

function withCorsStatus(statusCode, body, origin) {
  return {
    statusCode,
    headers: buildCorsHeaders(origin),
    body: JSON.stringify(body),
  };
}

function normalizeTradeLogRecord(record) {
  const source = record && typeof record === "object" ? record : {};
  const normalized = { ...source };
  if (!String(normalized.trade_id || "").trim()) {
    normalized.trade_id = tradeLogDedupKey(normalized);
  }
  return normalized;
}

// Event 函数入口 - 也支持通过函数网关调用（网关会把请求体作为 event）
exports.main = async (event, context) => {
  // 函数网关调用时，event 可能是 HTTP 请求对象
  let body = event;
  const headers = {};
  const isHttpRequest = event && event.body !== undefined && event.headers !== undefined;
  const method = String(event?.requestContext?.httpMethod || event?.httpMethod || event?.method || "").toUpperCase();
  const origin = String(event?.headers?.origin || event?.headers?.Origin || "").trim();

  if (isHttpRequest && method === "OPTIONS") {
    return withCorsStatus(204, { code: 0, message: "ok" }, origin);
  }
  
  // 检测是否是 HTTP 调用（通过函数网关）
  if (isHttpRequest) {
    // HTTP 调用格式
    try {
      body = typeof event.body === "string" ? JSON.parse(event.body) : event.body;
    } catch (e) {
      return isHttpRequest ? withCorsStatus(400, { code: 400, message: "invalid JSON" }, origin) : { code: 400, message: "invalid JSON" };
    }
    Object.assign(headers, event.headers);
  } else {
    // Event 调用格式 - event 就是请求体
    body = event;
  }

  const reply = (statusCode, payload) => (isHttpRequest ? withCorsStatus(statusCode, payload, origin) : payload);

  const { collection, data, action: dbAction, query, limit, orderBy, docId } = body || {};
  const action = String(dbAction || "").trim();

  if (!action) {
    return reply(400, { code: 400, message: "missing action" });
  }
  if (!collection) {
    return reply(400, { code: 400, message: "missing collection" });
  }
  if (!isCollectionAllowed(action, collection)) {
    return reply(403, { code: 403, message: "collection not allowed for action" });
  }
  if (AUTH_REQUIRED_ACTIONS.has(action) && !checkAuth(headers, body)) {
    return reply(401, { code: 401, message: "Unauthorized" });
  }
  if (COMMAND_AUTH_REQUIRED_ACTIONS.has(action) && !checkCommandAuth(headers, body)) {
    return reply(401, { code: 401, message: "Unauthorized" });
  }
  if (QUERY_AUTH_REQUIRED_ACTIONS.has(action) && !checkQueryAuth(headers, body)) {
    return reply(401, { code: 401, message: "Unauthorized" });
  }
  if (action === "add" && data === undefined) {
    return reply(400, { code: 400, message: "missing data" });
  }
  if (action === "update_status") {
    if (!docId) {
      return reply(400, { code: 400, message: "missing docId" });
    }
    if (data === undefined) {
      return reply(400, { code: 400, message: "missing data" });
    }
  }
  if (action === "claim_command" && !docId) {
    return reply(400, { code: 400, message: "missing docId" });
  }

  try {
    switch (action) {
      case "add": {
        const rawRecords = Array.isArray(data) ? data : [data];
        const records = collection === "trade_logs"
          ? rawRecords.map(normalizeTradeLogRecord)
          : rawRecords;
        if (records.length === 0) return reply(400, { code: 400, message: "empty data" });
        const results = [];
        const dedupFields = body.dedupFields || []; // 去重字段列表

        for (const record of records) {
          // 如果指定了去重字段，先检查是否存在
          if (dedupFields.length > 0) {
            const dedupQuery = {};
            for (const f of dedupFields) {
              if (record[f] !== undefined) dedupQuery[f] = record[f];
            }
            if (Object.keys(dedupQuery).length > 0) {
              const existing = await db.collection(collection).where(dedupQuery).limit(1).get();
              if (existing.data && existing.data.length > 0) {
                // 【修复】已有记录则更新字段（upsert），而非跳过
                await db.collection(collection).doc(existing.data[0]._id).update(record);
                results.push({ ok: true, id: existing.data[0]._id, updated: true });
                continue;
              }
            }
          }
          const res = await db.collection(collection).add(record);
          if (typeof res.code === "string") {
            results.push({ ok: false, error: res });
          } else {
            results.push({ ok: true, id: res.id });
          }
        }
        const failed = results.filter(r => !r.ok).length;
        const skipped = results.filter(r => r.skipped).length;
        return reply(200, { code: 0, message: `ok ${results.length - failed - skipped}/${results.length}, skipped ${skipped}`, results });
      }

      case "query":
      case "secure_query": {
        let q = db.collection(collection);
        if (query && Object.keys(query).length > 0) q = q.where(query);
        let sortField = orderBy?.field || "";
        let sortDirection = orderBy?.direction || "desc";
        if (collection === "system_heartbeat" && (!sortField || sortField === "upload_time")) {
          sortField = "timestamp";
          sortDirection = "desc";
        }
        if (sortField) q = q.orderBy(sortField, sortDirection);
        const maxLimit = action === "secure_query" ? 300 : 1000;
        const num = Math.min(limit || 100, maxLimit);
        q = q.limit(num);
        const res = await q.get();
        let data = res.data;
        if (collection === "system_heartbeat") {
          data = (data || []).slice().sort((a, b) => String(b.timestamp || "").localeCompare(String(a.timestamp || "")));
        }
        if (collection === "daily_reviews" || collection === "strategy_audits") {
          data = dedupeLatestByDate(data, "date");
        }
        if (collection === "trade_logs") {
          data = dedupeTradeLogs(data);
        }
        return reply(200, { code: 0, data });
      }

      case "validate_query_token":
        return reply(200, { code: 0, message: "ok" });

      case "query_trade_commands": {
        let q = db.collection(collection);
        if (query && Object.keys(query).length > 0) q = q.where(query);
        if (orderBy) q = q.orderBy(orderBy.field, orderBy.direction || "desc");
        const num = Math.min(limit || 100, 200);
        q = q.limit(num);
        const res = await q.get();
        const data = (res.data || []).map(sanitizeTradeCommandView);
        return reply(200, { code: 0, data });
      }

      case "query_pending": {
        const q = db
          .collection(collection)
          .where({ status: "pending" })
          .orderBy("created_at", "asc")
          .limit(Math.min(Number(limit) || 10, 50));
        const pendingRes = await q.get();
        const processingRes = await db
          .collection(collection)
          .where({ status: "processing" })
          .orderBy("claimed_at", "asc")
          .limit(Math.min(Number(limit) || 10, 50))
          .get();
        const staleProcessing = (processingRes.data || []).filter(row => isStaleProcessing(row.claimed_at));
        const combined = [...(pendingRes.data || []), ...staleProcessing]
          .sort((a, b) => String(a.created_at || "").localeCompare(String(b.created_at || "")))
          .slice(0, Math.min(Number(limit) || 10, 50));
        return reply(200, { code: 0, data: combined });
      }

      case "claim_command": {
        const doc = await db.collection(collection).doc(docId).get();
        const row = doc?.data?.[0] || doc?.data;
        if (!row?._id) {
          return reply(404, { code: 404, message: "command not found" });
        }
        if (row.status === "done" || row.status === "rejected") {
          return reply(409, { code: 409, message: "command already finalized" });
        }
        if (row.status === "processing" && !isStaleProcessing(row.claimed_at)) {
          return reply(409, { code: 409, message: "command already claimed" });
        }
        await db.collection(collection).doc(docId).update({
          status: "processing",
          claimed_at: new Date().toISOString(),
          claimed_by: "local_executor",
        });
        return reply(200, { code: 0, claimed: true });
      }

      case "submit_trade_command": {
        const validated = validateTradeCommand(data);
        if (validated.error) {
          return reply(400, { code: 400, message: validated.error });
        }
        const existed = await db.collection(collection)
          .where({ idempotency_key: validated.data.idempotency_key })
          .orderBy("created_at", "desc")
          .limit(1)
          .get();
        if (existed.data && existed.data.length > 0) {
          const row = existed.data[0];
          if (row.status !== "rejected") {
            return reply(200, { code: 0, id: row._id, message: "duplicate", duplicate: true, status: row.status });
          }
        }
        const res = await db.collection(collection).add(validated.data);
        return reply(200, { code: 0, id: res.id, message: "submitted" });
      }

      case "validate_command_token":
        return reply(200, { code: 0, message: "ok" });

      case "update_status": {
        const sanitized = sanitizeStatusUpdate(data);
        if (sanitized.error) {
          return reply(400, { code: 400, message: sanitized.error });
        }
        const doc = await db.collection(collection).doc(docId).get();
        const row = doc?.data?.[0] || doc?.data;
        if (!row?._id) {
          return reply(404, { code: 404, message: "command not found" });
        }
        if (!canTransitionCommandStatus(row.status, sanitized.data.status)) {
          return reply(409, { code: 409, message: `invalid status transition: ${row.status || "__new__"} -> ${sanitized.data.status}` });
        }
        const res = await db.collection(collection).doc(docId).update(sanitized.data);
        return reply(200, { code: 0, updated: res.updated });
      }

      default:
        return reply(400, { code: 400, message: `unknown action: ${action}` });
    }
  } catch (err) {
    return reply(500, { code: -1, message: err.message || String(err) });
  }
};
