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
const TRADE_TYPE_VALUES = new Set(["buy", "sell", "sync"]);
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
  update_status: new Set(["trade_commands"]),
  submit_trade_command: new Set(["trade_commands"]),
  validate_command_token: new Set(["trade_commands"]),
  validate_query_token: new Set(["trade_commands"]),
};
const AUTH_REQUIRED_ACTIONS = new Set(["add", "query_pending", "update_status"]);
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
    if (!prev || curTs >= prevTs) map.set(key, normalized);
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
    if (!prev || curTs >= prevTs) map.set(key, row);
  }
  return Array.from(map.values()).sort((a, b) => {
    const t = String(b.time || "").localeCompare(String(a.time || ""));
    if (t !== 0) return t;
    return String(b.upload_time || "").localeCompare(String(a.upload_time || ""));
  });
}

function normalizeTradeLogRecord(record) {
  const source = record && typeof record === "object" ? record : {};
  const normalized = { ...source };
  if (!String(normalized.trade_id || "").trim()) {
    normalized.trade_id = tradeLogDedupKey(normalized);
  }
  return normalized;
}

function cors(res) {
  res.headers = res.headers || {};
  res.headers["Access-Control-Allow-Origin"] = "*";
  res.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS";
  res.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, x-sync-token, x-command-token, x-query-token";
}

function checkAuth(headers, params) {
  const token =
    headers["x-sync-token"] ||
    headers["X-Sync-Token"] ||
    params?.syncToken ||
    "";
  return String(token || "").trim() === SYNC_TOKEN;
}

function checkCommandAuth(headers, params) {
  const token =
    headers["x-command-token"] ||
    headers["X-Command-Token"] ||
    params?.commandToken ||
    "";
  return String(token || "").trim() === COMMAND_TOKEN;
}

function checkQueryAuth(headers, params) {
  const token =
    headers["x-query-token"] ||
    headers["X-Query-Token"] ||
    params?.queryToken ||
    "";
  return String(token || "").trim() === QUERY_TOKEN;
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

function normalizeIdempotencyKey(value) {
  return String(value || "").trim().slice(0, 200);
}

function validateTradeCommand(data) {
  const source = data && typeof data === "object" ? data : {};
  const type = String(source.type || "").trim();
  const code = String(source.code || "").trim();
  const accountType = String(source.acc_type || "").trim();
  const priceRaw = String(source.price ?? "").trim();
  const volumeRaw = String(source.volume ?? "").trim();

  if (!TRADE_TYPE_VALUES.has(type)) return { error: "invalid trade type" };

  if (type === "sync") {
    return {
      data: {
        type: "sync",
        code: "",
        acc_type: "",
        price: "",
        volume: "",
        status: "pending",
        created_at: new Date().toISOString(),
        source: "cloudbase_frontend",
        idempotency_key: normalizeIdempotencyKey(source.idempotency_key) || `${new Date().toISOString().slice(0, 16)}|sync|web`
      }
    };
  }

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

module.exports = async (event) => {
  const { method, path, headers, body, queryStringParameters: query } = event;
  const res = {
    statusCode: 200,
    headers: { "Content-Type": "application/json" },
    body: ""
  };
  cors(res);

  if (method === "OPTIONS") {
    return res;
  }

  try {
    const params = typeof body === "string" ? JSON.parse(body || "{}") : (body || {});
    const collection = path.replace(/^\//, "") || params.collection;
    const action = String(query?.action || params.action || "").trim();

    if (!collection) {
      res.statusCode = 400;
      res.body = JSON.stringify({ code: 400, message: "missing collection" });
      return res;
    }
    if (!action) {
      res.statusCode = 400;
      res.body = JSON.stringify({ code: 400, message: "missing action" });
      return res;
    }
    if (!isCollectionAllowed(action, collection)) {
      res.statusCode = 403;
      res.body = JSON.stringify({ code: 403, message: "collection not allowed for action" });
      return res;
    }
    if (AUTH_REQUIRED_ACTIONS.has(action) && !checkAuth(headers || {}, params)) {
      res.statusCode = 401;
      res.body = JSON.stringify({ code: 401, message: "Unauthorized" });
      return res;
    }
    if (COMMAND_AUTH_REQUIRED_ACTIONS.has(action) && !checkCommandAuth(headers || {}, params)) {
      res.statusCode = 401;
      res.body = JSON.stringify({ code: 401, message: "Unauthorized" });
      return res;
    }
    if (QUERY_AUTH_REQUIRED_ACTIONS.has(action) && !checkQueryAuth(headers || {}, params)) {
      res.statusCode = 401;
      res.body = JSON.stringify({ code: 401, message: "Unauthorized" });
      return res;
    }
    if (action === "add" && params.data === undefined) {
      res.statusCode = 400;
      res.body = JSON.stringify({ code: 400, message: "missing data" });
      return res;
    }
    if (action === "update_status") {
      if (!params.docId) {
        res.statusCode = 400;
        res.body = JSON.stringify({ code: 400, message: "missing docId" });
        return res;
      }
      if (params.data === undefined) {
        res.statusCode = 400;
        res.body = JSON.stringify({ code: 400, message: "missing data" });
        return res;
      }
    }

    switch (action) {
      case "add": {
        const rawRecords = Array.isArray(params.data) ? params.data : [params.data || params];
        const records = collection === "trade_logs"
          ? rawRecords.map(normalizeTradeLogRecord)
          : rawRecords;
        if (records.length === 0) {
          res.statusCode = 400;
          res.body = JSON.stringify({ code: 400, message: "empty data" });
          return res;
        }
        const results = [];
        for (const record of records) {
          const r = await db.collection(collection).add(record);
          results.push(typeof r.code === "string" ? { ok: false, error: r } : { ok: true, id: r.id });
        }
        const failed = results.filter(r => !r.ok).length;
        res.body = JSON.stringify({ code: 0, message: `ok ${results.length - failed}/${results.length}`, results });
        return res;
      }

      case "query":
      case "secure_query": {
        let q = db.collection(collection);
        const qParams = query || params.query || {};
        if (Object.keys(qParams).length > 0) {
          q = q.where(qParams);
        }
        const orderBy = params.orderBy || query?.orderBy;
        let sortField = "";
        let sortDirection = "desc";
        if (orderBy) {
          if (typeof orderBy === "string") {
            const parts = orderBy.split(":");
            sortField = parts[0];
            sortDirection = parts[1] || "desc";
          } else {
            sortField = orderBy.field;
            sortDirection = orderBy.direction || orderBy.dir || "desc";
          }
        }
        if (collection === "system_heartbeat" && (!sortField || sortField === "upload_time")) {
          sortField = "timestamp";
          sortDirection = "desc";
        }
        if (sortField) q = q.orderBy(sortField, sortDirection);
        const maxLimit = action === "secure_query" ? 300 : 1000;
        const limit = Math.min(Number(query?.limit || params.limit) || 100, maxLimit);
        q = q.limit(limit);
        const result = await q.get();
        let data = result.data;
        if (collection === "system_heartbeat") {
          data = (data || []).slice().sort((a, b) => String(b.timestamp || "").localeCompare(String(a.timestamp || "")));
        }
        if (collection === "daily_reviews" || collection === "strategy_audits") {
          data = dedupeLatestByDate(data, "date");
        }
        if (collection === "trade_logs") {
          data = dedupeTradeLogs(data);
        }
        res.body = JSON.stringify({ code: 0, data });
        return res;
      }

      case "query_trade_commands": {
        let q = db.collection(collection);
        const qParams = query || params.query || {};
        if (Object.keys(qParams).length > 0) {
          q = q.where(qParams);
        }
        const orderBy = params.orderBy || query?.orderBy;
        if (orderBy) {
          if (typeof orderBy === "string") {
            const parts = orderBy.split(":");
            q = q.orderBy(parts[0], parts[1] || "desc");
          } else {
            q = q.orderBy(orderBy.field, orderBy.direction || orderBy.dir || "desc");
          }
        }
        const limit = Math.min(Number(query?.limit || params.limit) || 100, 200);
        q = q.limit(limit);
        const result = await q.get();
        const data = (result.data || []).map(sanitizeTradeCommandView);
        res.body = JSON.stringify({ code: 0, data });
        return res;
      }

      case "query_pending": {
        const limit = Math.min(Number(query?.limit || params.limit) || 10, 50);
        const result = await db
          .collection(collection)
          .where({ status: "pending" })
          .orderBy("created_at", "asc")
          .limit(limit)
          .get();
        res.body = JSON.stringify({ code: 0, data: result.data });
        return res;
      }

      case "submit_trade_command": {
        const validated = validateTradeCommand(params.data);
        if (validated.error) {
          res.statusCode = 400;
          res.body = JSON.stringify({ code: 400, message: validated.error });
          return res;
        }
        const existed = await db.collection(collection)
          .where({ idempotency_key: validated.data.idempotency_key })
          .orderBy("created_at", "desc")
          .limit(1)
          .get();
        if (existed.data && existed.data.length > 0) {
          const row = existed.data[0];
          if (row.status !== "rejected") {
            res.body = JSON.stringify({ code: 0, id: row._id, message: "duplicate", duplicate: true, status: row.status });
            return res;
          }
        }
        const result = await db.collection(collection).add(validated.data);
        res.body = JSON.stringify({ code: 0, id: result.id, message: "submitted" });
        return res;
      }

      case "validate_command_token": {
        res.body = JSON.stringify({ code: 0, message: "ok" });
        return res;
      }

      case "validate_query_token": {
        res.body = JSON.stringify({ code: 0, message: "ok" });
        return res;
      }

      case "update_status": {
        const sanitized = sanitizeStatusUpdate(params.data);
        if (sanitized.error) {
          res.statusCode = 400;
          res.body = JSON.stringify({ code: 400, message: sanitized.error });
          return res;
        }
        const result = await db.collection(collection).doc(params.docId).update(sanitized.data);
        res.body = JSON.stringify({ code: 0, updated: result.updated });
        return res;
      }

      default:
        res.statusCode = 400;
        res.body = JSON.stringify({ code: 400, message: `unknown action: ${action}` });
        return res;
    }
  } catch (err) {
    res.statusCode = 500;
    res.body = JSON.stringify({ code: -1, message: err.message || String(err) });
    return res;
  }
};
