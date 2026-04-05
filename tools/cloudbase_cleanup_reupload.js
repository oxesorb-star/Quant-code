const fs = require("fs");
const path = require("path");

function getRequiredEnv(name) {
  const value = String(process.env[name] || "").trim();
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

const ROOT = process.cwd();
const ENV_ID = getRequiredEnv("TCB_ENV_ID");
const TCB_PUBLISHABLE_KEY = getRequiredEnv("TCB_PUBLISHABLE_KEY");
const FUNCTION_URL = getRequiredEnv("TCB_FUNCTION_URL");
const DAILY_DIR = path.join(ROOT, "Daily_Review");
const AUDIT_DIR = path.join(ROOT, "Strategy_Review");
const MODE = String(process.argv[2] || "probe").toLowerCase();

function normalizeDateKey(value) {
  const s = String(value || "").trim();
  if (/^\d{8}$/.test(s)) {
    return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  }
  return s;
}

function nowText() {
  const d = new Date();
  const pad = n => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

async function callGateway(body) {
  const res = await fetch(FUNCTION_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${TCB_PUBLISHABLE_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch (err) {
    throw new Error(`网关返回非 JSON: ${text.slice(0, 300)}`);
  }
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${JSON.stringify(json)}`);
  }
  return json;
}

function buildKeepMap(rows, keyFn) {
  const keep = new Map();
  for (const row of rows || []) {
    const key = keyFn(row);
    if (!key) continue;
    const prev = keep.get(key);
    const rowTs = String(row.upload_time || "");
    const prevTs = String(prev?.upload_time || "");
    if (!prev || rowTs >= prevTs) {
      keep.set(key, row);
    }
  }
  return keep;
}

function dailyKey(row) {
  if (row?.archived) return "";
  return normalizeDateKey(row?.date);
}

function auditKey(row) {
  if (row?.archived) return "";
  return `${normalizeDateKey(row?.date)}|${String(row?.filename || "").trim()}`;
}

async function fetchCollection(collection, limit = 500) {
  const result = await callGateway({
    action: "query",
    collection,
    limit,
    orderBy: { field: "upload_time", direction: "desc" },
  });
  if (result.code !== 0) {
    throw new Error(`${collection} 查询失败: ${JSON.stringify(result)}`);
  }
  return Array.isArray(result.data) ? result.data : [];
}

async function tryDirectDelete(collection, docId, db) {
  try {
    const result = await db.collection(collection).doc(docId).remove();
    return {
      ok: Number(result?.deleted || 0) > 0,
      result,
      mode: "delete",
    };
  } catch (error) {
    return {
      ok: false,
      error: error && error.message ? error.message : String(error),
      mode: "delete",
    };
  }
}

async function archiveDuplicate(collection, row) {
  const archiveDate = `__archived__${normalizeDateKey(row.date || "unknown")}__${String(row._id || "").slice(-8)}`;
  const result = await callGateway({
    action: "update",
    collection,
    docId: row._id,
    data: {
      date: archiveDate,
      content: `[ARCHIVED_DUPLICATE] ${normalizeDateKey(row.date)} ${row.filename || ""}`.trim(),
      archived: true,
      archived_reason: "duplicate_cleanup",
      original_date: row.date || "",
      original_filename: row.filename || "",
      archived_at: nowText(),
      upload_time: "2000-01-01 00:00:00",
    },
  });
  return {
    ok: result.code === 0,
    result,
    mode: "archive",
  };
}

function readLocalDocs(dir, prefix, collection) {
  if (!fs.existsSync(dir)) return [];
  const files = fs.readdirSync(dir).filter(name => name.startsWith(prefix) && name.endsWith(".txt"));
  return files.map(name => {
    const full = path.join(dir, name);
    const content = fs.readFileSync(full, "utf8");
    const rawDate = name.replace(prefix, "").replace(".txt", "");
    const date = normalizeDateKey(rawDate);
    const base = {
      date,
      content,
      upload_time: nowText(),
    };
    if (collection === "strategy_audits") {
      base.filename = name;
    }
    if (collection === "daily_reviews") {
      base.filename = name;
    }
    return base;
  });
}

async function reuploadLocalDocs(collection, records, dedupFields) {
  if (!records.length) return { count: 0 };
  const result = await callGateway({
    action: "add",
    collection,
    data: records,
    dedupFields,
  });
  if (result.code !== 0) {
    throw new Error(`${collection} 重传失败: ${JSON.stringify(result)}`);
  }
  return { count: records.length, result };
}

async function main() {
  const cloudbase = require("@cloudbase/js-sdk");
  const app = cloudbase.init({
    env: ENV_ID,
    region: "ap-shanghai",
    accessKey: ACCESS_KEY,
    auth: { detectSessionInUrl: true },
  });
  const auth = app.auth();
  const signIn = await auth.signInAnonymously();
  if (signIn?.error) {
    throw new Error(`匿名登录失败: ${JSON.stringify(signIn.error)}`);
  }
  const db = app.database();

  const dailyRows = await fetchCollection("daily_reviews");
  const auditRows = await fetchCollection("strategy_audits");

  const keepDaily = buildKeepMap(dailyRows, dailyKey);
  const keepAudit = buildKeepMap(auditRows, auditKey);
  const deleteDaily = dailyRows.filter(row => {
    const key = dailyKey(row);
    return key && keepDaily.get(key)?._id !== row._id;
  });
  const deleteAudit = auditRows.filter(row => {
    const key = auditKey(row);
    return key && keepAudit.get(key)?._id !== row._id;
  });

  const summary = {
    mode: MODE,
    daily_reviews: {
      total: dailyRows.length,
      keep: keepDaily.size,
      dirty: deleteDaily.length,
      ids: deleteDaily.map(row => row._id),
    },
    strategy_audits: {
      total: auditRows.length,
      keep: keepAudit.size,
      dirty: deleteAudit.length,
      ids: deleteAudit.map(row => row._id),
    },
  };

  if (MODE === "probe") {
    console.log(JSON.stringify(summary, null, 2));
    return;
  }

  if (MODE !== "run") {
    throw new Error(`未知模式: ${MODE}，只支持 probe / run`);
  }

  const deleted = [];
  for (const row of deleteDaily) {
    let action = await tryDirectDelete("daily_reviews", row._id, db);
    if (!action.ok) {
      action = await archiveDuplicate("daily_reviews", row);
    }
    deleted.push({ collection: "daily_reviews", _id: row._id, ok: action.ok, mode: action.mode, detail: action.result || action.error || null });
  }
  for (const row of deleteAudit) {
    let action = await tryDirectDelete("strategy_audits", row._id, db);
    if (!action.ok) {
      action = await archiveDuplicate("strategy_audits", row);
    }
    deleted.push({ collection: "strategy_audits", _id: row._id, ok: action.ok, mode: action.mode, detail: action.result || action.error || null });
  }

  const failedDelete = deleted.filter(item => !item.ok);
  if (failedDelete.length) {
    throw new Error(`以下文档删除失败，已停止重传: ${JSON.stringify(failedDelete, null, 2)}`);
  }

  const localDaily = readLocalDocs(DAILY_DIR, "daily_", "daily_reviews");
  const localAudit = readLocalDocs(AUDIT_DIR, "audit_", "strategy_audits");

  const dailyResult = await reuploadLocalDocs("daily_reviews", localDaily, ["date"]);
  const auditResult = await reuploadLocalDocs("strategy_audits", localAudit, ["date", "filename"]);

  const finalState = {
    deleted_count: deleted.length,
    reupload: {
      daily_reviews: dailyResult.count,
      strategy_audits: auditResult.count,
    },
  };
  console.log(JSON.stringify({ summary, finalState }, null, 2));
}

main().catch(err => {
  console.error(err && err.stack ? err.stack : String(err));
  process.exit(1);
});
