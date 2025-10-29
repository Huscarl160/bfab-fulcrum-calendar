// server.js
// Node 18+ (global fetch). package.json: { "type": "module", "start": "node server.js", "engines": { "node": ">=18" } }
// Env vars:
//   FULCRUM_TOKEN (required)
//   FULCRUM_BASE (default: https://api.fulcrumpro.com)
//   ACCESS_KEY (optional) require ?key=... on requests
//   CACHE_TTL_SECONDS (default: 60)
//   CREATED_WINDOW_BUFFER_DAYS (default: 180)

import express from "express";
import crypto from "crypto";

/* -------------------- config -------------------- */
const PORT = process.env.PORT || 8787;
const BASE = process.env.FULCRUM_BASE || "https://api.fulcrumpro.com";
const TOKEN = process.env.FULCRUM_TOKEN;
const ACCESS_KEY = process.env.ACCESS_KEY || null;
const CACHE_TTL_SECONDS = Number(process.env.CACHE_TTL_SECONDS || 60);
const CREATED_WINDOW_BUFFER_DAYS = Number(process.env.CREATED_WINDOW_BUFFER_DAYS || 180);

// default statuses (exclude completed)
const DEFAULT_STATUSES = ["scheduled", "inProgress"];

if (!TOKEN) {
  console.error("Missing FULCRUM_TOKEN env var. Exiting.");
  process.exit(1);
}

/* -------------------- express -------------------- */
const app = express();
app.get("/", (req, res) => res.send("OK"));
app.get("/health", (req, res) => res.json({ ok: true, time: new Date().toISOString() }));
app.get("/wake", (req, res) => res.send("awake")); // for uptime pings

/* -------------------- helpers -------------------- */
function icsEscape(s = "") {
  return String(s || "").replace(/([,;])/g, "\\$1").replace(/\n/g, "\\n");
}
function toUTC(dt) {
  const d = new Date(dt);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
}
function vevent({ uid, start, end, summary, location, description, categories, allDay = false }) {
  const lines = [
    "BEGIN:VEVENT",
    `UID:${uid}`,
    `DTSTAMP:${toUTC(Date.now())}`,
  ];

  if (allDay) {
    // DTSTART;VALUE=DATE:YYYYMMDD  / DTEND;VALUE=DATE:YYYYMMDD (exclusive)
    const s = new Date(start);
    const e = new Date(end || start);
    const pad = (n) => String(n).padStart(2, "0");
    const dstr = (d) => `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}`;
    lines.push(`DTSTART;VALUE=DATE:${dstr(s)}`);
    // DTEND is exclusive; add 1 day
    const e2 = new Date(e);
    e2.setUTCDate(e2.getUTCDate() + 1);
    lines.push(`DTEND;VALUE=DATE:${dstr(e2)}`);
  } else {
    lines.push(`DTSTART:${toUTC(start)}`);
    lines.push(`DTEND:${toUTC(end || start)}`);
  }

  lines.push(`SUMMARY:${icsEscape(summary || "Scheduled Work")}`);
  if (location) lines.push(`LOCATION:${icsEscape(location)}`);
  if (description) lines.push(`DESCRIPTION:${icsEscape(description)}`);
  if (categories && categories.length) lines.push(`CATEGORIES:${categories.map(icsEscape).join(",")}`);
  lines.push("END:VEVENT");
  return lines.join("\r\n");
}

async function postJson(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  return res.json();
}

// fetch with timeout
async function postJsonWithTimeout(path, body, ms = 8000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const res = await fetch(`${BASE}${path}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${TOKEN}`,
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body || {}),
      signal: ctrl.signal,
    });
    if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
    return res.json();
  } finally {
    clearTimeout(t);
  }
}

function unwrapItems(raw) {
  if (Array.isArray(raw)) return raw;
  return raw?.items || raw?.results || raw?.data || [];
}

// RFC5545 folding
function foldLines(text) {
  const out = [];
  for (const line of text.split("\r\n")) {
    let cur = line;
    while (Buffer.byteLength(cur, "utf8") > 75) {
      let cut = 75;
      while (cut > 0 && Buffer.byteLength(cur.slice(0, cut), "utf8") > 75) cut--;
      out.push(cur.slice(0, cut));
      cur = " " + cur.slice(cut);
    }
    out.push(cur);
  }
  return out.join("\r\n");
}
function finalizeIcs(ics) {
  if (!ics.endsWith("\r\n")) ics += "\r\n";
  return foldLines(ics);
}

// tiny p-limit
function pLimit(concurrency) {
  const queue = [];
  let active = 0;
  const next = () => {
    if (active >= concurrency || queue.length === 0) return;
    active++;
    const { fn, resolve, reject } = queue.shift();
    Promise.resolve()
      .then(fn)
      .then((v) => { active--; resolve(v); next(); })
      .catch((e) => { active--; reject(e); next(); });
  };
  return (fn) =>
    new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
      next();
    });
}
const limit8 = pLimit(8);

// simple per-job ops cache (memory)
const OPS_CACHE_TTL_MS = 10 * 60 * 1000; // 10 minutes
const opsCache = new Map(); // jobId -> { at, items }
function getCachedOps(jobId) {
  const hit = opsCache.get(jobId);
  if (hit && (Date.now() - hit.at) < OPS_CACHE_TTL_MS) return hit.items;
  return null;
}
function setCachedOps(jobId, items) {
  opsCache.set(jobId, { at: Date.now(), items });
}

/* ----- status whitelists ----- */
// Job statuses accepted by /api/jobs/list (exact casing depends on API; use lower-case safe set)
const JOB_STATUS_WHITELIST = new Set([
  "scheduled",
  "inprogress",
  "inProgress", // keep both just in case
  "complete",
  "cancelled",
  "canceled",
  "onhold",
  "onHold",
]);
// Operation statuses we might filter client-side AFTER we fetch ops
const OP_STATUS_WHITELIST = new Set([
  "pending",
  "ready",
  "inprogress",
  "inProgress",
  "paused",
  "complete",
  "cancelled",
  "canceled",
]);

/* -------------------- API endpoints (declare ONCE) -------------------- */
const JOBS_LIST = "/api/jobs/list";
const JOB_OPS_LIST = (jobId) => `/api/jobs/${jobId}/operations/list`;

/* -------------------- ops selection & mapping for /calendar.ics -------------------- */
function pickPrimaryOperation(job, ops) {
  if (!Array.isArray(ops) || ops.length === 0) return null;
  const jStart = new Date(job.scheduledStartUtc || job.originalScheduledStartUtc || job.productionDueDate || 0).getTime();
  const jEnd = new Date(job.scheduledEndUtc || job.originalScheduledEndUtc || 0).getTime();

  const candidates = ops
    .filter((o) => o?.scheduledStartUtc || o?.originalScheduledStartUtc)
    .sort((a, b) => {
      const as = new Date(a.scheduledStartUtc || a.originalScheduledStartUtc).getTime();
      const bs = new Date(b.scheduledStartUtc || b.originalScheduledStartUtc).getTime();
      return as - bs;
    });

  if (!candidates.length) return null;
  if (jStart) {
    const overlapping = candidates.find((o) => {
      const os = new Date(o.scheduledStartUtc || o.originalScheduledStartUtc).getTime();
      const oe = new Date(o.scheduledEndUtc || o.originalScheduledEndUtc || os).getTime();
      return jEnd ? os <= jEnd && oe >= jStart : os >= jStart;
    });
    return overlapping || candidates[0];
  }
  return candidates[0];
}

function mapJobToEvent(job, primaryOp, itemToMake) {
  const jobStart = job.scheduledStartUtc || job.originalScheduledStartUtc || job.productionDueDate;
  const jobEnd = job.scheduledEndUtc || job.originalScheduledEndUtc;

  const opStart = primaryOp?.scheduledStartUtc || primaryOp?.originalScheduledStartUtc;
  const opEnd = primaryOp?.scheduledEndUtc || primaryOp?.originalScheduledEndUtc;

  const start = opStart || jobStart;
  let end = opEnd || jobEnd;
  if (!end && start) end = new Date(new Date(start).getTime() + 30 * 60 * 1000).toISOString();

  const title = job.name || (job.number != null ? `Job #${job.number}` : "Scheduled Work");
  const number = job.number != null ? `#${job.number}` : "";
  const status = job.status || "";
  const project = job.salesOrderId || "";

  const equipment = primaryOp?.scheduledEquipmentName || "";
  const opName = primaryOp?.name || "";

  const itemName = itemToMake?.itemReference?.name || itemToMake?.itemReference?.number || "";
  const itemDesc = itemToMake?.itemReference?.description || "";
  const qtyMake = itemToMake?.quantityToMake != null ? `Qty: ${itemToMake.quantityToMake}` : "";

  const summary = [title, number, opName ? `(${opName})` : ""].filter(Boolean).join(" ");
  const location = equipment || "";

  const descLines = [
    status ? `Status: ${status}` : null,
    project ? `Sales Order: ${project}` : null,
    equipment ? `Equipment: ${equipment}` : null,
    opName ? `Operation: ${opName}` : null,
    itemName ? `Item: ${itemName}` : null,
    itemDesc ? `Desc: ${itemDesc}` : null,
    qtyMake || null,
    job.id ? `Job ID: ${job.id}` : null,
  ].filter(Boolean);

  const categories = [equipment || null, opName || null, status || null].filter(Boolean);

  return {
    id: job.id,
    start,
    end,
    summary,
    location,
    description: descLines.join("\\n"), // escaped later
    categories,
  };
}

/* -------------------- tiny per-URL cache -------------------- */
const cache = new Map(); // key: req.url -> { at, body, etag }

/* -------------------- JOB-DRIVEN ICS (unchanged from your behavior) -------------------- */
// /calendar.ics?s=YYYY-MM-DD&u=YYYY-MM-DD&ops=1&statuses=scheduled,in-progress
app.get("/calendar.ics", async (req, res) => {
  try {
    if (ACCESS_KEY && req.query.key !== ACCESS_KEY) return res.sendStatus(403);

    const key = req.url;
    const now = Date.now();
    const hit = cache.get(key);
    if (hit && now - hit.at < CACHE_TTL_SECONDS * 1000) {
      const inm = req.headers["if-none-match"];
      if (inm && inm === hit.etag) return res.status(304).end();
      res.setHeader("Content-Type", "text/calendar; charset=utf-8");
      res.setHeader("Cache-Control", "no-cache");
      res.setHeader("ETag", hit.etag);
      res.setHeader("Content-Disposition", 'inline; filename="bettis-fulcrum.ics"');
      return res.status(200).send(hit.body);
    }

    const since = req.query.s;
    const until = req.query.u;
    const includeOps = req.query.ops === "1";
    const limit = parseInt(req.query.limit || "500", 10);

    const statuses = req.query.statuses
      ? String(req.query.statuses).split(",").map(s => s.trim()).filter(Boolean)
      : DEFAULT_STATUSES.slice();

    const addDays = (dateLike, n) => {
      const x = new Date(dateLike);
      x.setUTCDate(x.getUTCDate() + n);
      return x.toISOString();
    };
    let createdAfterUtc, createdBeforeUtc;
    if (since) createdAfterUtc = addDays(since, -CREATED_WINDOW_BUFFER_DAYS);
    if (until) createdBeforeUtc = addDays(until,  CREATED_WINDOW_BUFFER_DAYS);

    const listBody = { limit };
    if (statuses?.length) listBody.statuses = statuses;
    if (createdAfterUtc)  listBody.createdAfterUtc  = createdAfterUtc;
    if (createdBeforeUtc) listBody.createdBeforeUtc = createdBeforeUtc;

    const jobsResp = await postJson(JOBS_LIST, listBody);
    const jobs = unwrapItems(jobsResp);

    const primaryOpByJob = new Map();
    if (includeOps) {
      for (const job of jobs) {
        try {
          const opsResp = await postJson(JOB_OPS_LIST(job.id), { limit: 200 });
          const arr = unwrapItems(opsResp);
          const pairs = arr.map((o) => ({ op: o.operation || o, itm: o.itemToMake || null }));
          const primary = pickPrimaryOperation(job, pairs.map((p) => p.op));
          const pair = primary
            ? (pairs.find((p) => p.op?.id === primary.id) || { op: primary, itm: null })
            : null;
          primaryOpByJob.set(job.id, pair);
        } catch {
          primaryOpByJob.set(job.id, null);
        }
      }
    }

    const toMs = (d) => (d ? new Date(d).getTime() : NaN);
    const winStart = since ? new Date(since).getTime() : null;
    const winEnd   = until ? new Date(until).getTime() : null;

    const filteredJobs = jobs.filter((j) => {
      const pair = primaryOpByJob.get(j.id);
      const op   = pair?.op;

      const start =
        op?.scheduledStartUtc || op?.originalScheduledStartUtc ||
        j.scheduledStartUtc   || j.originalScheduledStartUtc   || j.productionDueDate;

      const end =
        op?.scheduledEndUtc || op?.originalScheduledEndUtc ||
        j.scheduledEndUtc   || j.originalScheduledEndUtc   || start;

      if (!start) return false;

      const s = toMs(start);
      const e = toMs(end) || s;

      if (winStart && e < winStart) return false;
      if (winEnd   && s > winEnd)   return false;
      return true;
    });

    const events = filteredJobs.map((j) => {
      const pair = primaryOpByJob.get(j.id);
      const primaryOp = pair?.op || null;
      const itemToMake = pair?.itm || null;
      return mapJobToEvent(j, primaryOp, itemToMake);
    });

    const ics = [
      "BEGIN:VCALENDAR",
      "VERSION:2.0",
      "PRODID:-//Bettis//Fulcrum Jobs Schedule//EN",
      "CALSCALE:GREGORIAN",
      "METHOD:PUBLISH",
      "X-WR-CALNAME:Fulcrum Schedule",
      "X-WR-TIMEZONE:UTC",
      "REFRESH-INTERVAL;VALUE=DURATION:PT1H",
      "X-PUBLISHED-TTL:PT1H",
      ...events.map((e) =>
        vevent({
          uid: crypto.createHash("sha1").update(`fulcrum:${e.id}`).digest("hex") + "@bettis",
          start: e.start,
          end: e.end,
          summary: e.summary,
          location: e.location,
          description: e.description,
          categories: e.categories,
        })
      ),
      "END:VCALENDAR",
    ].join("\r\n");

    const safeIcs = finalizeIcs(ics);

    const etag = 'W/"' + crypto.createHash("sha1").update(safeIcs).digest("hex") + '"';
    cache.set(key, { at: now, body: safeIcs, etag });

    res.setHeader("Content-Type", "text/calendar; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("ETag", etag);
    res.setHeader("Content-Disposition", 'inline; filename="bettis-fulcrum.ics"');
    res.status(200).send(safeIcs);
  } catch (err) {
    res.status(500).send(`Error: ${err.message}`);
  }
});

/* -------------------- OPS-DRIVEN ICS (fast + cached) -------------------- */
// Stable URL defaults: ?windowBefore=30&windowAfter=90&allday=1&statuses=scheduled,inProgress,ready,pending,paused
app.get("/calendar-ops.ics", async (req, res) => {
  try {
    if (ACCESS_KEY && req.query.key !== ACCESS_KEY) return res.sendStatus(403);

    // ----- parse window -----
    const today = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    const ymd = (d) => `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;

    const wb = Number(req.query.windowBefore || 30);
    const wa = Number(req.query.windowAfter || 90);

    const since = req.query.s || ymd(new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() - wb)));
    const until = req.query.u || ymd(new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() + wa)));
    const allDay = req.query.allday === "1";

    const limit = parseInt(req.query.limit || "300", 10);

    // ----- statuses: split into job vs op buckets, whitelist each -----
    const rawStatuses = req.query.statuses
      ? String(req.query.statuses).split(",").map(s => s.trim()).filter(Boolean)
      : [];

    const jobStatuses = rawStatuses
      .filter(s => JOB_STATUS_WHITELIST.has(s) || JOB_STATUS_WHITELIST.has(s.toLowerCase()))
      .map(s => (s === "inprogress" ? "inProgress" : s)); // normalize

    const opStatuses = rawStatuses
      .filter(s => OP_STATUS_WHITELIST.has(s) || OP_STATUS_WHITELIST.has(s.toLowerCase()))
      .map(s => (s === "inprogress" ? "inProgress" : s));

      // parse optional filters
      const opNames = (req.query.opNames || "")
      .split(",")
      .map(s => s.trim())
      .filter(Boolean)
      .map(s => s.toLowerCase());

      const equipFilter = (req.query.equip || "").trim().toLowerCase();

      // ... after we get `arr` (all ops for the job), replace arrFiltered definition:

      let arrFiltered = arr;

      // filter by op status (existing)
      if (opStatuses.length) {
      arrFiltered = arrFiltered.filter(o =>
        opStatuses.includes(String(o.status || ""))
      );
      }

      // filter by operation names (case-insensitive, partial match OK)
      if (opNames.length) {
      arrFiltered = arrFiltered.filter(o => {
        const name = String(o.name || "").toLowerCase();
        return opNames.some(n => name.includes(n));
      });
      }

      // filter by equipment code/name (case-insensitive contains)
      if (equipFilter) {
      arrFiltered = arrFiltered.filter(o => {
        const eq = String(o.scheduledEquipmentName || "").toLowerCase();
        return eq.includes(equipFilter);
      });
      }

    // ----- build jobs list body (created window expansion) -----
    const addDays = (dateLike, n) => {
      const x = new Date(dateLike);
      x.setUTCDate(x.getUTCDate() + n);
      return x.toISOString();
    };
    const listBody = { limit };
    const buf = Number(process.env.CREATED_WINDOW_BUFFER_DAYS || 180);
    if (since) listBody.createdAfterUtc  = addDays(since, -buf);
    if (until) listBody.createdBeforeUtc = addDays(until,  buf);
    if (jobStatuses.length) listBody.statuses = jobStatuses;

    // ----- fetch jobs -----
    const jobsResp = await postJson(JOBS_LIST, listBody);
    const jobs = unwrapItems(jobsResp);

    // ----- fetch ops per job (parallel, capped, cached, with timeout) -----
    const primaryOpByJob = new Map();

    await Promise.all(
      jobs.map((job) =>
        limit8(async () => {
          let arr = getCachedOps(job.id);
          if (!arr) {
            try {
              const resp = await postJsonWithTimeout(JOB_OPS_LIST(job.id), { limit: 500 }, 8000);
              arr = unwrapItems(resp).map(o => o.operation || o);
              setCachedOps(job.id, arr);
            } catch (e) {
              console.warn("ops fetch failed for job", job.id, e.message);
              arr = [];
            }
          }

          const arrFiltered = opStatuses.length
            ? arr.filter(o => opStatuses.includes(String(o.status || "")))
            : arr;

          const primary = pickPrimaryOperation(job, arrFiltered);
          primaryOpByJob.set(job.id, primary ? { op: primary, itm: null } : null);
        })
      )
    );

    // ----- filter by actual schedule window & map -----
    const toMs = (d) => (d ? new Date(d).getTime() : NaN);
    const winStart = new Date(since).getTime();
    const winEnd   = new Date(until).getTime();

    const filteredJobs = jobs.filter((j) => {
      const pair = primaryOpByJob.get(j.id);
      const op   = pair?.op;

      const start =
        op?.scheduledStartUtc || op?.originalScheduledStartUtc ||
        j.scheduledStartUtc   || j.originalScheduledStartUtc   || j.productionDueDate;
      const end =
        op?.scheduledEndUtc || op?.originalScheduledEndUtc ||
        j.scheduledEndUtc   || j.originalScheduledEndUtc   || start;

      if (!start) return false;
      const s = toMs(start);
      const e = toMs(end) || s;
      if (isNaN(s)) return false;
      if (winStart && e < winStart) return false;
      if (winEnd   && s > winEnd)   return false;
      return true;
    });

    const events = filteredJobs.map((j) => {
      const pair = primaryOpByJob.get(j.id);
      const primaryOp = pair?.op || null;
      const evt = mapJobToEvent(j, primaryOp, null);
      // for all-day, clamp to job window if present; else use op window
      if (allDay) {
        const s = primaryOp?.scheduledStartUtc || j.scheduledStartUtc || j.productionDueDate || evt.start;
        const e = primaryOp?.scheduledEndUtc   || j.scheduledEndUtc   || evt.end || s;
        evt.start = s;
        evt.end = e;
      }
      return evt;
    });

    // ----- build calendar -----
    const ics = [
      "BEGIN:VCALENDAR",
      "VERSION:2.0",
      "PRODID:-//Bettis//Fulcrum Ops Schedule//EN",
      "CALSCALE:GREGORIAN",
      "METHOD:PUBLISH",
      "X-WR-CALNAME:Fulcrum Ops",
      "X-WR-TIMEZONE:UTC",
      "REFRESH-INTERVAL;VALUE=DURATION:PT1H",
      "X-PUBLISHED-TTL:PT1H",
      ...events.map((e) =>
        vevent({
          uid: crypto.createHash("sha1").update(`fulcrum:${e.id}:${e.start}:${e.location}`).digest("hex") + "@bettis",
          start: e.start,
          end: e.end,
          summary: e.summary,
          location: e.location,
          description: e.description,
          categories: e.categories,
          allDay,
        })
      ),
      "END:VCALENDAR",
    ].join("\r\n");

    const safeIcs = finalizeIcs(ics);
    res.setHeader("Content-Type", "text/calendar; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Content-Disposition", 'inline; filename="bettis-fulcrum-ops.ics"');
    return res.status(200).send(safeIcs);
  } catch (err) {
    res.status(500).send(`Error: ${err.message}`);
  }
});

/* -------------------- test -------------------- */
app.get("/test.ics", (req, res) => {
  const now = new Date();
  const in30 = new Date(now.getTime() + 30 * 60 * 1000);
  const ics = [
    "BEGIN:VCALENDAR",
    "VERSION:2.0",
    "PRODID:-//Bettis//Fulcrum Test//EN",
    "CALSCALE:GREGORIAN",
    "METHOD:PUBLISH",
    "X-WR-CALNAME:Fulcrum Test",
    "X-WR-TIMEZONE:UTC",
    "BEGIN:VEVENT",
    "UID:test-one@bettis",
    `DTSTAMP:${toUTC(now)}`,
    `DTSTART:${toUTC(now)}`,
    `DTEND:${toUTC(in30)}`,
    "SUMMARY:Test Event (should appear today)",
    "DESCRIPTION:This is a diagnostic VEVENT\\nIf you can see this, Outlook is rendering.",
    "END:VEVENT",
    "END:VCALENDAR",
  ].join("\r\n") + "\r\n";

  res.setHeader("Content-Type", "text/calendar; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache");
  res.status(200).send(ics);
});

/* -------------------- feeds index (operation-specific ICS links) -------------------- */

// Master list of operation names exactly as they appear in Fulcrum
const OP_FEEDS = [
  "Saw",
  "Drill",
  "Plasma Cut",
  "Laser Cut",
  "OS Processing",
  "Shear",
  "Flex",
  "Press Brake",
  "Cobot Weld",
  "Weld",
  "Sand Blast / Clean",
  "Paint",
  "Repair",
  "Trucking",
  "Assemble",
  "CAD / Engineering",
  "Deburr / De-Slag",
  "Packaging",
  "Office / OH / Burden",
];

// Suggest Outlook/SharePoint colors per op (totally optional—edit to taste)
const FEED_COLORS = {
  "Saw": "#3b82f6",
  "Drill": "#0ea5e9",
  "Plasma Cut": "#06b6d4",
  "Laser Cut": "#14b8a6",
  "OS Processing": "#22c55e",
  "Shear": "#84cc16",
  "Flex": "#a3e635",
  "Press Brake": "#eab308",
  "Cobot Weld": "#f59e0b",
  "Weld": "#f97316",
  "Sand Blast / Clean": "#ef4444",
  "Paint": "#dc2626",
  "Repair": "#b91c1c",
  "Trucking": "#8b5cf6",
  "Assemble": "#6366f1",
  "CAD / Engineering": "#4f46e5",
  "Deburr / De-Slag": "#7c3aed",
  "Packaging": "#db2777",
  "Office / OH / Burden": "#475569",
};

function buildBaseUrl(req) {
  const proto = req.headers["x-forwarded-proto"] || req.protocol || "https";
  const host = req.get("host");
  return `${proto}://${host}`;
}

// JSON version if you want to wire anything else up later
app.get("/feeds.json", (req, res) => {
  const base = buildBaseUrl(req);
  const defaultParams =
    "allday=1&statuses=scheduled,inProgress,ready,pending,paused";
  const items = OP_FEEDS.map((name) => {
    const url =
      `${base}/calendar-ops.ics?${defaultParams}&opNames=` +
      encodeURIComponent(name);
    return {
      operation: name,
      url,
      color: FEED_COLORS[name] || "#64748b",
    };
  });
  res.json({ feeds: items });
});

// Pretty HTML index with copy buttons
app.get("/feeds", (req, res) => {
  const base = buildBaseUrl(req);
  const defaultParams =
    "allday=1&statuses=scheduled,inProgress,ready,pending,paused";

  const rows = OP_FEEDS.map((name) => {
    const url =
      `${base}/calendar-ops.ics?${defaultParams}&opNames=` +
      encodeURIComponent(name);
    const color = FEED_COLORS[name] || "#64748b";
    return `
      <tr>
        <td class="op">
          <span class="dot" style="background:${color}"></span>
          ${name}
        </td>
        <td class="url">
          <input type="text" readonly value="${url}">
        </td>
        <td class="actions">
          <button data-url="${url}">Copy</button>
          <a class="open" href="${url}" target="_blank" rel="noopener">Open</a>
        </td>
      </tr>`;
  }).join("");

  const html = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Fulcrum Operation Feeds</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { --bg:#0b1220; --panel:#0f172a; --text:#e5e7eb; --muted:#94a3b8; --accent:#22d3ee; --br:14px; }
  body { margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji"; background:var(--bg); color:var(--text); }
  .wrap { max-width: 1100px; margin: 40px auto; padding: 24px; }
  .card { background:var(--panel); border-radius:var(--br); padding: 24px; box-shadow: 0 10px 30px rgba(0,0,0,0.25); }
  h1 { margin:0 0 6px 0; font-size: 28px; letter-spacing: .3px; }
  p.sub { margin: 0 0 16px 0; color: var(--muted); }
  .tip { font-size: 14px; color: var(--muted); margin-bottom: 18px; }
  table { width:100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 10px 12px; vertical-align: middle; }
  th { color:#cbd5e1; font-weight: 600; border-bottom: 1px solid #1f2937; }
  tr + tr td { border-top: 1px dashed #1f2937; }
  .op { white-space: nowrap; }
  .dot { display:inline-block; width:12px; height:12px; border-radius: 999px; margin-right:8px; vertical-align: -1px; }
  .url input { width:100%; background:#0b1020; color:#e2e8f0; border:1px solid #1f2937; border-radius:8px; padding:8px 10px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size: 13px; }
  .actions { white-space: nowrap; }
  button, a.open {
    display:inline-block; margin-right: 8px; padding: 8px 12px; border-radius: 10px; border: 1px solid #1f2937;
    background: #0b1020; color: #e2e8f0; text-decoration: none; font-weight: 600; font-size: 13px;
  }
  button:hover, a.open:hover { border-color: var(--accent); color: #ecfeff; }
  .footer { margin-top: 12px; font-size: 13px; color: var(--muted); }
  .kbd { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; background:#111827; border:1px solid #1f2937; padding:2px 6px; border-radius:6px; }
</style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>Fulcrum Operation Feeds</h1>
      <p class="sub">Subscribe to operation-specific ICS feeds (all-day; auto-refreshed range; suggested colors included).</p>
      <div class="tip">Tip: In Outlook, use <span class="kbd">Add calendar → Subscribe from web</span>,
      paste a URL, and assign a color. These feeds use <span class="kbd">allday=1</span> and include statuses <span class="kbd">scheduled,inProgress,ready,pending,paused</span>.</div>
      <table>
        <thead>
          <tr><th>Operation</th><th>URL</th><th>Actions</th></tr>
        </thead>
        <tbody>
          ${rows}
        </tbody>
      </table>
      <div class="footer">Base URL: <span class="kbd">${base}</span></div>
    </div>
  </div>
<script>
  document.addEventListener("click", async (e) => {
    if (e.target.matches("button[data-url]")) {
      const url = e.target.getAttribute("data-url");
      try {
        await navigator.clipboard.writeText(url);
        e.target.textContent = "Copied!";
        setTimeout(() => (e.target.textContent = "Copy"), 1200);
      } catch {
        prompt("Copy URL:", url);
      }
    }
  });
</script>
</body>
</html>`;
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.status(200).send(html);
});


/* -------------------- start -------------------- */
app.listen(PORT, () => {
  console.log(`ICS feed running on :${PORT}`);
});
