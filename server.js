// server.js
// Node 18+ (global fetch). package.json: { "type": "module", "start": "node server.js", "engines": { "node": ">=18" } }
// Env:
//   FULCRUM_TOKEN (required)
//   FULCRUM_BASE (default: https://api.fulcrumpro.com)
//   ACCESS_KEY (optional) require ?key=...
//   CACHE_TTL_SECONDS (default: 60)
//   CREATED_WINDOW_BUFFER_DAYS (default: 180)
//   FEEDS_CACHE_TTL_SECONDS (default: 900)

import express from "express";
import crypto from "crypto";

/* -------------------- config -------------------- */
const PORT = process.env.PORT || 8787;
const BASE = process.env.FULCRUM_BASE || "https://api.fulcrumpro.com";
const TOKEN = process.env.FULCRUM_TOKEN;
const ACCESS_KEY = process.env.ACCESS_KEY || null;
const CACHE_TTL_SECONDS = Number(process.env.CACHE_TTL_SECONDS || 60);
const CREATED_WINDOW_BUFFER_DAYS = Number(process.env.CREATED_WINDOW_BUFFER_DAYS || 180);
const FEEDS_CACHE_TTL_SECONDS = Number(process.env.FEEDS_CACHE_TTL_SECONDS || 900); // 15 min
const feedsCache = { at: 0, payload: null };

if (!TOKEN) {
  console.error("Missing FULCRUM_TOKEN env var. Exiting.");
  process.exit(1);
}

/* -------------------- express -------------------- */
const app = express();

app.get("/", (req, res) => res.type("text/plain").send("OK"));
app.get("/health", (req, res) => res.json({ ok: true, time: new Date().toISOString() }));
app.get("/wake", (req, res) => res.type("text/plain").send("awake"));

/* -------------------- helpers -------------------- */
function icsEscape(s = "") {
  return String(s || "").replace(/([,;])/g, "\\$1").replace(/\n/g, "\\n");
}

function toUTC(dt) {
  const d = new Date(dt);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(
    d.getUTCMinutes()
  )}${pad(d.getUTCSeconds())}Z`;
}

function toUTCDateOnly(d) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}`;
}

function vevent({ uid, start, end, summary, location, description, categories, allday = false }) {
  const lines = ["BEGIN:VEVENT", `UID:${uid}`, `DTSTAMP:${toUTC(Date.now())}`];

  if (allday) {
    const ds = toUTCDateOnly(new Date(start));
    const de = toUTCDateOnly(new Date(end));
    lines.push(`DTSTART;VALUE=DATE:${ds}`);
    lines.push(`DTEND;VALUE=DATE:${de}`);
  } else {
    lines.push(`DTSTART:${toUTC(start)}`);
    lines.push(`DTEND:${toUTC(end || start)}`);
  }

  lines.push(`SUMMARY:${icsEscape(summary || "Scheduled Work")}`);
  if (location) lines.push(`LOCATION:${icsEscape(location)}`);
  if (description) lines.push(`DESCRIPTION:${icsEscape(description)}`);
  if (categories && categories.length) lines.push(`CATEGORIES:${categories.map(icsEscape).join(",")}`);

  // Play nice with Outlook and others
  lines.push(`TRANSP:OPAQUE`);
  lines.push(`X-MICROSOFT-CDO-BUSYSTATUS:BUSY`);
  lines.push(`CLASS:PUBLIC`);

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

// window helpers
function defaultWindowISO() {
  const today = new Date();
  const startDefault = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() - 30));
  const endDefault = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() + 180));
  return {
    since: startDefault.toISOString().slice(0, 10),
    until: endDefault.toISOString().slice(0, 10),
  };
}

function addDaysISO(isoDate, days) {
  const d = new Date(isoDate);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString();
}

/* ----- whitelists ----- */
const JOB_STATUS_WHITELIST = new Set([
  "scheduled",
  "inProgress",
  "complete",
  "cancelled",
  "canceled",
  "onHold",
]);

const OP_STATUS_WHITELIST = new Set([
  "pending",
  "ready",
  "inProgress",
  "paused",
  "complete",
  "cancelled",
  "canceled",
]);

/* -------------------- API endpoints -------------------- */
const JOBS_LIST = "/api/jobs/list";
const JOB_OPS_LIST = (jobId) => `/api/jobs/${jobId}/operations/list`;

/* -------------------- job -> event (legacy job-based feed) -------------------- */
function pickPrimaryOperation(job, ops) {
  if (!Array.isArray(ops) || ops.length === 0) return null;

  const jStart = new Date(
    job.scheduledStartUtc || job.originalScheduledStartUtc || job.productionDueDate || 0
  ).getTime();
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
    description: descLines.join("\\n"),
    categories,
  };
}

/* -------------------- tiny per-URL cache -------------------- */
const cache = new Map(); // key: req.url -> { at, body, etag }

/* -------------------- JOB-DRIVEN ICS (kept) -------------------- */
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

    const { since: defS, until: defU } = defaultWindowISO();
    const since = req.query.s || defS;
    const until = req.query.u || defU;
    const includeOps = req.query.ops === "1";
    const limit = parseInt(req.query.limit || "500", 10);

    const statuses = (req.query.statuses ? String(req.query.statuses).split(",") : ["scheduled", "inProgress"])
      .map((s) => s.trim())
      .filter((s) => s.length);

    const listBody = { limit };

    // expand created window
    if (since) listBody.createdAfterUtc = addDaysISO(since, -CREATED_WINDOW_BUFFER_DAYS);
    if (until) listBody.createdBeforeUtc = addDaysISO(until, CREATED_WINDOW_BUFFER_DAYS);

    // job status whitelist
    const jobStatuses = statuses.filter((s) => JOB_STATUS_WHITELIST.has(s));
    if (jobStatuses.length) listBody.statuses = jobStatuses;

    const jobsResp = await postJson(JOBS_LIST, listBody);
    const jobs = unwrapItems(jobsResp);

    const primaryOpByJob = new Map();

    if (includeOps) {
      for (const job of jobs) {
        try {
          const opsResp = await postJson(JOB_OPS_LIST(job.id), { limit: 200 });
          const opsRaw = unwrapItems(opsResp).map((o) => o.operation || o);
          const primary = pickPrimaryOperation(job, opsRaw);
          primaryOpByJob.set(job.id, primary ? { op: primary, itm: null } : null);
        } catch {
          primaryOpByJob.set(job.id, null);
        }
      }
    }

    const toMs = (d) => (d ? new Date(d).getTime() : NaN);
    const winStart = new Date(since).getTime();
    const winEnd = new Date(until).getTime();

    const filteredJobs = jobs.filter((j) => {
      const pair = primaryOpByJob.get(j.id);
      const op = pair?.op;

      const start =
        op?.scheduledStartUtc ||
        op?.originalScheduledStartUtc ||
        j.scheduledStartUtc ||
        j.originalScheduledStartUtc ||
        j.productionDueDate;
      const end =
        op?.scheduledEndUtc ||
        op?.originalScheduledEndUtc ||
        j.scheduledEndUtc ||
        j.originalScheduledEndUtc ||
        start;

      if (!start) return false;
      const s = toMs(start);
      const e = toMs(end) || s;
      if (e < winStart) return false;
      if (s > winEnd) return false;
      return true;
    });

    const events = filteredJobs.map((j) => {
      const pair = primaryOpByJob.get(j.id);
      const primaryOp = pair?.op || null;
      const itemToMake = pair?.itm || null;
      return mapJobToEvent(j, primaryOp, itemToMake);
    });

    const ics =
      [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Bettis//Fulcrum Jobs Schedule//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:Fulcrum Schedule",
        "X-WR-TIMEZONE:UTC",
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

/* -------------------- Aggregation-specific helpers -------------------- */

// Extract an F-style project code like "F251289-1"
function getProjectCode(job) {
  const candidates = [
    job?.projectNumber,
    job?.jobNumber,
    job?.numberFormatted,
    job?.numberString,
    job?.salesOrderId,
    job?.orderNumber,
    job?.number,
    job?.customFields?.projectNumber,
    job?.name,
  ]
    .filter(Boolean)
    .map(String);

  const rxStrict = /^F\d{6}(?:-\d+)?$/i;      // F + 6 digits, optional -suffix
  const rxLoose = /(F\d{6}(?:-\d+)?)/i;

  for (const c of candidates) {
    const mStrict = c.match(rxStrict);
    if (mStrict) return mStrict[0].toUpperCase();
    const mLoose = c.match(rxLoose);
    if (mLoose) return mLoose[1].toUpperCase();
  }

  if (job?.number && isNaN(Number(job.number))) return String(job.number).toUpperCase();
  const tail = String(job?.id ?? "").slice(-4) || "0000";
  return `F??${tail}`.toUpperCase();
}

// Format: MM-DD-YYYY, HH:MM (UTC, 24h)
function humanShortUTC(iso) {
  const d = new Date(iso);
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}-${d.getUTCFullYear()}, ${pad(d.getUTCHours())}:${pad(
    d.getUTCMinutes()
  )}`;
}

// "<number> (<name[, desc]>)" with fallbacks
function buildItemLabelPretty(op) {
  const r = op?.itemReference || {};
  const number = r.number || r.itemNumber || r.code || r.sku || "";
  const name = r.name || r.itemName || "";
  const desc = r.description || r.itemDescription || "";
  const paren = [name, desc].filter(Boolean).join(", ");
  if (number && paren) return `${number} (${paren})`;
  if (number) return number;
  if (paren) return `(${paren})`;
  return op?.name || op?.description || (op?.id ? `Item ${op.id}` : "Item");
}

// Try to extract a clean client/customer label
function getClientName(job) {
  return (
    job?.customerName ||
    job?.customer?.name ||
    job?.customer?.displayName ||
    job?.client ||
    job?.accountName ||
    job?.customer ||
    null
  );
}

// One line per item inside the aggregated DESCRIPTION (no URL for now)
function opLine(job, op, startIso, endIso) {
  const label = buildItemLabelPretty(op);
  const when = `${humanShortUTC(startIso)} → ${humanShortUTC(endIso)}`;
  const equip = op?.scheduledEquipmentName ? ` | ${op.scheduledEquipmentName}` : "";
  return `- ${label} | ${when}${equip}`;
}

/* -------------------- OPS-DRIVEN ICS (aggregated per job × op) -------------------- */
// /calendar-ops.ics?s=YYYY-MM-DD&u=YYYY-MM-DD&allday=1&only=Saw&statuses=ready,inProgress,paused,pending
app.get("/calendar-ops.ics", async (req, res) => {
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
      res.setHeader("Content-Disposition", 'inline; filename="bettis-fulcrum-ops.ics"');
      return res.status(200).send(hit.body);
    }

    const { since: defS, until: defU } = defaultWindowISO();
    const since = req.query.s || defS;
    const until = req.query.u || defU;
    const limit = parseInt(req.query.limit || "500", 10);
    const wantAllDay = req.query.allday === "1";

    const rawStatuses = req.query.statuses
      ? String(req.query.statuses).split(",").map((s) => s.trim()).filter(Boolean)
      : [];
    const jobStatuses = rawStatuses.filter((s) => JOB_STATUS_WHITELIST.has(s));
    const opStatuses = rawStatuses.filter((s) => OP_STATUS_WHITELIST.has(s));

    const onlyOp = req.query.only ? String(req.query.only).toLowerCase() : null;

    // Build job list using a created window that pads the schedule window
    const listBody = { limit };
    if (since) listBody.createdAfterUtc = addDaysISO(since, -CREATED_WINDOW_BUFFER_DAYS);
    if (until) listBody.createdBeforeUtc = addDaysISO(until, CREATED_WINDOW_BUFFER_DAYS);
    if (jobStatuses.length) listBody.statuses = jobStatuses;

    const jobsResp = await postJson(JOBS_LIST, listBody);
    const jobs = unwrapItems(jobsResp);

    // Fetch ops per job and filter
    const opMap = new Map(); // jobId -> filtered ops[]
    for (const job of jobs) {
      try {
        const opsResp = await postJson(JOB_OPS_LIST(job.id), { limit: 500 });
        const ops = unwrapItems(opsResp).map((o) => o.operation || o);
        const filtered = ops.filter((o) => {
          const opStatus = String(o.status || "");
          const allowedByStatus = opStatuses.length ? opStatuses.includes(opStatus) : true;
          const allowedByName = onlyOp ? String(o.name || "").toLowerCase().includes(onlyOp) : true;
          const hasTime = !!(o?.scheduledStartUtc || o?.originalScheduledStartUtc);
          return allowedByStatus && allowedByName && hasTime;
        });
        opMap.set(job.id, filtered);
      } catch {
        opMap.set(job.id, []);
      }
    }

    // Aggregate: bucket by jobId + opName (case-insensitive)
    const winStart = new Date(since).getTime();
    const winEnd = new Date(until).getTime();
    const buckets = new Map(); // key -> { job, opName, ops: [{op, startIso, endIso}] }

    for (const job of jobs) {
      const ops = opMap.get(job.id) || [];
      if (onlyOp && ops.length === 0) continue;

      for (const op of ops) {
        const startIso = op?.scheduledStartUtc || op?.originalScheduledStartUtc || null;
        let endIso = op?.scheduledEndUtc || op?.originalScheduledEndUtc || startIso;
        if (!startIso) continue;

        const sMs = new Date(startIso).getTime();
        const eMs = new Date(endIso).getTime() || sMs;
        if (eMs < winStart) continue;
        if (sMs > winEnd) continue;

        const opName = (op?.name || "Operation").trim();
        const key2 = `${job.id}::${opName.toLowerCase()}`;

        if (!buckets.has(key2)) {
          buckets.set(key2, { job, opName, ops: [] });
        }
        buckets.get(key2).ops.push({ op, startIso, endIso });
      }
    }

    // Build one event per bucket
    const events = [];
    for (const [, bucket] of buckets) {
      const { job, opName, ops } = bucket;
      if (!ops.length) continue;

      const sMs = Math.min(...ops.map((x) => new Date(x.startIso).getTime()));
      const eMs = Math.max(...ops.map((x) => new Date(x.endIso).getTime() || new Date(x.startIso).getTime()));

      let startIso = new Date(sMs).toISOString();
      let endIso = new Date(eMs).toISOString();

      // allday support (exclusive DTEND)
      if (wantAllDay) {
        const sDate = new Date(Date.UTC(new Date(sMs).getUTCFullYear(), new Date(sMs).getUTCMonth(), new Date(sMs).getUTCDate()));
        const eDate = new Date(Date.UTC(new Date(eMs).getUTCFullYear(), new Date(eMs).getUTCMonth(), new Date(eMs).getUTCDate() + 1));
        startIso = sDate.toISOString();
        endIso = eDate.toISOString();
      }

      const equipments = Array.from(new Set(ops.map((x) => x.op?.scheduledEquipmentName).filter(Boolean)));
      const location = equipments.length === 1 ? equipments[0] : equipments.length > 1 ? "Multiple" : "";

      const code = getProjectCode(job);
      const jobNum = code;
      const jobTitle = job.name || "Untitled Job";
      const countSuffix = ops.length > 1 ? ` (${ops.length} items)` : "";
      const summary = `${code} - ${opName}${countSuffix}`;

      const clientName = getClientName(job);

      const headerLines = [
        `Job: ${[jobNum, jobTitle].filter(Boolean).join(" — ")}`,
        `Operation: ${opName}`,
        clientName ? `Client: ${clientName}` : null,
        job.status ? `Status: ${job.status}` : null,
        equipments.length ? `Equipment: ${equipments.join(", ")}` : null,
        `Span: ${humanShortUTC(startIso)} → ${humanShortUTC(endIso)}`
      ].filter(Boolean);

      const itemLines = ops
        .sort((a, b) => new Date(a.startIso) - new Date(b.startIso))
        .map((x) => opLine(job, x.op, x.startIso, x.endIso));

      const description = [...headerLines, "", "Items:", ...itemLines].join("\\n");

      const categories = [opName, job.status || "", ...(equipments.slice(0, 3))].filter(Boolean);

      const uid =
        crypto
          .createHash("sha1")
          .update(`fulcrum:agg:${job.id}:${opName.toLowerCase()}:${startIso}:${endIso}`)
          .digest("hex") + "@bettis";

      events.push({
        id: job.id,
        start: startIso,
        end: endIso,
        summary,
        location,
        description,
        categories,
        uid,
      });
    }

    const ics =
      [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Bettis//Fulcrum Ops Schedule//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:Fulcrum Ops (Aggregated)",
        "X-WR-TIMEZONE:UTC",
        ...events.map((e) =>
          vevent({
            uid: e.uid,
            start: e.start,
            end: e.end,
            summary: e.summary,
            location: e.location,
            description: e.description,
            categories: e.categories,
            allday: wantAllDay,
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
    res.setHeader("Content-Disposition", 'inline; filename="bettis-fulcrum-ops.ics"');
    return res.status(200).send(safeIcs);
  } catch (err) {
    res.status(500).send(`Error: ${err.message}`);
  }
});

/* -------------------- Feeds discovery (cached) -------------------- */
async function getDiscoveredOps() {
  const now = Date.now();
  if (feedsCache.payload && now - feedsCache.at < FEEDS_CACHE_TTL_SECONDS * 1000) {
    return feedsCache.payload; // { opNames, debug }
  }

  const today = new Date();
  const addDays = (dateLike, n) => {
    const x = new Date(dateLike);
    x.setUTCDate(x.getUTCDate() + n);
    return x.toISOString();
  };

  const discoverStartISO = addDays(today, -365);
  const discoverEndISO = addDays(today, +60);

  const jobsResp = await postJson(JOBS_LIST, {
    limit: 300,
    createdAfterUtc: discoverStartISO,
    createdBeforeUtc: discoverEndISO,
  });
  const jobs = unwrapItems(jobsResp);

  const setNames = new Set();
  let scannedJobs = 0;
  let perJobErrors = 0;

  for (const j of jobs.slice(0, 200)) {
    scannedJobs++;
    try {
      const opsResp = await postJson(JOB_OPS_LIST(j.id), { limit: 500 });
      const ops = unwrapItems(opsResp).map((o) => o.operation || o);
      for (const o of ops) {
        if (o?.name && typeof o.name === "string") setNames.add(o.name);
      }
    } catch {
      perJobErrors++;
    }
    if (setNames.size >= 60) break;
  }

  const opNames = Array.from(setNames);
  const debug = { discovered: setNames.size, scannedJobs, perJobErrors };

  feedsCache.at = now;
  feedsCache.payload = { opNames, debug };
  return feedsCache.payload;
}

/* -------------------- Pretty feeds page (simple list) -------------------- */
/* -------------------- Pretty feeds page (simple list with graceful fallback) -------------------- */
app.get("/feeds", async (req, res) => {
  try {
    let discoveredNames = null;
    let debug = { discovered: 0, scannedJobs: 0, perJobErrors: 0 };
    let apiStatus = "ok"; // "ok" | "unauthorized" | "error"
    let apiErrorMsg = "";

    try {
      const payload = await getDiscoveredOps();
      discoveredNames = payload?.opNames || [];
      debug = payload?.debug || debug;
    } catch (err) {
      apiErrorMsg = String(err?.message || err || "Unknown error");
      apiStatus = /(^|\s)(401|403)(\s|$)/.test(apiErrorMsg) ? "unauthorized" : "error";
      // fall through to fallback list
    }

    const colorMap = {
      "Laser Cut": "#f59e0b",
      "Press Brake": "#3b82f6",
      "Saw": "#10b981",
      "Drill": "#ef4444",
      "Shear": "#a855f7",
      "Weld": "#f97316",
      "Cobot Weld": "#14b8a6",
      "Sand Blast / Clean": "#eab308",
      "Paint": "#22c55e",
      "Packaging": "#06b6d4",
      "Flex": "#8b5cf6",
      "OS Processing": "#64748b",
      "CAD / Engineering": "#60a5fa",
      "Trucking": "#84cc16",
      "Office / OH / Burden": "#94a3b8",
    };

    const fallbackOps = [
      "Laser Cut","Press Brake","Saw","Drill","Shear","Weld","Cobot Weld",
      "Sand Blast / Clean","Paint","Packaging","Flex","OS Processing",
      "CAD / Engineering","Trucking","Office / OH / Burden",
    ];

    const opNames = (discoveredNames?.length ? discoveredNames : fallbackOps)
      .slice()
      .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));

    const today = new Date();
    const isoDate = (d) => d.toISOString().slice(0, 10);
    const start = new Date(today); start.setUTCDate(start.getUTCDate() - 14);
    const end   = new Date(today); end.setUTCDate(end.getUTCDate() + 60);
    const s = isoDate(start), u = isoDate(end);
    const baseUrl = `${req.protocol}://${req.get("host")}`;
    const defaultStatuses = "scheduled,inProgress,ready,pending,paused";

    const feeds = opNames.map((name) => {
      const only = encodeURIComponent(name);
      const url =
        `${baseUrl}/calendar-ops.ics?only=${only}` +
        `&s=${s}&u=${u}&allday=1&limit=500&statuses=${encodeURIComponent(defaultStatuses)}`;
      return { name, url, color: colorMap[name] || "#6b7280" };
    });

    // Render page (always 200 OK, even if Fulcrum is down/unauthorized)
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(`<!doctype html>
        <html lang="en">
        <head>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width,initial-scale=1"/>
        <title>Fulcrum Operation Feeds</title>
        <style>
          :root { --ink:#ffffff; --muted:#a3aec0; --teal:#00848a; --bg:#0f110f; --card:#062b31; --bd:#1d3a40; --warn:#5b1b1b; --warnbd:#b91c1c; }
          *{box-sizing:border-box}
          body{margin:0; background:var(--bg); color:var(--ink); font-family:ui-sans-serif, system-ui, Segoe UI, Roboto, Helvetica, Arial;}
          main{max-width:900px; margin:32px auto; padding:0 16px;}
          h1{margin:0 0 6px; color:var(--teal); font-size:28px; font-weight:800;}
          p.sub{margin:0 0 16px; color:var(--muted);}
          .banner{display:${apiStatus==="ok"?"none":"block"}; background:var(--warn); border:1px solid var(--warnbd); color:#ffdada; padding:10px 12px; border-radius:10px; margin:10px 0 14px;}
          .banner .small{opacity:.9; font-size:.85rem}
          ul.list{list-style:none; padding:0; margin:16px 0 0;}
          li.row{
            display:flex; align-items:center; gap:12px; padding:12px 14px;
            background:var(--card); border:1px solid var(--bd); border-radius:12px; margin-bottom:10px;
          }
          .name{font-weight:700; flex:1 1 auto;}
          .sw{width:25px; height:25px; border-radius:4px; border:1px solid rgba(0,0,0,0.25);}
          button{appearance:none; border:1px solid var(--bd); background:#f04923; color:#000; padding:8px 10px; border-radius:10px; cursor:pointer; font-weight:600;}
          button.primary{background:#97999a; color:#fff; border:0;}
          button:active{transform:translateY(1px)}
          footer{margin-top:18px; color:var(--muted); font-size:12px;}
          .debug{margin-top:6px; color:#7aa0a7; font-size:12px;}
        </style>
        </head>
        <body>
          <main>
            <h1>Fulcrum Operation Feeds</h1>
            <p class="sub">Each feed is an ICS for a specific operation (rolling: past 14 → next 60 days).</p>

            <div class="banner">
              <div><strong>${apiStatus==="unauthorized" ? "Fulcrum authorization failed" : "Fulcrum API unavailable"}</strong></div>
              <div class="small">Showing fallback operation list. ${apiErrorMsg ? ("(" + apiErrorMsg.replace(/</g,"&lt;") + ")") : ""}</div>
            </div>

            <ul class="list" id="feedList"></ul>

            <footer>Tip: add multiple feeds to Outlook and color-code by operation.</footer>
            <div class="debug">Discovered ops: ${discoveredNames?.length || 0} | Jobs scanned: ${debug?.scannedJobs || 0}${discoveredNames?.length ? "" : " | Using fallback list"}${debug?.perJobErrors ? " | Per-job errors: " + debug.perJobErrors : ""}</div>
          </main>

        <script>
          const feeds = ${JSON.stringify(feeds)};
          const list = document.getElementById("feedList");

          function row(feed){
            const li = document.createElement("li");
            li.className = "row";

            const sw = document.createElement("span");
            sw.className = "sw";
            sw.style.background = feed.color;
            sw.title = feed.color;

            const name = document.createElement("span");
            name.className = "name";
            name.textContent = feed.name;

            const openBtn = document.createElement("button");
            openBtn.className = "primary";
            openBtn.textContent = "Open ICS";
            openBtn.onclick = () => window.open(feed.url, "_blank", "noopener,noreferrer");

            const copyUrlBtn = document.createElement("button");
            copyUrlBtn.textContent = "Copy URL";
            copyUrlBtn.onclick = async () => {
              try {
                await navigator.clipboard.writeText(feed.url);
                copyUrlBtn.textContent = "Copied!";
                setTimeout(() => (copyUrlBtn.textContent = "Copy URL"), 1200);
              } catch {
                const ta = document.createElement("textarea");
                ta.value = feed.url;
                document.body.appendChild(ta);
                ta.select();
                document.execCommand("copy");
                document.body.removeChild(ta);
                copyUrlBtn.textContent = "Copied!";
                setTimeout(() => (copyUrlBtn.textContent = "Copy URL"), 1200);
              }
            };

            const copyHexBtn = document.createElement("button");
            copyHexBtn.textContent = "Copy Hex";
            copyHexBtn.title = "Copy " + feed.color;

            // Apply background = feed.color, text color auto-adjusted for contrast
            copyHexBtn.style.background = feed.color;

            // Function to decide text color (white for dark colors, black for light)
            function luminance(hex) {
              const c = hex.replace("#", "");
              if (c.length !== 6) return 0; // fallback
              const r = parseInt(c.slice(0, 2), 16);
              const g = parseInt(c.slice(2, 4), 16);
              const b = parseInt(c.slice(4, 6), 16);
              return (0.299 * r + 0.587 * g + 0.114 * b) / 255;
            }
            const lum = luminance(feed.color);
            copyHexBtn.style.color = lum < 0.5 ? "#ffffff" : "#000000";
            copyHexBtn.style.border = "1px solid rgba(0,0,0,0.25)";

            copyHexBtn.onclick = async () => {
              try {
                await navigator.clipboard.writeText(feed.color);
                const original = copyHexBtn.textContent;
                copyHexBtn.textContent = "Copied!";
                setTimeout(() => (copyHexBtn.textContent = original), 1200);
              } catch {
                const ta = document.createElement("textarea");
                ta.value = feed.color;
                document.body.appendChild(ta);
                ta.select();
                document.execCommand("copy");
                document.body.removeChild(ta);
                copyHexBtn.textContent = "Copied!";
                setTimeout(() => (copyHexBtn.textContent = "Copy Hex"), 1200);
              }
            };


            li.appendChild(sw);
            li.appendChild(name);
            li.appendChild(openBtn);
            li.appendChild(copyUrlBtn);
            li.appendChild(copyHexBtn);
            return li;
          }

          feeds.forEach(f => list.appendChild(row(f)));
        </script>
        </body>
        </html>`);
  } catch (err) {
    // As a last resort, send a minimal page rather than a 500
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.status(200).send(`<pre>Feeds temporarily unavailable.\n${String(err?.message || err)}</pre>`);
  }
});

/* -------------------- test -------------------- */
app.get("/test.ics", (req, res) => {
  const now = new Date();
  const in30 = new Date(now.getTime() + 30 * 60 * 1000);
  const ics =
    [
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

// Simple favicon (teal circle on transparent)
app.get("/favicon.ico", (req, res) => {
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64">
    <circle cx="32" cy="32" r="28" fill="#00848a"/>
  </svg>`;
  res.setHeader("Content-Type", "image/svg+xml");
  res.status(200).send(svg);
});

/* -------------------- start -------------------- */
app.listen(PORT, () => {
  console.log(`ICS feed running on :${PORT}`);
});
