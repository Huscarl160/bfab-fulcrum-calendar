// server.js
// Node 18+. package.json: { "type":"module", "scripts":{ "start":"node server.js" }, "engines":{ "node": ">=18" } }
// Env:
//   FULCRUM_TOKEN (required)
//   FULCRUM_BASE = https://api.fulcrumpro.com (default)
//   ACCESS_KEY (optional) -> require ?key=...
//   CACHE_TTL_SECONDS (default 60)
//   CREATED_WINDOW_BUFFER_DAYS (default 180)

import express from "express";
import crypto from "crypto";

/* -------------------- config -------------------- */
const PORT = process.env.PORT || 8787;
const BASE = process.env.FULCRUM_BASE || "https://api.fulcrumpro.com";
const TOKEN = process.env.FULCRUM_TOKEN;
const ACCESS_KEY = process.env.ACCESS_KEY || null;
const CACHE_TTL_SECONDS = Number(process.env.CACHE_TTL_SECONDS || 60);
const CREATED_WINDOW_BUFFER_DAYS = Number(process.env.CREATED_WINDOW_BUFFER_DAYS || 180);

// default: exclude completed
const DEFAULT_STATUSES = ["scheduled", "inProgress"];

// map friendly -> API enum
const STATUS_MAP = new Map([
  ["scheduled", "scheduled"], ["schedule", "scheduled"],
  ["in-progress", "inProgress"], ["in_progress", "inProgress"], ["inprogress", "inProgress"],
  ["pending", "pending"], ["awaiting", "pending"], ["queued", "pending"],
  ["complete", "complete"], ["completed", "complete"],
  ["cancelled", "cancelled"], ["canceled", "cancelled"],
]);

if (!TOKEN) {
  console.error("Missing FULCRUM_TOKEN env var. Exiting.");
  process.exit(1);
}

// ----- Ops enrichment knobs -----
const OPS_CONCURRENCY = Number(process.env.OPS_CONCURRENCY || 8);
const OPS_CACHE_TTL_MS = Number(process.env.OPS_CACHE_TTL_MS || 5 * 60 * 1000);

// { jobId -> { at:number, data:Array } }
const opsCache = new Map();

function cacheGetOps(jobId) {
  const hit = opsCache.get(jobId);
  if (!hit) return null;
  if (Date.now() - hit.at > OPS_CACHE_TTL_MS) {
    opsCache.delete(jobId);
    return null;
  }
  return hit.data;
}
function cachePutOps(jobId, data) {
  opsCache.set(jobId, { at: Date.now(), data });
  // optional simple cap
  if (opsCache.size > 500) {
    const oldestKey = [...opsCache.entries()].sort((a, b) => a[1].at - b[1].at)[0][0];
    opsCache.delete(oldestKey);
  }
}

/* -------------------- express -------------------- */
const app = express();
app.get("/", (_req, res) => res.send("OK"));
app.get("/health", (_req, res) => res.json({ ok: true, time: new Date().toISOString() }));

/* -------------------- helpers -------------------- */
function icsEscape(s = "") {
  return String(s || "").replace(/([,;])/g, "\\$1").replace(/\n/g, "\\n");
}
function toUTC(dt) {
  const d = new Date(dt);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
}
function yyyymmdd(dateLike) {
  const d = new Date(dateLike);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}`;
}
function addDaysISO(dateLike, n) {
  const d = new Date(dateLike);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString();
}
function veventTimed({ uid, start, end, summary, location, description, categories }) {
  return [
    "BEGIN:VEVENT",
    `UID:${uid}`,
    `DTSTAMP:${toUTC(Date.now())}`,
    `DTSTART:${toUTC(start)}`,
    `DTEND:${toUTC(end || start)}`,
    `SUMMARY:${icsEscape(summary || "Scheduled Work")}`,
    location ? `LOCATION:${icsEscape(location)}` : null,
    description ? `DESCRIPTION:${icsEscape(description)}` : null,
    categories?.length ? `CATEGORIES:${categories.map(icsEscape).join(",")}` : null,
    "END:VEVENT",
  ].filter(Boolean).join("\r\n");
}
function veventAllDay({ uid, startDate, endDateInclusive, summary, location, description, categories }) {
  // For all-day: DTSTART/DTEND are VALUE=DATE and DTEND is EXCLUSIVE (so add 1 day)
  const dtStart = yyyymmdd(startDate);
  const dtEndExclusive = yyyymmdd(addDaysISO(endDateInclusive, 1));
  return [
    "BEGIN:VEVENT",
    `UID:${uid}`,
    `DTSTAMP:${toUTC(Date.now())}`,
    `DTSTART;VALUE=DATE:${dtStart}`,
    `DTEND;VALUE=DATE:${dtEndExclusive}`,
    `SUMMARY:${icsEscape(summary || "Scheduled Work")}`,
    location ? `LOCATION:${icsEscape(location)}` : null,
    description ? `DESCRIPTION:${icsEscape(description)}` : null,
    categories?.length ? `CATEGORIES:${categories.map(icsEscape).join(",")}` : null,
    "END:VEVENT",
  ].filter(Boolean).join("\r\n");
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

// RFC5545 75-octet folding
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

/* -------------------- API endpoints -------------------- */
const JOBS_LIST = "/api/jobs/list";
const JOB_OPS_LIST = (jobId) => `/api/jobs/${jobId}/operations/list`;

/* -------------------- operation selection -------------------- */
function pickPrimaryOperation(job, ops) {
  if (!Array.isArray(ops) || ops.length === 0) return null;
  const jStart = new Date(job.scheduledStartUtc || job.originalScheduledStartUtc || job.productionDueDate || 0).getTime();
  const jEnd = new Date(job.scheduledEndUtc || job.originalScheduledEndUtc || 0).getTime();

  const candidates = ops
    .filter((o) => o?.scheduledStartUtc || o?.originalScheduledStartUtc)
    .sort((a, b) => new Date(a.scheduledStartUtc || a.originalScheduledStartUtc) - new Date(b.scheduledStartUtc || b.originalScheduledStartUtc));

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
  // Window selection preference
  const jobStart = job.scheduledStartUtc || job.originalScheduledStartUtc || job.productionDueDate;
  const jobEnd   = job.scheduledEndUtc   || job.originalScheduledEndUtc;

  const opStart  = primaryOp?.scheduledStartUtc || primaryOp?.originalScheduledStartUtc;
  const opEnd    = primaryOp?.scheduledEndUtc   || primaryOp?.originalScheduledEndUtc;

  // For event timing we will use job window by default (all-day rendering)
  const start = jobStart || opStart;
  let end = jobEnd || opEnd || start;
  if (!end && start) end = addDaysISO(start, 1); // safety

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
const cache = new Map();

/* -------------------- ICS route -------------------- */
// /calendar.ics?s=YYYY-MM-DD&u=YYYY-MM-DD[&ops=1][&allday=0][&statuses=scheduled,in-progress,pending]
app.get("/calendar.ics", async (req, res) => {
  try {
    if (ACCESS_KEY && req.query.key !== ACCESS_KEY) return res.sendStatus(403);

    // cache read
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
    const includeOps = req.query.ops === "1"; // default off for speed
    const allDay = req.query.allday !== "0";  // default ON (all-day events)
    const limit = parseInt(req.query.limit || "500", 10);

    // statuses -> normalize to API enums
    const rawStatuses = req.query.statuses
      ? String(req.query.statuses).split(",").map(s => s.trim()).filter(Boolean)
      : DEFAULT_STATUSES.slice();

    const mapped = rawStatuses.map(s => STATUS_MAP.get(s.toLowerCase?.() || s)).filter(Boolean);
    const finalStatuses = mapped.length ? mapped : DEFAULT_STATUSES.slice();

    // created-window buffering -> server-side list
    let createdAfterUtc, createdBeforeUtc;
    if (since) createdAfterUtc = addDaysISO(since, -CREATED_WINDOW_BUFFER_DAYS);
    if (until) createdBeforeUtc = addDaysISO(until,  CREATED_WINDOW_BUFFER_DAYS);

    const listBody = { limit };
    if (finalStatuses.length === 1) listBody.status = finalStatuses[0];
    else if (finalStatuses.length > 1) listBody.statuses = finalStatuses;
    if (createdAfterUtc)  listBody.createdAfterUtc  = createdAfterUtc;
    if (createdBeforeUtc) listBody.createdBeforeUtc = createdBeforeUtc;

    // 1) list jobs
    const jobsResp = await postJson(JOBS_LIST, listBody);
    const jobs = unwrapItems(jobsResp);

    // 2) optionally fetch operations, but only for jobs likely in-window (by job dates)
const primaryOpByJob = new Map();
if (includeOps) {
  // First, rough prefilter by job-level dates to minimize ops calls
  const prefiltered = jobs.filter((j) => {
    const start =
      j.scheduledStartUtc || j.originalScheduledStartUtc || j.productionDueDate;
    const end =
      j.scheduledEndUtc || j.originalScheduledEndUtc || start;
    if (!start) return false;
    const s = since ? new Date(since).getTime() : null;
    const u = until ? new Date(until).getTime() : null;
    const js = new Date(start).getTime();
    const je = new Date(end).getTime();
    if (s && je < s) return false;
    if (u && js > u) return false;
    return true;
  });

  // Parallel fetch with small concurrency + 5-min cache
  let idx = 0;
  async function worker() {
    while (idx < prefiltered.length) {
      const i = idx++;
      const job = prefiltered[i];

      // cache hit?
      let arr = cacheGetOps(job.id);
      if (!arr) {
        try {
          const opsResp = await postJson(JOB_OPS_LIST(job.id), { limit: 200 });
          arr = unwrapItems(opsResp);
          cachePutOps(job.id, arr);
        } catch {
          arr = null;
        }
      }

      if (arr && Array.isArray(arr)) {
        const pairs = arr.map((o) => ({ op: o.operation || o, itm: o.itemToMake || null }));
        const primary = pickPrimaryOperation(job, pairs.map((p) => p.op));
        const pair =
          primary
            ? (pairs.find((p) => p.op?.id === primary.id) || { op: primary, itm: null })
            : null;
        primaryOpByJob.set(job.id, pair);
      } else {
        primaryOpByJob.set(job.id, null);
      }
    }
  }

  const workers = Math.min(OPS_CONCURRENCY, Math.max(prefiltered.length, 1));
  await Promise.all(Array.from({ length: workers }, worker));
}


    // 3) filter by schedule window (ops first, then job)
    const toMs = (d) => (d ? new Date(d).getTime() : NaN);
    const winStart = since ? new Date(since).getTime() : null;
    const winEnd   = until ? new Date(until).getTime() : null;

    const filteredJobs = jobs.filter((j) => {
      const pair = primaryOpByJob.get(j.id);
      const op   = pair?.op;

      // for inclusion, consider either op or job dates
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

    // 4) build ICS (all-day by default)
    const icsBody = events.map((e) => {
      const uid = crypto.createHash("sha1").update(`fulcrum:${e.id}`).digest("hex") + "@bettis";
      if (allDay) {
        // derive date-only start & end (inclusive end)
        const startDate = e.start || e.end;
        const endDate   = e.end || e.start;
        return veventAllDay({
          uid,
          startDate,
          endDateInclusive: endDate,
          summary: e.summary,
          location: e.location,
          description: e.description,
          categories: e.categories,
        });
      } else {
        return veventTimed({
          uid,
          start: e.start,
          end: e.end,
          summary: e.summary,
          location: e.location,
          description: e.description,
          categories: e.categories,
        });
      }
    }).join("\r\n");

    const ics = [
      "BEGIN:VCALENDAR",
      "VERSION:2.0",
      "PRODID:-//Bettis//Fulcrum Jobs Schedule//EN",
      "CALSCALE:GREGORIAN",
      "METHOD:PUBLISH",
      "X-WR-CALNAME:Fulcrum Schedule",
      "X-WR-TIMEZONE:UTC",
      icsBody,
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

/* -------------------- test route -------------------- */
app.get("/test.ics", (_req, res) => {
  const now = new Date();
  const in30 = new Date(now.getTime() + 30 * 60 * 1000);
  const pad = (n) => String(n).padStart(2, "0");
  const toUTC = (d) => `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
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
    "DESCRIPTION:Diagnostic event to confirm Outlook rendering.",
    "END:VEVENT",
    "END:VCALENDAR",
  ].join("\r\n") + "\r\n";
  res.setHeader("Content-Type", "text/calendar; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache");
  res.status(200).send(ics);
});

/* -------------------- start -------------------- */
app.listen(PORT, () => {
  console.log(`ICS feed running on :${PORT}`);
});
