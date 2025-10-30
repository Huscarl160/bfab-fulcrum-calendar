// server.js
// Node 18+ (global fetch). package.json: { "type": "module", "start": "node server.js", "engines": { "node": ">=18" } }
// Env:
//   FULCRUM_TOKEN (required)
//   FULCRUM_BASE (default: https://api.fulcrumpro.com)
//   ACCESS_KEY (optional) require ?key=...
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
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
}
function vevent({ uid, start, end, summary, location, description, categories, allday = false }) {
  const lines = ["BEGIN:VEVENT", `UID:${uid}`, `DTSTAMP:${toUTC(Date.now())}`];
  if (allday) {
    // DTSTART/DTEND as DATE (no time). DTEND is exclusive in RFC5545.
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
  lines.push("END:VEVENT");
  return lines.join("\r\n");
}
function toUTCDateOnly(d) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}`;
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
  const endDefault   = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate() + 180));
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

/* ----- whitelists (exact casing your tenant expects) ----- */
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

/* -------------------- API endpoints (declare ONCE) -------------------- */
const JOBS_LIST = "/api/jobs/list";
const JOB_OPS_LIST = (jobId) => `/api/jobs/${job.id ?? jobId}/operations/list`; // guarded in use

/* -------------------- ops selection & mapping (job->event) -------------------- */
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
  const jobEnd   = job.scheduledEndUtc || job.originalScheduledEndUtc;

  const opStart = primaryOp?.scheduledStartUtc || primaryOp?.originalScheduledStartUtc;
  const opEnd   = primaryOp?.scheduledEndUtc || primaryOp?.originalScheduledEndUtc;

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
          const opsResp = await postJson(`/api/jobs/${job.id}/operations/list`, { limit: 200 });
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
        op?.scheduledStartUtc || op?.originalScheduledStartUtc ||
        j.scheduledStartUtc || j.originalScheduledStartUtc || j.productionDueDate;

      const end =
        op?.scheduledEndUtc || op?.originalScheduledEndUtc ||
        j.scheduledEndUtc || j.originalScheduledEndUtc || start;

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

    const ics = [
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

/* -------------------- OPS-DRIVEN ICS (per-op friendly) -------------------- */
// /calendar-ops.ics?s=YYYY-MM-DD&u=YYYY-MM-DD&allday=1&op=Laser%20Cut&statuses=ready,inProgress,paused,pending
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
    const limit = parseInt(req.query.limit || "300", 10);
    const wantAllDay = req.query.allday === "1";

    const rawStatuses = req.query.statuses
      ? String(req.query.statuses).split(",").map((s) => s.trim()).filter(Boolean)
      : [];
    const jobStatuses = rawStatuses.filter((s) => JOB_STATUS_WHITELIST.has(s));
    const opStatuses  = rawStatuses.filter((s) => OP_STATUS_WHITELIST.has(s));

    // optional single-operation filter for per-op feeds
    const opNameFilter = req.query.op ? String(req.query.op).trim() : null;

    // jobs list body (created window around schedule window)
    const listBody = { limit };
    if (since) listBody.createdAfterUtc  = addDaysISO(since, -CREATED_WINDOW_BUFFER_DAYS);
    if (until) listBody.createdBeforeUtc = addDaysISO(until,  CREATED_WINDOW_BUFFER_DAYS);
    if (jobStatuses.length) listBody.statuses = jobStatuses;

    const jobsResp = await postJson(JOBS_LIST, listBody);
    const jobs = unwrapItems(jobsResp);

    // fetch ops per job; client-filter by op status and op name
    // optional: client-side op-status + operation-name filtering
    const onlyOp = req.query.only ? String(req.query.only).toLowerCase() : null;

    const arrFiltered = arr.filter(o => {
        const opStatus = String(o.status || "");
        const allowedByStatus = opStatuses.length
            ? (OP_STATUS_WHITELIST.has(opStatus) && opStatuses.includes(opStatus))
            : true;

        const allowedByName = onlyOp
            ? String(o.name || "").toLowerCase().includes(onlyOp)
            : true;

        return allowedByStatus && allowedByName;
    });


    // build events (one VEVENT per primary op per job, same logic as before)
    const winStart = new Date(since).getTime();
    const winEnd   = new Date(until).getTime();

    const events = [];
    for (const job of jobs) {
      const ops = opMap.get(job.id) || [];
      const primary = pickPrimaryOperation(job, ops);

      // compute times using op if available, otherwise job
      const startIso =
        primary?.scheduledStartUtc || primary?.originalScheduledStartUtc ||
        job.scheduledStartUtc || job.originalScheduledStartUtc || job.productionDueDate;

      let endIso =
        primary?.scheduledEndUtc || primary?.originalScheduledEndUtc ||
        job.scheduledEndUtc || job.originalScheduledEndUtc || startIso;

      if (!startIso) continue;

      // window clip
      const sMs = new Date(startIso).getTime();
      const eMs = new Date(endIso).getTime() || sMs;
      if (eMs < winStart) continue;
      if (sMs > winEnd) continue;

      // all-day needs exclusive DTEND: add 1 day to end
      let alldayStart = startIso;
      let alldayEnd = endIso;
      if (wantAllDay) {
        // coerce to date-only boundaries
        const sDate = new Date(Date.UTC(new Date(sMs).getUTCFullYear(), new Date(sMs).getUTCMonth(), new Date(sMs).getUTCDate()));
        const eDate = new Date(Date.UTC(new Date(eMs).getUTCFullYear(), new Date(eMs).getUTCMonth(), new Date(eMs).getUTCDate() + 1)); // exclusive
        alldayStart = sDate.toISOString();
        alldayEnd = eDate.toISOString();
      }

      const ev = mapJobToEvent(job, primary || null, null);
      // override event times with processed times
      ev.start = alldayStart;
      ev.end   = alldayEnd;

      events.push(ev);
    }

    const ics = [
      "BEGIN:VCALENDAR",
      "VERSION:2.0",
      "PRODID:-//Bettis//Fulcrum Ops Schedule//EN",
      "CALSCALE:GREGORIAN",
      "METHOD:PUBLISH",
      "X-WR-CALNAME:Fulcrum Ops",
      "X-WR-TIMEZONE:UTC",
      ...events.map((e) =>
        vevent({
          uid: crypto.createHash("sha1").update(`fulcrum:${e.id}:${e.start}`).digest("hex") + "@bettis",
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

/* -------------------- FEEDS index  -------------------- */
// Lists handy per-op URLs (rolling window, all-day). Copy-paste into Outlook.
// Pretty feeds index (HTML)
/* -------------------- feeds.css (dark theme) -------------------- */
app.get("/feeds.css", (req, res) => {
  const css = `
    :root{
      --bg:#0b1418;
      --card:#101b20;
      --ink:#d7e5e8;
      --muted:#9fb2b6;
      --brand:#00a2b1; /* Bettis teal-ish */
      --brand-2:#05424a;
      --ring:rgba(0,255,255,0.2);
    }
    *{box-sizing:border-box}
    html,body{height:100%}
    body{
      margin:0;
      font-family: ui-sans-serif, system-ui, Segoe UI, Roboto, Helvetica, Arial, Apple Color Emoji, Segoe UI Emoji;
      background: radial-gradient(1200px 700px at 20% -10%, #0d1f24 0%, var(--bg) 50%), var(--bg);
      color: var(--ink);
      padding: 2rem;
    }
    .wrap{max-width:1000px;margin:0 auto}
    h1{
      font-weight:700;
      font-size: clamp(1.2rem, 1rem + 1.2vw, 2rem);
      text-align:center;
      margin:0 0 1.25rem 0;
      color:#e6fbff;
      letter-spacing:.2px;
    }
    .desc{
      text-align:center;
      color:var(--muted);
      margin:0 0 2rem 0;
      font-size:.98rem;
    }
    .grid{
      display:grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 1rem;
    }
    .card{
      background: linear-gradient(180deg, var(--card), #0e171b);
      border: 1px solid rgba(255,255,255,0.05);
      border-radius: 14px;
      padding: 1rem;
      box-shadow: 0 10px 24px rgba(0,0,0,0.25);
    }
    .op-title{
      display:flex; align-items:center; gap:.6rem;
      font-weight:600;
      margin-bottom:.75rem;
      letter-spacing:.2px;
    }
    .swatch{
      width:12px;height:12px;border-radius:3px;flex:0 0 auto;box-shadow:0 0 0 2px #000 inset;
    }
    .url-row{
      display:flex; gap:.5rem; align-items:center;
      margin-top:.5rem; flex-wrap:wrap;
    }
    .url{
      flex:1 1 auto;
      background:#0b1214;
      color:#bfe7ec;
      border:1px solid #12343a;
      border-radius:10px;
      padding:.6rem .7rem;
      font-size:.85rem;
      overflow:auto;
      white-space:nowrap;
    }
    .btns{display:flex; gap:.5rem; margin-top:.7rem; flex-wrap:wrap;}
    .btn{
      appearance:none; border:0; cursor:pointer;
      background: linear-gradient(180deg, var(--brand), var(--brand-2));
      color:#eaffff; padding:.6rem .8rem; border-radius:10px;
      font-weight:600; font-size:.9rem; letter-spacing:.2px;
      transition: transform .08s ease, box-shadow .08s ease;
      box-shadow: 0 6px 16px rgba(0, 255, 255, .12);
    }
    .btn:hover{ transform: translateY(-1px); box-shadow: 0 10px 24px rgba(0, 255, 255, .15);}
    .btn.secondary{
      background: linear-gradient(180deg, #1b2c31, #172327);
      color:#c7e6ea;
      border:1px solid #12343a;
      box-shadow:none;
    }
    .foot{
      text-align:center;
      color:var(--muted);
      margin-top:2rem;
      font-size:.85rem;
    }
    .tiny{color:#7aa1a7; font-size:.85rem}
    .ok{color:#9cffd7}
    .err{color:#ff9c9c}
    .pill{
      display:inline-block; padding:.15rem .45rem; border-radius:999px;
      border:1px solid #1b3236; background:#0e171b; color:#a4cad0; font-size:.75rem; margin-left:.35rem;
    }
  `;
  res.setHeader("Content-Type", "text/css; charset=utf-8");
  res.status(200).send(css);
});

/* -------------------- feeds.css route -------------------- */
app.get("/feeds.css", (req, res) => {
  const css = `
    body {
      font-family: system-ui, sans-serif;
      background: #f8fafc;
      color: #0f172a;
      padding: 2rem;
      line-height: 1.6;
    }
    h1 {
      color: #004c50;
      text-align: center;
      margin-bottom: 1.5rem;
      font-size: 1.75rem;
    }
    .feed-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
      gap: 1rem;
      max-width: 900px;
      margin: 0 auto;
    }
    a.feed {
      display: block;
      padding: 1.25rem;
      background: white;
      border-radius: 0.75rem;
      box-shadow: 0 2px 4px rgba(0,0,0,0.1);
      text-decoration: none;
      color: #0f172a;
      transition: all 0.15s ease-in-out;
      border-left: 5px solid #004c50;
    }
    a.feed:hover {
      transform: translateY(-3px);
      box-shadow: 0 4px 12px rgba(0,0,0,0.12);
      background: #f1f5f9;
    }
    .feed-title {
      font-weight: 600;
      font-size: 1.1rem;
      margin-bottom: 0.25rem;
    }
    .feed-desc {
      font-size: 0.9rem;
      color: #475569;
    }
    footer {
      text-align: center;
      margin-top: 2rem;
      font-size: 0.85rem;
      color: #64748b;
    }
  `;
  res.setHeader("Content-Type", "text/css; charset=utf-8");
  res.status(200).send(css);
});


// ---------- Pretty feeds page (dark, buttons, copy/open) ----------
app.get("/feeds", async (req, res) => {
  try {
    // small discovery sweep
    const jobsResp = await postJson(JOBS_LIST, { limit: 150 });
    const jobs = unwrapItems(jobsResp);

    const opNames = new Set();
    for (const j of jobs) {
      try {
        const opsResp = await postJson(JOB_OPS_LIST(j.id), { limit: 200 });
        const ops = unwrapItems(opsResp).map(o => o.operation || o);
        for (const o of ops) if (o?.name) opNames.add(o.name);
      } catch {
        /* ignore per-job errors */
      }
      if (opNames.size >= 50) break; // don't overfetch
    }

    const baseUrl = `${req.protocol}://${req.get("host")}`;

    // rolling window: past 14 days to next 60 days
    const today = new Date();
    const isoDate = (d) => d.toISOString().slice(0, 10);
    const start = new Date(today); start.setUTCDate(start.getUTCDate() - 14);
    const end   = new Date(today); end.setUTCDate(end.getUTCDate() + 60);
    const s = isoDate(start);
    const u = isoDate(end);

    // friendly color suggestions (fallback gray if unknown)
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
      "Office / OH / Burden": "#94a3b8"
    };

    const defaultStatuses = "scheduled,inProgress,ready,pending,paused";
    const feeds = Array.from(opNames).sort().map(name => {
      const only = encodeURIComponent(name);
      const url =
        `${baseUrl}/calendar-ops.ics?only=${only}` +
        `&s=${s}&u=${u}&allday=1&limit=500&statuses=${encodeURIComponent(defaultStatuses)}`;
      return {
        name,
        url,
        color: colorMap[name] || "#6b7280"
      };
    });

    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Fulcrum Operation Feeds</title>
<style>
  :root{
    --bg:#0b1220; --panel:#111827; --muted:#9ca3af; --fg:#e5e7eb; --accent:#38bdf8; --card:#0f172a; --chip:#1f2937;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);font-family:ui-sans-serif,system-ui,Segoe UI,Roboto,Helvetica,Arial}
  header{max-width:1100px;margin:32px auto 0;padding:0 16px}
  h1{margin:0 0 4px 0;font-size:28px;font-weight:700}
  p.sub{margin:0;color:var(--muted)}
  .panel{max-width:1100px;margin:24px auto;padding:16px}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:14px}
  .card{background:var(--card);border:1px solid #1f2937;border-radius:14px;padding:14px;box-shadow:0 10px 18px rgba(0,0,0,.25)}
  .name{display:flex;align-items:center;gap:10px;margin-bottom:10px}
  .swatch{width:14px;height:14px;border-radius:3px;border:1px solid rgba(255,255,255,.25);flex:0 0 auto}
  .url{font-size:12px;color:var(--muted);word-break:break-all;margin:8px 0 12px}
  .btnrow{display:flex;gap:8px}
  button{appearance:none;border:1px solid #334155;background:#0b1220;color:var(--fg);padding:8px 10px;border-radius:10px;cursor:pointer}
  button.primary{background:var(--accent);color:#001018;border:none}
  button:active{transform:translateY(1px)}
  .chips{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0 0}
  .chip{background:var(--chip);color:var(--muted);border-radius:999px;padding:4px 10px;font-size:12px;border:1px solid #374151}
  footer{max-width:1100px;margin:18px auto 40px;color:var(--muted);font-size:12px;padding:0 16px}
</style>
</head>
<body>
  <header>
    <h1>Fulcrum Operation Feeds</h1>
    <p class="sub">Copy or open an ICS feed for a specific operation. These links use a rolling window (past 14 days → next 60).</p>
  </header>
  <section class="panel">
    <div id="grid" class="grid"></div>
  </section>
  <footer>
    Tip: Add multiple feeds to Outlook and color-code each one. Rolling dates prevent feeds from going stale.
  </footer>

<script>
  // server-embedded data
  const feeds = ${JSON.stringify(feeds)};

  const grid = document.getElementById("grid");

  function card(feed){
    const div = document.createElement("div");
    div.className = "card";

    const top = document.createElement("div");
    top.className = "name";
    const sw = document.createElement("span");
    sw.className = "swatch";
    sw.style.background = feed.color;
    const title = document.createElement("div");
    title.textContent = feed.name;
    title.style.fontWeight = "700";
    top.appendChild(sw);
    top.appendChild(title);

    const url = document.createElement("div");
    url.className = "url";
    url.textContent = feed.url;

    const btnrow = document.createElement("div");
    btnrow.className = "btnrow";

    const openBtn = document.createElement("button");
    openBtn.className = "primary";
    openBtn.textContent = "Open ICS";
    openBtn.onclick = () => window.open(feed.url, "_blank", "noopener,noreferrer");

    const copyBtn = document.createElement("button");
    copyBtn.textContent = "Copy URL";
    copyBtn.onclick = async () => {
      try {
        await navigator.clipboard.writeText(feed.url);
        copyBtn.textContent = "Copied!";
        setTimeout(() => (copyBtn.textContent = "Copy URL"), 1200);
      } catch {
        // fallback
        const ta = document.createElement("textarea");
        ta.value = feed.url;
        document.body.appendChild(ta);
        ta.select(); document.execCommand("copy"); document.body.removeChild(ta);
        copyBtn.textContent = "Copied!";
        setTimeout(() => (copyBtn.textContent = "Copy URL"), 1200);
      }
    };

    btnrow.appendChild(openBtn);
    btnrow.appendChild(copyBtn);

    const chips = document.createElement("div");
    chips.className = "chips";
    for (const txt of ["allday","rolling","ops feed"]) {
      const c = document.createElement("span");
      c.className = "chip";
      c.textContent = txt;
      chips.appendChild(c);
    }

    div.appendChild(top);
    div.appendChild(url);
    div.appendChild(btnrow);
    div.appendChild(chips);
    return div;
  }

  feeds.forEach(f => grid.appendChild(card(f)));
</script>
</body>
</html>`);
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

/* -------------------- start -------------------- */
app.listen(PORT, () => {
  console.log(`ICS feed running on :${PORT}`);
});
