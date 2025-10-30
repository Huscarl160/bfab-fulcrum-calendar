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
    // DTSTART/DTEND as DATE (DTEND exclusive per RFC5545)
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
const JOB_OPS_LIST = (jobId) => `/api/jobs/${jobId}/operations/list`;

/* -------------------- ops selection & mapping (job->event) -------------------- */
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

  const itemName =
    itemToMake?.itemReference?.name || itemToMake?.itemReference?.number || "";
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

/* -------------------- OPS-DRIVEN ICS (per-op friendly) -------------------- */
// /calendar-ops.ics?s=YYYY-MM-DD&u=YYYY-MM-DD&allday=1&only=Laser%20Cut&statuses=ready,inProgress,paused,pending
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

    // Required for per-op feeds: the operation name filter (case-insensitive substring match)
    const onlyOp = req.query.only ? String(req.query.only).toLowerCase() : null;

    // Build job list using a created window that pads the schedule window
    const listBody = { limit };
    if (since) listBody.createdAfterUtc = addDaysISO(since, -CREATED_WINDOW_BUFFER_DAYS);
    if (until) listBody.createdBeforeUtc = addDaysISO(until, CREATED_WINDOW_BUFFER_DAYS);
    if (jobStatuses.length) listBody.statuses = jobStatuses;

    const jobsResp = await postJson(JOBS_LIST, listBody);
    const jobs = unwrapItems(jobsResp);

    // Fetch ops per job, then filter to matching ops.
    // If onlyOp is present, we require at least one matching op or we skip the job entirely.
    const winStart = new Date(since).getTime();
    const winEnd = new Date(until).getTime();
    const events = [];

    for (const job of jobs) {
      let ops = [];
      try {
        const opsResp = await postJson(JOB_OPS_LIST(job.id), { limit: 500 });
        ops = unwrapItems(opsResp).map((o) => o.operation || o);
      } catch {
        ops = [];
      }

      // Filter ops by status and name
      const filtered = ops.filter((o) => {
        const opStatus = String(o.status || "");
        const allowedByStatus = opStatuses.length ? opStatuses.includes(opStatus) : true;
        const allowedByName = onlyOp ? String(o.name || "").toLowerCase().includes(onlyOp) : true;
        return allowedByStatus && allowedByName;
      });

      // If an op-name filter is set, skip jobs with no matching ops
      if (onlyOp && filtered.length === 0) continue;

      // If no op-name filter, you *could* fall back to a single primary op per job.
      // But since this is calendar-ops, we’ll still emit per-op events for any ops that passed status filters.
      const opsToEmit = filtered.length ? filtered : [];

      for (const op of opsToEmit) {
        // Prefer operation times; if missing, skip (don’t fall back to job times for per-op feeds)
        const startIso =
          op?.scheduledStartUtc ||
          op?.originalScheduledStartUtc ||
          null;

        let endIso =
          op?.scheduledEndUtc ||
          op?.originalScheduledEndUtc ||
          startIso;

        if (!startIso) continue;

        // Window clip against op times
        const sMs = new Date(startIso).getTime();
        const eMs = new Date(endIso).getTime() || sMs;
        if (eMs < winStart) continue;
        if (sMs > winEnd) continue;

        // All-day coercion (exclusive DTEND)
        let finalStart = startIso;
        let finalEnd = endIso;
        if (wantAllDay) {
          const sDate = new Date(Date.UTC(new Date(sMs).getUTCFullYear(), new Date(sMs).getUTCMonth(), new Date(sMs).getUTCDate()));
          const eDate = new Date(Date.UTC(new Date(eMs).getUTCFullYear(), new Date(eMs).getUTCMonth(), new Date(eMs).getUTCDate() + 1));
          finalStart = sDate.toISOString();
          finalEnd = eDate.toISOString();
        }

        // Build event using the operation as the “primary”
        const ev = mapJobToEvent(job, op, null);
        ev.start = finalStart;
        ev.end = finalEnd;

        // Stronger summary emphasis on op name
        // (mapJobToEvent already includes (opName) — keep that)
        events.push(ev);
      }
    }

    const ics =
      [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Bettis//Fulcrum Ops Schedule//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:Fulcrum Ops",
        "X-WR-TIMEZONE:UTC",
        ...events.map((e) =>
          vevent({
            // Include op id if present to avoid UID collisions across different operations
            uid:
              crypto
                .createHash("sha1")
                .update(`fulcrum:${e.id}:${e.start}:${e.summary}`)
                .digest("hex") + "@bettis",
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


/* -------------------- Pretty feeds page (SIMPLE LIST) -------------------- */
// Requirements: list only; show name + color + hex; "Open ICS" + "Copy URL" buttons; no URL text; no tags; sorted by name.
app.get("/feeds", async (req, res) => {
  try {
    // Rolling created window to discover ops reliably
    const today = new Date();
    const isoDate = (d) => d.toISOString().slice(0, 10);
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

    // Gather operation names
    const opNames = new Set();
    let scannedJobs = 0;
    let perJobErrors = 0;

    for (const j of jobs.slice(0, 200)) {
      scannedJobs++;
      try {
        const opsResp = await postJson(JOB_OPS_LIST(j.id), { limit: 500 });
        const ops = unwrapItems(opsResp).map((o) => o.operation || o);
        for (const o of ops) {
          if (o?.name && typeof o.name === "string") {
            opNames.add(o.name);
          }
        }
      } catch {
        perJobErrors++;
      }
      if (opNames.size >= 60) break; // cap discovery
    }

    // Suggested colors for known ops
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
      "Laser Cut",
      "Press Brake",
      "Saw",
      "Drill",
      "Shear",
      "Weld",
      "Cobot Weld",
      "Sand Blast / Clean",
      "Paint",
      "Packaging",
      "Flex",
      "OS Processing",
      "CAD / Engineering",
      "Trucking",
      "Office / OH / Burden",
    ];

    // Rolling window for feed URLs (past 14, next 60)
    const start = new Date(today);
    start.setUTCDate(start.getUTCDate() - 14);
    const end = new Date(today);
    end.setUTCDate(end.getUTCDate() + 60);
    const s = isoDate(start);
    const u = isoDate(end);
    const baseUrl = `${req.protocol}://${req.get("host")}`;
    const defaultStatuses = "scheduled,inProgress,ready,pending,paused";

    const names = (opNames.size ? Array.from(opNames) : fallbackOps).sort((a, b) =>
      a.localeCompare(b, undefined, { sensitivity: "base" })
    );

    const feeds = names.map((name) => {
      const only = encodeURIComponent(name);
      const url =
        `${baseUrl}/calendar-ops.ics?only=${only}` +
        `&s=${s}&u=${u}&allday=1&limit=500&statuses=${encodeURIComponent(defaultStatuses)}`;
      return { name, url, color: colorMap[name] || "#6b7280" };
    });

    // Render simple list; no URL text; buttons only
    res.setHeader("Content-Type", "text/html; charset=utf-8");
    res.send(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Fulcrum Operation Feeds</title>
<style>
  :root { --ink:#97999a; --muted:#475569; --teal:#004c50; --bg:#212721; --card:#062b31; --bd:#e2e8f0; }
  *{box-sizing:border-box}
  body{margin:0; background:var(--bg); color:var(--ink); font-family:ui-sans-serif, system-ui, Segoe UI, Roboto, Helvetica, Arial;}
  main{max-width:900px; margin:32px auto; padding:0 16px;}
  h1{margin:0 0 6px; color:var(--teal); font-size:28px; font-weight:800;}
  p.sub{margin:0 0 16px; color:var(--muted);}

  ul.list{list-style:none; padding:0; margin:16px 0 0;}
  li.row{
    display:flex; align-items:center; gap:12px; padding:12px 14px;
    background:var(--card); border:1px solid var(--bd); border-radius:12px; margin-bottom:10px;
  }
  .name{font-weight:700; flex:1 1 auto;}
  .sw{width:16px; height:16px; border-radius:4px; border:1px solid rgba(0,0,0,0.1);}
  .hex{font-family:ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; color:var(--muted); font-size:12px;}
  button{appearance:none; border:1px solid var(--bd); background:#f04923; color:#97999a; padding:8px 10px; border-radius:10px; cursor:pointer; font-weight:600;}
  button.primary{background:#97999a; color:#001317; border:0;}
  button:active{transform:translateY(1px)}
  footer{margin-top:18px; color:var(--muted); font-size:12px;}
  .debug{margin-top:6px; color:#64748b; font-size:12px;}
</style>
</head>
<body>
  <main>
    <h1>Fulcrum Operation Feeds</h1>
    <p class="sub">Each feed is an ICS for a specific operation (rolling: past 14 → next 60 days).</p>

    <ul class="list" id="feedList"></ul>

    <footer>Tip: add multiple feeds to Outlook and color-code by operation.</footer>
    <div class="debug">Discovered ops: ${opNames.size} | Jobs scanned: ${scannedJobs}${opNames.size ? "" : " | Using fallback list"}${perJobErrors ? " | Per-job errors: " + perJobErrors : ""}</div>
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

    const name = document.createElement("span");
    name.className = "name";
    name.textContent = feed.name;

    const hex = document.createElement("span");
    hex.className = "hex";
    hex.textContent = feed.color;

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
        const ta = document.createElement("textarea");
        ta.value = feed.url;
        document.body.appendChild(ta);
        ta.select();
        document.execCommand("copy");
        document.body.removeChild(ta);
        copyBtn.textContent = "Copied!";
        setTimeout(() => (copyBtn.textContent = "Copy URL"), 1200);
      }
    };

    li.appendChild(sw);
    li.appendChild(name);
    li.appendChild(hex);
    li.appendChild(openBtn);
    li.appendChild(copyBtn);
    return li;
  }

  feeds.forEach(f => list.appendChild(row(f)));
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

/* -------------------- start -------------------- */
app.listen(PORT, () => {
  console.log(`ICS feed running on :${PORT}`);
});
