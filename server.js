// server.js
// Node 18+ (global fetch). package.json: { "type": "module", "start": "node server.js", "engines": { "node": ">=18" } }
// Env vars:
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

/* -------------------- helpers -------------------- */
function icsEscape(s = "") {
  return String(s || "").replace(/([,;])/g, "\\$1").replace(/\n/g, "\\n");
}
function toUTC(dt) {
  const d = new Date(dt);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
}
function vevent({ uid, start, end, summary, location, description, categories }) {
  return [
    "BEGIN:VEVENT",
    `UID:${uid}`,
    `DTSTAMP:${toUTC(Date.now())}`,
    `DTSTART:${toUTC(start)}`,
    `DTEND:${toUTC(end || start)}`,
    `SUMMARY:${icsEscape(summary || "Scheduled Work")}`,
    location ? `LOCATION:${icsEscape(location)}` : null,
    description ? `DESCRIPTION:${icsEscape(description)}` : null,
    categories && categories.length ? `CATEGORIES:${categories.map(icsEscape).join(",")}` : null,
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

/* -------------------- JOB-DRIVEN ICS -------------------- */
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

/* -------------------- OPS-DRIVEN ICS -------------------- */
// /calendar-ops.ics?s=YYYY-MM-DD&u=YYYY-MM-DD&allday=1&statuses=scheduled,inProgress,ready,pending,paused
function parseISODateOnly(s) {
  if (!s) return null;
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : d.getTime();
}
function addDaysMillis(ms, days) {
  const d = new Date(ms);
  d.setUTCDate(d.getUTCDate() + days);
  return d.getTime();
}
function toUTCDateOnly(d) {
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}`;
}
function toUTCTimestamp(ms) {
  const d = new Date(ms);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
}
function pickOpTimes(op) {
  const s = op?.scheduledStartUtc || op?.originalScheduledStartUtc || null;
  const e = op?.scheduledEndUtc   || op?.originalScheduledEndUtc   || null;
  return { start: s ? new Date(s).getTime() : null, end: e ? new Date(e).getTime() : null };
}

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
      res.setHeader("Content-Disposition", 'inline; filename="bettis-ops.ics"');
      return res.status(200).send(hit.body);
    }

    // default rolling window: today-30d .. today+120d
    const today = new Date();
    const defStart = addDaysMillis(today.getTime(), -30);
    const defEnd   = addDaysMillis(today.getTime(), +120);

    const winStart = parseISODateOnly(req.query.s) ?? defStart;
    const winEnd   = parseISODateOnly(req.query.u) ?? defEnd;

    const allDay = String(req.query.allday || "0") === "1";

    const defaultOpStatuses = ["scheduled","inProgress","ready","pending","paused"];
    const opStatuses = req.query.statuses
      ? String(req.query.statuses).split(",").map(s => s.trim()).filter(Boolean)
      : defaultOpStatuses;

    const limit = parseInt(req.query.limit || "500", 10);

    const bufferDays = Number(process.env.CREATED_WINDOW_BUFFER_DAYS || 180);
    const createdAfterUtc  = new Date(addDaysMillis(winStart, -bufferDays)).toISOString();
    const createdBeforeUtc = new Date(addDaysMillis(winEnd,   +bufferDays)).toISOString();

    const listBody = {
      limit,
      statuses: ["scheduled","inProgress","pending","ready","paused"],
      createdAfterUtc,
      createdBeforeUtc,
    };

    const jobsResp = await postJson(JOBS_LIST, listBody);
    const jobs = unwrapItems(jobsResp);

    const rows = [];
    for (const j of jobs) {
      if (!j?.id) continue;
      try {
        const opsResp = await postJson(JOB_OPS_LIST(j.id), { limit: 500 });
        const items = unwrapItems(opsResp);
        for (const row of items) {
          const op = row.operation || row;
          const itm = row.itemToMake || {};
          const { start, end } = pickOpTimes(op);
          if (!start && !end) continue;

          const s = start ?? end;
          const e = end ?? (start || null);
          if (s == null) continue;
          const overlaps = (e ?? s) >= winStart && s <= winEnd;
          if (!overlaps) continue;

          const opStatus = String(op.status || "").trim();
          if (opStatuses.length && !opStatuses.includes(opStatus)) continue;

          const title  = j.name || (j.number != null ? `Job #${j.number}` : "Operation");
          const number = j.number != null ? `#${j.number}` : "";
          const opName = op.name || "";
          const equip  = op.scheduledEquipmentName || "";

          const summary = [title, number, opName ? `(${opName})` : ""].filter(Boolean).join(" ");
          const itemName = itm?.itemReference?.name || itm?.itemReference?.number || "";
          const descLines = [
            opStatus ? `Status: ${opStatus}` : null,
            equip    ? `Equipment: ${equip}` : null,
            opName   ? `Operation: ${opName}` : null,
            j.id     ? `Job ID: ${j.id}` : null,
            itemName ? `Item: ${itemName}` : null,
          ].filter(Boolean);

          rows.push({
            uid: crypto.createHash("sha1").update(`ops:${j.id}:${op.id}`).digest("hex") + "@bettis",
            summary,
            location: equip || "",
            description: descLines.join("\\n"),
            categories: [equip || null, opName || null, opStatus || null].filter(Boolean),
            startMs: s,
            endMs: e ?? (s + 30*60*1000),
          });
        }
      } catch {
        // ignore per-job failures
      }
    }

    const lines = [
      "BEGIN:VCALENDAR",
      "VERSION:2.0",
      "PRODID:-//Bettis//Fulcrum Operations Schedule//EN",
      "CALSCALE:GREGORIAN",
      "METHOD:PUBLISH",
      "X-WR-CALNAME:Fulcrum Ops",
      "X-WR-TIMEZONE:UTC",
    ];

    for (const e of rows) {
      if (allDay) {
        const startDate = new Date(e.startMs);
        const endDateExclusive = new Date(addDaysMillis(e.endMs || e.startMs, 1));
        lines.push(
          "BEGIN:VEVENT",
          `UID:${e.uid}`,
          `DTSTAMP:${toUTC(Date.now())}`,
          `DTSTART;VALUE=DATE:${toUTCDateOnly(startDate)}`,
          `DTEND;VALUE=DATE:${toUTCDateOnly(endDateExclusive)}`,
          `SUMMARY:${icsEscape(e.summary)}`,
          e.location ? `LOCATION:${icsEscape(e.location)}` : null,
          e.description ? `DESCRIPTION:${icsEscape(e.description)}` : null,
          e.categories?.length ? `CATEGORIES:${e.categories.map(icsEscape).join(",")}` : null,
          "END:VEVENT"
        );
      } else {
        lines.push(
          "BEGIN:VEVENT",
          `UID:${e.uid}`,
          `DTSTAMP:${toUTC(Date.now())}`,
          `DTSTART:${toUTC(e.startMs)}`,
          `DTEND:${toUTC(e.endMs)}`,
          `SUMMARY:${icsEscape(e.summary)}`,
          e.location ? `LOCATION:${icsEscape(e.location)}` : null,
          e.description ? `DESCRIPTION:${icsEscape(e.description)}` : null,
          e.categories?.length ? `CATEGORIES:${e.categories.map(icsEscape).join(",")}` : null,
          "END:VEVENT"
        );
      }
    }

    lines.push("END:VCALENDAR");
    const ics = lines.filter(Boolean).join("\r\n");
    const safe = finalizeIcs(ics);

    const etag = 'W/"' + crypto.createHash("sha1").update(safe).digest("hex") + '"';
    cache.set(key, { at: now, body: safe, etag });

    res.setHeader("Content-Type", "text/calendar; charset=utf-8");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("ETag", etag);
    res.setHeader("Content-Disposition", 'inline; filename="bettis-ops.ics"');
    res.status(200).send(safe);
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
