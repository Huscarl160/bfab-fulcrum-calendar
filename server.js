// server.js
// Node 18+ (global fetch). package.json should include: "type": "module", "start": "node server.js", "engines": { "node": ">=18" }
// Env vars:
//   FULCRUM_TOKEN (required)                      - your Fulcrum Pro JWT
//   FULCRUM_BASE  (default: https://api.fulcrumpro.com)
//   ACCESS_KEY    (optional)                      - require ?key=... on requests
//   CACHE_TTL_SECONDS (default: 60)               - in-memory cache TTL per unique URL
//   CREATED_WINDOW_BUFFER_DAYS (default: 180)     - expands created window around s/u

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
const DEFAULT_STATUSES = ["scheduled", "in-progress"];

if (!TOKEN) {
  console.error("Missing FULCRUM_TOKEN env var. Exiting.");
  process.exit(1);
}

/* -------------------- express -------------------- */
const app = express();

// quick probes
app.get("/", (req, res) => res.send("OK"));
app.get("/health", (req, res) =>
  res.json({ ok: true, time: new Date().toISOString() })
);

/* -------------------- helpers -------------------- */
function icsEscape(s = "") {
  return String(s || "").replace(/([,;])/g, "\\$1").replace(/\n/g, "\\n");
}
function toUTC(dt) {
  const d = new Date(dt);
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(
    d.getUTCDate()
  )}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(
    d.getUTCSeconds()
  )}Z`;
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
    categories && categories.length
      ? `CATEGORIES:${categories.map(icsEscape).join(",")}`
      : null,
    "END:VEVENT",
  ]
    .filter(Boolean)
    .join("\r\n");
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

// RFC5545: lines must be <=75 octets; continuation lines begin with a space
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

/* -------------------- API endpoints used -------------------- */
const JOBS_LIST = "/api/jobs/list";
const JOB_OPS_LIST = (jobId) => `/api/jobs/${jobId}/operations/list`;

/* -------------------- ops selection & mapping -------------------- */
function pickPrimaryOperation(job, ops) {
  if (!Array.isArray(ops) || ops.length === 0) return null;

  const jStart = new Date(
    job.scheduledStartUtc || job.originalScheduledStartUtc || job.productionDueDate || 0
  ).getTime();
  const jEnd = new Date(
    job.scheduledEndUtc || job.originalScheduledEndUtc || 0
  ).getTime();

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
  // Prefer op window; fall back to job window; fallback +30m
  const jobStart =
    job.scheduledStartUtc || job.originalScheduledStartUtc || job.productionDueDate;
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
  const qtyMake =
    itemToMake?.quantityToMake != null ? `Qty: ${itemToMake.quantityToMake}` : "";

  const summary = [title, number, opName ? `(${opName})` : ""]
    .filter(Boolean)
    .join(" ");
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

/* -------------------- ICS route -------------------- */
// /calendar.ics?s=YYYY-MM-DD&u=YYYY-MM-DD&ops=1&statuses=scheduled,in-progress
app.get("/calendar.ics", async (req, res) => {
  try {
    // Optional gate
    if (ACCESS_KEY && req.query.key !== ACCESS_KEY) return res.sendStatus(403);

    // cache read
    const key = req.url; // per-query caching
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

    const since = req.query.s; // ISO date (no time okay)
    const until = req.query.u;
    const includeOps = req.query.ops === "1";
    const limit = parseInt(req.query.limit || "500", 10);

    // statuses: default to scheduled + in-progress, allow override via ?statuses=a,b,c
    const statuses = req.query.statuses
      ? String(req.query.statuses).split(",").map(s => s.trim()).filter(Boolean)
      : DEFAULT_STATUSES.slice();

    // ---- Build server-side body using *created* window ----
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

    // 1) list jobs (by created window + statuses)
    const jobsResp = await postJson(JOBS_LIST, listBody);
    const jobs = unwrapItems(jobsResp);

    // 2) optionally fetch operations for each job
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

    // 3) filter by actual schedule window (ops first, job fallback)
    const toMs = (d) => (d ? new Date(d).getTime() : NaN);
    const winStart = since ? new Date(since).getTime() : null;
    const winEnd   = until ? new Date(until).getTime() : null;

    const filteredJobs = jobs.filter((j) => {
      // operation times when ops=1
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

      if (winStart && e < winStart) return false; // ends before window
      if (winEnd   && s > winEnd)   return false; // starts after window
      return true;
    });

    const events = filteredJobs.map((j) => {
      const pair = primaryOpByJob.get(j.id);
      const primaryOp = pair?.op || null;
      const itemToMake = pair?.itm || null;
      return mapJobToEvent(j, primaryOp, itemToMake);
    });

    // 4) build ICS
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

    // cache write
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

app.get("/test.ics", (req, res) => {
  const now = new Date();
  const in30 = new Date(now.getTime() + 30 * 60 * 1000);

  const pad = (n) => String(n).padStart(2, "0");
  const toUTC = (d) =>
    `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;

  const ics = [
    "BEGIN:VCALENDAR",
    "VERSION:2.0",
    "PRODID:-//Bettis//Fulcrum Test//EN",
    "CALSCALE:GREGORIAN",
    "METHOD:PUBLISH",
    "X-WR-CALNAME:Fulcrum Test",
    "X-WR-TIMEZONE:UTC",
    "BEGIN:VEVENT",
    "UID:test-one@" + "bettis",
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
