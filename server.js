// server.js (Node 18+)
// npm i express
import express from "express";
import crypto from "crypto";

const PORT = process.env.PORT || 8787;
const BASE = process.env.FULCRUM_BASE || "https://api.fulcrumpro.com";
const TOKEN = process.env.FULCRUM_TOKEN;

// Endpoints
const JOBS_LIST = "/api/jobs/list";
const JOB_OPS_LIST = (jobId) => `/api/jobs/${jobId}/operations/list`;

/* ---------------- helpers ---------------- */
function icsEscape(s = "") {
    return String(s || "").replace(/([,;])/g, "\\$1").replace(/\n/g, "\\n");
}
function toUTC(dt) {
    const d = new Date(dt);
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getUTCFullYear()}${pad(d.getUTCMonth()+1)}${pad(d.getUTCDate())}T${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
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
        (categories && categories.length) ? `CATEGORIES:${categories.map(icsEscape).join(",")}` : null,
        "END:VEVENT"
    ].filter(Boolean).join("\r\n");
}
async function postJson(path, body) {
    const res = await fetch(`${BASE}${path}`, {
        method: "POST",
        headers: {
            "Authorization": `Bearer ${TOKEN}`,
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        body: JSON.stringify(body || {})
    });
    if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
    return res.json();
}

/* ---------------- data enrichment ---------------- */

// choose the best/primary operation for a given job
function pickPrimaryOperation(job, ops) {
    if (!Array.isArray(ops) || ops.length === 0) return null;

    // Prefer an operation whose scheduled window overlaps the job’s scheduled window,
    // otherwise fall back to the earliest scheduled operation.
    const jStart = new Date(job.scheduledStartUtc || job.originalScheduledStartUtc || job.productionDueDate || 0).getTime();
    const jEnd   = new Date(job.scheduledEndUtc   || job.originalScheduledEndUtc   || 0).getTime();

    let candidates = ops.filter(o => o.scheduledStartUtc || o.originalScheduledStartUtc);

    if (jStart) {
        candidates = candidates.sort((a, b) =>
            new Date(a.scheduledStartUtc || a.originalScheduledStartUtc).getTime() -
            new Date(b.scheduledStartUtc || b.originalScheduledStartUtc).getTime()
        );

        const overlapping = candidates.find(o => {
            const os = new Date(o.scheduledStartUtc || o.originalScheduledStartUtc).getTime();
            const oe = new Date(o.scheduledEndUtc   || o.originalScheduledEndUtc   || os).getTime();
            if (jEnd) return (os <= jEnd) && (oe >= jStart);  // overlap
            return os >= jStart;                               // on/after job start
        });

        return overlapping || candidates[0];
    }

    // No job start? Just take earliest operation
    return candidates.sort((a, b) =>
        new Date(a.scheduledStartUtc || a.originalScheduledStartUtc) -
        new Date(b.scheduledStartUtc || b.originalScheduledStartUtc)
    )[0];
}

function mapJobToEvent(job, primaryOp, itemToMake) {
    // Base job timing
    const jobStart = job.scheduledStartUtc || job.originalScheduledStartUtc || job.productionDueDate;
    let jobEnd     = job.scheduledEndUtc   || job.originalScheduledEndUtc;

    // If we have a primary operation, prefer its window (usually tighter/more accurate)
    const opStart = primaryOp?.scheduledStartUtc || primaryOp?.originalScheduledStartUtc;
    const opEnd   = primaryOp?.scheduledEndUtc   || primaryOp?.originalScheduledEndUtc;

    const start = opStart || jobStart;
    let end     = opEnd   || jobEnd;

    if (!end && start) {
        // default to +30m if there's only a start
        end = new Date(new Date(start).getTime() + 30 * 60 * 1000).toISOString();
    }

    // Titles & identifiers
    const title   = job.name || (job.number != null ? `Job #${job.number}` : "Scheduled Work");
    const number  = (job.number != null) ? `#${job.number}` : "";
    const status  = job.status || "";
    const project = job.salesOrderId || "";

    // Equipment / operation
    const equipment = primaryOp?.scheduledEquipmentName || "";
    const opName    = primaryOp?.name || "";

    // Item to make (from your sample operation wrapper)
    const itemName = itemToMake?.itemReference?.name || itemToMake?.itemReference?.number || "";
    const itemDesc = itemToMake?.itemReference?.description || "";
    const qtyMake  = itemToMake?.quantityToMake != null ? `Qty: ${itemToMake.quantityToMake}` : "";

    const summary = [title, number, opName ? `(${opName})` : ""].filter(Boolean).join(" ");
    const location = equipment || ""; // drives room/machine display

    const descLines = [
        status ? `Status: ${status}` : null,
        project ? `Sales Order: ${project}` : null,
        equipment ? `Equipment: ${equipment}` : null,
        opName ? `Operation: ${opName}` : null,
        itemName ? `Item: ${itemName}` : null,
        itemDesc ? `Desc: ${itemDesc}` : null,
        qtyMake || null,
        job.id ? `Job ID: ${job.id}` : null
    ].filter(Boolean);

    const categories = [
        equipment || null,
        opName || null,
        status || null
    ].filter(Boolean);

    return {
        id: job.id,
        start, end,
        summary,
        location,
        description: descLines.join("\\n"),
        categories
    };
}

/* ---------------- express app ---------------- */

const app = express();

// /calendar.ics?s=2025-08-01&u=2025-08-31&status=scheduled&ops=1
app.get("/calendar.ics", async (req, res) => {
    try {
        const since  = req.query.s;
        const until  = req.query.u;
        const status = req.query.status;
        const includeOps = (req.query.ops === "1"); // fetch operations if requested
        const limit = parseInt(req.query.limit || "500", 10);

        // 1) list jobs
        const jobsResp = await postJson(JOBS_LIST, { limit /* add server-side filters later */ });
        const jobs = Array.isArray(jobsResp) ? jobsResp : (jobsResp.items || jobsResp.results || jobsResp.data || []);

        // 2) optionally fetch operations per job
        const primaryOpByJob = new Map();
        if (includeOps) {
            for (const job of jobs) {
                try {
                    const opsResp = await postJson(JOB_OPS_LIST(job.id), { limit: 200 });
                    const ops = Array.isArray(opsResp) ? opsResp : (opsResp.items || opsResp.results || opsResp.data || []);

                    // Your sample shows the op wrapped with itemToMake and operation objects
                    // Normalize to { op, itm } pairs.
                    const pairs = ops.map(o => ({
                        op: o.operation || o,          // some APIs return { operation: {...}, itemToMake: {...} }
                        itm: o.itemToMake || null
                    }));

                    // pick primary
                    const primaryPair =
                        pickPrimaryOperation(job, pairs.map(p => p.op)) &&
                        pairs.find(p => (p.op?.id === pickPrimaryOperation(job, pairs.map(x => x.op))?.id));

                    primaryOpByJob.set(job.id, primaryPair || null);
                } catch {
                    primaryOpByJob.set(job.id, null);
                }
            }
        }

        // 3) filter + map
        const filtered = jobs.filter(j => {
            const start = j.scheduledStartUtc || j.originalScheduledStartUtc || j.productionDueDate;
            if (!start) return false;
            const t = new Date(start).getTime();
            if (since && t < new Date(since).getTime()) return false;
            if (until && t > new Date(until).getTime()) return false;
            if (status && String(j.status || "").toLowerCase() !== String(status).toLowerCase()) return false;
            return true;
        });

        const events = filtered.map(j => {
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
            ...events.map(e => vevent({
                uid: crypto.createHash("sha1").update(`fulcrum:${e.id}`).digest("hex") + "@bettis",
                start: e.start, end: e.end,
                summary: e.summary,
                location: e.location,
                description: e.description,
                categories: e.categories
            })),
            "END:VCALENDAR"
        ].join("\r\n");

        res.setHeader("Content-Type", "text/calendar; charset=utf-8");
        res.setHeader("Content-Disposition", 'inline; filename="bettis-fulcrum.ics"');
        res.setHeader("Cache-Control", "no-cache");
        res.status(200).send(ics);
    } catch (err) {
        res.status(500).send(`Error: ${err.message}`);
    }
});

app.listen(PORT, () => console.log(`ICS feed running on :${PORT}`));
