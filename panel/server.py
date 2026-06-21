"""AgentDex live demo panel.

Tails the Redis demo-event stream and pushes every event to the browser over
SSE. The browser reconstructs state and renders two columns:

  • Live queries      — what the "developer's agent" asks, and how it's served.
  • Speculative pool   — workers pre-warming likely follow-ups, with the exact
                         timestamp each topic became ready.

The payoff: when a query is served as a WARM HIT, the panel shows how many
seconds *earlier* the speculative worker had already finished that topic — i.e.
the answer existed before the question was asked.

Run:
    uvicorn panel.server:app --reload --port 8000
Then open http://localhost:8000
"""

import asyncio
import json

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse

from shared.demo_events import history, read_events, reset

load_dotenv()

app = FastAPI(title="AgentDex Panel")


@app.get("/", response_class=HTMLResponse)
async def index():
    return _PAGE


@app.post("/reset")
async def do_reset():
    reset()
    return {"ok": True}


@app.get("/events")
async def events():
    """SSE: replay history, then stream live events."""

    async def gen():
        last_id = "0"
        # Cold-start: send everything already in the stream.
        for entry_id, payload in await asyncio.to_thread(history):
            last_id = entry_id
            yield f"data: {json.dumps(payload)}\n\n"
        # Live tail.
        while True:
            batch = await asyncio.to_thread(read_events, last_id, 15_000)
            if not batch:
                yield ": keep-alive\n\n"  # comment frame so the connection stays open
                continue
            for entry_id, payload in batch:
                last_id = entry_id
                yield f"data: {json.dumps(payload)}\n\n"

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>AgentDex — Live</title>
<style>
  :root {
    --bg:#0a0e14; --panel:#121823; --line:#1f2937; --muted:#7d8aa0;
    --txt:#e6edf3; --accent:#39d98a; --warn:#f5b14c; --bad:#ef5e6a; --cold:#5aa9ff;
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--txt);
         font:15px/1.5 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; }
  header { padding:18px 28px; border-bottom:1px solid var(--line);
           display:flex; align-items:center; gap:16px; }
  header h1 { font-size:20px; margin:0; letter-spacing:.5px; }
  header .tag { color:var(--muted); font-size:13px; }
  #banner { margin-left:auto; font-weight:600; font-size:15px; padding:8px 16px;
            border-radius:8px; opacity:0; transition:opacity .3s; }
  #banner.show { opacity:1; }
  #banner.win { background:rgba(57,217,138,.15); color:var(--accent);
                border:1px solid var(--accent); }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:0; height:calc(100vh - 67px); }
  .col { padding:20px 24px; overflow-y:auto; }
  .col + .col { border-left:1px solid var(--line); }
  .col h2 { font-size:13px; text-transform:uppercase; letter-spacing:1.5px;
            color:var(--muted); margin:0 0 16px; }
  .card { background:var(--panel); border:1px solid var(--line); border-left-width:3px;
          border-radius:8px; padding:12px 14px; margin-bottom:10px; animation:in .25s ease; }
  @keyframes in { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:none} }
  .card .top { display:flex; align-items:center; gap:10px; }
  .card .topic { font-weight:600; font-size:15px; }
  .card .t { margin-left:auto; color:var(--muted); font-variant-numeric:tabular-nums;
             font-size:12px; }
  .card .sub { color:var(--muted); font-size:13px; margin-top:4px; }
  .pill { font-size:11px; padding:2px 8px; border-radius:99px; font-weight:600;
          text-transform:uppercase; letter-spacing:.5px; }
  .running { border-left-color:var(--warn); }
  .running .pill { background:rgba(245,177,76,.16); color:var(--warn); }
  .running .topic::after { content:" ●"; color:var(--warn); animation:pulse 1s infinite; }
  @keyframes pulse { 50%{opacity:.25} }
  .warm    { border-left-color:var(--accent); }
  .warm .pill { background:rgba(57,217,138,.16); color:var(--accent); }
  .timeout { border-left-color:var(--bad); opacity:.6; }
  .timeout .pill { background:rgba(239,94,106,.16); color:var(--bad); }
  .rejected { border-left-color:var(--muted); opacity:.5; }
  .rejected .pill { background:rgba(125,138,160,.16); color:var(--muted); }
  .rejected .topic { text-decoration:line-through; text-decoration-color:var(--muted); }
  .cold    { border-left-color:var(--cold); }
  .cold .pill { background:rgba(90,169,255,.16); color:var(--cold); }
  .hit     { border-left-color:var(--accent); background:rgba(57,217,138,.07); }
  .hit .pill { background:var(--accent); color:#06281a; }
  .proof { margin-top:6px; font-size:13px; color:var(--accent); font-weight:600; }
  .empty { color:var(--muted); font-style:italic; }
</style>
</head>
<body>
<header>
  <h1>AgentDex</h1>
  <span class="tag">speculative research · live demo</span>
  <div id="banner"></div>
</header>
<div class="grid">
  <div class="col"><h2>Live queries</h2><div id="queries"><div class="empty">waiting for the developer's agent…</div></div></div>
  <div class="col"><h2>Speculative pool</h2><div id="workers"><div class="empty">no speculative workers yet</div></div></div>
</div>

<script>
let t0 = null;
const workers = {};        // topic -> {started, warm, timeout, el}
const warmTimes = {};      // topic -> epoch seconds it became warm
const qEl = document.getElementById("queries");
const wEl = document.getElementById("workers");
const banner = document.getElementById("banner");

function rel(ts) {
  if (t0 === null) return "T+0.0s";
  return "T+" + (ts - t0).toFixed(1) + "s";
}
function clearEmpty(host) { const e = host.querySelector(".empty"); if (e) e.remove(); }

function flashBanner(text) {
  banner.textContent = text;
  banner.className = "show win";
}

function upsertWorker(topic) {
  if (workers[topic]?.el) return workers[topic];
  clearEmpty(wEl);
  const el = document.createElement("div");
  el.className = "card running";
  wEl.prepend(el);
  workers[topic] = { ...(workers[topic]||{}), el };
  return workers[topic];
}
function renderWorker(topic) {
  const w = workers[topic]; if (!w?.el) return;
  let cls = "running", pill = "running", t = rel(w.started), sub = "browsing + structuring…";
  if (w.rejected) {
    cls="rejected"; pill="filtered"; t=rel(w.rejected);
    sub = "relevance filter rejected" + (w.reason ? " — " + w.reason : "");
  }
  else if (w.timeout) { cls="timeout"; pill="timeout"; sub="bet expired — discarded"; }
  else if (w.warm) {
    cls="warm"; pill="ready"; t=rel(w.warm);
    sub = "warmed in " + ((w.warm - w.started)).toFixed(1) + "s · cached for instant serving";
  }
  w.el.className = "card " + cls;
  w.el.innerHTML = `<div class="top"><span class="topic">${topic}</span>
    <span class="pill">${pill}</span><span class="t">${t}</span></div>
    <div class="sub">${sub}</div>`;
}

function addQuery(html, cls="") {
  clearEmpty(qEl);
  const el = document.createElement("div");
  el.className = "card " + cls;
  el.innerHTML = html;
  qEl.prepend(el);
  return el;
}

function handle(ev) {
  if (t0 === null) t0 = ev.timestamp;
  const ts = ev.timestamp, T = rel(ts);

  switch (ev.type) {
    case "dev_query":
      addQuery(`<div class="top"><span class="topic">“${ev.topic}”</span>
        <span class="pill" style="background:#1f2937;color:#9fb0c8">asked</span>
        <span class="t">${T}</span></div>
        <div class="sub">developer's agent requests <b>${ev.topic}</b></div>`);
      break;

    case "warm_hit": {
      // Person A's vector search can satisfy a query with a semantically-near
      // cached topic (e.g. "electron" -> "electrons"). Prefer the matched topic
      // it reports; fall back to the query string for exact hits.
      const matched = ev.matched_topic || ev.topic;
      // Lead time: prefer the spec_warm timestamp we observed live; otherwise
      // use a warmed_at epoch the orchestrator supplies (survives reconnects).
      let wt = warmTimes[matched.toLowerCase()];
      if (wt == null && ev.warmed_at != null) wt = ev.warmed_at;

      let proof = "", banner = `⚡ WARM HIT — “${ev.topic}” served instantly`;
      const via = (ev.matched_topic && ev.matched_topic.toLowerCase() !== ev.topic.toLowerCase())
        ? ` via “${ev.matched_topic}”${ev.similarity != null ? ` (${(+ev.similarity).toFixed(2)} sim)` : ""}`
        : "";
      if (wt != null) {
        const lead = (ts - wt).toFixed(1);
        proof = `<div class="proof">⚡ answer was ready ${lead}s before the question${via}</div>`;
        banner = `⚡ WARM HIT — “${ev.topic}” served instantly (ready ${lead}s early)`;
      } else if (via) {
        proof = `<div class="proof">⚡ matched a pre-warmed topic${via}</div>`;
      }
      flashBanner(banner);
      addQuery(`<div class="top"><span class="topic">“${ev.topic}”</span>
        <span class="pill">warm hit</span><span class="t">${T}</span></div>
        <div class="sub">served from cache — no browsing needed</div>${proof}`, "hit");
      break;
    }

    case "cold_dispatch":
      addQuery(`<div class="top"><span class="topic">“${ev.topic}”</span>
        <span class="pill">cold</span><span class="t">${T}</span></div>
        <div class="sub">not cached — dispatching primary worker</div>`, "cold");
      break;

    case "dev_result":
      // The warm/cold story is already shown by warm_hit / cold_dispatch; keep quiet.
      break;

    case "spec_dispatch":
      workers[ev.topic] = { ...(workers[ev.topic]||{}), started: ts };
      upsertWorker(ev.topic);
      renderWorker(ev.topic);
      break;

    case "spec_started":
      if (!workers[ev.topic]?.started) {
        workers[ev.topic] = { ...(workers[ev.topic]||{}), started: ts };
      }
      upsertWorker(ev.topic);
      renderWorker(ev.topic);
      break;

    case "spec_warm":
      warmTimes[ev.topic.toLowerCase()] = ts;
      workers[ev.topic] = { ...(workers[ev.topic]||{}), warm: ts };
      if (!workers[ev.topic].started) workers[ev.topic].started = ts;
      upsertWorker(ev.topic);
      renderWorker(ev.topic);
      break;

    case "spec_timeout":
      workers[ev.topic] = { ...(workers[ev.topic]||{}), timeout: ts };
      upsertWorker(ev.topic);
      renderWorker(ev.topic);
      break;

    case "candidate_rejected":
      // Person A's relevance filter dropped this candidate before any bet.
      workers[ev.topic] = {
        ...(workers[ev.topic]||{}),
        rejected: ts,
        reason: ev.reason || "not relevant",
      };
      upsertWorker(ev.topic);
      renderWorker(ev.topic);
      break;
  }
}

const es = new EventSource("/events");
es.onmessage = (m) => { try { handle(JSON.parse(m.data)); } catch(e){} };
</script>
</body>
</html>
"""
