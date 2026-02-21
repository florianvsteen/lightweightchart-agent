"""
server.py

PairServer — a self-contained Flask server instance for a single trading pair.
Each pair runs in its own thread on its own port.

Detection runs in a background thread every 30 seconds — completely independent
of whether anyone has the browser open. Discord alerts fire from there.
The Flask routes only serve chart data to the browser when it's open.
"""

import os
import json
import time
import threading
import pandas as pd
import yfinance as yf
from flask import Flask, render_template, jsonify, request

from detectors import REGISTRY

try:
    from discord_webhook import DiscordWebhook, DiscordEmbed
    DISCORD_AVAILABLE = True
except ImportError:
    DISCORD_AVAILABLE = False

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

# ── Debug page HTML ─────────────────────────────────────────────────────────
DEBUG_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<title>Debug — __LABEL__</title>
<script src="https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg:      #131316;
    --surface: #1a1a1f;
    --border:  #252530;
    --text:    #c8c8d8;
    --muted:   #55556a;
    --accent:  #5af0c4;
    --red:     #f05a7e;
    --yellow:  #f0c45a;
    --blue:    #5a9ef0;
    --pass:    #5af0c4;
  }
  html, body { height: 100%; overflow: hidden; background: var(--bg); color: var(--text);
    font-family: 'Space Mono', 'Menlo', monospace; font-size: 12px; }
  #layout { display: flex; height: 100vh; flex-direction: column; }
  #main   { display: flex; flex: 1; min-height: 0; }

  /* ── Left: chart ── */
  #left { flex: 1; display: flex; flex-direction: column; min-width: 0; }
  #topbar { display: flex; align-items: center; gap: 10px; padding: 8px 14px;
    border-bottom: 1px solid var(--border); flex-shrink: 0; flex-wrap: wrap; }
  #topbar h1 { font-size: 13px; color: #fff; white-space: nowrap; margin-right: 4px; }
  .tag { font-size: 10px; padding: 2px 8px; border-radius: 3px; border: 1px solid var(--border);
    color: var(--muted); white-space: nowrap; }
  .tag.session { border-color: #5a9ef0; color: #5a9ef0; }
  .tag.passed  { border-color: var(--pass); color: var(--pass); }
  .tag.range   { border-color: var(--yellow); color: var(--yellow); }
  .tag.replay  { border-color: var(--red); color: var(--red); }
  #btn-group { display: flex; gap: 6px; margin-left: auto; }
  .rbtn { background: var(--surface); border: 1px solid var(--border); border-radius: 3px;
    color: var(--muted); cursor: pointer; font-size: 11px; font-family: inherit;
    padding: 3px 10px; transition: all 0.15s; white-space: nowrap; }
  .rbtn:hover { border-color: var(--accent); color: var(--accent); }
  .rbtn.active { border-color: var(--accent); color: var(--accent); background: rgba(90,240,196,0.06); }
  .rbtn:disabled { opacity: 0.3; cursor: default; pointer-events: none; }

  #chart-wrap { flex: 1; position: relative; min-height: 0; }
  #chart { width: 100%; height: 100%; }

  /* ── Replay cursor line ── */
  #cursor-line {
    position: absolute; top: 0; bottom: 0; width: 1px;
    background: rgba(240, 90, 126, 0.6);
    pointer-events: none; display: none;
    z-index: 10;
  }

  /* ── Scrubber bar ── */
  #scrubber-row { display: flex; align-items: center; gap: 10px; padding: 6px 14px;
    border-top: 1px solid var(--border); flex-shrink: 0; background: var(--bg); }
  #scrubber { flex: 1; accent-color: var(--accent); cursor: pointer; height: 4px; }
  #scrub-label { font-size: 10px; color: var(--muted); min-width: 110px; text-align: right; white-space: nowrap; }
  #scrub-ts    { font-size: 10px; color: var(--muted); min-width: 120px; white-space: nowrap; }
  #replay-speed { width: 60px; font-size: 10px; background: var(--surface);
    border: 1px solid var(--border); border-radius: 3px; color: var(--text);
    padding: 2px 5px; font-family: inherit; }

  /* ── Right panel ── */
  #right { width: 340px; flex-shrink: 0; display: flex; flex-direction: column;
    border-left: 1px solid var(--border); overflow: hidden; }
  #right-tabs { display: flex; border-bottom: 1px solid var(--border); flex-shrink: 0; }
  .rtab { flex: 1; padding: 8px 4px; text-align: center; font-size: 10px; letter-spacing: 0.08em;
    text-transform: uppercase; cursor: pointer; color: var(--muted); border-bottom: 2px solid transparent;
    transition: all 0.15s; }
  .rtab.active { color: var(--accent); border-bottom-color: var(--accent); }
  .rtab-panel { display: none; flex: 1; overflow-y: auto; padding: 10px; }
  .rtab-panel.active { display: block; }

  /* Summary */
  .stat-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin-bottom: 12px; }
  .stat-card { background: var(--surface); border: 1px solid var(--border); border-radius: 4px; padding: 8px 10px; }
  .stat-label { font-size: 9px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 3px; }
  .stat-val { font-size: 18px; font-weight: 700; color: #fff; }
  .stat-val.green  { color: var(--pass); }
  .stat-val.red    { color: var(--red);  }
  .stat-val.yellow { color: var(--yellow); }
  .reject-list { margin-top: 10px; }
  .reject-row { padding: 5px 8px; border-radius: 3px; margin-bottom: 3px;
    background: var(--surface); border-left: 3px solid; }
  .reject-row.range   { border-color: var(--yellow); }
  .reject-row.slope   { border-color: var(--blue); }
  .reject-row.adx     { border-color: var(--red); }
  .reject-row.chop    { border-color: #a070f0; }
  .reject-row.v_shape { border-color: var(--muted); }
  .reject-label { font-size: 10px; color: var(--text); }
  .reject-count { font-size: 12px; font-weight: 700; color: #fff; }
  .reject-bar  { height: 3px; background: var(--border); border-radius: 2px; margin-top: 4px; }
  .reject-bar-fill { height: 100%; border-radius: 2px; }

  /* Windows */
  #window-search { width: 100%; background: var(--surface); border: 1px solid var(--border);
    border-radius: 3px; color: var(--text); padding: 5px 8px; font-family: inherit;
    font-size: 11px; margin-bottom: 8px; }
  #window-search:focus { outline: none; border-color: var(--accent); }
  .win-row { display: flex; align-items: center; gap: 6px; padding: 5px 8px; border-radius: 3px;
    margin-bottom: 2px; cursor: pointer; border: 1px solid transparent; transition: all 0.1s; }
  .win-row:hover   { border-color: var(--border); background: var(--surface); }
  .win-row.selected { border-color: var(--accent); background: rgba(90,240,196,0.04); }
  .win-row.pass-row { border-left: 3px solid var(--pass); }
  .win-row.fail-row { border-left: 3px solid #333; }
  .win-num    { width: 28px; color: var(--muted); font-size: 10px; flex-shrink: 0; }
  .win-status { font-size: 10px; flex: 1; }
  .win-status.pass { color: var(--pass); }
  .win-status.fail { color: var(--muted); }
  .win-chop   { font-size: 10px; color: var(--muted); width: 42px; text-align: right; flex-shrink: 0; }

  /* Detail */
  #detail-title { font-size: 11px; color: var(--accent); margin-bottom: 10px; padding-bottom: 6px;
    border-bottom: 1px solid var(--border); }
  .detail-row { display: flex; justify-content: space-between; padding: 4px 0;
    border-bottom: 1px solid rgba(255,255,255,0.04); }
  .detail-key  { color: var(--muted); font-size: 10px; }
  .detail-val  { font-size: 10px; font-weight: 600; }
  .detail-val.pass    { color: var(--pass); }
  .detail-val.fail    { color: var(--red);  }
  .detail-val.neutral { color: var(--text); }
  .bar-track { height: 3px; background: var(--border); border-radius: 2px; flex: 1; margin-left: 10px; }
  .bar-fill  { height: 100%; border-radius: 2px; max-width: 100%; }

  /* Replay status banner */
  #replay-banner { display: none; padding: 4px 14px; font-size: 10px; color: var(--red);
    background: rgba(240,90,126,0.07); border-bottom: 1px solid rgba(240,90,126,0.2);
    letter-spacing: 0.06em; }

  ::-webkit-scrollbar { width: 4px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }
</style>
</head>
<body>
<div id="layout">
  <div id="main">

    <!-- LEFT: Chart + scrubber -->
    <div id="left">
      <div id="topbar">
        <h1>⚙ DEBUG — __LABEL__</h1>
        <span class="tag session" id="tag-session">loading…</span>
        <span class="tag passed"  id="tag-passed">…</span>
        <span class="tag range"   id="tag-range">…</span>
        <span class="tag replay"  id="tag-replay" style="display:none">⏮ REPLAY</span>
        <div id="btn-group">
          <button class="rbtn active" id="btn-live">Live</button>
          <button class="rbtn" id="btn-replay">⏮ Replay</button>
          <button class="rbtn" id="btn-prev" disabled>◀</button>
          <button class="rbtn" id="btn-play" disabled>▶</button>
          <button class="rbtn" id="btn-next" disabled>▶|</button>
          <select id="replay-speed" disabled>
            <option value="600">0.5×</option>
            <option value="300" selected>1×</option>
            <option value="150">2×</option>
            <option value="60">4×</option>
          </select>
        </div>
      </div>
      <div id="replay-banner">⏮ REPLAY MODE — click any candle or drag the scrubber to seek</div>
      <div id="chart-wrap">
        <div id="chart"></div>
        <div id="cursor-line"></div>
      </div>
      <div id="scrubber-row">
        <input type="range" id="scrubber" min="0" value="0" step="1" disabled />
        <span id="scrub-ts">—</span>
        <span id="scrub-label">candle — / —</span>
        <button class="rbtn" id="btn-analyze" disabled>▶ Run Detector</button>
      </div>
    </div>

    <!-- RIGHT: Debug panel -->
    <div id="right">
      <div id="right-tabs">
        <div class="rtab active" data-tab="summary">Summary</div>
        <div class="rtab" data-tab="windows">Windows</div>
        <div class="rtab" data-tab="detail">Detail</div>
      </div>
      <div id="tab-summary" class="rtab-panel active">
        <div class="stat-grid" id="stat-grid"></div>
        <div class="reject-list" id="reject-list"></div>
      </div>
      <div id="tab-windows" class="rtab-panel">
        <input id="window-search" placeholder="Filter: pass / fail / range / slope / adx…" />
        <div id="window-list"></div>
      </div>
      <div id="tab-detail" class="rtab-panel">
        <div id="detail-title">← click a window row</div>
        <div id="detail-body"></div>
      </div>
    </div>

  </div>
</div>

<script>
const PAIR_ID  = "__PAIR_ID__";
const TIMEZONE = "__TIMEZONE__";

// ── Timezone offset (same logic as production chart) ─────────────────────
function getTzOffsetSeconds(tz) {
  try {
    const now    = new Date();
    const utcStr = now.toLocaleString('en-US', { timeZone: 'UTC' });
    const tzStr  = now.toLocaleString('en-US', { timeZone: tz });
    return (new Date(tzStr) - new Date(utcStr)) / 1000;
  } catch(e) { return 0; }
}
const TZ_OFFSET = getTzOffsetSeconds(TIMEZONE);
function shiftTime(ts) { return ts + TZ_OFFSET; }

// ── State ─────────────────────────────────────────────────────────────────
let liveData    = null;   // full /debug/data payload (never changes during session)
let replayData  = null;   // current /debug/replay?idx=N payload
let replayMode  = false;
let replayIdx   = 0;
let replayTimer = null;
let selectedWindow = null;
let overlaySeriesList = [];

// ── Chart ─────────────────────────────────────────────────────────────────
const container = document.getElementById('chart');
const chart = LightweightCharts.createChart(container, {
  layout:    { background: { color: '#131316' }, textColor: '#c8c8d8' },
  grid:      { vertLines: { visible: false }, horzLines: { visible: false } },
  timeScale: { timeVisible: true, secondsVisible: false, borderColor: '#252530' },
  crosshair: { mode: 1 },
});
const candleSeries = chart.addCandlestickSeries({
  upColor: '#d4d0d0', downColor: '#068c76',
  wickUpColor: '#d4d0d0', wickDownColor: '#068c76',
  borderVisible: false,
});

// Click-to-seek: when in replay mode, clicking a candle seeks to that index
chart.subscribeClick(param => {
  if (!replayMode || !param.time || !liveData) return;
  const ts  = param.time;
  const idx = liveData.candles.findIndex(c => c.time === ts);
  if (idx >= 0) seekTo(idx + 1);  // idx+1 = how many candles are visible (1-based)
});

window.addEventListener('resize', () => chart.applyOptions({
  width:  container.clientWidth,
  height: container.clientHeight,
}));

// ── Tabs ──────────────────────────────────────────────────────────────────
document.querySelectorAll('.rtab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.rtab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.rtab-panel').forEach(p => p.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
  });
});

// ── Load live data ────────────────────────────────────────────────────────
async function loadLiveData() {
  if (replayMode) return;   // don't clobber replay state
  const res = await fetch(`/debug/data`);
  liveData = await res.json();
  renderChart(liveData.candles);
  renderSummary(liveData);
  renderWindowList(liveData.windows);
  updateTopbar(liveData, false);
  const scrubber = document.getElementById('scrubber');
  scrubber.max   = liveData.candles.length - 1;
  scrubber.value = liveData.candles.length - 1;
}

// ── Replay fetch (real server-side analysis on sliced df) ────────────────
let replayFetchController = null;

async function fetchReplay(idx) {
  if (replayFetchController) replayFetchController.abort();
  replayFetchController = new AbortController();
  const btn = document.getElementById('btn-analyze');
  btn.textContent = '⏳ Running…';
  btn.disabled = true;
  try {
    const res = await fetch(`/debug/replay?idx=${idx}`, { signal: replayFetchController.signal });
    const data = await res.json();
    if (data.error) {
      console.error('Replay error from server:', data.error, data.trace || '');
      document.getElementById('tag-session').textContent = 'ERROR — see console';
      return;
    }
    replayData = data;
    renderSummary(replayData);
    renderWindowList(replayData.windows || []);
    updateTopbar(replayData, true);
    if (selectedWindow == null && replayData.best_zone) {
      drawWindowOverlay(replayData.best_zone);
    } else if (selectedWindow != null) {
      const w = (replayData.windows || []).find(x => x.window === selectedWindow);
      drawWindowOverlay(w || null);
    } else {
      drawWindowOverlay(null);
    }
  } catch(e) {
    if (e.name !== 'AbortError') console.error('Replay fetch error', e);
  } finally {
    btn.textContent = '▶ Run Detector';
    btn.disabled = false;
  }
}

// ── Seek to candle index (chart only — analysis runs on button click) ────
async function seekTo(idx) {
  const total = liveData.candles.length;
  idx = Math.max(18, Math.min(idx, total));
  replayIdx = idx;

  const slice = liveData.candles.slice(0, idx);
  candleSeries.setData(slice.map(c => ({ ...c, time: shiftTime(c.time) })));
  chart.timeScale().fitContent();

  const scrubber = document.getElementById('scrubber');
  scrubber.value = idx - 1;
  const lastCandle = slice[slice.length - 1];
  const dt = lastCandle ? new Date((lastCandle.time + TZ_OFFSET) * 1000) : null;
  document.getElementById('scrub-ts').textContent = dt
    ? dt.toISOString().replace('T', ' ').slice(0, 16) + ' ' + TIMEZONE : '—';
  document.getElementById('scrub-label').textContent = `candle ${idx} / ${total}`;
}

// ── Topbar update ─────────────────────────────────────────────────────────
function updateTopbar(d, isReplay) {
  document.getElementById('tag-session').textContent =
    d.session ? d.session.replace('_',' ').toUpperCase() : 'OUT OF SESSION';
  document.getElementById('tag-passed').textContent =
    `${d.passed} / ${d.windows_checked} passed`;
  document.getElementById('tag-range').textContent =
    `range ${d.effective_range ? (d.effective_range * 100).toFixed(3) + '%' : '—'}`;
  document.getElementById('tag-replay').style.display = isReplay ? '' : 'none';
}

// ── Summary tab ───────────────────────────────────────────────────────────
function renderSummary(d) {
  const total    = d.windows_checked || 1;
  const passRate = Math.round((d.passed / total) * 100);
  const stats = [
    { label: 'Candles',   val: d.idx || d.candles?.length || '—',  cls: 'neutral' },
    { label: 'Windows',   val: d.windows_checked,                   cls: 'neutral' },
    { label: 'Passed',    val: d.passed,                            cls: d.passed > 0 ? 'green' : 'red' },
    { label: 'Pass Rate', val: passRate + '%',                      cls: passRate > 10 ? 'green' : 'yellow' },
  ];
  document.getElementById('stat-grid').innerHTML = stats.map(s => `
    <div class="stat-card">
      <div class="stat-label">${s.label}</div>
      <div class="stat-val ${s.cls}">${s.val}</div>
    </div>`).join('');

  const reasons = d.rejection_summary || {};
  const sorted  = Object.entries(reasons).sort((a, b) => b[1] - a[1]);
  const maxCnt  = sorted.length ? sorted[0][1] : 1;
  const colorMap = { range:'#f0c45a', slope:'#5a9ef0', adx:'#f05a7e', chop:'#a070f0', v_shape:'#666' };
  document.getElementById('reject-list').innerHTML = `
    <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px;">Rejection Reasons</div>
    ${sorted.map(([key, cnt]) => `
      <div class="reject-row ${key}">
        <div style="display:flex;justify-content:space-between">
          <span class="reject-label">${key}</span>
          <span class="reject-count">${cnt}</span>
        </div>
        <div class="reject-bar">
          <div class="reject-bar-fill" style="width:${Math.round(cnt/maxCnt*100)}%;background:${colorMap[key]||'#666'}"></div>
        </div>
      </div>`).join('')}`;
}

// ── Windows tab ───────────────────────────────────────────────────────────
function getActiveWindows() {
  return replayMode && replayData ? replayData.windows : (liveData?.windows || []);
}

function renderWindowList(windows, filter = '') {
  const lf       = filter.toLowerCase().trim();
  const filtered = windows.filter(w => {
    if (!lf) return true;
    if (lf === 'pass') return w.pass;
    if (lf === 'fail') return !w.pass;
    return (w.reject || '').toLowerCase().includes(lf);
  });
  document.getElementById('window-list').innerHTML = filtered.map(w => {
    const cls   = w.pass ? 'pass-row' : 'fail-row';
    const stCls = w.pass ? 'pass' : 'fail';
    const label = w.pass ? '✓ pass' : (w.reject || '?').split(' ')[0];
    return `<div class="win-row ${cls}" data-win="${w.window}" onclick="selectWindow(${w.window})">
      <span class="win-num">${w.window}</span>
      <span class="win-status ${stCls}">${label}</span>
      <span class="win-chop">chop ${w.chop}</span>
    </div>`;
  }).join('');
  if (selectedWindow != null) {
    const el = document.querySelector(`[data-win="${selectedWindow}"]`);
    if (el) el.classList.add('selected');
  }
}

document.getElementById('window-search').addEventListener('input', e => {
  renderWindowList(getActiveWindows(), e.target.value);
});

// ── Detail tab ────────────────────────────────────────────────────────────
function selectWindow(windowSize) {
  selectedWindow = windowSize;
  document.querySelectorAll('.win-row').forEach(el => el.classList.remove('selected'));
  const el = document.querySelector(`[data-win="${windowSize}"]`);
  if (el) el.classList.add('selected');

  const windows = getActiveWindows();
  const w = windows.find(x => x.window === windowSize);
  if (!w) return;

  // Switch to detail tab
  document.querySelectorAll('.rtab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.rtab-panel').forEach(p => p.classList.remove('active'));
  document.querySelector('[data-tab="detail"]').classList.add('active');
  document.getElementById('tab-detail').classList.add('active');

  document.getElementById('detail-title').textContent =
    `Window ${windowSize} — ${w.pass ? '✓ PASS' : '✗ ' + (w.reject||'').split(' ')[0].toUpperCase()}`;

  const rows = [
    { key: 'Range %',  val: (w.range_pct*100).toFixed(4)+'%',   limit: w.range_limit ? (w.range_limit*100).toFixed(4)+'% max' : 'no session limit', pass: !w.range_limit || w.range_pct <= w.range_limit, ratio: w.range_limit ? w.range_pct/w.range_limit : 0.5 },
    { key: 'Slope',    val: w.slope,                             limit: w.slope_limit,   pass: w.slope < w.slope_limit, ratio: w.slope / w.slope_limit },
    { key: 'Chop',     val: w.chop,                              limit: '≥ 0.44 (found) / 0.36 (pot)', pass: w.chop >= 0.36, ratio: w.chop / 0.44 },
    { key: 'ADX',      val: w.adx != null ? w.adx : 'N/A',      limit: '< ' + w.adx_limit, pass: w.adx == null || w.adx < w.adx_limit, ratio: w.adx != null ? w.adx / w.adx_limit : 0 },
    { key: 'Top',      val: w.top,                               limit: '',               pass: null, ratio: 0 },
    { key: 'Bottom',   val: w.bottom,                            limit: '',               pass: null, ratio: 0 },
    { key: 'Active',   val: w.is_active ? 'YES' : 'no',         limit: '',               pass: null, ratio: 0 },
  ];

  document.getElementById('detail-body').innerHTML = rows.map(r => {
    const cls      = r.pass === null ? 'neutral' : r.pass ? 'pass' : 'fail';
    const barW     = Math.min(100, Math.round((r.ratio || 0) * 100));
    const barColor = r.pass === null ? '#444' : r.pass ? 'var(--pass)' : 'var(--red)';
    return `<div class="detail-row">
      <span class="detail-key">${r.key}</span>
      <div style="display:flex;align-items:center;flex:1;justify-content:flex-end;gap:6px">
        ${r.ratio > 0 ? `<div class="bar-track"><div class="bar-fill" style="width:${barW}%;background:${barColor}"></div></div>` : ''}
        <span class="detail-val ${cls}">${r.val}</span>
      </div>
    </div>
    ${r.limit ? `<div style="font-size:9px;color:var(--muted);padding:1px 0 5px 0;border-bottom:1px solid rgba(255,255,255,0.04)">limit: ${r.limit}</div>` : ''}`;
  }).join('');

  drawWindowOverlay(w);
}

// ── Overlay drawing ───────────────────────────────────────────────────────
function drawWindowOverlay(w) {
  overlaySeriesList.forEach(s => { try { chart.removeSeries(s); } catch(e){} });
  overlaySeriesList = [];
  if (!w || !w.start_ts) return;

  const color  = w.pass ? 'rgba(90,240,196,0.7)'  : 'rgba(240,90,126,0.55)';
  const fill   = w.pass ? 'rgba(90,240,196,0.06)' : 'rgba(240,90,126,0.04)';
  const opts   = { color, lineWidth: 1, lineStyle: 0,
    priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false };

  const top   = chart.addLineSeries(opts);
  const bot   = chart.addLineSeries(opts);
  const left  = chart.addLineSeries(opts);
  const right = chart.addLineSeries(opts);
  const fillS = chart.addBaselineSeries({
    baseValue: { type: 'price', price: w.bottom },
    topFillColor1: fill, topFillColor2: fill, topLineColor: 'rgba(0,0,0,0)',
    bottomFillColor1: 'rgba(0,0,0,0)', bottomFillColor2: 'rgba(0,0,0,0)',
    bottomLineColor: 'rgba(0,0,0,0)',
    lineWidth: 0, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false,
  });

  const s = shiftTime(w.start_ts);
  const e = shiftTime(w.end_ts);
  top.setData(  [{ time: s, value: w.top    }, { time: e, value: w.top    }]);
  bot.setData(  [{ time: s, value: w.bottom }, { time: e, value: w.bottom }]);
  left.setData( [{ time: s, value: w.top    }, { time: s, value: w.bottom }]);
  right.setData([{ time: e, value: w.top    }, { time: e, value: w.bottom }]);
  fillS.setData([{ time: s, value: w.top    }, { time: e, value: w.top    }]);

  overlaySeriesList = [top, bot, left, right, fillS];
}

// ── Chart render ──────────────────────────────────────────────────────────
function renderChart(candles) {
  candleSeries.setData(candles.map(c => ({ ...c, time: shiftTime(c.time) })));
  chart.timeScale().fitContent();
}

// ── Replay controls ───────────────────────────────────────────────────────
function enterReplayMode() {
  replayMode = true;
  selectedWindow = null;
  const total = liveData.candles.length;
  replayIdx = Math.max(22, Math.floor(total * 0.6));

  document.getElementById('btn-live').classList.remove('active');
  document.getElementById('btn-replay').classList.add('active');
  document.getElementById('btn-prev').disabled   = false;
  document.getElementById('btn-play').disabled   = false;
  document.getElementById('btn-next').disabled   = false;
  document.getElementById('replay-speed').disabled = false;
  document.getElementById('scrubber').disabled   = false;
  document.getElementById('btn-analyze').disabled = false;
  document.getElementById('scrubber').max        = total - 1;
  document.getElementById('replay-banner').style.display = '';

  seekTo(replayIdx);
}

function exitReplayMode() {
  stopPlay();
  replayMode  = false;
  replayData  = null;
  selectedWindow = null;

  document.getElementById('btn-live').classList.add('active');
  document.getElementById('btn-replay').classList.remove('active');
  document.getElementById('btn-prev').disabled   = true;
  document.getElementById('btn-play').disabled   = true;
  document.getElementById('btn-next').disabled   = true;
  document.getElementById('replay-speed').disabled = true;
  document.getElementById('scrubber').disabled    = true;
  document.getElementById('btn-analyze').disabled = true;
  document.getElementById('replay-banner').style.display = 'none';
  document.getElementById('tag-replay').style.display    = 'none';

  drawWindowOverlay(null);
  renderChart(liveData.candles);
  renderSummary(liveData);
  renderWindowList(liveData.windows);
  updateTopbar(liveData, false);
  const sc = document.getElementById('scrubber');
  sc.value = liveData.candles.length - 1;
  document.getElementById('scrub-label').textContent = `candle — / —`;
  document.getElementById('scrub-ts').textContent    = '—';
}

function startPlay() {
  const speed = parseInt(document.getElementById('replay-speed').value);
  document.getElementById('btn-play').textContent = '⏸';
  replayTimer = setInterval(async () => {
    if (replayIdx >= liveData.candles.length) { stopPlay(); return; }
    replayIdx++;
    await seekTo(replayIdx);
  }, speed);
}

function stopPlay() {
  if (replayTimer) { clearInterval(replayTimer); replayTimer = null; }
  document.getElementById('btn-play').textContent = '▶';
}

document.getElementById('btn-live').addEventListener('click', exitReplayMode);
document.getElementById('btn-replay').addEventListener('click', () => {
  if (!replayMode) enterReplayMode();
});
document.getElementById('btn-play').addEventListener('click', () => {
  replayTimer ? stopPlay() : startPlay();
});
document.getElementById('btn-prev').addEventListener('click', () => {
  stopPlay(); seekTo(replayIdx - 1);
});
document.getElementById('btn-next').addEventListener('click', () => {
  stopPlay(); seekTo(replayIdx + 1);
});

// Scrubber — moves chart only, no auto-fetch
const scrubberEl = document.getElementById('scrubber');
scrubberEl.addEventListener('input', () => {
  if (!replayMode) return;
  stopPlay();
  const idx = parseInt(scrubberEl.value) + 1;
  const slice = liveData.candles.slice(0, idx);
  candleSeries.setData(slice.map(c => ({ ...c, time: shiftTime(c.time) })));
  const lastCandle = slice[slice.length - 1];
  const dt = lastCandle ? new Date((lastCandle.time + TZ_OFFSET) * 1000) : null;
  document.getElementById('scrub-ts').textContent =
    dt ? dt.toISOString().replace('T',' ').slice(0,16) + ' ' + TIMEZONE : '—';
  document.getElementById('scrub-label').textContent = `candle ${idx} / ${liveData.candles.length}`;
  replayIdx = idx;
  drawWindowOverlay(null);
});

// Run Detector button — runs analysis against current replay slice
document.getElementById('btn-analyze').addEventListener('click', () => {
  if (!replayMode) return;
  fetchReplay(replayIdx);
});

// ── Init ──────────────────────────────────────────────────────────────────
loadLiveData();
setInterval(() => { if (!replayMode) loadLiveData(); }, 30000);
</script>
</body>
</html>"""

# Global lock — yfinance has shared internal state and returns wrong data
# when multiple tickers download simultaneously across threads.
_YF_LOCK = threading.Lock()

PERIOD_MAP = {
    "1m":  "1d",
    "2m":  "1d",
    "5m":  "5d",
    "15m": "5d",
    "30m": "5d",
    "1h":  "30d",
}

# How often the background detector loop runs (seconds)
DETECTION_INTERVAL = 30


class PairServer:

    def __init__(self, pair_id: str, config: dict):
        self.pair_id = pair_id
        self.ticker = config["ticker"]
        self.port = config["port"]
        self.label = config["label"]
        self.interval = config.get("interval", "1m")
        self.period = config.get("period", "1d")
        self.detector_names = config.get("detectors", [])
        self.detector_params = config.get("detector_params", {})
        self.default_interval = config.get("default_interval", self.interval)

        # Alert dedup — persisted to disk so restarts don't re-fire old alerts
        self._alerted_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f".alerted_{pair_id}.json"
        )
        self.last_alerted: dict[str, int] = self._load_alerted()
        self.last_active_zone: dict[str, dict] = {}

        # Per-request DataFrame cache (cleared each cycle)
        self._df_cache: dict[str, pd.DataFrame] = {}
        self._cache_lock = threading.Lock()

        self._detection_lock = threading.Lock()
        self._stagger_seconds = 0  # set by app.py before run()

        root = os.path.dirname(os.path.abspath(__file__))
        self.app = Flask(
            __name__,
            template_folder=os.path.join(root, "templates"),
            static_folder=os.path.join(root, "static") if os.path.exists(os.path.join(root, "static")) else None,
        )
        self._register_routes()

    # ------------------------------------------------------------------ #
    # Routes
    # ------------------------------------------------------------------ #

    def _register_routes(self):
        app = self.app
        pair_id = self.pair_id

        def _index():
            tz = os.environ.get("TZ", "UTC")
            return render_template("index.html", pair_id=pair_id, label=self.label, port=self.port, timezone=tz, default_interval=self.default_interval)
        _index.__name__ = f"index_{pair_id}"
        app.route("/")(_index)

        def _get_data():
            return self._api_data()
        _get_data.__name__ = f"get_data_{pair_id}"
        app.route("/api/data")(_get_data)

        def _test_alert():
            return self._test_alert()
        _test_alert.__name__ = f"test_alert_{pair_id}"
        app.route("/test-alert")(_test_alert)

        def _debug():
            return self._debug()
        _debug.__name__ = f"debug_{pair_id}"
        app.route("/debug")(_debug)

        def _debug_data():
            return self._debug_data()
        _debug_data.__name__ = f"debug_data_{pair_id}"
        app.route("/debug/data")(_debug_data)

        def _debug_replay():
            return self._debug_replay()
        _debug_replay.__name__ = f"debug_replay_{pair_id}"
        app.route("/debug/replay")(_debug_replay)

    # ------------------------------------------------------------------ #
    # Data fetching
    # ------------------------------------------------------------------ #

    def _fetch_df(self, interval: str) -> pd.DataFrame:
        period = PERIOD_MAP.get(interval, self.period)
        with _YF_LOCK:  # serialize all yfinance downloads process-wide
            df = yf.download(self.ticker, period=period, interval=interval, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df.dropna()

    def _get_df(self, interval: str, cache: dict) -> pd.DataFrame:
        """Return cached DataFrame for this interval within a single cycle."""
        if interval not in cache:
            cache[interval] = self._fetch_df(interval)
        return cache[interval]

    # ------------------------------------------------------------------ #
    # Detection (shared by background loop and browser API)
    # ------------------------------------------------------------------ #

    def _run_detectors(self, cache: dict) -> dict:
        """Run all detectors using their configured timeframes. Returns results dict."""
        results = {}
        for name in self.detector_names:
            params = dict(self.detector_params.get(name, {}))
            detector_interval = params.pop("timeframe", "1m")
            df = self._get_df(detector_interval, cache)
            fn = REGISTRY.get(name)
            if fn is None:
                print(f"[WARN] Detector '{name}' not found in registry.")
                results[name] = None
            else:
                try:
                    # Pass yf_lock to detectors that do their own downloads (supply_demand)
                    if name == "supply_demand":
                        params["yf_lock"] = _YF_LOCK
                    results[name] = fn(df, **params)
                except Exception as e:
                    print(f"[ERROR] Detector '{name}' failed: {e}")
                    results[name] = None
        return results

    def _process_alerts(self, detector_results: dict):
        """Check results and fire Discord alerts on breakout."""
        for name, result in detector_results.items():

            # ── Accumulation ──────────────────────────────────────────
            if name == "accumulation":
                # Clean up alerted timestamps older than 4 hours
                cutoff = int(time.time()) - (4 * 3600)
                if name in self.last_alerted and isinstance(self.last_alerted[name], int):
                    if self.last_alerted[name] < cutoff:
                        del self.last_alerted[name]
                        self._save_alerted()

                prev = self.last_active_zone.get(name)
                zone = result if (result and isinstance(result, dict)) else None
                is_active_found = (
                    zone is not None
                    and zone.get("is_active")
                    and zone.get("status") == "found"
                )

                # ── State machine ──────────────────────────────────────
                # looking   → found     (zone active, all checks pass)
                # found     → confirmed (breakout detected — screenshot dispatched
                #                        WHILE zone still drawn on chart this cycle)
                # confirmed → looking   (next cycle after screenshot dispatched, reset)
                prev_status = (prev or {}).get("status")

                if is_active_found:
                    # Zone alive — keep tracking
                    zone_start = zone["start"]
                    already_alerted = self.last_alerted.get(name, 0)
                    if zone_start != already_alerted:
                        self.last_active_zone[name] = zone

                elif prev_status == "found" and (
                    zone is None
                    or not zone.get("is_active")
                    or zone.get("status") == "looking"
                ):
                    # Breakout detected this cycle. Mark "confirmed" so the browser
                    # still renders the box for one more cycle while the screenshot runs.
                    zone_start = prev["start"]
                    already_alerted = self.last_alerted.get(name, 0)
                    if zone_start != already_alerted:
                        confirmed_zone = dict(prev)
                        confirmed_zone["status"] = "confirmed"
                        self.last_active_zone[name] = confirmed_zone
                        self.last_alerted[name] = zone_start
                        self._save_alerted()
                        threading.Thread(
                            target=self._send_discord_alert,
                            args=(confirmed_zone,),
                            daemon=True,
                        ).start()

                elif prev_status == "confirmed":
                    # Screenshot was dispatched last cycle — now truly reset
                    self.last_active_zone[name] = None

            # ── Supply & Demand ───────────────────────────────────────
            elif name == "supply_demand":
                if not result or not isinstance(result, dict):
                    continue
                zones = result.get("zones", [])
                curr_active = {z["start"] for z in zones if z.get("is_active")}
                prev_starts = set(self.last_active_zone.get(name + "_starts", []))

                # Remove invalidated zones from last_alerted so they can re-fire if they return
                invalidated = prev_starts - curr_active
                changed = False
                for start_ts in invalidated:
                    key = f"{name}_{start_ts}"
                    if key in self.last_alerted:
                        del self.last_alerted[key]
                        changed = True
                        print(f"[{self.pair_id}] Removed invalidated zone {key} from alerted state")
                if changed:
                    self._save_alerted()

                # Alert only once per zone (keyed by start timestamp)
                for z in zones:
                    if not z.get("is_active"):
                        continue
                    start_ts = z["start"]
                    alert_key = f"{name}_{start_ts}"
                    if self.last_alerted.get(alert_key):
                        continue
                    self.last_alerted[alert_key] = 1
                    self._save_alerted()
                    alert_zone = {
                        "detector": z.get("type", "supply_demand"),
                        "start":    start_ts,
                        "end":      z["end"],
                    }
                    threading.Thread(
                        target=self._send_discord_alert,
                        args=(alert_zone,),
                        daemon=True,
                    ).start()

                self.last_active_zone[name + "_starts"] = list(curr_active)

    # ------------------------------------------------------------------ #
    # Background detection loop — runs regardless of browser
    # ------------------------------------------------------------------ #

    def _detection_loop(self):
        # Stagger startup so pairs don't all hit yfinance simultaneously
        if self._stagger_seconds:
            time.sleep(self._stagger_seconds)
        print(f"[{self.pair_id}] Background detector started (every {DETECTION_INTERVAL}s)")
        while True:
            try:
                # ── Weekend halt: Fri 23:00 – Mon 01:00 UTC ───────────────
                from detectors.accumulation import is_weekend_halt
                if is_weekend_halt():
                    print(f"[{self.pair_id}] Weekend halt — skipping detection")
                    time.sleep(DETECTION_INTERVAL)
                    continue

                with self._detection_lock:
                    cache = {}
                    results = self._run_detectors(cache)
                    self._process_alerts(results)
                print(f"[{self.pair_id}] Detection cycle complete: {list(results.keys())}")
            except Exception as e:
                print(f"[{self.pair_id}] Detection loop error: {e}")
            time.sleep(DETECTION_INTERVAL)

    # ------------------------------------------------------------------ #
    # Flask API — serves chart data to browser when open
    # ------------------------------------------------------------------ #

    def _api_data(self):
        try:
            chart_interval = request.args.get("interval", self.interval)
            cache = {}

            # Run detectors fresh for the browser response
            detector_results = self._run_detectors(cache)

            # If a "confirmed" zone is held in state (breakout just detected,
            # screenshot in-flight), override the fresh result so the browser
            # still renders the box for one cycle while Playwright screenshots it.
            for det_name in self.detector_names:
                if det_name == "accumulation":
                    held = self.last_active_zone.get(det_name)
                    if held and held.get("status") == "confirmed":
                        detector_results[det_name] = held

            # Fetch chart candles at the requested interval
            df_chart = self._get_df(chart_interval, cache)
            candles = [
                {
                    "time": int(idx.timestamp()),
                    "open": float(r["Open"]),
                    "high": float(r["High"]),
                    "low": float(r["Low"]),
                    "close": float(r["Close"]),
                }
                for idx, r in df_chart.iterrows()
            ]

            return jsonify({
                "pair": self.pair_id,
                "label": self.label,
                "candles": candles,
                "detectors": detector_results,
            })

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    def _debug(self):
        """Rich debug page: chart + side panel showing per-window rejection reasons + replay mode."""
        tz = os.environ.get("TZ", "Europe/Brussels")
        return (DEBUG_HTML
            .replace("__PAIR_ID__",  self.pair_id)
            .replace("__LABEL__",    self.label)
            .replace("__TIMEZONE__", tz)
        )

    def _debug_data(self):
        """Return detailed rejection analysis JSON for the debug page."""
        try:
            import numpy as np
            from detectors.accumulation import (
                get_current_session, _slope_pct, _choppiness, _adx
            )

            cache = {}
            df = self._get_df("1m", cache)

            params        = dict(self.detector_params.get("accumulation", {}))
            params.pop("timeframe", None)
            lookback      = params.get("lookback", 40)
            min_candles   = params.get("min_candles", 20)
            adx_threshold = params.get("adx_threshold", 25)
            threshold_pct = params.get("threshold_pct", 0.003)

            session = get_current_session()
            session_range_key = f"{session}_range_pct" if session else None
            effective_range_pct = params.get(session_range_key) or params.get("max_range_pct")

            if isinstance(df.columns, __import__('pandas').MultiIndex):
                df = df.copy()
                df.columns = df.columns.get_level_values(0)
            df = df.loc[:, ~df.columns.duplicated()].copy()
            for col in ['Open','High','Low','Close']:
                df[col] = __import__('pandas').to_numeric(df[col].squeeze(), errors='coerce')
            df = df.dropna(subset=['Open','High','Low','Close'])

            last_closed_idx = len(df) - 2
            scan_start      = max(0, len(df) - lookback)

            last_closed_open  = float(df['Open'].iloc[-2])
            last_closed_close = float(df['Close'].iloc[-2])
            last_body_high    = max(last_closed_open, last_closed_close)
            last_body_low     = min(last_closed_open, last_closed_close)

            # Export candle data for the chart
            candles = [
                {
                    "time":  int(idx.timestamp()),
                    "open":  float(r["Open"]),
                    "high":  float(r["High"]),
                    "low":   float(r["Low"]),
                    "close": float(r["Close"]),
                }
                for idx, r in df.iterrows()
            ]

            windows = []
            for window_size in range(min_candles, lookback + 1):
                slope_limit = (threshold_pct * 0.10) / window_size
                i = last_closed_idx - window_size + 1
                if i < 0 or i < scan_start:
                    windows.append({"window": window_size, "skip": "out of scan range"})
                    continue

                window = df.iloc[i: i + window_size]
                closes = window['Close'].values.flatten().astype(float)
                opens  = window['Open'].values.flatten().astype(float)
                highs  = window['High'].values.flatten().astype(float)
                lows   = window['Low'].values.flatten().astype(float)

                avg_p = closes.mean()
                body_highs = np.maximum(opens, closes)
                body_lows  = np.minimum(opens, closes)
                h_max = float(body_highs.max())
                l_min = float(body_lows.min())
                range_pct = round((h_max - l_min) / avg_p, 6)
                slope     = round(_slope_pct(closes, avg_p), 8)
                chop      = round(_choppiness(closes), 4)
                adx_val   = _adx(highs, lows, closes)
                is_active = (last_body_low >= l_min) and (last_body_high <= h_max)

                reject = None
                if effective_range_pct and range_pct > effective_range_pct:
                    reject = f"range {range_pct} > limit {effective_range_pct}"
                elif slope >= slope_limit:
                    reject = f"slope {slope} >= limit {round(slope_limit,8)}"
                elif adx_val is not None and adx_val > adx_threshold:
                    reject = f"adx {round(adx_val,2)} > {adx_threshold}"
                elif chop < 0.36:
                    reject = f"chop {chop} < 0.36"

                windows.append({
                    "window":      window_size,
                    "start_ts":    int(df.index[i].timestamp()),
                    "end_ts":      int(df.index[i + window_size - 1].timestamp()),
                    "top":         round(h_max, 5),
                    "bottom":      round(l_min, 5),
                    "range_pct":   range_pct,
                    "range_limit": effective_range_pct,
                    "slope":       slope,
                    "slope_limit": round(slope_limit, 8),
                    "chop":        chop,
                    "adx":         round(adx_val, 2) if adx_val is not None else None,
                    "adx_limit":   adx_threshold,
                    "is_active":   is_active,
                    "reject":      reject,
                    "pass":        reject is None,
                })

            passed   = [w for w in windows if w.get("pass")]
            rejected = [w for w in windows if not w.get("pass") and "skip" not in w]
            reasons  = {}
            for r in rejected:
                key = r["reject"].split(" ")[0] if r.get("reject") else "unknown"
                reasons[key] = reasons.get(key, 0) + 1

            return jsonify({
                "pair":              self.pair_id,
                "session":           session,
                "effective_range":   effective_range_pct,
                "adx_threshold":     adx_threshold,
                "last_close":        round(float(df['Close'].iloc[-2]), 5),
                "windows_checked":   len([w for w in windows if "skip" not in w]),
                "passed":            len(passed),
                "rejection_summary": reasons,
                "windows":           windows,
                "candles":           candles,
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    def _debug_replay(self):
        """
        Run the accumulation detector against only the first `idx` candles.
        Query param: idx=N (1-based candle index to replay up to)
        """
        # Read query param immediately while Flask request context is guaranteed active
        try:
            raw_idx = int(request.args.get("idx", -1))
        except Exception:
            raw_idx = -1

        try:
            import numpy as np
            import pandas as pd
            from datetime import timezone
            from detectors.accumulation import _slope_pct, _choppiness, _adx

            acquired = _YF_LOCK.acquire(timeout=10)
            try:
                full_df = yf.download(self.ticker, period=self.period, interval="1m", progress=False)
            finally:
                if acquired:
                    _YF_LOCK.release()

            if isinstance(full_df.columns, pd.MultiIndex):
                full_df.columns = full_df.columns.get_level_values(0)
            full_df = full_df.dropna()

            params        = dict(self.detector_params.get("accumulation", {}))
            params.pop("timeframe", None)
            lookback      = params.get("lookback", 40)
            min_candles   = params.get("min_candles", 15)
            adx_threshold = params.get("adx_threshold", 25)
            threshold_pct = params.get("threshold_pct", 0.003)

            total = len(full_df)
            idx = raw_idx if raw_idx > 0 else total
            idx = max(min_candles + 3, min(idx, total))

            # Slice the dataframe — this is what the detector would have seen at candle N
            df = full_df.iloc[:idx].copy()

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df = df.loc[:, ~df.columns.duplicated()].copy()
            for col in ['Open', 'High', 'Low', 'Close']:
                df[col] = pd.to_numeric(df[col].squeeze(), errors='coerce')
            df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])

            # Determine session from the last candle's timestamp (not wall clock)
            last_ts = df.index[-1]
            if last_ts.tzinfo is None:
                last_ts = last_ts.tz_localize('UTC')
            else:
                last_ts = last_ts.tz_convert('UTC')
            hour = last_ts.hour

            session = None
            if 1 <= hour < 7:    session = "asian"
            elif 8 <= hour < 12: session = "london"
            elif 13 <= hour < 19: session = "new_york"

            # Resolve effective range — fall back through session → generic → None
            session_range_key   = f"{session}_range_pct" if session else None
            effective_range_pct = (
                params.get(session_range_key)
                or params.get("max_range_pct")
            )

            last_closed_idx   = len(df) - 2
            scan_start        = max(0, len(df) - lookback)
            last_closed_open  = float(df['Open'].iloc[-2])
            last_closed_close = float(df['Close'].iloc[-2])
            last_body_high    = max(last_closed_open, last_closed_close)
            last_body_low     = min(last_closed_open, last_closed_close)

            windows = []
            for window_size in range(min_candles, lookback + 1):
                slope_limit = (threshold_pct * 0.10) / window_size
                i = last_closed_idx - window_size + 1
                if i < 0 or i < scan_start:
                    continue

                window = df.iloc[i: i + window_size]
                closes = window['Close'].values.flatten().astype(float)
                opens  = window['Open'].values.flatten().astype(float)
                highs  = window['High'].values.flatten().astype(float)
                lows   = window['Low'].values.flatten().astype(float)

                avg_p = closes.mean()
                if avg_p == 0:
                    continue
                body_highs = np.maximum(opens, closes)
                body_lows  = np.minimum(opens, closes)
                h_max = float(body_highs.max())
                l_min = float(body_lows.min())
                range_pct = round((h_max - l_min) / avg_p, 6)
                slope     = round(_slope_pct(closes, avg_p), 8)
                chop      = round(_choppiness(closes), 4)
                adx_val   = _adx(highs, lows, closes)
                is_active = (last_body_low >= l_min) and (last_body_high <= h_max)

                reject = None
                if effective_range_pct and range_pct > effective_range_pct:
                    reject = f"range {range_pct} > limit {effective_range_pct}"
                elif slope >= slope_limit:
                    reject = f"slope {slope} >= limit {round(slope_limit,8)}"
                elif adx_val is not None and adx_val > adx_threshold:
                    reject = f"adx {round(adx_val,2)} > {adx_threshold}"
                elif chop < 0.36:
                    reject = f"chop {chop} < 0.36"

                windows.append({
                    "window":      window_size,
                    "start_ts":    int(df.index[i].timestamp()),
                    "end_ts":      int(df.index[i + window_size - 1].timestamp()),
                    "top":         round(h_max, 5),
                    "bottom":      round(l_min, 5),
                    "range_pct":   range_pct,
                    "range_limit": effective_range_pct,
                    "slope":       slope,
                    "slope_limit": round(slope_limit, 8),
                    "chop":        chop,
                    "adx":         round(adx_val, 2) if adx_val is not None else None,
                    "adx_limit":   adx_threshold,
                    "is_active":   is_active,
                    "reject":      reject,
                    "pass":        reject is None,
                })

            passed   = [w for w in windows if w.get("pass")]
            rejected = [w for w in windows if not w.get("pass")]
            reasons  = {}
            for r in rejected:
                key = r["reject"].split(" ")[0] if r.get("reject") else "unknown"
                reasons[key] = reasons.get(key, 0) + 1

            # Best zone: tightest active passing window
            best_zone = None
            active_passing = [w for w in passed if w.get("is_active")]
            if active_passing:
                best_zone = min(active_passing, key=lambda w: w["range_pct"])

            return jsonify({
                "idx":               idx,
                "total":             total,
                "session":           session,
                "effective_range":   effective_range_pct,
                "adx_threshold":     adx_threshold,
                "last_close":        round(float(df['Close'].iloc[-2]), 5),
                "windows_checked":   len(windows),
                "passed":            len(passed),
                "rejection_summary": reasons,
                "windows":           windows,
                "best_zone":         best_zone,
            })
        except Exception as e:
            import traceback
            return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

    def _test_alert(self):
        test_zone = {
            "detector": "accumulation",
            "start": int(time.time()),
            "end": int(time.time()),
            "top": 0,
            "bottom": 0,
            "is_active": True,
        }
        threading.Thread(target=self._send_discord_alert, args=(test_zone,), daemon=True).start()
        return f"Test alert triggered for {self.pair_id}. Check terminal and Discord."

    # ------------------------------------------------------------------ #
    # Discord
    # ------------------------------------------------------------------ #

    def _send_discord_alert(self, zone: dict):
        if not DISCORD_WEBHOOK_URL:
            print(f"[{self.pair_id}] Discord webhook URL not set.")
            return
        if not DISCORD_AVAILABLE:
            print(f"[{self.pair_id}] discord-webhook package not installed.")
            return

        screenshot_path = f"alert_{self.pair_id}_{int(time.time())}.png"
        raw = zone.get("detector", "unknown")
        if raw in ("demand", "supply"):
            detector_name = f"{raw.capitalize()} Zone"
        else:
            detector_name = raw.replace("_", " ").title()
        print(f"[{self.pair_id}] Sending Discord alert for {detector_name}...")

        try:
            if PLAYWRIGHT_AVAILABLE:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    page = browser.new_page(viewport={"width": 1280, "height": 720})
                    page.goto(f"http://127.0.0.1:{self.port}")
                    page.wait_for_timeout(6000)
                    page.screenshot(path=screenshot_path)
                    browser.close()

            if zone.get("detector") in ("demand", "supply"):
                emoji = "📈" if zone.get("detector") == "demand" else "📉"
                content = f"{emoji} **{self.pair_id} — {detector_name} Found**"
            else:
                content = f"🚀 **{self.pair_id} — {detector_name} Confirmed**"
            webhook = DiscordWebhook(url=DISCORD_WEBHOOK_URL, content=content)

            if PLAYWRIGHT_AVAILABLE and os.path.exists(screenshot_path):
                with open(screenshot_path, "rb") as f:
                    webhook.add_file(file=f.read(), filename="chart.png")

            webhook.execute()
            print(f"[{self.pair_id}] Discord alert sent.")

        except Exception as e:
            print(f"[{self.pair_id}] Discord error: {e}")
        finally:
            if os.path.exists(screenshot_path):
                os.remove(screenshot_path)

    # ------------------------------------------------------------------ #
    # Start
    # ------------------------------------------------------------------ #

    def _load_alerted(self) -> dict:
        try:
            if os.path.exists(self._alerted_file):
                with open(self._alerted_file, 'r') as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _save_alerted(self):
        try:
            with open(self._alerted_file, 'w') as f:
                json.dump(self.last_alerted, f)
        except Exception as e:
            print(f"[{self.pair_id}] Failed to save alerted state: {e}")

    def run(self):
        print(f"[{self.pair_id}] Starting on http://0.0.0.0:{self.port}")

        # Start background detection loop in a daemon thread
        t = threading.Thread(target=self._detection_loop, daemon=True, name=f"detector-{self.pair_id}")
        t.start()

        # Start Flask (blocks this thread) — threaded so slow /debug/replay
        # requests don't block concurrent requests for the HTML page or chart data
        self.app.run(host="0.0.0.0", port=self.port, use_reloader=False, threaded=True)
