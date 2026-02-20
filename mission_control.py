"""
mission_control.py — Mission Control dashboard server.

Runs on port 5010 (separate from all pair servers).
Serves the dashboard HTML and proxies /proxy/<pair_id>/api/data
to each pair's internal port so there are zero CORS issues.

Usage:
    python mission_control.py

Then open: http://localhost:5010
"""

import requests
from flask import Flask, jsonify, render_template_string
from config import PAIRS

app = Flask(__name__)

MISSION_PORT = 5010

# ── Proxy ──────────────────────────────────────────────────────────────────

@app.route('/proxy/<pair_id>/api/data')
def proxy(pair_id):
    cfg = PAIRS.get(pair_id.upper())
    if not cfg:
        return jsonify({"error": "unknown pair"}), 404
    try:
        interval = cfg.get("default_interval", cfg.get("interval", "1m"))
        url = f"http://127.0.0.1:{cfg['port']}/api/data?interval={interval}"
        r = requests.get(url, timeout=5)
        return (r.content, r.status_code, {"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ── Dashboard ──────────────────────────────────────────────────────────────

PAIRS_JS = [
    {
        "id":    pair_id,
        "label": cfg["label"],
        "port":  cfg["port"],
        "type":  "supply_demand" if "supply_demand" in cfg["detectors"] else "accumulation",
    }
    for pair_id, cfg in PAIRS.items()
]

DASHBOARD = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mission Control</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:        #0a0a0c;
    --surface:   #111116;
    --border:    #1e1e28;
    --border2:   #2a2a38;
    --text:      #c8c8d8;
    --muted:     #4a4a60;
    --accent:    #5af0c4;
    --accent2:   #f05a7e;
    --blue:      #5a9ef0;
    --found:     #e8e8f0;
    --potential: #888898;
  }

  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'Space Mono', monospace;
    min-height: 100vh;
    overflow-x: hidden;
  }

  body::before {
    content: '';
    position: fixed;
    inset: 0;
    background: repeating-linear-gradient(
      0deg, transparent, transparent 2px,
      rgba(0,0,0,0.03) 2px, rgba(0,0,0,0.03) 4px
    );
    pointer-events: none;
    z-index: 9999;
  }

  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 20px 32px;
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 0;
    background: var(--bg);
    z-index: 100;
  }

  .logo { display: flex; align-items: baseline; gap: 12px; }
  .logo-title {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 1.1rem;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: var(--accent);
  }
  .logo-sub { font-size: 0.65rem; color: var(--muted); letter-spacing: 0.2em; text-transform: uppercase; }

  .header-right { display: flex; align-items: center; gap: 20px; }
  #clock { font-size: 0.75rem; color: var(--muted); letter-spacing: 0.1em; }

  #session-global {
    font-size: 0.7rem;
    letter-spacing: 0.1em;
    padding: 4px 10px;
    border-radius: 3px;
    border: 1px solid var(--border2);
    color: var(--muted);
    transition: all 0.3s;
  }
  #session-global.asian    { border-color: #c8a84b; color: #c8a84b; }
  #session-global.london   { border-color: var(--blue); color: var(--blue); }
  #session-global.new_york { border-color: var(--accent); color: var(--accent); }

  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 1px;
    background: var(--border);
  }

  .card {
    background: var(--surface);
    padding: 20px 22px;
    position: relative;
    cursor: pointer;
    transition: background 0.2s;
    text-decoration: none;
    display: block;
    color: var(--text);
  }
  .card:hover { background: #13131a; }
  .card::after {
    content: '↗';
    position: absolute;
    top: 16px; right: 16px;
    font-size: 0.75rem;
    color: var(--muted);
    opacity: 0;
    transition: opacity 0.2s;
  }
  .card:hover::after { opacity: 1; }

  .card-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    margin-bottom: 14px;
  }

  .pair-name {
    font-family: 'Syne', sans-serif;
    font-weight: 800;
    font-size: 1.3rem;
    color: #fff;
    letter-spacing: 0.05em;
    line-height: 1;
  }
  .pair-label { font-size: 0.6rem; color: var(--muted); letter-spacing: 0.15em; text-transform: uppercase; margin-top: 4px; }

  .detector-badge {
    font-size: 0.6rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 3px 8px;
    border-radius: 2px;
    border: 1px solid var(--border2);
    color: var(--muted);
    background: var(--bg);
  }
  .detector-badge.accum { border-color: #3a3a50; color: #7070a0; }
  .detector-badge.sd    { border-color: #3a5050; color: #70a0a0; }

  .price-row { display: flex; align-items: baseline; gap: 8px; margin: 8px 0 12px; }
  .price { font-size: 1.6rem; font-weight: 700; color: #fff; letter-spacing: -0.02em; line-height: 1; }
  .price-change { font-size: 0.72rem; padding: 2px 6px; border-radius: 2px; }
  .price-change.up   { color: var(--accent);  background: rgba(90,240,196,0.08); }
  .price-change.down { color: var(--accent2); background: rgba(240,90,126,0.08); }

  .status-row { display: flex; align-items: center; gap: 8px; margin-bottom: 10px; }
  .status-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--muted); flex-shrink: 0; }
  .status-dot.found     { background: var(--found); box-shadow: 0 0 8px rgba(232,232,240,0.6); animation: pulse 2s ease-in-out infinite; }
  .status-dot.potential { background: var(--potential); }
  .status-dot.looking   { background: #2a2a3a; }
  .status-dot.offline   { background: #1a1a2a; }
  .status-dot.bullish   { background: var(--accent); box-shadow: 0 0 6px rgba(90,240,196,0.4); }
  .status-dot.bearish   { background: var(--accent2); box-shadow: 0 0 6px rgba(240,90,126,0.4); }
  .status-dot.misaligned{ background: var(--muted); }

  .status-text { font-size: 0.72rem; color: var(--text); letter-spacing: 0.05em; }
  .status-text.dim { color: var(--muted); }

  .accum-box {
    margin-top: 10px;
    padding: 8px 10px;
    border: 1px solid var(--border2);
    border-left: 2px solid var(--muted);
    font-size: 0.65rem;
    color: var(--muted);
    display: none;
    line-height: 1.6;
  }
  .accum-box.found     { border-left-color: var(--found); color: var(--text); display: block; }
  .accum-box.potential { border-left-color: var(--potential); color: var(--potential); display: block; }
  .accum-range { font-size: 0.7rem; color: #fff; font-weight: 700; }

  .zones-row { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 8px; }
  .zone-pill {
    font-size: 0.6rem;
    letter-spacing: 0.08em;
    padding: 3px 8px;
    border-radius: 2px;
    border: 1px solid;
    text-transform: uppercase;
  }
  .zone-pill.demand { border-color: rgba(90,158,240,0.4); color: #5a9ef0; background: rgba(90,158,240,0.06); }
  .zone-pill.supply { border-color: rgba(240,90,126,0.4); color: #f05a7e; background: rgba(240,90,126,0.06); }

  .bias-row { display: flex; align-items: center; gap: 6px; margin-top: 6px; }
  .bias-pill {
    font-size: 0.6rem;
    letter-spacing: 0.08em;
    padding: 3px 8px;
    border-radius: 2px;
    border: 1px solid var(--border2);
    color: var(--muted);
  }
  .bias-pill.bullish    { border-color: rgba(90,240,196,0.4); color: var(--accent); background: rgba(90,240,196,0.06); }
  .bias-pill.bearish    { border-color: rgba(240,90,126,0.4); color: var(--accent2); background: rgba(240,90,126,0.06); }
  .bias-pill.misaligned { border-color: var(--border2); color: var(--muted); }

  .card-divider { height: 1px; background: var(--border); margin: 12px 0; }
  .card-meta { display: flex; justify-content: space-between; font-size: 0.6rem; color: var(--muted); letter-spacing: 0.08em; }

  .card.error { opacity: 0.4; }
  .card.error .pair-name { color: var(--muted); }

  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.4; }
  }

  footer {
    border-top: 1px solid var(--border);
    padding: 12px 32px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 0.6rem;
    color: var(--muted);
    letter-spacing: 0.1em;
  }
  .refresh-indicator { display: flex; align-items: center; gap: 6px; }
  .refresh-dot { width: 5px; height: 5px; border-radius: 50%; background: var(--muted); }
  .refresh-dot.active { background: var(--accent); animation: pulse 0.5s ease-in-out; }
</style>
</head>
<body>

<header>
  <div class="logo">
    <span class="logo-title">Mission Control</span>
    <span class="logo-sub">Trading Agent</span>
  </div>
  <div class="header-right">
    <span id="session-global">--</span>
    <span id="clock">--:--:-- UTC</span>
  </div>
</header>

<div class="grid" id="grid"></div>

<footer>
  <span id="last-update">Waiting for data...</span>
  <div class="refresh-indicator">
    <div class="refresh-dot" id="refresh-dot"></div>
    <span>LIVE · 5s</span>
  </div>
</footer>

<script>
const PAIRS = """ + str(PAIRS_JS).replace("'", '"').replace("True", "true").replace("False", "false") + r""";

const SESSION_WINDOWS = [
  { name: 'asian',    label: 'Asian Session',    start: 1,  end: 7  },
  { name: 'london',   label: 'London Session',   start: 8,  end: 12 },
  { name: 'new_york', label: 'New York Session', start: 13, end: 19 },
];

function getCurrentSession() {
  const h = new Date().getUTCHours();
  return SESSION_WINDOWS.find(s => h >= s.start && h < s.end) || null;
}

function formatPrice(p, id) {
  if (p == null) return '---';
  if (id === 'EURUSD' || id === 'EURGBP') return p.toFixed(5);
  if (id === 'XAUUSD') return p.toFixed(2);
  return p.toFixed(0);
}

function formatUTC(ts) {
  return new Date(ts * 1000).toISOString().slice(11, 16) + ' UTC';
}

function buildGrid() {
  const grid = document.getElementById('grid');
  PAIRS.forEach(pair => {
    const card = document.createElement('a');
    card.className = 'card';
    card.id = `card-${pair.id}`;
    card.href = `http://localhost:${pair.port}`;
    card.target = '_blank';
    card.innerHTML = `
      <div class="card-header">
        <div>
          <div class="pair-name">${pair.id}</div>
          <div class="pair-label">${pair.label}</div>
        </div>
        <span class="detector-badge ${pair.type === 'accumulation' ? 'accum' : 'sd'}">
          ${pair.type === 'accumulation' ? 'Accum' : 'S/D'}
        </span>
      </div>
      <div class="price-row">
        <span class="price" id="price-${pair.id}">---</span>
        <span class="price-change" id="change-${pair.id}"></span>
      </div>
      <div class="status-row">
        <div class="status-dot looking" id="dot-${pair.id}"></div>
        <span class="status-text dim" id="status-${pair.id}">Connecting...</span>
      </div>
      <div id="extra-${pair.id}"></div>
      <div class="card-divider"></div>
      <div class="card-meta">
        <span>PORT ${pair.port}</span>
        <span id="meta-${pair.id}">--</span>
      </div>`;
    grid.appendChild(card);
  });
}

async function fetchPair(pair) {
  try {
    const res = await fetch(`/proxy/${pair.id}/api/data`);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();

    const candles = data.candles || [];
    const last = candles[candles.length - 1];
    const prev = candles[candles.length - 2];
    const price = last?.close;

    document.getElementById(`price-${pair.id}`).textContent = formatPrice(price, pair.id);

    if (prev?.close && price) {
      const chg = ((price - prev.close) / prev.close) * 100;
      const el = document.getElementById(`change-${pair.id}`);
      el.textContent = (chg >= 0 ? '+' : '') + chg.toFixed(3) + '%';
      el.className = 'price-change ' + (chg >= 0 ? 'up' : 'down');
    }

    const det = data.detectors || {};
    const dotEl    = document.getElementById(`dot-${pair.id}`);
    const statusEl = document.getElementById(`status-${pair.id}`);
    const extraEl  = document.getElementById(`extra-${pair.id}`);
    const metaEl   = document.getElementById(`meta-${pair.id}`);
    const card     = document.getElementById(`card-${pair.id}`);
    card.classList.remove('error');

    // ── Accumulation ─────────────────────────────────────────────────────
    if (pair.type === 'accumulation') {
      const z = det.accumulation;
      if (!z || z.status === 'looking' || !z.status) {
        dotEl.className   = 'status-dot looking';
        statusEl.textContent = 'Looking for accumulation';
        statusEl.className = 'status-text dim';
        extraEl.innerHTML  = '';
        metaEl.textContent = z?.session ? z.session.replace('_',' ').toUpperCase() : '--';
      } else if (z.status === 'found' && z.is_active) {
        dotEl.className   = 'status-dot found';
        statusEl.textContent = 'Accumulation found';
        statusEl.className = 'status-text';
        const adxStr = z.adx != null ? ` &nbsp;·&nbsp; ADX ${z.adx}` : '';
        extraEl.innerHTML = `
          <div class="accum-box found">
            <span class="accum-range">${formatPrice(z.bottom, pair.id)} – ${formatPrice(z.top, pair.id)}</span>
            ${adxStr}
            <br>Since ${formatUTC(z.start)}
          </div>`;
        metaEl.textContent = z.session ? z.session.replace('_',' ').toUpperCase() : '--';
      } else if (z.status === 'potential' && z.is_active) {
        dotEl.className   = 'status-dot potential';
        statusEl.textContent = 'Potential forming';
        statusEl.className = 'status-text dim';
        extraEl.innerHTML = `
          <div class="accum-box potential">
            ${formatPrice(z.bottom, pair.id)} – ${formatPrice(z.top, pair.id)}
          </div>`;
        metaEl.textContent = z.session ? z.session.replace('_',' ').toUpperCase() : '--';
      } else {
        dotEl.className   = 'status-dot looking';
        statusEl.textContent = 'Looking for accumulation';
        statusEl.className = 'status-text dim';
        extraEl.innerHTML  = '';
        metaEl.textContent = '--';
      }
    }

    // ── Supply & Demand ───────────────────────────────────────────────────
    if (pair.type === 'supply_demand') {
      const result = det.supply_demand;
      const bias   = result?.bias || {};
      const zones  = (result?.zones || []).filter(z => z.is_active);

      if (bias.bias === 'bullish') {
        dotEl.className = 'status-dot bullish';
      } else if (bias.bias === 'bearish') {
        dotEl.className = 'status-dot bearish';
      } else {
        dotEl.className = 'status-dot misaligned';
      }

      if (!bias.bias || bias.bias === 'misaligned') {
        statusEl.textContent = 'Bias misaligned — not looking';
        statusEl.className   = 'status-text dim';
      } else if (zones.length > 0) {
        statusEl.textContent = `${zones.length} zone${zones.length > 1 ? 's' : ''} active`;
        statusEl.className   = 'status-text';
      } else {
        statusEl.textContent = bias.bias === 'bullish' ? 'Seeking demand zones' : 'Seeking supply zones';
        statusEl.className   = 'status-text dim';
      }

      const biasLabel = bias.bias === 'bullish' ? '↑ Bullish D+W'
                      : bias.bias === 'bearish' ? '↓ Bearish D+W'
                      : '⚡ Misaligned';
      const biasClass = bias.bias || 'misaligned';

      const zonePills = zones.map(z =>
        `<span class="zone-pill ${z.type}">${z.type.toUpperCase()} ${formatPrice(z.bottom, pair.id)}–${formatPrice(z.top, pair.id)}</span>`
      ).join('');

      extraEl.innerHTML = `
        <div class="bias-row"><span class="bias-pill ${biasClass}">${biasLabel}</span></div>
        ${zones.length ? `<div class="zones-row">${zonePills}</div>` : ''}`;

      metaEl.textContent = last ? formatUTC(last.time) : '--';
    }

  } catch (e) {
    const card = document.getElementById(`card-${pair.id}`);
    card.classList.add('error');
    document.getElementById(`status-${pair.id}`).textContent = 'Offline';
    document.getElementById(`dot-${pair.id}`).className = 'status-dot offline';
    document.getElementById(`meta-${pair.id}`).textContent = 'ERR';
    document.getElementById(`extra-${pair.id}`).innerHTML = '';
  }
}

function updateClock() {
  const now = new Date();
  const pad = n => String(n).padStart(2, '0');
  document.getElementById('clock').textContent =
    `${pad(now.getUTCHours())}:${pad(now.getUTCMinutes())}:${pad(now.getUTCSeconds())} UTC`;

  const sess  = getCurrentSession();
  const badge = document.getElementById('session-global');
  if (sess) {
    badge.textContent = sess.label;
    badge.className   = sess.name;
  } else {
    const utcH = now.getUTCHours();
    const next = SESSION_WINDOWS.find(s => s.start > utcH) || SESSION_WINDOWS[0];
    const target = new Date(now);
    target.setUTCHours(next.start, 0, 0, 0);
    if (target <= now) target.setUTCDate(target.getUTCDate() + 1);
    const diff = target - now;
    const dh = Math.floor(diff / 3600000);
    const dm = Math.floor((diff % 3600000) / 60000);
    const ds = Math.floor((diff % 60000) / 1000);
    badge.textContent = `${next.label.split(' ')[0]} in ${pad(dh)}:${pad(dm)}:${pad(ds)}`;
    badge.className   = '';
  }
}

async function pollAll() {
  const dot = document.getElementById('refresh-dot');
  dot.classList.add('active');
  await Promise.all(PAIRS.map(fetchPair));
  dot.classList.remove('active');
  const now = new Date();
  document.getElementById('last-update').textContent =
    'Last update: ' + now.toISOString().slice(11, 19) + ' UTC';
}

buildGrid();
setInterval(updateClock, 1000);
updateClock();
pollAll();
setInterval(pollAll, 5000);
</script>
</body>
</html>"""


@app.route('/')
def index():
    return render_template_string(DASHBOARD)


if __name__ == '__main__':
    print("=" * 50)
    print("Mission Control — http://0.0.0.0:5010")
    print("=" * 50)
    for pair_id, cfg in PAIRS.items():
        print(f"  {pair_id:10s} → proxied from port {cfg['port']}")
    print("=" * 50)
    app.run(host='0.0.0.0', port=MISSION_PORT, use_reloader=False)
