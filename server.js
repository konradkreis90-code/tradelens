// Peekline relay: live quotes + AI chart analysis
// One WebSocket to Finnhub (key stays here), many WebSockets to your app.
//
//   npm init -y && npm install ws
//   FINNHUB_KEY=your_key node server.js
//
// Client protocol (JSON):
//   -> {"type":"subscribe","symbols":["AAPL","NBIS"]}
//   -> {"type":"unsubscribe","symbols":["AAPL"]}
//   <- {"type":"quote","symbol":"NBIS","price":240.15,"ts":1727180000000,"volume":100}
//   <- {"type":"status","upstream":"connected"|"reconnecting"}

const http = require('http');
const WebSocket = require('ws');

const FINNHUB_KEY = process.env.FINNHUB_KEY;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY; // for /analyze (chart reading)
const ANTHROPIC_MODEL = process.env.ANTHROPIC_MODEL || 'claude-sonnet-5';
const SNAP_CLIENT_ID = process.env.SNAPTRADE_CLIENT_ID, SNAP_CONSUMER_KEY = process.env.SNAPTRADE_CONSUMER_KEY;
const DATABASE_URL = process.env.DATABASE_URL;
// Shared secret the app must send (header x-peekline-key). Set APP_SECRET in Railway; leave unset while developing.
const APP_SECRET = process.env.APP_SECRET || null;
const DAILY_IMAGE_LIMIT = Number(process.env.DAILY_IMAGE_LIMIT || 25); // fair-use cap on chart-image analyses per device per day
// Universe for "Top stocks of the day": the most-traded US names. Refreshed every 5 min within Finnhub's rate limit.
const MOVERS_UNIVERSE = ['NVDA','TSLA','AAPL','AMD','PLTR','META','AMZN','MSFT','GOOGL','COIN','NBIS','SOFI','HOOD','MSTR','AVGO','NFLX','INTC','SMCI','MU','ARM','UBER','SHOP','CRWD','PANW','SNOW','RIVN','LCID','NIO','BABA','JPM','BAC','XOM','CVX','WMT','COST','DIS','BA','PFE','MRNA','LLY','UNH','V','MA','PYPL','SQ','SPY','QQQ','IWM','GLD','TLT'];

// ---------------------------------------------------------------------------
// Database (Railway Postgres). One row per app install ("device account").
// ---------------------------------------------------------------------------
const { Pool } = require('pg');
const db = DATABASE_URL ? new Pool({ connectionString: DATABASE_URL, ssl: DATABASE_URL.includes('railway') ? { rejectUnauthorized: false } : undefined }) : null;
async function initDb() {
  if (!db) { console.warn('DATABASE_URL not set; brokerage features disabled'); return; }
  await db.query(`CREATE TABLE IF NOT EXISTS users (
    device_id TEXT PRIMARY KEY,
    snap_user_id TEXT, snap_user_secret TEXT, broker_name TEXT,
    created_at TIMESTAMPTZ DEFAULT now(), connected_at TIMESTAMPTZ
  )`);
  await db.query(`CREATE TABLE IF NOT EXISTS usage (
    device_id TEXT NOT NULL, day DATE NOT NULL, image_count INT NOT NULL DEFAULT 0, text_count INT NOT NULL DEFAULT 0,
    PRIMARY KEY (device_id, day)
  )`);
  console.log('db ready');
}
async function getUsage(deviceId) {
  const r = await db.query(`SELECT image_count, text_count FROM usage WHERE device_id=$1 AND day=(now() AT TIME ZONE 'America/New_York')::date`, [deviceId]);
  const row = r.rows[0] || { image_count: 0, text_count: 0 };
  return { imageCount: row.image_count, textCount: row.text_count, limit: DAILY_IMAGE_LIMIT, remaining: Math.max(0, DAILY_IMAGE_LIMIT - row.image_count) };
}
async function bumpUsage(deviceId, withImage) {
  const col = withImage ? 'image_count' : 'text_count';
  await db.query(`INSERT INTO usage(device_id, day, ${col}) VALUES($1, (now() AT TIME ZONE 'America/New_York')::date, 1)
    ON CONFLICT (device_id, day) DO UPDATE SET ${col} = usage.${col} + 1`, [deviceId]);
}
const isDeviceId = v => typeof v === 'string' && /^[a-zA-Z0-9\-]{16,64}$/.test(v);
async function getUser(deviceId) {
  const r = await db.query('INSERT INTO users(device_id) VALUES($1) ON CONFLICT (device_id) DO UPDATE SET device_id=EXCLUDED.device_id RETURNING *', [deviceId]);
  return r.rows[0];
}

// ---------------------------------------------------------------------------
// SnapTrade (read-only brokerage connections)
// ---------------------------------------------------------------------------
const { Snaptrade } = require('snaptrade-typescript-sdk');
const snap = SNAP_CLIENT_ID && SNAP_CONSUMER_KEY ? new Snaptrade({ clientId: SNAP_CLIENT_ID, consumerKey: SNAP_CONSUMER_KEY }) : null;

async function ensureSnapUser(user) {
  if (user.snap_user_id && user.snap_user_secret) return user;
  const snapUserId = 'pl_' + user.device_id;
  const reg = await snap.authentication.registerSnapTradeUser({ userId: snapUserId });
  const secret = reg.data.userSecret;
  const r = await db.query('UPDATE users SET snap_user_id=$2, snap_user_secret=$3 WHERE device_id=$1 RETURNING *', [user.device_id, snapUserId, secret]);
  return r.rows[0];
}
const creds = u => ({ userId: u.snap_user_id, userSecret: u.snap_user_secret });

async function listPositions(user) {
  const accts = (await snap.accountInformation.listUserAccounts(creds(user))).data || [];
  const out = [];
  for (const a of accts) {
    let pos = [];
    try { pos = (await snap.accountInformation.getUserAccountPositions({ ...creds(user), accountId: a.id })).data || []; } catch (e) { console.error('positions', a.id, e.message); }
    for (const p of pos) {
      const sym = p.symbol?.symbol?.symbol || p.symbol?.symbol?.raw_symbol || p.symbol?.raw_symbol || p.symbol?.symbol || null;
      if (!sym || !(p.units > 0)) continue;
      out.push({ symbol: String(sym).toUpperCase(), qty: p.units, avgCost: p.average_purchase_price ?? null, brokerPrice: p.price ?? null, account: a.name || a.institution_name || '' });
    }
  }
  // merge same symbol across accounts
  const merged = new Map();
  for (const p of out) { const m = merged.get(p.symbol); if (!m) merged.set(p.symbol, { ...p }); else { const q = m.qty + p.qty; m.avgCost = m.avgCost != null && p.avgCost != null ? (m.avgCost * m.qty + p.avgCost * p.qty) / q : m.avgCost ?? p.avgCost; m.qty = q; } }
  return { accounts: accts.map(a => ({ id: a.id, name: a.name || '', institution: a.institution_name || '' })), positions: [...merged.values()] };
}

async function listTrades(user, days = 180) {
  const end = new Date(), start = new Date(Date.now() - days * 86400000);
  const fmt = d => d.toISOString().slice(0, 10);
  const acts = (await snap.transactionsAndReporting.getActivities({ ...creds(user), startDate: fmt(start), endDate: fmt(end) })).data || [];
  const fills = acts.filter(a => ['BUY', 'SELL'].includes(String(a.type || '').toUpperCase()) && a.units && a.price)
    .map(a => ({ symbol: String(a.symbol?.symbol || a.symbol?.raw_symbol || '').toUpperCase(), side: String(a.type).toUpperCase(), qty: Math.abs(a.units), price: a.price, date: (a.trade_date || a.settlement_date || '').slice(0, 10) }))
    .filter(f => f.symbol).sort((x, y) => x.date < y.date ? -1 : 1);
  // FIFO pairing into closed trades
  const open = new Map(), closed = [];
  for (const f of fills) {
    const q = open.get(f.symbol) || []; 
    if (f.side === 'BUY') { q.push({ ...f }); open.set(f.symbol, q); continue; }
    let left = f.qty;
    while (left > 0 && q.length) {
      const lot = q[0]; const take = Math.min(left, lot.qty);
      closed.push({ symbol: f.symbol, side: 'Long', qty: take, entry: lot.price, exit: f.price, entryDate: lot.date, exitDate: f.date, plPct: (f.price - lot.price) / lot.price * 100 });
      lot.qty -= take; left -= take; if (lot.qty <= 0) q.shift();
    }
  }
  return { fills, closed: closed.sort((a, b) => a.exitDate < b.exitDate ? 1 : -1).slice(0, 30) };
}

async function gradeTrades(closed) {
  if (!ANTHROPIC_API_KEY || !closed.length) return closed.map(t => ({ ...t, grade: null, why: '' }));
  const system = `You grade a retail trader's closed stock trades for education. For each trade give a letter grade A, B or C and a 1-2 sentence "why" in plain English that a beginner understands. Judge: was the entry at a sensible level relative to the move, was risk defined and proportionate, was the exit disciplined (took profit / cut loss) or emotional. You only know entry, exit, dates and size, so be fair about uncertainty and never invent chart details. Return ONLY JSON: {"grades":[{"i":index,"grade":"A|B|C","why":"..."}]}`;
  const list = closed.map((t, i) => `${i}: ${t.symbol} ${t.side} ${t.qty} sh, in ${t.entry} on ${t.entryDate}, out ${t.exit} on ${t.exitDate}, P/L ${t.plPct.toFixed(1)}%`).join('\n');
  const res = await fetch('https://api.anthropic.com/v1/messages', { method: 'POST', headers: { 'content-type': 'application/json', 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: ANTHROPIC_MODEL, max_tokens: 2000, system, messages: [{ role: 'user', content: list }] }) });
  if (!res.ok) throw new Error('grade model ' + res.status);
  const data = await res.json(); const text = (data.content || []).filter(b => b.type === 'text').map(b => b.text).join('');
  const j = JSON.parse(text.slice(text.indexOf('{'), text.lastIndexOf('}') + 1));
  const by = new Map((j.grades || []).map(g => [g.i, g]));
  return closed.map((t, i) => ({ ...t, grade: ['A', 'B', 'C'].includes(by.get(i)?.grade) ? by.get(i).grade : null, why: String(by.get(i)?.why || '') }));
}

let brokerListCache = { at: 0, data: [] };
async function listBrokerages() {
  if (Date.now() - brokerListCache.at < 6 * 3600e3 && brokerListCache.data.length) return brokerListCache.data;
  const r = await snap.referenceData.listAllBrokerages();
  const data = (r.data || []).filter(b => b.enabled !== false).map(b => ({
    slug: b.slug, name: b.display_name || b.name, logo: b.aws_s3_square_logo_url || b.aws_s3_logo_url || null, wideLogo: b.aws_s3_logo_url || null,
    maintenance: !!b.maintenance_mode
  })).sort((a, b) => a.name.localeCompare(b.name));
  brokerListCache = { at: Date.now(), data };
  return data;
}

// ---- movers: rolling quote cache for the universe ----
const moversCache = new Map(); // symbol -> {price, prevClose, changePct, high, low, at}
let moversIdx = 0;
async function moversTick() {
  if (!FINNHUB_KEY) return;
  const sym = MOVERS_UNIVERSE[moversIdx++ % MOVERS_UNIVERSE.length];
  try {
    const r = await fetch(`https://finnhub.io/api/v1/quote?symbol=${sym}&token=${FINNHUB_KEY}`);
    const q = await r.json();
    if (r.ok && typeof q.c === 'number' && q.c > 0) moversCache.set(sym, { symbol: sym, price: q.c, prevClose: q.pc, changePct: q.dp, high: q.h, low: q.l, at: Date.now() });
  } catch (e) { /* skip */ }
}
// 1 request every 6s = 10/min, well under the 60/min free limit; full universe refreshes every ~5 minutes.
setInterval(moversTick, 6000); for (let i = 0; i < 5; i++) setTimeout(moversTick, i * 800);
function moversList() {
  return [...moversCache.values()].filter(x => typeof x.changePct === 'number').sort((a, b) => Math.abs(b.changePct) - Math.abs(a.changePct));
}

function json(res, code, obj) { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(obj)); }
const PORT = process.env.PORT || 8080;
if (!FINNHUB_KEY) { console.error('Set FINNHUB_KEY'); process.exit(1); }

// symbol -> Set of client sockets watching it
const watchers = new Map();
// last price per symbol, sent to new subscribers immediately
const lastQuote = new Map();

let upstream = null;
let backoff = 1000;

function connectUpstream() {
  upstream = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_KEY}`);

  upstream.on('open', () => {
    backoff = 1000;
    broadcastStatus('connected');
    // re-subscribe everything clients are watching (after a reconnect)
    for (const symbol of watchers.keys()) upstreamSend({ type: 'subscribe', symbol });
  });

  upstream.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'ping') return;
    if (msg.type !== 'trade' || !Array.isArray(msg.data)) return;
    // Finnhub batches trades: [{s: symbol, p: price, t: ms, v: volume}]
    for (const t of msg.data) {
      const quote = { type: 'quote', symbol: t.s, price: t.p, ts: t.t, volume: t.v };
      lastQuote.set(t.s, quote);
      const set = watchers.get(t.s);
      if (!set) continue;
      const payload = JSON.stringify(quote);
      for (const client of set) if (client.readyState === WebSocket.OPEN) client.send(payload);
    }
  });

  const retry = () => {
    broadcastStatus('reconnecting');
    setTimeout(connectUpstream, backoff);
    backoff = Math.min(backoff * 2, 30000);
  };
  upstream.on('close', retry);
  upstream.on('error', (e) => { console.error('upstream error', e.message); upstream.terminate(); });
}

function upstreamSend(obj) {
  if (upstream && upstream.readyState === WebSocket.OPEN) upstream.send(JSON.stringify(obj));
}

function broadcastStatus(state) {
  const payload = JSON.stringify({ type: 'status', upstream: state });
  for (const client of wss.clients) if (client.readyState === WebSocket.OPEN) client.send(payload);
}

function subscribe(client, symbols) {
  for (let s of symbols) {
    s = String(s).toUpperCase().trim(); if (!s) continue;
    let set = watchers.get(s);
    if (!set) { set = new Set(); watchers.set(s, set); upstreamSend({ type: 'subscribe', symbol: s }); }
    set.add(client);
    client.symbols.add(s);
    const last = lastQuote.get(s);
    if (last) client.send(JSON.stringify(last)); // instant first paint
  }
}

// REST quote for each symbol, delivered over the socket so the client needs only one channel.
async function sendSnapshot(client, symbols) {
  for (let s of symbols) {
    s = String(s).toUpperCase().trim(); if (!/^[A-Z0-9.\-]{1,12}$/.test(s)) continue;
    try {
      const r = await fetch(`https://finnhub.io/api/v1/quote?symbol=${s}&token=${FINNHUB_KEY}`);
      const q = await r.json();
      if (!r.ok || typeof q.c !== 'number' || q.c === 0) continue;
      const quote = { type: 'quote', symbol: s, price: q.c, prevClose: q.pc, changePct: q.dp, ts: Date.now(), snapshot: true };
      if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(quote));
    } catch (e) { /* skip symbol */ }
  }
}

function unsubscribe(client, symbols) {
  for (let s of symbols) {
    s = String(s).toUpperCase().trim();
    const set = watchers.get(s); if (!set) continue;
    set.delete(client); client.symbols.delete(s);
    if (set.size === 0) { watchers.delete(s); upstreamSend({ type: 'unsubscribe', symbol: s }); }
  }
}


// ---------------------------------------------------------------------------
// POST /analyze  — real chart reading with a vision model.
// Body (JSON): { ticker, price, changePct?, horizon?, imageBase64?, mediaType? }
// Returns the Analysis shape the app expects. The image is optional; without it
// the model works from ticker + live price only and says so in the explanation.
// ---------------------------------------------------------------------------
function readBody(req, limit = 12 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => { size += c.length; if (size > limit) { reject(new Error('too large')); req.destroy(); } else chunks.push(c); });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}
const num = (v, fallback) => (typeof v === 'number' && isFinite(v) ? v : fallback);

async function analyzeChart({ ticker, price, changePct, horizon, imageBase64, mediaType, strategy }) {
  const tf = { Intraday: '15m', Swing: '4H', Position: '1D', 'Long term': '1W' }[horizon] || '1D';
  const system = `You are a disciplined technical analyst. You read price charts and describe structure, levels and a potential trade plan for education, never as advice.
Return ONLY a JSON object, no prose, no markdown fences, with exactly these keys:
{
 "timeframe": string (the chart's timeframe if visible, else "${tf}"),
 "trend": "Bullish" | "Bearish" | "Neutral",
 "support": number, "resistance": number,
 "rsi": number (0-100; estimate from the chart if an RSI pane is visible, else infer from momentum and say so in explanation),
 "entryLo": number, "entryHi": number, "target": number, "target2": number | null, "stop": number,
 "trigger": string (one sentence: the exact condition and price that would start the trade, e.g. "Buy on a 5-minute close above $241.20 with volume above the morning average"),
 "pattern": string (e.g. "Bull flag", "Range consolidation", or "No clear pattern"),
 "signals": [ {"t": string, "d": "up"|"dn"|""} ] (4 to 6 short items: RSI, MACD, volume, moving averages, pattern),
 "explanation": string (3-5 sentences, plain English, reference the actual levels you chose),
 "bullCase": string (1-2 sentences), "bearCase": string (1-2 sentences),
 "fit": { "score": "Strong" | "Partial" | "Poor" | null, "why": string } (how well THIS chart fits the trader's stated strategy; null score if no strategy given),
 "series": number[] (about 40 numbers: the approximate price path visible on the chart from left to right, ending near the current price; if no chart image, return [])
}
${tf === '15m' || tf === '5m' || tf === '1m' ? `INTRADAY MODE (day trader): the trader wants precise, actionable numbers. Give every level to the cent. Use the chart's own structure: opening range high/low, prior-day high/low/close, VWAP or moving averages if drawn, obvious intraday swing points, round numbers. Keep the stop tight (typically 0.3%-1.5% from entry) and place it just beyond a real level, not an arbitrary distance. target must be the nearest realistic objective; target2 the next level beyond it. The trigger must be a concrete, observable condition (a break, a reclaim, a rejection at a level) with a price. If the chart does not show enough intraday detail to be precise, say so in the explanation and widen the entry instead of guessing.` : `SWING MODE: levels can be rounded sensibly; target2 may be null.`}
Rules: all price levels must be plausible relative to the CURRENT PRICE given (typically within 40% of it). Support must be below current price and resistance above, unless the chart clearly shows otherwise. For a long setup: entryLo <= entryHi <= about current price, target > entryHi, stop < entryLo. For a short setup: reverse. If the image is not a price chart, set trend to "Neutral", pattern to "Not a chart", and explain that in one sentence.`;
  const stratText = strategy ? `\nTRADER'S STRATEGY: ${strategy.name}. Style: ${strategy.style}. Entry trigger: ${strategy.trigger}. Stop placement: ${strategy.stop}. Targets: ${strategy.target}.${strategy.notes ? ' Notes: ' + strategy.notes : ''}\nJudge the chart against THIS strategy: say plainly whether it fits (Strong/Partial/Poor) and why in one or two sentences, and shape trigger, stop and targets to match the strategy's rules. If the chart does not fit, still give the levels the strategy would need to see before entering.` : '';
  const userText = `Ticker: ${ticker}\nCURRENT PRICE (live): ${price}${typeof changePct === 'number' ? `\nChange today: ${changePct.toFixed(2)}%` : ''}\nTrader's preferred timeframe: ${tf}${stratText}${imageBase64 ? '\nA chart image is attached. Read the actual levels from it.' : '\nNo chart image was provided; analyze from ticker and price context only and say so.'}`;
  const content = [];
  if (imageBase64) {
    // Detect the real format from the bytes; phones often mislabel JPEGs as PNGs.
    const b = imageBase64.replace(/^data:[^,]+,/, '');
    const sniffed = b.startsWith('/9j/') ? 'image/jpeg' : b.startsWith('iVBORw0') ? 'image/png' : b.startsWith('R0lGOD') ? 'image/gif' : b.startsWith('UklGR') ? 'image/webp' : (mediaType || 'image/jpeg');
    content.push({ type: 'image', source: { type: 'base64', media_type: sniffed, data: b } });
  }
  content.push({ type: 'text', text: userText });

  const ctrl = new AbortController(); const timer = setTimeout(() => ctrl.abort(), 60000);
  let res;
  try {
    res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST', signal: ctrl.signal,
      headers: { 'content-type': 'application/json', 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: ANTHROPIC_MODEL, max_tokens: 1500, system, messages: [{ role: 'user', content }] })
    });
  } finally { clearTimeout(timer); }
  if (!res.ok) { const t = await res.text(); throw new Error(`model ${res.status}: ${t.slice(0, 300)}`); }
  const data = await res.json();
  const text = (data.content || []).filter(b => b.type === 'text').map(b => b.text).join('\n');
  const clean = text.replace(/```json|```/g, '').trim();
  const j = JSON.parse(clean.slice(clean.indexOf('{'), clean.lastIndexOf('}') + 1));

  // sanity-check numbers so a model slip can't put nonsense on screen
  const inBand = v => num(v, NaN) > price * 0.4 && v < price * 2.5;
  const support = inBand(j.support) ? j.support : price * 0.95, resistance = inBand(j.resistance) ? j.resistance : price * 1.05;
  const entryLo = inBand(j.entryLo) ? j.entryLo : price * 0.99, entryHi = inBand(j.entryHi) ? j.entryHi : price * 1.005;
  const target = inBand(j.target) ? j.target : resistance, stop = inBand(j.stop) ? j.stop : support * 0.985;
  const target2 = inBand(j.target2) ? j.target2 : null;
  const mid = (entryLo + entryHi) / 2, rr = Math.abs(target - mid) / Math.max(Math.abs(mid - stop), price * 0.001);
  let series = Array.isArray(j.series) ? j.series.filter(v => typeof v === 'number' && isFinite(v) && v > 0).slice(0, 80) : [];
  if (series.length < 8) series = Array.from({ length: 40 }, (_, i) => support + (price - support) * (i / 39)); // flat-ish placeholder path
  const k = price / series[series.length - 1]; series = series.map(v => v * k); // end exactly at the live price
  const trend = ['Bullish', 'Bearish', 'Neutral'].includes(j.trend) ? j.trend : 'Neutral';
  return {
    ticker, price, timeframe: String(j.timeframe || tf), trend, support, resistance,
    rsi: Math.round(Math.max(0, Math.min(100, num(j.rsi, 50)))), pts: series,
    entryLo, entryHi, target, target2, stop, rr: Math.round(rr * 10) / 10, trigger: String(j.trigger || ''), intraday: tf === '15m' || tf === '5m' || tf === '1m',
    signals: (Array.isArray(j.signals) ? j.signals : []).slice(0, 6).map(x => ({ t: String(x.t || ''), d: ['up', 'dn'].includes(x.d) ? x.d : '' })).filter(x => x.t),
    pattern: String(j.pattern || 'No clear pattern'), bull: trend !== 'Bearish',
    explanation: String(j.explanation || ''), bullCase: String(j.bullCase || ''), bearCase: String(j.bearCase || ''),
    fit: strategy && j.fit && ['Strong','Partial','Poor'].includes(j.fit.score) ? { score: j.fit.score, why: String(j.fit.why || ''), strategy: strategy.name } : null,
    at: Date.now(), priceSource: 'live', engine: ANTHROPIC_MODEL, hadImage: !!imageBase64
  };
}

// GET /quote?symbol=NBIS -> {symbol, price, change, changePct, high, low, open, prevClose}
// Proxies Finnhub's REST quote so the app never holds the API key. Node 18+ has global fetch.
const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'content-type, x-peekline-key');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, 'http://x');
  // ---- app secret (all app-only routes) ----
  const appOnly = url.pathname.startsWith('/brokerage/') || url.pathname === '/me' || url.pathname === '/analyze' || url.pathname === '/movers';
  if (appOnly && APP_SECRET && req.headers['x-peekline-key'] !== APP_SECRET) return json(res, 401, { error: 'unauthorized' });

  if (url.pathname === '/usage') {
    const deviceId = url.searchParams.get('deviceId');
    if (!db || !isDeviceId(deviceId)) return json(res, 400, { error: 'deviceId required' });
    return json(res, 200, await getUsage(deviceId));
  }
  if (url.pathname === '/movers') {
    return json(res, 200, { asOf: Date.now(), universe: MOVERS_UNIVERSE.length, movers: moversList() });
  }
  if (url.pathname === '/brokerage/list') {
    if (!snap) return json(res, 503, { error: 'SnapTrade keys not set' });
    try { return json(res, 200, { brokerages: await listBrokerages() }); } catch (e) { return json(res, 502, { error: 'could not load brokerages', detail: String(e.message).slice(0, 200) }); }
  }
  // ---- brokerage routes ----
  if (url.pathname.startsWith('/brokerage/') || url.pathname === '/me') {
    if (!db) return json(res, 503, { error: 'database not configured' });
    let body = {}; if (req.method === 'POST') { try { body = JSON.parse(await readBody(req, 1e6) || '{}'); } catch { return json(res, 400, { error: 'bad json' }); } }
    const deviceId = body.deviceId || url.searchParams.get('deviceId');
    if (!isDeviceId(deviceId)) return json(res, 400, { error: 'deviceId required' });
    try {
      let user = await getUser(deviceId);
      const connected = !!user.connected_at;
      if (url.pathname === '/me') return json(res, 200, { connected, broker: user.broker_name || null, connectedAt: user.connected_at });
      if (!snap) return json(res, 503, { error: 'SnapTrade keys not set' });
      if (url.pathname === '/brokerage/connect' && req.method === 'POST') {
        user = await ensureSnapUser(user);
        const login = await snap.authentication.loginSnapTradeUser({ ...creds(user), connectionType: 'read', ...(body.broker ? { broker: body.broker } : {}) });
        return json(res, 200, { url: login.data.redirectURI });
      }
      if (url.pathname === '/brokerage/positions') {
        if (!user.snap_user_id) return json(res, 200, { connected: false, accounts: [], positions: [] });
        const data = await listPositions(user);
        if (data.accounts.length && !connected) { await db.query('UPDATE users SET connected_at=now(), broker_name=$2 WHERE device_id=$1', [deviceId, data.accounts[0].institution || null]); }
        return json(res, 200, { connected: data.accounts.length > 0, broker: data.accounts[0]?.institution || user.broker_name || null, ...data });
      }
      if (url.pathname === '/brokerage/trades') {
        if (!user.snap_user_id) return json(res, 200, { closed: [] });
        const { closed } = await listTrades(user);
        const graded = url.searchParams.get('grade') === '1' ? await gradeTrades(closed) : closed.map(t => ({ ...t, grade: null, why: '' }));
        return json(res, 200, { closed: graded });
      }
      if (url.pathname === '/brokerage/disconnect' && req.method === 'POST') {
        if (user.snap_user_id) { try { await snap.authentication.deleteSnapTradeUser(creds(user)); } catch (e) { console.error('snap delete', e.message); } }
        await db.query('UPDATE users SET snap_user_id=NULL, snap_user_secret=NULL, broker_name=NULL, connected_at=NULL WHERE device_id=$1', [deviceId]);
        return json(res, 200, { ok: true });
      }
      return json(res, 404, { error: 'not found' });
    } catch (e) {
      const detail = e.response?.data ? JSON.stringify(e.response.data).slice(0, 300) : String(e.message).slice(0, 300);
      console.error('brokerage error', url.pathname, detail);
      return json(res, 502, { error: 'brokerage request failed', detail });
    }
  }
  if (url.pathname === '/analyze' && req.method === 'POST') {
    if (!ANTHROPIC_API_KEY) { res.writeHead(503, {'Content-Type':'application/json'}); return res.end('{"error":"ANTHROPIC_API_KEY not set"}'); }
    try {
      const body = JSON.parse(await readBody(req));
      const ticker = String(body.ticker || '').toUpperCase().trim(); const price = Number(body.price);
      if (!/^[A-Z0-9.\-]{1,12}$/.test(ticker) || !(price > 0)) { res.writeHead(400, {'Content-Type':'application/json'}); return res.end('{"error":"ticker and price required"}'); }
      const withImage = !!body.imageBase64; const deviceId = isDeviceId(body.deviceId) ? body.deviceId : null;
      let usage = null;
      if (db && deviceId) {
        usage = await getUsage(deviceId);
        if (withImage && usage.remaining <= 0) return json(res, 429, { error: 'daily limit', detail: `You've reached today's fair-use limit of ${DAILY_IMAGE_LIMIT} chart analyses. It resets at midnight Eastern.`, usage });
      }
      const st = body.strategy && typeof body.strategy === 'object' ? { name: String(body.strategy.name || '').slice(0, 60), style: String(body.strategy.style || '').slice(0, 40), trigger: String(body.strategy.trigger || '').slice(0, 120), stop: String(body.strategy.stop || '').slice(0, 120), target: String(body.strategy.target || '').slice(0, 120), notes: String(body.strategy.notes || '').slice(0, 300) } : null;
      const out = await analyzeChart({ ticker, price, changePct: body.changePct, horizon: body.horizon, imageBase64: body.imageBase64, mediaType: body.mediaType, strategy: st && st.name ? st : null });
      if (db && deviceId) { await bumpUsage(deviceId, withImage); usage = await getUsage(deviceId); }
      res.writeHead(200, {'Content-Type':'application/json'}); return res.end(JSON.stringify({ ...out, usage }));
    } catch (e) {
      console.error('analyze failed', e.message);
      res.writeHead(502, {'Content-Type':'application/json'}); return res.end(JSON.stringify({ error: 'analysis failed', detail: String(e.message).slice(0, 200) }));
    }
  }
  if (url.pathname === '/quote') {
    const symbol = String(url.searchParams.get('symbol') || '').toUpperCase().trim();
    if (!/^[A-Z0-9.\-]{1,12}$/.test(symbol)) { res.writeHead(400, {'Content-Type':'application/json'}); return res.end('{"error":"bad symbol"}'); }
    try {
      const r = await fetch(`https://finnhub.io/api/v1/quote?symbol=${symbol}&token=${FINNHUB_KEY}`);
      const q = await r.json();
      if (!r.ok || typeof q.c !== 'number' || q.c === 0) { res.writeHead(404, {'Content-Type':'application/json'}); return res.end(JSON.stringify({error:'no quote', symbol})); }
      res.writeHead(200, {'Content-Type':'application/json', 'Cache-Control':'no-store'});
      res.end(JSON.stringify({ symbol, price: q.c, change: q.d, changePct: q.dp, high: q.h, low: q.l, open: q.o, prevClose: q.pc, ts: Date.now() }));
    } catch (e) { res.writeHead(502, {'Content-Type':'application/json'}); res.end(JSON.stringify({error:'upstream failed'})); }
    return;
  }
  res.writeHead(200, {'Content-Type':'text/plain'}); res.end('Peekline relay OK');
});
const wss = new WebSocket.Server({ server });

wss.on('connection', (client) => {
  client.symbols = new Set();
  client.isAlive = true;
  client.on('pong', () => { client.isAlive = true; });
  client.send(JSON.stringify({ type: 'status', upstream: upstream && upstream.readyState === WebSocket.OPEN ? 'connected' : 'reconnecting' }));

  client.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type === 'subscribe' && Array.isArray(msg.symbols)) subscribe(client, msg.symbols.slice(0, 50));
    if (msg.type === 'unsubscribe' && Array.isArray(msg.symbols)) unsubscribe(client, msg.symbols);
    if (msg.type === 'snapshot' && Array.isArray(msg.symbols)) sendSnapshot(client, msg.symbols.slice(0, 50));
  });

  client.on('close', () => unsubscribe(client, [...client.symbols]));
});

// drop dead clients so their symbols get unsubscribed upstream
setInterval(() => {
  for (const client of wss.clients) {
    if (!client.isAlive) { client.terminate(); continue; }
    client.isAlive = false; client.ping();
  }
}, 30000);

initDb().catch(e => console.error('db init failed', e.message));
connectUpstream();
server.listen(PORT, () => console.log(`relay listening on ws://localhost:${PORT}`));
