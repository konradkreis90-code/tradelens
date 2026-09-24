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

async function analyzeChart({ ticker, price, changePct, horizon, imageBase64, mediaType }) {
  const tf = { Intraday: '15m', Swing: '4H', Position: '1D', 'Long term': '1W' }[horizon] || '1D';
  const system = `You are a disciplined technical analyst. You read price charts and describe structure, levels and a potential trade plan for education, never as advice.
Return ONLY a JSON object, no prose, no markdown fences, with exactly these keys:
{
 "timeframe": string (the chart's timeframe if visible, else "${tf}"),
 "trend": "Bullish" | "Bearish" | "Neutral",
 "support": number, "resistance": number,
 "rsi": number (0-100; estimate from the chart if an RSI pane is visible, else infer from momentum and say so in explanation),
 "entryLo": number, "entryHi": number, "target": number, "stop": number,
 "pattern": string (e.g. "Bull flag", "Range consolidation", or "No clear pattern"),
 "signals": [ {"t": string, "d": "up"|"dn"|""} ] (4 to 6 short items: RSI, MACD, volume, moving averages, pattern),
 "explanation": string (3-5 sentences, plain English, reference the actual levels you chose),
 "bullCase": string (1-2 sentences), "bearCase": string (1-2 sentences),
 "series": number[] (about 40 numbers: the approximate price path visible on the chart from left to right, ending near the current price; if no chart image, return [])
}
Rules: all price levels must be plausible relative to the CURRENT PRICE given (typically within 40% of it). Support must be below current price and resistance above, unless the chart clearly shows otherwise. For a long setup: entryLo <= entryHi <= about current price, target > entryHi, stop < entryLo. For a short setup: reverse. If the image is not a price chart, set trend to "Neutral", pattern to "Not a chart", and explain that in one sentence.`;
  const userText = `Ticker: ${ticker}\nCURRENT PRICE (live): ${price}${typeof changePct === 'number' ? `\nChange today: ${changePct.toFixed(2)}%` : ''}\nTrader's preferred timeframe: ${tf}${imageBase64 ? '\nA chart image is attached. Read the actual levels from it.' : '\nNo chart image was provided; analyze from ticker and price context only and say so.'}`;
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
  const mid = (entryLo + entryHi) / 2, rr = Math.abs(target - mid) / Math.max(Math.abs(mid - stop), price * 0.001);
  let series = Array.isArray(j.series) ? j.series.filter(v => typeof v === 'number' && isFinite(v) && v > 0).slice(0, 80) : [];
  if (series.length < 8) series = Array.from({ length: 40 }, (_, i) => support + (price - support) * (i / 39)); // flat-ish placeholder path
  const k = price / series[series.length - 1]; series = series.map(v => v * k); // end exactly at the live price
  const trend = ['Bullish', 'Bearish', 'Neutral'].includes(j.trend) ? j.trend : 'Neutral';
  return {
    ticker, price, timeframe: String(j.timeframe || tf), trend, support, resistance,
    rsi: Math.round(Math.max(0, Math.min(100, num(j.rsi, 50)))), pts: series,
    entryLo, entryHi, target, stop, rr: Math.round(rr * 10) / 10,
    signals: (Array.isArray(j.signals) ? j.signals : []).slice(0, 6).map(x => ({ t: String(x.t || ''), d: ['up', 'dn'].includes(x.d) ? x.d : '' })).filter(x => x.t),
    pattern: String(j.pattern || 'No clear pattern'), bull: trend !== 'Bearish',
    explanation: String(j.explanation || ''), bullCase: String(j.bullCase || ''), bearCase: String(j.bearCase || ''),
    at: Date.now(), priceSource: 'live', engine: ANTHROPIC_MODEL, hadImage: !!imageBase64
  };
}

// GET /quote?symbol=NBIS -> {symbol, price, change, changePct, high, low, open, prevClose}
// Proxies Finnhub's REST quote so the app never holds the API key. Node 18+ has global fetch.
const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'content-type');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/analyze' && req.method === 'POST') {
    if (!ANTHROPIC_API_KEY) { res.writeHead(503, {'Content-Type':'application/json'}); return res.end('{"error":"ANTHROPIC_API_KEY not set"}'); }
    try {
      const body = JSON.parse(await readBody(req));
      const ticker = String(body.ticker || '').toUpperCase().trim(); const price = Number(body.price);
      if (!/^[A-Z0-9.\-]{1,12}$/.test(ticker) || !(price > 0)) { res.writeHead(400, {'Content-Type':'application/json'}); return res.end('{"error":"ticker and price required"}'); }
      const out = await analyzeChart({ ticker, price, changePct: body.changePct, horizon: body.horizon, imageBase64: body.imageBase64, mediaType: body.mediaType });
      res.writeHead(200, {'Content-Type':'application/json'}); return res.end(JSON.stringify(out));
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

connectUpstream();
server.listen(PORT, () => console.log(`relay listening on ws://localhost:${PORT}`));
