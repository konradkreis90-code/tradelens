// TradeLens quote relay
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

function unsubscribe(client, symbols) {
  for (let s of symbols) {
    s = String(s).toUpperCase().trim();
    const set = watchers.get(s); if (!set) continue;
    set.delete(client); client.symbols.delete(s);
    if (set.size === 0) { watchers.delete(s); upstreamSend({ type: 'unsubscribe', symbol: s }); }
  }
}

// GET /quote?symbol=NBIS -> {symbol, price, change, changePct, high, low, open, prevClose}
// Proxies Finnhub's REST quote so the app never holds the API key. Node 18+ has global fetch.
const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const url = new URL(req.url, 'http://x');
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
  res.writeHead(200, {'Content-Type':'text/plain'}); res.end('TradeLens relay OK');
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
