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

const server = http.createServer((req, res) => { res.writeHead(200); res.end('TradeLens relay OK'); });
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
