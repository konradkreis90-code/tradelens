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

// Node 18 doesn't expose Web Crypto globally; the SnapTrade SDK needs it for request signatures.
if (!globalThis.crypto) globalThis.crypto = require('crypto').webcrypto;

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
const EXPLAIN_DAILY_LIMIT = Number(process.env.EXPLAIN_DAILY_LIMIT || 30); // "Explain my portfolio" requests per device per day (text_count)
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
  pruneUsage(); setInterval(pruneUsage, 24 * 3600e3);
}
// Privacy policy promise: daily usage counts are kept for 90 days, then deleted.
function pruneUsage() {
  db.query(`DELETE FROM usage WHERE day < (now() AT TIME ZONE 'America/New_York')::date - 90`)
    .then(r => { if (r.rowCount) console.log('pruned', r.rowCount, 'old usage rows'); })
    .catch(e => console.error('prune usage failed', e.message));
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
const snapSdk = require('snaptrade-typescript-sdk');
function makeSnap() {
  if (!SNAP_CLIENT_ID || !SNAP_CONSUMER_KEY) return null;
  // SDK v10+ uses an auth object; older versions take the keys directly.
  if (snapSdk.SnaptradeAuth && typeof snapSdk.SnaptradeAuth.commercialApiKey === 'function') {
    return new snapSdk.Snaptrade({ auth: snapSdk.SnaptradeAuth.commercialApiKey({ clientId: SNAP_CLIENT_ID, consumerKey: SNAP_CONSUMER_KEY }) });
  }
  return new snapSdk.Snaptrade({ clientId: SNAP_CLIENT_ID, consumerKey: SNAP_CONSUMER_KEY });
}
const snap = makeSnap();
if (snap) console.log('snaptrade sdk ready', snapSdk.SnaptradeAuth ? '(auth-object mode)' : '(legacy mode)');

async function ensureSnapUser(user) {
  if (user.snap_user_id && user.snap_user_secret) return user;
  const snapUserId = 'pl_' + user.device_id;
  const reg = await snap.authentication.registerSnapTradeUser({ userId: snapUserId });
  const secret = reg.data.userSecret;
  const r = await db.query('UPDATE users SET snap_user_id=$2, snap_user_secret=$3 WHERE device_id=$1 RETURNING *', [user.device_id, snapUserId, secret]);
  return r.rows[0];
}
const creds = u => ({ userId: u.snap_user_id, userSecret: u.snap_user_secret });

// Try the current endpoint first, fall back to older ones if SnapTrade returns 410/404.
async function tryChain(steps) {
  let lastErr;
  for (const step of steps) {
    try { return await step(); }
    catch (e) { lastErr = e; const code = e.response?.status; if (code && ![404, 410].includes(code)) throw e; console.log('endpoint unavailable, trying next:', code, e.message); }
  }
  throw lastErr;
}
const lastRefresh = new Map(); // authorizationId -> timestamp
async function refreshConnections(user) {
  try {
    const auths = (await snap.connections.listBrokerageAuthorizations(creds(user))).data || [];
    for (const a of auths) {
      if (Date.now() - (lastRefresh.get(a.id) || 0) < 10 * 60e3) continue; // at most every 10 min per connection
      lastRefresh.set(a.id, Date.now());
      try { await snap.connections.refreshBrokerageAuthorization({ ...creds(user), authorizationId: a.id }); console.log('refresh requested for', a.brokerage?.name || a.id, 'status', a.status || ''); }
      catch (e) { console.log('refresh failed', a.id, e.response?.status, e.message); }
    }
  } catch (e) { console.log('list authorizations failed', e.response?.status, e.message); }
}
async function accountPositions(user, accountId) {
  // Current SnapTrade API (accounts created after May 2026 only have these):
  //   GET /accounts/{id}/positions/all  -> { positions:[stocks], option_positions:[...] }
  //   GET /accounts/{id}/positions      -> [stocks]
  // The old /holdings endpoints return 410 Gone for new customers.
  return tryChain([
    async () => { const r = await snap.accountInformation.getAllAccountPositions({ ...creds(user), accountId });
      const d = r.data || {}; const list = Array.isArray(d) ? d : (d.results || d.positions || []);
      console.log('positions/all keys:', Object.keys(d).join(','), '| entries:', list.length);
      return list; },
    async () => { const r = await snap.accountInformation.getUserAccountPositions({ ...creds(user), accountId }); return r.data || []; },
  ]);
}
// Short in-memory cache (never written to the database) so one screen load doesn't hit SnapTrade twice.
const briefCache = new Map(); // key -> { at, data }
async function cached(key, ms, fn) {
  const c = briefCache.get(key); if (c && Date.now() - c.at < ms) return c.data;
  const data = await fn(); briefCache.set(key, { at: Date.now(), data }); return data;
}
setInterval(() => { const now = Date.now(); for (const [k, v] of briefCache) if (now - v.at > 10 * 60e3) briefCache.delete(k); }, 5 * 60e3);
function forgetUser(deviceId) { for (const k of briefCache.keys()) if (k.startsWith(deviceId + ':')) briefCache.delete(k); }
const listAccounts = user => cached(`${user.device_id}:accounts`, 60e3, async () => (await snap.accountInformation.listUserAccounts(creds(user))).data || []);

async function listPositions(user) {
  const accts = await listAccounts(user);
  // Privacy: log counts and types only, never account numbers, holdings, or prices.
  console.log('accounts:', accts.length, JSON.stringify(accts.map(a => ({ type: a.meta?.type || a.raw_type || null, sync: a.sync_status || null }))).slice(0, 300));
  const out = [];
  for (const a of accts) {
    let pos = [];
    try { pos = await accountPositions(user, a.id); } catch (e) { console.error('positions', a.id, e.response?.status, e.message); }
    console.log(`positions: an account returned ${pos.length} raw`);
    if (pos.length) console.log('positions sample keys:', Object.keys(pos[0]).join(','));
    for (const p of pos) {
      const kind = String(p.type || p.instrument?.type || p.symbol?.type?.code || '').toUpperCase();
      if (kind.includes('OPTION')) continue; // options come later
      const sym = p.instrument?.symbol || p.instrument?.ticker || p.symbol?.symbol?.symbol || p.symbol?.symbol?.raw_symbol || p.symbol?.raw_symbol || (typeof p.symbol === 'string' ? p.symbol : p.symbol?.symbol) || p.symbol?.ticker || p.ticker || null;
      const qty = Number(p.units ?? p.quantity ?? p.fractional_units ?? 0);
      if (!sym || !(qty > 0)) { console.log('skipped position; keys:', Object.keys(p).join(',')); continue; }
      const n = v => (v == null || v === '' ? null : (isFinite(Number(v)) ? Number(v) : null));
      // Average cost per share. Current SnapTrade API: `cost_basis` = "book price or average purchase price" (per share);
      // older fields kept for fallback; last resort: weighted average of the tax lots.
      const lots = Array.isArray(p.tax_lots) ? p.tax_lots.filter(l => n(l.quantity) > 0 && n(l.purchased_price) != null) : [];
      const lotAvg = lots.length ? lots.reduce((s, l) => s + n(l.purchased_price) * n(l.quantity), 0) / lots.reduce((s, l) => s + n(l.quantity), 0) : null;
      const avgCost = n(p.average_purchase_price ?? p.average_cost ?? p.cost_basis_per_unit ?? p.cost_basis) ?? lotAvg;
      out.push({ symbol: String(sym).toUpperCase(), qty, avgCost, brokerPrice: n(p.price ?? p.last_price), account: a.name || a.institution_name || '' });
    }
  }
  if (!out.length) await refreshConnections(user); // empty holdings right after connecting usually means the broker sync hasn't run yet
  const merged = new Map();
  for (const p of out) { const m = merged.get(p.symbol); if (!m) merged.set(p.symbol, { ...p }); else { const q = m.qty + p.qty; m.avgCost = m.avgCost != null && p.avgCost != null ? (m.avgCost * m.qty + p.avgCost * p.qty) / q : m.avgCost ?? p.avgCost; m.qty = q; } }
  return { accounts: accts.map(a => ({ id: a.id, name: a.name || '', institution: a.institution_name || '' })), positions: [...merged.values()] };
}

const HISTORY_DAYS = 730;
const accountActivities = user => cached(`${user.device_id}:activities`, 120e3, () => fetchActivities(user));
async function fetchActivities(user, days = HISTORY_DAYS) {
  const end = new Date(), start = new Date(Date.now() - days * 86400000);
  const fmt = d => d.toISOString().slice(0, 10);
  const accts = await listAccounts(user);
  let acts = [];
  for (const a of accts) {
    // Per-account activities (the older all-accounts endpoint returns 410 Gone).
    try {
      const page = await tryChain([
        async () => { const r = await snap.accountInformation.getAccountActivities({ ...creds(user), accountId: a.id, startDate: fmt(start), endDate: fmt(end), limit: 1000 }); return r.data?.data || r.data?.results || r.data?.activities || (Array.isArray(r.data) ? r.data : []); },
        async () => { const r = await snap.transactionsAndReporting.getActivities({ ...creds(user), accounts: a.id, startDate: fmt(start), endDate: fmt(end) }); return r.data || []; },
      ]);
      acts = acts.concat(page);
    } catch (e) { console.error('activities', a.id, e.response?.status, e.message); }
  }
  console.log(`activities: ${acts.length} raw across ${accts.length} account(s); types: ${[...new Set(acts.map(a => a.type))].join(',') || 'none'}`, acts.length ? '| sample keys: ' + Object.keys(acts[0]).join(',') : '');
  return acts;
}
const actSymbol = a => String(a.instrument?.symbol || a.symbol?.symbol || a.symbol?.raw_symbol || (typeof a.symbol === 'string' ? a.symbol : '') || a.option_symbol?.ticker || '').toUpperCase();

async function listTrades(user) {
  const acts = await accountActivities(user);
  const fills = acts.filter(a => ['BUY', 'SELL'].includes(String(a.type || '').toUpperCase()) && a.units && a.price)
    .map(a => ({ symbol: actSymbol(a), side: String(a.type).toUpperCase(), qty: Math.abs(Number(a.units)), price: Number(a.price), date: (a.trade_date || a.settlement_date || '').slice(0, 10) }))
    .filter(f => f.symbol && f.qty > 0 && f.price > 0).sort((x, y) => x.date < y.date ? -1 : 1);
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
    // Sold shares with no visible buy: bought before the history window (or transferred in). Keep the sale visible.
    if (left > 0) closed.push({ symbol: f.symbol, side: 'Long', qty: left, entry: null, exit: f.price, entryDate: null, exitDate: f.date, plPct: null, note: 'Bought before available history' });
  }
  console.log(`trades: ${fills.length} fills -> ${closed.length} closed (${closed.filter(c => c.entry == null).length} without a visible buy)`);
  // Realized P/L only from trades where both the buy and the sell are visible.
  const known = closed.filter(c => c.entry != null);
  const realized = { amount: known.reduce((s, c) => s + (c.exit - c.entry) * c.qty, 0), trades: known.length, unknownEntry: closed.length - known.length, sinceDays: HISTORY_DAYS };
  return { fills, closed: closed.sort((a, b) => a.exitDate < b.exitDate ? 1 : -1).slice(0, 30), realized };
}

async function gradeTrades(closed) {
  if (!ANTHROPIC_API_KEY || !closed.length) return closed.map(t => ({ ...t, grade: null, why: '' }));
  const system = `You grade a retail trader's closed stock trades for education. For each trade give a letter grade A, B or C and a 1-2 sentence "why" in plain English that a beginner understands. Judge: was the entry at a sensible level relative to the move, was risk defined and proportionate, was the exit disciplined (took profit / cut loss) or emotional. You only know entry, exit, dates and size, so be fair about uncertainty and never invent chart details. Always answer by calling the trade_grades tool.`;
  const list = closed.map((t, i) => t.entry == null ? `${i}: ${t.symbol} SELL ${t.qty} sh at ${t.exit} on ${t.exitDate} (entry unknown: bought before history window) - grade none` : `${i}: ${t.symbol} ${t.side} ${t.qty} sh, in ${t.entry} on ${t.entryDate}, out ${t.exit} on ${t.exitDate}, P/L ${t.plPct.toFixed(1)}%`).join('\n');
  const tool = { name: 'trade_grades', description: 'Return a grade for each trade.', input_schema: { type: 'object', required: ['grades'], properties: {
    grades: { type: 'array', items: { type: 'object', required: ['i', 'grade', 'why'], properties: { i: { type: 'integer' }, grade: { type: 'string', enum: ['A', 'B', 'C', 'none'], description: '"none" when the entry is unknown' }, why: { type: 'string' } } } },
  } } };
  const res = await fetch('https://api.anthropic.com/v1/messages', { method: 'POST', headers: { 'content-type': 'application/json', 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: ANTHROPIC_MODEL, max_tokens: 6000, system, tools: [tool], tool_choice: { type: 'tool', name: tool.name }, messages: [{ role: 'user', content: list }] }) });
  if (!res.ok) { console.error('grade model', res.status); return closed.map(t => ({ ...t, grade: null, why: '' })); }
  const data = await res.json();
  const j = (data.content || []).find(b => b.type === 'tool_use' && b.name === tool.name)?.input;
  if (!j || !Array.isArray(j.grades)) { console.error('grade: no tool output, stop_reason', data.stop_reason); return closed.map(t => ({ ...t, grade: null, why: '' })); }
  const by = new Map((j.grades || []).map(g => [g.i, g]));
  return closed.map((t, i) => ({ ...t, grade: ['A', 'B', 'C'].includes(by.get(i)?.grade) ? by.get(i).grade : null, why: String(by.get(i)?.why || '') }));
}

// ---------------------------------------------------------------------------
// Portfolio (read-only): balances, value history, returns, orders, activity, AI explain.
// Everything is fetched live from SnapTrade/Finnhub and never written to the database.
// ---------------------------------------------------------------------------
const nz = v => (v == null || v === '' || !isFinite(Number(v)) ? null : Number(v));
const isoDay = (daysFromNow = 0) => new Date(Date.now() + daysFromNow * 86400000).toISOString().slice(0, 10);

async function accountSummaries(user) {
  const accts = await listAccounts(user);
  return Promise.all(accts.map(async a => {
    const currency = a.balance?.total?.currency || 'USD';
    let cash = null, buyingPower = null;
    try {
      const b = (await snap.accountInformation.getUserAccountBalance({ ...creds(user), accountId: a.id })).data || [];
      const row = b.find(x => (x.currency?.code || '') === currency) || b[0];
      if (row) { cash = nz(row.cash); buyingPower = nz(row.buying_power); }
    } catch (e) { console.log('balance unavailable', e.response?.status); }
    return { id: a.id, name: a.name || '', institution: a.institution_name || '', total: nz(a.balance?.total?.amount), cash, buyingPower, currency };
  }));
}
const sumOrNull = xs => (xs.length && xs.every(v => v != null) ? xs.reduce((s, v) => s + v, 0) : null);

// Daily account value. With several accounts, only days where every account has a value are summed.
async function valueHistory(user, accts) {
  const byDate = new Map();
  for (const a of accts) {
    try {
      const h = (await snap.accountInformation.getAccountBalanceHistory({ ...creds(user), accountId: a.id })).data?.history || [];
      for (const p of h) { const v = nz(p.total_value), d = String(p.date || '').slice(0, 10); if (v == null || !d) continue; const e = byDate.get(d) || { value: 0, n: 0 }; e.value += v; e.n++; byDate.set(d, e); }
    } catch (e) { console.log('value history unavailable', e.response?.status); return []; }
  }
  return [...byDate.entries()].filter(([, e]) => e.n === accts.length).sort(([a], [b]) => (a < b ? -1 : 1)).map(([date, e]) => ({ date, value: Math.round(e.value * 100) / 100 }));
}
async function returnRates(user, accountId) {
  try {
    const d = (await snap.accountInformation.getUserAccountReturnRates({ ...creds(user), accountId })).data?.data || [];
    const out = {}; for (const r of d) if (r.timeframe && typeof r.return_percent === 'number') out[r.timeframe] = r.return_percent;
    return Object.keys(out).length ? out : null;
  } catch (e) { console.log('return rates unavailable', e.response?.status); return null; }
}

const OPEN_ORDER = new Set(['PENDING', 'ACCEPTED', 'PARTIAL', 'CANCEL_PENDING', 'REPLACE_PENDING', 'QUEUED', 'TRIGGERED', 'ACTIVATED', 'PENDING_RISK_REVIEW', 'CONTINGENT_ORDER']);
async function listOrders(user) {
  const accts = await listAccounts(user), out = [];
  for (const a of accts) {
    try {
      const r = await snap.accountInformation.getUserAccountOrders({ ...creds(user), accountId: a.id, state: 'all', days: 90 });
      for (const o of r.data || []) out.push({
        symbol: String(o.universal_symbol?.symbol || o.option_symbol?.ticker || o.quote_universal_symbol?.symbol || '').toUpperCase(),
        action: String(o.action || '').toUpperCase(), type: String(o.order_type || ''), status: String(o.status || ''),
        qty: nz(o.total_quantity), filled: nz(o.filled_quantity), limit: nz(o.limit_price), stop: nz(o.stop_price), price: nz(o.execution_price),
        tif: String(o.time_in_force || ''), placed: o.time_placed || null, updated: o.time_updated || o.time_executed || null,
        account: a.name || a.institution_name || '', option: !!o.option_symbol,
      });
    } catch (e) { console.error('orders', e.response?.status, e.message); }
  }
  out.sort((x, y) => String(y.placed || '').localeCompare(String(x.placed || '')));
  return { open: out.filter(o => OPEN_ORDER.has(o.status)), history: out.filter(o => !OPEN_ORDER.has(o.status)).slice(0, 60) };
}

async function activitySummary(user) {
  const acts = await accountActivities(user);
  const items = acts.map(a => ({
    date: String(a.trade_date || a.settlement_date || '').slice(0, 10), type: String(a.type || '').toUpperCase(), symbol: actSymbol(a),
    amount: nz(a.amount), units: nz(a.units), price: nz(a.price), description: String(a.description || '').slice(0, 120),
  })).filter(x => x.date).sort((x, y) => (x.date < y.date ? 1 : -1));
  const yearAgo = isoDay(-365);
  const divs = items.filter(i => i.type === 'DIVIDEND');
  return { items: items.slice(0, 120), dividends: { last12m: divs.filter(d => d.date >= yearAgo).reduce((s, d) => s + (d.amount || 0), 0), items: divs.slice(0, 40) }, sinceDays: HISTORY_DAYS };
}

// Market context from Finnhub (not user data; cached in memory).
const sectorCache = new Map(), quoteCache = new Map(), newsCache = new Map();
let earningsCache = { at: 0, list: [] };
async function sectorOf(sym) {
  const c = sectorCache.get(sym); if (c && Date.now() - c.at < 24 * 3600e3) return c.sector;
  try {
    const j = await (await fetch(`https://finnhub.io/api/v1/stock/profile2?symbol=${encodeURIComponent(sym)}&token=${FINNHUB_KEY}`)).json();
    const sector = j && j.finnhubIndustry ? String(j.finnhubIndustry) : 'Unclassified';
    sectorCache.set(sym, { at: Date.now(), sector }); return sector;
  } catch { return 'Unclassified'; }
}
async function quoteOf(sym) {
  const c = quoteCache.get(sym); if (c && Date.now() - c.at < 60e3) return c.q;
  try {
    const j = await (await fetch(`https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(sym)}&token=${FINNHUB_KEY}`)).json();
    const q = typeof j.c === 'number' && j.c > 0 ? { price: j.c, prevClose: j.pc, changePct: j.dp } : null;
    quoteCache.set(sym, { at: Date.now(), q }); return q;
  } catch { return null; }
}
async function upcomingEarnings(symbols) {
  if (Date.now() - earningsCache.at > 6 * 3600e3) {
    try {
      const j = await (await fetch(`https://finnhub.io/api/v1/calendar/earnings?from=${isoDay()}&to=${isoDay(30)}&token=${FINNHUB_KEY}`)).json();
      earningsCache = { at: Date.now(), list: (j.earningsCalendar || []).map(e => ({ symbol: String(e.symbol || '').toUpperCase(), date: e.date, hour: e.hour || '', epsEstimate: nz(e.epsEstimate) })) };
    } catch (e) { console.log('earnings calendar unavailable', e.message); }
  }
  const want = new Set(symbols);
  return earningsCache.list.filter(e => want.has(e.symbol)).sort((a, b) => (a.date < b.date ? -1 : 1));
}
async function headlines(sym) {
  const c = newsCache.get(sym); if (c && Date.now() - c.at < 30 * 60e3) return c.list;
  try {
    const j = await (await fetch(`https://finnhub.io/api/v1/company-news?symbol=${encodeURIComponent(sym)}&from=${isoDay(-3)}&to=${isoDay()}&token=${FINNHUB_KEY}`)).json();
    const list = (Array.isArray(j) ? j : []).slice(0, 3).map(n => ({ headline: String(n.headline || '').slice(0, 160), source: String(n.source || ''), date: n.datetime ? new Date(n.datetime * 1000).toISOString().slice(0, 10) : '' })).filter(n => n.headline);
    newsCache.set(sym, { at: Date.now(), list }); return list;
  } catch { return []; }
}

async function portfolioOverview(user) {
  const [accounts, pos] = await Promise.all([accountSummaries(user), listPositions(user)]);
  const symbols = pos.positions.map(p => p.symbol);
  const [sectorList, history, returns, earnings, trades] = await Promise.all([
    Promise.all(symbols.slice(0, 30).map(sectorOf)),
    valueHistory(user, accounts),
    accounts.length === 1 ? returnRates(user, accounts[0].id) : Promise.resolve(null),
    upcomingEarnings(symbols).catch(() => []),
    listTrades(user).catch(() => null),
  ]);
  const sectors = {}; symbols.slice(0, 30).forEach((s, i) => { sectors[s] = sectorList[i]; });
  return {
    connected: accounts.length > 0, broker: accounts[0]?.institution || user.broker_name || null,
    accounts, totals: { value: sumOrNull(accounts.map(a => a.total)), cash: sumOrNull(accounts.map(a => a.cash)), buyingPower: sumOrNull(accounts.map(a => a.buyingPower)), currency: accounts[0]?.currency || 'USD' },
    positions: pos.positions, sectors, history, returns, earnings, realized: trades?.realized || null,
  };
}

async function explainPortfolio(user, question) {
  const ov = await portfolioOverview(user);
  // Live quotes for the 30 largest holdings (Finnhub rate limit); the rest use the broker's price.
  const byEstValue = [...ov.positions].sort((a, b) => (b.brokerPrice || 0) * b.qty - (a.brokerPrice || 0) * a.qty);
  const quoted = new Set(byEstValue.slice(0, 30).map(p => p.symbol));
  const rows = await Promise.all(ov.positions.map(async p => {
    const q = quoted.has(p.symbol) ? await quoteOf(p.symbol) : null; const price = q?.price ?? p.brokerPrice;
    return { ...p, price, prevClose: q?.prevClose ?? null, value: price != null ? price * p.qty : null };
  }));
  const invested = rows.reduce((s, r) => s + (r.value || 0), 0);
  const top = rows.filter(r => r.value != null).sort((a, b) => b.value - a.value);
  const spy = await quoteOf('SPY');
  const news = {}; for (const r of top.slice(0, 3)) news[r.symbol] = await headlines(r.symbol);
  const f = (v, d = 2) => (v == null ? 'n/a' : Number(v).toFixed(d));
  const lines = top.slice(0, 25).map(r => `${r.symbol} | sector ${ov.sectors[r.symbol] || 'n/a'} | ${r.qty} sh | price ${f(r.price)} | prev close ${f(r.prevClose)} | value ${f(r.value, 0)} | weight ${invested ? f(r.value / invested * 100, 1) : 'n/a'}% | avg cost ${f(r.avgCost)} | unrealized ${r.avgCost != null && r.price != null ? f((r.price - r.avgCost) * r.qty, 0) : 'n/a'} | today ${r.prevClose ? f((r.price - r.prevClose) * r.qty, 0) + ' (' + f((r.price / r.prevClose - 1) * 100) + '%)' : 'n/a'}`);
  const context = [
    `Account total value: ${f(ov.totals.value, 0)} ${ov.totals.currency}. Cash: ${f(ov.totals.cash, 0)}. Buying power: ${f(ov.totals.buyingPower, 0)}. Invested in listed holdings: ${f(invested, 0)}.`,
    `Holdings (largest first):\n${lines.join('\n') || 'none'}`,
    `S&P 500 proxy (SPY) today: ${spy?.changePct != null ? f(spy.changePct) + '%' : 'n/a'}.`,
    ov.returns ? `Account returns: ${Object.entries(ov.returns).map(([k, v]) => `${k} ${f(v)}%`).join(', ')}.` : 'Account returns: not available.',
    ov.realized ? `Realized P/L from closed trades visible in the last ${ov.realized.sinceDays} days: ${f(ov.realized.amount, 0)} over ${ov.realized.trades} trades.` : '',
    `Upcoming earnings (30 days) for holdings: ${ov.earnings.map(e => `${e.symbol} ${e.date}${e.hour ? ' ' + e.hour : ''}`).join(', ') || 'none found'}.`,
    `Recent headlines:\n${Object.entries(news).map(([s, l]) => l.map(n => `${s}: ${n.headline} (${n.source}, ${n.date})`).join('\n')).filter(Boolean).join('\n') || 'none'}`,
  ].filter(Boolean).join('\n\n');
  const system = `You explain a retail investor's brokerage portfolio in plain English for education. Use ONLY the numbers and facts provided; never invent prices, news, or events, and say when data is missing. Point out concentration (single holdings over ~20% or sectors over ~35% of invested value), big winners/losers, what drove today's change, upcoming earnings, and how the day compares with SPY. Be balanced and humble; this is not financial advice. Never tell the user to buy, sell, or hold anything; if asked for a recommendation, say you can't give one and explain what in their portfolio is worth thinking about instead.
STYLE: organized and simple. Write for someone new to investing: everyday words, short sentences (under 20 words), no jargon (if a term is unavoidable, explain it in a few words). Use real numbers with $ and %. Every bullet is one idea.
If the user asked a question: answer it directly first (2-3 short sentences), and keep the sections to the 1-3 that help with that question. With no question: cover Today, Biggest movers, Concentration, and Coming up (skip any with nothing to say).
Always answer by calling the portfolio_explanation tool.`;
  const userText = `${context}${question ? `\n\nUser's question: ${String(question).slice(0, 300)}` : ''}`;
  // A forced tool call makes the model return a structured object instead of free text we'd have to parse.
  const tool = { name: 'portfolio_explanation', description: 'Return the portfolio explanation.', input_schema: { type: 'object', required: ['headline', 'answer', 'sections'], properties: {
    headline: { type: 'string', description: "One short sentence (max 15 words), e.g. \"You're up $312 today, mostly from NVDA.\"" },
    answer: { type: 'string', description: "Direct answer to the user's question in 2-3 short sentences, or empty string if there was no question." },
    sections: { type: 'array', description: '1-4 sections.', items: { type: 'object', required: ['title', 'bullets'], properties: {
      title: { type: 'string', enum: ['Today', 'Biggest movers', 'Concentration', 'Coming up', 'What if…', 'Worth knowing'] },
      bullets: { type: 'array', items: { type: 'string' }, description: '1-3 bullets, each one short sentence (max 20 words). "What if…" bullets describe exposure only, never predictions.' },
    } } },
  } } };
  let j = null;
  for (let attempt = 0; attempt < 2 && !j; attempt++) {
    const res = await fetch('https://api.anthropic.com/v1/messages', { method: 'POST', headers: { 'content-type': 'application/json', 'x-api-key': ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({ model: ANTHROPIC_MODEL, max_tokens: 3000, system, tools: [tool], tool_choice: { type: 'tool', name: tool.name }, messages: [{ role: 'user', content: userText }] }) });
    if (!res.ok) { console.error('explain model', res.status); continue; }
    const data = await res.json();
    const call = (data.content || []).find(b => b.type === 'tool_use' && b.name === tool.name);
    if (call && call.input && typeof call.input === 'object') j = call.input; else console.error('explain: no tool output, stop_reason', data.stop_reason);
  }
  if (!j) throw new Error('no explanation');
  const str = v => String(v || '');
  return {
    headline: str(j.headline), answer: str(j.answer), question: question ? String(question).slice(0, 300) : '',
    sections: (Array.isArray(j.sections) ? j.sections : []).slice(0, 4)
      .map(s => ({ title: str(s?.title), bullets: (Array.isArray(s?.bullets) ? s.bullets : []).slice(0, 3).map(str).filter(Boolean) }))
      .filter(s => s.title && s.bullets.length),
    at: Date.now(), engine: ANTHROPIC_MODEL,
  };
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

// Public web pages the App Store needs: /privacy, /terms, /support (files in ./legal). Loaded once at startup.
const fs = require('fs'), path = require('path');
const PAGES = {};
for (const name of ['privacy', 'terms', 'support']) {
  try { PAGES[name] = fs.readFileSync(path.join(__dirname, 'legal', `${name}.html`)); }
  catch (e) { console.error('missing page', name, e.message); }
}
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
  const page = PAGES[url.pathname.replace(/^\/|\.html$|\/$/g, '')];
  if (req.method === 'GET' && page) { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'public, max-age=300' }); return res.end(page); }
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
      if (url.pathname === '/brokerage/overview') {
        if (!user.snap_user_id) return json(res, 200, { connected: false });
        return json(res, 200, await portfolioOverview(user));
      }
      if (url.pathname === '/brokerage/orders') {
        if (!user.snap_user_id) return json(res, 200, { open: [], history: [] });
        return json(res, 200, await listOrders(user));
      }
      if (url.pathname === '/brokerage/activity') {
        if (!user.snap_user_id) return json(res, 200, { items: [], dividends: { last12m: 0, items: [] } });
        return json(res, 200, await activitySummary(user));
      }
      if (url.pathname === '/brokerage/explain' && req.method === 'POST') {
        if (!user.snap_user_id) return json(res, 400, { error: 'no brokerage connected' });
        if (!ANTHROPIC_API_KEY) return json(res, 503, { error: 'ANTHROPIC_API_KEY not set' });
        const usage = await getUsage(deviceId);
        if (usage.textCount >= EXPLAIN_DAILY_LIMIT) return json(res, 429, { error: 'daily limit', detail: `You've reached today's limit of ${EXPLAIN_DAILY_LIMIT} portfolio explanations. It resets at midnight Eastern.` });
        let out;
        try { out = await explainPortfolio(user, body.question); }
        catch (e) { console.error('explain failed', e.message); return json(res, 502, { error: 'explain failed', detail: 'Couldn’t get an explanation right now. Please try again in a moment.' }); }
        await bumpUsage(deviceId, false);
        return json(res, 200, out);
      }
      if (url.pathname === '/brokerage/disconnect' && req.method === 'POST') {
        if (user.snap_user_id) { try { await snap.authentication.deleteSnapTradeUser(creds(user)); } catch (e) { console.error('snap delete', e.message); } }
        await db.query('UPDATE users SET snap_user_id=NULL, snap_user_secret=NULL, broker_name=NULL, connected_at=NULL WHERE device_id=$1', [deviceId]);
        forgetUser(deviceId);
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
