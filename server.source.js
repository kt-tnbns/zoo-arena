// Wildlife Arena — room + lockstep turn server. No database: rooms live in memory.
// Every client runs the same deterministic resolve() with the same orders;
// the server only collects orders, runs the planning timer and checks for desync.
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { WebSocketServer } = require('ws');

// load .env next to the server (Node 20.12+ built-in, no dependency). Real env vars win.
try { process.loadEnvFile(path.join(__dirname, '.env')); } catch {}

const PORT = process.env.PORT || 3000;
const PLAN_SECS = 40;
// abuse limits (override with env vars when deploying)
const MAX_ROOMS = +process.env.MAX_ROOMS || 200;        // rooms alive at once
const MAX_CONN_PER_IP = +process.env.MAX_CONN_PER_IP || 12;
const ROOMS_PER_IP_PER_MIN = +process.env.ROOMS_PER_IP_PER_MIN || 6;
const MSG_RATE = 20, MSG_BURST = 40;                     // per socket token bucket          // planning time per round
const RESOLVE_TIMEOUT = 25000;
const DRAFT_SECS = 30;          // everyone picks their pride at the same time // max wait for every client to finish the replay
const SEATS = 4;
const POOL = ['rhino', 'owl', 'chameleon', 'turtle', 'mouse'];
const SEAT_ORDER = [0, 2, 1, 3]; // 2 players sit in opposite corners
const activeSeats = room => SEAT_ORDER.slice(0, room.cfg.size).sort();
const DEF_CFG = () => ({ size: 4, mode: 'ffa', team: 4, dup: 2, limit: 2, rounds: 8, public: true });
const CFG_OK = { size: [2, 3, 4], mode: ['ffa', 'team'], team: [3, 4, 5, 6], dup: [1, 2, 3, 9], limit: [1, 2, 3, 9], rounds: [5, 8, 12, 20, 0], public: [true, false] };

function lanIP() {
  for (const ifaces of Object.values(os.networkInterfaces())) {
    for (const i of ifaces || []) if (i.family === 'IPv4' && !i.internal) return i.address;
  }
  return null;
}
const INDEX = path.join(__dirname, 'index.html');
// analytics tags injected into <!--ANALYTICS--> from env (nothing is added when unset)
const attr = v => String(v).replace(/[&"<>]/g, c => ({ '&': '&amp;', '"': '&quot;', '<': '&lt;', '>': '&gt;' }[c]));
function analyticsTags() {
  const base = (process.env.ANALYTICS_URL || '').replace(/\/+$/, ''), id = process.env.ANALYTICS_WEBSITE_ID || '';
  if (!base || !id) return '';
  const tag = file => `<script defer src="${attr(base + '/' + file)}" data-website-id="${attr(id)}"></script>`;
  let out = tag(process.env.ANALYTICS_SCRIPT || 'script.js');
  if (/^(1|true|yes|on)$/i.test(process.env.ANALYTICS_RECORDER || '')) out += '\n' + tag(process.env.ANALYTICS_RECORDER_SCRIPT || 'recorder.js');
  return out;
}
const ANALYTICS = analyticsTags();
const server = http.createServer((req, res) => {
  fs.readFile(INDEX, 'utf8', (err, html) => {
    if (err) { res.writeHead(500); return res.end('index.html not found'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(html.replace('<!--ANALYTICS-->', ANALYTICS));
  });
});
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

/** room: { code, host, phase:'lobby'|'plan'|'resolve', round, seats:[{name,human,picks}]|null,
 *          players: Map<id,{ws,name,seat,picks}>, orders, ready:Set, botsIn, done:Set, hashes, timer, over } */
const rooms = new Map();
let nextId = 1;
const CODE_CHARS = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
function makeCode() {
  let c;
  do { c = ''; for (let i = 0; i < 5; i++) c += CODE_CHARS[(Math.random() * CODE_CHARS.length) | 0]; } while (rooms.has(c));
  return c;
}
function randomPicks(cfg) {
  const n = {}, o = [];
  while (o.length < cfg.team - 1) { const t = POOL[(Math.random() * POOL.length) | 0]; if ((n[t] || 0) < cfg.dup) { n[t] = (n[t] || 0) + 1; o.push(t); } }
  return o;
}
function validPicks(p, cfg) {
  if (!Array.isArray(p) || p.length !== cfg.team - 1) return null;
  const n = {};
  for (const t of p) { if (!POOL.includes(t)) return null; n[t] = (n[t] || 0) + 1; if (n[t] > cfg.dup) return null; }
  return p.slice();
}
const AVS = ['🦁', '🦏', '🦉', '🦎', '🐢', '🐭', '🐯', '🦊', '🐼', '🐸', '🐺', '🦜'];
const cleanName = n => String(n || 'ผู้เล่น').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 12) || 'ผู้เล่น';
const cleanAv = a => AVS.includes(a) ? a : '🦁';
const sysChat = (room, text) => broadcast(room, { t: 'chat', sys: true, text });
const ipOf = req => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
const connPerIp = new Map(), createLog = new Map();
const send = (ws, m) => { if (ws.readyState === 1) ws.send(JSON.stringify(m)); };
const broadcast = (room, m) => { const s = JSON.stringify(m); for (const p of room.players.values()) if (p.ws.readyState === 1) p.ws.send(s); };
function roomInfo(room) {
  return {
    t: 'room', code: room.code, phase: room.phase, cfg: room.cfg,
    name: room.name, players: [...room.players].map(([id, p]) => ({ id, name: p.name, av: p.av, seat: p.seat, host: id === room.host, picked: !!p.picks, ready: !!p.ready })),
  };
}
const humanSeats = room => [...room.players.values()].map(p => p.seat).filter(s => room.seats && room.seats[s] && room.seats[s].human);
const botSeats = room => room.seats ? room.seats.map((s, i) => (!s.human && s.active ? i : -1)).filter(i => i >= 0) : [];

function checkLobbyReady(room) {
  if (room.phase !== 'lobby' || !room.players.size) return;
  if ([...room.players.values()].every(p => p.ready)) beginDraft(room);
}
function beginDraft(room) {
  room.phase = 'draft';
  for (const p of room.players.values()) if (p.picks && !validPicks(p.picks, room.cfg)) p.picks = null;
  for (const p of room.players.values()) p.picked = false;
  broadcast(room, roomInfo(room));
  broadcast(room, { t: 'draft', secs: DRAFT_SECS, cfg: room.cfg });
  clearTimeout(room.timer);
  room.timer = setTimeout(() => startGame(room), DRAFT_SECS * 1000 + 800);
}
function checkDraft(room) {
  if (room.phase === 'draft' && [...room.players.values()].every(p => p.picked)) startGame(room);
}
function startGame(room) {
  if (room.phase !== 'draft') return;
  clearTimeout(room.timer);
  const bySeat = {};
  for (const p of room.players.values()) { bySeat[p.seat] = p; p.ready = false; }
  const act = activeSeats(room);
  room.seats = [0, 1, 2, 3].map(s => !act.includes(s) ? { name: '-', human: false, active: false, picks: [] }
    : bySeat[s] ? { name: bySeat[s].name, human: true, active: true, picks: bySeat[s].picks || randomPicks(room.cfg) }
    : { name: 'บอท', human: false, active: true, picks: randomPicks(room.cfg) });
  room.round = 1;
  room.phase = 'starting';
  broadcast(room, { t: 'start', seats: room.seats, cfg: room.cfg });
  room.timer = setTimeout(() => { if (rooms.get(room.code) === room) beginPlan(room); }, 300);
}
function beginPlan(room) {
  room.phase = 'plan'; room.orders = {}; room.ready = new Set(); room.botsIn = botSeats(room).length === 0;
  broadcast(room, { t: 'plan', round: room.round, secs: PLAN_SECS, bots: botSeats(room) });
  clearTimeout(room.timer);
  room.timer = setTimeout(() => resolveNow(room), PLAN_SECS * 1000 + 800);
  readyState(room);
}
function readyState(room) {
  const hs = humanSeats(room);
  broadcast(room, { t: 'readyState', ready: [...room.ready], humans: hs });
  if (room.phase === 'plan' && room.botsIn && hs.length && hs.every(s => room.ready.has(s))) resolveNow(room);
}
function resolveNow(room) {
  if (room.phase !== 'plan') return;
  clearTimeout(room.timer);
  room.phase = 'resolve'; room.done = new Set(); room.hashes = {}; room.over = false;
  broadcast(room, { t: 'resolve', round: room.round, orders: room.orders });
  room.timer = setTimeout(() => afterResolve(room), RESOLVE_TIMEOUT);
}
function checkDone(room) {
  if (room.phase !== 'resolve') return;
  if (humanSeats(room).every(s => room.done.has(s))) afterResolve(room);
}
function afterResolve(room) {
  if (room.phase !== 'resolve') return;
  clearTimeout(room.timer);
  const hs = new Set(Object.values(room.hashes));
  if (hs.size > 1) broadcast(room, { t: 'desync' });
  if (room.over) { room.phase = 'lobby'; room.seats = null; for (const p of room.players.values()) p.ready = false; broadcast(room, roomInfo(room)); return; }
  room.round++;
  beginPlan(room);
}

wss.on('connection', (ws, req) => {
  const clientIp = ipOf(req);
  if ((connPerIp.get(clientIp) || 0) >= MAX_CONN_PER_IP) { ws.close(1013, 'too many connections'); return; }
  connPerIp.set(clientIp, (connPerIp.get(clientIp) || 0) + 1);
  ws.on('close', () => { const n = (connPerIp.get(clientIp) || 1) - 1; if (n > 0) connPerIp.set(clientIp, n); else connPerIp.delete(clientIp); });
  let tokens = MSG_BURST, last = Date.now(), strikes = 0;
  const id = nextId++;
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('error', () => {}); // never let one bad socket crash the server
  let code = null;
  ws.on('message', data => {
    const now = Date.now(); tokens = Math.min(MSG_BURST, tokens + (now - last) / 1000 * MSG_RATE); last = now;
    if (tokens < 1) { if (++strikes > 200) ws.close(1008, 'rate limit'); return; }
    tokens -= 1;
    let m; try { m = JSON.parse(data); } catch { return; }
    if (!m || typeof m !== 'object') return;
    const room = code ? rooms.get(code) : null;
    const me = room && room.players.get(id);
    switch (m.t) {
      case 'list': { // public rooms waiting for players
        const list = [];
        for (const r of rooms.values()) {
          if (!r.cfg.public) continue;
          const playing = r.phase !== 'lobby';
          if (!playing && r.players.size >= r.cfg.size) continue;
          list.push({ code: r.code, name: r.name, players: r.players.size, size: r.cfg.size, mode: r.cfg.mode, team: r.cfg.team, limit: r.cfg.limit, rounds: r.cfg.rounds,
            avs: [...r.players.values()].map(p => p.av), playing, round: r.round });
        }
        list.sort((a, b) => (a.playing - b.playing) || (b.players - a.players));
        send(ws, { t: 'rooms', rooms: list.slice(0, 50), online: wss.clients.size, total: rooms.size });
        break;
      }
      case 'create': {
        if (code) return;
        if (rooms.size >= MAX_ROOMS) return send(ws, { t: 'err', msg: 'server เต็ม ลองใหม่อีกสักครู่' });
        const recent = (createLog.get(clientIp) || []).filter(t => Date.now() - t < 60000);
        if (recent.length >= ROOMS_PER_IP_PER_MIN) return send(ws, { t: 'err', msg: 'สร้างห้องถี่เกินไป รอสักนาที' });
        recent.push(Date.now()); createLog.set(clientIp, recent);
        code = makeCode();
        const r = { code, host: id, phase: 'lobby', round: 1, cfg: DEF_CFG(), seats: null, players: new Map(), timer: null };
        r.players.set(id, { ws, name: cleanName(m.name), av: cleanAv(m.av), seat: 0, picks: null });
        r.name = `ห้องของ ${cleanName(m.name)}`;
        rooms.set(code, r);
        const ip = lanIP();
        send(ws, { t: 'hello', id, code, lan: ip ? `http://${ip}:${PORT}` : null });
        broadcast(r, roomInfo(r));
        break;
      }
      case 'quick': {
        if (code) return;
        let best = null;
        for (const r of rooms.values()) if (r.phase === 'lobby' && r.cfg.public && r.players.size < r.cfg.size && (!best || r.players.size > best.players.size)) best = r;
        if (!best) { m.t = 'create'; return ws.emit('message', JSON.stringify(m)); }
        m.code = best.code;
      } // falls through to join
      case 'join': {
        if (code) return;
        const want = String(m.code || '').toUpperCase().trim();
        const r = rooms.get(want);
        if (!r) return send(ws, { t: 'err', msg: 'ไม่พบห้องนี้' });
        if (r.phase !== 'lobby') return send(ws, { t: 'err', msg: 'ห้องนี้กำลังเล่นอยู่' });
        const used = new Set([...r.players.values()].map(p => p.seat));
        const seat = SEAT_ORDER.slice(0, r.cfg.size).find(s => !used.has(s));
        if (seat === undefined) return send(ws, { t: 'err', msg: `ห้องเต็มแล้ว (${r.cfg.size} คน)` });
        code = want;
        r.players.set(id, { ws, name: cleanName(m.name), av: cleanAv(m.av), seat, picks: null });
        sysChat(r, `${cleanName(m.name)} เข้าห้อง`);
        const ip = lanIP();
        send(ws, { t: 'hello', id, code, lan: ip ? `http://${ip}:${PORT}` : null });
        broadcast(r, roomInfo(r));
        break;
      }
      case 'cfg': { // host-only match settings
        if (!room || room.host !== id || room.phase !== 'lobby') return;
        const k = String(m.k), v = m.v;
        if (!CFG_OK[k] || !CFG_OK[k].includes(v)) return;
        const c = { ...room.cfg, [k]: v };
        if (k === 'mode' && v === 'team') c.size = 4;
        if (k === 'size' && v !== 4 && c.mode === 'team') c.mode = 'ffa';
        const act = SEAT_ORDER.slice(0, c.size);
        if ([...room.players.values()].some(p => !act.includes(p.seat))) return send(ws, { t: 'err', msg: 'มีผู้เล่นมากกว่าจำนวนฝูงที่เลือก' });
        room.cfg = c;
        for (const p of room.players.values()) p.ready = false;
        if (k === 'team' || k === 'dup') for (const p of room.players.values()) if (p.picks && !validPicks(p.picks, c)) p.picks = null;
        broadcast(room, roomInfo(room));
        break;
      }
      case 'profile': {
        if (!me) return;
        me.name = cleanName(m.name); me.av = cleanAv(m.av);
        if (room.phase === 'lobby') broadcast(room, roomInfo(room));
        break;
      }
      case 'rname': {
        if (!room || room.host !== id) return;
        room.name = String(m.name || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 24) || room.name;
        broadcast(room, roomInfo(room));
        break;
      }
      case 'chat': {
        if (!me) return;
        const now = Date.now();
        if (now - (me.lastChat || 0) < 700) return;
        me.lastChat = now;
        const text = String(m.text || '').replace(/[\u0000-\u001f]/g, '').trim().slice(0, 120);
        if (text) broadcast(room, { t: 'chat', id, name: me.name, av: me.av, seat: me.seat, text });
        break;
      }
      case 'kick': {
        if (!room || room.host !== id || room.phase !== 'lobby') return;
        const tgt = room.players.get(m.id | 0);
        if (!tgt || (m.id | 0) === id) return;
        send(tgt.ws, { t: 'kicked' });
        setTimeout(() => tgt.ws.close(4001, 'kicked'), 50);
        break;
      }
      case 'picks': {
        if (!me || (room.phase !== 'lobby' && room.phase !== 'draft')) return;
        me.picks = validPicks(m.picks, room.cfg);
        if (room.phase === 'draft' && me.picks) me.picked = true;
        broadcast(room, roomInfo(room));
        checkDraft(room);
        break;
      }
      case 'lready': { // lobby ready toggle — when everyone is ready, the whole room goes to draft together
        if (!me || room.phase !== 'lobby') return;
        me.ready = !!m.v;
        broadcast(room, roomInfo(room));
        checkLobbyReady(room);
        break;
      }
      case 'seat': { // move to another empty seat (pick your team in 2v2)
        if (!me || room.phase !== 'lobby') return;
        const want = m.seat | 0, act = activeSeats(room);
        if (!act.includes(want) || [...room.players.values()].some(p => p.seat === want)) return;
        me.seat = want; me.ready = false;
        broadcast(room, roomInfo(room));
        break;
      }
      case 'start': { // host can force the draft to begin without waiting for ready
        if (!room || room.host !== id || room.phase !== 'lobby') return;
        beginDraft(room);
        break;
      }
      case 'orders': {
        if (!me || room.phase !== 'plan' || m.round !== room.round) return;
        room.orders[me.seat] = m.orders && typeof m.orders === 'object' ? m.orders : {};
        break;
      }
      case 'ready': {
        if (!me || room.phase !== 'plan' || m.round !== room.round) return;
        if (m.ready) room.ready.add(me.seat); else room.ready.delete(me.seat);
        readyState(room);
        break;
      }
      case 'bot': { // host computes orders for bot seats (deterministic state => same for everyone)
        if (!room || room.host !== id || room.phase !== 'plan' || m.round !== room.round) return;
        for (const s of botSeats(room)) room.orders[s] = (m.orders && m.orders[s]) || {};
        room.botsIn = true;
        readyState(room);
        break;
      }
      case 'done': {
        if (!me || room.phase !== 'resolve' || m.round !== room.round) return;
        room.done.add(me.seat);
        room.hashes[me.seat] = String(m.hash || '');
        if (m.over) room.over = true;
        checkDone(room);
        break;
      }
    }
  });
  ws.on('close', () => {
    if (!code) return;
    const room = rooms.get(code);
    if (!room) return;
    const p = room.players.get(id);
    room.players.delete(id);
    if (room.players.size === 0) { clearTimeout(room.timer); rooms.delete(code); return; }
    if (room.host === id) room.host = room.players.keys().next().value;
    if (p && room.phase === 'lobby') sysChat(room, `${p.name} ออกจากห้อง`);
    if (room.seats && p) { // mid-game: the seat is taken over by a bot
      room.seats[p.seat].human = false;
      broadcast(room, { t: 'seatBot', seat: p.seat, name: p.name });
      if (room.phase === 'plan') room.botsIn = false;
    }
    broadcast(room, roomInfo(room));
    if (room.phase === 'plan') readyState(room);
    if (room.phase === 'resolve') checkDone(room);
    if (room.phase === 'lobby') checkLobbyReady(room);
    if (room.phase === 'draft') checkDraft(room);
  });
});

// heartbeat: keeps idle LAN connections open and drops dead ones (laptop asleep, Wi-Fi lost)
setInterval(() => { // forget old room-creation timestamps
  for (const [k, v] of createLog) if (!v.some(t => Date.now() - t < 60000)) createLog.delete(k);
}, 60000);
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    try { ws.ping(); } catch {}
  }
}, 15000);
process.on('uncaughtException', e => console.error('error:', e && e.message));

server.listen(PORT, () => {
  console.log('Wildlife Arena');
  console.log('  Local:    http://localhost:' + PORT);
  const ip = lanIP();
  if (ip) console.log('  Network:  http://' + ip + ':' + PORT + '   <- share this on your Wi-Fi (LAN mode)');
  console.log('  Analytics: ' + (ANALYTICS ? process.env.ANALYTICS_URL + (/recorder/.test(ANALYTICS) ? ' (+ recorder)' : '') : 'off'));
});
