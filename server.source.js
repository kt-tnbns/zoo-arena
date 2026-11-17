// Zoo Arena — room + lockstep turn server. No database: rooms live in memory.
// Every client runs the same deterministic resolve() with the same orders;
// the server only collects orders, runs the planning timer and checks for desync.
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PLAN_SECS = 40;          // planning time per round
const RESOLVE_TIMEOUT = 25000; // max wait for every client to finish the replay
const SEATS = 4;
const POOL = ['rhino', 'owl', 'chameleon', 'turtle', 'mouse'];
const SEAT_ORDER = [0, 2, 1, 3]; // 2 players sit in opposite corners
const activeSeats = room => SEAT_ORDER.slice(0, room.size).sort();

function lanIP() {
  for (const ifaces of Object.values(os.networkInterfaces())) {
    for (const i of ifaces || []) if (i.family === 'IPv4' && !i.internal) return i.address;
  }
  return null;
}
const INDEX = path.join(__dirname, 'index.html');
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  fs.createReadStream(INDEX).pipe(res);
});
const wss = new WebSocketServer({ server });

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
function randomPicks() {
  const n = {}, o = [];
  while (o.length < 3) { const t = POOL[(Math.random() * POOL.length) | 0]; if ((n[t] || 0) < 2) { n[t] = (n[t] || 0) + 1; o.push(t); } }
  return o;
}
function validPicks(p) {
  if (!Array.isArray(p) || p.length !== 3) return null;
  const n = {};
  for (const t of p) { if (!POOL.includes(t)) return null; n[t] = (n[t] || 0) + 1; if (n[t] > 2) return null; }
  return p.slice();
}
const send = (ws, m) => { if (ws.readyState === 1) ws.send(JSON.stringify(m)); };
const broadcast = (room, m) => { const s = JSON.stringify(m); for (const p of room.players.values()) if (p.ws.readyState === 1) p.ws.send(s); };
function roomInfo(room) {
  return {
    t: 'room', code: room.code, phase: room.phase, size: room.size, limit: room.limit, rounds: room.rounds,
    players: [...room.players].map(([id, p]) => ({ id, name: p.name, seat: p.seat, host: id === room.host, picked: !!p.picks })),
  };
}
const humanSeats = room => [...room.players.values()].map(p => p.seat).filter(s => room.seats && room.seats[s] && room.seats[s].human);
const botSeats = room => room.seats ? room.seats.map((s, i) => (!s.human && s.active ? i : -1)).filter(i => i >= 0) : [];

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
  if (room.over) { room.phase = 'lobby'; room.seats = null; broadcast(room, roomInfo(room)); return; }
  room.round++;
  beginPlan(room);
}

wss.on('connection', ws => {
  const id = nextId++;
  ws.alive = true;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('error', () => {}); // never let one bad socket crash the server
  let code = null;
  ws.on('message', data => {
    let m; try { m = JSON.parse(data); } catch { return; }
    if (!m || typeof m !== 'object') return;
    const room = code ? rooms.get(code) : null;
    const me = room && room.players.get(id);
    switch (m.t) {
      case 'create': {
        if (code) return;
        code = makeCode();
        const r = { code, host: id, phase: 'lobby', round: 1, size: 4, limit: 2, rounds: 8, seats: null, players: new Map(), timer: null };
        r.players.set(id, { ws, name: String(m.name || 'ผู้เล่น').slice(0, 12), seat: 0, picks: null });
        rooms.set(code, r);
        const ip = lanIP();
        send(ws, { t: 'hello', id, code, lan: ip ? `http://${ip}:${PORT}` : null });
        broadcast(r, roomInfo(r));
        break;
      }
      case 'join': {
        if (code) return;
        const want = String(m.code || '').toUpperCase().trim();
        const r = rooms.get(want);
        if (!r) return send(ws, { t: 'err', msg: 'ไม่พบห้องนี้' });
        if (r.phase !== 'lobby') return send(ws, { t: 'err', msg: 'ห้องนี้กำลังเล่นอยู่' });
        const used = new Set([...r.players.values()].map(p => p.seat));
        const seat = SEAT_ORDER.slice(0, r.size).find(s => !used.has(s));
        if (seat === undefined) return send(ws, { t: 'err', msg: `ห้องเต็มแล้ว (${r.size} คน)` });
        code = want;
        r.players.set(id, { ws, name: String(m.name || 'ผู้เล่น').slice(0, 12), seat, picks: null });
        const ip = lanIP();
        send(ws, { t: 'hello', id, code, lan: ip ? `http://${ip}:${PORT}` : null });
        broadcast(r, roomInfo(r));
        break;
      }
      case 'cfg': { // host picks 2 / 3 / 4 teams
        if (!room || room.host !== id || room.phase !== 'lobby') return;
        if (m.rounds !== undefined) { room.rounds = [5, 8, 12, 20].includes(m.rounds | 0) ? (m.rounds | 0) : 8; broadcast(room, roomInfo(room)); break; }
        if (m.limit !== undefined) { room.limit = [1, 2, 3, 9].includes(m.limit | 0) ? (m.limit | 0) : 2; broadcast(room, roomInfo(room)); break; }
        const size = [2, 3, 4].includes(m.size | 0) ? (m.size | 0) : 4;
        const act = SEAT_ORDER.slice(0, size);
        if ([...room.players.values()].some(p => !act.includes(p.seat))) return send(ws, { t: 'err', msg: 'มีผู้เล่นมากกว่าจำนวนฝูงที่เลือก' });
        room.size = size;
        broadcast(room, roomInfo(room));
        break;
      }
      case 'picks': {
        if (!me || room.phase !== 'lobby') return;
        me.picks = validPicks(m.picks);
        broadcast(room, roomInfo(room));
        break;
      }
      case 'start': {
        if (!room || room.host !== id || room.phase !== 'lobby') return;
        const bySeat = {};
        for (const p of room.players.values()) bySeat[p.seat] = p;
        const act = activeSeats(room);
        room.seats = [0, 1, 2, 3].map(s => !act.includes(s) ? { name: '-', human: false, active: false, picks: [] }
          : bySeat[s] ? { name: bySeat[s].name, human: true, active: true, picks: bySeat[s].picks || randomPicks() }
          : { name: 'บอท', human: false, active: true, picks: randomPicks() });
        room.round = 1;
        room.phase = 'starting';
        broadcast(room, { t: 'start', seats: room.seats, limit: room.limit, rounds: room.rounds });
        setTimeout(() => { if (rooms.get(code) === room) beginPlan(room); }, 300);
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
    if (room.seats && p) { // mid-game: the seat is taken over by a bot
      room.seats[p.seat].human = false;
      broadcast(room, { t: 'seatBot', seat: p.seat, name: p.name });
      if (room.phase === 'plan') room.botsIn = false;
    }
    broadcast(room, roomInfo(room));
    if (room.phase === 'plan') readyState(room);
    if (room.phase === 'resolve') checkDone(room);
  });
});

// heartbeat: keeps idle LAN connections open and drops dead ones (laptop asleep, Wi-Fi lost)
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    try { ws.ping(); } catch {}
  }
}, 15000);
process.on('uncaughtException', e => console.error('error:', e && e.message));

server.listen(PORT, () => {
  console.log('Zoo Arena');
  console.log('  Local:    http://localhost:' + PORT);
  const ip = lanIP();
  if (ip) console.log('  Network:  http://' + ip + ':' + PORT + '   <- share this on your Wi-Fi (LAN mode)');
});
