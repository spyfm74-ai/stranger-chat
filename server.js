// Stranger chat backend: matching + WebRTC signaling + moderation basics
const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');
const geoip = require('geoip-lite');

const PORT = process.env.PORT || 3000;
const TURN_URL = process.env.TURN_URL || '';
const TURN_SECRET = process.env.TURN_SECRET || '';
// Only these countries may connect (default: Sri Lanka). Localhost/LAN is always allowed for testing.
const ALLOWED = (process.env.ALLOW_COUNTRIES || 'LK').split(',').map((c) => c.trim().toUpperCase());

const app = express();
app.set('trust proxy', true);
app.use(express.static(require('path').join(__dirname, 'public')));
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: process.env.ORIGIN || '*' },
  maxHttpBufferSize: 50000,
});

// ---------- state (in memory: use Redis/DB if you run many servers) ----------
const users = new Map();      // socketId -> user
const queue = [];             // socketIds waiting
const bans = new Map();       // ip -> banned until (ms)
const reports = new Map();    // ip -> [timestamps]
const REPORTS_TO_BAN = 3;
const BAN_MS = 24 * 3600 * 1000;

// ---------- HTTP ----------
app.get('/health', (_, res) => res.json({ ok: true, online: users.size }));

// Short-lived TURN credentials (coturn "use-auth-secret" mode)
app.get('/turn', (_, res) => {
  const iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
  if (TURN_URL && TURN_SECRET) {
    const username = String(Math.floor(Date.now() / 1000) + 3600);
    const credential = crypto.createHmac('sha1', TURN_SECRET).update(username).digest('base64');
    iceServers.push({ urls: TURN_URL, username, credential });
  }
  res.json({ iceServers });
});

// ---------- helpers ----------
const clean = (s, n) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, n);
const isBanned = (ip) => (bans.get(ip) || 0) > Date.now();

function removeFromQueue(id) {
  const i = queue.indexOf(id);
  if (i > -1) queue.splice(i, 1);
}

function compatible(a, b) {
  if (a.lastPartner === b.id || b.lastPartner === a.id) return false;
  if (a.want !== 'any' && a.want !== b.country) return false;
  if (b.want !== 'any' && b.want !== a.country) return false;
  const now = Date.now();
  const common = a.interests.filter((x) => b.interests.includes(x));
  const noPrefs = !a.interests.length || !b.interests.length;
  const waited = now - a.since > 8000 || now - b.since > 8000; // relax after 8s
  return noPrefs || common.length > 0 || waited;
}

function pair(a, b) {
  removeFromQueue(a.id);
  removeFromQueue(b.id);
  const convo = { log: [] };
  a.partner = b.id; b.partner = a.id;
  a.convo = b.convo = convo;
  const common = a.interests.filter((x) => b.interests.includes(x));
  io.to(a.id).emit('matched', { initiator: true, country: b.country, common });
  io.to(b.id).emit('matched', { initiator: false, country: a.country, common });
}

function unpair(user, notify = true) {
  if (!user.partner) return;
  const other = users.get(user.partner);
  user.lastPartner = user.partner;
  user.partner = null;
  user.convo = null;
  if (other) {
    other.lastPartner = user.id;
    other.partner = null;
    other.convo = null;
    if (notify) io.to(other.id).emit('partner_left');
  }
}

function runMatching() {
  for (let i = 0; i < queue.length; i++) {
    const a = users.get(queue[i]);
    if (!a) continue;
    for (let j = i + 1; j < queue.length; j++) {
      const b = users.get(queue[j]);
      if (b && compatible(a, b)) {
        pair(a, b);
        i = -1; // restart scan, queue changed
        break;
      }
    }
  }
}
setInterval(runMatching, 2000);
setInterval(() => io.emit('online', users.size), 5000);

// ---------- sockets ----------
io.on('connection', (socket) => {
  // Real client IP (behind Render/Railway/nginx the socket IP is the proxy's)
  const xff = String(socket.handshake.headers['x-forwarded-for'] || '').split(',')[0].trim();
  const ip = xff || socket.handshake.address;
  const isLocal = /^(::1|127\.|::ffff:127\.|10\.|192\.168\.|::ffff:192\.168\.)/.test(ip);
  const geoCheck = geoip.lookup(ip);
  if (!isLocal && !ALLOWED.includes(geoCheck ? geoCheck.country : 'XX')) {
    socket.emit('blocked');
    return socket.disconnect(true);
  }
  if (isBanned(ip)) {
    socket.emit('banned', { until: bans.get(ip) });
    return socket.disconnect(true);
  }
  const geo = geoip.lookup(ip);
  const user = {
    id: socket.id, ip, country: geo ? geo.country : 'XX',
    interests: [], want: 'any', since: 0,
    partner: null, lastPartner: null, convo: null,
    msgTimes: [], adult: false,
  };
  users.set(socket.id, user);
  socket.emit('hello', { country: user.country, online: users.size });

  socket.on('join_queue', (opts = {}) => {
    if (opts.adult !== true) return socket.emit('error_msg', '18+ confirmation ඕනේ');
    if (isBanned(ip)) return socket.emit('banned', { until: bans.get(ip) });
    unpair(user);
    removeFromQueue(user.id);
    user.adult = true;
    user.interests = (Array.isArray(opts.interests) ? opts.interests : [])
      .slice(0, 5).map((s) => clean(s, 20).toLowerCase()).filter(Boolean);
    const want = clean(opts.country, 2).toUpperCase();
    user.want = /^[A-Z]{2}$/.test(want) ? want : 'any';
    user.since = Date.now();
    queue.push(user.id);
    socket.emit('waiting');
    runMatching();
  });

  // WebRTC signaling relay (offer / answer / ice)
  socket.on('signal', (data) => {
    if (!user.partner) return;
    if (JSON.stringify(data || {}).length > 20000) return;
    io.to(user.partner).emit('signal', data);
  });

  socket.on('message', (text) => {
    if (!user.partner) return;
    const now = Date.now();
    user.msgTimes = user.msgTimes.filter((t) => now - t < 3000);
    if (user.msgTimes.length >= 5) return socket.emit('error_msg', 'හෙමින් යවන්න');
    user.msgTimes.push(now);
    const t = clean(text, 500);
    if (!t) return;
    if (user.convo) {
      user.convo.log.push({ from: user.id, t, at: now });
      if (user.convo.log.length > 30) user.convo.log.shift();
    }
    io.to(user.partner).emit('message', t);
  });

  socket.on('next', () => {
    unpair(user);
    socket.emit('left');
  });

  socket.on('leave', () => {
    unpair(user);
    removeFromQueue(user.id);
    socket.emit('left');
  });

  socket.on('report', (reason) => {
    const other = users.get(user.partner);
    if (!other) return;
    const list = (reports.get(other.ip) || []).filter((t) => Date.now() - t < BAN_MS);
    list.push(Date.now());
    reports.set(other.ip, list);
    // Evidence for manual review (store in a DB in production)
    console.log('REPORT', JSON.stringify({
      reportedIp: other.ip, by: user.ip, reason: clean(reason, 100),
      log: user.convo ? user.convo.log : [],
    }));
    if (list.length >= REPORTS_TO_BAN) {
      bans.set(other.ip, Date.now() + BAN_MS);
      io.to(other.id).emit('banned', { until: bans.get(other.ip) });
      io.sockets.sockets.get(other.id)?.disconnect(true);
    }
    unpair(user, false);
    socket.emit('left');
  });

  socket.on('disconnect', () => {
    unpair(user);
    removeFromQueue(user.id);
    users.delete(user.id);
  });
});

server.listen(PORT, () => console.log('Stranger backend on :' + PORT));
