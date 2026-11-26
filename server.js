const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const { createClient } = require('@libsql/client');
const fs = require('fs');
const path = require('path');
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

let dbClient;
let dbReady = false;

try {
 dbClient = createClient({
 url: TURSO_DB_URL,
 authToken: TURSO_AUTH_TOKEN,
 });
} catch (error) {
 console.error('[DB] Turso client creation failed:', error.message);
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
 CREATE TABLE IF NOT EXISTS admins (
 id TEXT PRIMARY KEY,
 port INTEGER,
 last_connected INTEGER,
 status TEXT DEFAULT 'Online'
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

 if (dbReady && dbClient) {
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
// WEBSOCKET SERVER
// =====================================================

const wss = new WebSocket.Server({ noServer: true });

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

 // Register/Update Admin in DB
 if (dbReady && dbClient) {
 const existingAdmin = await queryOne('SELECT * FROM admins WHERE id = ?', [clientId]);
 if (!existingAdmin) {
 await dbClient.execute('INSERT INTO admins (id, port, last_connected, status) VALUES (?, ?, ?, ?)', 
 [clientId, req.socket.remotePort, Date.now(), 'Online']);
 } else {
 await dbClient.execute('UPDATE admins SET port = ?, last_connected = ?, status = ? WHERE id = ?', 
 [req.socket.remotePort, Date.now(), 'Online', clientId]);
 }
 }

 notifyAdmin(clientId, '🛡️ Admin Session Started');
 sendMessage(ws, { type: 'registered', id: clientId });
 broadcastToAdmins(ws, { type: 'refresh_users' });
 return;
 }

 // AGENT INFO — THE CRITICAL FIX
 if (data.type === 'info') {
 if (ws.isAdmin) return;

 const uniqueId = String(data.uniqueId || data.id || '').trim();
 if (!uniqueId || uniqueId.length > 255) {
 logDB('[WARN] Invalid agent unique ID');
 return;
 }

 ws.uniqueId = uniqueId;
 const now = Date.now();

 // Check if this agent already exists in DB
 if (dbClient) {
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
 } else {
 logDB(`[AGENT] NO DB: ${uniqueId} (${data.os || 'unknown'})`);
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
 if (dbClient) {
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
// SERVER STARTUP — VERCEL SERVERLESS FUNCTION
// =====================================================

module.exports = async (req, res) => {
 // Handle WebSocket Upgrade
 const upgradeHeader = req.headers['upgrade'] || '';
 if (upgradeHeader.toLowerCase() === 'websocket') {
 wss.handleUpgrade(req, req.socket, Buffer.from(''), (socket) => {
 wss.emit('connection', socket, req);
 });
 return;
 }

 // Handle HTTP Requests (Health Check, Admin Panel)
 if (req.method === 'GET' && req.url === '/health') {
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
 return;
 }

 // NEW: API Endpoint to get all users from DB
 if (req.method === 'GET' && req.url === '/api/users') {
 try {
 const rows = await queryRows('SELECT * FROM users ORDER BY last_seen DESC');
 res.json(rows);
 } catch (error) {
 res.status(500).json({ error: error.message });
 }
 return;
 }

 // Serve Admin HTML
 if (req.method === 'GET' && (req.url === '/' || req.url === '/admin.html')) {
 try {
 const adminHtmlPath = path.join(__dirname, 'admin.html');
 const content = fs.readFileSync(adminHtmlPath, 'utf8');
 res.setHeader('Content-Type', 'text/html');
 res.send(content);
 } catch (error) {
 res.statusCode = 500;
 res.end('Error loading admin panel');
 }
 return;
 }

 // Default response
 res.statusCode = 404;
 res.end('Not Found');
};

// Start schema check on load
ensureSchema().catch(e => console.error('Initial schema check failed', e));
