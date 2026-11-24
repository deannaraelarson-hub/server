
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const os = require('os');
const sqlite3 = require('better-sqlite3');
const fs = require('fs');

// =====================================================
// DATABASE SETUP
// =====================================================

// Preserve the database filename: c2.db
//
// Local / VPS / Render:
//   Uses c2.db beside server.js by default.
//
// Vercel:
//   Uses /tmp/c2.db if DB_PATH is not configured.
//   IMPORTANT: Vercel temporary storage is not persistent.
//
// Persistent hosting:
//   Set DB_PATH=/var/data/c2.db (example Render disk).

const DB_PATH =
    process.env.DB_PATH ||
    (
        process.env.VERCEL === '1'
            ? path.join(os.tmpdir(), 'c2.db')
            : path.join(__dirname, 'c2.db')
    );

try {
    fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
} catch (error) {
    console.error('Database directory creation failed:', error);
    process.exit(1);
}

let db;

try {
    db = new sqlite3(DB_PATH);

    // Database configuration
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('foreign_keys = ON');
    db.pragma('busy_timeout = 5000');

    console.log(`[DB] Connected to: ${DB_PATH}`);
} catch (error) {
    console.error('[DB] Database connection failed:', error);
    process.exit(1);
}

// =====================================================
// DATABASE TABLES
// =====================================================

try {
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
} catch (error) {
    console.error('[DB] Table initialization failed:', error);
    process.exit(1);
}

// =====================================================
// LOGGING
// =====================================================

function logDB(msg) {
    const timestamp = Date.now();

    console.log(`[${new Date(timestamp).toISOString()}] ${msg}`);

    try {
        db.prepare(
            'INSERT INTO logs (message, timestamp) VALUES (?, ?)'
        ).run(String(msg), Math.floor(timestamp / 1000));
    } catch (error) {
        console.error('[DB LOG ERROR]', error.message);
    }
}

logDB('Initializing Database...');
logDB('Database initialized successfully.');

// =====================================================
// EXPRESS AND HTTP SERVER
// =====================================================

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
    server,
    perMessageDeflate: false
});

app.disable('x-powered-by');

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        service: 'screen-connect',
        database: 'connected',
        uptime: process.uptime()
    });
});

// Prevent public access to the SQLite database and its WAL files.
app.use((req, res, next) => {
    let requestPath;

    try {
        requestPath = decodeURIComponent(req.path);
    } catch {
        return res.sendStatus(400);
    }

    if (/(^|\/)c2\.db(?:-(?:wal|shm))?$/i.test(requestPath)) {
        return res.sendStatus(404);
    }

    next();
});

// Preserve existing static-file serving.
app.use(express.static(__dirname, {
    dotfiles: 'ignore',
    index: true
}));

// =====================================================
// WEBSOCKET HELPERS
// =====================================================

function sendMessage(ws, data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
        return true;
    }

    return false;
}

function notifyAdmin(adminId, message) {
    for (const client of wss.clients) {
        if (
            client.isAdmin &&
            client.adminId === adminId &&
            client.readyState === WebSocket.OPEN
        ) {
            sendMessage(client, {
                type: 'notification',
                message
            });
        }
    }
}

function broadcastToAdmins(sender, msg) {
    for (const client of wss.clients) {
        if (
            client.isAdmin &&
            client.readyState === WebSocket.OPEN
        ) {
            sendMessage(client, msg);
        }
    }
}

// Find an agent using its actual unique ID,
// not the randomly generated WebSocket connection ID.
function findAgentByUniqueID(uniqueId) {
    for (const client of wss.clients) {
        if (
            !client.isAdmin &&
            client.uniqueId === uniqueId &&
            client.readyState === WebSocket.OPEN
        ) {
            return client;
        }
    }

    return null;
}

function isAgentConnected(uniqueId, excludedClient = null) {
    for (const client of wss.clients) {
        if (
            client !== excludedClient &&
            !client.isAdmin &&
            client.uniqueId === uniqueId &&
            client.readyState === WebSocket.OPEN
        ) {
            return true;
        }
    }

    return false;
}

// =====================================================
// WEBSOCKET CONNECTION HANDLING
// =====================================================

wss.on('connection', (ws, req) => {

    const clientId = uuidv4().substring(0, 8);

    ws.id = clientId;
    ws.isAdmin = false;
    ws.adminId = null;
    ws.uniqueId = null;

    ws.isAlive = true;

    const remoteAddress = req.socket.remoteAddress || 'Unknown';

    logDB(`[+] WebSocket Connected: ${clientId}`);

    // -------------------------------------------------
    // MESSAGE HANDLER
    // -------------------------------------------------

    ws.on('message', (msgRaw) => {

        try {
            const data = JSON.parse(msgRaw.toString());

            if (!data || typeof data !== 'object') {
                return;
            }

            // =============================================
            // ADMIN REGISTRATION
            // =============================================

            if (data.type === 'register_admin') {

                ws.isAdmin = true;
                ws.adminId = clientId;

                logDB(
                    `[ADMIN] Connected via port ${req.socket.remotePort}`
                );

                notifyAdmin(
                    clientId,
                    '🛡️ Admin Session Started'
                );

                broadcastToAdmins(ws, {
                    type: 'refresh_users'
                });

                return;
            }

            // =============================================
            // AGENT INFORMATION / REGISTRATION
            // =============================================

            if (data.type === 'info') {

                if (ws.isAdmin) {
                    return;
                }

                const uniqueId = String(
                    data.uniqueId || data.id || ''
                ).trim();

                if (!uniqueId || uniqueId.length > 255) {
                    logDB('[WARN] Invalid agent unique ID');
                    return;
                }

                // Store the real agent ID on this socket.
                ws.uniqueId = uniqueId;

                const now = Date.now();

                const stmt = db.prepare(`
                    INSERT INTO users (
                        id,
                        os,
                        arch,
                        user,
                        ip,
                        last_seen,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'Online')

                    ON CONFLICT(id) DO UPDATE SET
                        last_seen = excluded.last_seen,
                        status = 'Online',
                        os = COALESCE(excluded.os, users.os),
                        arch = COALESCE(excluded.arch, users.arch),
                        user = COALESCE(excluded.user, users.user),
                        ip = COALESCE(excluded.ip, users.ip)
                `);

                stmt.run(
                    uniqueId,
                    data.os || null,
                    data.arch || null,
                    data.user || null,
                    data.ip || remoteAddress,
                    now
                );

                logDB(`[AGENT] Online: ${uniqueId}`);

                // Notify connected admins.
                for (const client of wss.clients) {

                    if (
                        client.readyState === WebSocket.OPEN &&
                        client.isAdmin
                    ) {
                        sendMessage(client, {
                            type: 'user_update',
                            id: uniqueId,
                            info: data,
                            lastSeen: now
                        });
                    }
                }

                return;
            }

            // =============================================
            // WEBRTC SIGNALING
            // OFFER / ANSWER / ICE CANDIDATE
            // =============================================

            if (
                ['offer', 'answer', 'candidate'].includes(data.type)
            ) {

                if (ws.isAdmin && data.targetId) {

                    // Admin -> Agent
                    const targetWs = findAgentByUniqueID(
                        String(data.targetId)
                    );

                    if (
                        targetWs &&
                        targetWs.readyState === WebSocket.OPEN
                    ) {
                        sendMessage(targetWs, data);
                    } else {
                        logDB(
                            `[WARN] Agent not connected: ${data.targetId}`
                        );
                    }

                    return;
                }

                if (!ws.isAdmin && data.targetId === 'admin') {

                    // Agent -> Admin
                    for (const client of wss.clients) {

                        if (
                            client.readyState === WebSocket.OPEN &&
                            client.isAdmin
                        ) {
                            sendMessage(client, data);
                        }
                    }

                    return;
                }
            }

        } catch (error) {

            logDB(
                `[ERR] Message processing error: ${error.message}`
            );

        }
    });

    // -------------------------------------------------
    // PONG / CONNECTION HEALTH
    // -------------------------------------------------

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    // -------------------------------------------------
    // DISCONNECTION HANDLER
    // -------------------------------------------------

    ws.on('close', () => {

        logDB(`[-] Client Disconnected: ${ws.id}`);

        // Only update agent records for registered agents.
        if (!ws.isAdmin && ws.uniqueId) {

            const uniqueId = ws.uniqueId;

            // Don't mark an agent offline if another
            // active connection uses the same unique ID.
            if (!isAgentConnected(uniqueId, ws)) {

                try {
                    db.prepare(`
                        UPDATE users
                        SET status = 'Offline',
                            last_seen = ?
                        WHERE id = ?
                    `).run(Date.now(), uniqueId);

                    logDB(`[AGENT] Offline: ${uniqueId}`);

                } catch (error) {
                    logDB(
                        `[DB ERR] Disconnect update: ${error.message}`
                    );
                }
            }
        }

        // Refresh connected admin dashboards.
        broadcastToAdmins(ws, {
            type: 'refresh_users'
        });

    });

    // -------------------------------------------------
    // CONNECTION ERROR
    // -------------------------------------------------

    ws.on('error', (error) => {
        logDB(
            `[WS ERR] ${ws.id}: ${error.message}`
        );
    });

});

// =====================================================
// WEBSOCKET HEARTBEAT
// =====================================================

// Detect disconnected or unresponsive sockets.
const heartbeat = setInterval(() => {

    for (const ws of wss.clients) {

        if (ws.isAlive === false) {
            ws.terminate();
            continue;
        }

        ws.isAlive = false;

        if (ws.readyState === WebSocket.OPEN) {
            ws.ping();
        }
    }

}, 30000);

wss.on('close', () => {
    clearInterval(heartbeat);
});

// =====================================================
// HTTP SERVER STARTUP
// =====================================================

const PORT = process.env.PORT || 3000;

server.listen(PORT, '0.0.0.0', () => {

    logDB(`C2 Server running on port ${PORT}`);
    logDB(`Database location: ${DB_PATH}`);

});

// =====================================================
// SERVER ERROR HANDLING
// =====================================================

server.on('error', (error) => {

    logDB(`[SERVER ERROR] ${error.message}`);

});

process.on('SIGTERM', () => {

    logDB('SIGTERM received. Shutting down...');

    clearInterval(heartbeat);

    wss.close(() => {
        server.close(() => {
            try {
                db.close();
            } finally {
                process.exit(0);
            }
        });
    });

});

process.on('SIGINT', () => {

    logDB('SIGINT received. Shutting down...');

    clearInterval(heartbeat);

    wss.close(() => {
        server.close(() => {
            try {
                db.close();
            } finally {
                process.exit(0);
            }
        });
    });

});
