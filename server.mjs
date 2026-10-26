import http from "node:http";
import { randomUUID } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const {
  SUPABASE_URL,
  SUPABASE_PUBLISHABLE_KEY,
  ALLOWED_ORIGIN = "https://dragojsgames.com",
  PORT = "10000"
} = process.env;

if (!SUPABASE_URL || !SUPABASE_PUBLISHABLE_KEY) {
  throw new Error("Missing Supabase configuration");
}

const peers = new Map();
const MAX_MEMBERS = 6;
const MAX_CONNECTIONS = 40;

function send(ws, message) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(message));
  }
}

function broadcast(message) {
  for (const peer of peers.values()) {
    send(peer.ws, message);
  }
}

const server = http.createServer((req, res) => {
  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json",
      "Cache-Control": "no-store"
    });

    res.end(JSON.stringify({
      ok: true,
      service: "BlueMoon Voice signaling",
      members: peers.size,
      voiceRelayConfigured: false
    }));
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({
  noServer: true,
  maxPayload: 32768,
  perMessageDeflate: false
});

server.on("upgrade", (req, socket, head) => {
  if (
    req.url !== "/voice" ||
    req.headers.origin !== ALLOWED_ORIGIN ||
    wss.clients.size >= MAX_CONNECTIONS
  ) {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit("connection", ws);
  });
});

wss.on("connection", ws => {
  const id = randomUUID();
  let authenticated = false;
  let authenticating = false;
  let joined = false;
  let alive = true;
  let messageCount = 0;
  let windowStart = Date.now();
  let sessionTimer;

  const authTimer = setTimeout(() => {
    ws.close(4001, "Authentication required");
  }, 10000);

  function cleanup() {
    clearTimeout(authTimer);
    clearTimeout(sessionTimer);

    if (joined) {
      joined = false;
      peers.delete(id);
      broadcast({ type: "left", id });
    }
  }

  ws.on("pong", () => {
    alive = true;
  });

  const heartbeat = setInterval(() => {
    if (!alive) {
      ws.terminate();
      return;
    }
    alive = false;
    ws.ping();
  }, 30000);

  ws.on("close", () => {
    clearInterval(heartbeat);
    cleanup();
  });

  ws.on("error", () => {
    ws.terminate();
  });

  ws.on("message", async raw => {
    if (Date.now() - windowStart >= 10000) {
      windowStart = Date.now();
      messageCount = 0;
    }

    if (++messageCount > 120) {
      ws.close(4008, "Rate limit exceeded");
      return;
    }

    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      ws.close(4000, "Invalid message");
      return;
    }

    if (!message || typeof message !== "object") {
      ws.close(4000, "Invalid message");
      return;
    }

    if (!authenticated) {
      if (
        authenticating ||
        message.type !== "auth" ||
        typeof message.token !== "string" ||
        message.token.length > 8192
      ) {
        ws.close(4001, "Authentication required");
        return;
      }

      authenticating = true;

      try {
        const response = await fetch(
          `${SUPABASE_URL.replace(/\/$/, "")}/auth/v1/user`,
          {
            headers: {
              apikey: SUPABASE_PUBLISHABLE_KEY,
              Authorization: `Bearer ${message.token}`
            },
            signal: AbortSignal.timeout(8000)
          }
        );

        if (!response.ok) throw new Error("Invalid session");

        const user = await response.json();
        if (!user.id) throw new Error("Invalid user");

        if (ws.readyState !== WebSocket.OPEN) return;

        if (peers.size >= MAX_MEMBERS) {
          ws.close(4010, "Room full");
          return;
        }

        if ([...peers.values()].some(p => p.userId === user.id)) {
          ws.close(4009, "Already connected");
          return;
        }

        const name = String(
          user.user_metadata?.full_name || "BlueMoon member"
        ).slice(0, 60);

        const existingPeers = [...peers].map(([peerId, peer]) => ({
          id: peerId,
          name: peer.name
        }));

        authenticated = true;
        joined = true;
        clearTimeout(authTimer);

        peers.set(id, { ws, userId: user.id, name });

        send(ws, {
          type: "welcome",
          id,
          peers: existingPeers,
          iceServers: [],
          relayConfigured: false
        });

        broadcast({ type: "joined", id, name });

        // Require fresh authentication periodically.
        sessionTimer = setTimeout(() => {
          ws.close(4001, "Please reconnect to renew your session");
        }, 15 * 60 * 1000);
      } catch {
        ws.close(4001, "Authentication failed");
      }

      return;
    }

    if (message.type === "leave") {
      ws.close(1000, "Left room");
      return;
    }

    if (message.type === "signal") {
      if (
        typeof message.to !== "string" ||
        message.to === id ||
        !message.data ||
        typeof message.data !== "object"
      ) return;

      const target = peers.get(message.to);
      if (!target) return;

      send(target.ws, {
        type: "signal",
        from: id,
        data: message.data
      });
    }
  });
});

server.listen(Number(PORT), "0.0.0.0", () => {
  console.log("BlueMoon Voice signaling started");
});

process.on("SIGTERM", () => {
  for (const ws of wss.clients) {
    ws.close(1012, "Service restarting");
  }

  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
});