import http from "node:http";
import { AccessToken } from "livekit-server-sdk";

const {
  PORT = "10000",
  ALLOWED_ORIGIN = "https://dragojsgames.com",
  SUPABASE_URL,
  SUPABASE_PUBLISHABLE_KEY,
  LIVEKIT_URL,
  LIVEKIT_API_KEY,
  LIVEKIT_API_SECRET,
  ALLOWED_USER_IDS = ""
} = process.env;

if (!SUPABASE_URL || !SUPABASE_PUBLISHABLE_KEY) {
  throw new Error("Missing Supabase configuration");
}

// Restrict initial testing to explicitly approved Supabase user IDs.
// These IDs are NOT passwords or Google client IDs.
const allowedUsers = new Set(
  ALLOWED_USER_IDS.split(",").map(id => id.trim()).filter(Boolean)
);

const roomName = "bluemoon-lounge";
const rateLimits = new Map();

function livekitConfigured() {
  try {
    return (
      new URL(LIVEKIT_URL).protocol === "wss:" &&
      Boolean(LIVEKIT_API_KEY && LIVEKIT_API_SECRET)
    );
  } catch {
    return false;
  }
}

function respond(res, status, body, corsAllowed = false) {
  const headers = {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
    "Vary": "Origin"
  };

  if (corsAllowed) {
    headers["Access-Control-Allow-Origin"] = ALLOWED_ORIGIN;
  }

  res.writeHead(status, headers);
  res.end(JSON.stringify(body));
}

function allowTokenRequest(userId) {
  const now = Date.now();
  let entry = rateLimits.get(userId);

  if (!entry || now >= entry.resetAt) {
    entry = { count: 0, resetAt: now + 60000 };
    rateLimits.set(userId, entry);
  }

  entry.count += 1;
  return entry.count <= 10;
}

const cleanupTimer = setInterval(() => {
  const now = Date.now();
  for (const [id, entry] of rateLimits) {
    if (entry.resetAt <= now) rateLimits.delete(id);
  }
}, 60000);

cleanupTimer.unref();

const server = http.createServer(async (req, res) => {
  const path = (req.url || "").split("?")[0];
  const originAllowed = req.headers.origin === ALLOWED_ORIGIN;

  if (req.method === "GET" && path === "/health") {
    respond(res, 200, {
      ok: true,
      service: "BlueMoon LiveKit token service",
      livekitConfigured: livekitConfigured(),
      testAccessConfigured: allowedUsers.size > 0
    });
    return;
  }

  if (path !== "/api/livekit/token") {
    respond(res, 404, { error: "Not found" });
    return;
  }

  if (!originAllowed) {
    respond(res, 403, { error: "Origin not allowed" });
    return;
  }

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
      "Access-Control-Allow-Methods": "POST, OPTIONS",
      "Access-Control-Allow-Headers": "Authorization, Content-Type",
      "Access-Control-Max-Age": "600",
      "Vary": "Origin"
    });
    res.end();
    return;
  }

  if (req.method !== "POST") {
    respond(res, 405, { error: "Use POST" }, true);
    return;
  }

  if (!livekitConfigured()) {
    respond(res, 503, {
      error: "LiveKit is not configured"
    }, true);
    return;
  }

  if (allowedUsers.size === 0) {
    respond(res, 503, {
      error: "Test access has not been configured"
    }, true);
    return;
  }

  const authorization = req.headers.authorization || "";

  if (
    !authorization.startsWith("Bearer ") ||
    authorization.length > 8192
  ) {
    respond(res, 401, { error: "Sign in required" }, true);
    return;
  }

  try {
    // Verify the session with Supabase, not by trusting browser data.
    const authResponse = await fetch(
      `${SUPABASE_URL.replace(/\/$/, "")}/auth/v1/user`,
      {
        headers: {
          apikey: SUPABASE_PUBLISHABLE_KEY,
          Authorization: authorization
        },
        signal: AbortSignal.timeout(8000)
      }
    );

    if (!authResponse.ok) {
      respond(
        res,
        authResponse.status >= 500 ? 503 : 401,
        { error: "Unable to verify your session" },
        true
      );
      return;
    }

    const user = await authResponse.json();

    if (!user.id) {
      respond(res, 401, { error: "Invalid session" }, true);
      return;
    }

    if (!allowedUsers.has(user.id)) {
      respond(res, 403, {
        error: "This account is not approved for the calls test"
      }, true);
      return;
    }

    if (!allowTokenRequest(user.id)) {
      respond(res, 429, {
        error: "Too many requests. Please wait a minute."
      }, true);
      return;
    }

    // Opaque identity: do not put email addresses in LiveKit identities.
    const token = new AccessToken(
      LIVEKIT_API_KEY,
      LIVEKIT_API_SECRET,
      {
        identity: user.id,
        name: `Member ${user.id.slice(0, 6)}`,
        ttl: "5m"
      }
    );

    // Room and permissions are chosen by the server.
    // No administrative or recording permissions are granted.
    token.addGrant({
      roomJoin: true,
      room: roomName,
     canSubscribe: false,
canPublish: false,
canPublishData: true,
    });

    const participantToken = await token.toJwt();

    respond(res, 200, {
      serverUrl: LIVEKIT_URL,
      participantToken,
      roomName
    }, true);
  } catch {
    // Never log session tokens, API secrets or generated room tokens.
    console.error("BlueMoon token request failed");
    respond(res, 503, {
      error: "Calling service temporarily unavailable"
    }, true);
  }
});

server.requestTimeout = 15000;
server.headersTimeout = 10000;
server.maxHeadersCount = 40;

server.listen(Number(PORT), "0.0.0.0", () => {
  console.log("BlueMoon LiveKit token service started");
});

process.on("SIGTERM", () => {
  clearInterval(cleanupTimer);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
});