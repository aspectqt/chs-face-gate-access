const fs = require("fs");
const path = require("path");
const http = require("http");
const https = require("https");
const crypto = require("crypto");
const { Server } = require("socket.io");

loadLocalEnv(path.join(__dirname, ".env"));

const DEFAULT_CHANNEL = (process.env.LIVE_MONITORING_CHANNEL || "main-gate").trim() || "main-gate";
const SIGNALING_HOST = (process.env.LIVE_MONITORING_HOST || "0.0.0.0").trim() || "0.0.0.0";
const SIGNALING_PORT = parseInteger(process.env.LIVE_MONITORING_SIGNALING_PORT, 5445);
const TOKEN_SECRET = Buffer.from(
  (process.env.LIVE_MONITORING_TOKEN_SECRET || process.env.FLASK_SECRET_KEY || "live-monitoring-secret").trim()
    || "live-monitoring-secret",
  "utf8"
);
const ALLOWED_ORIGINS = parseOrigins(process.env.LIVE_MONITORING_ALLOWED_ORIGINS || process.env.APP_BASE_URL || "");

const publisherByChannel = new Map();
const viewersByChannel = new Map();

const server = createServer();
const io = new Server(server, {
  cors: {
    origin: ALLOWED_ORIGINS.length ? ALLOWED_ORIGINS : true,
    methods: ["GET", "POST"],
    credentials: true,
  },
  transports: ["websocket", "polling"],
});

io.use((socket, next) => {
  try {
    const authToken = String(socket.handshake.auth?.token || socket.handshake.query?.token || "").trim();
    if (!authToken) {
      throw new Error("Missing token.");
    }

    const auth = verifyToken(authToken);
    socket.data.auth = auth;
    socket.data.role = String(auth.role || "").trim();
    socket.data.channel = normalizeChannel(auth.channel || DEFAULT_CHANNEL);
    socket.data.mode = null;
    next();
  } catch (error) {
    next(new Error(`Authentication failed: ${error.message}`));
  }
});

io.on("connection", (socket) => {
  const role = socket.data.role;
  const channel = socket.data.channel;

  socket.on("live-monitor:register-viewer", (_payload = {}, callback = () => {}) => {
    if (role !== "Full Admin") {
      callback({ ok: false, error: "forbidden" });
      return;
    }

    socket.data.mode = "viewer";
    addViewer(channel, socket.id);
    callback({
      ok: true,
      channel,
      publisher_available: publisherByChannel.has(channel),
    });
  });

  socket.on("live-monitor:register-publisher", (_payload = {}, callback = () => {}) => {
    if (role !== "Staff") {
      callback({ ok: false, error: "forbidden" });
      return;
    }

    const previousPublisherId = publisherByChannel.get(channel);
    if (previousPublisherId && previousPublisherId !== socket.id) {
      const previousPublisher = io.sockets.sockets.get(previousPublisherId);
      if (previousPublisher) {
        previousPublisher.emit("live-monitor:force-offline", {
          channel,
          reason: "replaced",
        });
        previousPublisher.disconnect(true);
      }
    }

    socket.data.mode = "publisher";
    publisherByChannel.set(channel, socket.id);
    notifyChannelViewers(channel, {
      online: true,
      channel,
    });
    callback({ ok: true, channel });
  });

  socket.on("live-monitor:request-stream", (_payload = {}, callback = () => {}) => {
    if (socket.data.mode !== "viewer") {
      callback({ ok: false, error: "forbidden" });
      return;
    }

    const publisherId = publisherByChannel.get(channel);
    if (!publisherId) {
      callback({ ok: false, error: "publisher_offline" });
      return;
    }

    io.to(publisherId).emit("live-monitor:request-stream", {
      viewer_id: socket.id,
      channel,
    });
    callback({ ok: true });
  });

  socket.on("live-monitor:signal", (payload = {}, callback = () => {}) => {
    const targetId = String(payload.target || "").trim();
    const targetSocket = io.sockets.sockets.get(targetId);
    if (!targetId || !targetSocket) {
      callback({ ok: false, error: "target_unavailable" });
      return;
    }

    const sourceMode = socket.data.mode;
    const targetMode = targetSocket.data.mode;
    const sameChannel = targetSocket.data.channel === channel;
    const validPair =
      (sourceMode === "publisher" && targetMode === "viewer")
      || (sourceMode === "viewer" && targetMode === "publisher");

    if (!sameChannel || !validPair) {
      callback({ ok: false, error: "invalid_peer" });
      return;
    }

    targetSocket.emit("live-monitor:signal", {
      source: socket.id,
      channel,
      description: payload.description || null,
      candidate: payload.candidate || null,
    });
    callback({ ok: true });
  });

  socket.on("live-monitor:unregister-publisher", (callback = () => {}) => {
    if (socket.data.mode !== "publisher") {
      callback({ ok: false, error: "not_publisher" });
      return;
    }

    removePublisher(channel, socket.id);
    callback({ ok: true });
  });

  socket.on("disconnect", () => {
    if (socket.data.mode === "viewer") {
      removeViewer(channel, socket.id);
      const publisherId = publisherByChannel.get(channel);
      if (publisherId) {
        io.to(publisherId).emit("live-monitor:viewer-left", {
          viewer_id: socket.id,
          channel,
        });
      }
      return;
    }

    if (socket.data.mode === "publisher") {
      removePublisher(channel, socket.id);
    }
  });
});

server.listen(SIGNALING_PORT, SIGNALING_HOST, () => {
  const protocol = isHttpsServer(server) ? "https" : "http";
  console.log(`[LiveMonitoring] Signaling server listening on ${protocol}://${SIGNALING_HOST}:${SIGNALING_PORT}`);
  console.log(`[LiveMonitoring] Default channel: ${DEFAULT_CHANNEL}`);
});

function addViewer(channel, socketId) {
  if (!viewersByChannel.has(channel)) {
    viewersByChannel.set(channel, new Set());
  }
  viewersByChannel.get(channel).add(socketId);
}

function removeViewer(channel, socketId) {
  const viewers = viewersByChannel.get(channel);
  if (!viewers) {
    return;
  }

  viewers.delete(socketId);
  if (!viewers.size) {
    viewersByChannel.delete(channel);
  }
}

function removePublisher(channel, socketId) {
  if (publisherByChannel.get(channel) !== socketId) {
    return;
  }

  publisherByChannel.delete(channel);
  notifyChannelViewers(channel, {
    online: false,
    channel,
  });
}

function notifyChannelViewers(channel, payload) {
  const viewers = viewersByChannel.get(channel);
  if (!viewers || !viewers.size) {
    return;
  }

  for (const viewerId of viewers) {
    io.to(viewerId).emit("live-monitor:publisher-status", payload);
  }
}

function normalizeChannel(value) {
  return String(value || DEFAULT_CHANNEL).trim() || DEFAULT_CHANNEL;
}

function verifyToken(token) {
  const parts = String(token || "").split(".");
  if (parts.length !== 2) {
    throw new Error("Malformed token.");
  }

  const [payloadB64, signatureB64] = parts;
  const expectedSignature = base64Url(
    crypto.createHmac("sha256", TOKEN_SECRET).update(payloadB64).digest()
  );

  const signatureBuffer = Buffer.from(signatureB64, "utf8");
  const expectedBuffer = Buffer.from(expectedSignature, "utf8");
  if (
    signatureBuffer.length !== expectedBuffer.length
    || !crypto.timingSafeEqual(signatureBuffer, expectedBuffer)
  ) {
    throw new Error("Invalid signature.");
  }

  const payload = JSON.parse(base64UrlDecode(payloadB64).toString("utf8"));
  const exp = Number(payload.exp || 0);
  if (!exp || Math.floor(Date.now() / 1000) >= exp) {
    throw new Error("Token expired.");
  }

  return payload;
}

function base64Url(buffer) {
  return Buffer.from(buffer)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function base64UrlDecode(value) {
  const padded = String(value || "")
    .replace(/-/g, "+")
    .replace(/_/g, "/")
    .padEnd(Math.ceil(String(value || "").length / 4) * 4, "=");
  return Buffer.from(padded, "base64");
}

function parseOrigins(value) {
  return String(value || "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parseInteger(value, fallback) {
  const parsed = Number.parseInt(String(value || "").trim(), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function createServer() {
  const keyFile = resolveFile(process.env.SSL_KEY_FILE || "");
  const certFile = resolveFile(process.env.SSL_CERT_FILE || "");

  if (keyFile && certFile) {
    return https.createServer({
      key: fs.readFileSync(keyFile),
      cert: fs.readFileSync(certFile),
    });
  }

  return http.createServer();
}

function isHttpsServer(activeServer) {
  return typeof activeServer.setSecureContext === "function";
}

function resolveFile(filePath) {
  const normalized = String(filePath || "").trim();
  if (!normalized) {
    return "";
  }

  const resolved = path.isAbsolute(normalized)
    ? normalized
    : path.resolve(__dirname, normalized);

  return fs.existsSync(resolved) ? resolved : "";
}

function loadLocalEnv(filePath) {
  if (!fs.existsSync(filePath)) {
    return;
  }

  const contents = fs.readFileSync(filePath, "utf8");
  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }

    const separatorIndex = line.indexOf("=");
    if (separatorIndex <= 0) {
      continue;
    }

    const key = line.slice(0, separatorIndex).trim();
    if (!key || Object.prototype.hasOwnProperty.call(process.env, key)) {
      continue;
    }

    let value = line.slice(separatorIndex + 1).trim();
    if (
      (value.startsWith("\"") && value.endsWith("\""))
      || (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }

    process.env[key] = value;
  }
}
