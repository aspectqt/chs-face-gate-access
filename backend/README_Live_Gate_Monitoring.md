# Live Gate Monitoring

This add-on streams the active staff recognition camera to the admin dashboard sidebar with WebRTC and Socket.IO signaling.

## Default startup behavior

- Flask now auto-starts the local Socket.IO signaling server during app startup when live monitoring is locally managed.
- If the app was started without eager bootstrap, the backend will still start the signaling server on the first live-monitor token request or when staff start scanning.
- Staff publishes the camera feed only while scanning is active.
- Full Admin dashboards auto-connect and show the feed in the sidebar.
- When no staff feed is available, the admin sidebar shows `Stream offline`.

## Manual fallback

If you intentionally disable backend-managed startup, you can still launch the signaling server manually from `face-gate-access/backend`:

```powershell
npm run start:live-monitor
```

## Optional environment variables

```env
LIVE_MONITORING_MANAGED=1
LIVE_MONITORING_AUTO_START=1
LIVE_MONITORING_NODE_BIN=node
LIVE_MONITORING_HOST=0.0.0.0
LIVE_MONITORING_CHANNEL=main-gate
LIVE_MONITORING_SIGNALING_PORT=5445
LIVE_MONITORING_SIGNALING_URL=https://YOUR_HOST:5445
LIVE_MONITORING_TOKEN_SECRET=change-me
LIVE_MONITORING_TOKEN_TTL_SECONDS=120
LIVE_MONITORING_STARTUP_TIMEOUT_SECONDS=6
LIVE_MONITORING_ALLOWED_ORIGINS=https://YOUR_HOST:5444
LIVE_MONITORING_STUN_URLS=stun:stun.l.google.com:19302,stun:stun1.l.google.com:19302
```

## HTTPS note

If the Flask dashboard is served over HTTPS, run the signaling server with valid `SSL_CERT_FILE` and `SSL_KEY_FILE` values so the browser can load the Socket.IO client and connect without mixed-content blocking.
