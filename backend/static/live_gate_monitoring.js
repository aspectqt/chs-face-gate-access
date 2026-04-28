(function () {
    const config = window.liveGateMonitoringConfig || {};
    const ROLE_FULL_ADMIN = "Full Admin";
    const ROLE_STAFF = "Staff";
    const OFFLINE_FALLBACK_MS = 5000;
    const VIEWER_RECONNECT_MS = 1800;
    const VIEWER_REQUEST_TIMEOUT_MS = 3200;
    const PUBLISHER_SYNC_INTERVAL_MS = 1200;
    const PUBLISHER_BURST_ATTEMPTS = 10;
    const PUBLISHER_BURST_INTERVAL_MS = 300;
    const MAX_PENDING_ICE_CANDIDATES = 24;

    if (!config.tokenEndpoint || !window.RTCPeerConnection) {
        return;
    }

    class LiveGateMonitoringManager {
        constructor(options) {
            this.options = options || {};
            this.socket = null;
            this.socketMode = null;
            this.socketRetryTimer = null;
            this.socketScriptPromise = null;
            this.tokenPayload = null;
            this.viewerPeer = null;
            this.viewerPeerSource = "";
            this.viewerReconnectTimer = null;
            this.viewerOfflineTimer = null;
            this.viewerRequestTimer = null;
            this.viewerLastRequestAt = 0;
            this.viewerPendingCandidates = [];
            this.publisherPeers = new Map();
            this.publisherSourceStream = null;
            this.publisherSourceCleanup = null;
            this.publisherStartPromise = null;
            this.publisherWatchTimer = null;
            this.publisherBurstTimer = null;
            this.viewerDisplays = this.resolveViewerDisplays();
        }

        init() {
            if (this.isAdminViewer() && this.hasViewerPanel()) {
                this.startViewer();
            }

            if (this.isStaffPublisher()) {
                this.attachPublisherHooks();
            }
        }

        isAdminViewer() {
            return this.isEnabledFlag(
                this.options.viewerEnabled,
                String(this.options.role || "").trim() === ROLE_FULL_ADMIN
            );
        }

        isStaffPublisher() {
            return this.isEnabledFlag(
                this.options.publisherEnabled,
                String(this.options.role || "").trim() === ROLE_STAFF
            );
        }

        isEnabledFlag(value, fallback = false) {
            if (typeof value === "boolean") {
                return value;
            }

            if (typeof value === "string") {
                const normalized = value.trim().toLowerCase();
                if (["1", "true", "yes", "on"].includes(normalized)) {
                    return true;
                }
                if (["0", "false", "no", "off"].includes(normalized)) {
                    return false;
                }
            }

            if (typeof value === "number") {
                return value !== 0;
            }

            return fallback;
        }

        resolveViewerDisplays() {
            const displays = [];
            const sidebarDisplay = this.buildViewerDisplay({
                rootId: this.options.viewerRootId || "liveGateMonitoringPanel",
                videoId: this.options.viewerVideoId || "liveGateMonitoringVideo",
                overlayId: this.options.viewerOverlayId || "liveGateMonitoringOverlay",
                statusId: this.options.viewerStatusId || "liveGateMonitoringStatus",
                badgeId: this.options.viewerBadgeId || "liveGateMonitoringBadge",
            });
            if (sidebarDisplay) {
                displays.push(sidebarDisplay);
            }

            const pageDisplay = this.buildViewerDisplay({
                rootId: this.options.viewerPageRootId || "",
                videoId: this.options.viewerPageVideoId || "",
                overlayId: this.options.viewerPageOverlayId || "",
                statusId: this.options.viewerPageStatusId || "",
                badgeId: this.options.viewerPageBadgeId || "",
            });
            if (pageDisplay) {
                displays.push(pageDisplay);
            }

            return displays;
        }

        buildViewerDisplay({ rootId = "", videoId = "", overlayId = "", statusId = "", badgeId = "" } = {}) {
            const display = {
                root: rootId ? document.getElementById(rootId) : null,
                video: videoId ? document.getElementById(videoId) : null,
                overlay: overlayId ? document.getElementById(overlayId) : null,
                status: statusId ? document.getElementById(statusId) : null,
                badge: badgeId ? document.getElementById(badgeId) : null,
            };

            if (!display.root && !display.video && !display.overlay && !display.status && !display.badge) {
                return null;
            }

            return display;
        }

        forEachViewerDisplay(callback) {
            this.viewerDisplays.forEach((display) => {
                if (!display) {
                    return;
                }
                callback(display);
            });
        }

        hasViewerPanel() {
            return this.viewerDisplays.some((display) => Boolean(display?.root || display?.video));
        }

        setViewerState(state, message) {
            const normalizedState = String(state || "offline").trim() || "offline";
            const safeMessage = String(message || "").trim() || "Stream offline";
            const badgeTextMap = {
                connecting: "Connecting",
                live: "Live",
                offline: "Offline",
                error: "Offline",
            };

            this.forEachViewerDisplay((display) => {
                if (display.status) {
                    display.status.textContent = safeMessage;
                }

                if (display.badge) {
                    display.badge.dataset.state = normalizedState;
                    display.badge.textContent = badgeTextMap[normalizedState] || "Offline";
                }

                if (display.overlay) {
                    display.overlay.textContent = safeMessage;
                    display.overlay.hidden = normalizedState === "live";
                }
            });
        }

        showOfflineAfterDelay(message, delayMs = OFFLINE_FALLBACK_MS) {
            this.clearViewerOfflineTimer();
            this.viewerOfflineTimer = window.setTimeout(() => {
                this.setViewerState("offline", message || "Stream offline");
            }, delayMs);
        }

        clearViewerOfflineTimer() {
            if (!this.viewerOfflineTimer) {
                return;
            }

            window.clearTimeout(this.viewerOfflineTimer);
            this.viewerOfflineTimer = null;
        }

        clearViewerRequestTimer() {
            if (!this.viewerRequestTimer) {
                return;
            }

            window.clearTimeout(this.viewerRequestTimer);
            this.viewerRequestTimer = null;
        }

        scheduleViewerRequestRetry(delayMs = VIEWER_REQUEST_TIMEOUT_MS) {
            this.clearViewerRequestTimer();
            this.viewerRequestTimer = window.setTimeout(() => {
                this.viewerRequestTimer = null;

                if (!this.socket || this.socketMode !== "viewer" || !this.socket.connected) {
                    return;
                }

                if (this.viewerPeer?.connectionState === "connected") {
                    return;
                }

                this.requestViewerStream(true);
            }, Math.max(Number(delayMs) || VIEWER_REQUEST_TIMEOUT_MS, 800));
        }

        async startViewer() {
            if (!this.hasViewerPanel()) {
                return;
            }

            this.setViewerState("connecting", "Connecting to live stream...");
            this.showOfflineAfterDelay("Stream offline");
            try {
                await this.ensureSocket("viewer");
                this.registerViewer();
            } catch (error) {
                console.error("[LiveMonitoring] Viewer startup failed:", error);
                this.setViewerState("offline", "Stream offline");
                this.scheduleViewerReconnect();
            }
        }

        scheduleViewerReconnect() {
            this.clearViewerRequestTimer();
            if (this.viewerReconnectTimer) {
                return;
            }

            this.viewerReconnectTimer = window.setTimeout(() => {
                this.viewerReconnectTimer = null;
                this.startViewer();
            }, VIEWER_RECONNECT_MS);
        }

        attachPublisherHooks() {
            this.publisherWatchTimer = window.setInterval(() => {
                this.syncPublisherState();
            }, PUBLISHER_SYNC_INTERVAL_MS);

            const startButton = this.resolveStartButton();
            if (startButton) {
                startButton.addEventListener("click", () => {
                    this.schedulePublisherBurstSync();
                });
            }

            const stopButton = this.resolveStopButton();
            if (stopButton) {
                stopButton.addEventListener("click", () => {
                    this.schedulePublisherBurstSync();
                });
            }

            window.addEventListener("beforeunload", () => {
                this.stopPublishing({ disconnectSocket: true, notifyServer: false });
            });

            this.schedulePublisherBurstSync();
        }

        schedulePublisherBurstSync() {
            let attempts = 0;
            if (this.publisherBurstTimer) {
                window.clearInterval(this.publisherBurstTimer);
                this.publisherBurstTimer = null;
            }

            this.syncPublisherState();
            this.publisherBurstTimer = window.setInterval(() => {
                attempts += 1;
                this.syncPublisherState();
                if (attempts >= PUBLISHER_BURST_ATTEMPTS) {
                    window.clearInterval(this.publisherBurstTimer);
                    this.publisherBurstTimer = null;
                }
            }, PUBLISHER_BURST_INTERVAL_MS);
        }

        async syncPublisherState() {
            const shouldPublish = this.shouldPublish();
            const currentStream = this.getCurrentRecognitionStream();

            if (!shouldPublish || !currentStream) {
                if (this.publisherSourceStream || this.socketMode === "publisher" || this.publisherPeers.size) {
                    this.stopPublishing({ disconnectSocket: true, notifyServer: true });
                }
                return;
            }

            if (this.publisherSourceStream && this.publisherSourceStream !== currentStream) {
                this.stopPublishing({ disconnectSocket: true, notifyServer: true });
            }

            if (this.publisherSourceStream) {
                return;
            }

            try {
                await this.startPublishing(currentStream);
            } catch (error) {
                console.error("[LiveMonitoring] Publisher startup failed:", error);
            }
        }

        shouldPublish() {
            return this.isStaffPublisher() && this.getRecognitionState() && !!this.getCurrentRecognitionStream();
        }

        getRecognitionState() {
            try {
                return typeof isScanning !== "undefined" ? Boolean(isScanning) : false;
            } catch (_error) {
                return false;
            }
        }

        getCurrentRecognitionStream() {
            try {
                if (typeof clientCamera === "undefined" || !clientCamera || !(clientCamera.stream instanceof MediaStream)) {
                    return null;
                }
                return clientCamera.stream;
            } catch (_error) {
                return null;
            }
        }

        resolveStartButton() {
            try {
                return typeof startBtn !== "undefined" ? startBtn : null;
            } catch (_error) {
                return null;
            }
        }

        resolveStopButton() {
            try {
                return typeof stopBtn !== "undefined" ? stopBtn : null;
            } catch (_error) {
                return null;
            }
        }

        async startPublishing(sourceStream) {
            if (!sourceStream) {
                return;
            }

            if (this.publisherStartPromise) {
                return this.publisherStartPromise;
            }

            this.publisherStartPromise = (async () => {
                this.publisherSourceStream = sourceStream;
                this.bindPublisherSourceLifecycle(sourceStream);
                await this.ensureSocket("publisher");
                this.registerPublisher();
            })();

            try {
                await this.publisherStartPromise;
            } finally {
                this.publisherStartPromise = null;
            }
        }

        stopPublishing({ disconnectSocket = false, notifyServer = true } = {}) {
            this.unbindPublisherSourceLifecycle();

            for (const peerEntry of this.publisherPeers.values()) {
                this.closePublisherPeerEntry(peerEntry);
            }
            this.publisherPeers.clear();
            this.publisherSourceStream = null;

            if (notifyServer && this.socketMode === "publisher" && this.socket?.connected) {
                this.socket.emit("live-monitor:unregister-publisher", () => null);
            }

            if (disconnectSocket && this.socketMode === "publisher") {
                this.disconnectSocket();
            }
        }

        bindPublisherSourceLifecycle(stream) {
            this.unbindPublisherSourceLifecycle();
            const videoTrack = stream.getVideoTracks()[0];
            if (!videoTrack) {
                return;
            }

            const handleEnded = () => {
                this.stopPublishing({ disconnectSocket: true, notifyServer: true });
            };

            videoTrack.addEventListener("ended", handleEnded);
            this.publisherSourceCleanup = () => {
                try {
                    videoTrack.removeEventListener("ended", handleEnded);
                } catch (_error) {
                    // Ignore stale track cleanup failures.
                }
            };
        }

        unbindPublisherSourceLifecycle() {
            if (!this.publisherSourceCleanup) {
                return;
            }

            this.publisherSourceCleanup();
            this.publisherSourceCleanup = null;
        }

        async ensureSocket(mode) {
            if (this.socket && this.socket.connected && this.socketMode === mode) {
                return this.socket;
            }

            if (this.socketMode && this.socketMode !== mode) {
                this.disconnectSocket();
            }

            const tokenPayload = await this.fetchTokenPayload();
            await this.loadSocketIoClient(tokenPayload.signaling_url);
            return this.openSocket(mode, tokenPayload);
        }

        async fetchTokenPayload() {
            const response = await fetch(this.options.tokenEndpoint, {
                credentials: "same-origin",
            });
            const payload = await response.json().catch(() => ({}));

            if (!response.ok || payload.status !== "ok") {
                throw new Error(payload.message || "Unable to initialize live monitoring.");
            }

            this.tokenPayload = payload;
            return payload;
        }

        loadSocketIoClient(signalingUrl) {
            if (window.io) {
                return Promise.resolve();
            }

            if (this.socketScriptPromise) {
                return this.socketScriptPromise;
            }

            this.socketScriptPromise = new Promise((resolve, reject) => {
                const script = document.createElement("script");
                script.src = `${String(signalingUrl || "").replace(/\/$/, "")}/socket.io/socket.io.js`;
                script.async = true;
                script.onload = () => resolve();
                script.onerror = () => {
                    this.socketScriptPromise = null;
                    reject(new Error("Unable to load the live monitoring client."));
                };
                document.head.appendChild(script);
            });

            return this.socketScriptPromise;
        }

        openSocket(mode, tokenPayload) {
            if (!window.io) {
                return Promise.reject(new Error("Socket.IO client is unavailable."));
            }

            return new Promise((resolve, reject) => {
                const socket = window.io(tokenPayload.signaling_url, {
                    transports: ["websocket", "polling"],
                    auth: {
                        token: tokenPayload.token,
                    },
                    timeout: 6000,
                    reconnection: false,
                });

                let settled = false;

                const finishFailure = (error) => {
                    if (settled) {
                        return;
                    }
                    settled = true;
                    socket.disconnect();
                    reject(error instanceof Error ? error : new Error(String(error || "Unable to connect.")));
                };

                socket.once("connect", () => {
                    settled = true;
                    this.socket = socket;
                    this.socketMode = mode;
                    this.bindSocketEvents(socket, mode);
                    resolve(socket);
                });

                socket.once("connect_error", (error) => {
                    finishFailure(error || new Error("Unable to connect."));
                });
            });
        }

        bindSocketEvents(socket, mode) {
            socket.on("disconnect", () => {
                if (this.socket !== socket) {
                    return;
                }

                this.socket = null;
                this.socketMode = null;

                if (mode === "viewer") {
                    this.clearViewerPeer({ clearMedia: true });
                    this.setViewerState("offline", "Stream offline");
                    this.scheduleViewerReconnect();
                    return;
                }

                for (const peerEntry of this.publisherPeers.values()) {
                    this.closePublisherPeerEntry(peerEntry);
                }
                this.publisherPeers.clear();
                this.publisherSourceStream = null;
                this.unbindPublisherSourceLifecycle();
                this.schedulePublisherBurstSync();
            });

            socket.on("live-monitor:signal", async (payload) => {
                try {
                    if (mode === "viewer") {
                        await this.handleViewerSignal(payload || {});
                    } else {
                        await this.handlePublisherSignal(payload || {});
                    }
                } catch (error) {
                    console.error("[LiveMonitoring] Signal handling failed:", error);
                }
            });

            if (mode === "viewer") {
                socket.on("live-monitor:publisher-status", (payload) => {
                    if (payload?.online) {
                        this.setViewerState("connecting", "Connecting to live stream...");
                        this.showOfflineAfterDelay("Stream offline");
                        this.requestViewerStream();
                        return;
                    }

                    this.clearViewerPeer({ clearMedia: true });
                    this.clearViewerRequestTimer();
                    this.setViewerState("offline", "Stream offline");
                });

                return;
            }

            socket.on("live-monitor:request-stream", async (payload) => {
                const viewerId = String(payload?.viewer_id || "").trim();
                if (!viewerId) {
                    return;
                }

                try {
                    await this.startPublisherOffer(viewerId);
                } catch (error) {
                    console.error("[LiveMonitoring] Failed to create publisher offer:", error);
                }
            });

            socket.on("live-monitor:viewer-left", (payload) => {
                const viewerId = String(payload?.viewer_id || "").trim();
                if (!viewerId) {
                    return;
                }
                this.removePublisherPeer(viewerId);
            });

            socket.on("live-monitor:force-offline", () => {
                this.stopPublishing({ disconnectSocket: true, notifyServer: false });
            });
        }

        registerViewer() {
            if (!this.socket || this.socketMode !== "viewer") {
                return;
            }

            this.socket.emit("live-monitor:register-viewer", {}, (response) => {
                if (!response?.ok) {
                    this.disconnectSocket();
                    this.setViewerState("offline", "Stream offline");
                    this.scheduleViewerReconnect();
                    return;
                }

                if (response.publisher_available) {
                    this.requestViewerStream(true);
                    return;
                }

                this.showOfflineAfterDelay("Stream offline");
            });
        }

        registerPublisher() {
            if (!this.socket || this.socketMode !== "publisher") {
                return;
            }

            this.socket.emit("live-monitor:register-publisher", {}, (response) => {
                if (!response?.ok) {
                    this.stopPublishing({ disconnectSocket: true, notifyServer: false });
                }
            });
        }

        requestViewerStream(force = false) {
            if (!this.socket || this.socketMode !== "viewer" || !this.socket.connected) {
                return;
            }

            const now = Date.now();
            if (!force && (now - this.viewerLastRequestAt) < 1200) {
                return;
            }

            this.viewerLastRequestAt = now;
            this.setViewerState("connecting", "Connecting to live stream...");
            this.socket.emit("live-monitor:request-stream", {}, (response) => {
                if (!response?.ok) {
                    this.clearViewerRequestTimer();
                    this.showOfflineAfterDelay("Stream offline", 1400);
                    return;
                }

                this.scheduleViewerRequestRetry();
            });
        }

        async handleViewerSignal(payload) {
            const source = String(payload?.source || "").trim();
            if (!source) {
                return;
            }

            if (payload.description) {
                if (payload.description.type !== "offer") {
                    return;
                }

                const peer = this.ensureViewerPeer(source);
                await peer.setRemoteDescription(new RTCSessionDescription(payload.description));
                await this.flushViewerPendingCandidates(source);
                const answer = await peer.createAnswer();
                await peer.setLocalDescription(answer);
                this.emitSignal(source, {
                    description: peer.localDescription,
                });
            }

            if (payload.candidate) {
                if (!this.viewerPeer || this.viewerPeerSource !== source || !this.viewerPeer.remoteDescription) {
                    this.queueViewerCandidate(source, payload.candidate);
                    return;
                }

                await this.viewerPeer.addIceCandidate(new RTCIceCandidate(payload.candidate)).catch(() => {
                    this.queueViewerCandidate(source, payload.candidate);
                    return null;
                });
            }
        }

        ensureViewerPeer(sourceId) {
            if (this.viewerPeerSource && this.viewerPeerSource !== sourceId) {
                this.clearViewerPeer({ clearMedia: true });
            }

            if (this.viewerPeer) {
                return this.viewerPeer;
            }

            const peer = new RTCPeerConnection({
                iceServers: this.resolveIceServers(),
            });
            this.viewerPeer = peer;
            this.viewerPeerSource = sourceId;

            peer.onicecandidate = (event) => {
                if (!event.candidate) {
                    return;
                }
                this.emitSignal(sourceId, {
                    candidate: event.candidate,
                });
            };

            peer.ontrack = async (event) => {
                const incomingStream = event.streams?.[0] || null;
                if (!incomingStream) {
                    return;
                }

                this.clearViewerOfflineTimer();
                this.clearViewerRequestTimer();
                const playPromises = [];
                this.forEachViewerDisplay((display) => {
                    if (!display.video) {
                        return;
                    }
                    display.video.srcObject = incomingStream;
                    try {
                        const playResult = display.video.play();
                        if (playResult && typeof playResult.then === "function") {
                            playPromises.push(playResult.catch(() => null));
                        }
                    } catch (_error) {
                        // Ignore autoplay race conditions.
                    }
                });
                if (playPromises.length) {
                    await Promise.all(playPromises);
                }
                this.setViewerState("live", "Live gate feed active");
            };

            peer.onconnectionstatechange = () => {
                const state = peer.connectionState;
                if (state === "connected") {
                    this.clearViewerOfflineTimer();
                    this.clearViewerRequestTimer();
                    this.setViewerState("live", "Live gate feed active");
                    return;
                }

                if (state === "failed" || state === "disconnected" || state === "closed") {
                    this.clearViewerPeer({ clearMedia: true });
                    if (this.socket && this.socketMode === "viewer" && this.socket.connected) {
                        this.setViewerState("connecting", "Reconnecting to live stream...");
                        this.showOfflineAfterDelay("Stream offline");
                        this.requestViewerStream(true);
                        return;
                    }

                    this.clearViewerRequestTimer();
                    this.setViewerState("offline", "Stream offline");
                }
            };

            return peer;
        }

        clearViewerPeer({ clearMedia = false } = {}) {
            if (this.viewerPeer) {
                try {
                    this.viewerPeer.ontrack = null;
                    this.viewerPeer.onicecandidate = null;
                    this.viewerPeer.onconnectionstatechange = null;
                    this.viewerPeer.close();
                } catch (_error) {
                    // Ignore stale peer cleanup failures.
                }
            }

            this.viewerPeer = null;
            this.viewerPeerSource = "";
            this.viewerPendingCandidates = [];

            if (clearMedia) {
                this.forEachViewerDisplay((display) => {
                    if (display.video) {
                        display.video.srcObject = null;
                    }
                });
            }
        }

        async handlePublisherSignal(payload) {
            const viewerId = String(payload?.source || "").trim();
            if (!viewerId) {
                return;
            }

            const peerEntry = this.publisherPeers.get(viewerId);
            if (!peerEntry) {
                return;
            }

            if (payload.description) {
                await peerEntry.peer.setRemoteDescription(new RTCSessionDescription(payload.description));
                await this.flushPublisherPendingCandidates(peerEntry);
            }

            if (payload.candidate) {
                if (!peerEntry.peer.remoteDescription) {
                    this.queuePendingCandidate(peerEntry.pendingCandidates, payload.candidate);
                    return;
                }

                await peerEntry.peer.addIceCandidate(new RTCIceCandidate(payload.candidate)).catch(() => {
                    this.queuePendingCandidate(peerEntry.pendingCandidates, payload.candidate);
                    return null;
                });
            }
        }

        async startPublisherOffer(viewerId) {
            if (!this.publisherSourceStream) {
                return;
            }

            this.removePublisherPeer(viewerId);

            const clonedStream = this.publisherSourceStream.clone();
            const peer = new RTCPeerConnection({
                iceServers: this.resolveIceServers(),
            });

            clonedStream.getTracks().forEach((track) => {
                peer.addTrack(track, clonedStream);
            });

            const peerEntry = {
                peer,
                stream: clonedStream,
                pendingCandidates: [],
                viewerId,
            };
            this.publisherPeers.set(viewerId, peerEntry);

            peer.onicecandidate = (event) => {
                if (!event.candidate) {
                    return;
                }

                this.emitSignal(viewerId, {
                    candidate: event.candidate,
                });
            };

            peer.onconnectionstatechange = () => {
                const state = peer.connectionState;
                if (state === "failed" || state === "disconnected" || state === "closed") {
                    this.removePublisherPeer(viewerId);
                }
            };

            const offer = await peer.createOffer({
                offerToReceiveAudio: false,
                offerToReceiveVideo: false,
            });
            await peer.setLocalDescription(offer);
            this.emitSignal(viewerId, {
                description: peer.localDescription,
            });
        }

        removePublisherPeer(viewerId) {
            const peerEntry = this.publisherPeers.get(viewerId);
            if (!peerEntry) {
                return;
            }

            this.closePublisherPeerEntry(peerEntry);
            this.publisherPeers.delete(viewerId);
        }

        closePublisherPeerEntry(peerEntry) {
            if (!peerEntry) {
                return;
            }

            try {
                peerEntry.peer.onicecandidate = null;
                peerEntry.peer.onconnectionstatechange = null;
                peerEntry.peer.close();
            } catch (_error) {
                // Ignore stale peer cleanup failures.
            }

            if (peerEntry.stream instanceof MediaStream) {
                peerEntry.stream.getTracks().forEach((track) => {
                    try {
                        track.stop();
                    } catch (_error) {
                        // Ignore clone shutdown failures.
                    }
                });
            }

            peerEntry.pendingCandidates = [];
        }

        emitSignal(target, payload) {
            if (!this.socket || !this.socket.connected) {
                return;
            }

            this.socket.emit("live-monitor:signal", {
                target,
                description: payload.description || null,
                candidate: payload.candidate || null,
            }, () => null);
        }

        resolveIceServers() {
            const iceServers = Array.isArray(this.tokenPayload?.ice_servers)
                ? this.tokenPayload.ice_servers
                : [{ urls: ["stun:stun.l.google.com:19302"] }];
            return iceServers.length ? iceServers : [{ urls: ["stun:stun.l.google.com:19302"] }];
        }

        queuePendingCandidate(queue, candidate) {
            if (!Array.isArray(queue) || !candidate) {
                return;
            }

            queue.push(candidate);
            while (queue.length > MAX_PENDING_ICE_CANDIDATES) {
                queue.shift();
            }
        }

        queueViewerCandidate(sourceId, candidate) {
            if (!candidate) {
                return;
            }

            this.viewerPendingCandidates.push({
                sourceId: String(sourceId || "").trim(),
                candidate,
            });
            while (this.viewerPendingCandidates.length > MAX_PENDING_ICE_CANDIDATES) {
                this.viewerPendingCandidates.shift();
            }
        }

        async flushViewerPendingCandidates(sourceId) {
            if (!this.viewerPeer || !this.viewerPeer.remoteDescription) {
                return;
            }

            const normalizedSourceId = String(sourceId || "").trim();
            const remainingCandidates = [];
            for (const entry of this.viewerPendingCandidates) {
                if (!entry?.candidate || entry.sourceId !== normalizedSourceId) {
                    if (entry?.candidate) {
                        remainingCandidates.push(entry);
                    }
                    continue;
                }

                try {
                    await this.viewerPeer.addIceCandidate(new RTCIceCandidate(entry.candidate));
                } catch (_error) {
                    remainingCandidates.push(entry);
                }
            }

            this.viewerPendingCandidates = remainingCandidates;
        }

        async flushPublisherPendingCandidates(peerEntry) {
            if (!peerEntry?.peer?.remoteDescription || !Array.isArray(peerEntry.pendingCandidates) || !peerEntry.pendingCandidates.length) {
                return;
            }

            const queuedCandidates = [...peerEntry.pendingCandidates];
            peerEntry.pendingCandidates = [];

            for (const candidate of queuedCandidates) {
                if (!candidate) {
                    continue;
                }

                try {
                    await peerEntry.peer.addIceCandidate(new RTCIceCandidate(candidate));
                } catch (_error) {
                    this.queuePendingCandidate(peerEntry.pendingCandidates, candidate);
                }
            }
        }

        disconnectSocket() {
            if (!this.socket) {
                this.socketMode = null;
                return;
            }

            try {
                this.socket.removeAllListeners();
                this.socket.disconnect();
            } catch (_error) {
                // Ignore stale socket cleanup failures.
            }

            this.socket = null;
            this.socketMode = null;
        }
    }

    const manager = new LiveGateMonitoringManager(config);
    window.liveGateMonitoringManager = manager;
    manager.init();
})();
