(function () {
    const config = window.liveGateMonitoringActivityConfig || {};
    const endpoint = String(config.endpoint || "/scan_events").trim() || "/scan_events";
    const streamEndpoint = String(config.streamEndpoint || "/api/scan/stream").trim() || "/api/scan/stream";
    const realtimeEnabled = config.realtimeEnabled !== false;
    const pollIntervalMs = Math.max(Number(config.pollIntervalMs) || 400, 200);
    const streamHealthyPollIntervalMs = Math.max(
        Number(config.streamHealthyPollIntervalMs) || Math.max(pollIntervalMs * 5, 1500),
        1000
    );
    const displayLimit = Math.max(Number(config.limit) || 5, 1);
    const processedHistoryLimit = 300;
    const streamReconnectMs = 1400;

    const feed = document.getElementById("liveGateMonitoringActivityFeed");
    const ghostLayer = document.getElementById("liveGateMonitoringActivityGhostLayer");
    const emptyState = document.getElementById("liveGateMonitoringActivityEmpty");
    const countNode = document.getElementById("liveGateMonitoringActivityCount");

    if (!feed || !ghostLayer || !emptyState || !countNode) {
        return;
    }

    const processedEventIds = [];
    const pendingEntries = new Map();
    let pendingEntryOrder = [];
    let lastEventId = 0;
    let pollTimer = null;
    let activePollIntervalMs = 0;
    let requestInFlight = false;
    let activityStream = null;
    let streamReconnectTimer = null;
    let streamConnected = false;
    let renderFrame = 0;

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    function normalizeAction(action) {
        return String(action || "").trim().toUpperCase() === "OUT" ? "OUT" : "IN";
    }

    function formatActivityTime(entry) {
        const explicitTime = String(
            entry?.time || entry?.timestamp_display || entry?.display_time || ""
        ).trim();
        if (explicitTime) {
            return explicitTime;
        }

        const timestamp = String(entry?.timestamp || "").trim();
        if (timestamp) {
            const parsedDate = new Date(timestamp);
            if (!Number.isNaN(parsedDate.getTime())) {
                try {
                    return new Intl.DateTimeFormat(undefined, {
                        hour: "numeric",
                        minute: "2-digit",
                    }).format(parsedDate);
                } catch (error) {
                    void error;
                }
            }
        }

        return "Recently scanned";
    }

    function updateFeedState() {
        const count = feed.children.length;
        countNode.textContent = `${Math.min(count, displayLimit)} / ${displayLimit}`;
        emptyState.classList.toggle("hidden", count > 0);
        feed.classList.toggle("hidden", count === 0);
    }

    function captureItemPositions() {
        const positions = new Map();
        Array.from(feed.children).forEach((node) => {
            if (!(node instanceof HTMLElement)) {
                return;
            }
            positions.set(node.dataset.studentId || "", node.getBoundingClientRect());
        });
        return positions;
    }

    function animateLayoutShift(previousPositions) {
        Array.from(feed.children).forEach((node) => {
            if (!(node instanceof HTMLElement)) {
                return;
            }

            if (node.classList.contains("live-monitor-activity-enter")) {
                return;
            }

            const previousRect = previousPositions.get(node.dataset.studentId || "");
            if (!previousRect) {
                return;
            }

            const nextRect = node.getBoundingClientRect();
            const deltaY = previousRect.top - nextRect.top;
            if (Math.abs(deltaY) < 1) {
                return;
            }

            node.style.transition = "none";
            node.style.transform = `translateY(${deltaY}px)`;
            node.style.willChange = "transform";

            requestAnimationFrame(() => {
                node.style.transition = "";
                node.style.transform = "";
                window.setTimeout(() => {
                    node.style.willChange = "";
                }, 420);
            });
        });
    }

    function animateOverflowExit(node) {
        if (!(node instanceof HTMLElement)) {
            return;
        }

        const shell = ghostLayer.parentElement;
        if (!(shell instanceof HTMLElement)) {
            return;
        }

        const shellRect = shell.getBoundingClientRect();
        const nodeRect = node.getBoundingClientRect();
        const ghost = node.cloneNode(true);

        if (!(ghost instanceof HTMLElement)) {
            return;
        }

        ghost.classList.add("live-monitor-activity-ghost");
        ghost.style.top = `${nodeRect.top - shellRect.top}px`;
        ghost.style.left = `${nodeRect.left - shellRect.left}px`;
        ghost.style.width = `${nodeRect.width}px`;
        ghost.style.height = `${nodeRect.height}px`;
        ghost.setAttribute("aria-hidden", "true");
        ghostLayer.appendChild(ghost);

        requestAnimationFrame(() => {
            ghost.classList.add("live-monitor-activity-exit-active");
        });

        window.setTimeout(() => {
            ghost.remove();
        }, 440);
    }

    function createActivityItem(entry) {
        const action = normalizeAction(entry?.gate_action || "IN");
        const studentId = String(entry?.student_id || "").trim();
        const studentName = String(entry?.name || entry?.student_name || "Unknown Student").trim() || "Unknown Student";
        const timeLabel = formatActivityTime(entry);

        const item = document.createElement("article");
        item.className = "live-monitor-activity-item";
        item.dataset.studentId = studentId;
        item.dataset.action = action;
        item.innerHTML = `
            <div class="live-monitor-activity-item-layout">
                <div class="live-monitor-activity-copy">
                    <p class="live-monitor-activity-name">${escapeHtml(studentName)}</p>
                    <p class="live-monitor-activity-time">${escapeHtml(timeLabel)}</p>
                </div>
                <span class="live-monitor-activity-badge">${action}</span>
            </div>
        `;
        return item;
    }

    function normalizeActivityEntry(entry) {
        return {
            student_id: String(entry?.student_id || "").trim(),
            student_name: String(entry?.student_name || entry?.name || "").trim(),
            name: String(entry?.name || entry?.student_name || "").trim(),
            gate_action: normalizeAction(entry?.gate_action || "IN"),
            time: String(entry?.time || "").trim(),
            timestamp: String(entry?.timestamp || "").trim(),
        };
    }

    function normalizeFeed() {
        const seenStudentIds = new Set();
        Array.from(feed.children).forEach((node) => {
            if (!(node instanceof HTMLElement)) {
                return;
            }

            const studentId = String(node.dataset.studentId || "").trim();
            if (!studentId || seenStudentIds.has(studentId)) {
                node.remove();
                return;
            }

            seenStudentIds.add(studentId);
        });

        while (feed.children.length > displayLimit) {
            const overflowNode = feed.lastElementChild;
            if (!overflowNode) {
                break;
            }
            overflowNode.remove();
        }

        updateFeedState();
    }

    function scheduleActivityFlush() {
        if (renderFrame) {
            return;
        }

        renderFrame = requestAnimationFrame(() => {
            renderFrame = 0;
            flushActivityEntries();
        });
    }

    function flushActivityEntries() {
        if (!pendingEntryOrder.length) {
            return;
        }

        const previousPositions = captureItemPositions();
        const orderedEntries = [];
        const seenStudentIds = new Set();

        pendingEntryOrder.forEach((studentId) => {
            if (!studentId || seenStudentIds.has(studentId)) {
                return;
            }

            const entry = pendingEntries.get(studentId);
            if (!entry || !entry.student_id) {
                return;
            }

            seenStudentIds.add(studentId);
            orderedEntries.push(entry);
        });

        pendingEntries.clear();
        pendingEntryOrder = [];

        if (!orderedEntries.length) {
            return;
        }

        const currentNodes = new Map();
        Array.from(feed.children).forEach((node) => {
            if (!(node instanceof HTMLElement)) {
                return;
            }
            const studentId = String(node.dataset.studentId || "").trim();
            if (studentId) {
                currentNodes.set(studentId, node);
            }
        });

        orderedEntries.forEach((entry) => {
            const existingNode = currentNodes.get(entry.student_id);
            if (existingNode instanceof HTMLElement) {
                existingNode.remove();
            }
        });

        const fragment = document.createDocumentFragment();
        const enteringNodes = [];
        for (let index = orderedEntries.length - 1; index >= 0; index -= 1) {
            const item = createActivityItem(orderedEntries[index]);
            item.classList.add("live-monitor-activity-enter");
            fragment.appendChild(item);
            enteringNodes.push(item);
        }

        feed.prepend(fragment);

        while (feed.children.length > displayLimit) {
            const overflowNode = feed.lastElementChild;
            if (!(overflowNode instanceof HTMLElement)) {
                break;
            }
            animateOverflowExit(overflowNode);
            overflowNode.remove();
        }

        updateFeedState();

        requestAnimationFrame(() => {
            animateLayoutShift(previousPositions);
            requestAnimationFrame(() => {
                enteringNodes.forEach((item) => {
                    item.classList.remove("live-monitor-activity-enter");
                    item.classList.add("live-monitor-activity-highlight");
                    window.setTimeout(() => {
                        item.classList.remove("live-monitor-activity-highlight");
                    }, 1500);
                });
            });
        });
    }

    function upsertActivityEntry(entry) {
        if (!entry || !entry.student_id) {
            return;
        }

        const normalizedEntry = normalizeActivityEntry(entry);
        if (!normalizedEntry.student_id) {
            return;
        }

        if (!pendingEntries.has(normalizedEntry.student_id)) {
            pendingEntryOrder.push(normalizedEntry.student_id);
        }
        pendingEntries.set(normalizedEntry.student_id, normalizedEntry);
        scheduleActivityFlush();
    }

    function processEvents(events) {
        if (!Array.isArray(events) || !events.length) {
            return;
        }

        const orderedEvents = [...events].sort((left, right) => {
            const leftId = Number(left?.id || 0);
            const rightId = Number(right?.id || 0);
            if (leftId !== rightId) {
                return leftId - rightId;
            }
            return String(left?.timestamp || "").localeCompare(String(right?.timestamp || ""));
        });

        orderedEvents.forEach((eventPayload) => {
            const eventId = Number(eventPayload?.id || 0);
            if (eventId > 0) {
                if (processedEventIds.includes(eventId)) {
                    lastEventId = Math.max(lastEventId, eventId);
                    return;
                }

                processedEventIds.push(eventId);
                while (processedEventIds.length > processedHistoryLimit) {
                    processedEventIds.shift();
                }
                lastEventId = Math.max(lastEventId, eventId);
            }

            if (String(eventPayload?.type || "").trim().toLowerCase() !== "verified") {
                return;
            }

            if (eventPayload?.feed_update === false) {
                return;
            }

            const activityEntry = eventPayload?.activity_entry || {
                student_id: eventPayload?.student_id || "",
                student_name: eventPayload?.name || "",
                gate_action: eventPayload?.gate_action || "IN",
                time: eventPayload?.time || "",
                timestamp: eventPayload?.timestamp || "",
            };

            upsertActivityEntry(activityEntry);
        });
    }

    function processPayload(payload) {
        if (!payload || typeof payload !== "object") {
            return;
        }

        const payloadLastEventId = Number(payload.last_event_id || 0);
        if (payloadLastEventId > 0) {
            lastEventId = Math.max(lastEventId, payloadLastEventId);
        }

        processEvents(payload.events || []);
    }

    async function pollActivityEvents() {
        if (requestInFlight) {
            return;
        }

        requestInFlight = true;
        try {
            const response = await fetch(`${endpoint}?since=${encodeURIComponent(lastEventId)}`, {
                headers: { Accept: "application/json" },
                cache: "no-store",
            });

            if (!response.ok) {
                if (response.status === 401 || response.status === 403) {
                    stopPolling();
                }
                return;
            }

            const payload = await response.json().catch(() => ({}));
            processPayload(payload);
        } catch (error) {
            console.error("[LiveMonitoringActivity] Failed to poll scan events:", error);
        } finally {
            requestInFlight = false;
        }
    }

    function stopPolling() {
        if (pollTimer) {
            window.clearInterval(pollTimer);
            pollTimer = null;
        }
        activePollIntervalMs = 0;
    }

    function startPolling(intervalMs = pollIntervalMs) {
        const normalizedInterval = Math.max(Number(intervalMs) || pollIntervalMs, 200);
        if (pollTimer && activePollIntervalMs === normalizedInterval) {
            return;
        }

        stopPolling();
        activePollIntervalMs = normalizedInterval;
        normalizeFeed();
        void pollActivityEvents();
        pollTimer = window.setInterval(() => {
            void pollActivityEvents();
        }, normalizedInterval);
    }

    function stopStream() {
        if (activityStream) {
            activityStream.close();
            activityStream = null;
        }
        streamConnected = false;
    }

    function scheduleStreamReconnect() {
        if (streamReconnectTimer) {
            return;
        }

        streamReconnectTimer = window.setTimeout(() => {
            streamReconnectTimer = null;
            connectStream();
        }, streamReconnectMs);
    }

    function buildStreamUrl() {
        const streamUrl = new URL(streamEndpoint, window.location.origin);
        if (lastEventId > 0) {
            streamUrl.searchParams.set("since", String(lastEventId));
        }
        return streamUrl.toString();
    }

    function connectStream() {
        if (!realtimeEnabled || !streamEndpoint || !("EventSource" in window)) {
            startPolling(pollIntervalMs);
            return false;
        }

        stopStream();
        normalizeFeed();
        startPolling(pollIntervalMs);

        try {
            const stream = new EventSource(buildStreamUrl());
            activityStream = stream;

            stream.onopen = () => {
                streamConnected = true;
                startPolling(streamHealthyPollIntervalMs);
            };

            stream.addEventListener("scan_event", (event) => {
                if (activityStream !== stream) {
                    return;
                }

                streamConnected = true;
                startPolling(streamHealthyPollIntervalMs);
                const payload = JSON.parse(String(event?.data || "{}"));
                processPayload(payload);
            });

            stream.onerror = () => {
                if (activityStream !== stream) {
                    return;
                }

                stopStream();
                startPolling(pollIntervalMs);
                scheduleStreamReconnect();
            };

            return true;
        } catch (error) {
            console.error("[LiveMonitoringActivity] Failed to open activity stream:", error);
            startPolling(pollIntervalMs);
            scheduleStreamReconnect();
            return false;
        }
    }

    window.addEventListener("beforeunload", () => {
        stopPolling();
        stopStream();
        if (renderFrame) {
            cancelAnimationFrame(renderFrame);
            renderFrame = 0;
        }
        if (streamReconnectTimer) {
            window.clearTimeout(streamReconnectTimer);
            streamReconnectTimer = null;
        }
    });

    if (!connectStream()) {
        startPolling();
    }
})();
