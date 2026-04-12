/**
 * Enhanced ClientCamera - High-performance browser-based camera access
 * Optimized for high-volume face scanning with multi-face detection and tracking
 */

class EnhancedClientCamera {
    constructor() {
        this.stream = null;
        this.videoElement = null;
        this.canvas = null;
        this.canvasContext = null;
        this.uploadCanvas = null;
        this.uploadCanvasContext = null;
        this.isStreaming = false;
        this.onError = null;
        this.onSuccess = null;
        this.onFaceDetected = null;
        this.onFaceRecognized = null;
        this.onMultipleFaces = null;
        this.onStreamEnded = null;
        this.trackEventBindings = [];
        this.streamInactiveBinding = null;
        this.suppressStreamEndedCallback = false;
        
        // Performance optimization
        this.frameProcessingQueue = [];
        this.isProcessingFrame = false;
        this.lastFrameTime = 0;
        this.targetFPS = 30;
        this.frameInterval = 1000 / this.targetFPS;
        
        // Face tracking
        this.trackedFaces = new Map();
        this.faceTracker = null;
        this.lastDetectionTime = 0;
        this.detectionInterval = 100; // Detect faces every 100ms
        this.trackFadeDuration = 450;
        this.trackSmoothingFactor = 0.42;
        this.faceDetectionLoopActive = false;
        this.faceDetectionRafId = null;
        
        // Voice feedback
        this.voiceQueue = [];
        this.isSpeaking = false;
        this.lastVoiceTimes = new Map();
        this.voiceCooldown = 3000; // 3 seconds cooldown per person
        
        // Performance metrics
        this.metrics = {
            framesProcessed: 0,
            facesDetected: 0,
            facesRecognized: 0,
            averageProcessingTime: 0,
            startTime: null
        };
    }

    /**
     * Initialize camera with optimized settings for face scanning
     */
    async initialize(videoElement, constraints = null) {
        try {
            this.metrics.startTime = Date.now();
            
            // Enhanced camera constraints for optimal face detection
            const mediaConstraints = constraints || {
                video: {
                    width: { ideal: 1920, max: 1920 },
                    height: { ideal: 1080, max: 1080 },
                    facingMode: 'user',
                    frameRate: { ideal: 30, max: 30 },
                    // Optimize for face detection
                    focusMode: 'continuous',
                    exposureMode: 'continuous',
                    whiteBalanceMode: 'continuous'
                },
                audio: false
            };

            // Get video element
            if (typeof videoElement === 'string') {
                this.videoElement = document.getElementById(videoElement);
            } else {
                this.videoElement = videoElement;
            }

            if (!this.videoElement) {
                throw new Error('Video element not found');
            }

            // Request camera access with optimized settings
            this.stream = await navigator.mediaDevices.getUserMedia(mediaConstraints);
            this.suppressStreamEndedCallback = false;
            this.bindStreamLifecycleEvents();

            // Apply advanced camera settings if supported
            await this.optimizeCameraSettings();

            // Setup video element
            await this.setupVideoElement();

            // Initialize face tracking
            this.initializeFaceTracking();

            this.isStreaming = true;
            this.triggerSuccess('Enhanced camera initialized successfully');
            
            return true;

        } catch (error) {
            const errorMessage = this.getErrorMessage(error);
            this.triggerError(errorMessage);
            return false;
        }
    }

    /**
     * Bind lifecycle handlers for the active media stream so manual camera exits
     * can be handled gracefully by the dashboard.
     */
    bindStreamLifecycleEvents() {
        this.unbindStreamLifecycleEvents();
        if (!this.stream) return;

        const handleTrackEnded = () => {
            if (this.suppressStreamEndedCallback || !this.isStreaming) return;
            this.isStreaming = false;
            this.faceDetectionLoopActive = false;
            if (this.faceDetectionRafId) {
                cancelAnimationFrame(this.faceDetectionRafId);
                this.faceDetectionRafId = null;
            }
            if (this.onStreamEnded && typeof this.onStreamEnded === 'function') {
                this.onStreamEnded('Camera stream ended.');
            }
        };

        this.stream.getTracks().forEach((track) => {
            const endedHandler = () => handleTrackEnded();
            track.addEventListener('ended', endedHandler);
            this.trackEventBindings.push({ track, endedHandler });
        });

        if (typeof this.stream.addEventListener === 'function') {
            const inactiveHandler = () => handleTrackEnded();
            this.stream.addEventListener('inactive', inactiveHandler);
            this.streamInactiveBinding = { stream: this.stream, inactiveHandler };
        }
    }

    unbindStreamLifecycleEvents() {
        this.trackEventBindings.forEach(({ track, endedHandler }) => {
            try {
                track.removeEventListener('ended', endedHandler);
            } catch (_error) {
                // Ignore cleanup failures for stale tracks.
            }
        });
        this.trackEventBindings = [];

        if (this.streamInactiveBinding) {
            const { stream, inactiveHandler } = this.streamInactiveBinding;
            try {
                stream.removeEventListener('inactive', inactiveHandler);
            } catch (_error) {
                // Ignore cleanup failures for stale streams.
            }
            this.streamInactiveBinding = null;
        }
    }

    /**
     * Optimize camera settings for best face detection performance
     */
    async optimizeCameraSettings() {
        try {
            const track = this.stream.getVideoTracks()[0];
            if (!track) return;

            const capabilities = track.getCapabilities();
            const settings = track.getSettings();

            // Enable continuous focus if available
            if (capabilities.focusMode && capabilities.focusMode.includes('continuous')) {
                await track.applyConstraints({
                    advanced: [{ focusMode: 'continuous' }]
                });
            }

            // Optimize exposure for face detection
            if (capabilities.exposureMode && capabilities.exposureMode.includes('continuous')) {
                await track.applyConstraints({
                    advanced: [{ exposureMode: 'continuous' }]
                });
            }

            // Set optimal resolution for face detection
            const idealWidth = 1280;
            const idealHeight = 720;
            
            if (settings.width > idealWidth || settings.height > idealHeight) {
                await track.applyConstraints({
                    width: { ideal: idealWidth, max: idealWidth },
                    height: { ideal: idealHeight, max: idealHeight }
                });
            }

            console.log('[EnhancedCamera] Camera settings optimized for face detection');
            
        } catch (error) {
            console.debug('[EnhancedCamera] Camera optimization failed:', error.message);
        }
    }

    /**
     * Setup video element with performance optimizations
     */
    setupVideoElement() {
        // Attach stream to video element
        if (this.videoElement.srcObject !== undefined) {
            this.videoElement.srcObject = this.stream;
        } else {
            this.videoElement.src = URL.createObjectURL(this.stream);
        }

        // Optimize video element for performance
        this.videoElement.playsInline = true;
        this.videoElement.muted = true;
        this.videoElement.disablePictureInPicture = true;

        // Wait for video to be ready
        return new Promise((resolve, reject) => {
            let timeoutId = null;
            let readinessInterval = null;
            let metadataHandled = false;

            const cleanup = () => {
                this.videoElement.removeEventListener('loadedmetadata', onLoadedMetadata);
                this.videoElement.removeEventListener('loadeddata', onLoadedData);
                if (timeoutId) {
                    clearTimeout(timeoutId);
                    timeoutId = null;
                }
                if (readinessInterval) {
                    clearInterval(readinessInterval);
                    readinessInterval = null;
                }
            };

            const finishIfReady = () => {
                if (this.videoElement.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA &&
                    this.videoElement.videoWidth > 0 &&
                    this.videoElement.videoHeight > 0) {
                    cleanup();
                    resolve();
                    return true;
                }
                return false;
            };

            const onLoadedData = () => {
                finishIfReady();
            };

            const onLoadedMetadata = () => {
                if (metadataHandled) {
                    return;
                }
                metadataHandled = true;
                this.videoElement.play()
                    .then(() => {
                        if (finishIfReady()) {
                            return;
                        }
                        this.videoElement.addEventListener('loadeddata', onLoadedData);
                        readinessInterval = setInterval(() => {
                            finishIfReady();
                        }, 50);
                    })
                    .catch((error) => {
                        cleanup();
                        reject(error);
                    });
            };

            this.videoElement.addEventListener('loadedmetadata', onLoadedMetadata);

            if (this.videoElement.readyState >= HTMLMediaElement.HAVE_METADATA) {
                onLoadedMetadata();
            }

            // Timeout after 3 seconds (reduced from 5 for faster startup)
            timeoutId = setTimeout(() => {
                cleanup();
                reject(new Error('Video initialization timeout'));
            }, 3000);
        });
    }

    createTimeoutSignal(timeoutMs) {
        if (typeof AbortSignal !== 'undefined' && typeof AbortSignal.timeout === 'function') {
            return {
                signal: AbortSignal.timeout(timeoutMs),
                cleanup() {}
            };
        }

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
        return {
            signal: controller.signal,
            cleanup() {
                clearTimeout(timeoutId);
            }
        };
    }

    /**
     * Initialize face tracking system
     */
    initializeFaceTracking() {
        // Initialize face tracking data structures
        this.trackedFaces.clear();
        this.faceTracker = {
            nextId: 1,
            maxTrackedFaces: 10,
            trackingDistance: 100, // pixels
            lostFaceTimeout: 2000 // ms
        };
        
        console.log('[EnhancedCamera] Face tracking initialized');
    }

    /**
     * Start face detection loop with optimized performance
     */
    startFaceDetectionLoop() {
        if (this.faceDetectionLoopActive) return;
        this.faceDetectionLoopActive = true;

        const detectFaces = async () => {
            if (!this.isStreaming || !this.faceDetectionLoopActive) {
                this.faceDetectionRafId = null;
                return;
            }
            
            const now = Date.now();
            
            // Throttle face detection to maintain performance
            if (now - this.lastDetectionTime < this.detectionInterval) {
                this.faceDetectionRafId = requestAnimationFrame(detectFaces);
                return;
            }
            
            try {
                // Capture frame for face detection
                const frame = this.captureFrame();
                if (frame) {
                    // Process frame for face detection
                    await this.processFrameForFaces(frame);
                    this.metrics.framesProcessed++;
                }
                
                this.lastDetectionTime = now;
                
            } catch (error) {
                console.debug('[EnhancedCamera] Face detection error:', error.message);
            }
            
            // Continue detection loop
            this.faceDetectionRafId = requestAnimationFrame(detectFaces);
        };
        
        // Start the detection loop
        this.faceDetectionRafId = requestAnimationFrame(detectFaces);
    }

    /**
     * Process frame for multi-face detection and tracking
     */
    async processFrameForFaces(frame) {
        if (this.isProcessingFrame) return;
        
        this.isProcessingFrame = true;
        const startTime = performance.now();
        
        try {
            // Send frame to server for face detection
            const faces = await this.detectFacesInFrame(frame);
            const detectedFaces = Array.isArray(faces) ? faces : [];
            
            // Update tracked faces for scan state, but do not render visual face boxes.
            this.updateTrackedFaces(detectedFaces);

            if (this.onFaceDetected && typeof this.onFaceDetected === 'function') {
                const visibleFaces = this.getVisibleTrackedFaces();
                this.onFaceDetected({
                    count: visibleFaces.length,
                    faces: visibleFaces.map((trackedFace) => ({
                        id: trackedFace.id,
                        trackId: trackedFace.serverTrackId || trackedFace.id,
                        x: Number((trackedFace.renderBox || trackedFace.detection || {}).x || 0),
                        y: Number((trackedFace.renderBox || trackedFace.detection || {}).y || 0),
                        width: Number((trackedFace.renderBox || trackedFace.detection || {}).width || 0),
                        height: Number((trackedFace.renderBox || trackedFace.detection || {}).height || 0),
                        confidence: Number((trackedFace.detection || {}).confidence || 0),
                        stability: Number(trackedFace.stabilityCount || 0),
                        recognized: Boolean(trackedFace.recognized),
                        studentId: trackedFace.studentId || null,
                        frameWidth: Number((trackedFace.detection || {}).frame_width || this.videoElement?.videoWidth || 0),
                        frameHeight: Number((trackedFace.detection || {}).frame_height || this.videoElement?.videoHeight || 0),
                        lastSeen: trackedFace.lastSeen || Date.now(),
                    })),
                    timestamp: Date.now()
                });
            }

            if (detectedFaces.length > 0) {
                
                // Handle multiple faces
                if (detectedFaces.length > 1) {
                    this.handleMultipleFaces(detectedFaces);
                }
                
                this.metrics.facesDetected += detectedFaces.length;
            }
            
        } catch (error) {
            console.debug('[EnhancedCamera] Frame processing error:', error.message);
        } finally {
            this.isProcessingFrame = false;
            
            // Update performance metrics
            const processingTime = performance.now() - startTime;
            this.updatePerformanceMetrics(processingTime);
        }
    }

    /**
     * Detect faces in frame using server-side processing
     */
    async detectFacesInFrame(frame) {
        try {
            const blob = await this.getFrameAsBlob('image/jpeg', 0.85);
            if (!blob) return [];
            
            const formData = new FormData();
            formData.append('frame', blob, 'frame.jpg');
            
            const timeout = this.createTimeoutSignal(1000);
            let response;
            try {
                response = await fetch('/detect_faces', {
                    method: 'POST',
                    body: formData,
                    signal: timeout.signal
                });
            } finally {
                timeout.cleanup();
            }
            
            if (!response.ok) return [];
            
            const result = await response.json();
            return result.faces || [];
            
        } catch (error) {
            // Don't log timeout errors as they're expected
            if (error.name !== 'TimeoutError') {
                console.debug('[EnhancedCamera] Face detection error:', error.message);
            }
            return [];
        }
    }

    /**
     * Update tracked faces with new detections
     */
    updateTrackedFaces(detectedFaces) {
        const now = Date.now();
        const updatedFaces = new Set();
        
        // Match detected faces with tracked faces
        for (const detectedFace of detectedFaces) {
            const serverTrackId = detectedFace.track_id || null;
            let matchedTrack = null;
            let minDistance = Infinity;

            if (serverTrackId) {
                for (const [trackId, trackedFace] of this.trackedFaces) {
                    if (trackedFace.serverTrackId === serverTrackId) {
                        matchedTrack = trackId;
                        break;
                    }
                }
            }
            
            // Find closest tracked face
            if (!matchedTrack) {
                for (const [trackId, trackedFace] of this.trackedFaces) {
                    const distance = this.calculateFaceDistance(detectedFace, trackedFace);
                    if (distance < minDistance && distance < this.faceTracker.trackingDistance) {
                        minDistance = distance;
                        matchedTrack = trackId;
                    }
                }
            }
            
            if (matchedTrack) {
                // Update existing track
                const trackedFace = this.trackedFaces.get(matchedTrack);
                trackedFace.lastSeen = now;
                trackedFace.detection = detectedFace;
                trackedFace.stabilityCount = (trackedFace.stabilityCount || 0) + 1;
                trackedFace.serverTrackId = serverTrackId || trackedFace.serverTrackId || null;
                trackedFace.renderBox = this.interpolateBox(
                    trackedFace.renderBox || detectedFace,
                    detectedFace,
                    this.trackSmoothingFactor
                );
                updatedFaces.add(matchedTrack);
            } else {
                // Create new track
                const trackId = this.faceTracker.nextId++;
                this.trackedFaces.set(trackId, {
                    id: trackId,
                    detection: detectedFace,
                    renderBox: { ...detectedFace },
                    firstSeen: now,
                    lastSeen: now,
                    stabilityCount: 1,
                    recognized: false,
                    studentId: null,
                    serverTrackId
                });
                updatedFaces.add(trackId);
            }
        }
        
        // Remove old tracks
        for (const [trackId, trackedFace] of this.trackedFaces) {
            if (!updatedFaces.has(trackId) && (now - trackedFace.lastSeen) > this.faceTracker.lostFaceTimeout) {
                this.trackedFaces.delete(trackId);
            }
        }
        
        // Limit maximum tracked faces
        if (this.trackedFaces.size > this.faceTracker.maxTrackedFaces) {
            const sortedFaces = Array.from(this.trackedFaces.entries())
                .sort((a, b) => b[1].lastSeen - a[1].lastSeen);
            
            const toRemove = sortedFaces.slice(this.faceTracker.maxTrackedFaces);
            for (const [trackId] of toRemove) {
                this.trackedFaces.delete(trackId);
            }
        }
    }

    interpolateBox(previousBox, nextBox, factor = 0.4) {
        const alpha = Math.min(Math.max(factor, 0), 1);
        return {
            ...nextBox,
            x: previousBox.x + (nextBox.x - previousBox.x) * alpha,
            y: previousBox.y + (nextBox.y - previousBox.y) * alpha,
            width: previousBox.width + (nextBox.width - previousBox.width) * alpha,
            height: previousBox.height + (nextBox.height - previousBox.height) * alpha
        };
    }

    getVisibleTrackedFaces() {
        const now = Date.now();
        return Array.from(this.trackedFaces.values()).filter((trackedFace) => (
            now - trackedFace.lastSeen <= this.trackFadeDuration
        ));
    }

    /**
     * Calculate distance between two face detections
     */
    calculateFaceDistance(face1, face2) {
        const center1 = {
            x: face1.x + face1.width / 2,
            y: face1.y + face1.height / 2
        };
        const center2 = {
            x: face2.detection.x + face2.detection.width / 2,
            y: face2.detection.y + face2.detection.height / 2
        };
        
        return Math.sqrt(
            Math.pow(center1.x - center2.x, 2) + 
            Math.pow(center1.y - center2.y, 2)
        );
    }

    /**
     * Handle multiple faces detection
     */
    handleMultipleFaces(faces) {
        if (this.onMultipleFaces && typeof this.onMultipleFaces === 'function') {
            this.onMultipleFaces({
                count: faces.length,
                faces: faces,
                timestamp: Date.now()
            });
        }
    }

    /**
     * Process individual detected face for recognition
     */
    async processDetectedFace(face) {
        // Find corresponding tracked face
        for (const [trackId, trackedFace] of this.trackedFaces) {
            if (trackedFace.detection === face) {
                // Only process stable faces
                if (trackedFace.stabilityCount < 3) return;
                
                // Only process if not recently recognized
                if (trackedFace.recognized) {
                    const timeSinceRecognition = Date.now() - trackedFace.lastRecognitionTime;
                    if (timeSinceRecognition < 5000) return; // 5 second cooldown
                }
                
                try {
                    // Send face for recognition
                    const recognitionResult = await this.recognizeFace(face);
                    
                    if (recognitionResult && recognitionResult.recognized) {
                        // Update tracked face with recognition info
                        trackedFace.recognized = true;
                        trackedFace.studentId = recognitionResult.student_id;
                        trackedFace.studentName = recognitionResult.name;
                        trackedFace.lastRecognitionTime = Date.now();
                        
                        // Trigger recognition callback
                        if (this.onFaceRecognized && typeof this.onFaceRecognized === 'function') {
                            this.onFaceRecognized({
                                ...recognitionResult,
                                trackId: trackId,
                                face: face,
                                timestamp: Date.now()
                            });
                        }
                        
                        // Trigger voice feedback
                        this.triggerVoiceFeedback(recognitionResult.name, recognitionResult.student_id);
                        
                        this.metrics.facesRecognized++;
                    }
                    
                } catch (error) {
                    console.debug('[EnhancedCamera] Face recognition error:', error.message);
                }
                
                break;
            }
        }
    }

    /**
     * Recognize face using server-side processing
     */
    async recognizeFace(face) {
        try {
            // Extract face region from frame
            const faceBlob = await this.extractFaceRegion(face);
            if (!faceBlob) return null;
            
            const formData = new FormData();
            formData.append('face', faceBlob, 'face.jpg');
            formData.append('face_id', face.id || 'unknown');
            
            const timeout = this.createTimeoutSignal(2000);
            let response;
            try {
                response = await fetch('/recognize_face', {
                    method: 'POST',
                    body: formData,
                    signal: timeout.signal
                });
            } finally {
                timeout.cleanup();
            }
            
            if (!response.ok) return null;
            
            return await response.json();
            
        } catch (error) {
            console.debug('[EnhancedCamera] Face recognition error:', error.message);
            return null;
        }
    }

    /**
     * Extract face region from frame
     */
    async extractFaceRegion(face) {
        try {
            const canvas = this.captureFrame();
            if (!canvas) return null;
            
            // Create face region canvas
            const faceCanvas = document.createElement('canvas');
            const faceCtx = faceCanvas.getContext('2d');
            
            // Add padding around face
            const padding = 20;
            const x = Math.max(0, face.x - padding);
            const y = Math.max(0, face.y - padding);
            const width = Math.min(canvas.width - x, face.width + padding * 2);
            const height = Math.min(canvas.height - y, face.height + padding * 2);
            
            faceCanvas.width = width;
            faceCanvas.height = height;
            
            // Copy face region
            faceCtx.drawImage(canvas, x, y, width, height, 0, 0, width, height);
            
            return new Promise(resolve => {
                faceCanvas.toBlob(resolve, 'image/jpeg', 0.90);
            });
            
        } catch (error) {
            console.debug('[EnhancedCamera] Face extraction error:', error.message);
            return null;
        }
    }

    /**
     * Trigger voice feedback with duplicate prevention
     */
    triggerVoiceFeedback(studentName, studentId) {
        const now = Date.now();
        const lastVoiceTime = this.lastVoiceTimes.get(studentId) || 0;
        
        // Check cooldown period
        if (now - lastVoiceTime < this.voiceCooldown) {
            return; // Skip due to cooldown
        }
        
        // Add to voice queue
        this.voiceQueue.push({
            name: studentName,
            studentId: studentId,
            timestamp: now
        });
        
        this.lastVoiceTimes.set(studentId, now);
        
        // Process voice queue
        this.processVoiceQueue();
    }

    /**
     * Process voice feedback queue
     */
    async processVoiceQueue() {
        if (this.isSpeaking || this.voiceQueue.length === 0) return;
        
        this.isSpeaking = true;
        
        try {
            const voiceItem = this.voiceQueue.shift();
            
            // Trigger voice callback
            if (this.onVoiceFeedback && typeof this.onVoiceFeedback === 'function') {
                await this.onVoiceFeedback(voiceItem.name, voiceItem.studentId);
            }
            
            // Wait for speech to complete
            await new Promise(resolve => setTimeout(resolve, 1500));
            
        } catch (error) {
            console.debug('[EnhancedCamera] Voice feedback error:', error.message);
        } finally {
            this.isSpeaking = false;
            
            // Process next item in queue
            if (this.voiceQueue.length > 0) {
                setTimeout(() => this.processVoiceQueue(), 100);
            }
        }
    }

    /**
     * Update performance metrics
     */
    updatePerformanceMetrics(processingTime) {
        const totalTime = Date.now() - this.metrics.startTime;
        const avgTime = this.metrics.averageProcessingTime || 0;
        
        // Calculate rolling average
        this.metrics.averageProcessingTime = (avgTime * 0.9) + (processingTime * 0.1);
    }

    /**
     * Get current performance metrics
     */
    getPerformanceMetrics() {
        const totalTime = Date.now() - this.metrics.startTime;
        const fps = totalTime > 0 ? (this.metrics.framesProcessed * 1000) / totalTime : 0;
        
        return {
            ...this.metrics,
            fps: Math.round(fps),
            trackedFaces: this.trackedFaces.size,
            uptime: totalTime
        };
    }

    /**
     * Capture current frame from video stream
     */
    captureFrame(canvas = null) {
        if (!this.isStreaming || !this.videoElement) {
            return null;
        }

        try {
            const videoWidth = Number(this.videoElement.videoWidth) || 0;
            const videoHeight = Number(this.videoElement.videoHeight) || 0;
            if (!videoWidth || !videoHeight || this.videoElement.readyState < HTMLMediaElement.HAVE_CURRENT_DATA) {
                return null;
            }

            // Reuse the same capture canvas between frames to avoid repeated DOM allocation.
            let targetCanvas = null;
            if (typeof canvas === 'string') {
                targetCanvas = document.getElementById(canvas);
            } else if (canvas) {
                targetCanvas = canvas;
            } else if (this.canvas) {
                targetCanvas = this.canvas;
            } else {
                targetCanvas = document.createElement('canvas');
            }
            if (!targetCanvas) return null;
            if (this.canvas !== targetCanvas) {
                this.canvas = targetCanvas;
                this.canvasContext = null;
            }

            // Set canvas size to match video
            if (this.canvas.width !== videoWidth || this.canvas.height !== videoHeight) {
                this.canvas.width = videoWidth;
                this.canvas.height = videoHeight;
            }

            // Get 2D context
            if (!this.canvasContext) {
                this.canvasContext = this.canvas.getContext('2d', {
                    alpha: false,
                    desynchronized: true,
                }) || this.canvas.getContext('2d');
            }

            // Draw current video frame to canvas
            this.canvasContext.drawImage(this.videoElement, 0, 0, videoWidth, videoHeight);

            return this.canvas;

        } catch (error) {
            if (this.isStreaming) {
                this.triggerError('Failed to capture frame: ' + error.message);
            }
            return null;
        }
    }

    /**
     * Get current frame as Blob for sending to server
     */
    async getFrameAsBlob(format = 'image/jpeg', quality = 0.95, options = {}) {
        try {
            const sourceCanvas = this.captureFrame();
            if (!sourceCanvas) return null;
            let exportCanvas = sourceCanvas;

            const maxWidth = Number(options?.maxWidth || 0) || 0;
            const maxHeight = Number(options?.maxHeight || 0) || 0;
            if (maxWidth > 0 || maxHeight > 0) {
                const sourceWidth = Number(sourceCanvas.width || 0) || 0;
                const sourceHeight = Number(sourceCanvas.height || 0) || 0;
                if (sourceWidth > 0 && sourceHeight > 0) {
                    const widthScale = maxWidth > 0 ? maxWidth / sourceWidth : 1;
                    const heightScale = maxHeight > 0 ? maxHeight / sourceHeight : 1;
                    const scale = Math.min(widthScale, heightScale, 1);
                    if (scale < 0.999) {
                        const targetWidth = Math.max(1, Math.round(sourceWidth * scale));
                        const targetHeight = Math.max(1, Math.round(sourceHeight * scale));
                        if (!this.uploadCanvas) {
                            this.uploadCanvas = document.createElement('canvas');
                        }
                        if (this.uploadCanvas.width !== targetWidth || this.uploadCanvas.height !== targetHeight) {
                            this.uploadCanvas.width = targetWidth;
                            this.uploadCanvas.height = targetHeight;
                        }
                        if (!this.uploadCanvasContext) {
                            this.uploadCanvasContext = this.uploadCanvas.getContext('2d', {
                                alpha: false,
                                desynchronized: true,
                            }) || this.uploadCanvas.getContext('2d');
                        }
                        this.uploadCanvasContext.drawImage(sourceCanvas, 0, 0, targetWidth, targetHeight);
                        exportCanvas = this.uploadCanvas;
                    }
                }
            }

            return new Promise((resolve, reject) => {
                exportCanvas.toBlob(
                    (blob) => {
                        if (blob) {
                            resolve(blob);
                        } else {
                            reject(new Error('Failed to convert canvas to blob'));
                        }
                    },
                    format,
                    quality
                );
            });

        } catch (error) {
            if (this.isStreaming) {
                this.triggerError('Failed to get frame as blob: ' + error.message);
            }
            return null;
        }
    }

    /**
     * Stop camera and cleanup all resources
     */
    stop() {
        try {
            // Stop face detection
            this.isStreaming = false;
            this.faceDetectionLoopActive = false;
            if (this.faceDetectionRafId) {
                cancelAnimationFrame(this.faceDetectionRafId);
                this.faceDetectionRafId = null;
            }
            this.suppressStreamEndedCallback = true;
            
            // Clear tracked faces
            this.trackedFaces.clear();
            this.uploadCanvas = null;
            this.uploadCanvasContext = null;
            
            // Clear voice queue
            this.voiceQueue = [];
            this.isSpeaking = false;
            
            // Stop camera stream
            if (this.stream) {
                this.unbindStreamLifecycleEvents();
                this.stream.getTracks().forEach(track => track.stop());
                this.stream = null;
            }

            // Stop video element
            if (this.videoElement) {
                this.videoElement.srcObject = null;
                this.videoElement.pause();
            }

            console.log('[EnhancedCamera] Camera stopped and cleaned up');

        } catch (error) {
            this.triggerError('Error stopping camera: ' + error.message);
        }
    }

    /**
     * Check if camera is currently streaming
     */
    isActive() {
        if (!this.isStreaming || !this.stream) return false;
        const tracks = this.stream.getVideoTracks();
        if (!tracks.length) return false;
        return tracks.some((track) => track.readyState === 'live');
    }

    /**
     * Get camera capabilities and settings
     */
    getSettings() {
        if (!this.stream) return null;

        try {
            const videoTrack = this.stream.getVideoTracks()[0];
            if (!videoTrack) return null;

            return {
                capabilities: videoTrack.getCapabilities ? videoTrack.getCapabilities() : null,
                settings: videoTrack.getSettings ? videoTrack.getSettings() : null,
                enabled: videoTrack.enabled
            };
        } catch (error) {
            return null;
        }
    }

    /**
     * Get user-friendly error message from error code
     */
    getErrorMessage(error) {
        if (!error) return 'Unknown camera error';

        if (error.name === 'NotAllowedError' || error.name === 'PermissionDeniedError') {
            return 'Camera access denied. Please grant camera permission in browser settings.';
        }
        if (error.name === 'NotFoundError' || error.name === 'DevicesNotFoundError') {
            return 'No camera device found on this device.';
        }
        if (error.name === 'NotReadableError' || error.name === 'TrackStartError') {
            return 'Camera is being used by another application.';
        }
        if (error.name === 'OverconstrainedError' || error.name === 'ConstraintError') {
            return 'Could not find camera matching your requirements.';
        }
        if (error.name === 'TypeError') {
            return 'Invalid camera constraints specified.';
        }

        return error.message || 'Camera error occurred';
    }

    /**
     * Trigger error callback
     */
    triggerError(message) {
        if (this.onError && typeof this.onError === 'function') {
            this.onError(message);
        } else {
            console.error('[EnhancedCamera] Error:', message);
        }
    }

    /**
     * Trigger success callback
     */
    triggerSuccess(message) {
        if (this.onSuccess && typeof this.onSuccess === 'function') {
            this.onSuccess(message);
        } else {
            console.log('[EnhancedCamera] Success:', message);
        }
    }
}

// Export for use in modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = EnhancedClientCamera;
}
