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
        this.overlayCanvas = null;
        this.overlayContext = null;
        this.isStreaming = false;
        this.onError = null;
        this.onSuccess = null;
        this.onFaceDetected = null;
        this.onFaceRecognized = null;
        this.onMultipleFaces = null;
        
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

            // Apply advanced camera settings if supported
            await this.optimizeCameraSettings();

            // Setup video element
            this.setupVideoElement();

            // Create overlay canvas for face detection visualization
            this.createOverlayCanvas();

            // Initialize face tracking
            this.initializeFaceTracking();

            this.isStreaming = true;
            this.triggerSuccess('Enhanced camera initialized successfully');
            
            // Start face detection loop
            this.startFaceDetectionLoop();
            
            return true;

        } catch (error) {
            const errorMessage = this.getErrorMessage(error);
            this.triggerError(errorMessage);
            return false;
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
            const onLoadedMetadata = () => {
                this.videoElement.removeEventListener('loadedmetadata', onLoadedMetadata);
                this.videoElement.play().then(resolve).catch(reject);
            };
            
            this.videoElement.addEventListener('loadedmetadata', onLoadedMetadata);
            
            // Timeout after 3 seconds (reduced from 5 for faster startup)
            setTimeout(() => {
                this.videoElement.removeEventListener('loadedmetadata', onLoadedMetadata);
                reject(new Error('Video initialization timeout'));
            }, 3000);
        });
    }

    /**
     * Create overlay canvas for face detection visualization
     */
    createOverlayCanvas() {
        // Create overlay canvas
        this.overlayCanvas = document.createElement('canvas');
        this.overlayContext = this.overlayCanvas.getContext('2d');
        
        // Position overlay over video
        const videoRect = this.videoElement.getBoundingClientRect();
        this.overlayCanvas.style.position = 'absolute';
        this.overlayCanvas.style.top = '0';
        this.overlayCanvas.style.left = '0';
        this.overlayCanvas.style.width = '100%';
        this.overlayCanvas.style.height = '100%';
        this.overlayCanvas.style.pointerEvents = 'none';
        this.overlayCanvas.style.zIndex = '10';
        
        // Add overlay to video container
        const videoContainer = this.videoElement.parentElement;
        if (videoContainer) {
            videoContainer.style.position = 'relative';
            videoContainer.appendChild(this.overlayCanvas);
        }
        
        // Set canvas size
        this.updateOverlayCanvasSize();
    }

    /**
     * Update overlay canvas size to match video
     */
    updateOverlayCanvasSize() {
        if (!this.overlayCanvas || !this.videoElement) return;
        
        const videoRect = this.videoElement.getBoundingClientRect();
        this.overlayCanvas.width = videoRect.width;
        this.overlayCanvas.height = videoRect.height;
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
        const detectFaces = async () => {
            if (!this.isStreaming) return;
            
            const now = Date.now();
            
            // Throttle face detection to maintain performance
            if (now - this.lastDetectionTime < this.detectionInterval) {
                requestAnimationFrame(detectFaces);
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
            requestAnimationFrame(detectFaces);
        };
        
        // Start the detection loop
        requestAnimationFrame(detectFaces);
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
            
            if (faces && faces.length > 0) {
                // Update tracked faces
                this.updateTrackedFaces(faces);
                
                // Draw face detection overlay
                this.drawFaceOverlay(faces);
                
                // Handle multiple faces
                if (faces.length > 1) {
                    this.handleMultipleFaces(faces);
                }
                
                this.metrics.facesDetected += faces.length;
            } else {
                // Clear overlay if no faces detected
                this.clearOverlay();
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
            
            const response = await fetch('/detect_faces', {
                method: 'POST',
                body: formData,
                signal: AbortSignal.timeout(1000) // 1 second timeout
            });
            
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
            let matchedTrack = null;
            let minDistance = Infinity;
            
            // Find closest tracked face
            for (const [trackId, trackedFace] of this.trackedFaces) {
                const distance = this.calculateFaceDistance(detectedFace, trackedFace);
                if (distance < minDistance && distance < this.faceTracker.trackingDistance) {
                    minDistance = distance;
                    matchedTrack = trackId;
                }
            }
            
            if (matchedTrack) {
                // Update existing track
                const trackedFace = this.trackedFaces.get(matchedTrack);
                trackedFace.lastSeen = now;
                trackedFace.detection = detectedFace;
                trackedFace.stabilityCount = (trackedFace.stabilityCount || 0) + 1;
                updatedFaces.add(matchedTrack);
            } else {
                // Create new track
                const trackId = this.faceTracker.nextId++;
                this.trackedFaces.set(trackId, {
                    id: trackId,
                    detection: detectedFace,
                    firstSeen: now,
                    lastSeen: now,
                    stabilityCount: 1,
                    recognized: false,
                    studentId: null
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
     * Draw face detection overlay with bounding boxes and names
     */
    drawFaceOverlay(detectedFaces) {
        if (!this.overlayContext) return;
        
        const canvas = this.overlayCanvas;
        const ctx = this.overlayContext;
        
        // Clear overlay
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        
        // Scale factors for overlay
        const scaleX = canvas.width / this.videoElement.videoWidth;
        const scaleY = canvas.height / this.videoElement.videoHeight;
        
        // Draw each tracked face
        for (const [trackId, trackedFace] of this.trackedFaces) {
            const face = trackedFace.detection;
            
            // Scale face coordinates to overlay size
            const x = face.x * scaleX;
            const y = face.y * scaleY;
            const width = face.width * scaleX;
            const height = face.height * scaleY;
            
            // Determine color based on recognition status
            let color = '#10b981'; // Green for recognized
            let lineWidth = 3;
            
            if (!trackedFace.recognized) {
                color = '#3b82f6'; // Blue for detected but not recognized
                lineWidth = 2;
            }
            
            if (trackedFace.stabilityCount < 3) {
                color = '#f59e0b'; // Amber for unstable tracking
                lineWidth = 2;
            }
            
            // Draw bounding box
            ctx.strokeStyle = color;
            ctx.lineWidth = lineWidth;
            ctx.strokeRect(x, y, width, height);
            
            // Draw corner accents for better visibility
            const cornerLength = 15;
            ctx.beginPath();
            // Top-left corner
            ctx.moveTo(x, y + cornerLength);
            ctx.lineTo(x, y);
            ctx.lineTo(x + cornerLength, y);
            // Top-right corner
            ctx.moveTo(x + width - cornerLength, y);
            ctx.lineTo(x + width, y);
            ctx.lineTo(x + width, y + cornerLength);
            // Bottom-left corner
            ctx.moveTo(x, y + height - cornerLength);
            ctx.lineTo(x, y + height);
            ctx.lineTo(x + cornerLength, y + height);
            // Bottom-right corner
            ctx.moveTo(x + width - cornerLength, y + height);
            ctx.lineTo(x + width, y + height);
            ctx.lineTo(x + width, y + height - cornerLength);
            ctx.stroke();
            
            // Draw student name if recognized
            if (trackedFace.recognized && trackedFace.studentName) {
                // Background for text
                ctx.fillStyle = 'rgba(0, 0, 0, 0.7)';
                const textMetrics = ctx.measureText(trackedFace.studentName);
                const textHeight = 20;
                const padding = 6;
                
                ctx.fillRect(
                    x, 
                    y - textHeight - padding * 2, 
                    textMetrics.width + padding * 2, 
                    textHeight + padding * 2
                );
                
                // Draw text
                ctx.fillStyle = '#ffffff';
                ctx.font = '14px system-ui, -apple-system, sans-serif';
                ctx.fillText(trackedFace.studentName, x + padding, y - padding);
            }
            
            // Draw confidence score if available
            if (face.confidence) {
                ctx.fillStyle = 'rgba(0, 0, 0, 0.7)';
                ctx.fillRect(x, y + height + 2, 60, 18);
                
                ctx.fillStyle = '#ffffff';
                ctx.font = '11px system-ui, -apple-system, sans-serif';
                ctx.fillText(`${(face.confidence * 100).toFixed(1)}%`, x + 4, y + height + 14);
            }
        }
    }

    /**
     * Clear overlay canvas
     */
    clearOverlay() {
        if (!this.overlayContext) return;
        this.overlayContext.clearRect(0, 0, this.overlayCanvas.width, this.overlayCanvas.height);
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
            
            const response = await fetch('/recognize_face', {
                method: 'POST',
                body: formData,
                signal: AbortSignal.timeout(2000) // 2 second timeout
            });
            
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
            this.triggerError('Camera not initialized or not streaming');
            return null;
        }

        try {
            // Get or create canvas
            if (typeof canvas === 'string') {
                this.canvas = document.getElementById(canvas);
            } else if (canvas) {
                this.canvas = canvas;
            } else {
                this.canvas = document.createElement('canvas');
            }

            // Set canvas size to match video
            this.canvas.width = this.videoElement.videoWidth;
            this.canvas.height = this.videoElement.videoHeight;

            // Get 2D context
            this.canvasContext = this.canvas.getContext('2d');

            // Draw current video frame to canvas
            this.canvasContext.drawImage(this.videoElement, 0, 0);

            return this.canvas;

        } catch (error) {
            this.triggerError('Failed to capture frame: ' + error.message);
            return null;
        }
    }

    /**
     * Get current frame as Blob for sending to server
     */
    async getFrameAsBlob(format = 'image/jpeg', quality = 0.95) {
        try {
            const canvas = this.captureFrame();
            if (!canvas) return null;

            return new Promise((resolve, reject) => {
                canvas.toBlob(
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
            this.triggerError('Failed to get frame as blob: ' + error.message);
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
            
            // Clear tracked faces
            this.trackedFaces.clear();
            
            // Clear voice queue
            this.voiceQueue = [];
            this.isSpeaking = false;
            
            // Stop camera stream
            if (this.stream) {
                this.stream.getTracks().forEach(track => track.stop());
                this.stream = null;
            }

            // Stop video element
            if (this.videoElement) {
                this.videoElement.srcObject = null;
                this.videoElement.pause();
            }

            // Remove overlay canvas
            if (this.overlayCanvas && this.overlayCanvas.parentElement) {
                this.overlayCanvas.parentElement.removeChild(this.overlayCanvas);
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
        return this.isStreaming;
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
