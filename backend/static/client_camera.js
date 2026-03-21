/**
 * ClientCamera - Browser-based camera access using MediaStream API
 * Accesses the camera on the client device, not the server
 */

class ClientCamera {
    constructor() {
        this.stream = null;
        this.videoElement = null;
        this.canvas = null;
        this.canvasContext = null;
        this.isStreaming = false;
        this.onError = null;
        this.onSuccess = null;
    }

    /**
     * Initialize camera access on the client device
     * @param {HTMLVideoElement|string} videoElement - Video element or element ID
     * @param {Object} constraints - Media stream constraints (optional)
     * @returns {Promise<boolean>} - True if successful, false otherwise
     */
    async initialize(videoElement, constraints = null) {
        if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
            const error = 'Camera API not supported in this browser';
            this.triggerError(error);
            return false;
        }

        const isSecureOrigin = (typeof window === 'undefined' || typeof location === 'undefined')
            ? true
            : (window.isSecureContext
                || location.protocol === 'https:'
                || ['localhost', '127.0.0.1'].includes(location.hostname));

        if (!isSecureOrigin) {
            const error = 'Camera access requires HTTPS (or localhost). Open the site over https:// to grant permission.';
            this.triggerError(error);
            return false;
        }

        try {
            // Get or find video element
            if (typeof videoElement === 'string') {
                this.videoElement = document.getElementById(videoElement);
            } else {
                this.videoElement = videoElement;
            }

            if (!this.videoElement) {
                throw new Error('Video element not found');
            }

            // Default constraints for accessing client camera
            const mediaConstraints = constraints || {
                video: {
                    width: { ideal: 1280 },
                    height: { ideal: 720 },
                    facingMode: 'user'
                },
                audio: false
            };

            // Request camera access from CLIENT device
            this.stream = await navigator.mediaDevices.getUserMedia(mediaConstraints);

            // Try to enable continuous focus if supported
            try {
                const track = this.stream.getVideoTracks()[0];
                if (track && typeof track.getCapabilities === 'function') {
                    const capabilities = track.getCapabilities();
                    if (capabilities.focusMode && capabilities.focusMode.includes('continuous')) {
                        await track.applyConstraints({
                            advanced: [{ focusMode: 'continuous' }]
                        });
                        console.log('[ClientCamera] Continuous focus enabled');
                    }
                }
            } catch (e) {
                console.debug('[ClientCamera] Focus mode capability check failed:', e.message);
            }

            // Attach stream to video element
            if (this.videoElement.srcObject !== undefined) {
                this.videoElement.srcObject = this.stream;
            } else {
                // Fallback for older browsers
                this.videoElement.src = URL.createObjectURL(this.stream);
            }

            // Wait for video to load and start playing
            await new Promise((resolve, reject) => {
                const onLoadedMetadata = () => {
                    this.videoElement.removeEventListener('loadedmetadata', onLoadedMetadata);
                    this.videoElement.play().then(resolve).catch(reject);
                };
                this.videoElement.addEventListener('loadedmetadata', onLoadedMetadata);

                // Timeout after 5 seconds
                setTimeout(() => {
                    this.videoElement.removeEventListener('loadedmetadata', onLoadedMetadata);
                    reject(new Error('Video initialization timeout'));
                }, 5000);
            });

            this.isStreaming = true;
            this.triggerSuccess('Camera initialized successfully');
            return true;

        } catch (error) {
            const errorMessage = this.getErrorMessage(error);
            this.triggerError(errorMessage);
            return false;
        }
    }

    /**
     * Capture current frame from video stream as image data
     * @param {HTMLCanvasElement|string} canvas - Canvas element or element ID
     * @returns {HTMLCanvasElement|null} - Canvas with frame drawn, or null if error
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
                // Create temporary canvas if not provided
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
     * @param {string} format - Image format ('image/jpeg' or 'image/png')
     * @returns {Promise<Blob|null>} - Frame as Blob, or null if error
     */
    async getFrameAsBlob(format = 'image/jpeg') {
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
                    0.95
                );
            });

        } catch (error) {
            this.triggerError('Failed to get frame as blob: ' + error.message);
            return null;
        }
    }

    /**
     * Get current frame as Base64 data URL
     * @param {string} format - Image format ('image/jpeg' or 'image/png')
     * @returns {string} - Data URL string
     */
    getFrameAsDataURL(format = 'image/jpeg') {
        try {
            const canvas = this.captureFrame();
            if (!canvas) return null;
            return canvas.toDataURL(format, 0.95);
        } catch (error) {
            this.triggerError('Failed to get frame as data URL: ' + error.message);
            return null;
        }
    }

    /**
     * Stop camera stream and cleanup resources
     */
    stop() {
        try {
            if (this.stream) {
                this.stream.getTracks().forEach(track => track.stop());
                this.stream = null;
            }

            if (this.videoElement) {
                this.videoElement.srcObject = null;
                this.videoElement.pause();
            }

            this.isStreaming = false;

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

        // Handle different types of errors
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
            console.error('[ClientCamera] Error:', message);
        }
    }

    /**
     * Trigger success callback
     */
    triggerSuccess(message) {
        if (this.onSuccess && typeof this.onSuccess === 'function') {
            this.onSuccess(message);
        } else {
            console.log('[ClientCamera] Success:', message);
        }
    }
}

// Export for use in modules
if (typeof module !== 'undefined' && module.exports) {
    module.exports = ClientCamera;
}
