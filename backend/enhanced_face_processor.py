#!/usr/bin/env python3
"""
Enhanced Face Detection and Recognition Backend
Optimized for high-volume multi-face scanning with tracking and performance monitoring
"""

import cv2
import numpy as np
import face_recognition
import json
import time
import threading
from datetime import datetime, timedelta
from collections import defaultdict, deque
from flask import request, jsonify
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Demo mode for testing without face photos
DEMO_MODE = True
DEMO_STUDENTS = [
    {"student_id": "120526180006", "name": "ARADAN, LOUIS MIGUEL SITOY"},
    {"student_id": "120507180005", "name": "AUJERO, IYAN ARDIENTE"}, 
    {"student_id": "120508130014", "name": "BALIGASA, RICKY AURILIO"}
]


class EnhancedFaceProcessor:
    def __init__(self):
        # Face detection models
        self.face_detector = None
        self.face_encoder = None
        
        # Performance optimization
        self.detection_scale = 0.25  # Scale down for faster detection
        self.recognition_scale = 0.5  # Scale for recognition
        self.max_face_size = 800
        self.min_face_size = 50
        
        # Multi-face tracking
        self.tracked_faces = {}
        self.next_track_id = 1
        self.face_tracks = defaultdict(dict)
        self.max_tracks = 20
        self.track_timeout = 5.0  # seconds
        
        # Performance monitoring
        self.metrics = {
            'detections': 0,
            'recognitions': 0,
            'processing_times': deque(maxlen=100),
            'fps': 0,
            'last_frame_time': 0
        }
        
        # Recognition cache for performance
        self.recognition_cache = {}
        self.cache_timeout = 30  # seconds
        
        # Duplicate prevention
        self.recent_recognitions = {}
        self.recognition_cooldown = 3.0  # seconds
        
        # Initialize models
        self.initialize_models()
        
        # Start cleanup thread
        self.start_cleanup_thread()
    
    def initialize_models(self):
        """Initialize face detection and recognition models"""
        try:
            # Use HOG for faster detection (can switch to CNN for accuracy)
            self.face_detector = 'hog'  # or 'cnn'
            
            # Initialize face encoding model
            # Load known face encodings from database
            self.load_known_faces()
            
            logger.info("Enhanced face processor initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize face processor: {e}")
            raise
    
    def load_known_faces(self):
        """Load known face encodings from student database"""
        try:
            from pymongo import MongoClient
            from config import MONGO_URI, DB_NAME
            
            client = MongoClient(MONGO_URI)
            db = client[DB_NAME]
            
            # Get current school year students
            current_school_year = "2025-2026"
            students_collection = db.get_collection("students")
            
            # Load students with face data
            students = list(students_collection.find({
                "face_data": {"$exists": True, "$ne": []},
                "face_registered": True,
                "school_year": current_school_year
            }))
            
            self.known_encodings = []
            self.known_students = []
            
            for student in students:
                try:
                    # Load face encodings
                    face_data = student.get('face_data', [])
                    if face_data:
                        # Convert stored encodings back to numpy array
                        for encoding_data in face_data:
                            encoding = np.array(encoding_data['encoding'])
                            self.known_encodings.append(encoding)
                            self.known_students.append({
                                'student_id': student['student_id'],
                                'name': student['name'],
                                'grade_level': student.get('grade_level', ''),
                                'section': student.get('section', '')
                            })
                except Exception as e:
                    logger.warning(f"Failed to load face data for student {student.get('student_id')}: {e}")
            
            self.known_encodings = np.array(self.known_encodings) if self.known_encodings else np.empty((0, 128))
            
            logger.info(f"Loaded {len(self.known_encodings)} face encodings for {len(self.known_students)} students")
            
            client.close()
            
        except Exception as e:
            logger.error(f"Failed to load known faces: {e}")
            self.known_encodings = np.empty((0, 128))
            self.known_students = []
    
    def detect_faces(self, frame_bytes):
        """Detect faces in frame with optimized performance"""
        start_time = time.time()
        
        try:
            # Demo mode - return mock faces for testing
            if DEMO_MODE:
                import random
                num_faces = random.randint(1, 3)  # Random 1-3 faces
                
                demo_faces = []
                for i in range(num_faces):
                    # Random face position
                    x = random.randint(100, 400)
                    y = random.randint(100, 300)
                    width = random.randint(80, 150)
                    height = random.randint(100, 180)
                    
                    demo_faces.append({
                        "id": f"demo_face_{i}",
                        "x": x,
                        "y": y, 
                        "width": width,
                        "height": height,
                        "confidence": round(random.uniform(0.85, 0.98), 2),
                        "landmarks": [],
                        "embedding": []
                    })
                
                return {
                    'faces': demo_faces, 
                    'processing_time': time.time() - start_time,
                    'demo_mode': True
                }
            
            # Decode image
            nparr = np.frombuffer(frame_bytes, np.uint8)
            frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if frame is None or frame.size == 0:
                return {'faces': [], 'error': 'Invalid frame'}
            
            # Resize for faster detection
            height, width = frame.shape[:2]
            if width > self.max_face_size:
                scale = self.max_face_size / width
                frame = cv2.resize(frame, (self.max_face_size, int(height * scale)))
            
            # Convert to RGB for face_recognition
            rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            
            # Detect faces
            face_locations = face_recognition.face_locations(
                rgb_frame, 
                model=self.face_detector,
                number_of_times_to_upsample=1
            )
            
            # Detect face encodings for recognition
            face_encodings = face_recognition.face_encodings(
                rgb_frame, 
                face_locations,
                num_jitters=1  # Reduce for speed
            )
            
            faces = []
            for i, (location, encoding) in enumerate(zip(face_locations, face_encodings)):
                top, right, bottom, left = location
                
                # Filter by size
                face_width = right - left
                face_height = bottom - top
                if face_width < self.min_face_size or face_height < self.min_face_size:
                    continue
                
                face_data = {
                    'id': f'face_{i}_{int(time.time() * 1000)}',
                    'x': int(left),
                    'y': int(top),
                    'width': int(face_width),
                    'height': int(face_height),
                    'encoding': encoding.tolist(),
                    'confidence': self.calculate_face_confidence(encoding),
                    'timestamp': time.time()
                }
                
                faces.append(face_data)
            
            # Update metrics
            processing_time = time.time() - start_time
            self.metrics['processing_times'].append(processing_time)
            self.metrics['detections'] += len(faces)
            
            # Calculate FPS
            current_time = time.time()
            if self.metrics['last_frame_time'] > 0:
                frame_time = current_time - self.metrics['last_frame_time']
                self.metrics['fps'] = 1.0 / frame_time
            self.metrics['last_frame_time'] = current_time
            
            return {'faces': faces, 'processing_time': processing_time}
            
        except Exception as e:
            logger.error(f"Face detection error: {e}")
            return {'faces': [], 'error': str(e)}
    
    def calculate_face_confidence(self, encoding):
        """Calculate confidence score for face detection"""
        try:
            if len(self.known_encodings) == 0:
                return 0.5
            
            # Compare with known faces
            distances = face_recognition.face_distance(self.known_encodings, encoding)
            min_distance = np.min(distances) if len(distances) > 0 else 1.0
            
            # Convert distance to confidence (lower distance = higher confidence)
            confidence = max(0, 1.0 - min_distance)
            return min(confidence, 1.0)
            
        except Exception:
            return 0.5
    
    def recognize_face(self, face_data, frame_bytes=None):
        """Recognize face with caching and duplicate prevention"""
        start_time = time.time()
        
        try:
            # Demo mode - return mock recognition
            if DEMO_MODE:
                import random
                demo_student = random.choice(DEMO_STUDENTS)
                
                return {
                    'recognized': True,
                    'student_id': demo_student["student_id"],
                    'name': demo_student["name"],
                    'confidence': round(random.uniform(0.90, 0.99), 2),
                    'match_distance': round(random.uniform(0.1, 0.3), 3),
                    'demo_mode': True
                }
            
            face_id = face_data.get('id', 'unknown')
            encoding = np.array(face_data.get('encoding', []))
            
            if len(encoding) == 0:
                return {'recognized': False, 'error': 'No encoding provided'}
            
            # Check recognition cache
            cache_key = hash(encoding.tobytes())
            current_time = time.time()
            
            if cache_key in self.recognition_cache:
                cached_result = self.recognition_cache[cache_key]
                if current_time - cached_result['timestamp'] < self.cache_timeout:
                    return cached_result['result']
            
            # Check duplicate prevention
            if face_id in self.recent_recognitions:
                last_recognition = self.recent_recognitions[face_id]
                if current_time - last_recognition < self.recognition_cooldown:
                    return {'recognized': False, 'duplicate': True}
            
            # Perform recognition
            if len(self.known_encodings) == 0:
                return {'recognized': False, 'error': 'No known faces loaded'}
            
            # Compare with known faces
            distances = face_recognition.face_distance(self.known_encodings, encoding)
            
            # Find best match
            min_distance = np.min(distances)
            best_match_index = np.argmin(distances)
            
            # Recognition threshold (adjustable based on requirements)
            threshold = 0.6
            
            if min_distance < threshold:
                # Face recognized
                student = self.known_students[best_match_index]
                result = {
                    'recognized': True,
                    'student_id': student['student_id'],
                    'name': student['name'],
                    'grade_level': student['grade_level'],
                    'section': student['section'],
                    'confidence': float(1.0 - min_distance),
                    'distance': float(min_distance),
                    'timestamp': current_time
                }
                
                # Update recent recognitions
                self.recent_recognitions[face_id] = current_time
                
                # Cache result
                self.recognition_cache[cache_key] = {
                    'result': result,
                    'timestamp': current_time
                }
                
                # Update metrics
                self.metrics['recognitions'] += 1
                
                logger.info(f"Face recognized: {student['name']} ({student['student_id']}) - Confidence: {result['confidence']:.2f}")
                
                return result
            else:
                # Face not recognized
                result = {
                    'recognized': False,
                    'confidence': float(1.0 - min_distance),
                    'distance': float(min_distance),
                    'timestamp': current_time
                }
                
                # Cache result
                self.recognition_cache[cache_key] = {
                    'result': result,
                    'timestamp': current_time
                }
                
                return result
                
        except Exception as e:
            logger.error(f"Face recognition error: {e}")
            return {'recognized': False, 'error': str(e)}
        finally:
            # Update metrics
            processing_time = time.time() - start_time
            self.metrics['processing_times'].append(processing_time)
    
    def update_tracking(self, faces):
        """Update face tracking with new detections"""
        current_time = time.time()
        updated_tracks = {}
        
        # Match new faces with existing tracks
        for face in faces:
            best_track_id = None
            best_distance = float('inf')
            
            # Find closest existing track
            for track_id, track_data in self.face_tracks.items():
                if current_time - track_data['last_seen'] > self.track_timeout:
                    continue
                
                # Calculate distance between face centers
                face_center_x = face['x'] + face['width'] / 2
                face_center_y = face['y'] + face['height'] / 2
                
                track_center_x = track_data['last_x'] + track_data['last_width'] / 2
                track_center_y = track_data['last_y'] + track_data['last_height'] / 2
                
                distance = np.sqrt((face_center_x - track_center_x)**2 + (face_center_y - track_center_y)**2)
                
                if distance < best_distance and distance < 100:  # 100 pixel threshold
                    best_distance = distance
                    best_track_id = track_id
            
            if best_track_id:
                # Update existing track
                updated_tracks[best_track_id] = {
                    **self.face_tracks[best_track_id],
                    'last_seen': current_time,
                    'last_x': face['x'],
                    'last_y': face['y'],
                    'last_width': face['width'],
                    'last_height': face['height'],
                    'stability': self.face_tracks[best_track_id].get('stability', 0) + 1
                }
            else:
                # Create new track
                new_track_id = self.next_track_id
                self.next_track_id += 1
                
                updated_tracks[new_track_id] = {
                    'first_seen': current_time,
                    'last_seen': current_time,
                    'last_x': face['x'],
                    'last_y': face['y'],
                    'last_width': face['width'],
                    'last_height': face['height'],
                    'stability': 1,
                    'recognized': False,
                    'student_id': None
                }
        
        # Remove old tracks
        for track_id, track_data in self.face_tracks.items():
            if current_time - track_data['last_seen'] <= self.track_timeout:
                updated_tracks[track_id] = track_data
        
        # Limit number of tracks
        if len(updated_tracks) > self.max_tracks:
            # Keep most recent tracks
            sorted_tracks = sorted(updated_tracks.items(), key=lambda x: x[1]['last_seen'], reverse=True)
            updated_tracks = dict(sorted_tracks[:self.max_tracks])
        
        self.face_tracks = updated_tracks
        
        # Add track IDs to faces
        for face in faces:
            face['track_id'] = self.find_track_for_face(face, current_time)
    
    def find_track_for_face(self, face, current_time):
        """Find track ID for a face"""
        face_center_x = face['x'] + face['width'] / 2
        face_center_y = face['y'] + face['height'] / 2
        
        for track_id, track_data in self.face_tracks.items():
            if current_time - track_data['last_seen'] > self.track_timeout:
                continue
            
            track_center_x = track_data['last_x'] + track_data['last_width'] / 2
            track_center_y = track_data['last_y'] + track_data['last_height'] / 2
            
            distance = np.sqrt((face_center_x - track_center_x)**2 + (face_center_y - track_center_y)**2)
            
            if distance < 100:  # 100 pixel threshold
                return track_id
        
        return None
    
    def get_metrics(self):
        """Get current performance metrics"""
        processing_times = list(self.metrics['processing_times'])
        
        return {
            'detections': self.metrics['detections'],
            'recognitions': self.metrics['recognitions'],
            'fps': self.metrics['fps'],
            'active_tracks': len(self.face_tracks),
            'average_processing_time': np.mean(processing_times) if processing_times else 0,
            'cache_size': len(self.recognition_cache),
            'known_faces': len(self.known_encodings)
        }
    
    def start_cleanup_thread(self):
        """Start background thread for cleanup tasks"""
        def cleanup():
            while True:
                try:
                    current_time = time.time()
                    
                    # Clean old recognition cache entries
                    expired_cache = [
                        key for key, value in self.recognition_cache.items()
                        if current_time - value['timestamp'] > self.cache_timeout
                    ]
                    for key in expired_cache:
                        del self.recognition_cache[key]
                    
                    # Clean old recent recognitions
                    expired_recognitions = [
                        face_id for face_id, timestamp in self.recent_recognitions.items()
                        if current_time - timestamp > self.recognition_cooldown * 2
                    ]
                    for face_id in expired_recognitions:
                        del self.recent_recognitions[face_id]
                    
                    # Clean old tracks
                    expired_tracks = [
                        track_id for track_id, track_data in self.face_tracks.items()
                        if current_time - track_data['last_seen'] > self.track_timeout
                    ]
                    for track_id in expired_tracks:
                        del self.face_tracks[track_id]
                    
                    time.sleep(10)  # Cleanup every 10 seconds
                    
                except Exception as e:
                    logger.error(f"Cleanup error: {e}")
                    time.sleep(10)
        
        cleanup_thread = threading.Thread(target=cleanup, daemon=True)
        cleanup_thread.start()

# Global instance
face_processor = EnhancedFaceProcessor()

def detect_faces_endpoint():
    """Endpoint for face detection"""
    try:
        if 'frame' not in request.files:
            return jsonify({'status': 'error', 'message': 'No frame provided'}), 400
        
        frame_file = request.files['frame']
        frame_bytes = frame_file.read()
        
        if not frame_bytes:
            return jsonify({'status': 'error', 'message': 'Empty frame'}), 400
        
        # Detect faces
        result = face_processor.detect_faces(frame_bytes)
        
        # Update tracking
        if 'faces' in result:
            face_processor.update_tracking(result['faces'])
        
        return jsonify({
            'status': 'ok',
            'faces': result.get('faces', []),
            'processing_time': result.get('processing_time', 0),
            'metrics': face_processor.get_metrics()
        })
        
    except Exception as e:
        logger.error(f"Face detection endpoint error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

def recognize_face_endpoint():
    """Endpoint for face recognition"""
    try:
        if 'face' not in request.files:
            return jsonify({'status': 'error', 'message': 'No face provided'}), 400
        
        face_file = request.files['face']
        face_bytes = face_file.read()
        
        if not face_bytes:
            return jsonify({'status': 'error', 'message': 'Empty face data'}), 400
        
        # Get face data from request
        face_data = {}
        if request.form.get('face_data'):
            face_data = json.loads(request.form.get('face_data'))
        
        # Recognize face
        result = face_processor.recognize_face(face_data, face_bytes)
        
        return jsonify({
            'status': 'ok',
            'result': result,
            'metrics': face_processor.get_metrics()
        })
        
    except Exception as e:
        logger.error(f"Face recognition endpoint error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

def get_metrics_endpoint():
    """Endpoint for performance metrics"""
    try:
        return jsonify({
            'status': 'ok',
            'metrics': face_processor.get_metrics()
        })
    except Exception as e:
        logger.error(f"Metrics endpoint error: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500
