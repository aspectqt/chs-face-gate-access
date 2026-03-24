#!/usr/bin/env python3
"""
Enhanced Database Integration for High-Volume Scanning
Optimized for duplicate prevention and high-performance data storage
"""

import time
import threading
import hashlib
import json
from datetime import datetime, timedelta
from pymongo import MongoClient, errors
from pymongo.collection import Collection
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class EnhancedDatabaseIntegration:
    def __init__(self):
        # Database connection
        self.client = None
        self.db = None
        
        # Collections
        self.attendance_logs = None
        self.scan_events = None
        self.students = None
        
        # Duplicate prevention
        self.duplicate_cache = {}
        self.duplicate_window = 300  # 5 minutes
        self.cache_cleanup_interval = 600  # 10 minutes
        self.last_cache_cleanup = time.time()
        
        # Performance optimization
        self.batch_operations = {
            'attendance_logs': [],
            'scan_events': [],
            'batch_size': 50,
            'flush_interval': 5  # seconds
        }
        
        # Connection pooling
        self.connection_pool = {
            'max_connections': 10,
            'active_connections': 0,
            'connection_timeout': 30
        }
        
        # Error handling
        self.error_handler = {
            'max_retries': 3,
            'retry_delay': 1.0,
            'consecutive_errors': 0,
            'last_error_time': 0
        }
        
        # Metrics
        self.metrics = {
            'records_processed': 0,
            'duplicates_prevented': 0,
            'batch_operations': 0,
            'errors': 0,
            'average_write_time': 0
        }
        
        # Initialize database
        self.initialize_database()
        
        # Start background threads
        self.start_background_threads()
    
    def initialize_database(self):
        """Initialize database connection and collections"""
        try:
            from config import MONGO_URI, DB_NAME
            
            # Connect with optimized settings
            self.client = MongoClient(
                MONGO_URI,
                maxPoolSize=self.connection_pool['max_connections'],
                serverSelectionTimeoutMS=self.connection_pool['connection_timeout'] * 1000,
                connectTimeoutMS=self.connection_pool['connection_timeout'] * 1000,
                socketTimeoutMS=self.connection_pool['connection_timeout'] * 1000
            )
            
            self.db = self.client[DB_NAME]
            
            # Get collections with school year awareness
            from app import get_attendance_logs_storage, get_students_collection
            
            # Use current school year (2025-2026)
            current_school_year = "2025-2026"
            
            self.attendance_logs, _, _ = get_attendance_logs_storage(current_school_year)
            self.students = get_students_collection(current_school_year)
            self.scan_events = self.db['scan_events']
            
            # Create indexes for performance
            self.create_indexes()
            
            logger.info("Database integration initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def create_indexes(self):
        """Create performance indexes"""
        try:
            # Attendance logs indexes
            self.attendance_logs.create_index([("student_id", 1), ("timestamp", -1)])
            self.attendance_logs.create_index([("date", 1), ("session_type", 1)])
            self.attendance_logs.create_index([("scan_hash", 1)], unique=True, sparse=True)
            
            # Scan events indexes
            self.scan_events.create_index([("timestamp", -1)])
            self.scan_events.create_index([("student_id", 1)])
            self.scan_events.create_index([("event_type", 1)])
            
            # Students indexes
            self.students.create_index([("student_id", 1)], unique=True)
            self.students.create_index([("school_year", 1)])
            
            logger.info("Database indexes created")
            
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
    
    def generate_scan_hash(self, scan_data: Dict) -> str:
        """Generate unique hash for scan to prevent duplicates"""
        # Create hash from key fields
        hash_data = {
            'student_id': scan_data.get('student_id'),
            'timestamp': scan_data.get('timestamp'),
            'session_type': scan_data.get('session_type'),
            'date': scan_data.get('date')
        }
        
        hash_string = json.dumps(hash_data, sort_keys=True)
        return hashlib.sha256(hash_string.encode()).hexdigest()
    
    def is_duplicate_scan(self, scan_data: Dict) -> Tuple[bool, str]:
        """Check if scan is a duplicate"""
        scan_hash = self.generate_scan_hash(scan_data)
        current_time = time.time()
        
        # Check cache
        if scan_hash in self.duplicate_cache:
            cached_time = self.duplicate_cache[scan_hash]
            if current_time - cached_time < self.duplicate_window:
                return True, f"Duplicate scan (hash: {scan_hash[:8]}...)"
        
        # Check database
        try:
            existing = self.attendance_logs.find_one({"scan_hash": scan_hash})
            if existing:
                # Add to cache
                self.duplicate_cache[scan_hash] = current_time
                return True, f"Duplicate scan in database (hash: {scan_hash[:8]}...)"
        
        except Exception as e:
            logger.error(f"Error checking duplicate in database: {e}")
        
        # Not a duplicate
        self.duplicate_cache[scan_hash] = current_time
        return False, ""
    
    def record_attendance(self, scan_data: Dict) -> Dict:
        """Record attendance with duplicate prevention"""
        start_time = time.time()
        
        try:
            # Check for duplicates
            is_duplicate, duplicate_reason = self.is_duplicate_scan(scan_data)
            if is_duplicate:
                self.metrics['duplicates_prevented'] += 1
                return {
                    'status': 'duplicate',
                    'reason': duplicate_reason,
                    'scan_hash': self.generate_scan_hash(scan_data)
                }
            
            # Add metadata
            scan_data['scan_hash'] = self.generate_scan_hash(scan_data)
            scan_data['recorded_at'] = datetime.now()
            scan_data['school_year'] = "2025-2026"
            
            # Add to batch for performance
            self.batch_operations['attendance_logs'].append(scan_data)
            
            # Check if batch should be flushed
            if len(self.batch_operations['attendance_logs']) >= self.batch_operations['batch_size']:
                self.flush_attendance_batch()
            
            self.metrics['records_processed'] += 1
            
            processing_time = time.time() - start_time
            self.update_performance_metrics(processing_time)
            
            return {
                'status': 'success',
                'scan_hash': scan_data['scan_hash'],
                'processing_time': processing_time
            }
            
        except Exception as e:
            self.metrics['errors'] += 1
            logger.error(f"Error recording attendance: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def record_scan_event(self, event_data: Dict) -> Dict:
        """Record scan event"""
        try:
            # Add metadata
            event_data['timestamp'] = datetime.now()
            event_data['school_year'] = "2025-2026"
            
            # Add to batch
            self.batch_operations['scan_events'].append(event_data)
            
            # Check if batch should be flushed
            if len(self.batch_operations['scan_events']) >= self.batch_operations['batch_size']:
                self.flush_scan_events_batch()
            
            return {
                'status': 'success',
                'event_id': str(event_data.get('_id', ''))
            }
            
        except Exception as e:
            self.metrics['errors'] += 1
            logger.error(f"Error recording scan event: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def flush_attendance_batch(self):
        """Flush batch of attendance records"""
        if not self.batch_operations['attendance_logs']:
            return
        
        try:
            batch = self.batch_operations['attendance_logs'].copy()
            self.batch_operations['attendance_logs'].clear()
            
            # Perform bulk insert
            result = self.attendance_logs.insert_many(batch, ordered=False)
            
            self.metrics['batch_operations'] += 1
            logger.info(f"Flushed {len(result.inserted_ids)} attendance records")
            
        except errors.BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
            # Handle partial success
            for error in e.details['writeErrors']:
                logger.error(f"Write error: {error}")
        except Exception as e:
            logger.error(f"Error flushing attendance batch: {e}")
            # Re-add items to batch for retry
            self.batch_operations['attendance_logs'].extend(batch)
    
    def flush_scan_events_batch(self):
        """Flush batch of scan events"""
        if not self.batch_operations['scan_events']:
            return
        
        try:
            batch = self.batch_operations['scan_events'].copy()
            self.batch_operations['scan_events'].clear()
            
            # Perform bulk insert
            result = self.scan_events.insert_many(batch, ordered=False)
            
            logger.info(f"Flushed {len(result.inserted_ids)} scan events")
            
        except errors.BulkWriteError as e:
            logger.error(f"Bulk write error: {e.details}")
        except Exception as e:
            logger.error(f"Error flushing scan events batch: {e}")
            # Re-add items to batch for retry
            self.batch_operations['scan_events'].extend(batch)
    
    def flush_all_batches(self):
        """Flush all pending batches"""
        self.flush_attendance_batch()
        self.flush_scan_events_batch()
    
    def cleanup_duplicate_cache(self):
        """Clean up old entries from duplicate cache"""
        current_time = time.time()
        cutoff_time = current_time - self.duplicate_window
        
        # Remove old entries
        old_hashes = [
            hash_key for hash_key, timestamp in self.duplicate_cache.items()
            if timestamp < cutoff_time
        ]
        
        for hash_key in old_hashes:
            del self.duplicate_cache[hash_key]
        
        self.last_cache_cleanup = current_time
        
        if old_hashes:
            logger.debug(f"Cleaned up {len(old_hashes)} old duplicate cache entries")
    
    def update_performance_metrics(self, processing_time: float):
        """Update performance metrics"""
        current_avg = self.metrics['average_write_time']
        count = self.metrics['records_processed']
        
        # Calculate rolling average
        self.metrics['average_write_time'] = (current_avg * (count - 1) + processing_time) / count
    
    def get_student_info(self, student_id: str) -> Optional[Dict]:
        """Get student information with caching"""
        try:
            student = self.students.find_one({"student_id": student_id})
            if student:
                # Convert ObjectId to string
                student['_id'] = str(student['_id'])
                return student
            return None
            
        except Exception as e:
            logger.error(f"Error getting student info: {e}")
            return None
    
    def get_attendance_stats(self, date: str = None) -> Dict:
        """Get attendance statistics"""
        try:
            if not date:
                date = datetime.now().strftime("%Y-%m-%d")
            
            # Get today's attendance
            pipeline = [
                {"$match": {"date": date}},
                {"$group": {
                    "_id": "$session_type",
                    "count": {"$sum": 1},
                    "unique_students": {"$addToSet": "$student_id"}
                }},
                {"$project": {
                    "session_type": "$_id",
                    "count": 1,
                    "unique_count": {"$size": "$unique_students"},
                    "_id": 0
                }}
            ]
            
            results = list(self.attendance_logs.aggregate(pipeline))
            
            stats = {
                'date': date,
                'total_scans': sum(r['count'] for r in results),
                'unique_students': len(set(
                    student for r in results 
                    for student in self.students.find({}, {"student_id": 1})
                )),
                'by_session': {r['session_type']: r for r in results}
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting attendance stats: {e}")
            return {}
    
    def get_performance_metrics(self) -> Dict:
        """Get performance metrics"""
        return {
            'records_processed': self.metrics['records_processed'],
            'duplicates_prevented': self.metrics['duplicates_prevented'],
            'batch_operations': self.metrics['batch_operations'],
            'errors': self.metrics['errors'],
            'average_write_time': self.metrics['average_write_time'],
            'cache_size': len(self.duplicate_cache),
            'pending_attendance': len(self.batch_operations['attendance_logs']),
            'pending_scan_events': len(self.batch_operations['scan_events'])
        }
    
    def start_background_threads(self):
        """Start background threads for maintenance"""
        # Batch flush thread
        flush_thread = threading.Thread(target=self.batch_flush_loop, daemon=True)
        flush_thread.start()
        
        # Cache cleanup thread
        cleanup_thread = threading.Thread(target=self.cache_cleanup_loop, daemon=True)
        cleanup_thread.start()
        
        logger.info("Background threads started")
    
    def batch_flush_loop(self):
        """Background loop for flushing batches"""
        while True:
            try:
                time.sleep(self.batch_operations['flush_interval'])
                self.flush_all_batches()
                
            except Exception as e:
                logger.error(f"Error in batch flush loop: {e}")
                time.sleep(self.batch_operations['flush_interval'])
    
    def cache_cleanup_loop(self):
        """Background loop for cache cleanup"""
        while True:
            try:
                time.sleep(self.cache_cleanup_interval)
                self.cleanup_duplicate_cache()
                
            except Exception as e:
                logger.error(f"Error in cache cleanup loop: {e}")
                time.sleep(self.cache_cleanup_interval)
    
    def health_check(self) -> Dict:
        """Perform health check"""
        try:
            # Test database connection
            self.client.admin.command('ping')
            
            # Check collections
            collections_status = {
                'attendance_logs': self.attendance_logs is not None,
                'scan_events': self.scan_events is not None,
                'students': self.students is not None
            }
            
            # Get metrics
            metrics = self.get_performance_metrics()
            
            return {
                'status': 'healthy',
                'database_connected': True,
                'collections': collections_status,
                'metrics': metrics,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def shutdown(self):
        """Graceful shutdown"""
        try:
            # Flush all pending batches
            self.flush_all_batches()
            
            # Close database connection
            if self.client:
                self.client.close()
            
            logger.info("Database integration shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

# Global instance
db_integration = EnhancedDatabaseIntegration()

def record_student_attendance(scan_data: Dict) -> Dict:
    """Record student attendance with duplicate prevention"""
    return db_integration.record_attendance(scan_data)

def record_scan_event(event_data: Dict) -> Dict:
    """Record scan event"""
    return db_integration.record_scan_event(event_data)

def get_student_information(student_id: str) -> Optional[Dict]:
    """Get student information"""
    return db_integration.get_student_info(student_id)

def get_database_metrics() -> Dict:
    """Get database performance metrics"""
    return db_integration.get_performance_metrics()

def check_database_health() -> Dict:
    """Check database health"""
    return db_integration.health_check()
