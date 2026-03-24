#!/usr/bin/env python3
"""
High-Volume Scanning Optimization System
Optimized for 100+ student scans every 5-10 minutes with performance monitoring
"""

import time
import asyncio
import threading
import queue
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import psutil
import gc

logger = logging.getLogger(__name__)

class HighVolumeScanningOptimizer:
    def __init__(self):
        # Performance monitoring
        self.metrics = {
            'scans_per_minute': 0,
            'peak_scans_per_minute': 0,
            'average_processing_time': 0,
            'memory_usage_mb': 0,
            'cpu_usage_percent': 0,
            'queue_size': 0,
            'error_rate': 0,
            'success_rate': 0
        }
        
        # Scanning optimization
        self.scan_queue = queue.Queue(maxsize=1000)
        self.result_queue = queue.Queue(maxsize=1000)
        self.processing_workers = 4  # Number of parallel workers
        self.batch_size = 5  # Process faces in batches
        self.batch_timeout = 0.1  # 100ms batch timeout
        
        # Rate limiting and throttling
        self.rate_limiter = {
            'max_scans_per_second': 20,
            'current_scans': [],
            'window_size': 1.0  # 1 second window
        }
        
        # Memory management
        self.memory_manager = {
            'max_memory_mb': 512,
            'cleanup_interval': 30,  # seconds
            'last_cleanup': time.time()
        }
        
        # Error handling and recovery
        self.error_handler = {
            'max_consecutive_errors': 5,
            'consecutive_errors': 0,
            'error_backoff': 1.0,  # seconds
            'last_error_time': 0
        }
        
        # Performance tracking
        self.performance_tracker = {
            'scan_times': deque(maxlen=1000),
            'success_count': 0,
            'error_count': 0,
            'start_time': time.time(),
            'last_minute_scans': deque(maxlen=60)
        }
        
        # Initialize workers
        self.initialize_workers()
        
        # Start monitoring threads
        self.start_monitoring_threads()
    
    def initialize_workers(self):
        """Initialize parallel processing workers"""
        self.executor = ThreadPoolExecutor(max_workers=self.processing_workers)
        self.workers = []
        
        for i in range(self.processing_workers):
            worker = threading.Thread(target=self.worker_loop, args=(i,), daemon=True)
            worker.start()
            self.workers.append(worker)
        
        logger.info(f"Initialized {self.processing_workers} parallel processing workers")
    
    def worker_loop(self, worker_id):
        """Main worker processing loop"""
        logger.info(f"Worker {worker_id} started")
        
        while True:
            try:
                # Get scan batch
                batch = []
                batch_start_time = time.time()
                
                # Collect batch items
                while len(batch) < self.batch_size and (time.time() - batch_start_time) < self.batch_timeout:
                    try:
                        item = self.scan_queue.get(timeout=0.01)
                        batch.append(item)
                        self.scan_queue.task_done()
                    except queue.Empty:
                        break
                
                if not batch:
                    continue
                
                # Process batch
                results = self.process_scan_batch(batch, worker_id)
                
                # Put results in result queue
                for result in results:
                    try:
                        self.result_queue.put_nowait(result)
                    except queue.Full:
                        logger.warning("Result queue full, dropping result")
                
                # Update metrics
                self.update_batch_metrics(batch, results)
                
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                self.handle_worker_error(worker_id, e)
    
    def process_scan_batch(self, batch, worker_id):
        """Process a batch of scans"""
        results = []
        
        for scan_item in batch:
            try:
                start_time = time.time()
                
                # Process individual scan
                result = self.process_individual_scan(scan_item)
                
                processing_time = time.time() - start_time
                result['processing_time'] = processing_time
                result['worker_id'] = worker_id
                result['timestamp'] = time.time()
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"Error processing scan item: {e}")
                results.append({
                    'status': 'error',
                    'error': str(e),
                    'scan_item': scan_item,
                    'worker_id': worker_id,
                    'timestamp': time.time()
                })
        
        return results
    
    def process_individual_scan(self, scan_item):
        """Process individual scan item"""
        scan_type = scan_item.get('type', 'face_recognition')
        
        if scan_type == 'face_recognition':
            return self.process_face_recognition(scan_item)
        elif scan_type == 'face_detection':
            return self.process_face_detection(scan_item)
        else:
            raise ValueError(f"Unknown scan type: {scan_type}")
    
    def process_face_recognition(self, scan_item):
        """Process face recognition scan"""
        try:
            # Import here to avoid circular imports
            from enhanced_face_processor import face_processor
            
            # Extract face data
            face_data = scan_item.get('face_data', {})
            frame_bytes = scan_item.get('frame_bytes', b'')
            
            # Perform recognition with caching
            result = face_processor.recognize_face(face_data, frame_bytes)
            
            return {
                'status': 'success',
                'type': 'face_recognition',
                'result': result,
                'scan_item': scan_item
            }
            
        except Exception as e:
            logger.error(f"Face recognition error: {e}")
            return {
                'status': 'error',
                'type': 'face_recognition',
                'error': str(e),
                'scan_item': scan_item
            }
    
    def process_face_detection(self, scan_item):
        """Process face detection scan"""
        try:
            # Import here to avoid circular imports
            from enhanced_face_processor import face_processor
            
            # Extract frame data
            frame_bytes = scan_item.get('frame_bytes', b'')
            
            # Perform detection
            result = face_processor.detect_faces(frame_bytes)
            
            return {
                'status': 'success',
                'type': 'face_detection',
                'result': result,
                'scan_item': scan_item
            }
            
        except Exception as e:
            logger.error(f"Face detection error: {e}")
            return {
                'status': 'error',
                'type': 'face_detection',
                'error': str(e),
                'scan_item': scan_item
            }
    
    def submit_scan(self, scan_item):
        """Submit scan item for processing"""
        try:
            # Check rate limiting
            if not self.check_rate_limit():
                return {
                    'status': 'rejected',
                    'reason': 'Rate limit exceeded',
                    'retry_after': self.rate_limiter['window_size']
                }
            
            # Check queue size
            if self.scan_queue.qsize() >= self.scan_queue.maxsize:
                return {
                    'status': 'rejected',
                    'reason': 'Processing queue full'
                }
            
            # Add timestamp and priority
            scan_item['submitted_at'] = time.time()
            scan_item['priority'] = scan_item.get('priority', 1)
            
            # Submit to queue
            self.scan_queue.put(scan_item, timeout=0.1)
            
            return {
                'status': 'submitted',
                'queue_size': self.scan_queue.qsize()
            }
            
        except queue.Full:
            return {
                'status': 'rejected',
                'reason': 'Processing queue full'
            }
        except Exception as e:
            logger.error(f"Error submitting scan: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }
    
    def check_rate_limit(self):
        """Check if rate limit allows new scan"""
        current_time = time.time()
        window_start = current_time - self.rate_limiter['window_size']
        
        # Clean old scan records
        self.rate_limiter['current_scans'] = [
            scan_time for scan_time in self.rate_limiter['current_scans']
            if scan_time > window_start
        ]
        
        # Check if under limit
        if len(self.rate_limiter['current_scans']) < self.rate_limiter['max_scans_per_second']:
            self.rate_limiter['current_scans'].append(current_time)
            return True
        
        return False
    
    def get_results(self, timeout=1.0):
        """Get processed results"""
        results = []
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                result = self.result_queue.get(timeout=0.1)
                results.append(result)
                self.result_queue.task_done()
            except queue.Empty:
                break
        
        return results
    
    def update_batch_metrics(self, batch, results):
        """Update performance metrics for batch processing"""
        current_time = time.time()
        
        # Update scan times
        for result in results:
            if result.get('status') == 'success':
                processing_time = result.get('processing_time', 0)
                self.performance_tracker['scan_times'].append(processing_time)
                self.performance_tracker['success_count'] += 1
            else:
                self.performance_tracker['error_count'] += 1
        
        # Update scans per minute
        self.performance_tracker['last_minute_scans'].append(current_time)
        recent_scans = [
            scan_time for scan_time in self.performance_tracker['last_minute_scans']
            if current_time - scan_time < 60
        ]
        self.metrics['scans_per_minute'] = len(recent_scans)
        
        # Update peak scans per minute
        if self.metrics['scans_per_minute'] > self.metrics['peak_scans_per_minute']:
            self.metrics['peak_scans_per_minute'] = self.metrics['scans_per_minute']
        
        # Update average processing time
        if self.performance_tracker['scan_times']:
            self.metrics['average_processing_time'] = sum(self.performance_tracker['scan_times']) / len(self.performance_tracker['scan_times'])
        
        # Update success/error rates
        total_scans = self.performance_tracker['success_count'] + self.performance_tracker['error_count']
        if total_scans > 0:
            self.metrics['success_rate'] = self.performance_tracker['success_count'] / total_scans
            self.metrics['error_rate'] = self.performance_tracker['error_count'] / total_scans
        
        # Update queue size
        self.metrics['queue_size'] = self.scan_queue.qsize()
    
    def handle_worker_error(self, worker_id, error):
        """Handle worker errors with backoff"""
        self.error_handler['consecutive_errors'] += 1
        self.error_handler['last_error_time'] = time.time()
        
        if self.error_handler['consecutive_errors'] >= self.error_handler['max_consecutive_errors']:
            logger.error(f"Worker {worker_id} has too many consecutive errors, implementing backoff")
            time.sleep(self.error_handler['error_backoff'])
            self.error_handler['consecutive_errors'] = 0
    
    def start_monitoring_threads(self):
        """Start background monitoring threads"""
        # Performance monitoring thread
        perf_thread = threading.Thread(target=self.performance_monitoring_loop, daemon=True)
        perf_thread.start()
        
        # Memory management thread
        memory_thread = threading.Thread(target=self.memory_management_loop, daemon=True)
        memory_thread.start()
        
        # Metrics update thread
        metrics_thread = threading.Thread(target=self.metrics_update_loop, daemon=True)
        metrics_thread.start()
        
        logger.info("Started monitoring threads")
    
    def performance_monitoring_loop(self):
        """Performance monitoring loop"""
        while True:
            try:
                # Update system metrics
                self.metrics['cpu_usage_percent'] = psutil.cpu_percent(interval=1)
                self.metrics['memory_usage_mb'] = psutil.virtual_memory().used / (1024 * 1024)
                
                # Log performance warnings
                if self.metrics['cpu_usage_percent'] > 80:
                    logger.warning(f"High CPU usage: {self.metrics['cpu_usage_percent']:.1f}%")
                
                if self.metrics['memory_usage_mb'] > self.memory_manager['max_memory_mb']:
                    logger.warning(f"High memory usage: {self.metrics['memory_usage_mb']:.1f} MB")
                
                if self.metrics['error_rate'] > 0.1:
                    logger.warning(f"High error rate: {self.metrics['error_rate']:.1%}")
                
                time.sleep(5)  # Update every 5 seconds
                
            except Exception as e:
                logger.error(f"Performance monitoring error: {e}")
                time.sleep(5)
    
    def memory_management_loop(self):
        """Memory management loop"""
        while True:
            try:
                current_time = time.time()
                
                if current_time - self.memory_manager['last_cleanup'] > self.memory_manager['cleanup_interval']:
                    # Perform garbage collection
                    gc.collect()
                    
                    # Clean old rate limit records
                    window_start = current_time - self.rate_limiter['window_size']
                    self.rate_limiter['current_scans'] = [
                        scan_time for scan_time in self.rate_limiter['current_scans']
                        if scan_time > window_start
                    ]
                    
                    # Clean old performance data
                    cutoff_time = current_time - 3600  # Keep 1 hour of data
                    self.performance_tracker['scan_times'] = deque(
                        [t for t in self.performance_tracker['scan_times']],
                        maxlen=1000
                    )
                    
                    self.memory_manager['last_cleanup'] = current_time
                    logger.debug("Memory cleanup completed")
                
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Memory management error: {e}")
                time.sleep(10)
    
    def metrics_update_loop(self):
        """Metrics update loop"""
        while True:
            try:
                # Calculate uptime
                uptime = time.time() - self.performance_tracker['start_time']
                
                # Log current metrics
                logger.info(f"Scanning metrics - SPM: {self.metrics['scans_per_minute']:.1f}, "
                          f"Avg time: {self.metrics['average_processing_time']:.3f}s, "
                          f"Success rate: {self.metrics['success_rate']:.1%}, "
                          f"Queue: {self.metrics['queue_size']}")
                
                time.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                logger.error(f"Metrics update error: {e}")
                time.sleep(30)
    
    def get_performance_metrics(self):
        """Get comprehensive performance metrics"""
        uptime = time.time() - self.performance_tracker['start_time']
        
        return {
            'uptime_seconds': uptime,
            'scans_per_minute': self.metrics['scans_per_minute'],
            'peak_scans_per_minute': self.metrics['peak_scans_per_minute'],
            'average_processing_time': self.metrics['average_processing_time'],
            'success_rate': self.metrics['success_rate'],
            'error_rate': self.metrics['error_rate'],
            'cpu_usage_percent': self.metrics['cpu_usage_percent'],
            'memory_usage_mb': self.metrics['memory_usage_mb'],
            'queue_size': self.metrics['queue_size'],
            'active_workers': len(self.workers),
            'total_scans_processed': self.performance_tracker['success_count'] + self.performance_tracker['error_count']
        }
    
    def optimize_for_high_volume(self):
        """Optimize system for high-volume scanning"""
        # Increase worker count for high volume
        if self.metrics['queue_size'] > 50:
            new_worker_count = min(self.processing_workers + 2, 8)
            if new_worker_count > self.processing_workers:
                logger.info(f"Increasing workers from {self.processing_workers} to {new_worker_count}")
                self.processing_workers = new_worker_count
                # Note: In production, you'd want to dynamically scale workers
        
        # Adjust batch size based on performance
        if self.metrics['average_processing_time'] < 0.1:
            self.batch_size = min(self.batch_size + 1, 10)
        elif self.metrics['average_processing_time'] > 0.5:
            self.batch_size = max(self.batch_size - 1, 1)
        
        # Adjust rate limit based on system performance
        if self.metrics['cpu_usage_percent'] < 50 and self.metrics['success_rate'] > 0.95:
            self.rate_limiter['max_scans_per_second'] = min(
                self.rate_limiter['max_scans_per_second'] + 5, 50
            )
        elif self.metrics['cpu_usage_percent'] > 80 or self.metrics['success_rate'] < 0.8:
            self.rate_limiter['max_scans_per_second'] = max(
                self.rate_limiter['max_scans_per_second'] - 5, 10
            )
        
        logger.info(f"Optimized: batch_size={self.batch_size}, "
                   f"max_scans_per_second={self.rate_limiter['max_scans_per_second']}")
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down high-volume scanning optimizer")
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        # Clear queues
        while not self.scan_queue.empty():
            try:
                self.scan_queue.get_nowait()
                self.scan_queue.task_done()
            except queue.Empty:
                break
        
        while not self.result_queue.empty():
            try:
                self.result_queue.get_nowait()
                self.result_queue.task_done()
            except queue.Empty:
                break
        
        logger.info("High-volume scanning optimizer shutdown complete")

# Global instance
scanning_optimizer = HighVolumeScanningOptimizer()

def submit_scan_for_processing(scan_item):
    """Submit scan for high-volume processing"""
    return scanning_optimizer.submit_scan(scan_item)

def get_scan_results(timeout=1.0):
    """Get scan results"""
    return scanning_optimizer.get_results(timeout)

def get_scanning_metrics():
    """Get scanning performance metrics"""
    return scanning_optimizer.get_performance_metrics()

def optimize_scanning_performance():
    """Optimize scanning for high volume"""
    scanning_optimizer.optimize_for_high_volume()
