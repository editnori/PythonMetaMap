"""Adaptive Pool Manager for PythonMetaMap"""
import os
import time
import psutil
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
import threading
from collections import deque
from datetime import datetime

from ..core.config import PyMMConfig

logger = logging.getLogger(__name__)


class ResourceMetrics:
    """Track system resource metrics"""
    
    def __init__(self, window_size: int = 60):
        self.window_size = window_size
        self.cpu_history = deque(maxlen=window_size)
        self.memory_history = deque(maxlen=window_size)
        self.io_history = deque(maxlen=window_size)
        self.last_update = None
        
    def update(self):
        """Update metrics"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.1)
            self.cpu_history.append(cpu_percent)
            
            # Memory usage
            mem = psutil.virtual_memory()
            self.memory_history.append(mem.percent)
            
            # IO stats
            io = psutil.disk_io_counters()
            if io:
                self.io_history.append({
                    'read_bytes': io.read_bytes,
                    'write_bytes': io.write_bytes,
                    'read_time': io.read_time,
                    'write_time': io.write_time
                })
            
            self.last_update = datetime.now()
            
        except Exception as e:
            logger.warning(f"Failed to update metrics: {e}")
    
    def get_average_cpu(self) -> float:
        """Get average CPU usage"""
        if not self.cpu_history:
            return 0.0
        return sum(self.cpu_history) / len(self.cpu_history)
    
    def get_average_memory(self) -> float:
        """Get average memory usage"""
        if not self.memory_history:
            return 0.0
        return sum(self.memory_history) / len(self.memory_history)
    
    def get_io_pressure(self) -> float:
        """Calculate IO pressure (0-100)"""
        if len(self.io_history) < 2:
            return 0.0
        
        # Calculate IO rate
        recent = self.io_history[-1]
        older = self.io_history[-2]
        
        time_diff = 1.0  # Approximate
        read_rate = (recent['read_bytes'] - older['read_bytes']) / time_diff
        write_rate = (recent['write_bytes'] - older['write_bytes']) / time_diff
        
        # Normalize to percentage (100MB/s = 100%)
        io_rate = (read_rate + write_rate) / (100 * 1024 * 1024)
        return min(io_rate * 100, 100.0)


class AdaptivePoolManager:
    """Manages worker pool size based on system resources"""
    
    def __init__(self, config: PyMMConfig):
        self.config = config
        self.metrics = ResourceMetrics()
        self.monitoring = False
        self.monitor_thread = None
        
        # Thresholds
        self.cpu_threshold_high = 80.0
        self.cpu_threshold_low = 50.0
        self.memory_threshold_high = 85.0
        self.memory_threshold_low = 60.0
        self.io_threshold_high = 70.0
        
        # Worker limits
        self.min_workers = 1
        self.max_workers = self._calculate_max_workers()
        self.current_workers = config.get('max_parallel_workers', 4)
        
        # Adjustment history
        self.adjustment_history = deque(maxlen=10)
        self.last_adjustment = None
        self.adjustment_cooldown = 30  # seconds
        
    def _calculate_max_workers(self) -> int:
        """Calculate maximum safe worker count"""
        cpu_count = os.cpu_count() or 4
        mem_gb = psutil.virtual_memory().total / (1024**3)
        
        # Base calculation
        cpu_based = max(1, cpu_count // 2)  # Half of CPU cores
        mem_based = max(1, int(mem_gb / 4))  # 4GB per worker
        
        # Take the minimum to be safe
        max_workers = min(cpu_based, mem_based)
        
        # Apply hard limits
        max_workers = min(max_workers, 16)  # Never exceed 16
        max_workers = max(max_workers, 2)   # At least 2
        
        return max_workers
    
    def analyze_system(self) -> Dict[str, Any]:
        """Analyze system and provide recommendations"""
        # Update metrics
        self.metrics.update()
        
        # Get system info
        cpu_count = os.cpu_count() or 4
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        mem = psutil.virtual_memory()
        mem_total_gb = mem.total / (1024**3)
        mem_available_gb = mem.available / (1024**3)
        mem_percent = mem.percent
        
        # Calculate optimal workers
        optimal_workers = self._calculate_optimal_workers(cpu_percent, mem_percent)
        
        # Get disk info
        disk = psutil.disk_usage('/')
        disk_free_gb = disk.free / (1024**3)
        
        return {
            'system': {
                'cpu_count': cpu_count,
                'cpu_percent': cpu_percent,
                'memory_total_gb': round(mem_total_gb, 1),
                'memory_available_gb': round(mem_available_gb, 1),
                'memory_percent': mem_percent,
                'disk_free_gb': round(disk_free_gb, 1)
            },
            'workers': {
                'current': self.current_workers,
                'optimal': optimal_workers,
                'min': self.min_workers,
                'max': self.max_workers
            },
            'memory': {
                'total_gb': round(mem_total_gb, 1),
                'available_gb': round(mem_available_gb, 1),
                'percent': mem_percent
            },
            'recommendations': self._get_recommendations(cpu_percent, mem_percent, optimal_workers)
        }
    
    def _calculate_optimal_workers(self, cpu_percent: float, mem_percent: float) -> int:
        """Calculate optimal worker count based on current system state"""
        cpu_count = os.cpu_count() or 4
        mem_gb = psutil.virtual_memory().available / (1024**3)
        
        # Base calculation
        if cpu_percent > 70 or mem_percent > 80:
            # System under load, be conservative
            optimal = max(1, min(2, cpu_count // 4))
        elif cpu_percent < 30 and mem_percent < 50:
            # System idle, can use more
            optimal = min(self.max_workers, cpu_count // 2)
        else:
            # Normal load
            optimal = min(max(2, cpu_count // 3), int(mem_gb / 3))
        
        return max(self.min_workers, min(optimal, self.max_workers))
    
    def _get_recommendations(self, cpu_percent: float, mem_percent: float, optimal_workers: int) -> List[str]:
        """Get system recommendations"""
        recommendations = []
        
        if mem_percent > 85:
            recommendations.append("High memory usage - consider closing other applications")
        elif mem_percent > 70:
            recommendations.append("Memory usage elevated - monitor for stability")
            
        if cpu_percent > 80:
            recommendations.append("High CPU usage - processing may be slower")
            
        if optimal_workers < self.current_workers:
            recommendations.append(f"Consider reducing workers to {optimal_workers} for better stability")
        elif optimal_workers > self.current_workers:
            recommendations.append(f"System can handle {optimal_workers} workers for faster processing")
            
        if not recommendations:
            recommendations.append("System resources optimal for processing")
            
        return recommendations
    
    def start_monitoring(self):
        """Start resource monitoring"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("Started adaptive pool monitoring")
    
    def stop_monitoring(self):
        """Stop monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
        logger.info("Stopped adaptive pool monitoring")
    
    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.monitoring:
            try:
                # Update metrics
                self.metrics.update()
                
                # Check if adjustment needed
                if self._should_adjust():
                    self._adjust_workers()
                
                # Sleep interval
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
                time.sleep(10)
    
    def _should_adjust(self) -> bool:
        """Check if worker adjustment is needed"""
        # Check cooldown
        if self.last_adjustment:
            elapsed = (datetime.now() - self.last_adjustment).total_seconds()
            if elapsed < self.adjustment_cooldown:
                return False
        
        # Need enough data
        if len(self.metrics.cpu_history) < 5:
            return False
        
        # Get current metrics
        avg_cpu = self.metrics.get_average_cpu()
        avg_memory = self.metrics.get_average_memory()
        io_pressure = self.metrics.get_io_pressure()
        
        # Check if adjustment needed
        if avg_cpu > self.cpu_threshold_high:
            return True  # Scale down
        elif avg_cpu < self.cpu_threshold_low and avg_memory < self.memory_threshold_low:
            return True  # Scale up
        elif avg_memory > self.memory_threshold_high:
            return True  # Scale down
        elif io_pressure > self.io_threshold_high:
            return True  # Scale down
        
        return False
    
    def _adjust_workers(self):
        """Adjust worker count based on metrics"""
        avg_cpu = self.metrics.get_average_cpu()
        avg_memory = self.metrics.get_average_memory()
        io_pressure = self.metrics.get_io_pressure()
        
        old_workers = self.current_workers
        new_workers = old_workers
        
        # Determine adjustment
        if avg_cpu > self.cpu_threshold_high or avg_memory > self.memory_threshold_high:
            # Scale down
            new_workers = max(self.min_workers, old_workers - 1)
            reason = f"High resource usage (CPU: {avg_cpu:.1f}%, MEM: {avg_memory:.1f}%)"
            
        elif io_pressure > self.io_threshold_high:
            # Scale down for IO
            new_workers = max(self.min_workers, old_workers - 1)
            reason = f"High IO pressure ({io_pressure:.1f}%)"
            
        elif avg_cpu < self.cpu_threshold_low and avg_memory < self.memory_threshold_low:
            # Scale up
            new_workers = min(self.max_workers, old_workers + 1)
            reason = f"Low resource usage (CPU: {avg_cpu:.1f}%, MEM: {avg_memory:.1f}%)"
        
        # Apply change if different
        if new_workers != old_workers:
            self.current_workers = new_workers
            self.config.set('max_parallel_workers', new_workers)
            self.last_adjustment = datetime.now()
            
            # Record adjustment
            self.adjustment_history.append({
                'timestamp': self.last_adjustment,
                'old_workers': old_workers,
                'new_workers': new_workers,
                'reason': reason,
                'metrics': {
                    'cpu': avg_cpu,
                    'memory': avg_memory,
                    'io': io_pressure
                }
            })
            
            logger.info(f"Adjusted workers: {old_workers} -> {new_workers} ({reason})")
    
    def calculate_optimal_workers(self, file_count: Optional[int] = None) -> int:
        """Calculate optimal worker count for current system state"""
        # Update metrics
        self.metrics.update()
        
        # Get current resources
        cpu_count = os.cpu_count() or 4
        mem_available = psutil.virtual_memory().available / (1024**3)
        current_cpu = psutil.cpu_percent(interval=0.1)
        
        # Base calculation
        optimal = cpu_count // 4  # Conservative: 1 worker per 4 cores
        
        # Adjust based on available memory
        mem_per_worker = 2.0  # GB
        mem_based = int(mem_available / mem_per_worker)
        optimal = min(optimal, mem_based)
        
        # Adjust based on current CPU usage
        if current_cpu > 50:
            optimal = max(1, optimal - 1)
        
        # Adjust based on file count
        if file_count:
            if file_count < 100:
                optimal = min(optimal, 2)  # Small batch
            elif file_count > 10000:
                optimal = max(optimal, 4)  # Large batch needs more
        
        # Apply limits
        optimal = max(self.min_workers, min(optimal, self.max_workers))
        
        return optimal
    
    def get_status(self) -> Dict[str, Any]:
        """Get current pool status"""
        return {
            'current_workers': self.current_workers,
            'min_workers': self.min_workers,
            'max_workers': self.max_workers,
            'monitoring': self.monitoring,
            'metrics': {
                'cpu_avg': self.metrics.get_average_cpu(),
                'memory_avg': self.metrics.get_average_memory(),
                'io_pressure': self.metrics.get_io_pressure()
            },
            'last_adjustment': self.last_adjustment.isoformat() if self.last_adjustment else None,
            'adjustment_history': [
                {
                    'timestamp': adj['timestamp'].isoformat(),
                    'change': f"{adj['old_workers']} -> {adj['new_workers']}",
                    'reason': adj['reason']
                }
                for adj in list(self.adjustment_history)[-5:]  # Last 5
            ]
        }
    
    def recommend_settings(self, dataset_size: int) -> Dict[str, Any]:
        """Recommend settings for a dataset"""
        # Get system info
        cpu_count = os.cpu_count() or 4
        mem_gb = psutil.virtual_memory().total / (1024**3)
        
        # Calculate recommendations
        recommendations = {
            'workers': self.calculate_optimal_workers(dataset_size),
            'chunk_size': 500,
            'use_adaptive': True,
            'use_instance_pool': dataset_size > 1000,
            'estimated_time_minutes': dataset_size / 30,  # ~30 files/minute
            'memory_required_gb': min(dataset_size * 0.01, mem_gb * 0.7)
        }
        
        # Adjust chunk size based on memory
        if mem_gb < 8:
            recommendations['chunk_size'] = 250
        elif mem_gb > 32:
            recommendations['chunk_size'] = 1000
        
        # Add warnings
        warnings = []
        if dataset_size > 10000 and mem_gb < 16:
            warnings.append("Large dataset with limited memory - consider smaller chunks")
        
        if cpu_count < 4:
            warnings.append("Limited CPU cores - processing may be slower")
        
        try:
            import resource
            soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
            if soft < 32768 and dataset_size > 5000:
                warnings.append(f"File descriptor limit is {soft} - run: ulimit -n 32768")
        except:
            pass
        
        recommendations['warnings'] = warnings
        
        return recommendations


class WorkerPool:
    """Enhanced worker pool with adaptive management"""
    
    def __init__(self, config: PyMMConfig, adaptive: bool = True):
        self.config = config
        self.adaptive_manager = AdaptivePoolManager(config) if adaptive else None
        self.workers = []
        self.active_tasks = {}
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.start_time = None
        
    def start(self):
        """Start the worker pool"""
        self.start_time = datetime.now()
        
        if self.adaptive_manager:
            self.adaptive_manager.start_monitoring()
        
        logger.info("Worker pool started")
    
    def stop(self):
        """Stop the worker pool"""
        if self.adaptive_manager:
            self.adaptive_manager.stop_monitoring()
        
        # Clean up workers
        for worker in self.workers:
            try:
                worker.terminate()
            except:
                pass
        
        logger.info("Worker pool stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics"""
        elapsed = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        stats = {
            'workers': len(self.workers),
            'active_tasks': len(self.active_tasks),
            'completed_tasks': self.completed_tasks,
            'failed_tasks': self.failed_tasks,
            'elapsed_seconds': elapsed,
            'throughput': self.completed_tasks / elapsed if elapsed > 0 else 0
        }
        
        if self.adaptive_manager:
            stats['adaptive'] = self.adaptive_manager.get_status()
        
        return stats


# Import the actual MetaMapInstancePool from instance_pool module
from .instance_pool import MetaMapInstancePool