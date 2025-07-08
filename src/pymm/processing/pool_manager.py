"""MetaMap instance pool management"""
import os
import logging
import threading
import time
from typing import Optional, Tuple, List
from pathlib import Path

# resource module is Unix-only
try:
    import resource
    HAS_RESOURCE = True
except ImportError:
    HAS_RESOURCE = False
    logging.debug("resource module not available (Windows) - FD limits not enforced")

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    logging.warning("psutil not available - adaptive scaling limited")

from ..pymm import Metamap as PyMetaMap
from ..core.config import PyMMConfig

class MetaMapInstancePool:
    """Manages a pool of MetaMap instances for efficient parallel processing"""
    
    def __init__(self, metamap_binary_path: str, max_instances: Optional[int] = None,
                 config: Optional[PyMMConfig] = None):
        """Initialize the MetaMap instance pool
        
        Args:
            metamap_binary_path: Path to the MetaMap binary
            max_instances: Maximum number of MetaMap instances (None = auto)
            config: Configuration object
        """
        self.metamap_binary_path = metamap_binary_path
        self.config = config or PyMMConfig()
        self.instances = []
        self.instance_in_use = {}
        self.instance_stats = {}  # Track usage statistics
        self.lock = threading.RLock()
        
        # Auto-configure max_instances based on system resources
        if max_instances is None:
            cpu_count = os.cpu_count() or 4
            available_memory_gb = self._get_available_memory_gb()
            
            # 1 instance per 4 cores, adjusted for available memory
            instances_by_cpu = max(2, cpu_count // 4)
            instances_by_memory = max(2, int(available_memory_gb / 4))
            self.max_instances = min(instances_by_cpu, instances_by_memory)
            
            logging.info(
                f"Auto-configured MetaMap instance pool: {self.max_instances} instances "
                f"(based on {cpu_count} cores, {available_memory_gb:.1f}GB available memory)"
            )
        else:
            self.max_instances = max_instances
        
        # Initialize semaphore for concurrency control
        self.semaphore = threading.Semaphore(self.max_instances)
        
        # Pre-warm instances
        self._prewarm_instances()
    
    def _get_available_memory_gb(self) -> float:
        """Get available system memory in GB"""
        if HAS_PSUTIL:
            return psutil.virtual_memory().available / (1024**3)
        else:
            # Fallback: try to read from /proc/meminfo on Linux
            try:
                with open('/proc/meminfo', 'r') as f:
                    for line in f:
                        if line.startswith('MemAvailable:'):
                            kb = int(line.split()[1])
                            return kb / (1024**2)
            except:
                pass
            
            # Default assumption
            logging.warning("Cannot determine available memory, assuming 8GB")
            return 8.0
    
    def _prewarm_instances(self):
        """Pre-warm a minimal set of instances"""
        initial_instances = min(2, self.max_instances)
        if initial_instances > 0:
            logging.info(f"Pre-warming {initial_instances} MetaMap instances...")
            for _ in range(initial_instances):
                self._create_new_instance()
    
    def _create_new_instance(self) -> int:
        """Create a new MetaMap instance"""
        try:
            with self.lock:
                instance_id = len(self.instances)
                
                # All instances connect to the same servers
                instance = PyMetaMap(self.metamap_binary_path, debug=False)
                
                self.instances.append(instance)
                self.instance_in_use[instance_id] = False
                self.instance_stats[instance_id] = {
                    'created': time.time(),
                    'uses': 0,
                    'total_time': 0
                }
                
            logging.debug(f"Created MetaMap instance #{instance_id}")
            return instance_id
            
        except Exception as e:
            logging.error(f"Failed to create MetaMap instance: {e}")
            raise
    
    def get_instance(self) -> Tuple[int, PyMetaMap]:
        """Get an available MetaMap instance"""
        # Wait for available slot
        self.semaphore.acquire()
        
        try:
            with self.lock:
                # Find available instance
                for instance_id, in_use in self.instance_in_use.items():
                    if not in_use:
                        self.instance_in_use[instance_id] = True
                        self.instance_stats[instance_id]['uses'] += 1
                        logging.debug(f"Acquired MetaMap instance #{instance_id}")
                        return instance_id, self.instances[instance_id]
                
                # Create new instance if under limit
                if len(self.instances) < self.max_instances:
                    instance_id = self._create_new_instance()
                    self.instance_in_use[instance_id] = True
                    self.instance_stats[instance_id]['uses'] += 1
                    return instance_id, self.instances[instance_id]
                
                # Should not reach here due to semaphore
                raise RuntimeError("No MetaMap instances available")
                
        except Exception as e:
            self.semaphore.release()
            raise e
    
    def release_instance(self, instance_id: int, processing_time: float = 0):
        """Release a MetaMap instance back to the pool"""
        with self.lock:
            if 0 <= instance_id < len(self.instances):
                if self.instance_in_use.get(instance_id, False):
                    self.instance_in_use[instance_id] = False
                    
                    # Update statistics
                    if instance_id in self.instance_stats:
                        self.instance_stats[instance_id]['total_time'] += processing_time
                    
                    logging.debug(f"Released MetaMap instance #{instance_id}")
                    self.semaphore.release()
                else:
                    logging.warning(f"Instance #{instance_id} was not in use")
            else:
                logging.warning(f"Invalid instance ID: {instance_id}")
    
    def get_statistics(self) -> dict:
        """Get pool statistics"""
        with self.lock:
            total_instances = len(self.instances)
            in_use = sum(1 for in_use in self.instance_in_use.values() if in_use)
            
            stats = {
                'total_instances': total_instances,
                'in_use': in_use,
                'available': total_instances - in_use,
                'max_instances': self.max_instances,
                'instance_details': []
            }
            
            for instance_id, stat in self.instance_stats.items():
                avg_time = stat['total_time'] / stat['uses'] if stat['uses'] > 0 else 0
                stats['instance_details'].append({
                    'id': instance_id,
                    'uses': stat['uses'],
                    'avg_processing_time': avg_time,
                    'age': time.time() - stat['created']
                })
            
            return stats
    
    def health_check(self) -> Tuple[int, int]:
        """Check health of all instances
        
        Returns:
            Tuple of (healthy_count, unhealthy_count)
        """
        healthy = 0
        unhealthy = 0
        
        with self.lock:
            for instance_id, instance in enumerate(self.instances):
                if instance and not self.instance_in_use.get(instance_id, False):
                    try:
                        # Quick health check
                        if instance.is_alive():
                            healthy += 1
                        else:
                            unhealthy += 1
                            # Replace unhealthy instance
                            logging.warning(f"Instance #{instance_id} unhealthy, replacing...")
                            instance.close()
                            self.instances[instance_id] = PyMetaMap(
                                self.metamap_binary_path, debug=False
                            )
                    except Exception as e:
                        logging.error(f"Health check failed for instance #{instance_id}: {e}")
                        unhealthy += 1
        
        return healthy, unhealthy
    
    def shutdown(self):
        """Shut down all MetaMap instances"""
        logging.info("Shutting down MetaMap instance pool...")
        
        with self.lock:
            for instance_id, instance in enumerate(self.instances):
                try:
                    if instance:
                        logging.debug(f"Closing MetaMap instance #{instance_id}")
                        instance.close()
                except Exception as e:
                    logging.error(f"Error closing instance #{instance_id}: {e}")
            
            self.instances.clear()
            self.instance_in_use.clear()
            self.instance_stats.clear()


class AdaptivePoolManager:
    """Resource-aware instance pool scaler"""
    
    def __init__(self, config: PyMMConfig):
        self.config = config
        self.current_workers = 2
        self.min_workers = 1
        self.max_workers = config.get("max_parallel_workers", 4)
        self.performance_history = []
        self.last_adjustment = time.time()
        self.adjustment_interval = 60  # seconds
        self.logger = logging.getLogger("AdaptivePoolManager")
    
    def get_system_resources(self) -> dict:
        """Get current system resource availability"""
        resources = {
            'cpu_percent': 50.0,  # Default
            'memory_available_gb': 8.0,
            'open_files': 100,
            'fd_limit': 1024
        }
        
        if HAS_PSUTIL:
            try:
                resources['cpu_percent'] = psutil.cpu_percent(interval=1)
                resources['memory_available_gb'] = psutil.virtual_memory().available / (1024**3)
                
                process = psutil.Process()
                resources['open_files'] = len(process.open_files())
            except Exception as e:
                self.logger.warning(f"Error getting system resources: {e}")
        
        # Get file descriptor limit
        if HAS_RESOURCE:
            try:
                soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                resources['fd_limit'] = soft
            except:
                pass
        
        return resources
    
    def calculate_optimal_workers(self) -> int:
        """Dynamically calculate optimal worker count"""
        resources = self.get_system_resources()
        
        # CPU-based limit (leave 20% headroom)
        cpu_available = 100 - resources['cpu_percent']
        cpu_workers = max(self.min_workers, int(cpu_available / 20))
        
        # Memory-based limit (4GB per worker)
        memory_workers = max(self.min_workers, int(resources['memory_available_gb'] / 4))
        
        # FD-based limit (100 FDs per worker, 50% safety margin)
        fd_available = resources['fd_limit'] - resources['open_files']
        fd_workers = max(self.min_workers, int(fd_available * 0.5 / 100))
        
        # Take minimum of all limits
        optimal = min(
            cpu_workers,
            memory_workers,
            fd_workers,
            self.max_workers
        )
        
        self.logger.debug(
            f"Optimal workers calculation: "
            f"CPU={cpu_workers}, Memory={memory_workers}, "
            f"FD={fd_workers}, Final={optimal}"
        )
        
        return optimal
    
    def record_performance(self, files_processed: int, time_taken: float):
        """Record performance metrics"""
        if files_processed > 0 and time_taken > 0:
            throughput = files_processed / time_taken
            self.performance_history.append({
                'timestamp': time.time(),
                'workers': self.current_workers,
                'throughput': throughput,
                'files': files_processed,
                'time': time_taken
            })
            
            # Keep only recent history (last hour)
            cutoff = time.time() - 3600
            self.performance_history = [
                p for p in self.performance_history 
                if p['timestamp'] > cutoff
            ]
    
    def should_adjust(self) -> bool:
        """Check if we should adjust worker count"""
        # Don't adjust too frequently
        if time.time() - self.last_adjustment < self.adjustment_interval:
            return False
            
        # Need sufficient performance history
        if len(self.performance_history) < 3:
            return False
            
        return True
    
    def adjust_pool_size(self, pool) -> bool:
        """Adjust pool size based on performance and resources
        
        Returns:
            True if adjustment was made
        """
        if not self.should_adjust():
            return False
        
        optimal = self.calculate_optimal_workers()
        
        # Analyze recent performance trends
        if len(self.performance_history) >= 2:
            recent = self.performance_history[-2:]
            throughput_trend = recent[-1]['throughput'] - recent[0]['throughput']
            
            # If throughput is decreasing, we might be overloaded
            if throughput_trend < 0 and optimal < self.current_workers:
                optimal = min(optimal, self.current_workers - 1)
        
        adjusted = False
        
        if optimal > self.current_workers:
            # Scale up gradually
            new_workers = min(optimal, self.current_workers + 1)
            self.logger.info(f"Scaling up workers: {self.current_workers} -> {new_workers}")
            self.current_workers = new_workers
            adjusted = True
            
        elif optimal < self.current_workers - 1:
            # Scale down more aggressively
            self.logger.info(f"Scaling down workers: {self.current_workers} -> {optimal}")
            self.current_workers = optimal
            adjusted = True
        
        if adjusted:
            self.last_adjustment = time.time()
            
        return adjusted
    
    def get_current_workers(self) -> int:
        """Get current recommended worker count"""
        return self.current_workers
    
    def get_performance_summary(self) -> dict:
        """Get performance summary"""
        if not self.performance_history:
            return {'avg_throughput': 0, 'total_files': 0}
            
        total_files = sum(p['files'] for p in self.performance_history)
        avg_throughput = sum(p['throughput'] for p in self.performance_history) / len(self.performance_history)
        
        return {
            'avg_throughput': avg_throughput,
            'total_files': total_files,
            'current_workers': self.current_workers,
            'history_size': len(self.performance_history)
        }