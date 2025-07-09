"""Ultra-optimized batch processing with advanced worker management"""
import os
import time
import logging
import json
import gc
import threading
import queue
from pathlib import Path
from typing import List, Dict, Optional, Iterator, Callable, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import psutil

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.console import Console

from ..core.config import PyMMConfig
from ..server.manager import ServerManager
from .pool_manager import MetaMapInstancePool
from .worker import FileProcessor

logger = logging.getLogger(__name__)
console = Console()


class WorkerPool:
    """Advanced worker pool with dynamic scaling and health monitoring"""
    
    def __init__(self, config: PyMMConfig):
        self.config = config
        self.max_workers = config.get("max_parallel_workers", 4)
        self.base_timeout = config.get("pymm_timeout", 300)
        
        # Dynamic worker management
        self.active_workers = 0
        self.worker_lock = threading.Lock()
        self.worker_stats = {}
        self.performance_history = []
        
        # Health monitoring
        self.health_check_interval = 30
        self.last_health_check = time.time()
        self.unhealthy_workers = set()
        
        # Resource limits
        self.memory_threshold = 0.85  # Max 85% memory usage
        self.cpu_threshold = 0.90     # Max 90% CPU usage
        
    def get_optimal_workers(self) -> int:
        """Calculate optimal worker count based on current system state"""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory_percent = psutil.virtual_memory().percent / 100
            
            # Reduce workers if system is under load
            if memory_percent > self.memory_threshold:
                return max(1, self.max_workers // 2)
            elif cpu_percent > self.cpu_threshold * 100:
                return max(2, self.max_workers - 1)
            else:
                return self.max_workers
                
        except Exception:
            return self.max_workers
    
    def adjust_timeout(self, file_size: int) -> int:
        """Dynamically adjust timeout based on file size and performance"""
        # Base timeout adjustment
        size_mb = file_size / (1024 * 1024)
        
        if size_mb < 1:
            timeout = self.base_timeout
        elif size_mb < 5:
            timeout = int(self.base_timeout * 1.5)
        else:
            timeout = int(self.base_timeout * 2)
        
        # Adjust based on recent performance
        if self.performance_history:
            avg_time = sum(self.performance_history[-10:]) / len(self.performance_history[-10:])
            if avg_time > self.base_timeout * 0.8:
                timeout = int(timeout * 1.2)
        
        return timeout
    
    def record_performance(self, elapsed_time: float):
        """Record performance metrics for adaptive optimization"""
        self.performance_history.append(elapsed_time)
        # Keep only last 100 records
        if len(self.performance_history) > 100:
            self.performance_history = self.performance_history[-100:]


class UltraOptimizedBatchRunner:
    """Ultra-optimized batch processing with advanced features"""
    
    def __init__(self, input_dir: str, output_dir: str, config: PyMMConfig):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.config = config
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Advanced configuration
        self.worker_pool = WorkerPool(config)
        self.chunk_size = config.get("chunk_size", 100)
        
        # State management with memory-mapped file for large datasets
        self.state_file = self.output_dir / ".processing_state.json"
        self.processed_files = set()
        self.failed_files = set()
        self._load_state()
        
        # Server and instance management
        self.server_manager = ServerManager(config)
        self.instance_pool = None
        self.pool_lock = threading.Lock()
        
        # Processing queue for better memory management
        self.file_queue = queue.Queue(maxsize=self.chunk_size * 2)
        self.result_queue = queue.Queue()
        
        # Statistics
        self.stats = {
            "total": 0,
            "processed": 0,
            "failed": 0,
            "skipped": 0,
            "start_time": None,
            "bytes_processed": 0
        }
    
    def _load_state(self):
        """Load processing state efficiently"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                self.processed_files = set(state.get('processed', []))
                self.failed_files = set(state.get('failed', []))
            except Exception as e:
                logger.warning(f"Failed to load state: {e}")
    
    def _save_state(self):
        """Save processing state efficiently"""
        try:
            # Only save if there are changes
            if self.stats["processed"] > 0 or self.stats["failed"] > 0:
                state = {
                    'processed': list(self.processed_files)[-10000:],  # Limit size
                    'failed': list(self.failed_files)[-1000:],
                    'timestamp': datetime.now().isoformat(),
                    'stats': self.stats
                }
                with open(self.state_file, 'w') as f:
                    json.dump(state, f)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def _discover_files_streaming(self) -> Iterator[Path]:
        """Stream files with size filtering and smart ordering"""
        patterns = ['*.txt', '*.text', '*.input']
        
        # Collect files with sizes for smart ordering
        file_info = []
        
        for pattern in patterns:
            try:
                for file in self.input_dir.glob(pattern):
                    # Skip processed files
                    if file.stem in self.processed_files:
                        continue
                    
                    # Skip if output exists and is valid
                    output_file = self.output_dir / f"{file.stem}.csv"
                    if output_file.exists() and output_file.stat().st_size > 100:
                        self.processed_files.add(file.stem)
                        continue
                    
                    # Get file size
                    try:
                        size = file.stat().st_size
                        # Skip empty or huge files
                        if size == 0 or size > 100 * 1024 * 1024:  # 100MB limit
                            logger.warning(f"Skipping {file.name}: size {size}")
                            continue
                        file_info.append((file, size))
                    except Exception:
                        file_info.append((file, 0))
                        
            except Exception as e:
                logger.error(f"Error discovering files: {e}")
        
        # Sort by size (process smaller files first for better throughput)
        file_info.sort(key=lambda x: x[1])
        
        # Yield files
        for file, _ in file_info:
            yield file
    
    def _init_instance_pool_advanced(self):
        """Initialize instance pool with advanced configuration"""
        if self.instance_pool is None:
            with self.pool_lock:
                if self.instance_pool is None:
                    # Calculate instances based on current system state
                    memory_gb = psutil.virtual_memory().available / (1024**3)
                    cpu_count = psutil.cpu_count()
                    
                    # Very conservative calculation
                    max_instances = min(
                        max(2, int(memory_gb / 3)),  # 1 instance per 3GB
                        self.worker_pool.get_optimal_workers(),
                        cpu_count // 2
                    )
                    
                    logger.info(f"Creating pool with {max_instances} instances")
                    self.instance_pool = MetaMapInstancePool(
                        self.config.get("metamap_binary_path"),
                        max_instances=max_instances,
                        config=self.config
                    )
    
    def _process_file_advanced(self, file: Path) -> Tuple[bool, float, Optional[str]]:
        """Process file with advanced error handling and monitoring"""
        instance_id = None
        start_time = time.time()
        
        try:
            # Check system resources before processing
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 90:
                # Wait for memory to free up
                time.sleep(5)
                gc.collect()
            
            # Get instance from pool
            instance_id, mm_instance = self.instance_pool.get_instance()
            
            # Adjust timeout based on file size
            file_size = file.stat().st_size
            timeout = self.worker_pool.adjust_timeout(file_size)
            
            # Process file
            processor = FileProcessor(
                self.config.get("metamap_binary_path"),
                str(self.output_dir),
                self.config.get("metamap_processing_options", ""),
                timeout,
                metamap_instance=mm_instance,
                worker_id=instance_id
            )
            
            output_file = processor.process_file(str(file))
            elapsed = time.time() - start_time
            
            # Record performance
            self.worker_pool.record_performance(elapsed)
            self.stats["bytes_processed"] += file_size
            
            return (True, elapsed, None)
            
        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = str(e)
            
            # Check if it's a timeout
            if "timeout" in error_msg.lower():
                logger.warning(f"Timeout processing {file.name} after {elapsed:.1f}s")
            
            return (False, elapsed, error_msg)
            
        finally:
            # Always return instance to pool
            if instance_id is not None and self.instance_pool:
                self.instance_pool.release_instance(instance_id)
    
    def _process_chunk_advanced(self, files: List[Path], chunk_num: int):
        """Process chunk with advanced worker management"""
        logger.info(f"Processing chunk {chunk_num} ({len(files)} files)")
        
        # Get optimal worker count for this chunk
        optimal_workers = self.worker_pool.get_optimal_workers()
        
        with ThreadPoolExecutor(max_workers=optimal_workers) as executor:
            # Submit tasks with priority (smaller files first)
            future_to_file = {}
            for file in files:
                future = executor.submit(self._process_file_advanced, file)
                future_to_file[future] = file
            
            # Process results with timeout handling
            completed = 0
            for future in as_completed(future_to_file, timeout=self.chunk_size * 600):
                file = future_to_file[future]
                
                try:
                    success, elapsed, error = future.result(timeout=10)
                    
                    if success:
                        self.processed_files.add(file.stem)
                        self.stats["processed"] += 1
                        logger.debug(f"Processed {file.name} in {elapsed:.2f}s")
                    else:
                        self.failed_files.add(file.stem)
                        self.stats["failed"] += 1
                        logger.error(f"Failed {file.name}: {error}")
                        
                except Exception as e:
                    self.failed_files.add(file.stem)
                    self.stats["failed"] += 1
                    logger.error(f"Exception processing {file.name}: {e}")
                
                completed += 1
                
                # Progress callback
                if hasattr(self, 'progress_callback') and self.progress_callback:
                    self.progress_callback(self.stats)
                
                # Periodic state save
                if completed % 10 == 0:
                    self._save_state()
        
        # Force garbage collection after chunk
        gc.collect()
    
    def run(self, progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """Run ultra-optimized batch processing"""
        self.progress_callback = progress_callback
        self.stats["start_time"] = time.time()
        
        # Ensure servers are running
        if not self.server_manager.is_running():
            logger.info("Starting MetaMap servers...")
            if not self.server_manager.start():
                return {
                    "success": False,
                    "error": "Failed to start MetaMap servers",
                    "total": 0,
                    "successful": 0,
                    "failed": 0,
                    "duration": 0
                }
        
        # Count files efficiently
        logger.info("Discovering files...")
        file_list = list(self._discover_files_streaming())
        self.stats["total"] = len(file_list)
        
        if not file_list:
            return {
                "success": True,
                "total": 0,
                "successful": 0,
                "failed": 0,
                "duration": 0
            }
        
        logger.info(f"Found {len(file_list)} files to process")
        
        # Initialize instance pool
        self._init_instance_pool_advanced()
        
        try:
            # Process in adaptive chunks
            chunk_start = 0
            chunk_num = 0
            
            while chunk_start < len(file_list):
                # Adapt chunk size based on available memory
                memory_percent = psutil.virtual_memory().percent
                if memory_percent > 80:
                    current_chunk_size = max(10, self.chunk_size // 2)
                else:
                    current_chunk_size = self.chunk_size
                
                # Get chunk
                chunk_end = min(chunk_start + current_chunk_size, len(file_list))
                chunk = file_list[chunk_start:chunk_end]
                
                # Process chunk
                chunk_num += 1
                self._process_chunk_advanced(chunk, chunk_num)
                
                # Move to next chunk
                chunk_start = chunk_end
                
                # Health check
                if time.time() - self.worker_pool.last_health_check > self.worker_pool.health_check_interval:
                    self._perform_health_check()
                    self.worker_pool.last_health_check = time.time()
            
            # Final statistics
            duration = time.time() - self.stats["start_time"]
            throughput_mbps = (self.stats["bytes_processed"] / (1024 * 1024)) / duration if duration > 0 else 0
            
            return {
                "success": True,
                "total": self.stats["total"],
                "successful": self.stats["processed"],
                "failed": self.stats["failed"],
                "skipped": self.stats["skipped"],
                "duration": duration,
                "throughput": self.stats["processed"] / duration if duration > 0 else 0,
                "throughput_mbps": throughput_mbps
            }
            
        finally:
            # Cleanup
            if self.instance_pool:
                logger.info("Shutting down instance pool...")
                self.instance_pool.shutdown()
            
            # Save final state
            self._save_state()
    
    def _perform_health_check(self):
        """Perform system health check and adjust resources"""
        try:
            # Check system resources
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory_percent = psutil.virtual_memory().percent
            disk_usage = psutil.disk_usage(str(self.output_dir)).percent
            
            logger.info(f"System health: CPU {cpu_percent:.1f}%, RAM {memory_percent:.1f}%, Disk {disk_usage:.1f}%")
            
            # Adjust workers if needed
            if memory_percent > 90 or cpu_percent > 95:
                logger.warning("System under high load, reducing workers")
                self.worker_pool.max_workers = max(1, self.worker_pool.max_workers - 1)
            elif memory_percent < 60 and cpu_percent < 50:
                # System has capacity, can increase workers
                optimal = self.worker_pool.get_optimal_workers()
                if self.worker_pool.max_workers < optimal:
                    self.worker_pool.max_workers = min(
                        self.worker_pool.max_workers + 1,
                        optimal
                    )
                    
        except Exception as e:
            logger.debug(f"Health check error: {e}")