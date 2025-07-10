"""Unified processor combining all features from various batch runners

This module consolidates all processing capabilities into a single, comprehensive processor:
- Core batch processing (from batch_runner.py)
- Smart file tracking and selection (from smart_batch_runner.py)
- Memory-efficient streaming and optimization (from optimized_batch_runner.py)
- Advanced worker management and health monitoring (from ultra_optimized_runner.py)
- Chunked processing support (from chunked_batch_runner.py)
- Validation capabilities (from validated_batch_runner.py)
- Built-in monitoring (from monitored_batch_runner.py)
"""
import os
import time
import logging
import json
import gc
import signal
import subprocess
import shutil
import sys
import threading
import queue
import psutil
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any, Iterator, Callable, Set
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from datetime import datetime
from collections import defaultdict, deque

from tqdm import tqdm
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from ..core.config import PyMMConfig
from ..core.state import StateManager
from ..core.job_manager import get_job_manager
from ..core.file_tracker import UnifiedFileTracker
from ..core.enhanced_state import AtomicStateManager, FileTracker
from ..server.manager import ServerManager
from ..server.health_check import HealthMonitor
from .pool_manager import MetaMapInstancePool, AdaptivePoolManager
from .worker import FileProcessor
from .retry_manager import RetryManager

logger = logging.getLogger(__name__)
console = Console()


class ProcessingMode:
    """Processing mode enumeration"""
    STANDARD = "standard"
    OPTIMIZED = "optimized"
    ULTRA = "ultra"
    SMART = "smart"
    VALIDATED = "validated"
    MONITORED = "monitored"
    CHUNKED = "chunked"


class UnifiedProcessor:
    """Unified processor with all features from various batch runners"""
    
    def __init__(self, input_dir: str, output_dir: str, config: PyMMConfig = None, mode: str = ProcessingMode.SMART):
        """Initialize unified processor
        
        Args:
            input_dir: Input directory path
            output_dir: Output directory path
            config: PyMMConfig instance
            mode: Processing mode (standard, optimized, ultra, smart, validated, monitored, chunked)
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.config = config or PyMMConfig()
        self.mode = mode
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self.logs_dir = self.output_dir / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        self._setup_logging()
        
        # Core configuration
        self.max_workers = self.config.get("max_parallel_workers", 4)
        self.timeout = self.config.get("pymm_timeout", 300)
        self.use_instance_pool = self.config.get("use_instance_pool", True)
        self.show_progress = self.config.get("progress_bar", True)
        
        # Advanced features flags based on mode
        self._configure_features()
        
        # State management - use appropriate state manager based on features
        if self.features.get("unified_tracking"):
            self.file_tracker = UnifiedFileTracker(self.config)
            self.state_manager = self.file_tracker.state_manager if hasattr(self.file_tracker, 'state_manager') else StateManager(str(self.output_dir))
        else:
            self.state_manager = StateManager(str(self.output_dir))
            self.file_tracker = None
            
        # Enhanced state for atomic operations (from smart runner)
        if self.features.get("atomic_state"):
            self.atomic_state = AtomicStateManager(self.output_dir)
        else:
            self.atomic_state = None
        
        # Job management
        self.job_manager = get_job_manager() if self.config.get("enable_job_tracking", True) else None
        self.job_id = self.config.get("job_id", None)
        
        # Create job if needed
        if self.job_manager and not self.job_id:
            from ..core.job_manager import JobType
            self.job_id = self.job_manager.create_job(
                job_type=JobType.BATCH,
                input_dir=str(self.input_dir),
                output_dir=str(self.output_dir),
                config=self._get_job_config()
            )
            logger.info(f"Created job {self.job_id} for {mode} processing")
        
        # Retry management
        self.retry_manager = RetryManager(self.config, self.state_manager)
        
        # Server management
        self.server_manager = ServerManager(self.config)
        self.health_monitor = HealthMonitor(self.config, self.server_manager)
        
        # Instance pool - use adaptive pool for advanced modes
        self.instance_pool = None
        if self.use_instance_pool:
            if self.features.get("adaptive_pool"):
                logger.info("Creating adaptive instance pool")
                self.instance_pool = AdaptivePoolManager(self.config)
            else:
                logger.info("Creating standard instance pool")
                self.instance_pool = MetaMapInstancePool(self.config)
                
        # Memory-efficient features (from optimized runner)
        if self.features.get("memory_streaming"):
            self.chunk_size = self._calculate_chunk_size()
            self.processed_files = set()
            self.failed_files = set()
            self._load_lightweight_state()
        else:
            self.chunk_size = self.config.get("chunk_size", 100)
            
        # Advanced worker management (from ultra runner)
        if self.features.get("dynamic_workers"):
            self.worker_stats = defaultdict(dict)
            self.performance_history = deque(maxlen=100)
            self.memory_threshold = 0.85
            self.cpu_threshold = 0.90
            
        # Monitoring features (from monitored runner)
        if self.features.get("live_monitoring"):
            self.monitor = None  # Will be initialized when needed
            self.monitor_thread = None
            
        # Validation features (from validated runner)
        if self.features.get("validation"):
            self.validation_enabled = True
            self.validation_results = []
            
        # Statistics
        self.stats = {
            "total_files": 0,
            "processed": 0,
            "failed": 0,
            "skipped": 0,
            "start_time": None,
            "end_time": None,
            "bytes_processed": 0,
            "concepts_found": 0
        }
        
    def _configure_features(self):
        """Configure features based on processing mode"""
        self.features = {
            "unified_tracking": False,
            "atomic_state": False,
            "memory_streaming": False,
            "dynamic_workers": False,
            "adaptive_pool": False,
            "live_monitoring": False,
            "validation": False,
            "chunked_processing": False,
            "smart_selection": False,
            "health_monitoring": False
        }
        
        # Enable features based on mode
        if self.mode == ProcessingMode.STANDARD:
            # Basic features only
            pass
            
        elif self.mode == ProcessingMode.OPTIMIZED:
            self.features.update({
                "memory_streaming": True,
                "chunked_processing": True
            })
            
        elif self.mode == ProcessingMode.ULTRA:
            self.features.update({
                "memory_streaming": True,
                "dynamic_workers": True,
                "adaptive_pool": True,
                "chunked_processing": True,
                "health_monitoring": True
            })
            
        elif self.mode == ProcessingMode.SMART:
            self.features.update({
                "unified_tracking": True,
                "atomic_state": True,
                "smart_selection": True,
                "validation": True,
                "adaptive_pool": True
            })
            
        elif self.mode == ProcessingMode.VALIDATED:
            self.features.update({
                "validation": True,
                "memory_streaming": True
            })
            
        elif self.mode == ProcessingMode.MONITORED:
            self.features.update({
                "live_monitoring": True,
                "unified_tracking": True
            })
            
        elif self.mode == ProcessingMode.CHUNKED:
            self.features.update({
                "chunked_processing": True,
                "memory_streaming": True
            })
            
        # Override with config if specified
        for feature, enabled in self.features.items():
            config_key = f"feature_{feature}"
            if self.config.get(config_key) is not None:
                self.features[feature] = self.config.get(config_key)
                
    def _setup_logging(self):
        """Setup file logging for batch processing"""
        log_file = self.logs_dir / f"{self.mode}_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter(
            '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to root logger
        logging.getLogger().addHandler(file_handler)
        
        logger.info(f"Unified processor log: {log_file}")
        logger.info(f"Mode: {self.mode}, Features: {self.features}")
        
    def _get_job_config(self) -> Dict[str, Any]:
        """Get job configuration"""
        return {
            'mode': self.mode,
            'max_workers': self.max_workers,
            'timeout': self.timeout,
            'use_instance_pool': self.use_instance_pool,
            'features': self.features
        }
        
    def _calculate_chunk_size(self) -> int:
        """Calculate optimal chunk size based on available memory"""
        try:
            memory_gb = psutil.virtual_memory().available / (1024**3)
            if memory_gb < 4:
                return 50
            elif memory_gb < 8:
                return 100
            elif memory_gb < 16:
                return 250
            else:
                return self.config.get("chunk_size", 500)
        except:
            return self.config.get("chunk_size", 100)
            
    def _load_lightweight_state(self):
        """Load lightweight state for memory-efficient processing"""
        state_file = self.output_dir / ".processing_state.json"
        if state_file.exists():
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                self.processed_files = set(state.get('processed', []))
                self.failed_files = set(state.get('failed', []))
                logger.info(f"Loaded state: {len(self.processed_files)} processed")
            except Exception as e:
                logger.warning(f"Failed to load state: {e}")
                
    def _save_lightweight_state(self):
        """Save lightweight state"""
        if self.features.get("memory_streaming"):
            state_file = self.output_dir / ".processing_state.json"
            try:
                state = {
                    'processed': list(self.processed_files)[-10000:],  # Limit size
                    'failed': list(self.failed_files)[-1000:],
                    'timestamp': datetime.now().isoformat(),
                    'stats': self.stats
                }
                with open(state_file, 'w') as f:
                    json.dump(state, f)
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                
    def collect_input_files(self) -> List[Path]:
        """Collect all input files to process
        
        Uses appropriate method based on enabled features
        """
        if self.features.get("smart_selection") and self.file_tracker:
            # Use smart selection from file tracker
            return self.file_tracker.get_unprocessed_files()
            
        elif self.features.get("memory_streaming"):
            # Use streaming approach
            return list(self._discover_files_streaming())
            
        else:
            # Standard collection
            return self._collect_input_files_standard()
            
    def _collect_input_files_standard(self) -> List[Path]:
        """Standard file collection method"""
        input_files = []
        
        if self.input_dir.is_file():
            input_files.append(self.input_dir)
        else:
            # Collect all text files
            for pattern in ['*.txt', '*.text', '*.input']:
                input_files.extend(self.input_dir.glob(pattern))
            
            # Also check for files without extension
            for file in self.input_dir.iterdir():
                if file.is_file() and file.suffix == '' and not file.name.startswith('.'):
                    input_files.append(file)
        
        # Sort for consistent ordering
        input_files.sort()
        
        logger.info(f"Found {len(input_files)} input files")
        return input_files
        
    def _discover_files_streaming(self) -> Iterator[Path]:
        """Stream files without loading all into memory (from optimized runner)"""
        patterns = ['*.txt', '*.text', '*.input']
        
        # If using smart features, get file info for ordering
        if self.features.get("dynamic_workers"):
            file_info = []
            
            for pattern in patterns:
                try:
                    for file in self.input_dir.glob(pattern):
                        # Skip processed files if tracking
                        if hasattr(self, 'processed_files') and file.stem in self.processed_files:
                            continue
                        
                        # Skip if output already exists
                        output_file = self.output_dir / f"{file.stem}.csv"
                        if output_file.exists() and output_file.stat().st_size > 100:
                            if hasattr(self, 'processed_files'):
                                self.processed_files.add(file.stem)
                            continue
                        
                        # Get file size
                        try:
                            size = file.stat().st_size
                            # Skip empty or huge files
                            if size == 0 or size > 100 * 1024 * 1024:  # 100MB limit
                                logger.warning(f"Skipping {file.name}: size {size}")
                                self.stats["skipped"] += 1
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
        else:
            # Simple streaming without size sorting
            for pattern in patterns:
                try:
                    for file in self.input_dir.glob(pattern):
                        # Skip processed files
                        if hasattr(self, 'processed_files') and file.stem in self.processed_files:
                            continue
                        
                        # Skip if output already exists
                        output_file = self.output_dir / f"{file.stem}.csv"
                        if output_file.exists() and output_file.stat().st_size > 100:
                            if hasattr(self, 'processed_files'):
                                self.processed_files.add(file.stem)
                            continue
                        
                        yield file
                except Exception as e:
                    logger.error(f"Error discovering files: {e}")
                    
    def get_optimal_workers(self) -> int:
        """Calculate optimal worker count based on current system state"""
        if not self.features.get("dynamic_workers"):
            return self.max_workers
            
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
            
    def process_file(self, file: Path) -> Tuple[bool, float, Optional[str]]:
        """Process a single file using appropriate method"""
        if self.features.get("adaptive_pool") and self.instance_pool:
            return self._process_file_with_pool(file)
        else:
            return self._process_file_direct(file)
            
    def _process_file_with_pool(self, file: Path) -> Tuple[bool, float, Optional[str]]:
        """Process file using instance pool"""
        instance_id = None
        mm_instance = None
        start_time = time.time()
        
        try:
            # Check system resources if health monitoring enabled
            if self.features.get("health_monitoring"):
                memory_percent = psutil.virtual_memory().percent
                if memory_percent > 90:
                    time.sleep(5)
                    gc.collect()
            
            # Get instance from pool
            instance_id, mm_instance = self.instance_pool.get_instance()
            
            # Calculate timeout based on file size if dynamic
            file_size = file.stat().st_size
            timeout = self._calculate_timeout(file_size) if self.features.get("dynamic_workers") else self.timeout
            
            # Create processor
            processor = FileProcessor(
                self.config.get("metamap_binary_path"),
                str(self.output_dir),
                self.config.get("metamap_processing_options", ""),
                timeout,
                metamap_instance=mm_instance,
                state_manager=self.state_manager if not self.features.get("memory_streaming") else None,
                file_tracker=self.file_tracker
            )
            
            # Process the file
            success, elapsed, error = processor.process_file(str(file))
            
            # Update statistics
            if success:
                self.stats["bytes_processed"] += file_size
                if hasattr(processor, 'concepts_found'):
                    self.stats["concepts_found"] += processor.concepts_found
                    
            # Record performance if tracking
            if self.features.get("dynamic_workers") and success:
                self.performance_history.append(elapsed)
                
            return success, elapsed, error
            
        finally:
            # Return instance to pool
            if instance_id is not None:
                self.instance_pool.release_instance(instance_id, mm_instance if 'mm_instance' in locals() else None)
                
    def _process_file_direct(self, file: Path) -> Tuple[bool, float, Optional[str]]:
        """Process file without instance pool"""
        processor = FileProcessor(
            self.config.get("metamap_binary_path"),
            str(self.output_dir),
            self.config.get("metamap_processing_options", ""),
            self.timeout,
            state_manager=self.state_manager if not self.features.get("memory_streaming") else None,
            file_tracker=self.file_tracker
        )
        
        return processor.process_file(str(file))
        
    def _calculate_timeout(self, file_size: int) -> int:
        """Dynamically calculate timeout based on file size"""
        # Base timeout adjustment
        size_mb = file_size / (1024 * 1024)
        
        if size_mb < 1:
            timeout = self.timeout
        elif size_mb < 5:
            timeout = int(self.timeout * 1.5)
        else:
            timeout = int(self.timeout * 2)
        
        # Adjust based on recent performance
        if hasattr(self, 'performance_history') and self.performance_history:
            avg_time = sum(self.performance_history) / len(self.performance_history)
            if avg_time > self.timeout * 0.8:
                timeout = int(timeout * 1.2)
        
        return timeout
        
    def validate_environment(self) -> Dict[str, Any]:
        """Validate processing environment (from validated runner)"""
        if not self.features.get("validation"):
            return {"valid": True}
            
        validation = {
            "valid": True,
            "errors": [],
            "warnings": []
        }
        
        # Check Java
        try:
            result = subprocess.run(["java", "-version"], capture_output=True, text=True)
            if result.returncode != 0:
                validation["errors"].append("Java not found")
                validation["valid"] = False
        except:
            validation["errors"].append("Java not accessible")
            validation["valid"] = False
            
        # Check MetaMap
        metamap_path = self.config.get("metamap_binary_path")
        if not metamap_path or not Path(metamap_path).exists():
            validation["errors"].append("MetaMap binary not found")
            validation["valid"] = False
            
        # Check disk space
        try:
            disk_usage = psutil.disk_usage(str(self.output_dir))
            if disk_usage.percent > 95:
                validation["errors"].append("Insufficient disk space")
                validation["valid"] = False
            elif disk_usage.percent > 80:
                validation["warnings"].append("Low disk space")
        except:
            pass
            
        return validation
        
    def process_with_progress(self, files: List[Path]) -> Dict[str, Any]:
        """Process files with progress tracking"""
        results = {
            "success": True,
            "total_files": len(files),
            "processed": 0,
            "failed": 0,
            "failed_files": [],
            "elapsed_time": 0
        }
        
        start_time = time.time()
        
        # Use chunked processing if enabled
        if self.features.get("chunked_processing"):
            return self._process_chunked(files)
            
        # Use monitored processing if enabled
        if self.features.get("live_monitoring"):
            return self._process_with_monitoring(files)
            
        # Standard processing with progress
        if self.show_progress:
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeRemainingColumn(),
                console=console,
                refresh_per_second=2
            )
            
            with progress:
                task = progress.add_task(
                    f"Processing {len(files)} files...",
                    total=len(files)
                )
                
                # Get optimal workers
                workers = self.get_optimal_workers()
                
                # Process with thread pool
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    future_to_file = {}
                    for file in files:
                        future = executor.submit(self.process_file, file)
                        future_to_file[future] = file
                    
                    # Process results
                    for future in as_completed(future_to_file):
                        file = future_to_file[future]
                        
                        try:
                            success, elapsed, error = future.result(timeout=self.timeout + 30)
                            
                            if success:
                                results["processed"] += 1
                                self._mark_completed(file)
                                logger.info(f"Processed {file.name} in {elapsed:.2f}s")
                            else:
                                results["failed"] += 1
                                results["failed_files"].append(str(file.resolve()))
                                self._mark_failed(file, error)
                                logger.error(f"Failed to process {file.name}: {error}")
                        
                        except Exception as e:
                            results["failed"] += 1
                            results["failed_files"].append(str(file.resolve()))
                            self._mark_failed(file, str(e))
                            logger.error(f"Exception processing {file.name}: {e}")
                        
                        progress.update(task, advance=1)
                        
                        # Update job manager
                        if self.job_manager and self.job_id:
                            self.job_manager.update_progress(self.job_id, {
                                'total_files': len(files),
                                'processed': results["processed"],
                                'failed': results["failed"],
                                'percentage': int((results["processed"] + results["failed"]) / len(files) * 100)
                            })
        
        else:
            # Process without progress bar
            workers = self.get_optimal_workers()
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_to_file = {}
                for file in files:
                    future = executor.submit(self.process_file, file)
                    future_to_file[future] = file
                
                for future in as_completed(future_to_file):
                    file = future_to_file[future]
                    
                    try:
                        success, elapsed, error = future.result(timeout=self.timeout + 30)
                        
                        if success:
                            results["processed"] += 1
                            self._mark_completed(file)
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(str(file.resolve()))
                            self._mark_failed(file, error)
                    
                    except Exception as e:
                        results["failed"] += 1
                        results["failed_files"].append(str(file.resolve()))
                        self._mark_failed(file, str(e))
        
        results["elapsed_time"] = time.time() - start_time
        results["throughput"] = results["processed"] / results["elapsed_time"] if results["elapsed_time"] > 0 else 0
        
        return results
        
    def _process_chunked(self, files: List[Path]) -> Dict[str, Any]:
        """Process files in chunks (from chunked runner)"""
        results = {
            "success": True,
            "total_files": len(files),
            "processed": 0,
            "failed": 0,
            "failed_files": [],
            "elapsed_time": 0
        }
        
        start_time = time.time()
        chunk_num = 0
        
        # Process in chunks
        for i in range(0, len(files), self.chunk_size):
            chunk = files[i:i + self.chunk_size]
            chunk_num += 1
            
            logger.info(f"Processing chunk {chunk_num} ({len(chunk)} files)")
            
            # Process chunk
            chunk_results = self._process_chunk(chunk, chunk_num)
            
            # Update totals
            results["processed"] += chunk_results["processed"]
            results["failed"] += chunk_results["failed"]
            results["failed_files"].extend(chunk_results["failed_files"])
            
            # Save state after each chunk
            if self.features.get("memory_streaming"):
                self._save_lightweight_state()
                
            # Garbage collection
            gc.collect()
            
            # Health check
            if self.features.get("health_monitoring"):
                self._perform_health_check()
                
        results["elapsed_time"] = time.time() - start_time
        results["throughput"] = results["processed"] / results["elapsed_time"] if results["elapsed_time"] > 0 else 0
        
        return results
        
    def _process_chunk(self, files: List[Path], chunk_num: int) -> Dict[str, Any]:
        """Process a single chunk of files"""
        results = {
            "processed": 0,
            "failed": 0,
            "failed_files": []
        }
        
        # Get optimal workers for this chunk
        workers = self.get_optimal_workers()
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_file = {}
            for file in files:
                future = executor.submit(self.process_file, file)
                future_to_file[future] = file
            
            for future in as_completed(future_to_file):
                file = future_to_file[future]
                
                try:
                    success, elapsed, error = future.result(timeout=self.timeout + 30)
                    
                    if success:
                        results["processed"] += 1
                        self._mark_completed(file)
                    else:
                        results["failed"] += 1
                        results["failed_files"].append(str(file.resolve()))
                        self._mark_failed(file, error)
                        
                except Exception as e:
                    results["failed"] += 1
                    results["failed_files"].append(str(file.resolve()))
                    self._mark_failed(file, str(e))
                    
        logger.info(f"Chunk {chunk_num} complete: {results['processed']} processed, {results['failed']} failed")
        return results
        
    def _process_with_monitoring(self, files: List[Path]) -> Dict[str, Any]:
        """Process files with live monitoring (from monitored runner)"""
        # Initialize monitor
        from ..monitoring.unified_monitor import UnifiedMonitor
        self.monitor = UnifiedMonitor(self.output_dir)
        
        # Start monitoring in background
        self.monitor_thread = threading.Thread(target=self.monitor.start, daemon=True)
        self.monitor_thread.start()
        
        # Process files
        results = self.process_with_progress(files)
        
        # Stop monitor
        if self.monitor:
            self.monitor.stop()
            
        return results
        
    def _mark_completed(self, file: Path):
        """Mark file as completed"""
        file_str = str(file.resolve())
        
        # Update state manager
        if self.state_manager:
            self.state_manager.mark_completed(file_str)
            
        # Update lightweight state
        if hasattr(self, 'processed_files'):
            self.processed_files.add(file.stem)
            
        # Update file tracker
        if self.file_tracker:
            try:
                output_file = self.output_dir / f"{file.stem}.csv"
                if output_file.exists():
                    # Count concepts if possible
                    concepts = 0
                    try:
                        with open(output_file, 'r') as f:
                            concepts = sum(1 for line in f if line.strip() and not line.startswith('#'))
                    except:
                        pass
                    
                    self.file_tracker.mark_processed(file, concepts)
            except:
                pass
                
    def _mark_failed(self, file: Path, error: str):
        """Mark file as failed"""
        file_str = str(file.resolve())
        
        # Update state manager
        if self.state_manager:
            self.state_manager.mark_failed(file_str, error)
            
        # Update lightweight state
        if hasattr(self, 'failed_files'):
            self.failed_files.add(file.stem)
            
        # Update file tracker
        if self.file_tracker:
            try:
                self.file_tracker.mark_failed(file, error)
            except:
                pass
                
    def _perform_health_check(self):
        """Perform system health check"""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory_percent = psutil.virtual_memory().percent
            disk_usage = psutil.disk_usage(str(self.output_dir)).percent
            
            logger.info(f"System health: CPU {cpu_percent:.1f}%, RAM {memory_percent:.1f}%, Disk {disk_usage:.1f}%")
            
            # Adjust workers if needed
            if self.features.get("dynamic_workers"):
                if memory_percent > 90 or cpu_percent > 95:
                    logger.warning("System under high load, reducing workers")
                    self.max_workers = max(1, self.max_workers - 1)
                elif memory_percent < 60 and cpu_percent < 50:
                    # System has capacity
                    if self.max_workers < self.config.get("max_parallel_workers", 4):
                        self.max_workers += 1
                        
        except Exception as e:
            logger.debug(f"Health check error: {e}")
            
    def run(self) -> Dict[str, Any]:
        """Run processing with all configured features"""
        logger.info(f"Starting unified processing in {self.mode} mode")
        
        # Update job status
        if self.job_manager and self.job_id:
            self.job_manager.start_job(self.job_id)
        
        # Validate environment if enabled
        if self.features.get("validation"):
            validation = self.validate_environment()
            if not validation["valid"]:
                logger.error(f"Environment validation failed: {validation['errors']}")
                return {
                    "success": False,
                    "error": f"Validation failed: {', '.join(validation['errors'])}"
                }
        
        # Ensure servers are running
        if not self.server_manager.is_tagger_server_running():
            logger.warning("Starting MetaMap servers...")
            if not self.server_manager.start_all():
                return {
                    "success": False,
                    "error": "Failed to start MetaMap servers"
                }
                
        # Initialize instance pool
        if self.use_instance_pool and not self.instance_pool:
            if self.features.get("adaptive_pool"):
                self.instance_pool = AdaptivePoolManager(self.config)
            else:
                self.instance_pool = MetaMapInstancePool(self.config)
        
        try:
            # Collect files
            input_files = self.collect_input_files()
            if not input_files:
                return {
                    "success": False,
                    "error": "No input files found"
                }
            
            # Filter pending files
            if self.features.get("smart_selection") and self.file_tracker:
                pending_files = input_files  # Already filtered by file tracker
            else:
                pending_files = self._filter_pending_files(input_files)
                
            if not pending_files:
                return {
                    "success": True,
                    "total_files": len(input_files),
                    "processed": len(input_files),
                    "failed": 0,
                    "elapsed_time": 0,
                    "throughput": 0
                }
            
            # Update statistics
            self.stats["total_files"] = len(input_files)
            self.stats["start_time"] = time.time()
            
            # Process files
            results = self.process_with_progress(pending_files)
            
            # Handle retries if configured
            if self.config.get("retry_max_attempts", 0) > 0 and results["failed_files"]:
                logger.info(f"Retrying {len(results['failed_files'])} failed files...")
                
                retry_results = self.retry_manager.retry_failed_files(
                    results["failed_files"], 
                    lambda file_path: self.process_file(Path(file_path))
                )
                
                # Update results
                results["processed"] += retry_results["recovered"]
                results["failed"] -= retry_results["recovered"]
                results["failed_files"] = retry_results["still_failed"]
                results["retry_summary"] = retry_results
            
            # Final statistics
            self.stats["end_time"] = time.time()
            self.stats["processed"] = results["processed"]
            self.stats["failed"] = results["failed"]
            
            # Add extended statistics
            results.update({
                "bytes_processed": self.stats.get("bytes_processed", 0),
                "concepts_found": self.stats.get("concepts_found", 0),
                "mode": self.mode,
                "features": self.features
            })
            
            logger.info(f"Processing complete: {results['processed']} successful, "
                       f"{results['failed']} failed, elapsed time: {results['elapsed_time']:.2f}s")
            
            return results
            
        finally:
            # Cleanup
            if self.instance_pool:
                logger.info("Shutting down instance pool...")
                self.instance_pool.shutdown()
                
            # Save final state
            if self.features.get("memory_streaming"):
                self._save_lightweight_state()
                
            # Update job status
            if self.job_manager and self.job_id:
                if 'results' in locals():
                    error = None if results.get('success', True) else results.get('error', 'Processing failed')
                    self.job_manager.complete_job(self.job_id, error)
                else:
                    self.job_manager.complete_job(self.job_id, "Process terminated unexpectedly")
                    
    def _filter_pending_files(self, input_files: List[Path]) -> List[Path]:
        """Filter out already processed files"""
        pending = []
        
        for file in input_files:
            # Check various tracking methods
            file_str = str(file.resolve())
            
            # Check state manager
            if self.state_manager and self.state_manager.is_completed(file_str):
                logger.debug(f"Skipping completed file: {file}")
                continue
                
            # Check lightweight state
            if hasattr(self, 'processed_files') and file.stem in self.processed_files:
                continue
            
            # Check if output exists and is valid
            output_file = self.output_dir / f"{file.stem}.csv"
            if output_file.exists() and output_file.stat().st_size > 100:
                # Verify it has proper end marker
                try:
                    with open(output_file, 'r') as f:
                        lines = f.readlines()
                        if lines and "META_BATCH_END" in lines[-1]:
                            # Mark as completed
                            if self.state_manager:
                                self.state_manager.mark_completed(file_str)
                            if hasattr(self, 'processed_files'):
                                self.processed_files.add(file.stem)
                            logger.debug(f"Skipping file with valid output: {file}")
                            continue
                except:
                    pass
            
            pending.append(file)
        
        logger.info(f"Filtered to {len(pending)} pending files")
        return pending
        
    @classmethod
    def resume(cls, output_dir: str, config: PyMMConfig = None, mode: str = ProcessingMode.SMART) -> Dict[str, Any]:
        """Resume interrupted processing"""
        config = config or PyMMConfig()
        state_manager = StateManager(output_dir)
        
        # Find input directory from state
        if state_manager._state.get("completed_files"):
            first_file = Path(state_manager._state["completed_files"][0])
            input_dir = first_file.parent
        else:
            return {
                "success": False,
                "error": "Cannot determine input directory from state"
            }
        
        # Create processor and run
        processor = cls(str(input_dir), output_dir, config, mode)
        return processor.run()
        
    def get_interactive_options(self) -> Tuple[List[Path], str]:
        """Get interactive processing options (for smart mode)"""
        if not self.features.get("smart_selection") or not self.file_tracker:
            # Just return all pending files
            files = self.collect_input_files()
            return files, f"Processing {len(files)} files"
            
        # Use smart batch runner's interactive selection
        from .smart_batch_runner import SmartBatchRunner
        smart_runner = SmartBatchRunner(self.config)
        return smart_runner.show_processing_options()
        
    def run_background(self) -> subprocess.Popen:
        """Run processing in background"""
        log_file = self.logs_dir / f"background_{self.mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Create command
        cmd = [
            sys.executable, "-m", "pymm",
            "process",
            str(self.input_dir),
            str(self.output_dir),
            "--mode", self.mode,
            "--workers", str(self.max_workers),
            "--timeout", str(self.timeout),
            "--background"
        ]
        
        if not self.show_progress:
            cmd.append("--no-progress")
            
        # Start process
        with open(log_file, 'w') as f:
            process = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                start_new_session=True
            )
            
        logger.info(f"Started background process {process.pid}, log: {log_file}")
        return process 