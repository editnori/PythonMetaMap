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
import time
import logging
import json
import gc
import subprocess
import sys
import threading
import psutil
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from collections import defaultdict, deque
import os

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.console import Console

from ..core.config import PyMMConfig
from ..core.state import StateManager
from ..core.job_manager import get_job_manager
from ..core.file_tracker import UnifiedFileTracker
from ..core.enhanced_state import AtomicStateManager
from ..server.manager import ServerManager
from ..server.health_check import HealthMonitor
from .instance_pool import MetaMapInstancePool
from .worker import FileProcessor
from .retry_manager import RetryManager
from .java_bridge import JavaAPIBridge

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
    FAST = "fast"  # New Java API with optimizations
    ULTRA_FAST = "ultra_fast"  # Java API + MetaMapLite (future)


class UnifiedProcessor:
    """Unified processor with all features from various batch runners"""

    def __init__(
            self,
            input_dir: str,
            output_dir: str,
            config: PyMMConfig = None,
            mode: str = ProcessingMode.SMART):
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

        # Core configuration
        self.max_workers = self.config.get("max_parallel_workers", 4)
        self.timeout = self.config.get("pymm_timeout", 300)
        self.use_instance_pool = self.config.get("use_instance_pool", True)
        self.show_progress = self.config.get("progress_bar", True)

        # Advanced features flags based on mode - MUST BE DONE FIRST
        self._configure_features()

        # Setup logging (after features are configured)
        self.logs_dir = self.output_dir / "logs"
        try:
            self.logs_dir.mkdir(parents=True, exist_ok=True)
        except FileExistsError:
            # Directory already exists, which is fine
            pass
        except OSError as e:
            # Handle Windows/WSL specific issues
            if not self.logs_dir.exists():
                raise
        self._setup_logging()

        # State management - use appropriate state manager based on features
        if self.features.get("unified_tracking"):
            self.file_tracker = UnifiedFileTracker(self.config, str(self.input_dir), str(self.output_dir))
            self.state_manager = self.file_tracker.state_manager if hasattr(
                self.file_tracker, 'state_manager') else StateManager(str(self.output_dir))
        else:
            self.state_manager = StateManager(str(self.output_dir))
            self.file_tracker = None

        # Enhanced state for atomic operations (from smart runner)
        if self.features.get("atomic_state"):
            self.atomic_state = AtomicStateManager(self.output_dir)
        else:
            self.atomic_state = None

        # Job management
        self.job_manager = get_job_manager() if self.config.get(
            "enable_job_tracking", True) else None
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

        # Check if Java API is configured or if FAST mode is enabled
        self.use_java_api = self.config.get("use_java_api", False) or self.mode in [ProcessingMode.FAST, ProcessingMode.ULTRA_FAST]
        self.java_api_path = self.config.get("java_api_path", "")
        self.java_bridge = None
        
        # Auto-configure settings for fast mode
        if self.mode == ProcessingMode.FAST:
            self._auto_configure_fast_mode()
        
        if self.use_java_api:
            logger.info("Java API mode enabled - will use optimized Java processing")
            # Create a Config instance for Enhanced JavaAPIBridge
            from ..core.config import Config
            from .java_bridge_v2 import EnhancedJavaAPIBridge
            
            bridge_config = Config.from_pymm_config(self.config)
            bridge_config.java_api_path = self.java_api_path
            
            try:
                self.java_bridge = EnhancedJavaAPIBridge(bridge_config)
                logger.info("Enhanced Java API bridge initialized successfully")
            except Exception as e:
                logger.warning(f"Failed to initialize Enhanced Java API: {e}")
                logger.info("Falling back to standard binary mode")
                self.use_java_api = False
                self.java_bridge = None
        else:
            logger.info("Standard binary mode - using MetaMap binary for processing")

        # Server management
        self.server_manager = ServerManager(self.config)
        self.health_monitor = HealthMonitor(self.config, self.server_manager)

        # Instance pool - use adaptive pool for advanced modes
        self.instance_pool = None
        if self.use_instance_pool:
            if self.features.get("adaptive_pool"):
                logger.info("Creating adaptive instance pool")
                # Create MetaMapInstancePool with adaptive management
                from .pool_manager import AdaptivePoolManager
                adaptive_manager = AdaptivePoolManager(self.config)
                # Get optimal worker count from adaptive manager
                optimal_workers = adaptive_manager.calculate_optimal_workers()
                self.instance_pool = MetaMapInstancePool(
                    self.config, max_instances=optimal_workers)
                # Store adaptive manager for dynamic adjustments
                self.adaptive_manager = adaptive_manager
                if hasattr(adaptive_manager, 'start_monitoring'):
                    adaptive_manager.start_monitoring()
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
            
        elif self.mode == ProcessingMode.FAST:
            self.features.update({
                "use_java_api": True,
                "adaptive_pool": True,
                "unified_tracking": True,
                "health_monitoring": True,
                "chunked_processing": True,
                "java_optimizations": True
            })
            
        elif self.mode == ProcessingMode.ULTRA_FAST:
            self.features.update({
                "use_metamap_lite": True,
                "use_java_api": True,
                "ultra_optimizations": True,
                "unified_tracking": True
            })

        # Override with config if specified
        for feature, enabled in self.features.items():
            config_key = f"feature_{feature}"
            if self.config.get(config_key) is not None:
                self.features[feature] = self.config.get(config_key)

    def _auto_configure_fast_mode(self):
        """Auto-configure settings for fast mode based on system resources"""
        try:
            import psutil
            
            # Get system info
            cpu_count = os.cpu_count() or 4
            memory_gb = psutil.virtual_memory().total / (1024**3)
            available_memory_gb = psutil.virtual_memory().available / (1024**3)
            
            logger.info(f"System info: {cpu_count} CPUs, {memory_gb:.1f}GB total RAM, {available_memory_gb:.1f}GB available")
            
            # Auto-configure Java heap size
            if not self.config.get("java_heap_size"):
                # Use 50% of available memory for Java heap, max 32GB
                heap_gb = min(int(available_memory_gb * 0.5), 32)
                heap_gb = max(2, heap_gb)  # Minimum 2GB
                self.config.set("java_heap_size", f"{heap_gb}G")
                logger.info(f"Auto-configured Java heap size: {heap_gb}GB")
            
            # Auto-configure parallel workers
            if self.config.get("max_parallel_workers") == 4:  # Default value
                # For fast mode with Java API, we can use more workers
                # Each Java instance pool already handles parallelization
                optimal_workers = min(cpu_count // 2, 8)
                optimal_workers = max(2, optimal_workers)
                self.max_workers = optimal_workers
                self.config.set("max_parallel_workers", optimal_workers)
                logger.info(f"Auto-configured parallel workers: {optimal_workers}")
            
            # Auto-configure Java API pool size
            if not self.config.get("java_api_pool_size"):
                # Pool size should be based on available memory
                # Each MetaMap instance uses ~500MB-1GB
                pool_size = min(int(available_memory_gb / 1.5), cpu_count, 12)
                pool_size = max(4, pool_size)  # Minimum 4 instances
                self.config.set("java_api_pool_size", pool_size)
                logger.info(f"Auto-configured Java API pool size: {pool_size}")
            
            # Auto-configure chunk size for large datasets
            if not self.config.get("chunk_size"):
                # Larger chunks for systems with more memory
                if memory_gb >= 32:
                    chunk_size = 500
                elif memory_gb >= 16:
                    chunk_size = 250
                else:
                    chunk_size = 100
                self.chunk_size = chunk_size
                self.config.set("chunk_size", chunk_size)
                logger.info(f"Auto-configured chunk size: {chunk_size}")
            
            # Auto-configure timeout based on system performance
            if self.config.get("pymm_timeout") == 300:  # Default value
                # Faster systems can use shorter timeouts
                if cpu_count >= 16 and memory_gb >= 32:
                    timeout = 180
                elif cpu_count >= 8 and memory_gb >= 16:
                    timeout = 240
                else:
                    timeout = 300
                self.timeout = timeout
                self.config.set("pymm_timeout", timeout)
                logger.info(f"Auto-configured timeout: {timeout}s")
            
            # Enable additional optimizations for powerful systems
            if cpu_count >= 16 and memory_gb >= 32:
                self.config.set("java_api_thread_pool", min(cpu_count, 16))
                self.config.set("java_api_optimizations", True)
                logger.info("Enabled advanced optimizations for high-performance system")
            
        except Exception as e:
            logger.warning(f"Failed to auto-configure fast mode: {e}")
            logger.info("Using default configuration")

    def _setup_logging(self):
        """Setup file logging for batch processing"""
        # Ensure logs directory exists with better error handling
        try:
            # Create logs directory if it doesn't exist
            self.logs_dir.mkdir(parents=True, exist_ok=True)
            
            # Double-check it exists (for symbolic links and special paths)
            if not self.logs_dir.exists():
                # Try alternative creation method
                import os
                os.makedirs(str(self.logs_dir), exist_ok=True)
        except Exception as e:
            # If we still can't create logs dir, use output dir directly
            logger.warning(f"Failed to create logs directory: {e}")
            self.logs_dir = self.output_dir
            try:
                self.logs_dir.mkdir(parents=True, exist_ok=True)
            except:
                # Last resort - use temp directory
                import tempfile
                self.logs_dir = Path(tempfile.gettempdir()) / "pymm_logs"
                self.logs_dir.mkdir(parents=True, exist_ok=True)
                logger.warning(f"Using temporary logs directory: {self.logs_dir}")
        
        # Create log file with sanitized name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = f"{self.mode}_run_{timestamp}.log"
        log_file = self.logs_dir / log_filename

        try:
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
        except Exception as e:
            # If we can't create the log file, just log to console
            logger.warning(f"Failed to create log file: {e}")
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
        except BaseException:
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
                logger.info(
                    f"Loaded state: {len(self.processed_files)} processed")
            except Exception as e:
                logger.warning(f"Failed to load state: {e}")

    def _save_lightweight_state(self):
        """Save lightweight state"""
        if self.features.get("memory_streaming"):
            state_file = self.output_dir / ".processing_state.json"
            try:
                state = {
                    # Limit size
                    'processed': list(self.processed_files)[-10000:],
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
                        if hasattr(
                                self, 'processed_files') and file.stem in self.processed_files:
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
                                logger.warning(
                                    f"Skipping {file.name}: size {size}")
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
                        if hasattr(
                                self, 'processed_files') and file.stem in self.processed_files:
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
        
        # Use Java API if configured
        if self.java_bridge:
            return self._process_file_with_java_api(file)
        elif self.features.get("adaptive_pool") and self.instance_pool:
            return self._process_file_with_pool(file)
        else:
            return self._process_file_direct(file)

    def _process_file_with_java_api(self, file: Path) -> Tuple[bool, float, Optional[str]]:
        """Process file using Java API bridge"""
        start_time = time.time()
        output_file = self.output_dir / f"{file.stem}.csv"
        
        try:
            logger.info(f"Processing {file.name} with Java API")
            
            # Delete any existing output file from previous failed attempts
            if output_file.exists():
                logger.info(f"Removing existing output file from previous attempt: {output_file}")
                try:
                    output_file.unlink()
                except Exception as e:
                    logger.warning(f"Could not delete existing output file: {e}")
            
            # Mark as started
            if self.file_tracker:
                self.file_tracker.mark_file_started(file)
            
            # Process with Enhanced Java API
            if hasattr(self.java_bridge, 'process_single_file_fast'):
                # Use enhanced fast processing
                success = self.java_bridge.process_single_file_fast(
                    str(file),
                    str(output_file),
                    options={
                        "topMappingOnly": self.features.get("java_optimizations", True),
                        "scoreThreshold": -800 if self.features.get("java_optimizations", True) else -1000
                    }
                )
            else:
                # Fallback to standard Java API
                success = self.java_bridge.process_single_file(
                    str(file),
                    str(output_file),
                    self.config.get("metamap_processing_options", "")
                )
            
            processing_time = time.time() - start_time
            
            if success:
                # Verify output file has content
                if not output_file.exists():
                    logger.warning(f"Java API reported success but output file not found: {output_file}")
                    success = False
                    error = "Output file not created"
                else:
                    # Check if file has actual content (more than just header)
                    with open(output_file, 'r') as f:
                        lines = f.readlines()
                        if len(lines) <= 1:
                            logger.warning(f"Output file exists but contains no concepts: {output_file}")
                            # For files with no medical content, this might be valid
                            # But log it for debugging
                            logger.info(f"File processed in {processing_time:.2f}s with 0 concepts")
                
                # Mark as completed
                if self.file_tracker:
                    # Count concepts in output
                    concepts_count = 0
                    if output_file.exists():
                        with open(output_file, 'r') as f:
                            concepts_count = sum(1 for line in f) - 1  # Minus header
                    
                    self.file_tracker.mark_file_completed(
                        file, 
                        concepts_found=concepts_count,
                        processing_time=processing_time
                    )
                
                self.stats["processed"] += 1
                return True, processing_time, None
            else:
                error = "Java API processing failed"
                # Check if it's a javac issue
                if "Failed to compile Java sources" in error or "javac" in error:
                    logger.warning("Java API failed due to missing JDK, falling back to standard processing")
                    # Disable Java API for future files
                    self.java_bridge = None
                    self.use_java_api = False
                    # Process with standard method
                    return self._process_file_direct(file)
                
                if self.file_tracker:
                    self.file_tracker.mark_file_failed(file, error)
                self.stats["failed"] += 1
                return False, processing_time, error
                
        except Exception as e:
            error = str(e)
            logger.error(f"Java API processing error for {file.name}: {error}")
            
            # Check if it's a compilation issue
            if "Failed to compile Java sources" in error or "javac" in error:
                logger.warning("Java API failed due to missing JDK, falling back to standard processing")
                # Disable Java API for future files
                self.java_bridge = None
                self.use_java_api = False
                # Process with standard method
                return self._process_file_direct(file)
            
            if self.file_tracker:
                self.file_tracker.mark_file_failed(file, error)
            self.stats["failed"] += 1
            return False, time.time() - start_time, error

    def _process_file_with_pool(
            self, file: Path) -> Tuple[bool, float, Optional[str]]:
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
            timeout = self._calculate_timeout(file_size) if self.features.get(
                "dynamic_workers") else self.timeout

            # Create processor
            processor = FileProcessor(
                self.config.get("metamap_binary_path"),
                str(self.output_dir),
                self.config.get("metamap_processing_options", ""),
                timeout,
                metamap_instance=mm_instance,
                state_manager=self.state_manager if not self.features.get("memory_streaming") else None,
                file_tracker=self.file_tracker,
                config=self.config
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
                self.instance_pool.release_instance(
                    instance_id, mm_instance if 'mm_instance' in locals() else None)

    def _process_file_direct(
            self, file: Path) -> Tuple[bool, float, Optional[str]]:
        """Process file without instance pool"""
        processor = FileProcessor(
            self.config.get("metamap_binary_path"),
            str(self.output_dir),
            self.config.get("metamap_processing_options", ""),
            self.timeout,
            state_manager=self.state_manager if not self.features.get("memory_streaming") else None,
            file_tracker=self.file_tracker,
            config=self.config
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
            avg_time = sum(self.performance_history) / \
                len(self.performance_history)
            if avg_time > self.timeout * 0.8:
                timeout = int(timeout * 1.2)

        return timeout

    def validate_environment(self) -> Dict[str, Any]:
        """Validate processing environment (from validated runner)"""
        validation = {
            "valid": True,
            "errors": [],
            "warnings": []
        }
        
        # Always run basic checks regardless of mode
        # Check Java
        try:
            result = subprocess.run(
                ["java", "-version"], capture_output=True, text=True)
            if result.returncode != 0:
                if self.mode in [ProcessingMode.FAST, ProcessingMode.ULTRA_FAST]:
                    validation["errors"].append("Java not found - required for FAST mode")
                    validation["valid"] = False
                else:
                    validation["warnings"].append("Java not found")
        except BaseException:
            if self.mode in [ProcessingMode.FAST, ProcessingMode.ULTRA_FAST]:
                validation["errors"].append("Java not accessible - required for FAST mode")
                validation["valid"] = False
            else:
                validation["warnings"].append("Java not accessible")

        # Check MetaMap - more lenient validation for fast mode
        metamap_path = self.config.get("metamap_binary_path")
        
        # In fast mode with Java API, check for JARs instead of binary
        if self.mode == ProcessingMode.FAST and self.use_java_api:
            # Check Java API path
            java_api_path = self.config.get("java_api_path")
            if not java_api_path:
                validation["errors"].append("Java API path not configured for FAST mode")
                validation["valid"] = False
            elif not Path(java_api_path).exists():
                validation["errors"].append(f"Java API path does not exist: {java_api_path}")
                validation["valid"] = False
            else:
                # Check for required JAR files in various locations
                jar_locations = [
                    ("metamap-api-2.0.jar", ["src/javaapi/target", "src/javaapi/dist", "lib", "."]),
                    ("prologbeans.jar", ["src/javaapi/dist", "lib", "."])
                ]
                missing_jars = []
                found_jars = {}
                
                for jar_name, search_dirs in jar_locations:
                    jar_found = False
                    for subdir in search_dirs:
                        jar_path = Path(java_api_path) / subdir / jar_name
                        if jar_path.exists():
                            jar_found = True
                            found_jars[jar_name] = str(jar_path)
                            logger.debug(f"Found {jar_name} at: {jar_path}")
                            break
                    if not jar_found:
                        missing_jars.append(jar_name)
                
                if missing_jars:
                    validation["errors"].append(f"Missing required JARs: {', '.join(missing_jars)}")
                    validation["warnings"].append("Found JARs: " + ", ".join(f"{k}: {v}" for k, v in found_jars.items()))
                    validation["valid"] = False
                else:
                    logger.info("All required JARs found in Java API installation")
                    
            # Check MetaMap servers for Java API
            if validation["valid"] and not self.server_manager.is_tagger_server_running():
                validation["warnings"].append("MetaMap servers not running - will attempt to start")
        else:
            # Standard validation for non-fast modes
            if not metamap_path:
                # Try auto-detection
                try:
                    from ..utils.auto_detector import AutoDetector
                    detector = AutoDetector()
                    detected_path = detector.detect_metamap_binary()
                    if detected_path:
                        self.config.set("metamap_binary_path", detected_path)
                        logger.info(f"Auto-detected MetaMap at: {detected_path}")
                        metamap_path = detected_path
                    else:
                        validation["errors"].append("MetaMap binary not found")
                        validation["valid"] = False
                except Exception as e:
                    logger.debug(f"Auto-detection failed: {e}")
                    validation["errors"].append("MetaMap binary path not configured")
                    validation["valid"] = False
            elif not Path(metamap_path).exists():
                validation["errors"].append(f"MetaMap binary not found at: {metamap_path}")
                validation["valid"] = False

        # Check disk space
        try:
            disk_usage = psutil.disk_usage(str(self.output_dir))
            if disk_usage.percent > 95:
                validation["errors"].append("Insufficient disk space")
                validation["valid"] = False
            elif disk_usage.percent > 80:
                validation["warnings"].append("Low disk space")
        except BaseException:
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
                            success, elapsed, error = future.result(
                                timeout=self.timeout + 30)

                            if success:
                                results["processed"] += 1
                                self._mark_completed(file)
                                logger.info(
                                    f"Processed {file.name} in {elapsed:.2f}s")
                            else:
                                results["failed"] += 1
                                results["failed_files"].append(
                                    str(file.resolve()))
                                self._mark_failed(file, error)
                                logger.error(
                                    f"Failed to process {file.name}: {error}")

                        except Exception as e:
                            results["failed"] += 1
                            results["failed_files"].append(str(file.resolve()))
                            self._mark_failed(file, str(e))
                            logger.error(
                                f"Exception processing {file.name}: {e}")

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
                        success, elapsed, error = future.result(
                            timeout=self.timeout + 30)

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
        results["throughput"] = results["processed"] / \
            results["elapsed_time"] if results["elapsed_time"] > 0 else 0

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
        results["throughput"] = results["processed"] / \
            results["elapsed_time"] if results["elapsed_time"] > 0 else 0

        return results

    def _process_chunk(
            self, files: List[Path], chunk_num: int) -> Dict[str, Any]:
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
                    success, elapsed, error = future.result(
                        timeout=self.timeout + 30)

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

        logger.info(
            f"Chunk {chunk_num} complete: {results['processed']} processed, {results['failed']} failed")
        return results
    
    def _process_batch_with_java_api(self, files: List[Path]) -> Dict[str, Any]:
        """Process a batch of files using the Java API bridge"""
        results = {
            "success": True,
            "processed": 0,
            "failed": 0,
            "failed_files": []
        }
        
        try:
            # Prepare file paths and output paths
            input_paths = [str(f) for f in files]
            output_paths = [str(self.output_dir / f"{f.stem}.csv") for f in files]
            
            # Call Java API batch processing
            batch_results = self.java_bridge.process_batch_fast(
                input_paths,
                output_paths,
                options={
                    "topMappingOnly": self.features.get("java_optimizations", True),
                    "scoreThreshold": -800 if self.features.get("java_optimizations", True) else -1000,
                    "chunkLargeDocuments": True,
                    "chunkSizeChars": 5000
                }
            )
            
            # Process results
            if batch_results and "results" in batch_results:
                for file_path, result in batch_results["results"].items():
                    file = Path(file_path)
                    if result.get("status") == "success":
                        results["processed"] += 1
                        self._mark_completed(file)
                    else:
                        results["failed"] += 1
                        results["failed_files"].append(str(file))
                        self._mark_failed(file, result.get("error", "Unknown error"))
            
            # Update success based on results
            results["success"] = results["failed"] == 0
            
        except Exception as e:
            logger.error(f"Java API batch processing failed: {e}")
            # Mark all files as failed
            for file in files:
                results["failed"] += 1
                results["failed_files"].append(str(file))
                self._mark_failed(file, str(e))
            results["success"] = False
            
        return results

    def _process_with_monitoring(self, files: List[Path]) -> Dict[str, Any]:
        """Process files with live monitoring (from monitored runner)"""
        # Initialize monitor
        from ..monitoring.unified_monitor import UnifiedMonitor
        self.monitor = UnifiedMonitor(self.output_dir)

        # Start monitoring in background
        self.monitor_thread = threading.Thread(
            target=self.monitor.start, daemon=True)
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
                            concepts = sum(
                                1 for line in f if line.strip() and not line.startswith('#'))
                    except BaseException:
                        pass

                    self.file_tracker.mark_processed(file, concepts)
            except BaseException:
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
            except BaseException:
                pass

    def _perform_health_check(self):
        """Perform system health check"""
        try:
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory_percent = psutil.virtual_memory().percent
            disk_usage = psutil.disk_usage(str(self.output_dir)).percent

            logger.info(
                f"System health: CPU {cpu_percent:.1f}%, RAM {memory_percent:.1f}%, Disk {disk_usage:.1f}%")

            # Adjust workers if needed
            if self.features.get("dynamic_workers"):
                if memory_percent > 90 or cpu_percent > 95:
                    logger.warning("System under high load, reducing workers")
                    self.max_workers = max(1, self.max_workers - 1)
                elif memory_percent < 60 and cpu_percent < 50:
                    # System has capacity
                    if self.max_workers < self.config.get(
                            "max_parallel_workers", 4):
                        self.max_workers += 1

        except Exception as e:
            logger.debug(f"Health check error: {e}")

    def run(self) -> Dict[str, Any]:
        """Run processing with all configured features"""
        logger.info(f"Starting unified processing in {self.mode} mode")

        # Update job status
        if self.job_manager and self.job_id:
            self.job_manager.start_job(self.job_id)

        # Always validate environment for all modes
        validation = self.validate_environment()
        if not validation["valid"]:
            logger.error(
                f"Environment validation failed: {validation['errors']}")
            # Show validation errors in console
            console.print("\n[red]Validation failed:[/red]")
            for error in validation["errors"]:
                console.print(f"  • {error}")
            if validation.get("warnings"):
                console.print("\n[yellow]Warnings:[/yellow]")
                for warning in validation["warnings"]:
                    console.print(f"  • {warning}")
            return {
                "success": False,
                "error": f"Validation failed: {', '.join(validation['errors'])}",
                "validation": validation
            }
        elif validation.get("warnings"):
            console.print("\n[yellow]Validation warnings:[/yellow]")
            for warning in validation["warnings"]:
                console.print(f"  • {warning}")

        # Ensure servers are running - only for modes that need them
        if not self.use_java_api or self.mode not in [ProcessingMode.FAST, ProcessingMode.ULTRA_FAST]:
            # Standard modes need MetaMap servers
            try:
                if not self.server_manager.is_tagger_server_running():
                    logger.warning("Tagger server not running, attempting to start...")
                    if not self.server_manager.start_all():
                        # Try once more with force kill
                        logger.warning("First start attempt failed, trying force restart...")
                        self.server_manager.force_kill_all()
                        time.sleep(2)
                        if not self.server_manager.start_all():
                            return {
                                "success": False,
                                "error": "Failed to start MetaMap servers after retry"
                            }
                else:
                    logger.info("MetaMap servers are already running")
            except Exception as e:
                logger.error(f"Error checking/starting servers: {e}")
                return {
                    "success": False,
                    "error": f"Failed to check/start MetaMap servers: {e}"
                }
        else:
            # Fast mode with Java API - servers are still required
            try:
                if not self.server_manager.is_tagger_server_running():
                    logger.info("Starting MetaMap servers for Java API...")
                    if self.server_manager.start_all():
                        logger.info("MetaMap servers started successfully")
                    else:
                        # Try once more with force kill
                        logger.warning("First start attempt failed, trying force restart...")
                        self.server_manager.force_kill_all()
                        time.sleep(2)
                        if not self.server_manager.start_all():
                            return {
                                "success": False,
                                "error": "Failed to start MetaMap servers required for Java API"
                            }
                else:
                    logger.info("MetaMap servers are already running")
            except Exception as e:
                logger.error(f"Error checking/starting servers for Java API: {e}")
                return {
                    "success": False,
                    "error": f"Failed to check/start MetaMap servers: {e}"
                }

        # Initialize instance pool and adaptive manager if not already done
        if self.use_instance_pool and not self.instance_pool:
            # Check if pool is explicitly disabled in config
            if self.config.get('use_instance_pool', True) == False:
                logger.info("Instance pool disabled by configuration")
                self.use_instance_pool = False
            else:
                try:
                    if self.features.get("adaptive_pool") and not hasattr(self, 'adaptive_manager'):
                        logger.info("Creating adaptive instance pool")
                        # Create MetaMapInstancePool with adaptive management
                        from .pool_manager import AdaptivePoolManager
                        self.adaptive_manager = AdaptivePoolManager(self.config)
                        # Get optimal worker count from adaptive manager
                        optimal_workers = self.adaptive_manager.calculate_optimal_workers()
                        logger.info(f"Creating instance pool with {optimal_workers} workers")
                        self.instance_pool = MetaMapInstancePool(
                            self.config, max_instances=optimal_workers)
                        # Fix: Check if the adaptive manager has the right methods
                        if hasattr(self.adaptive_manager, 'set_pool'):
                            self.adaptive_manager.set_pool(self.instance_pool)
                        if hasattr(self.adaptive_manager, 'start_monitoring'):
                            self.adaptive_manager.start_monitoring()
                    else:
                        # Standard instance pool
                        logger.info("Creating standard instance pool")
                        self.instance_pool = MetaMapInstancePool(self.config)
                    
                    # Warm up the pool with a few instances
                    if self.config.get("pool_warmup_instances", 0) > 0:
                        logger.info("Warming up instance pool...")
                        warmup_count = min(
                            self.config.get("pool_warmup_instances", 2),
                            self.instance_pool.max_instances
                        )
                        for i in range(warmup_count):
                            try:
                                instance_id, mm_instance = self.instance_pool._create_instance()
                                self.instance_pool.release_instance(instance_id, mm_instance)
                                logger.info(f"Warmed up instance {i+1}/{warmup_count}")
                            except Exception as e:
                                logger.warning(f"Failed to warm up instance {i+1}: {e}")
                                
                except Exception as e:
                    logger.error(f"Failed to create instance pool: {e}")
                    # Disable instance pool and continue without it
                    self.use_instance_pool = False
                    self.instance_pool = None

        try:
            # Collect files
            logger.info(f"Collecting input files from: {self.input_dir}")
            input_files = self.collect_input_files()
            logger.info(f"Found {len(input_files)} total input files")
            
            if not input_files:
                logger.warning(f"No input files found in {self.input_dir}")
                return {
                    "success": False,
                    "error": f"No input files found in {self.input_dir}"
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
            if (self.config.get("retry_max_attempts", 0) > 0 and 
                "failed_files" in results and 
                results.get("failed_files") and 
                len(results["failed_files"]) > 0):
                logger.info(
                    f"Retrying {len(results['failed_files'])} failed files...")

                retry_results = self.retry_manager.retry_failed_files(
                    results["failed_files"],
                    lambda file_path: self.process_file(Path(file_path))
                )

                # Update results
                results["processed"] += retry_results["recovered"]
                results["failed"] -= retry_results["recovered"]
                results["failed_files"] = retry_results["still_failed"]
                results["retry_summary"] = retry_results
            elif self.config.get("retry_max_attempts", 0) > 0:
                logger.debug("No failed files to retry")

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

            logger.info(
                f"Processing complete: {results['processed']} successful, "
                f"{results['failed']} failed, elapsed time: {results['elapsed_time']:.2f}s")

            return results

        finally:
            # Cleanup
            if self.instance_pool:
                logger.info("Shutting down instance pool...")
                self.instance_pool.shutdown()

            # Stop adaptive monitoring if enabled
            if hasattr(self, 'adaptive_manager') and self.adaptive_manager:
                logger.info("Stopping adaptive monitoring...")
                self.adaptive_manager.stop_monitoring()

            # Save final state
            if self.features.get("memory_streaming"):
                self._save_lightweight_state()

            # Update job status
            if self.job_manager and self.job_id:
                if 'results' in locals():
                    error = None if results.get(
                        'success', True) else results.get(
                        'error', 'Processing failed')
                    self.job_manager.complete_job(self.job_id, error)
                else:
                    self.job_manager.complete_job(
                        self.job_id, "Process terminated unexpectedly")

    def _filter_pending_files(self, input_files: List[Path]) -> List[Path]:
        """Filter out already processed files"""
        pending = []

        for file in input_files:
            # Check various tracking methods
            file_str = str(file.resolve())

            # Check state manager
            if self.state_manager and self.state_manager.is_completed(
                    file_str):
                logger.debug(f"Skipping completed file: {file}")
                continue

            # Check lightweight state
            if hasattr(
                    self,
                    'processed_files') and file.stem in self.processed_files:
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
                            logger.debug(
                                f"Skipping file with valid output: {file}")
                            continue
                except BaseException:
                    pass

            pending.append(file)

        logger.info(f"Filtered to {len(pending)} pending files")
        return pending

    @classmethod
    def resume(cls, output_dir: str, config: PyMMConfig = None,
               mode: str = ProcessingMode.SMART) -> Dict[str, Any]:
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

        # Use file tracker's interactive selection
        # TODO: Implement interactive file selection here
        files = self.collect_input_files()
        return files, f"Processing {len(files)} files"

    def run_background(self) -> subprocess.Popen:
        """Run processing in background"""
        log_file = self.logs_dir / \
            f"background_{self.mode}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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

        logger.info(
            f"Started background process {process.pid}, log: {log_file}")
        return process
