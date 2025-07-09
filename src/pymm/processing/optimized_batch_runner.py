"""Optimized batch processing with memory-efficient streaming"""
import os
import time
import logging
import json
import gc
from pathlib import Path
from typing import List, Dict, Optional, Iterator, Callable, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import threading
import psutil

from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.console import Console

from ..core.config import PyMMConfig
from ..server.manager import ServerManager
from .pool_manager import MetaMapInstancePool
from .worker import FileProcessor

logger = logging.getLogger(__name__)
console = Console()


class OptimizedBatchRunner:
    """Memory-efficient batch processing for large datasets"""
    
    def __init__(self, input_dir: str, output_dir: str, config: PyMMConfig):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.config = config
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Processing configuration with memory-aware defaults
        self.max_workers = config.get("max_parallel_workers", 4)
        self.timeout = config.get("pymm_timeout", 300)
        
        # Dynamic chunk size based on available memory
        memory_gb = psutil.virtual_memory().available / (1024**3)
        if memory_gb < 4:
            self.chunk_size = 50
        elif memory_gb < 8:
            self.chunk_size = 100
        elif memory_gb < 16:
            self.chunk_size = 250
        else:
            self.chunk_size = config.get("chunk_size", 500)
        
        # Lightweight state tracking using sets for O(1) lookups
        self.state_file = self.output_dir / ".processing_state.json"
        self.processed_files = set()
        self.failed_files = set()
        self._load_state()
        
        # Server and pool management
        self.server_manager = ServerManager(config)
        self.instance_pool = None
        self.pool_lock = threading.Lock()
        
        # Progress tracking
        self.progress_callback = None
        self.stats = {
            "total": 0,
            "processed": 0,
            "failed": 0,
            "start_time": None
        }
    
    def _load_state(self):
        """Load processing state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                self.processed_files = set(state.get('processed', []))
                self.failed_files = set(state.get('failed', []))
                logger.info(f"Loaded state: {len(self.processed_files)} processed")
            except Exception as e:
                logger.warning(f"Failed to load state: {e}")
    
    def _save_state(self):
        """Save processing state to file"""
        try:
            state = {
                'processed': list(self.processed_files),
                'failed': list(self.failed_files),
                'timestamp': datetime.now().isoformat()
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def _discover_files_streaming(self) -> Iterator[Path]:
        """Stream files without loading all into memory"""
        patterns = ['*.txt', '*.text', '*.input']
        
        for pattern in patterns:
            try:
                # Use iterdir() for memory efficiency
                for file in self.input_dir.glob(pattern):
                    # Skip processed files
                    if file.stem in self.processed_files:
                        continue
                    
                    # Skip if output already exists
                    output_file = self.output_dir / f"{file.stem}.csv"
                    if output_file.exists() and output_file.stat().st_size > 100:
                        self.processed_files.add(file.stem)
                        continue
                    
                    yield file
            except Exception as e:
                logger.error(f"Error discovering files: {e}")
    
    def _count_files(self) -> int:
        """Count files to process without loading into memory"""
        count = 0
        for _ in self._discover_files_streaming():
            count += 1
        return count
    
    def _init_instance_pool(self):
        """Initialize instance pool with memory-aware sizing"""
        if self.instance_pool is None:
            with self.pool_lock:
                if self.instance_pool is None:
                    # Calculate instances based on available memory
                    memory_gb = psutil.virtual_memory().available / (1024**3)
                    
                    # Conservative: 1 instance per 2GB RAM, capped by workers
                    max_instances = min(
                        max(int(memory_gb / 2), 2),
                        self.max_workers
                    )
                    
                    logger.info(f"Creating pool with {max_instances} instances")
                    self.instance_pool = MetaMapInstancePool(self.config)
    
    def _process_file(self, file: Path) -> Tuple[bool, float, Optional[str]]:
        """Process a single file with resource management"""
        instance_id = None
        start_time = time.time()
        
        try:
            # Get instance from pool
            instance_id, mm_instance = self.instance_pool.get_instance()
            
            # Process file
            processor = FileProcessor(
                self.config.get("metamap_binary_path"),
                str(self.output_dir),
                self.config.get("metamap_processing_options", ""),
                self.timeout,
                metamap_instance=mm_instance,
                worker_id=instance_id
            )
            
            output_file = processor.process_file(str(file))
            elapsed = time.time() - start_time
            
            return (True, elapsed, None)
            
        except Exception as e:
            elapsed = time.time() - start_time
            return (False, elapsed, str(e))
        finally:
            # Return instance to pool
            if instance_id is not None and self.instance_pool:
                self.instance_pool.release_instance(instance_id, mm_instance if 'mm_instance' in locals() else None)
    
    def run(self, progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """Run optimized batch processing"""
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
        
        # Count files first
        logger.info("Counting files to process...")
        total_files = self._count_files()
        self.stats["total"] = total_files
        
        if total_files == 0:
            return {
                "success": True,
                "total": 0,
                "successful": 0,
                "failed": 0,
                "duration": 0
            }
        
        logger.info(f"Found {total_files} files to process")
        
        # Initialize instance pool
        self._init_instance_pool()
        
        try:
            # Process files in streaming chunks
            chunk = []
            chunk_num = 0
            
            for file in self._discover_files_streaming():
                chunk.append(file)
                
                # Process chunk when it reaches the limit
                if len(chunk) >= self.chunk_size:
                    chunk_num += 1
                    self._process_chunk(chunk, chunk_num)
                    chunk = []
                    
                    # Force garbage collection between chunks
                    gc.collect()
                    
                    # Update progress
                    if self.progress_callback:
                        self.progress_callback(self.stats)
            
            # Process remaining files
            if chunk:
                chunk_num += 1
                self._process_chunk(chunk, chunk_num)
            
            # Calculate final stats
            duration = time.time() - self.stats["start_time"]
            
            return {
                "success": True,
                "total": self.stats["total"],
                "successful": self.stats["processed"],
                "failed": self.stats["failed"],
                "duration": duration,
                "throughput": self.stats["processed"] / duration if duration > 0 else 0
            }
            
        finally:
            # Cleanup
            if self.instance_pool:
                logger.info("Shutting down instance pool...")
                self.instance_pool.shutdown()
            
            # Save final state
            self._save_state()
    
    def _process_chunk(self, files: List[Path], chunk_num: int):
        """Process a chunk of files with progress tracking"""
        logger.info(f"Processing chunk {chunk_num} ({len(files)} files)")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_file = {
                executor.submit(self._process_file, file): file
                for file in files
            }
            
            # Process results as they complete
            for future in as_completed(future_to_file):
                file = future_to_file[future]
                
                try:
                    success, elapsed, error = future.result(timeout=self.timeout + 30)
                    
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
                
                # Update progress callback
                if self.progress_callback:
                    self.progress_callback(self.stats)
        
        # Save state after each chunk
        self._save_state()
        logger.info(f"Chunk {chunk_num} complete: {self.stats['processed']} total processed")
    
    def clear_state(self):
        """Clear processing state"""
        self.processed_files.clear()
        self.failed_files.clear()
        if self.state_file.exists():
            self.state_file.unlink()