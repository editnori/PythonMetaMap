"""Chunked batch processing for handling large file sets efficiently"""
import os
import time
import logging
import json
import signal
import shutil
import sys
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import threading
from queue import Queue
import hashlib

from tqdm import tqdm
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.console import Console

from ..core.config import PyMMConfig
from ..core.state import StateManager
from ..server.manager import ServerManager
from .pool_manager import MetaMapInstancePool
from .worker import FileProcessor

logger = logging.getLogger(__name__)
console = Console()


class ChunkedBatchRunner:
    """Efficient batch processing with chunking for large file sets"""
    
    def __init__(self, input_dir: str, output_dir: str, config: PyMMConfig):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.config = config
        
        # Create output directory structure
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = self.output_dir / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        
        # Processing configuration
        self.max_workers = config.get("max_parallel_workers", 4)
        self.timeout = config.get("pymm_timeout", 300)
        self.chunk_size = config.get("chunk_size", 500)  # Process files in chunks
        
        # Use lightweight state tracking
        self.processed_files = set()
        self.failed_files = set()
        self._load_state()
        
        # Server management
        self.server_manager = ServerManager(config)
        
        # Instance pool with better management
        self.instance_pool = None
        self.pool_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            "total_files": 0,
            "processed": 0,
            "failed": 0,
            "start_time": None,
            "chunks_processed": 0
        }
    
    def _load_state(self):
        """Load processing state from file"""
        state_file = self.output_dir / ".processing_state.json"
        if state_file.exists():
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                self.processed_files = set(state.get('processed', []))
                self.failed_files = set(state.get('failed', []))
                logger.info(f"Loaded state: {len(self.processed_files)} processed, {len(self.failed_files)} failed")
            except Exception as e:
                logger.warning(f"Failed to load state: {e}, starting fresh")
    
    def _save_state(self):
        """Save processing state to file"""
        state_file = self.output_dir / ".processing_state.json"
        try:
            state = {
                'processed': list(self.processed_files),
                'failed': list(self.failed_files),
                'last_update': datetime.now().isoformat()
            }
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
    
    def _get_file_chunks(self) -> Iterator[List[Path]]:
        """Yield chunks of files to process"""
        # Use a generator to avoid loading all files at once
        chunk = []
        processed_count = 0
        
        for pattern in ['*.txt', '*.text', '*.input']:
            for file in self.input_dir.glob(pattern):
                # Skip already processed files
                file_key = file.stem
                if file_key in self.processed_files:
                    continue
                    
                # Check if output already exists
                output_file = self.output_dir / f"{file.stem}.csv"
                if output_file.exists() and output_file.stat().st_size > 100:
                    self.processed_files.add(file_key)
                    continue
                
                chunk.append(file)
                
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []
        
        # Yield remaining files
        if chunk:
            yield chunk
    
    def _init_instance_pool(self):
        """Initialize instance pool with proper resource management"""
        if self.instance_pool is None:
            with self.pool_lock:
                if self.instance_pool is None:
                    # Calculate optimal instance count based on resources
                    import psutil
                    cpu_count = os.cpu_count() or 4
                    memory_gb = psutil.virtual_memory().available / (1024**3)
                    
                    # Conservative: 1 instance per 4GB RAM, max of workers + 2
                    max_instances = min(
                        int(memory_gb / 4),
                        self.max_workers + 2,
                        cpu_count // 2
                    )
                    
                    logger.info(f"Creating instance pool with {max_instances} instances")
                    self.instance_pool = MetaMapInstancePool(
                        self.config.get("metamap_binary_path"),
                        max_instances=max_instances,
                        config=self.config
                    )
    
    def _process_chunk(self, files: List[Path]) -> Dict[str, Any]:
        """Process a chunk of files"""
        results = {
            "processed": 0,
            "failed": 0,
            "chunk_size": len(files)
        }
        
        # Initialize pool if needed
        self._init_instance_pool()
        
        # Process files with progress bar
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=console,
            transient=True
        ) as progress:
            
            task = progress.add_task(
                f"Processing chunk ({len(files)} files)...",
                total=len(files)
            )
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit tasks
                future_to_file = {}
                for file in files:
                    future = executor.submit(self._process_file_safe, file)
                    future_to_file[future] = file
                
                # Process results
                for future in as_completed(future_to_file):
                    file = future_to_file[future]
                    
                    try:
                        success, elapsed, error = future.result(timeout=self.timeout + 30)
                        
                        if success:
                            results["processed"] += 1
                            self.processed_files.add(file.stem)
                            self.stats["processed"] += 1
                            logger.info(f"Processed {file.name} in {elapsed:.2f}s")
                        else:
                            results["failed"] += 1
                            self.failed_files.add(file.stem)
                            self.stats["failed"] += 1
                            logger.error(f"Failed {file.name}: {error}")
                    
                    except Exception as e:
                        results["failed"] += 1
                        self.failed_files.add(file.stem)
                        self.stats["failed"] += 1
                        logger.error(f"Exception processing {file.name}: {e}")
                    
                    progress.advance(task)
        
        # Save state after each chunk
        self._save_state()
        
        return results
    
    def _process_file_safe(self, file: Path) -> Tuple[bool, float, Optional[str]]:
        """Process file with error handling and resource management"""
        instance_id = None
        
        try:
            # Get instance from pool
            instance_id, mm_instance = self.instance_pool.get_instance()
            
            # Create processor with pooled instance
            processor = FileProcessor(
                self.config.get("metamap_binary_path"),
                str(self.output_dir),
                self.config.get("metamap_processing_options", ""),
                self.timeout,
                metamap_instance=mm_instance,
                worker_id=instance_id
            )
            
            # Process file
            return processor.process_file(str(file))
            
        except Exception as e:
            logger.error(f"Error processing {file}: {e}")
            return False, 0, str(e)
            
        finally:
            # Always return instance to pool
            if instance_id is not None and self.instance_pool:
                self.instance_pool.release_instance(instance_id)
    
    def run(self) -> Dict[str, Any]:
        """Run chunked batch processing"""
        logger.info("Starting chunked batch processing")
        
        # Ensure servers are running
        if not self.server_manager.is_tagger_server_running():
            logger.info("Starting MetaMap servers...")
            if not self.server_manager.start_all():
                return {
                    "success": False,
                    "error": "Failed to start MetaMap servers"
                }
        
        self.stats["start_time"] = time.time()
        
        try:
            # Process files in chunks
            chunk_num = 0
            total_processed = len(self.processed_files)
            
            for chunk in self._get_file_chunks():
                chunk_num += 1
                logger.info(f"Processing chunk {chunk_num} with {len(chunk)} files")
                
                chunk_results = self._process_chunk(chunk)
                self.stats["chunks_processed"] += 1
                
                total_processed += chunk_results["processed"]
                
                # Log progress
                logger.info(
                    f"Chunk {chunk_num} complete: "
                    f"{chunk_results['processed']} processed, "
                    f"{chunk_results['failed']} failed. "
                    f"Total progress: {total_processed} files"
                )
                
                # Check if we should continue
                if chunk_results["failed"] > chunk_results["processed"] * 0.5:
                    logger.warning("High failure rate detected, stopping")
                    break
            
            # Final statistics
            elapsed = time.time() - self.stats["start_time"]
            
            return {
                "success": True,
                "total_files": total_processed + len(self.failed_files),
                "processed": total_processed,
                "failed": len(self.failed_files),
                "elapsed_time": elapsed,
                "throughput": total_processed / elapsed if elapsed > 0 else 0,
                "chunks_processed": self.stats["chunks_processed"]
            }
            
        finally:
            # Cleanup
            if self.instance_pool:
                logger.info("Shutting down instance pool...")
                self.instance_pool.shutdown()
            
            # Final state save
            self._save_state()
    
    def clear_state(self):
        """Clear processing state to start fresh"""
        self.processed_files.clear()
        self.failed_files.clear()
        self._save_state()
        
        # Also clear state file
        state_file = self.output_dir / ".processing_state.json"
        if state_file.exists():
            state_file.unlink() 