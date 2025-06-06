"""Batch processing for MetaMap with advanced features"""
import os
import time
import logging
import json
import signal
import subprocess
import shutil
import sys
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from datetime import datetime
import threading
from queue import Queue, Empty

from tqdm import tqdm
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.console import Console

from ..core.config import PyMMConfig
from ..core.state import StateManager
from ..core.exceptions import MetamapStuck, ServerConnectionError, ParseError
from ..server.manager import ServerManager
from ..server.health_check import HealthMonitor
from .pool_manager import MetaMapInstancePool
from .worker import FileProcessor
from .retry_manager import RetryManager

logger = logging.getLogger(__name__)
console = Console()


class BatchRunner:
    """Advanced batch processing with progress tracking and scalability"""
    
    def __init__(self, input_dir: str, output_dir: str, config: PyMMConfig):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.config = config
        
        # Create output directory structure
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir = self.output_dir / "logs"
        self.logs_dir.mkdir(exist_ok=True)
        
        # Setup logging to file
        self._setup_logging()
        
        # Processing configuration
        self.max_workers = config.get("max_parallel_workers", 4)
        self.timeout = config.get("pymm_timeout", 300)
        
        # Handle string boolean values
        use_pool = config.get("use_instance_pool", True)
        if isinstance(use_pool, str):
            self.use_instance_pool = use_pool.lower() in ('yes', 'true', '1')
        else:
            self.use_instance_pool = bool(use_pool)
            
        show_progress = config.get("progress_bar", True)
        if isinstance(show_progress, str):
            self.show_progress = show_progress.lower() in ('yes', 'true', '1')
        else:
            self.show_progress = bool(show_progress)
        
        # State management
        self.state_manager = StateManager(str(self.output_dir))
        self.retry_manager = RetryManager(config, self.state_manager)
        
        # Server management
        self.server_manager = ServerManager(config)
        self.health_monitor = HealthMonitor(config, self.server_manager)
        
        # Instance pool
        self.instance_pool = None
        if self.use_instance_pool:
            self.instance_pool = MetaMapInstancePool(
                config.get("metamap_binary_path"),
                max_instances=config.get("max_instances", 4)
            )
        
        # Progress tracking
        self.progress_queue = Queue()
        self.stats = {
            "total_files": 0,
            "processed": 0,
            "failed": 0,
            "start_time": None,
            "end_time": None
        }
    
    def _setup_logging(self):
        """Setup file logging for batch processing"""
        log_file = self.logs_dir / f"batch_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter(
            '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to root logger
        logging.getLogger().addHandler(file_handler)
        
        logger.info(f"Batch processing log: {log_file}")
    
    def _collect_input_files(self) -> List[Path]:
        """Collect all input files to process"""
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
    
    def _filter_pending_files(self, input_files: List[Path]) -> List[Path]:
        """Filter out already processed files"""
        pending = []
        
        for file in input_files:
            output_file = self.output_dir / f"{file.name}.csv"
            
            # Check if already completed
            if self.state_manager.is_completed(str(file)):
                logger.debug(f"Skipping completed file: {file}")
                continue
            
            # Check if output exists and is valid
            if output_file.exists() and output_file.stat().st_size > 100:
                # Verify it has proper end marker
                try:
                    with open(output_file, 'r') as f:
                        lines = f.readlines()
                        if lines and "META_BATCH_END" in lines[-1]:
                            self.state_manager.mark_completed(str(file))
                            logger.debug(f"Skipping file with valid output: {file}")
                            continue
                except:
                    pass
            
            pending.append(file)
        
        logger.info(f"Filtered to {len(pending)} pending files")
        return pending
    
    def _process_file_with_pool(self, file: Path) -> Tuple[bool, float, Optional[str]]:
        """Process file using instance pool"""
        instance_id = None
        mm_instance = None
        
        try:
            # Get instance from pool
            instance_id, mm_instance = self.instance_pool.get_instance()
            
            # Process file with pooled instance
            processor = FileProcessor(
                self.config.get("metamap_binary_path"),
                str(self.output_dir),
                self.config.get("metamap_processing_options", ""),
                self.timeout,
                metamap_instance=mm_instance,  # Pass the pooled instance
                state_manager=self.state_manager  # Pass state manager for concept tracking
            )
            
            # Process the file
            return processor.process_file(str(file))
            
        finally:
            # Return instance to pool
            if instance_id is not None:
                self.instance_pool.release_instance(instance_id)
    
    def _process_file_direct(self, file: Path) -> Tuple[bool, float, Optional[str]]:
        """Process file without instance pool"""
        processor = FileProcessor(
            self.config.get("metamap_binary_path"),
            str(self.output_dir),
            self.config.get("metamap_processing_options", ""),
            self.timeout,
            state_manager=self.state_manager  # Pass state manager for concept tracking
        )
        
        return processor.process_file(str(file))
    
    def _process_with_progress(self, files: List[Path]) -> Dict[str, Any]:
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
        
        # Create progress bar
        if self.show_progress:
            progress = Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeRemainingColumn(),
                console=console,
                refresh_per_second=2  # Reduce refresh rate to prevent flickering
            )
            
            with progress:
                task = progress.add_task(
                    f"Processing {len(files)} files...",
                    total=len(files)
                )
                
                # Process with thread pool
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # Submit all tasks
                    future_to_file = {}
                    for file in files:
                        if self.use_instance_pool:
                            future = executor.submit(self._process_file_with_pool, file)
                        else:
                            future = executor.submit(self._process_file_direct, file)
                        future_to_file[future] = file
                    
                    # Process results as they complete
                    completed = 0
                    last_update_time = time.time()
                    last_percentage = 0
                    
                    for future in as_completed(future_to_file):
                        file = future_to_file[future]
                        
                        try:
                            success, elapsed, error = future.result(timeout=self.timeout + 30)
                            
                            if success:
                                results["processed"] += 1
                                self.state_manager.mark_completed(str(file))
                                logger.info(f"Processed {file.name} in {elapsed:.2f}s")
                            else:
                                results["failed"] += 1
                                results["failed_files"].append(str(file))
                                self.state_manager.mark_failed(str(file), error or "Unknown error")
                                logger.error(f"Failed to process {file.name}: {error}")
                        
                        except Exception as e:
                            results["failed"] += 1
                            results["failed_files"].append(str(file))
                            self.state_manager.mark_failed(str(file), str(e))
                            logger.error(f"Exception processing {file.name}: {e}")
                        
                        # Update progress with rate limiting and smooth updates
                        completed += 1
                        current_time = time.time()
                        current_percentage = int((completed / len(files)) * 100)
                        
                        # Only update if enough time has passed AND percentage has changed
                        if (current_time - last_update_time >= 0.5 and 
                            current_percentage != last_percentage):
                            progress.update(task, completed=completed)
                            last_update_time = current_time
                            last_percentage = current_percentage
                    
                    # Final update to ensure 100%
                    progress.update(task, completed=len(files))
        
        else:
            # Process without progress bar
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_file = {}
                for file in files:
                    if self.use_instance_pool:
                        future = executor.submit(self._process_file_with_pool, file)
                    else:
                        future = executor.submit(self._process_file_direct, file)
                    future_to_file[future] = file
                
                for future in as_completed(future_to_file):
                    file = future_to_file[future]
                    
                    try:
                        success, elapsed, error = future.result(timeout=self.timeout + 30)
                        
                        if success:
                            results["processed"] += 1
                            self.state_manager.mark_completed(str(file))
                            logger.info(f"Processed {file.name} in {elapsed:.2f}s")
                        else:
                            results["failed"] += 1
                            results["failed_files"].append(str(file))
                            self.state_manager.mark_failed(str(file), error or "Unknown error")
                            logger.error(f"Failed to process {file.name}: {error}")
                    
                    except Exception as e:
                        results["failed"] += 1
                        results["failed_files"].append(str(file))
                        self.state_manager.mark_failed(str(file), str(e))
                        logger.error(f"Exception processing {file.name}: {e}")
        
        results["elapsed_time"] = time.time() - start_time
        results["throughput"] = results["processed"] / results["elapsed_time"] if results["elapsed_time"] > 0 else 0
        
        return results
    
    def run(self) -> Dict[str, Any]:
        """Run batch processing"""
        logger.info("Starting batch processing")
        
        # Ensure servers are running
        tagger_running = self.server_manager.is_tagger_server_running()
        wsd_running = self.server_manager.is_wsd_server_running()
        
        if not tagger_running:
            logger.warning("MetaMap servers not running, starting them...")
            if not self.server_manager.start_all():
                return {
                    "success": False,
                    "error": "Failed to start MetaMap servers"
                }
        
        # Start health monitoring
        # TODO: Fix async health monitoring
        # self.health_monitor.start_monitoring()
        
        try:
            # Collect and filter files
            input_files = self._collect_input_files()
            if not input_files:
                return {
                    "success": False,
                    "error": "No input files found"
                }
            
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
            
            # Update state
            self.state_manager.update_statistics(
                total_files=len(input_files),
                in_progress=len(pending_files)
            )
            
            # Process files
            results = self._process_with_progress(pending_files)
            
            # Handle retries if configured
            # TODO: Implement retry logic
            # if self.config.get("retry_max_attempts", 0) > 0 and results["failed_files"]:
            #     logger.info(f"Retrying {len(results['failed_files'])} failed files...")
            #     retry_results = self.retry_manager.retry_failed_files(results["failed_files"])
            #     
            #     # Update results
            #     results["processed"] += retry_results["recovered"]
            #     results["failed"] -= retry_results["recovered"]
            #     results["failed_files"] = retry_results["still_failed"]
            
            # Final statistics
            self.state_manager.update_statistics(
                completed=results["processed"],
                failed=results["failed"],
                in_progress=0
            )
            
            logger.info(f"Batch processing complete: {results['processed']} successful, "
                       f"{results['failed']} failed, elapsed time: {results['elapsed_time']:.2f}s")
            
            return results
            
        finally:
            # Cleanup
            # self.health_monitor.stop_monitoring()
            if self.instance_pool:
                logger.info("Shutting down MetaMap instance pool...")
                self.instance_pool.shutdown()
    
    @classmethod
    def resume(cls, output_dir: str, config: PyMMConfig) -> Dict[str, Any]:
        """Resume interrupted processing"""
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
        
        # Create runner and process
        runner = cls(str(input_dir), output_dir, config)
        return runner.run()
    
    def run_background(self) -> subprocess.Popen:
        """Run processing in background with nohup"""
        log_file = self.logs_dir / f"background_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Create command - use pymm directly if available, otherwise python -m pymm
        pymm_cmd = shutil.which('pymm')
        if pymm_cmd:
            cmd = [
                "nohup",
                pymm_cmd,
                "process",
                str(self.input_dir),
                str(self.output_dir),
                "--workers", str(self.max_workers),
                "--timeout", str(self.timeout)
            ]
        else:
            # Fallback to python -m pymm
            cmd = [
                "nohup",
                sys.executable,
                "-m", "pymm",
                "process",
                str(self.input_dir),
                str(self.output_dir),
                "--workers", str(self.max_workers),
                "--timeout", str(self.timeout)
            ]
        
        # Start background process
        with open(log_file, 'w') as log:
            process = subprocess.Popen(
                cmd,
                stdout=log,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid if os.name != 'nt' else None
            )
        
        logger.info(f"Started background processing, PID: {process.pid}, log: {log_file}")
        
        # Save PID for monitoring
        pid_file = self.output_dir / ".background_pid"
        pid_file.write_text(str(process.pid))
        
        return process
    
    def clear_failed_outputs(self) -> Dict[str, int]:
        """Clear failed output files and reset their state"""
        removed_files = 0
        reset_states = 0
        cleared_retries = 0
        
        # Get failed files from state
        failed_files = self.state_manager._state.get('failed_files', {})
        
        for file_path in list(failed_files.keys()):
            # Remove output file if exists
            output_file = self.output_dir / f"{Path(file_path).name}.csv"
            if output_file.exists():
                try:
                    output_file.unlink()
                    removed_files += 1
                except Exception as e:
                    logger.error(f"Failed to remove {output_file}: {e}")
            
            # Reset state
            self.state_manager.reset_file_state(file_path)
            reset_states += 1
        
        # Clear retry queue
        if hasattr(self, 'retry_manager'):
            cleared_retries = self.retry_manager.clear_retry_queue()
        
        logger.info(f"Cleared {removed_files} failed outputs, reset {reset_states} states")
        
        return {
            'removed_files': removed_files,
            'reset_states': reset_states,
            'cleared_retries': cleared_retries
        }
    
    def clear_all_outputs(self, confirm: bool = False) -> Dict[str, int]:
        """Clear all output files and state"""
        if not confirm:
            raise ValueError("Must confirm=True to clear all outputs")
        
        removed_csv_files = 0
        removed_state_files = 0
        
        # Remove all CSV files
        for csv_file in self.output_dir.glob("*.csv"):
            try:
                csv_file.unlink()
                removed_csv_files += 1
            except Exception as e:
                logger.error(f"Failed to remove {csv_file}: {e}")
        
        # Remove state files
        for state_file in self.output_dir.glob(".*json"):
            try:
                state_file.unlink()
                removed_state_files += 1
            except Exception as e:
                logger.error(f"Failed to remove {state_file}: {e}")
        
        # Clear state manager
        self.state_manager.reset()
        
        logger.info(f"Cleared {removed_csv_files} CSV files and {removed_state_files} state files")
        
        return {
            'removed_csv_files': removed_csv_files,
            'removed_state_files': removed_state_files,
            'total_removed': removed_csv_files + removed_state_files
        }