"""Enhanced batch runner with integrated real-time monitoring"""
import os
import time
import logging
from pathlib import Path
from typing import List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

from rich.console import Console
from rich.prompt import Confirm

from ..core.config import PyMMConfig
from ..server.manager import ServerManager
from ..monitoring.unified_monitor import UnifiedMonitor
from .pool_manager import MetaMapInstancePool
from .worker import FileProcessor

console = Console()
logger = logging.getLogger(__name__)


class MonitoredBatchRunner:
    """Batch runner with integrated real-time monitoring"""
    
    def __init__(self, input_dir: str, output_dir: str, config: PyMMConfig,
                 monitor: Optional[UnifiedMonitor] = None):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.config = config
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize or create monitor
        self.monitor = monitor or UnifiedMonitor([self.output_dir], config=config)
        self.owns_monitor = monitor is None  # Track if we created the monitor
        
        # Processing configuration
        self.max_workers = config.get("max_parallel_workers", 4)
        self.timeout = config.get("pymm_timeout", 300)
        
        # Server and pool management
        self.server_manager = ServerManager(config)
        self.instance_pool = None
        
        # Batch tracking
        self.batch_id = None
        self.processed_count = 0
        self.failed_count = 0
        
    def run(self, file_pattern: str = "*.txt", resume: bool = True) -> dict:
        """Run batch processing with monitoring"""
        start_time = time.time()
        
        # Start monitor if we own it
        if self.owns_monitor:
            self.monitor.start()
        
        try:
            # Discover files
            files = self._discover_files(file_pattern)
            if not files:
                self.monitor.log("WARNING", "BatchRunner", "No files found to process")
                return {"processed": 0, "failed": 0, "duration": 0}
            
            # Create batch
            self.batch_id = f"batch_{uuid.uuid4().hex[:8]}"
            self.monitor.create_batch(self.batch_id, len(files))
            self.monitor.log("INFO", "BatchRunner", f"Starting batch {self.batch_id} with {len(files)} files")
            
            # Ensure server is running
            if not self.server_manager.is_running():
                self.monitor.log("INFO", "BatchRunner", "Starting MetaMap server...")
                self.server_manager.start()
                time.sleep(5)  # Wait for server startup
            
            # Initialize instance pool
            self.instance_pool = MetaMapInstancePool(
                server_manager=self.server_manager,
                max_instances=self.max_workers,
                timeout=self.timeout
            )
            
            # Process files
            self._process_files_parallel(files)
            
            # Complete batch
            self.monitor.statistics_dashboard.complete_batch(self.batch_id)
            
            duration = time.time() - start_time
            self.monitor.log("INFO", "BatchRunner", 
                           f"Batch {self.batch_id} completed: {self.processed_count} processed, "
                           f"{self.failed_count} failed in {duration:.1f}s")
            
            return {
                "processed": self.processed_count,
                "failed": self.failed_count,
                "duration": duration
            }
            
        finally:
            # Cleanup
            if self.instance_pool:
                self.instance_pool.cleanup()
            
            # Stop monitor if we own it
            if self.owns_monitor:
                self.monitor.stop()
    
    def _discover_files(self, pattern: str) -> List[Path]:
        """Discover files to process"""
        files = []
        
        self.monitor.log("INFO", "BatchRunner", f"Scanning {self.input_dir} for {pattern}")
        
        for file_path in self.input_dir.glob(pattern):
            if file_path.is_file():
                files.append(file_path)
        
        self.monitor.log("INFO", "BatchRunner", f"Found {len(files)} files")
        return files
    
    def _process_files_parallel(self, files: List[Path]):
        """Process files in parallel with monitoring"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all files
            future_to_file = {
                executor.submit(self._process_single_file, file_path): file_path
                for file_path in files
            }
            
            # Process completions
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                
                try:
                    result = future.result()
                    if result['success']:
                        self.processed_count += 1
                    else:
                        self.failed_count += 1
                except Exception as e:
                    self.failed_count += 1
                    self.monitor.fail_file(
                        self.batch_id,
                        str(file_path),
                        f"Unexpected error: {str(e)}"
                    )
    
    def _process_single_file(self, file_path: Path) -> dict:
        """Process a single file with detailed monitoring"""
        start_time = time.time()
        file_size = file_path.stat().st_size
        
        # Start file processing
        self.monitor.progress_tracker.start_file(
            str(file_path),
            file_size,
            self.batch_id
        )
        
        try:
            # Update stage: Reading
            self.monitor.update_file_progress(
                self.batch_id,
                str(file_path),
                "reading",
                10
            )
            
            # Read file content
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if not content.strip():
                raise ValueError("Empty file")
            
            # Update stage: Acquiring instance
            self.monitor.update_file_progress(
                self.batch_id,
                str(file_path),
                "acquiring instance",
                20
            )
            
            # Get MetaMap instance
            instance = self.instance_pool.acquire()
            
            try:
                # Update stage: Processing
                self.monitor.update_file_progress(
                    self.batch_id,
                    str(file_path),
                    "processing",
                    30
                )
                
                # Create processor
                processor = FileProcessor(
                    instance_id=instance['id'],
                    host=instance['host'],
                    port=instance['port'],
                    timeout=self.timeout
                )
                
                # Process with progress callback
                def progress_callback(stage: str, progress: float):
                    self.monitor.update_file_progress(
                        self.batch_id,
                        str(file_path),
                        f"metamap: {stage}",
                        30 + (progress * 0.5)  # 30-80% for MetaMap processing
                    )
                
                result = processor.process_text(
                    text=content,
                    doc_id=file_path.stem,
                    progress_callback=progress_callback
                )
                
                # Update stage: Writing
                self.monitor.update_file_progress(
                    self.batch_id,
                    str(file_path),
                    "writing results",
                    85
                )
                
                # Write results
                output_file = self.output_dir / f"{file_path.stem}_concepts.csv"
                processor.write_csv(result, output_file)
                
                # Extract statistics
                concepts_count = len(result.get('concepts', []))
                
                # Update concepts in statistics
                for concept in result.get('concepts', []):
                    self.monitor.statistics_dashboard.update_concept(
                        concept.get('preferred_name', ''),
                        concept.get('semantic_types', [''])[0] if concept.get('semantic_types') else ''
                    )
                
                # Complete file
                self.monitor.complete_file(
                    self.batch_id,
                    str(file_path),
                    concepts_count
                )
                
                processing_time = time.time() - start_time
                self.monitor.log("INFO", "FileProcessor", 
                               f"Processed {file_path.name} in {processing_time:.2f}s "
                               f"({concepts_count} concepts)")
                
                return {
                    'success': True,
                    'concepts': concepts_count,
                    'time': processing_time,
                    'output': str(output_file)
                }
                
            finally:
                # Release instance
                self.instance_pool.release(instance['id'])
                
        except Exception as e:
            error_msg = str(e)
            self.monitor.fail_file(
                self.batch_id,
                str(file_path),
                error_msg
            )
            
            self.monitor.log("ERROR", "FileProcessor", 
                           f"Failed to process {file_path.name}: {error_msg}")
            
            return {
                'success': False,
                'error': error_msg,
                'time': time.time() - start_time
            }


class InteractiveMonitoredRunner:
    """Interactive batch runner with live monitoring display"""
    
    def __init__(self, config: PyMMConfig):
        self.config = config
        output_dir = config.get('default_output_dir', './output_csvs')
        self.monitor = UnifiedMonitor([Path(output_dir)], config=config)
        
    def run_interactive(self, input_dir: str, output_dir: str, file_pattern: str = "*.txt"):
        """Run batch processing with interactive monitoring"""
        # Create runner with shared monitor
        runner = MonitoredBatchRunner(
            input_dir=input_dir,
            output_dir=output_dir,
            config=self.config,
            monitor=self.monitor
        )
        
        # Start monitor
        self.monitor.start()
        
        try:
            # Run processing in background thread
            import threading
            
            result = {}
            def run_processing():
                nonlocal result
                result = runner.run(file_pattern=file_pattern)
            
            processing_thread = threading.Thread(target=run_processing)
            processing_thread.start()
            
            # Wait for completion (monitor is displaying)
            processing_thread.join()
            
            # Show final results
            console.print("\n[bold green]Processing Complete![/bold green]")
            console.print(f"Processed: {result.get('processed', 0)} files")
            console.print(f"Failed: {result.get('failed', 0)} files")
            console.print(f"Duration: {result.get('duration', 0):.1f} seconds")
            
            # Ask if user wants to export statistics
            if Confirm.ask("\nExport statistics?"):
                stats_file = Path("./statistics.json")
                self.monitor.statistics_dashboard.export_statistics(stats_file)
                console.print(f"[green]Statistics exported to {stats_file}[/green]")
                
        finally:
            # Stop monitor
            self.monitor.stop()