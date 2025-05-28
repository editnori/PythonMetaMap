"""Fast batch processing with scaled servers"""
import os
import time
import logging
import concurrent.futures
from pathlib import Path
from typing import List, Dict, Any, Optional
import multiprocessing as mp

from ..core.config import PyMMConfig
from ..core.state import StateManager
from ..server.scaled_manager import ScaledServerManager
from .worker import FileProcessor
from ..pymm import Metamap as PyMetaMap

class FastBatchRunner:
    """High-performance batch processor with scaled servers"""
    
    def __init__(self, input_dir: str, output_dir: str, 
                 config: Optional[PyMMConfig] = None):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.config = config or PyMMConfig()
        
        # Performance settings
        self.num_workers = min(
            self.config.get("max_parallel_workers", 4),
            mp.cpu_count()
        )
        self.num_server_instances = max(2, self.num_workers // 2)
        self.timeout = self.config.get("pymm_timeout", 120)
        
        # Components
        self.state_manager = StateManager(output_dir)
        self.scaled_servers = ScaledServerManager(self.config, self.num_server_instances)
        
        # Setup logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Setup comprehensive logging"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        log_dir = self.output_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # Main log file
        log_file = log_dir / f"batch_run_{int(time.time())}.log"
        
        # Configure root logger
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger("FastBatchRunner")
        self.logger.info(f"Logging to {log_file}")
        
    def start_servers(self) -> bool:
        """Start all server instances"""
        self.logger.info(f"Starting {self.num_server_instances} server instances...")
        
        results = self.scaled_servers.start_all_instances()
        
        success_count = sum(1 for v in results.values() if v)
        if success_count == 0:
            self.logger.error("Failed to start any server instances")
            return False
            
        if success_count < self.num_server_instances:
            self.logger.warning(f"Only {success_count}/{self.num_server_instances} instances started")
            
        # Log server status
        status = self.scaled_servers.get_status()
        for instance_id, inst_status in status.items():
            self.logger.info(
                f"Instance {instance_id}: "
                f"Tagger={inst_status['tagger']} (port {inst_status['ports']['tagger']}), "
                f"WSD={inst_status['wsd']} (port {inst_status['ports']['wsd']})"
            )
            
        return True
        
    def gather_files(self) -> List[Path]:
        """Gather input files"""
        files = sorted(self.input_dir.glob("*.txt"))
        self.logger.info(f"Found {len(files)} input files")
        return files
        
    def process_file_wrapper(self, args) -> tuple:
        """Wrapper for multiprocessing"""
        file_path, worker_id = args
        
        # Get server ports for this worker
        tagger_port, wsd_port = self.scaled_servers.get_instance_for_worker(worker_id)
        
        # Create processor with custom ports
        processor = FileProcessor(
            self.config.get("metamap_binary_path"),
            str(self.output_dir),
            self.config.get("metamap_processing_options", ""),
            self.timeout,
            tagger_port=tagger_port,
            wsd_port=wsd_port,
            worker_id=worker_id
        )
        
        # Process file
        return processor.process_file(str(file_path))
        
    def run(self) -> Dict[str, Any]:
        """Run fast batch processing"""
        start_time = time.time()
        
        # Start servers
        if not self.start_servers():
            return {"success": False, "error": "Failed to start servers"}
            
        # Gather files
        files = self.gather_files()
        if not files:
            return {"success": True, "total_files": 0, "processed": 0}
            
        # Process files in parallel
        self.logger.info(f"Processing {len(files)} files with {self.num_workers} workers")
        
        processed = 0
        failed = 0
        
        # Create work items (file, worker_id)
        work_items = [(file, i % self.num_workers) for i, file in enumerate(files)]
        
        # Use process pool for true parallelism
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            # Submit all files
            future_to_file = {
                executor.submit(self.process_file_wrapper, item): item[0]
                for item in work_items
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                file = future_to_file[future]
                
                try:
                    success, proc_time, error = future.result()
                    
                    if success:
                        processed += 1
                        self.state_manager.mark_completed(str(file))
                        self.logger.info(f"✓ {file.name} ({proc_time:.1f}s)")
                    else:
                        failed += 1
                        self.logger.error(f"✗ {file.name}: {error}")
                        
                except Exception as e:
                    failed += 1
                    self.logger.exception(f"Worker exception for {file.name}")
                    
                # Progress update
                total_done = processed + failed
                if total_done % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = total_done / elapsed
                    self.logger.info(
                        f"Progress: {total_done}/{len(files)} "
                        f"({rate:.1f} files/sec)"
                    )
                    
        # Cleanup
        self.scaled_servers.stop_all_instances()
        
        # Final summary
        elapsed = time.time() - start_time
        throughput = processed / elapsed if elapsed > 0 else 0
        
        summary = {
            "success": True,
            "total_files": len(files),
            "processed": processed,
            "failed": failed,
            "elapsed_time": elapsed,
            "throughput": throughput,
            "files_per_second": throughput
        }
        
        self.logger.info(
            f"\nBatch complete: {processed}/{len(files)} files in {elapsed:.1f}s "
            f"({throughput:.2f} files/sec)"
        )
        
        # Save summary to file
        summary_file = self.output_dir / "processing_summary.txt"
        with open(summary_file, 'w') as f:
            f.write("Processing Summary\n")
            f.write("=" * 50 + "\n")
            for key, value in summary.items():
                f.write(f"{key}: {value}\n")
                
        return summary
        
    @classmethod
    def quick_process(cls, input_dir: str, output_dir: str, 
                      workers: int = 4) -> Dict[str, Any]:
        """Quick entry point for fast processing"""
        config = PyMMConfig()
        config.set("max_parallel_workers", workers)
        
        runner = cls(input_dir, output_dir, config)
        return runner.run()