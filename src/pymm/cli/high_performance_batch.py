"""High-performance batch processing with aggressive optimization"""
import os
import sys
import psutil
import multiprocessing as mp
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
import logging
from datetime import datetime
import subprocess
import signal
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import (
    Progress, SpinnerColumn, TextColumn, BarColumn, 
    TimeRemainingColumn, MofNCompleteColumn, TimeElapsedColumn,
    TaskProgressColumn
)
from rich.live import Live
from rich.layout import Layout

from ..core.config import PyMMConfig
from ..core.file_tracker import UnifiedFileTracker
from ..processing.unified_processor import UnifiedProcessor, ProcessingMode
from ..server.manager import ServerManager
from ..utils.setup_verifier import SetupVerifier

logger = logging.getLogger(__name__)
console = Console()


class HighPerformanceBatchProcessor:
    """Ultra-fast batch processor with aggressive resource utilization"""
    
    def __init__(self, input_dir: str, output_dir: str, config: Optional[PyMMConfig] = None):
        self.input_dir = Path(input_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        self.config = config or PyMMConfig()
        
        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.file_tracker = UnifiedFileTracker(
            self.config, 
            str(self.input_dir), 
            str(self.output_dir)
        )
        self.server_manager = ServerManager(self.config)
        
        # System resources
        self.cpu_count = psutil.cpu_count(logical=True)
        self.memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # Calculate optimal settings
        self._calculate_optimal_settings()
    
    def _calculate_optimal_settings(self):
        """Calculate optimal settings based on system resources"""
        # Use 80% of CPU cores
        self.optimal_workers = max(1, int(self.cpu_count * 0.8))
        
        # Use 80% of available memory
        self.max_memory_gb = self.memory_gb * 0.8
        
        # Calculate instances per worker (more instances = more parallel MetaMap processes)
        # Each MetaMap instance uses ~500MB
        metamap_memory_per_instance = 0.5  # GB
        total_instances = int(self.max_memory_gb / metamap_memory_per_instance)
        self.instances_per_worker = max(1, total_instances // self.optimal_workers)
        
        # Adjust chunk size based on available memory
        # Smaller chunks = more parallelism but more overhead
        if self.memory_gb > 16:
            self.chunk_size = 500  # Very small chunks for high parallelism
        elif self.memory_gb > 8:
            self.chunk_size = 1000
        else:
            self.chunk_size = 2000
    
    def show_system_info(self):
        """Display system information and optimization settings"""
        console.print("\n")
        console.print(Panel.fit(
            f"[bold cyan]High-Performance Batch Processing[/bold cyan]\n"
            f"[yellow]System Resources:[/yellow]\n"
            f"  • CPU Cores: {self.cpu_count} (using {self.optimal_workers} workers)\n"
            f"  • Memory: {self.memory_gb:.1f}GB (using {self.max_memory_gb:.1f}GB)\n"
            f"  • MetaMap Instances: {self.optimal_workers * self.instances_per_worker} total\n"
            f"  • Chunk Size: {self.chunk_size} chars\n"
            f"\n[green]Optimizations Enabled:[/green]\n"
            f"  • Multi-worker parallel processing\n"
            f"  • Instance pooling with {self.instances_per_worker} instances/worker\n"
            f"  • Memory-efficient streaming\n"
            f"  • Aggressive CPU utilization (80%)\n"
            f"  • Smart chunking for large files",
            title="⚡ Performance Mode ⚡"
        ))
    
    def collect_input_files(self) -> List[Path]:
        """Collect all text files from input directory"""
        files = []
        for pattern in ['*.txt', '*.text', '*.input']:
            files.extend(self.input_dir.glob(pattern))
        return sorted(files)
    
    def process(self, background: bool = False) -> Dict[str, Any]:
        """Main processing entry point with performance options"""
        
        # Collect files
        all_files = self.collect_input_files()
        unprocessed = self.file_tracker.get_unprocessed_files()
        
        if not background:
            self.show_system_info()
            self._show_file_summary(all_files, unprocessed)
        
        if not unprocessed:
            console.print("\n[green]✓ All files already processed![/green]")
            return {"success": True, "processed": 0, "failed": 0}
        
        # Get user confirmation for settings
        if not background:
            if not self._confirm_settings(len(unprocessed)):
                return {"success": False, "cancelled": True}
        
        # Configure for maximum performance
        self._configure_high_performance()
        
        # Process based on mode
        if background:
            return self._process_background_nohup(unprocessed)
        else:
            return self._process_foreground_with_progress(unprocessed)
    
    def _show_file_summary(self, all_files: List[Path], unprocessed: List[Path]):
        """Show file processing summary"""
        console.print(f"\nInput directory: {self.input_dir}")
        console.print(f"Output directory: {self.output_dir}")
        
        console.print(f"\nFound {len(all_files)} text files")
        processed = len(all_files) - len(unprocessed)
        if processed > 0:
            console.print(f"  • [green]{processed} already processed[/green]")
        if unprocessed:
            console.print(f"  • [yellow]{len(unprocessed)} to process[/yellow]")
            
            # Estimate time
            files_per_second = self.optimal_workers * 2  # Conservative estimate
            estimated_minutes = len(unprocessed) / files_per_second / 60
            console.print(f"\n[cyan]Estimated time: {estimated_minutes:.1f} minutes[/cyan]")
    
    def _confirm_settings(self, file_count: int) -> bool:
        """Confirm processing settings with user"""
        console.print(f"\n[bold]Ready to process {file_count} files[/bold]")
        console.print(f"Workers: {self.optimal_workers}")
        console.print(f"Memory allocation: {self.max_memory_gb:.1f}GB")
        console.print(f"Total MetaMap instances: {self.optimal_workers * self.instances_per_worker}")
        
        response = console.input("\nProceed with these settings? [Y/n]: ")
        return not response.lower().startswith('n')
    
    def _configure_high_performance(self):
        """Configure system for maximum performance"""
        # Set aggressive configuration
        self.config.set("max_parallel_workers", self.optimal_workers)
        self.config.set("metamap_instances_per_worker", self.instances_per_worker)
        self.config.set("chunk_size", self.chunk_size)
        self.config.set("use_instance_pool", True)
        self.config.set("chunked_processing", False)  # Disable chunking - it's slower!
        self.config.set("memory_streaming", True)
        self.config.set("pymm_timeout", 120)  # 2 minutes max per chunk
        self.config.set("batch_size", 50)  # Process files in batches
        
        # Enable all optimizations
        self.config.set("enable_retry", True)
        self.config.set("max_retries", 2)
        self.config.set("parallel_chunks", True)  # Process chunks in parallel too
        
        # Set process priority
        try:
            os.nice(-5)  # Increase process priority (requires permissions)
        except:
            pass
    
    def _process_foreground_with_progress(self, files: List[Path]) -> Dict[str, Any]:
        """Process files in foreground with rich progress display"""
        console.print(f"\n[cyan]Starting high-performance processing...[/cyan]")
        
        # Create progress display
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            refresh_per_second=2
        ) as progress:
            
            # Main task
            main_task = progress.add_task(
                "[cyan]Processing files", 
                total=len(files)
            )
            
            # Worker tasks
            worker_tasks = []
            for i in range(self.optimal_workers):
                task = progress.add_task(
                    f"[yellow]Worker {i+1}", 
                    total=100,
                    visible=True
                )
                worker_tasks.append(task)
            
            # Stats task
            stats_task = progress.add_task(
                "[green]Performance", 
                total=None
            )
            
            # Create processor with ULTRA mode for maximum speed
            processor = UnifiedProcessor(
                str(self.input_dir),
                str(self.output_dir),
                self.config,
                ProcessingMode.ULTRA
            )
            
            # Custom progress callback
            processed_count = 0
            start_time = datetime.now()
            
            def update_progress(status):
                nonlocal processed_count
                if status.get("type") == "file_complete":
                    processed_count += 1
                    progress.update(main_task, advance=1)
                    
                    # Update performance stats
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = processed_count / elapsed if elapsed > 0 else 0
                    progress.update(
                        stats_task,
                        description=f"[green]Performance: {rate:.1f} files/sec"
                    )
                    
                elif status.get("type") == "worker_status":
                    worker_id = status.get("worker_id", 0)
                    if worker_id < len(worker_tasks):
                        progress.update(
                            worker_tasks[worker_id],
                            description=f"[yellow]Worker {worker_id+1}: {status.get('file', 'idle')}"
                        )
            
            # Inject progress callback
            processor.progress_callback = update_progress
            
            # Run processing
            results = processor.run()
            
            # Show final stats
            self._show_performance_results(results, processed_count, start_time)
            
            return results
    
    def _process_background_nohup(self, files: List[Path]) -> Dict[str, Any]:
        """Process files in background with nohup"""
        console.print(f"\n[yellow]Starting background processing with nohup...[/yellow]")
        
        # Create job ID and log directory
        job_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        log_dir = self.output_dir / "logs"
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
        except FileExistsError:
            # Handle case where logs might be a file
            if log_dir.exists() and not log_dir.is_dir():
                log_dir.unlink()
                log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"background_{job_id}.log"
        pid_file = log_dir / f"background_{job_id}.pid"
        
        # Create script for nohup execution
        script_path = log_dir / f"run_{job_id}.sh"
        script_content = f"""#!/bin/bash
# High-performance batch processing script
# Job ID: {job_id}

# Set environment
export PYTHONUNBUFFERED=1
export OMP_NUM_THREADS={self.optimal_workers}

# Log system info
echo "Starting job {job_id} at $(date)"
echo "Workers: {self.optimal_workers}"
echo "Memory: {self.max_memory_gb:.1f}GB"
echo "Files to process: {len(files)}"
echo ""

# Run Python with optimized settings
{sys.executable} -u -m pymm.cli.high_performance_batch \\
    "{self.input_dir}" \\
    "{self.output_dir}" \\
    --workers {self.optimal_workers} \\
    --no-confirm \\
    --ultra-mode \\
    2>&1 | tee -a "{log_file}"

echo ""
echo "Job completed at $(date)"
"""
        
        with open(script_path, 'w') as f:
            f.write(script_content)
        os.chmod(script_path, 0o755)
        
        # Start with nohup
        cmd = f"nohup bash {script_path} > {log_file} 2>&1 & echo $! > {pid_file}"
        subprocess.run(cmd, shell=True)
        
        # Read PID
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
        
        # Show instructions
        console.print(f"\n[green]✓ Background processing started![/green]")
        console.print(f"Job ID: {job_id}")
        console.print(f"Process ID: {pid}")
        console.print(f"Log file: {log_file}")
        console.print(f"\n[bold]Monitor progress:[/bold]")
        console.print(f"  • tail -f {log_file}")
        console.print(f"  • ps -p {pid}")
        console.print(f"  • kill {pid}  # To stop")
        
        # Save job info
        job_info = {
            "job_id": job_id,
            "pid": pid,
            "start_time": datetime.now().isoformat(),
            "input_dir": str(self.input_dir),
            "output_dir": str(self.output_dir),
            "file_count": len(files),
            "workers": self.optimal_workers,
            "log_file": str(log_file)
        }
        
        job_info_file = log_dir / f"job_{job_id}.json"
        import json
        with open(job_info_file, 'w') as f:
            json.dump(job_info, f, indent=2)
        
        return {
            "success": True,
            "background": True,
            "job_info": job_info
        }
    
    def _show_performance_results(self, results: Dict[str, Any], processed_count: int, start_time: datetime):
        """Show detailed performance results"""
        elapsed = (datetime.now() - start_time).total_seconds()
        rate = processed_count / elapsed if elapsed > 0 else 0
        
        console.print("\n")
        table = Table(title="🚀 Performance Results", title_style="bold cyan")
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="green")
        
        table.add_row("Files Processed", str(results.get("processed", processed_count)))
        table.add_row("Files Failed", str(results.get("failed", 0)))
        table.add_row("Time Elapsed", f"{elapsed:.1f}s")
        table.add_row("Processing Rate", f"{rate:.2f} files/sec")
        table.add_row("Projected for 30k files", f"{30000/rate/3600:.1f} hours" if rate > 0 else "N/A")
        table.add_row("Workers Used", str(self.optimal_workers))
        table.add_row("Total Instances", str(self.optimal_workers * self.instances_per_worker))
        
        console.print(table)
        
        if results.get("failed", 0) > 0:
            console.print("\n[red]Failed files:[/red]")
            for file in results.get("failed_files", [])[:10]:
                console.print(f"  • {file}")
            if len(results.get("failed_files", [])) > 10:
                console.print(f"  ... and {len(results['failed_files']) - 10} more")


def main():
    """CLI entry point for high-performance processing"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="High-performance batch processing with aggressive optimization"
    )
    parser.add_argument("input_dir", help="Input directory containing text files")
    parser.add_argument("output_dir", help="Output directory for CSV results")
    parser.add_argument("--workers", type=int, help="Override number of workers")
    parser.add_argument("--background", action="store_true", help="Run in background with nohup")
    parser.add_argument("--no-confirm", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--ultra-mode", action="store_true", help="Enable ultra performance mode")
    
    args = parser.parse_args()
    
    # Create config
    config = PyMMConfig()
    if args.workers:
        config.set("override_workers", args.workers)
    
    # Create processor
    processor = HighPerformanceBatchProcessor(args.input_dir, args.output_dir, config)
    
    # Override workers if specified
    if args.workers:
        processor.optimal_workers = args.workers
    
    # Process
    if args.no_confirm:
        processor._configure_high_performance()
        if args.ultra_mode:
            config.set("processing_mode", "ULTRA")
        
        # Run directly without UI
        processor_impl = UnifiedProcessor(
            args.input_dir,
            args.output_dir,
            config,
            ProcessingMode.ULTRA if args.ultra_mode else ProcessingMode.OPTIMIZED
        )
        results = processor_impl.run()
        
        # Print summary
        print(f"\nProcessing complete:")
        print(f"  Processed: {results.get('processed', 0)}")
        print(f"  Failed: {results.get('failed', 0)}")
        print(f"  Time: {results.get('elapsed_time', 0):.1f}s")
        
        sys.exit(0 if results.get("success", False) else 1)
    else:
        # Run with UI
        results = processor.process(background=args.background)
        sys.exit(0 if results.get("success", False) else 1)


if __name__ == "__main__":
    main()