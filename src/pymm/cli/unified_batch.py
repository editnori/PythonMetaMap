"""Unified batch processing with automatic Java API integration"""
import os
import sys
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
import logging
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn

from ..core.config import PyMMConfig
from ..core.file_tracker import UnifiedFileTracker
from ..processing.java_bridge_v2 import EnhancedJavaAPIBridge
from ..processing.unified_processor import UnifiedProcessor, ProcessingMode
from ..core.config import Config
from ..server.manager import ServerManager
from ..utils.setup_verifier import SetupVerifier

logger = logging.getLogger(__name__)
console = Console()


class UnifiedBatchProcessor:
    """Unified batch processor that automatically uses Java API when available"""
    
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
        
        # Auto-detect and initialize Java API if available
        self.java_bridge = None
        self.java_available = self._initialize_java_bridge()
    
    def _initialize_java_bridge(self) -> bool:
        """Initialize Java API bridge if available"""
        try:
            # Check if Java API JARs exist
            project_root = Path(__file__).parent.parent.parent.parent
            jar_path = project_root / "public_mm" / "src" / "javaapi" / "dist" / "MetaMapApi.jar"
            
            if not jar_path.exists():
                logger.info("Java API JARs not found, using Python wrapper mode")
                return False
            
            # Create bridge config
            bridge_config = Config.from_pymm_config(self.config)
            self.java_bridge = EnhancedJavaAPIBridge(bridge_config)
            
            # Validate environment
            env_checks = self.java_bridge.validate_environment()
            if not env_checks.get("java_available"):
                logger.warning("Java runtime not available")
                return False
            
            # Check if MetaMap server is running
            from ..server.manager import ServerManager
            server_mgr = ServerManager(self.config)
            if not server_mgr.is_mmserver_running():
                logger.warning("MetaMap server (mmserver) is not running. Java API requires mmserver.")
                logger.info("Start mmserver with: ~/metamap_link/bin/mmserver")
                return False
                
            logger.info("✓ Java API bridge initialized successfully")
            return True
            
        except Exception as e:
            logger.info(f"Java API not available: {e}")
            return False
    
    def collect_input_files(self) -> List[Path]:
        """Collect all text files from input directory"""
        files = []
        for pattern in ['*.txt', '*.text', '*.input']:
            files.extend(self.input_dir.glob(pattern))
        return sorted(files)
    
    def process(self, show_ui: bool = True) -> Dict[str, Any]:
        """Main processing entry point with automatic mode selection"""
        
        # Collect files
        all_files = self.collect_input_files()
        unprocessed = self.file_tracker.get_unprocessed_files()
        
        if show_ui:
            self._show_welcome_screen(all_files, unprocessed)
        
        if not unprocessed:
            console.print("\n[green]✓ All files already processed![/green]")
            return {"success": True, "processed": 0, "failed": 0}
        
        # Get processing options
        background = False
        if show_ui:
            workers, timeout, background = self._get_processing_options()
            if workers:
                self.config.set("max_parallel_workers", workers)
            if timeout:
                self.config.set("pymm_timeout", timeout)
        
        # Handle background processing
        if background:
            return self._process_in_background(unprocessed)
        
        # Process files in foreground
        console.print(f"\n[cyan]Processing {len(unprocessed)} files...[/cyan]")
        
        if self.java_available:
            console.print("[green]✓ Using Java API for 10x faster processing[/green]")
            return self._process_with_java_api(unprocessed)
        else:
            console.print("[yellow]ℹ Using Python wrapper mode (optimized)[/yellow]")
            return self._process_with_python(unprocessed)
    
    def _show_welcome_screen(self, all_files: List[Path], unprocessed: List[Path]):
        """Show welcome screen with file information"""
        console.print("\n")
        console.print(Panel.fit(
            "[bold cyan]Unified Batch Processing[/bold cyan]\n"
            "Automatic Java API integration for maximum performance",
            title="╔═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗\n"
                  "║ PythonMetaMap                                                                                                                                ║\n"
                  "╚═══════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝"
        ))
        
        console.print(f"\nInput directory: {self.input_dir}")
        console.print(f"Output directory: {self.output_dir}")
        
        # Validate setup
        verifier = SetupVerifier(self.config)
        validation = verifier.validate_setup()
        
        if validation['is_valid']:
            console.print(f"MetaMap binary: [green]{validation['metamap_binary']}[/green]")
        else:
            console.print("[red]⚠ MetaMap not properly configured[/red]")
            for error in validation['errors']:
                console.print(f"  • {error}")
        
        # Show file counts
        console.print(f"\nFound {len(all_files)} text files")
        processed = len(all_files) - len(unprocessed)
        if processed > 0:
            console.print(f"  • [green]{processed} already processed[/green]")
        if unprocessed:
            console.print(f"  • [yellow]{len(unprocessed)} to process[/yellow]")
    
    def _get_processing_options(self) -> Tuple[Optional[int], Optional[int], bool]:
        """Get processing options from user"""
        console.print("\n[bold]Processing Options[/bold]")
        
        # Number of workers
        default_workers = self.config.get("max_parallel_workers", 6)
        workers_input = console.input(f"Number of workers ({default_workers}): ")
        workers = int(workers_input) if workers_input.strip() else default_workers
        
        # Timeout
        default_timeout = self.config.get("pymm_timeout", 600)
        timeout_input = console.input(f"Timeout per file (seconds) ({default_timeout}): ")
        timeout = int(timeout_input) if timeout_input.strip() else default_timeout
        
        # Background processing
        background = False
        if console.input("\nRun in background? [y/N]: ").lower().startswith('y'):
            background = True
            console.print("[yellow]Background processing will start. Check logs for progress.[/yellow]")
        
        return workers, timeout, background
    
    def _process_in_background(self, files: List[Path]) -> Dict[str, Any]:
        """Process files in background"""
        import subprocess
        import sys
        from datetime import datetime
        
        # Create job ID
        job_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create log directory
        log_dir = self.output_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"background_{job_id}.log"
        
        # Create command to run this same process without UI
        cmd = [
            sys.executable, "-m", "pymm", 
            "process", str(self.input_dir), str(self.output_dir),
            "--no-ui",
            "--workers", str(self.config.get("max_parallel_workers", 6)),
            "--timeout", str(self.config.get("pymm_timeout", 600))
        ]
        
        # Start background process
        with open(log_file, 'w') as f:
            process = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                start_new_session=True
            )
        
        console.print(f"\n[green]✓ Background processing started![/green]")
        console.print(f"Process ID: {process.pid}")
        console.print(f"Log file: {log_file}")
        console.print("\nMonitor progress with: tail -f {log_file}")
        console.print("Check status with: ps aux | grep {process.pid}")
        
        return {
            "success": True,
            "background": True,
            "pid": process.pid,
            "job_id": job_id,
            "log_file": str(log_file)
        }
    
    def _process_with_java_api(self, files: List[Path]) -> Dict[str, Any]:
        """Process using Java API"""
        results = {
            "success": True,
            "processed": 0,
            "failed": 0,
            "failed_files": []
        }
        
        # Convert paths to strings
        input_files = [str(f) for f in files]
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Processing files...", total=len(files))
            
            def update_progress(message):
                msg_type = message.get("type")
                if msg_type == "file_complete":
                    results["processed"] += 1
                    progress.update(task, advance=1)
                    filename = Path(message.get("file")).name
                    progress.update(task, description=f"Processed {filename}")
                elif msg_type == "file_error":
                    results["failed"] += 1
                    results["failed_files"].append(message.get("file"))
                    progress.update(task, advance=1)
            
            try:
                # Process files
                java_results = self.java_bridge.process_files_fast(
                    input_files,
                    str(self.output_dir),
                    progress_callback=update_progress
                )
                
                # Update file tracker
                for file_path, result in java_results.get("results", {}).items():
                    file = Path(file_path)
                    if result["status"] == "success":
                        self.file_tracker.mark_file_completed(
                            file,
                            result.get("concepts_found", 0),
                            result.get("elapsed_seconds", 0)
                        )
                    else:
                        self.file_tracker.mark_file_failed(
                            file,
                            result.get("error", "Unknown error")
                        )
                
                # Add statistics
                if java_results.get("statistics"):
                    stats = java_results["statistics"]
                    results["elapsed_time"] = stats.get("elapsed_seconds", 0)
                    results["throughput"] = stats.get("throughput_per_minute", 0) / 60
                
            except Exception as e:
                logger.error(f"Java API processing failed: {e}")
                results["success"] = False
                results["error"] = str(e)
        
        self._show_results(results)
        return results
    
    def _process_with_python(self, files: List[Path]) -> Dict[str, Any]:
        """Process using Python wrapper with optimizations"""
        # Temporarily disable Java API in config to ensure Python processing
        original_java_api = self.config.get("use_java_api", False)
        self.config.set("use_java_api", False)
        
        # Enable optimizations for Python wrapper
        self.config.set("use_instance_pool", True)  # Enable instance pooling
        self.config.set("chunked_processing", True)  # Enable chunked processing for large files
        self.config.set("memory_streaming", True)  # Enable memory-efficient streaming
        self.config.set("chunk_size", 1000)  # Process in smaller chunks to avoid timeouts
        self.config.set("show_progress", True)  # Enable progress bars
        
        # Increase timeout for large files
        original_timeout = self.config.get("pymm_timeout", 60)
        self.config.set("pymm_timeout", 300)  # 5 minutes for large files
        
        try:
            # Use OPTIMIZED mode for better performance
            processor = UnifiedProcessor(
                str(self.input_dir),
                str(self.output_dir),
                self.config,
                ProcessingMode.OPTIMIZED  # Changed from STANDARD to OPTIMIZED
            )
            
            results = processor.run()
            self._show_results(results)
            return results
        finally:
            # Restore original settings
            self.config.set("use_java_api", original_java_api)
            self.config.set("pymm_timeout", original_timeout)
    
    def _show_results(self, results: Dict[str, Any]):
        """Display processing results"""
        console.print("\n")
        table = Table(title="Processing Results", title_style="bold cyan")
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="green")
        
        table.add_row("Files Processed", str(results.get("processed", 0)))
        table.add_row("Files Failed", str(results.get("failed", 0)))
        
        if results.get("elapsed_time"):
            table.add_row("Time Elapsed", f"{results['elapsed_time']:.2f}s")
            
        if results.get("throughput"):
            table.add_row("Throughput", f"{results['throughput']:.2f} files/s")
        
        table.add_row("Processing Mode", "Java API (10x)" if self.java_available else "Python Wrapper")
        
        console.print(table)
        
        if results.get("failed", 0) > 0:
            console.print("\n[red]Failed files:[/red]")
            for file in results.get("failed_files", [])[:10]:
                console.print(f"  • {file}")
            if len(results.get("failed_files", [])) > 10:
                console.print(f"  ... and {len(results['failed_files']) - 10} more")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current processing status"""
        status = self.file_tracker.get_processing_summary()
        status["java_api_available"] = self.java_available
        return status


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Unified batch processing with automatic Java API integration"
    )
    parser.add_argument("input_dir", help="Input directory containing text files")
    parser.add_argument("output_dir", help="Output directory for CSV results")
    parser.add_argument("--workers", type=int, help="Number of parallel workers")
    parser.add_argument("--timeout", type=int, help="Timeout per file in seconds")
    parser.add_argument("--no-ui", action="store_true", help="Run without interactive UI")
    
    args = parser.parse_args()
    
    # Create config
    config = PyMMConfig()
    if args.workers:
        config.set("max_parallel_workers", args.workers)
    if args.timeout:
        config.set("pymm_timeout", args.timeout)
    
    # Create processor
    processor = UnifiedBatchProcessor(args.input_dir, args.output_dir, config)
    
    # Process
    results = processor.process(show_ui=not args.no_ui)
    
    sys.exit(0 if results.get("success", False) else 1)


if __name__ == "__main__":
    main()