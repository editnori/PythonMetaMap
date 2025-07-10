"""Smart batch runner with unified file tracking"""
import os
import time
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich import box

from ..core.config import PyMMConfig
from ..core.file_tracker import UnifiedFileTracker
from .validated_batch_runner import ValidatedBatchRunner, ValidationResult

console = Console()


class SmartBatchRunner(ValidatedBatchRunner):
    """Smart batch runner with file tracking and intelligent processing"""
    
    def __init__(self, config: PyMMConfig = None):
        # Use unified directories from config
        config = config or PyMMConfig()
        self.tracker = UnifiedFileTracker(config)
        
        # Initialize with unified directories
        super().__init__(
            str(self.tracker.input_dir),
            str(self.tracker.output_dir),
            config
        )
        
        # Ensure server_manager is initialized
        from ..server.manager import ServerManager
        if not hasattr(self, 'server_manager'):
            self.server_manager = ServerManager(config)
        
    def show_processing_options(self) -> Tuple[List[Path], str]:
        """Show interactive processing options to user"""
        # First check if this is a new setup
        if not self.tracker.manifest_path.exists():
            console.print("\n[yellow]First time setup detected.[/yellow]")
            console.print(f"Creating unified tracking structure in: {self.tracker.base_dir}")
            console.print("\nPlease place your text files in:")
            console.print(f"  [cyan]{self.tracker.input_dir}[/cyan]\n")
            
            # Check if there are files in the input directory
            input_files = list(self.tracker.input_dir.glob("*.txt"))
            if not input_files:
                console.print("[yellow]No input files found. Please add text files to the input directory.[/yellow]")
                return [], "No input files found"
                
        # Get current status
        summary = self.tracker.get_processing_summary()
        
        # Display current status
        console.print("\n[bold cyan]File Processing Status[/bold cyan]\n")
        
        status_table = Table(box=box.ROUNDED)
        status_table.add_column("Status", style="cyan")
        status_table.add_column("Count", justify="right")
        
        status_table.add_row("Total Files", str(summary['total_files']))
        status_table.add_row("Processed", f"[green]{summary['processed']}[/green]")
        status_table.add_row("Failed", f"[red]{summary['failed']}[/red]")
        status_table.add_row("In Progress", f"[yellow]{summary['in_progress']}[/yellow]")
        status_table.add_row("Unprocessed", f"[blue]{summary['unprocessed']}[/blue]")
        status_table.add_row("", "")
        status_table.add_row("Total Concepts", f"[magenta]{summary['total_concepts']:,}[/magenta]")
        
        console.print(status_table)
        console.print(f"\n[dim]Last updated: {summary['last_updated']}[/dim]")
        
        # Get file lists
        unprocessed = self.tracker.get_unprocessed_files()
        failed = [path for path, _ in self.tracker.get_failed_files()]
        
        if not unprocessed and not failed:
            console.print("\n[green]All files have been processed successfully![/green]")
            
            # Ask if they want to reprocess with change detection
            if Confirm.ask("\nCheck for modified files?", default=False):
                unprocessed = self.tracker.get_unprocessed_files(rescan=True)
                if unprocessed:
                    console.print(f"[yellow]Found {len(unprocessed)} modified files[/yellow]")
            else:
                return [], "No files to process"
                
        # Show processing options
        console.print("\n[bold]Processing Options:[/bold]")
        console.print("1. Process all unprocessed files")
        console.print("2. Process specific number of files")
        console.print("3. Retry failed files only")
        console.print("4. Process custom selection")
        console.print("5. Show detailed file list")
        console.print("0. Cancel")
        
        choice = Prompt.ask("\nSelect option", choices=["0", "1", "2", "3", "4", "5"], default="1")
        
        if choice == "0":
            return [], "Cancelled"
            
        elif choice == "1":
            # Process all unprocessed
            files = unprocessed
            if not files and failed:
                if Confirm.ask("\nNo new files. Retry failed files?", default=True):
                    files = failed
            return files, f"Processing {len(files)} unprocessed files"
            
        elif choice == "2":
            # Process specific number
            available = len(unprocessed) + len(failed)
            console.print(f"\n[cyan]Available: {len(unprocessed)} unprocessed, {len(failed)} failed[/cyan]")
            
            count = IntPrompt.ask(
                "How many files to process?",
                default=min(10, available),
                show_default=True
            )
            
            files, message = self.tracker.suggest_batch_size(count)
            return files, message
            
        elif choice == "3":
            # Retry failed only
            if not failed:
                console.print("[yellow]No failed files to retry[/yellow]")
                return [], "No failed files"
            return failed, f"Retrying {len(failed)} failed files"
            
        elif choice == "4":
            # Custom selection
            return self._custom_file_selection(unprocessed, failed)
            
        elif choice == "5":
            # Show detailed list
            self._show_detailed_file_list()
            return self.show_processing_options()  # Recursive call
            
        return [], "Invalid selection"
        
    def _custom_file_selection(self, unprocessed: List[Path], failed: List[Path]) -> Tuple[List[Path], str]:
        """Allow custom file selection"""
        console.print("\n[bold]Custom File Selection[/bold]")
        
        # Combine all available files
        all_files = []
        for f in unprocessed:
            all_files.append((f, "new"))
        for f in failed:
            all_files.append((f, "retry"))
            
        # Show files with numbers
        console.print("\n[cyan]Available files:[/cyan]")
        for i, (file, status) in enumerate(all_files[:50], 1):
            status_icon = "[NEW]" if status == "new" else "[RETRY]"
            console.print(f"{i:3d}. {status_icon} {file.name}")
            
        if len(all_files) > 50:
            console.print(f"\n[dim]... and {len(all_files) - 50} more files[/dim]")
            
        # Get selection
        selection = Prompt.ask(
            "\nEnter file numbers to process (comma-separated, ranges like 1-5 allowed)",
            default="1-10"
        )
        
        # Parse selection
        selected_files = []
        for part in selection.split(','):
            part = part.strip()
            if '-' in part:
                # Range
                try:
                    start, end = map(int, part.split('-'))
                    for i in range(start, min(end + 1, len(all_files) + 1)):
                        if 0 < i <= len(all_files):
                            selected_files.append(all_files[i-1][0])
                except:
                    pass
            else:
                # Single number
                try:
                    i = int(part)
                    if 0 < i <= len(all_files):
                        selected_files.append(all_files[i-1][0])
                except:
                    pass
                    
        # Remove duplicates while preserving order
        seen = set()
        unique_files = []
        for f in selected_files:
            if f not in seen:
                seen.add(f)
                unique_files.append(f)
                
        return unique_files, f"Custom selection: {len(unique_files)} files"
        
    def _show_detailed_file_list(self):
        """Show detailed list of all files and their status"""
        console.print("\n[bold]Detailed File List[/bold]\n")
        
        # Create table
        table = Table(title="File Processing Status", box=box.ROUNDED)
        table.add_column("File", style="cyan")
        table.add_column("Status", width=12)
        table.add_column("Size", justify="right")
        table.add_column("Processed", width=20)
        table.add_column("Concepts", justify="right")
        table.add_column("Error", style="red")
        
        # Add processed files
        for file_path, record in self.tracker.get_processed_files():
            table.add_row(
                file_path.name,
                "[green]Complete[/green]",
                self._format_size(record.file_size),
                record.process_date[:10],
                str(record.concepts_found),
                ""
            )
            
        # Add failed files
        for file_path, record in self.tracker.get_failed_files():
            error_msg = record.error_message[:30] + "..." if len(record.error_message) > 30 else record.error_message
            table.add_row(
                file_path.name,
                "[red]Failed[/red]",
                self._format_size(record.file_size),
                record.process_date[:10],
                "-",
                error_msg
            )
            
        # Add unprocessed files
        for file_path in self.tracker.get_unprocessed_files()[:20]:
            size = file_path.stat().st_size
            table.add_row(
                file_path.name,
                "[blue]New[/blue]",
                self._format_size(size),
                "-",
                "-",
                ""
            )
            
        console.print(table)
        
        if len(self.tracker.get_unprocessed_files()) > 20:
            console.print(f"\n[dim]... and {len(self.tracker.get_unprocessed_files()) - 20} more unprocessed files[/dim]")
            
        input("\nPress Enter to continue...")
        
    def _format_size(self, size: int) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.0f}{unit}"
            size /= 1024
        return f"{size:.0f}TB"
        
    def run_smart_processing(self, background: bool = False) -> Dict[str, Any]:
        """Run processing with smart file selection
        
        Args:
            background: If True, run processing in background
        """
        # Get files to process
        files_to_process, message = self.show_processing_options()
        
        if not files_to_process:
            return {"status": "cancelled", "reason": message}
            
        console.print(f"\n[bold cyan]{message}[/bold cyan]")
        
        # Update input/output for parent class
        self.input_dir = self.tracker.input_dir
        self.output_dir = self.tracker.output_dir
        
        # Ask about background processing
        background = Confirm.ask("\nRun processing in background?", default=False)
        
        # Ask about memory system
        current_memory = self.config.get("memory_system", "standard")
        console.print(f"\n[cyan]Current memory system: {current_memory}[/cyan]")
        console.print("Available memory systems:")
        console.print("  1. Standard - Balanced memory usage")
        console.print("  2. Conservative - Lower memory usage, slower processing")
        console.print("  3. Aggressive - Higher memory usage, faster processing")
        
        memory_choice = Prompt.ask("\nSelect memory system", choices=["1", "2", "3"], default="1")
        memory_systems = {"1": "standard", "2": "conservative", "3": "aggressive"}
        selected_memory = memory_systems[memory_choice]
        
        if selected_memory != current_memory:
            self.config.set("memory_system", selected_memory)
            # Adjust settings based on memory system
            if selected_memory == "conservative":
                self.config.set("max_parallel_workers", 2)
                self.config.set("chunk_size", 50)
                self.config.set("java_heap_size", "2g")
            elif selected_memory == "aggressive":
                self.config.set("max_parallel_workers", 8)
                self.config.set("chunk_size", 500)
                self.config.set("java_heap_size", "8g")
        
        # Run validation
        if not Confirm.ask("\nRun validation checks?", default=True):
            validation_passed = True
        else:
            try:
                validation = self.validate_environment()
                validation_passed = self.display_validation_results(validation)
                
                if not validation_passed:
                    if not Confirm.ask("\n[yellow]Validation failed. Continue anyway?[/yellow]", default=False):
                        return {"status": "failed", "reason": "validation_failed"}
            except (AttributeError, TypeError) as e:
                console.print(f"[yellow]Warning: Validation check encountered an issue: {type(e).__name__}[/yellow]")
                console.print("[yellow]Proceeding without validation...[/yellow]")
                validation_passed = True
            except Exception as e:
                console.print(f"[red]Unexpected validation error: {e}[/red]")
                console.print("[yellow]Proceeding without validation...[/yellow]")
                validation_passed = True
                    
        # Start servers if needed
        console.print("\n[cyan]Checking MetaMap server status...[/cyan]")
        tagger_running = self.server_manager.is_tagger_server_running()
        wsd_running = self.server_manager.is_wsd_server_running()
        console.print(f"  Tagger Server: {'[green]Running[/green]' if tagger_running else '[red]Not Running[/red]'}")
        console.print(f"  WSD Server: {'[green]Running[/green]' if wsd_running else '[red]Not Running[/red]'}")
        
        if not (tagger_running and wsd_running):
            console.print("\n[cyan]Starting MetaMap servers...[/cyan]")
            try:
                self.server_manager.start_all()
                console.print("Waiting for servers to initialize...")
                time.sleep(5)  # Give servers more time to initialize
                
                # Verify servers started
                tagger_running = self.server_manager.is_tagger_server_running()
                wsd_running = self.server_manager.is_wsd_server_running()
                
                if tagger_running and wsd_running:
                    console.print("[green]MetaMap servers started successfully[/green]")
                else:
                    console.print("[yellow]Warning: Servers may not have started fully[/yellow]")
                    console.print(f"  Tagger: {'Running' if tagger_running else 'Not Running'}")
                    console.print(f"  WSD: {'Running' if wsd_running else 'Not Running'}")
            except Exception as e:
                console.print(f"[red]Failed to start servers: {e}[/red]")
                return {"status": "failed", "reason": "server_start_failed"}
        
        # Start processing
        console.print("\n[bold green]Starting smart batch processing...[/bold green]\n")
        
        if background:
            # Run in background using nohup
            import subprocess
            import sys
            import json
            
            console.print("\n[yellow]Starting background processing...[/yellow]")
            console.print("[dim]The process will continue running even if you close this session[/dim]")
            console.print("\nMonitor progress with: [cyan]pymm monitor[/cyan]")
            console.print("View logs in: [cyan]pymm_data/logs/[/cyan]\n")
            
            # Save the current state for background processing
            state_file = self.tracker.base_dir / "background_state.json"
            state = {
                "files": [str(f) for f in files_to_process],
                "config": self.config.to_dict(),
                "timestamp": datetime.now().isoformat()
            }
            state_file.write_text(json.dumps(state, indent=2))
            
            # Create a Python script to run in background
            script_path = self.tracker.base_dir / "background_runner.py"
            script_content = f'''#!/usr/bin/env python3
import sys
import json
from pathlib import Path
from pymm.processing.smart_batch_runner import SmartBatchRunner
from pymm.core.config import PyMMConfig

# Load state
state_file = Path("{state_file}")
state = json.loads(state_file.read_text())
files = [Path(f) for f in state["files"]]

# Create config
config = PyMMConfig()
for key, value in state["config"].items():
    config.set(key, value)

# Run processing
runner = SmartBatchRunner(config)
runner.process_files_with_tracker(files)
'''
            script_path.write_text(script_content)
            script_path.chmod(0o755)
            
            # Start background process using nohup
            log_file = self.tracker.base_dir / "logs" / f"background_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            log_file.parent.mkdir(exist_ok=True)
            
            cmd = ["nohup", sys.executable, str(script_path)]
            with open(log_file, 'w') as log:
                process = subprocess.Popen(
                    cmd,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    cwd=str(self.tracker.base_dir)
                )
            
            console.print(f"[green]Background process started with PID: {process.pid}[/green]")
            console.print(f"[dim]Log file: {log_file}[/dim]")
            
            return {
                "status": "started_background",
                "message": "Processing started in background",
                "files": len(files_to_process),
                "pid": process.pid,
                "log_file": str(log_file)
            }
        
        # Process files with tracking
        results = {
            "status": "completed",
            "processed": 0,
            "failed": 0,
            "total_concepts": 0,
            "files": []
        }
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task(
                f"Processing {len(files_to_process)} files...",
                total=len(files_to_process)
            )
            
            for file_path in files_to_process:
                start_time = time.time()
                
                # Mark as started
                output_path = self.tracker.mark_file_started(file_path)
                
                try:
                    # Process file
                    from ..processing.worker import FileProcessor
                    
                    # Get required configuration
                    metamap_binary = self.config.get('metamap_binary_path')
                    if not metamap_binary:
                        raise ValueError("MetaMap binary path not configured")
                        
                    processor = FileProcessor(
                        metamap_binary_path=metamap_binary,
                        output_dir=str(Path(output_path).parent),
                        metamap_options=self.config.get('metamap_processing_options', ''),
                        timeout=self.config.get('pymm_timeout', 300)
                    )
                    
                    # Process
                    success, processing_time, error_msg = processor.process_file(str(file_path))
                    
                    if success:
                        # The actual output file created by processor
                        actual_output = Path(output_path).parent / f"{file_path.stem}.csv"
                        
                        # Count concepts in output
                        concepts_found = self._count_concepts_in_csv(str(actual_output))
                        processing_time = time.time() - start_time
                        
                        self.tracker.mark_file_completed(
                            file_path,
                            concepts_found,
                            processing_time
                        )
                        
                        results["processed"] += 1
                        results["total_concepts"] += concepts_found
                    else:
                        self.tracker.mark_file_failed(file_path, error_msg or "Processing failed")
                        results["failed"] += 1
                        
                except Exception as e:
                    error_msg = str(e)
                    console.print(f"[red]Error processing {file_path.name}: {error_msg}[/red]")
                    self.tracker.mark_file_failed(file_path, error_msg)
                    results["failed"] += 1
                    
                progress.update(task, advance=1)
                
        # Show summary
        console.print("\n[bold green]Processing Complete![/bold green]\n")
        
        summary_table = Table(box=box.ROUNDED)
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", justify="right")
        
        summary_table.add_row("Files Processed", f"[green]{results['processed']}[/green]")
        summary_table.add_row("Files Failed", f"[red]{results['failed']}[/red]")
        summary_table.add_row("Total Concepts", f"[magenta]{results['total_concepts']:,}[/magenta]")
        summary_table.add_row("Success Rate", f"{results['processed'] / len(files_to_process) * 100:.1f}%")
        
        console.print(summary_table)
        
        # Save manifest
        self.tracker.save_manifest()
        
        return results
        
    def _count_concepts_in_csv(self, csv_path: str) -> int:
        """Count concepts in output CSV"""
        try:
            import pandas as pd
            df = pd.read_csv(csv_path)
            return len(df)
        except:
            return 0
    
    def process_files_with_tracker(self, files_to_process: List[Path]):
        """Process files and update tracker - for background processing"""
        # Create processor
        from ..processor import FileProcessor
        processor = FileProcessor(self.config)
        
        # Process each file
        for file_path in files_to_process:
            try:
                # Calculate output path
                relative_path = file_path.relative_to(self.tracker.input_dir)
                output_path = self.tracker.output_dir / relative_path
                output_path = output_path.with_suffix('.csv')
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Process file
                start_time = time.time()
                success, processing_time, error_msg = processor.process_file(str(file_path))
                
                if success:
                    # The actual output file created by processor
                    actual_output = Path(output_path).parent / f"{file_path.stem}.csv"
                    
                    # Count concepts in output
                    concepts_found = self._count_concepts_in_csv(str(actual_output))
                    processing_time = time.time() - start_time
                    
                    self.tracker.mark_file_completed(
                        file_path,
                        concepts_found,
                        processing_time
                    )
                else:
                    self.tracker.mark_file_failed(file_path, error_msg or "Processing failed")
                    
            except Exception as e:
                error_msg = str(e)
                self.tracker.mark_file_failed(file_path, error_msg)