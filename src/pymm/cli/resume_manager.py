"""Enhanced Resume/Retry Manager with TUI"""
import os
import shutil
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Set
from collections import defaultdict

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.text import Text
from rich.align import Align
from rich import box
from rich.columns import Columns
from rich.tree import Tree
from rich.syntax import Syntax

from ..core.config import PyMMConfig
from ..core.job_manager import get_job_manager, JobStatus
from ..core.file_tracker import UnifiedFileTracker, ProcessingManifest

console = Console()

# Color scheme
COLORS = {
    'primary': 'bright_cyan',
    'secondary': 'bright_magenta',
    'success': 'bright_green',
    'warning': 'yellow',
    'error': 'bright_red',
    'info': 'bright_blue',
    'dim': 'dim white',
    'header': 'bold bright_cyan'
}


class ResumeRetryManager:
    """Comprehensive resume/retry management with TUI"""
    
    def __init__(self, config: PyMMConfig = None):
        self.config = config or PyMMConfig()
        self.job_manager = get_job_manager()
        self.tracker = UnifiedFileTracker(config) if config.get('use_unified_tracking', True) else None
        
        # Paths
        self.log_dir = Path(self.config.get('default_output_dir', './output_csvs')) / 'logs'
        self.state_dir = Path(self.config.get('state_dir', './.pymm_state'))
        
    def run(self):
        """Run the resume/retry manager TUI"""
        while True:
            self.clear_screen()
            
            # Get current state
            failed_jobs = self.job_manager.list_jobs(status=JobStatus.FAILED, limit=100)
            cancelled_jobs = self.job_manager.list_jobs(status=JobStatus.CANCELLED, limit=100)
            in_progress_jobs = self.job_manager.list_jobs(status=JobStatus.RUNNING, limit=10)
            
            # Get file tracking info
            if self.tracker:
                failed_files = self.tracker.get_failed_files()
                summary = self.tracker.get_processing_summary()
            else:
                failed_files = []
                summary = {}
            
            # Display header
            console.print(Panel(
                "[bold]Resume/Retry Manager[/bold]\nManage failed jobs, clean logs, and retry processing",
                box=box.DOUBLE,
                style=COLORS['primary']
            ))
            
            # Show summary
            self._show_summary(failed_jobs, cancelled_jobs, in_progress_jobs, failed_files, summary)
            
            # Menu
            console.print("\n[bold]Options:[/bold]")
            console.print("[1] View Failed Jobs")
            console.print("[2] Retry Failed Jobs")
            console.print("[3] Manage Log Files")
            console.print("[4] Clean Failed File Records")
            console.print("[5] Reset to Clean State")
            console.print("[6] Resume In-Progress Jobs")
            console.print("[7] View Job Details")
            console.print("[8] Export Failure Report")
            console.print("[B] Back to Main Menu")
            
            choice = Prompt.ask("\nSelect option", default="b").lower()
            
            if choice == "b":
                break
            elif choice == "1":
                self._view_failed_jobs(failed_jobs, failed_files)
            elif choice == "2":
                self._retry_failed_jobs(failed_jobs, failed_files)
            elif choice == "3":
                self._manage_log_files()
            elif choice == "4":
                self._clean_failed_records(failed_files)
            elif choice == "5":
                self._reset_clean_state()
            elif choice == "6":
                self._resume_in_progress(in_progress_jobs)
            elif choice == "7":
                self._view_job_details(failed_jobs + cancelled_jobs + in_progress_jobs)
            elif choice == "8":
                self._export_failure_report(failed_jobs, failed_files)
                
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
        
    def _show_summary(self, failed_jobs, cancelled_jobs, in_progress_jobs, failed_files, summary):
        """Show summary of current state"""
        # Create summary table
        table = Table(box=box.ROUNDED, show_header=False)
        table.add_column("Category", style="cyan")
        table.add_column("Count", justify="right", style="yellow")
        
        table.add_row("Failed Jobs", f"[red]{len(failed_jobs)}[/red]")
        table.add_row("Cancelled Jobs", f"[yellow]{len(cancelled_jobs)}[/yellow]")
        table.add_row("In-Progress Jobs", f"[blue]{len(in_progress_jobs)}[/blue]")
        
        if self.tracker:
            table.add_row("", "")
            table.add_row("Failed Files", f"[red]{len(failed_files)}[/red]")
            table.add_row("Total Processed", f"[green]{summary.get('processed', 0)}[/green]")
            table.add_row("Unprocessed", f"[dim]{summary.get('unprocessed', 0)}[/dim]")
            
        # Log file summary
        log_count, log_size = self._get_log_summary()
        table.add_row("", "")
        table.add_row("Log Files", str(log_count))
        table.add_row("Log Size", self._format_size(log_size))
        
        console.print(table)
        
    def _view_failed_jobs(self, failed_jobs, failed_files):
        """View detailed failed job information"""
        self.clear_screen()
        console.print(Panel(
            "[bold red]Failed Jobs & Files[/bold red]",
            box=box.DOUBLE
        ))
        
        if not failed_jobs and not failed_files:
            console.print("\n[green]No failed jobs or files![/green]")
            input("\nPress Enter to continue...")
            return
            
        # Failed jobs table
        if failed_jobs:
            job_table = Table(title="Failed Jobs", box=box.ROUNDED)
            job_table.add_column("ID", style="dim")
            job_table.add_column("Type", style="yellow")
            job_table.add_column("Input", style="cyan")
            job_table.add_column("Failed At", style="red")
            job_table.add_column("Error", style="red")
            
            for job in failed_jobs[:20]:
                error = job.error[:40] + "..." if job.error and len(job.error) > 40 else job.error or "Unknown"
                job_table.add_row(
                    job.job_id[:8],
                    job.job_type.value,
                    Path(job.input_dir).name,
                    (job.end_time or job.start_time).strftime("%Y-%m-%d %H:%M"),
                    error
                )
                
            console.print(job_table)
            
        # Failed files table
        if failed_files and self.tracker:
            console.print()
            file_table = Table(title="Failed Files", box=box.ROUNDED)
            file_table.add_column("File", style="cyan")
            file_table.add_column("Size", justify="right")
            file_table.add_column("Failed At", style="red")
            file_table.add_column("Error", style="red")
            
            for file_path, record in failed_files[:20]:
                error = record.error_message[:40] + "..." if len(record.error_message) > 40 else record.error_message
                file_table.add_row(
                    file_path.name,
                    self._format_size(record.file_size),
                    record.process_date[:16],
                    error
                )
                
            console.print(file_table)
            
        input("\nPress Enter to continue...")
        
    def _retry_failed_jobs(self, failed_jobs, failed_files):
        """Retry failed jobs and files"""
        self.clear_screen()
        console.print(Panel(
            "[bold]Retry Failed Processing[/bold]",
            box=box.DOUBLE,
            style="yellow"
        ))
        
        # Show options
        console.print("\n[bold]Retry Options:[/bold]")
        console.print("[1] Retry all failed jobs")
        console.print("[2] Retry specific jobs")
        console.print("[3] Retry all failed files")
        console.print("[4] Retry specific files")
        console.print("[5] Smart retry (most recent failures)")
        console.print("[B] Back")
        
        choice = Prompt.ask("\nSelect option", default="b").lower()
        
        if choice == "b":
            return
        elif choice == "1":
            self._retry_all_jobs(failed_jobs)
        elif choice == "2":
            self._retry_specific_jobs(failed_jobs)
        elif choice == "3":
            self._retry_all_files(failed_files)
        elif choice == "4":
            self._retry_specific_files(failed_files)
        elif choice == "5":
            self._smart_retry(failed_jobs, failed_files)
            
    def _manage_log_files(self):
        """Manage log files"""
        self.clear_screen()
        console.print(Panel(
            "[bold]Log File Management[/bold]",
            box=box.DOUBLE,
            style="blue"
        ))
        
        # Get log files
        log_files = self._get_log_files()
        
        if not log_files:
            console.print("\n[green]No log files found![/green]")
            input("\nPress Enter to continue...")
            return
            
        # Group logs by type and age
        grouped_logs = self._group_log_files(log_files)
        
        # Display log summary
        console.print(f"\n[bold]Found {len(log_files)} log files[/bold]")
        
        for category, files in grouped_logs.items():
            size = sum(f.stat().st_size for f in files)
            console.print(f"\n[cyan]{category}:[/cyan] {len(files)} files ({self._format_size(size)})")
            
        # Options
        console.print("\n[bold]Options:[/bold]")
        console.print("[1] Delete all logs")
        console.print("[2] Delete logs older than 7 days")
        console.print("[3] Delete logs older than 30 days")
        console.print("[4] Delete failed job logs only")
        console.print("[5] View specific log")
        console.print("[6] Archive logs")
        console.print("[B] Back")
        
        choice = Prompt.ask("\nSelect option", default="b").lower()
        
        if choice == "b":
            return
        elif choice == "1":
            if Confirm.ask("\n[red]Delete ALL log files?[/red]", default=False):
                self._delete_logs(log_files)
        elif choice == "2":
            old_logs = [f for f in log_files if self._is_older_than(f, days=7)]
            if old_logs and Confirm.ask(f"\nDelete {len(old_logs)} logs older than 7 days?", default=True):
                self._delete_logs(old_logs)
        elif choice == "3":
            old_logs = [f for f in log_files if self._is_older_than(f, days=30)]
            if old_logs and Confirm.ask(f"\nDelete {len(old_logs)} logs older than 30 days?", default=True):
                self._delete_logs(old_logs)
        elif choice == "4":
            failed_logs = grouped_logs.get("Failed Jobs", [])
            if failed_logs and Confirm.ask(f"\nDelete {len(failed_logs)} failed job logs?", default=True):
                self._delete_logs(failed_logs)
        elif choice == "5":
            self._view_log_file(log_files)
        elif choice == "6":
            self._archive_logs(log_files)
            
    def _clean_failed_records(self, failed_files):
        """Clean failed file records from tracking"""
        self.clear_screen()
        console.print(Panel(
            "[bold]Clean Failed Records[/bold]",
            box=box.DOUBLE,
            style="yellow"
        ))
        
        if not self.tracker or not failed_files:
            console.print("\n[green]No failed file records to clean![/green]")
            input("\nPress Enter to continue...")
            return
            
        console.print(f"\n[bold]Found {len(failed_files)} failed file records[/bold]")
        
        # Show failed files
        for i, (file_path, record) in enumerate(failed_files[:10], 1):
            console.print(f"{i}. {file_path.name} - {record.error_message[:50]}")
            
        if len(failed_files) > 10:
            console.print(f"\n[dim]... and {len(failed_files) - 10} more[/dim]")
            
        # Options
        console.print("\n[bold]Options:[/bold]")
        console.print("[1] Remove all failed records (files remain)")
        console.print("[2] Remove records and delete source files")
        console.print("[3] Remove specific records")
        console.print("[B] Back")
        
        choice = Prompt.ask("\nSelect option", default="b").lower()
        
        if choice == "b":
            return
        elif choice == "1":
            if Confirm.ask("\n[yellow]Remove all failed records from tracking?[/yellow]", default=False):
                for file_path, _ in failed_files:
                    relative_path = str(file_path.relative_to(self.tracker.input_dir))
                    del self.tracker.manifest.files[relative_path]
                self.tracker.manifest.statistics['total_failed'] = 0
                self.tracker.save_manifest()
                console.print(f"\n[green]Removed {len(failed_files)} failed records[/green]")
        elif choice == "2":
            if Confirm.ask("\n[red]Remove records AND delete source files?[/red]", default=False):
                deleted = 0
                for file_path, _ in failed_files:
                    try:
                        file_path.unlink()
                        relative_path = str(file_path.relative_to(self.tracker.input_dir))
                        del self.tracker.manifest.files[relative_path]
                        deleted += 1
                    except:
                        pass
                self.tracker.manifest.statistics['total_failed'] = 0
                self.tracker.save_manifest()
                console.print(f"\n[green]Deleted {deleted} files and records[/green]")
        elif choice == "3":
            self._remove_specific_records(failed_files)
            
        input("\nPress Enter to continue...")
        
    def _reset_clean_state(self):
        """Reset to a completely clean state"""
        self.clear_screen()
        console.print(Panel(
            "[bold red]Reset to Clean State[/bold red]\nThis will remove all processing history",
            box=box.DOUBLE,
            style="red"
        ))
        
        # Show what will be deleted
        console.print("\n[bold]This will:[/bold]")
        console.print("• [red]Delete all log files[/red]")
        console.print("• [red]Clear all job history[/red]")
        console.print("• [red]Reset processing manifest[/red]")
        console.print("• [red]Remove all state files[/red]")
        console.print("\n[bold]This will NOT:[/bold]")
        console.print("• [green]Delete input files[/green]")
        console.print("• [green]Delete output CSV files[/green]")
        
        if not Confirm.ask("\n[bold red]Are you absolutely sure?[/bold red]", default=False):
            return
            
        # Second confirmation
        confirm_text = Prompt.ask(
            '\n[bold red]Type "RESET" to confirm[/bold red]'
        )
        
        if confirm_text != "RESET":
            console.print("\n[yellow]Reset cancelled[/yellow]")
            input("\nPress Enter to continue...")
            return
            
        # Perform reset
        with console.status("[bold red]Resetting to clean state...[/bold red]"):
            # Delete logs
            if self.log_dir.exists():
                shutil.rmtree(self.log_dir)
                self.log_dir.mkdir(parents=True, exist_ok=True)
                
            # Clear job history
            self.job_manager.cleanup_old_jobs(older_than_hours=0)
            
            # Reset manifest
            if self.tracker:
                self.tracker.manifest = ProcessingManifest()
                self.tracker.save_manifest()
                
            # Remove state files
            if self.state_dir.exists():
                shutil.rmtree(self.state_dir)
                
        console.print("\n[green]Successfully reset to clean state![/green]")
        console.print("\n[cyan]You can now start fresh with:[/cyan]")
        console.print("• No processing history")
        console.print("• No failed records")
        console.print("• Clean log directory")
        
        input("\nPress Enter to continue...")
        
    def _resume_in_progress(self, in_progress_jobs):
        """Resume or manage in-progress jobs"""
        if not in_progress_jobs:
            console.print("\n[green]No in-progress jobs found![/green]")
            input("\nPress Enter to continue...")
            return
            
        self.clear_screen()
        console.print(Panel(
            "[bold blue]In-Progress Jobs[/bold blue]",
            box=box.DOUBLE
        ))
        
        # Show in-progress jobs
        for i, job in enumerate(in_progress_jobs, 1):
            progress = job.progress or {}
            console.print(f"\n[bold]{i}. Job {job.job_id[:8]}[/bold]")
            console.print(f"   Type: {job.job_type.value}")
            console.print(f"   Started: {job.created_at}")
            console.print(f"   Progress: {progress.get('percentage', 0)}%")
            
        console.print("\n[bold]Options:[/bold]")
        console.print("[1] Cancel all in-progress jobs")
        console.print("[2] View job details")
        console.print("[B] Back")
        
        choice = Prompt.ask("\nSelect option", default="b").lower()
        
        if choice == "1":
            if Confirm.ask("\nCancel all in-progress jobs?", default=False):
                for job in in_progress_jobs:
                    self.job_manager.cancel_job(job.job_id)
                console.print(f"\n[yellow]Cancelled {len(in_progress_jobs)} jobs[/yellow]")
                
        input("\nPress Enter to continue...")
        
    def _view_job_details(self, all_jobs):
        """View detailed job information"""
        if not all_jobs:
            console.print("\n[yellow]No jobs found[/yellow]")
            input("\nPress Enter to continue...")
            return
            
        # Select job
        console.print("\n[bold]Recent Jobs:[/bold]")
        for i, job in enumerate(all_jobs[:20], 1):
            status_color = {
                JobStatus.FAILED: "red",
                JobStatus.CANCELLED: "yellow",
                JobStatus.RUNNING: "blue",
                JobStatus.COMPLETED: "green"
            }.get(job.status, "white")
            
            console.print(f"{i:2d}. [{status_color}]{job.status.value:10s}[/{status_color}] {job.job_id[:8]} - {job.job_type.value}")
            
        job_num = IntPrompt.ask("\nSelect job number (0 to cancel)", default=0)
        
        if job_num == 0 or job_num > len(all_jobs):
            return
            
        # Show detailed info
        job = all_jobs[job_num - 1]
        self.clear_screen()
        
        console.print(Panel(
            f"[bold]Job Details: {job.job_id}[/bold]",
            box=box.DOUBLE
        ))
        
        # Job info
        info_table = Table(show_header=False, box=box.SIMPLE)
        info_table.add_column("Field", style="cyan")
        info_table.add_column("Value")
        
        info_table.add_row("Type", job.job_type.value)
        info_table.add_row("Status", f"[{self._get_status_color(job.status)}]{job.status.value}[/]")
        info_table.add_row("Created", str(job.start_time))
        info_table.add_row("Updated", str(job.end_time or job.start_time))
        info_table.add_row("Input Dir", job.input_dir)
        info_table.add_row("Output Dir", job.output_dir)
        
        if job.progress:
            info_table.add_row("Progress", f"{job.progress.get('percentage', 0)}%")
            info_table.add_row("Files", f"{job.progress.get('processed', 0)}/{job.progress.get('total_files', 0)}")
            
        if job.error:
            info_table.add_row("Error", Text(job.error, style="red"))
            
        console.print(info_table)
        
        # Show log if available
        log_file = self.log_dir / f"{job.job_id}.log"
        if log_file.exists():
            console.print(f"\n[bold]Log Preview:[/bold]")
            with open(log_file, 'r') as f:
                lines = f.readlines()
                # Show last 20 lines
                for line in lines[-20:]:
                    console.print(f"[dim]{line.rstrip()}[/dim]")
                    
        input("\nPress Enter to continue...")
        
    def _export_failure_report(self, failed_jobs, failed_files):
        """Export detailed failure report"""
        report_path = Path(self.config.get('default_output_dir', './output_csvs')) / f"failure_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_path, 'w') as f:
            f.write("PythonMetaMap Failure Report\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write("=" * 80 + "\n\n")
            
            # Failed jobs
            f.write(f"FAILED JOBS ({len(failed_jobs)})\n")
            f.write("-" * 40 + "\n")
            for job in failed_jobs:
                f.write(f"\nJob ID: {job.job_id}\n")
                f.write(f"Type: {job.job_type.value}\n")
                f.write(f"Input: {job.input_dir}\n")
                f.write(f"Failed: {job.end_time or job.start_time}\n")
                f.write(f"Error: {job.error}\n")
                
            # Failed files
            if failed_files and self.tracker:
                f.write(f"\n\nFAILED FILES ({len(failed_files)})\n")
                f.write("-" * 40 + "\n")
                for file_path, record in failed_files:
                    f.write(f"\nFile: {file_path}\n")
                    f.write(f"Size: {self._format_size(record.file_size)}\n")
                    f.write(f"Failed: {record.process_date}\n")
                    f.write(f"Error: {record.error_message}\n")
                    
        console.print(f"\n[green]Report exported to: {report_path}[/green]")
        input("\nPress Enter to continue...")
        
    # Helper methods
    def _get_log_files(self) -> List[Path]:
        """Get all log files"""
        if not self.log_dir.exists():
            return []
        return list(self.log_dir.glob("*.log"))
        
    def _get_log_summary(self) -> Tuple[int, int]:
        """Get log file count and total size"""
        log_files = self._get_log_files()
        total_size = sum(f.stat().st_size for f in log_files)
        return len(log_files), total_size
        
    def _group_log_files(self, log_files: List[Path]) -> Dict[str, List[Path]]:
        """Group log files by category"""
        groups = defaultdict(list)
        
        for log_file in log_files:
            # Categorize by content
            if "failed" in log_file.name.lower():
                groups["Failed Jobs"].append(log_file)
            elif "error" in log_file.name.lower():
                groups["Error Logs"].append(log_file)
            elif self._is_older_than(log_file, days=30):
                groups["Old Logs (30+ days)"].append(log_file)
            elif self._is_older_than(log_file, days=7):
                groups["Recent Logs (7-30 days)"].append(log_file)
            else:
                groups["Current Logs (<7 days)"].append(log_file)
                
        return dict(groups)
        
    def _is_older_than(self, file_path: Path, days: int) -> bool:
        """Check if file is older than specified days"""
        file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        return datetime.now() - file_time > timedelta(days=days)
        
    def _delete_logs(self, log_files: List[Path]):
        """Delete specified log files"""
        deleted = 0
        for log_file in log_files:
            try:
                log_file.unlink()
                deleted += 1
            except:
                pass
                
        console.print(f"\n[green]Deleted {deleted} log files[/green]")
        input("\nPress Enter to continue...")
        
    def _view_log_file(self, log_files: List[Path]):
        """View a specific log file"""
        console.print("\n[bold]Recent Log Files:[/bold]")
        
        # Sort by modification time
        sorted_logs = sorted(log_files, key=lambda f: f.stat().st_mtime, reverse=True)
        
        for i, log_file in enumerate(sorted_logs[:20], 1):
            size = self._format_size(log_file.stat().st_size)
            modified = datetime.fromtimestamp(log_file.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            console.print(f"{i:2d}. {log_file.name:40s} {size:>10s} {modified}")
            
        log_num = IntPrompt.ask("\nSelect log number (0 to cancel)", default=0)
        
        if log_num > 0 and log_num <= len(sorted_logs):
            log_file = sorted_logs[log_num - 1]
            
            # Show log content
            self.clear_screen()
            console.print(Panel(
                f"[bold]Log: {log_file.name}[/bold]",
                box=box.DOUBLE
            ))
            
            with open(log_file, 'r') as f:
                content = f.read()
                
            # Use syntax highlighting for better readability
            syntax = Syntax(content, "log", theme="monokai", line_numbers=True)
            console.print(syntax)
            
        input("\nPress Enter to continue...")
        
    def _archive_logs(self, log_files: List[Path]):
        """Archive log files"""
        if not log_files:
            return
            
        archive_dir = self.log_dir / "archive" / datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        archived = 0
        for log_file in log_files:
            try:
                shutil.move(str(log_file), str(archive_dir / log_file.name))
                archived += 1
            except:
                pass
                
        console.print(f"\n[green]Archived {archived} log files to: {archive_dir}[/green]")
        input("\nPress Enter to continue...")
        
    def _retry_all_jobs(self, failed_jobs):
        """Retry all failed jobs"""
        if not failed_jobs:
            return
            
        console.print(f"\n[yellow]Retrying {len(failed_jobs)} failed jobs...[/yellow]")
        
        # Implementation would restart the jobs
        # For now, just show what would happen
        console.print("\n[dim]This would restart all failed jobs with their original parameters[/dim]")
        input("\nPress Enter to continue...")
        
    def _retry_specific_jobs(self, failed_jobs):
        """Retry specific jobs"""
        if not failed_jobs:
            return
            
        # Show job list
        for i, job in enumerate(failed_jobs[:20], 1):
            console.print(f"{i:2d}. {job.job_id[:8]} - {job.job_type.value} - {Path(job.input_dir).name}")
            
        selection = Prompt.ask("\nEnter job numbers to retry (comma-separated)", default="1")
        
        # Parse selection
        selected_indices = []
        for part in selection.split(','):
            try:
                idx = int(part.strip()) - 1
                if 0 <= idx < len(failed_jobs):
                    selected_indices.append(idx)
            except:
                pass
                
        console.print(f"\n[yellow]Would retry {len(selected_indices)} jobs[/yellow]")
        input("\nPress Enter to continue...")
        
    def _retry_all_files(self, failed_files):
        """Retry all failed files"""
        if not failed_files or not self.tracker:
            return
            
        console.print(f"\n[yellow]Retrying {len(failed_files)} failed files...[/yellow]")
        
        # Would use the smart batch runner to retry these files
        console.print("\n[dim]This would reprocess all failed files[/dim]")
        input("\nPress Enter to continue...")
        
    def _retry_specific_files(self, failed_files):
        """Retry specific files"""
        if not failed_files or not self.tracker:
            return
            
        # Show file list
        for i, (file_path, record) in enumerate(failed_files[:20], 1):
            console.print(f"{i:2d}. {file_path.name} - {record.error_message[:40]}")
            
        selection = Prompt.ask("\nEnter file numbers to retry (comma-separated)", default="1")
        
        # Parse selection
        selected_indices = []
        for part in selection.split(','):
            try:
                idx = int(part.strip()) - 1
                if 0 <= idx < len(failed_files):
                    selected_indices.append(idx)
            except:
                pass
                
        console.print(f"\n[yellow]Would retry {len(selected_indices)} files[/yellow]")
        input("\nPress Enter to continue...")
        
    def _smart_retry(self, failed_jobs, failed_files):
        """Smart retry based on failure patterns"""
        console.print("\n[cyan]Analyzing failure patterns...[/cyan]")
        
        # Analyze recent failures
        recent_threshold = datetime.now() - timedelta(hours=24)
        recent_jobs = [j for j in failed_jobs if (j.end_time or j.start_time) > recent_threshold]
        
        console.print(f"\nFound {len(recent_jobs)} jobs failed in last 24 hours")
        console.print("Common failure reasons:")
        console.print("• Timeout errors: Increase timeout setting")
        console.print("• Memory errors: Reduce chunk size")
        console.print("• Server errors: Check MetaMap server status")
        
        if Confirm.ask("\nRetry recent failures with adjusted settings?", default=True):
            console.print("\n[yellow]Would retry with optimized settings[/yellow]")
            
        input("\nPress Enter to continue...")
        
    def _remove_specific_records(self, failed_files):
        """Remove specific failed records"""
        # Show numbered list
        for i, (file_path, record) in enumerate(failed_files[:30], 1):
            console.print(f"{i:2d}. {file_path.name} - {record.error_message[:40]}")
            
        selection = Prompt.ask("\nEnter record numbers to remove (comma-separated)")
        
        # Parse and remove
        removed = 0
        for part in selection.split(','):
            try:
                idx = int(part.strip()) - 1
                if 0 <= idx < len(failed_files):
                    file_path, _ = failed_files[idx]
                    relative_path = str(file_path.relative_to(self.tracker.input_dir))
                    if relative_path in self.tracker.manifest.files:
                        del self.tracker.manifest.files[relative_path]
                        removed += 1
            except:
                pass
                
        if removed > 0:
            self.tracker.save_manifest()
            console.print(f"\n[green]Removed {removed} records[/green]")
            
    def _format_size(self, size: int) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.0f}{unit}"
            size /= 1024
        return f"{size:.0f}TB"
        
    def _get_status_color(self, status: JobStatus) -> str:
        """Get color for job status"""
        return {
            JobStatus.FAILED: "red",
            JobStatus.CANCELLED: "yellow", 
            JobStatus.RUNNING: "blue",
            JobStatus.COMPLETED: "green",
            JobStatus.PENDING: "dim"
        }.get(status, "white")