"""Live monitoring interface for PythonMetaMap jobs"""
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import psutil

from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn
from rich.live import Live
from rich.align import Align
from rich.text import Text
from rich import box
from rich.columns import Columns
from rich.chart import Chart

from ..core.job_manager import get_job_manager, JobStatus, JobType

console = Console()


class JobMonitor:
    """Beautiful live monitoring interface for all jobs"""
    
    def __init__(self):
        self.job_manager = get_job_manager()
        self.running = False
        self.selected_job = None
        self.view_mode = "overview"  # overview, details, resources
        
    def create_header(self) -> Panel:
        """Create header panel"""
        jobs = self.job_manager.list_jobs()
        active = len([j for j in jobs if j.status == JobStatus.RUNNING])
        completed = len([j for j in jobs if j.status == JobStatus.COMPLETED])
        failed = len([j for j in jobs if j.status == JobStatus.FAILED])
        
        header_text = f"""[bold cyan]PythonMetaMap Job Monitor[/bold cyan]
[dim]Live monitoring of all processing jobs[/dim]

Active: [green]{active}[/green]  Completed: [blue]{completed}[/blue]  Failed: [red]{failed}[/red]  Total: [yellow]{len(jobs)}[/yellow]
"""
        
        return Panel(
            Align.center(header_text),
            box=box.DOUBLE,
            style="bright_blue"
        )
    
    def create_jobs_table(self) -> Table:
        """Create jobs overview table"""
        table = Table(
            title="Active & Recent Jobs",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta"
        )
        
        table.add_column("ID", style="cyan", width=20)
        table.add_column("Type", style="yellow", width=10)
        table.add_column("Status", width=12)
        table.add_column("Progress", width=25)
        table.add_column("Duration", style="dim", width=10)
        table.add_column("Files", style="green", width=15)
        table.add_column("CPU", style="blue", width=8)
        table.add_column("Memory", style="magenta", width=10)
        
        # Get recent jobs
        jobs = self.job_manager.list_jobs(limit=20)
        
        for job in jobs:
            # Status color
            status_color = {
                JobStatus.RUNNING: "green",
                JobStatus.COMPLETED: "blue",
                JobStatus.FAILED: "red",
                JobStatus.CANCELLED: "yellow",
                JobStatus.QUEUED: "cyan",
                JobStatus.PAUSED: "magenta"
            }.get(job.status, "white")
            
            status_text = f"[{status_color}]{job.status.value.upper()}[/{status_color}]"
            
            # Progress bar
            progress = job.progress or {}
            total = progress.get('total_files', 0)
            processed = progress.get('processed', 0)
            percentage = progress.get('percentage', 0)
            
            if job.status == JobStatus.RUNNING and total > 0:
                # Create mini progress bar
                bar_width = 20
                filled = int(bar_width * percentage / 100)
                empty = bar_width - filled
                progress_bar = f"[green]{'█' * filled}[/green][dim]{'░' * empty}[/dim] {percentage}%"
            else:
                progress_bar = "[dim]—[/dim]"
            
            # Duration
            if job.end_time:
                duration = job.end_time - job.start_time
            else:
                duration = datetime.now() - job.start_time
            
            duration_str = self._format_duration(duration)
            
            # Files
            if total > 0:
                files_str = f"{processed}/{total}"
                if progress.get('failed', 0) > 0:
                    files_str += f" [red]({progress['failed']} failed)[/red]"
            else:
                files_str = "[dim]—[/dim]"
            
            # Resource usage
            cpu_str = "[dim]—[/dim]"
            mem_str = "[dim]—[/dim]"
            
            if job.status == JobStatus.RUNNING:
                stats = self.job_manager.get_job_stats(job.job_id)
                usage = stats.get('resource_usage', {})
                
                if usage.get('cpu_percent') is not None:
                    cpu_str = f"{usage['cpu_percent']:.1f}%"
                
                if usage.get('memory_mb') is not None:
                    mem_mb = usage['memory_mb']
                    if mem_mb > 1024:
                        mem_str = f"{mem_mb/1024:.1f}GB"
                    else:
                        mem_str = f"{mem_mb:.0f}MB"
            
            table.add_row(
                job.job_id[:20],
                job.job_type.value,
                status_text,
                progress_bar,
                duration_str,
                files_str,
                cpu_str,
                mem_str
            )
        
        return table
    
    def create_system_stats(self) -> Panel:
        """Create system statistics panel"""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # CPU bar
        cpu_bar = self._create_bar(cpu_percent, 100, 30, "green" if cpu_percent < 80 else "red")
        
        # Memory bar
        mem_bar = self._create_bar(memory.percent, 100, 30, "green" if memory.percent < 80 else "red")
        
        # Disk bar
        disk_bar = self._create_bar(disk.percent, 100, 30, "green" if disk.percent < 90 else "red")
        
        stats_text = f"""[bold]System Resources[/bold]

CPU Usage:    {cpu_bar} {cpu_percent:.1f}%
Memory:       {mem_bar} {memory.percent:.1f}% ({memory.used/1024**3:.1f}GB / {memory.total/1024**3:.1f}GB)
Disk Space:   {disk_bar} {disk.percent:.1f}% ({disk.used/1024**3:.1f}GB / {disk.total/1024**3:.1f}GB)

Cores: {psutil.cpu_count()}  Load Avg: {', '.join(f'{x:.2f}' for x in psutil.getloadavg())}
"""
        
        return Panel(
            stats_text,
            title="System Status",
            box=box.ROUNDED,
            style="cyan"
        )
    
    def create_job_details(self, job_id: str) -> Panel:
        """Create detailed view for a specific job"""
        job = self.job_manager.get_job(job_id)
        if not job:
            return Panel("Job not found", title="Job Details", style="red")
        
        stats = self.job_manager.get_job_stats(job_id)
        
        # Build details text
        details = f"""[bold cyan]Job ID:[/bold cyan] {job.job_id}
[bold cyan]Type:[/bold cyan] {job.job_type.value}
[bold cyan]Status:[/bold cyan] {self._status_badge(job.status)}
[bold cyan]Started:[/bold cyan] {job.start_time.strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        if job.end_time:
            details += f"[bold cyan]Ended:[/bold cyan] {job.end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        details += f"""[bold cyan]Duration:[/bold cyan] {self._format_duration(timedelta(seconds=stats['duration']))}

[bold]Directories:[/bold]
Input:  {job.input_dir}
Output: {job.output_dir}

[bold]Progress:[/bold]
"""
        
        # Progress details
        progress = job.progress or {}
        if progress.get('total_files', 0) > 0:
            details += f"""Total Files: {progress.get('total_files', 0)}
Processed: {progress.get('processed', 0)} ({progress.get('percentage', 0)}%)
Failed: {progress.get('failed', 0)}
"""
        else:
            details += "No progress data available\n"
        
        # Resource usage
        if stats.get('resource_usage'):
            usage = stats['resource_usage']
            details += f"""
[bold]Resource Usage:[/bold]
CPU: {usage.get('cpu_percent', 0):.1f}%
Memory: {usage.get('memory_mb', 0):.1f}MB
Threads: {usage.get('num_threads', 0)}
"""
        
        # Error info
        if job.error:
            details += f"\n[bold red]Error:[/bold red] {job.error}"
        
        return Panel(
            details,
            title=f"Job Details - {job_id}",
            box=box.ROUNDED,
            style="yellow"
        )
    
    def create_activity_log(self) -> Panel:
        """Create activity log panel"""
        jobs = self.job_manager.list_jobs(limit=10)
        
        log_entries = []
        for job in jobs:
            timestamp = job.start_time.strftime('%H:%M:%S')
            
            if job.status == JobStatus.RUNNING:
                icon = "🔄"
                action = "started"
                color = "green"
            elif job.status == JobStatus.COMPLETED:
                icon = "✅"
                action = "completed"
                color = "blue"
            elif job.status == JobStatus.FAILED:
                icon = "❌"
                action = "failed"
                color = "red"
            else:
                icon = "⏸️"
                action = job.status.value
                color = "yellow"
            
            log_entries.append(
                f"[dim]{timestamp}[/dim] {icon} [{color}]{job.job_type.value}[/{color}] job {action}"
            )
        
        log_text = "\n".join(log_entries) if log_entries else "[dim]No recent activity[/dim]"
        
        return Panel(
            log_text,
            title="Recent Activity",
            box=box.ROUNDED,
            style="dim"
        )
    
    def _create_bar(self, value: float, max_value: float, width: int, color: str) -> str:
        """Create a progress bar"""
        filled = int(width * value / max_value)
        empty = width - filled
        return f"[{color}]{'█' * filled}[/{color}][dim]{'░' * empty}[/dim]"
    
    def _format_duration(self, duration: timedelta) -> str:
        """Format duration as human-readable string"""
        total_seconds = int(duration.total_seconds())
        
        if total_seconds < 60:
            return f"{total_seconds}s"
        elif total_seconds < 3600:
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            return f"{minutes}m {seconds}s"
        else:
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            return f"{hours}h {minutes}m"
    
    def _status_badge(self, status: JobStatus) -> str:
        """Create a status badge"""
        badges = {
            JobStatus.RUNNING: "[green]● RUNNING[/green]",
            JobStatus.COMPLETED: "[blue]✓ COMPLETED[/blue]",
            JobStatus.FAILED: "[red]✗ FAILED[/red]",
            JobStatus.CANCELLED: "[yellow]⚠ CANCELLED[/yellow]",
            JobStatus.QUEUED: "[cyan]◎ QUEUED[/cyan]",
            JobStatus.PAUSED: "[magenta]‖ PAUSED[/magenta]"
        }
        return badges.get(status, status.value)
    
    def create_layout(self) -> Layout:
        """Create the main layout"""
        layout = Layout()
        
        # Split into header and body
        layout.split_column(
            Layout(name="header", size=6),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=1)
        )
        
        # Split body into main and sidebar
        layout["body"].split_row(
            Layout(name="main", ratio=2),
            Layout(name="sidebar", ratio=1)
        )
        
        # Split sidebar into stats and log
        layout["sidebar"].split_column(
            Layout(name="stats", ratio=1),
            Layout(name="log", ratio=1)
        )
        
        # Update content
        layout["header"].update(self.create_header())
        layout["main"].update(self.create_jobs_table())
        layout["stats"].update(self.create_system_stats())
        layout["log"].update(self.create_activity_log())
        layout["footer"].update(
            Panel(
                "[dim]Press [bold]q[/bold] to quit | [bold]r[/bold] to refresh | [bold]↑↓[/bold] to select job | [bold]Enter[/bold] for details | [bold]c[/bold] to cancel job[/dim]",
                box=box.MINIMAL,
                style="dim"
            )
        )
        
        return layout
    
    def run(self):
        """Run the live monitor"""
        self.running = True
        
        with Live(
            self.create_layout(),
            refresh_per_second=2,
            screen=True
        ) as live:
            while self.running:
                try:
                    # Update display
                    live.update(self.create_layout())
                    
                    # Check for input (would need proper input handling)
                    time.sleep(0.5)
                    
                except KeyboardInterrupt:
                    self.running = False
                except Exception as e:
                    console.print(f"[red]Error: {e}[/red]")
                    self.running = False
    
    def stop(self):
        """Stop monitoring"""
        self.running = False


def monitor_jobs():
    """Main entry point for job monitoring"""
    monitor = JobMonitor()
    
    try:
        monitor.run()
    except KeyboardInterrupt:
        pass
    finally:
        console.print("\n[yellow]Monitoring stopped[/yellow]")