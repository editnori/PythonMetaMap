"""Enhanced TUI-based monitoring system with integrated file explorer and job management"""
import os
import time
import threading
import psutil
import json
import subprocess
import shutil
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any, Tuple, Callable
from collections import deque, defaultdict
from dataclasses import dataclass, field
import csv

from rich.console import Console
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.live import Live
from rich.align import Align
from rich import box
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn

from ..core.job_manager import get_job_manager, JobStatus, JobType
from ..core.config import PyMMConfig
from ..processing.worker import FileProcessor, CSV_HEADER
from ..processing.optimized_batch_runner import OptimizedBatchRunner

console = Console()

# Color scheme matching Claude Code style
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


@dataclass
class FileItem:
    """Represents a file in the explorer"""
    path: Path
    name: str
    size: int
    modified: datetime
    is_dir: bool
    is_selected: bool = False
    processing_status: Optional[str] = None  # None, 'queued', 'processing', 'completed', 'failed'


class EnhancedMonitor:
    """Enhanced monitoring system with integrated file explorer and job management"""
    
    def __init__(self, config: PyMMConfig = None):
        self.config = config or PyMMConfig()
        self.job_manager = get_job_manager()
        self.running = False
        
        # File explorer state
        self.current_path = Path(os.getcwd())
        self.selected_files: Set[Path] = set()
        self.file_items: List[FileItem] = []
        self.selected_index = 0
        self.show_hidden = False
        self.filter_pattern = "*.txt"
        
        # View modes
        self.view_mode = "split"  # 'split', 'jobs', 'files', 'monitor'
        self.active_pane = "files"  # 'files', 'jobs'
        
        # Quick process state
        self.quick_process_queue: List[Path] = []
        self.quick_process_results: Dict[str, Any] = {}
        self.quick_process_thread: Optional[threading.Thread] = None
        
        # Resource monitoring
        self.cpu_history = deque(maxlen=60)
        self.memory_history = deque(maxlen=60)
        self.monitor_thread = None
        self._start_resource_monitoring()
        
        # Job selection for actions
        self.selected_job_index = 0
        self.active_jobs: List[Any] = []
        
    def _start_resource_monitoring(self):
        """Start background resource monitoring"""
        def monitor():
            while self.running:
                try:
                    self.cpu_history.append(psutil.cpu_percent(interval=0.1))
                    self.memory_history.append(psutil.virtual_memory().percent)
                    time.sleep(1)
                except:
                    pass
                    
        self.monitor_thread = threading.Thread(target=monitor, daemon=True)
        self.monitor_thread.start()
        
    def run(self):
        """Run the enhanced monitor"""
        self.running = True
        self._load_directory()
        
        try:
            import readchar
        except ImportError:
            console.print("[yellow]Installing required package: readchar[/yellow]")
            subprocess.check_call([os.executable, "-m", "pip", "install", "readchar"])
            import readchar
            
        with Live(self.create_layout(), refresh_per_second=4, screen=True) as live:
            while self.running:
                try:
                    # Handle keyboard input
                    if self._handle_input(readchar.readkey()):
                        live.update(self.create_layout())
                        
                except KeyboardInterrupt:
                    self.running = False
                except Exception as e:
                    console.print(f"[red]Error: {e}[/red]")
                    
    def _handle_input(self, key) -> bool:
        """Handle keyboard input"""
        import readchar
        
        # Global keys
        if key.lower() == 'q':
            self.running = False
            return False
        elif key == readchar.key.TAB:
            # Switch active pane
            if self.view_mode == "split":
                self.active_pane = "jobs" if self.active_pane == "files" else "files"
        elif key.lower() == 'v':
            # Cycle view modes
            modes = ["split", "files", "jobs", "monitor"]
            current_idx = modes.index(self.view_mode)
            self.view_mode = modes[(current_idx + 1) % len(modes)]
        elif key.lower() == 'h':
            self.show_hidden = not self.show_hidden
            self._load_directory()
            
        # Pane-specific keys
        if self.active_pane == "files" and self.view_mode in ["split", "files"]:
            self._handle_file_input(key)
        elif self.active_pane == "jobs" and self.view_mode in ["split", "jobs"]:
            self._handle_job_input(key)
            
        return True
        
    def _handle_file_input(self, key):
        """Handle file explorer input"""
        import readchar
        
        if key == readchar.key.UP:
            if self.selected_index > 0:
                self.selected_index -= 1
        elif key == readchar.key.DOWN:
            if self.selected_index < len(self.file_items) - 1:
                self.selected_index += 1
        elif key == readchar.key.LEFT:
            # Go up directory
            self.current_path = self.current_path.parent
            self._load_directory()
        elif key == readchar.key.RIGHT or key == readchar.key.ENTER:
            # Enter directory or toggle selection
            if self.selected_index < len(self.file_items):
                item = self.file_items[self.selected_index]
                if item.is_dir:
                    self.current_path = item.path
                    self._load_directory()
                else:
                    item.is_selected = not item.is_selected
                    if item.is_selected:
                        self.selected_files.add(item.path)
                    else:
                        self.selected_files.discard(item.path)
        elif key == ' ':
            # Toggle selection
            if self.selected_index < len(self.file_items):
                item = self.file_items[self.selected_index]
                if not item.is_dir:
                    item.is_selected = not item.is_selected
                    if item.is_selected:
                        self.selected_files.add(item.path)
                    else:
                        self.selected_files.discard(item.path)
        elif key.lower() == 'a':
            # Select all
            for item in self.file_items:
                if not item.is_dir:
                    item.is_selected = True
                    self.selected_files.add(item.path)
        elif key.lower() == 'd':
            # Deselect all
            for item in self.file_items:
                item.is_selected = False
            self.selected_files.clear()
        elif key.lower() == 'p':
            # Quick process selected files
            self._quick_process_selected()
            
    def _handle_job_input(self, key):
        """Handle job management input"""
        import readchar
        
        if key == readchar.key.UP:
            if self.selected_job_index > 0:
                self.selected_job_index -= 1
        elif key == readchar.key.DOWN:
            if self.selected_job_index < len(self.active_jobs) - 1:
                self.selected_job_index += 1
        elif key.lower() == 'c':
            # Cancel selected job
            if self.selected_job_index < len(self.active_jobs):
                job = self.active_jobs[self.selected_job_index]
                self.job_manager.cancel_job(job.job_id)
        elif key.lower() == 'k':
            # Kill all jobs
            for job in self.active_jobs:
                if job.status == JobStatus.RUNNING:
                    self.job_manager.cancel_job(job.job_id)
                    
    def _load_directory(self):
        """Load current directory contents"""
        self.file_items = []
        self.selected_index = 0
        
        try:
            # Add parent directory
            if self.current_path != self.current_path.parent:
                self.file_items.append(FileItem(
                    path=self.current_path.parent,
                    name="..",
                    size=0,
                    modified=datetime.now(),
                    is_dir=True
                ))
                
            # List directory contents
            for path in sorted(self.current_path.iterdir()):
                if not self.show_hidden and path.name.startswith('.'):
                    continue
                    
                # Apply filter for files
                if path.is_file() and self.filter_pattern:
                    if not path.match(self.filter_pattern):
                        continue
                        
                stat = path.stat()
                self.file_items.append(FileItem(
                    path=path,
                    name=path.name,
                    size=stat.st_size,
                    modified=datetime.fromtimestamp(stat.st_mtime),
                    is_dir=path.is_dir(),
                    is_selected=path in self.selected_files
                ))
                
        except PermissionError:
            pass
            
    def _quick_process_selected(self):
        """Quick process selected files"""
        if not self.selected_files:
            return
            
        # Get output directory
        output_dir = self.config.get('default_output_dir', './output_csvs')
        output_path = Path(output_dir) / f"quick_process_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create job
        job_id = self.job_manager.create_job(
            job_type=JobType.QUICK,
            input_dir=str(self.current_path),
            output_dir=str(output_path),
            config={'files': list(str(f) for f in self.selected_files)}
        )
        
        self.job_manager.start_job(job_id)
        
        # Start processing thread
        def process_files():
            metamap_path = self.config.get('metamap_binary_path')
            processor = FileProcessor(
                metamap_binary_path=metamap_path,
                output_dir=str(output_path),
                metamap_options=self.config.get('metamap_options', ''),
                timeout=self.config.get('pymm_timeout', 300)
            )
            
            total = len(self.selected_files)
            processed = 0
            failed = 0
            
            for file_path in self.selected_files:
                try:
                    # Update file status
                    for item in self.file_items:
                        if item.path == file_path:
                            item.processing_status = 'processing'
                            break
                            
                    # Process file
                    success, processing_time, error = processor.process_file(str(file_path))
                    
                    if success:
                        processed += 1
                        status = 'completed'
                    else:
                        failed += 1
                        status = 'failed'
                        
                    # Update file status
                    for item in self.file_items:
                        if item.path == file_path:
                            item.processing_status = status
                            break
                            
                    # Update job progress
                    self.job_manager.update_progress(job_id, {
                        'total_files': total,
                        'processed': processed,
                        'failed': failed,
                        'percentage': int((processed + failed) / total * 100)
                    })
                    
                except Exception as e:
                    failed += 1
                    
            # Complete job
            self.job_manager.complete_job(job_id, error=None if failed == 0 else f"{failed} files failed")
            
            # Clear processing status after a delay
            time.sleep(2)
            for item in self.file_items:
                if item.processing_status:
                    item.processing_status = None
                    
        self.quick_process_thread = threading.Thread(target=process_files, daemon=True)
        self.quick_process_thread.start()
        
    def create_layout(self) -> Layout:
        """Create the main layout"""
        layout = Layout()
        
        # Main structure
        layout.split_column(
            Layout(name="header", size=4),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=2)
        )
        
        # Header
        layout["header"].update(self._create_header())
        
        # Body based on view mode
        if self.view_mode == "split":
            layout["body"].split_row(
                Layout(name="files", ratio=1),
                Layout(name="jobs", ratio=1)
            )
            layout["body"]["files"].update(self._create_file_explorer())
            layout["body"]["jobs"].update(self._create_job_monitor())
        elif self.view_mode == "files":
            layout["body"].update(self._create_file_explorer())
        elif self.view_mode == "jobs":
            layout["body"].update(self._create_job_monitor())
        elif self.view_mode == "monitor":
            layout["body"].update(self._create_system_monitor())
            
        # Footer
        layout["footer"].update(self._create_footer())
        
        return layout
        
    def _create_header(self) -> Panel:
        """Create header panel"""
        # Get stats
        jobs = self.job_manager.list_jobs()
        active = len([j for j in jobs if j.status == JobStatus.RUNNING])
        completed = len([j for j in jobs if j.status == JobStatus.COMPLETED])
        failed = len([j for j in jobs if j.status == JobStatus.FAILED])
        
        # Resource stats
        cpu = self.cpu_history[-1] if self.cpu_history else 0
        mem = self.memory_history[-1] if self.memory_history else 0
        
        header_text = f"""[bold cyan]PythonMetaMap Enhanced Monitor[/bold cyan]
Jobs: [green]{active} active[/green] | [blue]{completed} completed[/blue] | [red]{failed} failed[/red]  •  CPU: {cpu:.1f}%  •  Memory: {mem:.1f}%  •  Mode: {self.view_mode.title()}"""
        
        return Panel(
            Align.center(header_text),
            box=box.DOUBLE,
            style="bright_blue"
        )
        
    def _create_file_explorer(self) -> Panel:
        """Create file explorer panel"""
        # Create table
        table = Table(show_header=True, header_style="bold magenta", box=box.SIMPLE)
        table.add_column("", width=2)  # Selection indicator
        table.add_column("Name", style="cyan", no_wrap=True)
        table.add_column("Size", justify="right", style="green")
        table.add_column("Modified", style="dim")
        table.add_column("Status", style="yellow")
        
        # Add rows
        visible_start = max(0, self.selected_index - 15)
        visible_end = min(len(self.file_items), visible_start + 30)
        
        for i, item in enumerate(self.file_items[visible_start:visible_end], visible_start):
            # Selection indicator
            if i == self.selected_index:
                selector = "▶" if self.active_pane == "files" else ">"
                style = "bold cyan"
            else:
                selector = ""
                style = ""
                
            # File/dir icon and name
            if item.is_dir:
                name = f"📁 {item.name}"
                size = ""
            else:
                name = f"📄 {item.name}"
                size = self._format_size(item.size)
                
            # Selection checkbox
            if item.is_selected:
                select_mark = "✓"
            else:
                select_mark = " "
                
            # Status
            status = ""
            if item.processing_status:
                status_map = {
                    'queued': '[yellow]⏳[/yellow]',
                    'processing': '[cyan]🔄[/cyan]',
                    'completed': '[green]✓[/green]',
                    'failed': '[red]✗[/red]'
                }
                status = status_map.get(item.processing_status, '')
                
            table.add_row(
                selector,
                f"[{select_mark}] {name}",
                size,
                item.modified.strftime("%m/%d %H:%M"),
                status,
                style=style
            )
            
        # Panel with path info
        content = f"[bold]{self.current_path}[/bold] • {len(self.selected_files)} selected\n\n{table}"
        
        border_style = "bright_cyan" if self.active_pane == "files" else "dim"
        return Panel(
            content,
            title="[bold]File Explorer[/bold]",
            box=box.ROUNDED,
            style=border_style,
            subtitle="[dim]↑↓Navigate ←Back →Enter Space:Select A:All P:Process[/dim]"
        )
        
    def _create_job_monitor(self) -> Panel:
        """Create job monitor panel"""
        # Get active jobs
        self.active_jobs = self.job_manager.list_jobs(limit=20)
        
        # Create table
        table = Table(show_header=True, header_style="bold magenta", box=box.SIMPLE)
        table.add_column("", width=2)  # Selection indicator
        table.add_column("Type", style="yellow", width=8)
        table.add_column("Status", width=10)
        table.add_column("Progress", width=20)
        table.add_column("Files", style="green")
        table.add_column("Time", style="dim")
        
        for i, job in enumerate(self.active_jobs[:15]):
            # Selection indicator
            if i == self.selected_job_index and self.active_pane == "jobs":
                selector = "▶"
                style = "bold cyan"
            else:
                selector = ""
                style = ""
                
            # Status
            status_color = {
                JobStatus.RUNNING: "green",
                JobStatus.COMPLETED: "blue",
                JobStatus.FAILED: "red",
                JobStatus.CANCELLED: "yellow"
            }.get(job.status, "white")
            
            status = f"[{status_color}]{job.status.value}[/{status_color}]"
            
            # Progress bar
            progress = job.progress or {}
            if job.status == JobStatus.RUNNING and progress.get('percentage', 0) > 0:
                pct = progress['percentage']
                bar_width = 15
                filled = int(bar_width * pct / 100)
                empty = bar_width - filled
                progress_bar = f"[green]{'█' * filled}[/green][dim]{'░' * empty}[/dim] {pct}%"
            else:
                progress_bar = ""
                
            # Files
            total = progress.get('total_files', 0)
            processed = progress.get('processed', 0)
            failed_count = progress.get('failed', 0)
            if total > 0:
                files = f"{processed}/{total}"
                if failed_count > 0:
                    files += f" [red]({failed_count})[/red]"
            else:
                files = ""
                
            # Duration
            if job.end_time:
                duration = job.end_time - job.start_time
            else:
                duration = datetime.now() - job.start_time
            time_str = str(duration).split('.')[0]
            
            table.add_row(
                selector,
                job.job_type.value,
                status,
                progress_bar,
                files,
                time_str,
                style=style
            )
            
        border_style = "bright_cyan" if self.active_pane == "jobs" else "dim"
        return Panel(
            table,
            title="[bold]Job Monitor[/bold]",
            box=box.ROUNDED,
            style=border_style,
            subtitle="[dim]↑↓Navigate C:Cancel K:Kill All[/dim]"
        )
        
    def _create_system_monitor(self) -> Panel:
        """Create system monitor panel"""
        # CPU graph
        cpu_graph = self._create_mini_graph(self.cpu_history, "CPU Usage", "green")
        
        # Memory graph
        mem_graph = self._create_mini_graph(self.memory_history, "Memory Usage", "blue")
        
        # Process info
        process_info = self._get_process_info()
        
        content = f"""{cpu_graph}

{mem_graph}

[bold]Top Processes:[/bold]
{process_info}"""
        
        return Panel(
            content,
            title="[bold]System Monitor[/bold]",
            box=box.ROUNDED,
            style="cyan"
        )
        
    def _create_footer(self) -> Panel:
        """Create footer panel"""
        controls = "[cyan]Tab[/cyan] Switch Pane • [cyan]V[/cyan] View Mode • [cyan]H[/cyan] Hidden Files • [cyan]Q[/cyan] Quit"
        
        return Panel(
            controls,
            box=box.MINIMAL,
            style="dim"
        )
        
    def _format_size(self, size: int) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.0f}{unit}"
            size /= 1024
        return f"{size:.0f}TB"
        
    def _create_mini_graph(self, data: deque, title: str, color: str) -> str:
        """Create a mini ASCII graph"""
        if not data:
            return f"[bold]{title}:[/bold] No data"
            
        # Normalize to 0-10 range
        max_val = max(data) if data else 100
        normalized = [int(v * 10 / max_val) for v in data]
        
        # Create graph
        graph_chars = " ▁▂▃▄▅▆▇█"
        graph = "".join(graph_chars[min(v, 8)] for v in normalized[-30:])
        
        current = data[-1] if data else 0
        return f"[bold]{title}:[/bold] [{color}]{graph}[/{color}] {current:.1f}%"
        
    def _get_process_info(self) -> str:
        """Get top process information"""
        try:
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                if proc.info['cpu_percent'] > 0:
                    processes.append(proc.info)
                    
            # Sort by CPU usage
            processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
            
            lines = []
            for proc in processes[:5]:
                lines.append(
                    f"{proc['name'][:20]:20} CPU: {proc['cpu_percent']:5.1f}% MEM: {proc['memory_percent']:5.1f}%"
                )
                
            return "\n".join(lines)
        except:
            return "Unable to retrieve process information"


def run_enhanced_monitor():
    """Entry point for enhanced monitor"""
    monitor = EnhancedMonitor()
    monitor.run()