"""Unified monitoring system combining job monitoring, file explorer, and logs"""
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
from rich.syntax import Syntax

from ..core.job_manager import get_job_manager, JobStatus, JobType
from ..core.config import PyMMConfig
from ..processing.worker import FileProcessor, CSV_HEADER
from ..server.manager import ServerManager

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


@dataclass
class FileItem:
    """File information for explorer"""
    path: Path
    name: str
    size: int
    modified: datetime
    is_dir: bool
    is_selected: bool = False
    processing_status: Optional[str] = None


class UnifiedMonitor:
    """Unified monitoring system with all features in one interface"""
    
    def __init__(self, config: PyMMConfig = None):
        self.config = config or PyMMConfig()
        self.job_manager = get_job_manager()
        self.server_manager = ServerManager(self.config)
        self.running = False
        
        # View modes
        self.view_mode = "dashboard"  # 'dashboard', 'jobs', 'files', 'logs', 'system'
        self.active_pane = "main"
        
        # File explorer state
        self.current_path = Path(self.config.get('default_input_dir', os.getcwd()))
        self.selected_files: Set[Path] = set()
        self.file_items: List[FileItem] = []
        self.file_selected_index = 0
        self.show_hidden = False
        self.filter_pattern = "*.txt"
        
        # Job monitoring state
        self.selected_job_index = 0
        self.active_jobs: List[Any] = []
        self.job_filter = "all"  # 'all', 'active', 'completed', 'failed'
        
        # Log viewing state
        self.log_files: List[Path] = []
        self.selected_log_index = 0
        self.log_content: List[str] = []
        self.log_offset = 0
        self.log_search = ""
        
        # Resource monitoring
        self.cpu_history = deque(maxlen=120)
        self.memory_history = deque(maxlen=120)
        self.disk_io_history = deque(maxlen=60)
        self.network_history = deque(maxlen=60)
        self.last_disk_read = 0
        self.last_disk_write = 0
        self.last_net_recv = 0
        self.last_net_sent = 0
        
        # Quick process
        self.quick_process_queue: List[Path] = []
        self.quick_process_thread: Optional[threading.Thread] = None
        
        # Start monitoring thread
        self._start_monitoring()
        
    def _start_monitoring(self):
        """Start background resource monitoring"""
        def monitor():
            while self.running:
                try:
                    # CPU and Memory
                    self.cpu_history.append(psutil.cpu_percent(interval=0.1))
                    self.memory_history.append(psutil.virtual_memory().percent)
                    
                    # Disk I/O
                    disk_io = psutil.disk_io_counters()
                    if disk_io:
                        read_rate = disk_io.read_bytes - self.last_disk_read if self.last_disk_read else 0
                        write_rate = disk_io.write_bytes - self.last_disk_write if self.last_disk_write else 0
                        self.last_disk_read = disk_io.read_bytes
                        self.last_disk_write = disk_io.write_bytes
                        self.disk_io_history.append({'read': read_rate, 'write': write_rate})
                    
                    # Network
                    net_io = psutil.net_io_counters()
                    if net_io:
                        recv_rate = net_io.bytes_recv - self.last_net_recv if self.last_net_recv else 0
                        sent_rate = net_io.bytes_sent - self.last_net_sent if self.last_net_sent else 0
                        self.last_net_recv = net_io.bytes_recv
                        self.last_net_sent = net_io.bytes_sent
                        self.network_history.append({'recv': recv_rate, 'sent': sent_rate})
                    
                    time.sleep(1)
                except Exception:
                    pass
                    
        self.monitor_thread = threading.Thread(target=monitor, daemon=True)
        self.monitor_thread.start()
        
    def run(self):
        """Run the unified monitor"""
        self.running = True
        self._load_directory()
        self._load_logs()
        
        try:
            import readchar
        except ImportError:
            console.print("[yellow]Installing required package: readchar[/yellow]")
            subprocess.check_call([os.executable, "-m", "pip", "install", "readchar"])
            import readchar
            
        with Live(self.create_layout(), refresh_per_second=2, screen=True) as live:
            while self.running:
                try:
                    key = readchar.readkey()
                    if self._handle_input(key):
                        self._update_data()
                        live.update(self.create_layout())
                        
                except KeyboardInterrupt:
                    self.running = False
                except Exception as e:
                    console.print(f"[red]Error: {e}[/red]")
                    
    def _handle_input(self, key) -> bool:
        """Handle keyboard input based on current view"""
        import readchar
        
        # Global navigation keys
        if key.lower() == 'q':
            self.running = False
            return False
        elif key in ['1', '2', '3', '4', '5']:
            # Switch view modes
            modes = ['dashboard', 'jobs', 'files', 'logs', 'system']
            self.view_mode = modes[int(key) - 1]
        elif key == readchar.key.TAB:
            # Tab through panes in dashboard view
            if self.view_mode == "dashboard":
                panes = ['jobs', 'files', 'logs', 'system']
                current_idx = panes.index(self.active_pane) if self.active_pane in panes else -1
                self.active_pane = panes[(current_idx + 1) % len(panes)]
        
        # View-specific keys
        if self.view_mode == "files" or (self.view_mode == "dashboard" and self.active_pane == "files"):
            self._handle_file_input(key)
        elif self.view_mode == "jobs" or (self.view_mode == "dashboard" and self.active_pane == "jobs"):
            self._handle_job_input(key)
        elif self.view_mode == "logs" or (self.view_mode == "dashboard" and self.active_pane == "logs"):
            self._handle_log_input(key)
            
        return True
        
    def _handle_file_input(self, key):
        """Handle file explorer input"""
        import readchar
        
        if key == readchar.key.UP:
            if self.file_selected_index > 0:
                self.file_selected_index -= 1
        elif key == readchar.key.DOWN:
            if self.file_selected_index < len(self.file_items) - 1:
                self.file_selected_index += 1
        elif key == readchar.key.LEFT:
            self.current_path = self.current_path.parent
            self._load_directory()
        elif key == readchar.key.RIGHT or key == readchar.key.ENTER:
            if self.file_selected_index < len(self.file_items):
                item = self.file_items[self.file_selected_index]
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
            if self.file_selected_index < len(self.file_items):
                item = self.file_items[self.file_selected_index]
                if not item.is_dir:
                    item.is_selected = not item.is_selected
                    if item.is_selected:
                        self.selected_files.add(item.path)
                    else:
                        self.selected_files.discard(item.path)
        elif key.lower() == 'a':
            for item in self.file_items:
                if not item.is_dir:
                    item.is_selected = True
                    self.selected_files.add(item.path)
        elif key.lower() == 'd':
            for item in self.file_items:
                item.is_selected = False
            self.selected_files.clear()
        elif key.lower() == 'p':
            self._quick_process_selected()
        elif key.lower() == 'h':
            self.show_hidden = not self.show_hidden
            self._load_directory()
            
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
            if self.selected_job_index < len(self.active_jobs):
                job = self.active_jobs[self.selected_job_index]
                self.job_manager.cancel_job(job.job_id)
        elif key.lower() == 'k':
            for job in self.active_jobs:
                if job.status == JobStatus.RUNNING:
                    self.job_manager.cancel_job(job.job_id)
        elif key.lower() == 'f':
            # Cycle through filters
            filters = ['all', 'active', 'completed', 'failed']
            current_idx = filters.index(self.job_filter)
            self.job_filter = filters[(current_idx + 1) % len(filters)]
            
    def _handle_log_input(self, key):
        """Handle log viewing input"""
        import readchar
        
        if key == readchar.key.UP:
            if self.selected_log_index > 0:
                self.selected_log_index -= 1
                self._load_log_content()
        elif key == readchar.key.DOWN:
            if self.selected_log_index < len(self.log_files) - 1:
                self.selected_log_index += 1
                self._load_log_content()
        elif key == readchar.key.PAGE_UP:
            self.log_offset = max(0, self.log_offset - 20)
        elif key == readchar.key.PAGE_DOWN:
            self.log_offset += 20
        elif key.lower() == 's':
            # Search in logs (simplified for now)
            pass
        elif key == readchar.key.ENTER:
            self._load_log_content()
            
    def _load_directory(self):
        """Load current directory contents"""
        self.file_items = []
        self.file_selected_index = 0
        
        try:
            if self.current_path != self.current_path.parent:
                self.file_items.append(FileItem(
                    path=self.current_path.parent,
                    name="..",
                    size=0,
                    modified=datetime.now(),
                    is_dir=True
                ))
                
            for path in sorted(self.current_path.iterdir()):
                if not self.show_hidden and path.name.startswith('.'):
                    continue
                    
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
            
    def _load_logs(self):
        """Load available log files"""
        self.log_files = []
        
        # Processing logs
        log_dir = Path(self.config.get('default_output_dir', './output_csvs')) / "logs"
        if log_dir.exists():
            self.log_files.extend(sorted(log_dir.glob("*.log"), reverse=True))
            
        # Server logs
        server_log = Path("metamap_server.log")
        if server_log.exists():
            self.log_files.append(server_log)
            
    def _load_log_content(self):
        """Load content of selected log file"""
        if self.selected_log_index < len(self.log_files):
            try:
                log_file = self.log_files[self.selected_log_index]
                with open(log_file, 'r') as f:
                    self.log_content = f.readlines()
            except Exception:
                self.log_content = ["Error reading log file"]
                
    def _update_data(self):
        """Update dynamic data"""
        # Update jobs list
        if self.job_filter == "all":
            self.active_jobs = self.job_manager.list_jobs(limit=50)
        elif self.job_filter == "active":
            self.active_jobs = self.job_manager.list_jobs(status=JobStatus.RUNNING)
        elif self.job_filter == "completed":
            self.active_jobs = self.job_manager.list_jobs(status=JobStatus.COMPLETED)
        elif self.job_filter == "failed":
            self.active_jobs = self.job_manager.list_jobs(status=JobStatus.FAILED)
            
    def _quick_process_selected(self):
        """Quick process selected files"""
        if not self.selected_files:
            return
            
        # Check servers
        if not self.server_manager.is_running():
            try:
                self.server_manager.start_all()
                time.sleep(2)
            except Exception:
                for item in self.file_items:
                    if item.is_selected:
                        item.processing_status = 'failed'
                return
                
        # Create output directory
        output_dir = self.config.get('default_output_dir', './output_csvs')
        output_path = Path(output_dir) / f"quick_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Create job
        job_id = self.job_manager.create_job(
            job_type=JobType.QUICK,
            input_dir=str(self.current_path),
            output_dir=str(output_path),
            config={'files': list(str(f) for f in self.selected_files)}
        )
        
        self.job_manager.start_job(job_id)
        
        # Process in thread
        def process():
            processor = FileProcessor(
                self.config.get('metamap_binary_path'),
                str(output_path),
                self.config.get('metamap_options', ''),
                self.config.get('pymm_timeout', 300)
            )
            
            total = len(self.selected_files)
            processed = 0
            failed = 0
            
            for file_path in self.selected_files:
                # Update status
                for item in self.file_items:
                    if item.path == file_path:
                        item.processing_status = 'processing'
                        break
                        
                # Process
                success, _, _ = processor.process_file(str(file_path))
                
                if success:
                    processed += 1
                    status = 'completed'
                else:
                    failed += 1
                    status = 'failed'
                    
                # Update status
                for item in self.file_items:
                    if item.path == file_path:
                        item.processing_status = status
                        break
                        
                # Update job
                self.job_manager.update_progress(job_id, {
                    'total_files': total,
                    'processed': processed,
                    'failed': failed,
                    'percentage': int((processed + failed) / total * 100)
                })
                
            self.job_manager.complete_job(job_id)
            
            # Clear status after delay
            time.sleep(2)
            for item in self.file_items:
                item.processing_status = None
                
        self.quick_process_thread = threading.Thread(target=process, daemon=True)
        self.quick_process_thread.start()
        
    def create_layout(self) -> Layout:
        """Create the main layout based on view mode"""
        layout = Layout()
        
        # Header is always visible
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=2)
        )
        
        # Header
        layout["header"].update(self._create_header())
        
        # Body based on view mode
        if self.view_mode == "dashboard":
            # Split screen dashboard
            layout["body"].split_column(
                Layout(name="top", ratio=1),
                Layout(name="bottom", ratio=1)
            )
            layout["body"]["top"].split_row(
                Layout(name="jobs", ratio=1),
                Layout(name="system", ratio=1)
            )
            layout["body"]["bottom"].split_row(
                Layout(name="files", ratio=1),
                Layout(name="logs", ratio=1)
            )
            
            # Update each quadrant
            layout["body"]["top"]["jobs"].update(self._create_job_panel(mini=True))
            layout["body"]["top"]["system"].update(self._create_system_panel(mini=True))
            layout["body"]["bottom"]["files"].update(self._create_file_panel(mini=True))
            layout["body"]["bottom"]["logs"].update(self._create_log_panel(mini=True))
            
        elif self.view_mode == "jobs":
            layout["body"].update(self._create_job_panel(mini=False))
        elif self.view_mode == "files":
            layout["body"].update(self._create_file_panel(mini=False))
        elif self.view_mode == "logs":
            layout["body"].update(self._create_log_panel(mini=False))
        elif self.view_mode == "system":
            layout["body"].update(self._create_system_panel(mini=False))
            
        # Footer
        layout["footer"].update(self._create_footer())
        
        return layout
        
    def _create_header(self) -> Panel:
        """Create header panel"""
        jobs = self.job_manager.list_jobs()
        active = len([j for j in jobs if j.status == JobStatus.RUNNING])
        server_status = "[green]●[/green]" if self.server_manager.is_running() else "[red]●[/red]"
        
        header_text = f"[bold cyan]PythonMetaMap Monitor[/bold cyan] • Mode: {self.view_mode.title()} • Jobs: {active} active • Server: {server_status}"
        
        return Panel(
            Align.center(header_text),
            box=box.MINIMAL,
            style="bright_blue"
        )
        
    def _create_footer(self) -> Panel:
        """Create footer panel"""
        mode_keys = "[cyan]1[/cyan]Dashboard [cyan]2[/cyan]Jobs [cyan]3[/cyan]Files [cyan]4[/cyan]Logs [cyan]5[/cyan]System"
        
        if self.view_mode == "dashboard":
            controls = f"{mode_keys} • [cyan]Tab[/cyan]Switch • [cyan]Q[/cyan]uit"
        elif self.view_mode == "files":
            controls = f"{mode_keys} • [cyan]↑↓[/cyan]Nav [cyan]Space[/cyan]Select [cyan]P[/cyan]rocess • [cyan]Q[/cyan]uit"
        elif self.view_mode == "jobs":
            controls = f"{mode_keys} • [cyan]↑↓[/cyan]Nav [cyan]C[/cyan]ancel [cyan]F[/cyan]ilter • [cyan]Q[/cyan]uit"
        elif self.view_mode == "logs":
            controls = f"{mode_keys} • [cyan]↑↓[/cyan]Select [cyan]PgUp/Dn[/cyan]Scroll • [cyan]Q[/cyan]uit"
        else:
            controls = f"{mode_keys} • [cyan]Q[/cyan]uit"
            
        return Panel(controls, box=box.MINIMAL, style="dim")
        
    def _create_job_panel(self, mini: bool = False) -> Panel:
        """Create job monitoring panel"""
        table = Table(show_header=True, header_style="bold magenta", box=box.SIMPLE)
        
        if not mini:
            table.add_column("", width=2)
        table.add_column("Type", style="yellow", width=8)
        table.add_column("Status", width=10)
        table.add_column("Progress", width=20 if mini else 25)
        if not mini:
            table.add_column("Files", style="green")
            table.add_column("Duration", style="dim")
            
        # Show fewer jobs in mini mode
        job_limit = 5 if mini else 20
        for i, job in enumerate(self.active_jobs[:job_limit]):
            # Selection indicator
            if not mini and i == self.selected_job_index:
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
            
            status = f"[{status_color}]{job.status.value[:8]}[/{status_color}]"
            
            # Progress
            progress = job.progress or {}
            if job.status == JobStatus.RUNNING and progress.get('percentage', 0) > 0:
                pct = progress['percentage']
                bar_width = 15 if mini else 20
                filled = int(bar_width * pct / 100)
                empty = bar_width - filled
                progress_bar = f"[green]{'█' * filled}[/green][dim]{'░' * empty}[/dim] {pct}%"
            else:
                progress_bar = ""
                
            row = [job.job_type.value, status, progress_bar]
            
            if not mini:
                # Files
                total = progress.get('total_files', 0)
                processed = progress.get('processed', 0)
                files = f"{processed}/{total}" if total > 0 else ""
                
                # Duration
                if job.end_time:
                    duration = job.end_time - job.start_time
                else:
                    duration = datetime.now() - job.start_time
                time_str = str(duration).split('.')[0]
                
                row = [selector] + row + [files, time_str]
                
            table.add_row(*row, style=style)
            
        title = f"[bold]Jobs ({self.job_filter})[/bold]" if not mini else "[bold]Active Jobs[/bold]"
        border_style = "bright_cyan" if self.active_pane == "jobs" else "dim"
        
        return Panel(table, title=title, box=box.ROUNDED, style=border_style)
        
    def _create_file_panel(self, mini: bool = False) -> Panel:
        """Create file explorer panel"""
        table = Table(show_header=False, box=box.SIMPLE)
        
        if not mini:
            table.add_column("", width=2)
        table.add_column("", width=3)
        table.add_column("Name", no_wrap=True)
        if not mini:
            table.add_column("Size", justify="right", style="green")
            table.add_column("Status", style="yellow")
            
        # Show fewer files in mini mode
        file_limit = 8 if mini else 25
        start_idx = max(0, self.file_selected_index - file_limit // 2)
        end_idx = min(len(self.file_items), start_idx + file_limit)
        
        for i, item in enumerate(self.file_items[start_idx:end_idx], start_idx):
            # Selection indicator
            if not mini and i == self.file_selected_index:
                selector = "▶"
                style = "bold cyan"
            else:
                selector = ""
                style = ""
                
            # File icon and selection
            if item.is_dir:
                icon = "📁"
                select_mark = " "
            else:
                icon = "📄"
                select_mark = "✓" if item.is_selected else " "
                
            name = f"[{select_mark}] {icon} {item.name}"
            
            row = [name]
            
            if not mini:
                # Size
                size = self._format_size(item.size) if not item.is_dir else ""
                
                # Status
                status = ""
                if item.processing_status:
                    status_map = {
                        'processing': '[cyan]🔄[/cyan]',
                        'completed': '[green]✓[/green]',
                        'failed': '[red]✗[/red]'
                    }
                    status = status_map.get(item.processing_status, '')
                    
                row = [selector] + row + [size, status]
                
            table.add_row(*row, style=style)
            
        # Path info
        path_info = f"{self.current_path} ({len(self.selected_files)} selected)"
        content = f"[dim]{path_info}[/dim]\n{table}"
        
        title = "[bold]File Explorer[/bold]"
        border_style = "bright_cyan" if self.active_pane == "files" else "dim"
        
        return Panel(content, title=title, box=box.ROUNDED, style=border_style)
        
    def _create_log_panel(self, mini: bool = False) -> Panel:
        """Create log viewer panel"""
        if mini:
            # Show log file list
            table = Table(show_header=False, box=box.SIMPLE)
            table.add_column("Log Files", style="cyan")
            
            for i, log_file in enumerate(self.log_files[:8]):
                style = "bold cyan" if i == self.selected_log_index else ""
                table.add_row(log_file.name, style=style)
                
            content = table
        else:
            # Show log content
            if self.log_content:
                # Show lines with offset
                visible_lines = self.log_content[self.log_offset:self.log_offset + 30]
                content = Text("\n".join(visible_lines))
            else:
                content = "[dim]Select a log file to view[/dim]"
                
        title = "[bold]Logs[/bold]"
        border_style = "bright_cyan" if self.active_pane == "logs" else "dim"
        
        return Panel(content, title=title, box=box.ROUNDED, style=border_style)
        
    def _create_system_panel(self, mini: bool = False) -> Panel:
        """Create system resource panel"""
        cpu = self.cpu_history[-1] if self.cpu_history else 0
        mem = self.memory_history[-1] if self.memory_history else 0
        
        if mini:
            # Compact view
            cpu_bar = self._create_mini_bar(cpu, 15)
            mem_bar = self._create_mini_bar(mem, 15)
            
            content = f"""CPU: {cpu_bar} {cpu:>5.1f}%
MEM: {mem_bar} {mem:>5.1f}%"""
            
            # Add disk and network if space
            if self.disk_io_history:
                disk = self.disk_io_history[-1]
                content += f"\nDisk: ↓{self._format_bytes(disk['read'])}/s ↑{self._format_bytes(disk['write'])}/s"
                
            if self.network_history:
                net = self.network_history[-1]
                content += f"\nNet: ↓{self._format_bytes(net['recv'])}/s ↑{self._format_bytes(net['sent'])}/s"
        else:
            # Full view with graphs
            cpu_graph = self._create_graph(self.cpu_history, "CPU Usage", "green")
            mem_graph = self._create_graph(self.memory_history, "Memory Usage", "blue")
            
            # Process list
            processes = self._get_top_processes()
            
            content = f"""{cpu_graph}

{mem_graph}

[bold]Top Processes:[/bold]
{processes}"""
            
        title = "[bold]System Resources[/bold]"
        border_style = "bright_cyan" if self.active_pane == "system" else "dim"
        
        return Panel(content, title=title, box=box.ROUNDED, style=border_style)
        
    def _create_mini_bar(self, value: float, width: int = 20) -> str:
        """Create mini progress bar"""
        filled = int(width * value / 100)
        empty = width - filled
        color = "green" if value < 80 else "yellow" if value < 90 else "red"
        return f"[{color}]{'█' * filled}[/{color}][dim]{'░' * empty}[/dim]"
        
    def _create_graph(self, data: deque, title: str, color: str) -> str:
        """Create ASCII graph"""
        if not data:
            return f"[bold]{title}:[/bold] No data"
            
        # Create sparkline graph
        graph_chars = " ▁▂▃▄▅▆▇█"
        max_val = max(data) if max(data) > 0 else 100
        
        graph_line = ""
        for val in list(data)[-50:]:  # Last 50 values
            idx = int(val * 8 / max_val)
            graph_line += graph_chars[min(idx, 8)]
            
        current = data[-1]
        avg = sum(data) / len(data)
        
        return f"""[bold]{title}:[/bold]
[{color}]{graph_line}[/{color}]
Current: {current:.1f}% | Average: {avg:.1f}% | Peak: {max_val:.1f}%"""
        
    def _get_top_processes(self) -> str:
        """Get top processes by CPU"""
        try:
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                if proc.info['cpu_percent'] > 0:
                    processes.append(proc.info)
                    
            processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
            
            lines = []
            for proc in processes[:10]:
                name = proc['name'][:20].ljust(20)
                cpu = proc['cpu_percent']
                mem = proc['memory_percent']
                lines.append(f"{name} CPU: {cpu:>5.1f}% MEM: {mem:>5.1f}%")
                
            return "\n".join(lines)
        except:
            return "Unable to retrieve process information"
            
    def _format_size(self, size: int) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.0f}{unit}"
            size /= 1024
        return f"{size:.0f}TB"
        
    def _format_bytes(self, bytes_val: float) -> str:
        """Format bytes for transfer rates"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_val < 1024:
                return f"{bytes_val:.0f}{unit}"
            bytes_val /= 1024
        return f"{bytes_val:.0f}TB"


def run_unified_monitor():
    """Entry point for unified monitor"""
    monitor = UnifiedMonitor()
    monitor.run()