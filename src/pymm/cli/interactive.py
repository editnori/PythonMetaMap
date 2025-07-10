"""Ultimate Interactive TUI for PythonMetaMap - All Features with Claude Code Style"""
import os
import sys
import time
import psutil
import threading
import subprocess
import shutil
import json
import hashlib
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple, Set
from datetime import datetime, timedelta
from collections import deque, defaultdict, Counter
import tempfile
import platform
import numpy as np

import click
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.text import Text
from rich.live import Live
from rich import box
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn, MofNCompleteColumn
from rich.align import Align
from rich.columns import Columns
from rich.layout import Layout
from rich.tree import Tree
from rich.syntax import Syntax
from rich.rule import Rule
from rich.markdown import Markdown

from ..core.config import PyMMConfig
from ..core.enhanced_state import AtomicStateManager, FileTracker
from ..server.manager import ServerManager
from ..processing.optimized_batch_runner import OptimizedBatchRunner
from ..processing.ultra_optimized_runner import UltraOptimizedBatchRunner
from ..processing.pool_manager import AdaptivePoolManager
from .analysis import ConceptAnalyzer
from .enhanced_analysis import EnhancedConceptAnalyzer
from .unified_monitor import UnifiedMonitor

console = Console()

# Claude Code inspired color scheme
COLORS = {
    'primary': 'bright_cyan',
    'secondary': 'bright_magenta',
    'success': 'bright_green',
    'warning': 'yellow',
    'error': 'bright_red',
    'info': 'bright_blue',
    'dim': 'dim white',
    'header': 'bold bright_cyan',
    'box_primary': 'cyan',
    'box_secondary': 'blue'
}

# Analysis Templates
ANALYSIS_TEMPLATES = {
    'clinical_summary': {
        'name': 'Clinical Summary Analysis',
        'description': 'Analyze clinical summaries for key medical concepts',
        'filters': ['diagnosis', 'symptom', 'treatment', 'medication', 'procedure'],
        'semantic_types': ['dsyn', 'sosy', 'topp', 'phsu', 'diap'],
        'report_sections': ['Chief Complaints', 'Diagnoses', 'Medications', 'Procedures']
    },
    'radiology_report': {
        'name': 'Radiology Report Analysis',
        'description': 'Extract findings from radiology reports',
        'filters': ['finding', 'abnormal', 'normal', 'impression', 'recommendation'],
        'semantic_types': ['fndg', 'dsyn', 'bpoc', 'anab'],
        'report_sections': ['Findings', 'Impressions', 'Recommendations']
    },
    'medication_review': {
        'name': 'Medication Review',
        'description': 'Comprehensive medication and drug interaction analysis',
        'filters': ['medication', 'drug', 'dose', 'interaction', 'adverse'],
        'semantic_types': ['phsu', 'orch', 'antb', 'vita'],
        'report_sections': ['Active Medications', 'Drug Classes', 'Potential Interactions']
    },
    'symptom_analysis': {
        'name': 'Symptom Analysis',
        'description': 'Detailed symptom extraction and categorization',
        'filters': ['pain', 'fever', 'nausea', 'fatigue', 'cough', 'symptom'],
        'semantic_types': ['sosy', 'fndg', 'dsyn'],
        'report_sections': ['Primary Symptoms', 'Associated Symptoms', 'Severity Indicators']
    },
    'lab_results': {
        'name': 'Laboratory Results Analysis',
        'description': 'Extract and analyze laboratory test results',
        'filters': ['lab', 'test', 'result', 'value', 'abnormal', 'normal'],
        'semantic_types': ['lbpr', 'lbtr', 'clna'],
        'report_sections': ['Test Results', 'Abnormal Values', 'Trends']
    }
}

# Claude Code style banner
CLAUDE_BANNER = """[bold bright_cyan on black]
╔═════════════════════════════════════════════════════════════════════════╗
║                                                                         ║
║  ██████╗ ██╗   ██╗███╗   ███╗███╗   ███╗     ██████╗██╗     ██╗         ║
║  ██╔══██╗╚██╗ ██╔╝████╗ ████║████╗ ████║    ██╔════╝██║     ██║         ║
║  ██████╔╝ ╚████╔╝ ██╔████╔██║██╔████╔██║    ██║     ██║     ██║         ║
║  ██╔═══╝   ╚██╔╝  ██║╚██╔╝██║██║╚██╔╝██║    ██║     ██║     ██║         ║
║  ██║        ██║   ██║ ╚═╝ ██║██║ ╚═╝ ██║    ╚██████╗███████╗██║         ║
║  ╚═╝        ╚═╝   ╚═╝     ╚═╝╚═╝     ╚═╝     ╚═════╝╚══════╝╚═╝         ║
║                                                                         ║
╚═════════════════════════════════════════════════════════════════════════╝[/bold bright_cyan on black]
            [dim]Advanced Medical Text Processing Suite v9.0.1[/dim]"""


class EnhancedResourceMonitor:
    """Advanced resource monitoring with history and predictions"""
    
    def __init__(self):
        self.cpu_history = deque(maxlen=120)
        self.memory_history = deque(maxlen=120)
        self.disk_history = deque(maxlen=60)
        self.network_history = deque(maxlen=60)
        self.monitoring = False
        self.monitor_thread = None
        self.last_net_recv = 0
        self.last_net_sent = 0
        
    def start(self):
        """Start background monitoring"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
    def stop(self):
        """Stop monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
            
    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.monitoring:
            try:
                # CPU and Memory
                self.cpu_history.append(psutil.cpu_percent(interval=0.1))
                self.memory_history.append(psutil.virtual_memory().percent)
                
                # Disk I/O
                disk_io = psutil.disk_io_counters()
                if disk_io:
                    self.disk_history.append({
                        'read': disk_io.read_bytes,
                        'write': disk_io.write_bytes
                    })
                
                # Network
                net_io = psutil.net_io_counters()
                if net_io:
                    recv_rate = net_io.bytes_recv - self.last_net_recv if self.last_net_recv else 0
                    sent_rate = net_io.bytes_sent - self.last_net_sent if self.last_net_sent else 0
                    self.last_net_recv = net_io.bytes_recv
                    self.last_net_sent = net_io.bytes_sent
                    
                    self.network_history.append({
                        'recv': recv_rate,
                        'sent': sent_rate
                    })
                
                time.sleep(1)
            except Exception:
                pass
                
    def get_compact_status_panel(self) -> Panel:
        """Get compact status panel"""
        cpu = self.cpu_history[-1] if self.cpu_history else 0
        mem = self.memory_history[-1] if self.memory_history else 0
        disk = psutil.disk_usage('/').percent
        
        # Compact bars
        cpu_bar = self._create_mini_bar(cpu, 10)
        mem_bar = self._create_mini_bar(mem, 10)
        disk_bar = self._create_mini_bar(disk, 10)
        
        # Network rates
        if self.network_history:
            net = self.network_history[-1]
            net_text = f"↓{self._format_bytes(net['recv'])}/s ↑{self._format_bytes(net['sent'])}/s"
        else:
            net_text = "↓0B/s ↑0B/s"
        
        content = f"CPU {cpu_bar} {cpu:>3.0f}%  MEM {mem_bar} {mem:>3.0f}%  DSK {disk_bar} {disk:>3.0f}%  NET {net_text}"
        
        return Panel(
            content,
            box=box.MINIMAL,
            style="dim",
            padding=(0, 1)
        )
        
    def _create_mini_bar(self, percent: float, width: int = 10) -> str:
        """Create mini progress bar"""
        filled = int(width * percent / 100)
        empty = width - filled
        
        if percent > 80:
            color = "red"
        elif percent > 60:
            color = "yellow"
        else:
            color = "green"
            
        return f"[{color}]{'█' * filled}[/{color}][dim]{'░' * empty}[/dim]"
        
    def _format_bytes(self, bytes_val: float) -> str:
        """Format bytes compactly"""
        for unit in ['B', 'K', 'M', 'G']:
            if bytes_val < 1024:
                return f"{bytes_val:.0f}{unit}"
            bytes_val /= 1024
        return f"{bytes_val:.0f}T"


class AdvancedFileExplorer:
    """Advanced file explorer with preview and navigation"""
    
    def __init__(self, start_path: Path = None):
        self.current_path = Path(start_path or os.getcwd())
        self.selected_index = 0
        self.selected_files = set()
        self.show_hidden = False
        self.sort_by = 'name'
        self.filter_pattern = None
        self.preview_enabled = True
        self.bookmarks = self._load_bookmarks()
        self.history = deque(maxlen=20)
        
    def _load_bookmarks(self) -> Dict[str, Path]:
        """Load bookmarks"""
        bookmarks = {
            'home': Path.home(),
            'desktop': Path.home() / 'Desktop',
            'documents': Path.home() / 'Documents'
        }
        
        # Add project directories if they exist
        for name, path in [
            ('input', './input_notes'),
            ('output', './output_csvs'),
            ('logs', './output_csvs/logs')
        ]:
            p = Path(path)
            if p.exists():
                bookmarks[name] = p.absolute()
                
        return bookmarks
        
    def navigate(self) -> List[Path]:
        """Interactive file navigation with keyboard controls"""
        import readchar
        
        while True:
            self._render_explorer()
            
            # Get keyboard input
            try:
                key = readchar.readkey()
                
                if key == readchar.key.UP:
                    self._move_selection(-1)
                elif key == readchar.key.DOWN:
                    self._move_selection(1)
                elif key == readchar.key.LEFT:
                    self._go_up()
                elif key == readchar.key.RIGHT or key == readchar.key.ENTER:
                    self._enter_or_select()
                elif key == ' ':
                    self._toggle_selection()
                elif key.lower() == 'a':
                    self._select_all()
                elif key.lower() == 'd':
                    self._deselect_all()
                elif key.lower() == 'h':
                    self.show_hidden = not self.show_hidden
                elif key.lower() == 'p':
                    self.preview_enabled = not self.preview_enabled
                elif key.lower() == 'b':
                    self._show_bookmarks()
                elif key.lower() == 'f':
                    self._filter_files()
                elif key.lower() == 's':
                    self._change_sort()
                elif key.lower() == 'q':
                    break
                elif key == readchar.key.ESC:
                    break
                    
            except KeyboardInterrupt:
                break
                
        return list(self.selected_files)
        
    def _render_explorer(self):
        """Render the file explorer interface"""
        self.clear_screen()
        
        # Create layout
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=3)
        )
        
        # Get file counts for header
        items = self._get_items()
        total_files = sum(1 for i in items if i.is_file())
        total_dirs = sum(1 for i in items if i.is_dir())
        
        # Header with colorful path and file counts
        header_content = f"[bold bright_cyan]📁 File Explorer[/bold bright_cyan]\n"
        header_content += f"[bright_yellow]Path:[/bright_yellow] [cyan]{self.current_path.absolute()}[/cyan]\n"
        header_content += f"[bright_green]Total:[/bright_green] [white]{len(items)}[/white] items "
        header_content += f"([bright_blue]{total_dirs}[/bright_blue] folders, [bright_magenta]{total_files}[/bright_magenta] files) | "
        header_content += f"[bright_yellow]Selected:[/bright_yellow] [white]{len(self.selected_files)}[/white] | "
        header_content += f"[dim]Hidden: {'On' if self.show_hidden else 'Off'} | Sort: {self.sort_by.title()}[/dim]"
        
        header = Panel(
            header_content,
            box=box.ROUNDED,
            style=COLORS['box_primary'],
            padding=(0, 1)
        )
        layout["header"].update(header)
        
        # Main area with files and preview
        if self.preview_enabled:
            main_layout = Layout()
            main_layout.split_row(
                Layout(name="files", ratio=3),
                Layout(name="preview", ratio=2)
            )
            
            # File list
            files_panel = self._create_file_list_panel()
            main_layout["files"].update(files_panel)
            
            # Preview
            preview_panel = self._create_preview_panel()
            main_layout["preview"].update(preview_panel)
            
            layout["main"].update(main_layout)
        else:
            # Just file list
            files_panel = self._create_file_list_panel()
            layout["main"].update(files_panel)
        
        # Footer with controls
        controls = "[cyan]↑↓[/cyan]Navigate [cyan]←[/cyan]Back [cyan]→/Enter[/cyan]Open [cyan]Space[/cyan]Select "
        controls += "[cyan]A[/cyan]ll [cyan]D[/cyan]eselect [cyan]H[/cyan]idden [cyan]P[/cyan]review "
        controls += "[cyan]B[/cyan]ookmarks [cyan]F[/cyan]ilter [cyan]S[/cyan]ort [cyan]Q[/cyan]uit"
        
        footer = Panel(
            controls,
            box=box.MINIMAL,
            style="dim"
        )
        layout["footer"].update(footer)
        
        console.print(layout)
        
    def _create_file_list_panel(self) -> Panel:
        """Create file list panel"""
        items = self._get_items()
        
        table = Table(show_header=True, box=None, padding=(0, 1))
        table.add_column("", width=3)
        table.add_column("Name", style="white", no_wrap=True)
        table.add_column("Size", justify="right", style="dim")
        table.add_column("Modified", style="dim")
        table.add_column("Type", style="dim")
        
        # Show items with pagination
        start = max(0, self.selected_index - 10)
        end = min(len(items), start + 20)
        
        for idx in range(start, end):
            item = items[idx]
            
            # Selection markers
            if idx == self.selected_index:
                marker = "[bright_cyan]▶[/bright_cyan]"
                row_style = "reverse"
            else:
                marker = " "
                row_style = None
                
            if item in self.selected_files:
                marker = "[green]✓[/green]" + marker
                
            # File info with more color
            if item.is_dir():
                name = f"[bold bright_blue]📁 {item.name}/[/bold bright_blue]"
                size = "-"
                type_str = "[bright_blue]DIR[/bright_blue]"
            else:
                # Color code by file type
                if item.suffix in ['.txt', '.log', '.md']:
                    icon = "📄"
                    color = "white"
                elif item.suffix in ['.py', '.sh', '.js', '.java']:
                    icon = "📜"
                    color = "bright_green"
                elif item.suffix in ['.csv', '.json', '.xml']:
                    icon = "📊"
                    color = "bright_yellow"
                else:
                    icon = "📃"
                    color = "dim white"
                    
                name = f"[{color}]{icon} {item.name}[/{color}]"
                size = f"[bright_cyan]{self._format_size(item.stat().st_size)}[/bright_cyan]"
                type_str = f"[dim]{item.suffix[1:].upper() if item.suffix else 'FILE'}[/dim]"
                
            # Modified time
            mtime = datetime.fromtimestamp(item.stat().st_mtime)
            if (datetime.now() - mtime).days < 1:
                modified = mtime.strftime("%H:%M")
            else:
                modified = mtime.strftime("%m/%d")
                
            table.add_row(marker, name, size, modified, type_str, style=row_style)
            
        # Add scroll indicators
        if start > 0:
            console.print("[dim]  ↑ More files above[/dim]")
        if end < len(items):
            console.print("[dim]  ↓ More files below[/dim]")
            
        return Panel(
            table,
            title=f"[bold]Files[/bold] ({start+1}-{end}/{len(items)})",
            box=box.ROUNDED,
            style=COLORS['box_primary']
        )
        
    def _create_preview_panel(self) -> Panel:
        """Create preview panel"""
        items = self._get_items()
        
        if not items or self.selected_index >= len(items):
            content = "[dim]No file selected[/dim]"
        else:
            item = items[self.selected_index]
            
            if item.is_dir():
                # Directory preview
                content = self._preview_directory(item)
            elif item.suffix in ['.txt', '.log', '.csv', '.json', '.md']:
                # Text file preview
                content = self._preview_text_file(item)
            elif item.suffix in ['.py', '.sh', '.js', '.java']:
                # Code file preview
                content = self._preview_code_file(item)
            else:
                # File info
                content = self._preview_file_info(item)
                
        return Panel(
            content,
            title="[bold]Preview[/bold]",
            box=box.ROUNDED,
            style=COLORS['box_secondary']
        )
        
    def _preview_directory(self, path: Path) -> str:
        """Preview directory contents"""
        try:
            items = list(path.iterdir())
            if not self.show_hidden:
                items = [i for i in items if not i.name.startswith('.')]
                
            content = f"[bold cyan]Directory: {path.name}[/bold cyan]\n\n"
            content += f"Total items: {len(items)}\n"
            
            dirs = sum(1 for i in items if i.is_dir())
            files = len(items) - dirs
            content += f"Directories: {dirs}, Files: {files}\n\n"
            
            # Show first few items
            for item in items[:10]:
                if item.is_dir():
                    content += f"[blue]📁 {item.name}/[/blue]\n"
                else:
                    content += f"📄 {item.name}\n"
                    
            if len(items) > 10:
                content += f"\n[dim]... and {len(items) - 10} more items[/dim]"
                
            return content
        except PermissionError:
            return "[red]Permission denied[/red]"
            
    def _preview_text_file(self, path: Path) -> str:
        """Preview text file contents"""
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()[:30]
                
            content = f"[bold cyan]File: {path.name}[/bold cyan]\n"
            content += f"[dim]Lines: {len(lines)} shown of {sum(1 for _ in open(path))}[/dim]\n\n"
            
            for i, line in enumerate(lines, 1):
                content += f"[dim]{i:3}[/dim] {line.rstrip()[:80]}\n"
                
            if len(lines) == 30:
                content += "\n[dim]... more content below[/dim]"
                
            return content
        except Exception as e:
            return f"[red]Cannot read file: {e}[/red]"
            
    def _preview_code_file(self, path: Path) -> str:
        """Preview code file with syntax highlighting"""
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                code = f.read(2000)
                
            lexer = {
                '.py': 'python',
                '.sh': 'bash',
                '.js': 'javascript',
                '.java': 'java'
            }.get(path.suffix, 'text')
            
            syntax = Syntax(code, lexer, theme="monokai", line_numbers=True)
            return syntax
        except Exception as e:
            return f"[red]Cannot read file: {e}[/red]"
            
    def _preview_file_info(self, path: Path) -> str:
        """Preview file information"""
        stat = path.stat()
        
        content = f"[bold cyan]File: {path.name}[/bold cyan]\n\n"
        content += f"Type: {path.suffix or 'No extension'}\n"
        content += f"Size: {self._format_size(stat.st_size)}\n"
        content += f"Modified: {datetime.fromtimestamp(stat.st_mtime)}\n"
        content += f"Created: {datetime.fromtimestamp(stat.st_ctime)}\n"
        content += f"Permissions: {oct(stat.st_mode)[-3:]}\n"
        
        # Try to detect file type
        try:
            import mimetypes
            mime_type = mimetypes.guess_type(path)[0]
            if mime_type:
                content += f"MIME Type: {mime_type}\n"
        except:
            pass
            
        return content
        
    def _get_items(self) -> List[Path]:
        """Get sorted and filtered directory items"""
        try:
            items = list(self.current_path.iterdir())
            
            # Filter hidden
            if not self.show_hidden:
                items = [i for i in items if not i.name.startswith('.')]
                
            # Filter pattern
            if self.filter_pattern:
                items = [i for i in items if self.filter_pattern.lower() in i.name.lower()]
                
            # Sort
            if self.sort_by == 'name':
                items.sort(key=lambda x: (not x.is_dir(), x.name.lower()))
            elif self.sort_by == 'size':
                items.sort(key=lambda x: (not x.is_dir(), x.stat().st_size if x.is_file() else 0))
            elif self.sort_by == 'date':
                items.sort(key=lambda x: (not x.is_dir(), x.stat().st_mtime))
            elif self.sort_by == 'type':
                items.sort(key=lambda x: (not x.is_dir(), x.suffix))
                
            return items
        except PermissionError:
            return []
            
    def _move_selection(self, delta: int):
        """Move selection up or down"""
        items = self._get_items()
        if items:
            self.selected_index = max(0, min(len(items) - 1, self.selected_index + delta))
            
    def _go_up(self):
        """Go to parent directory"""
        self.history.append(self.current_path)
        self.current_path = self.current_path.parent
        self.selected_index = 0
        
    def _enter_or_select(self):
        """Enter directory or select file"""
        items = self._get_items()
        if items and 0 <= self.selected_index < len(items):
            item = items[self.selected_index]
            if item.is_dir():
                self.history.append(self.current_path)
                self.current_path = item
                self.selected_index = 0
            else:
                self._toggle_selection()
                
    def _toggle_selection(self):
        """Toggle file selection"""
        items = self._get_items()
        if items and 0 <= self.selected_index < len(items):
            item = items[self.selected_index]
            if item.is_file():
                if item in self.selected_files:
                    self.selected_files.remove(item)
                else:
                    self.selected_files.add(item)
                    
    def _select_all(self):
        """Select all files"""
        for item in self._get_items():
            if item.is_file():
                self.selected_files.add(item)
                
    def _deselect_all(self):
        """Deselect all files"""
        self.selected_files.clear()
        
    def _show_bookmarks(self):
        """Show bookmarks menu"""
        self.clear_screen()
        console.print("[bold]Bookmarks[/bold]")
        console.rule(style="dim")
        
        for i, (name, path) in enumerate(self.bookmarks.items(), 1):
            console.print(f"[{i}] {name:<10} {path}")
            
        console.print("\n[B] Back")
        
        choice = Prompt.ask("Select bookmark")
        if choice.lower() != 'b':
            try:
                idx = int(choice) - 1
                if 0 <= idx < len(self.bookmarks):
                    path = list(self.bookmarks.values())[idx]
                    if path.exists():
                        self.current_path = path
                        self.selected_index = 0
            except:
                pass
                
    def _filter_files(self):
        """Set filter pattern"""
        self.clear_screen()
        pattern = Prompt.ask("Filter pattern (empty to clear)")
        self.filter_pattern = pattern if pattern else None
        
    def _change_sort(self):
        """Change sort order"""
        self.clear_screen()
        console.print("[bold]Sort by:[/bold]")
        console.print("[1] Name")
        console.print("[2] Size")
        console.print("[3] Date")
        console.print("[4] Type")
        
        choice = Prompt.ask("Select", choices=["1", "2", "3", "4"], default="1")
        self.sort_by = ['name', 'size', 'date', 'type'][int(choice) - 1]
        
    def _format_size(self, size: int) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.1f}TB"
        
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')


class BackgroundProcessor:
    """Handle background processing with monitoring"""
    
    def __init__(self, config: PyMMConfig):
        self.config = config
        self.processes = {}
        self.monitor_thread = None
        self.monitoring = False
        
    def start_background_process(self, input_dir: str, output_dir: str, process_id: str = None) -> str:
        """Start a background processing job"""
        if not process_id:
            process_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
        # Create command
        cmd = [
            sys.executable, "-m", "pymm", "process",
            input_dir, output_dir,
            "--background"
        ]
        
        # Start process
        log_file = Path(output_dir) / "logs" / f"{process_id}.log"
        log_file.parent.mkdir(exist_ok=True)
        
        with open(log_file, 'w') as f:
            process = subprocess.Popen(
                cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True
            )
            
        self.processes[process_id] = {
            'process': process,
            'input_dir': input_dir,
            'output_dir': output_dir,
            'start_time': datetime.now(),
            'log_file': log_file,
            'status': 'running'
        }
        
        return process_id
        
    def get_job_status(self, process_id: str) -> Dict[str, Any]:
        """Get status of a background job"""
        if process_id not in self.processes:
            return {'status': 'not_found'}
            
        job = self.processes[process_id]
        process = job['process']
        
        # Update status
        if process.poll() is None:
            job['status'] = 'running'
        else:
            job['status'] = 'completed' if process.returncode == 0 else 'failed'
            job['end_time'] = datetime.now()
            job['return_code'] = process.returncode
            
        return job
        
    def list_jobs(self) -> List[Dict[str, Any]]:
        """List all background jobs"""
        jobs = []
        for process_id, job in self.processes.items():
            status = self.get_job_status(process_id)
            jobs.append({
                'id': process_id,
                'status': status['status'],
                'input': job['input_dir'],
                'output': job['output_dir'],
                'start_time': job['start_time'],
                'duration': str(datetime.now() - job['start_time']).split('.')[0]
            })
        return jobs


class ProcessingMonitor:
    """Advanced processing monitor with logging and statistics"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.log_file = output_dir / "logs" / f"processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.stats = {
            'total': 0,
            'processed': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': None,
            'current_file': None,
            'errors': defaultdict(int),
            'processing_times': []
        }
        
        self.log_buffer = deque(maxlen=500)
        self.event_history = deque(maxlen=1000)
        
    def log(self, message: str, level: str = "INFO", file: str = None):
        """Log message with context"""
        timestamp = datetime.now()
        log_entry = {
            'timestamp': timestamp,
            'level': level,
            'message': message,
            'file': file
        }
        
        # Add to buffer
        self.log_buffer.append(log_entry)
        self.event_history.append(log_entry)
        
        # Write to file
        log_line = f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] [{level:>7}] "
        if file:
            log_line += f"[{file}] "
        log_line += message
        
        with open(self.log_file, 'a') as f:
            f.write(log_line + "\n")
            
    def update_stats(self, **kwargs):
        """Update processing statistics"""
        for key, value in kwargs.items():
            if key in self.stats:
                self.stats[key] = value
                
    def record_error(self, error: str, file: str = None):
        """Record error for analysis"""
        self.stats['errors'][error] += 1
        self.log(f"Error: {error}", "ERROR", file)
        
    def get_progress_panel(self) -> Panel:
        """Get detailed progress panel"""
        if self.stats['total'] == 0:
            progress = 0
        else:
            progress = (self.stats['processed'] + self.stats['failed']) / self.stats['total'] * 100
            
        # Calculate rates
        if self.stats['start_time']:
            elapsed = (datetime.now() - self.stats['start_time']).total_seconds()
            rate = self.stats['processed'] / elapsed if elapsed > 0 else 0
            
            # ETA
            remaining = self.stats['total'] - self.stats['processed'] - self.stats['failed']
            eta_seconds = remaining / rate if rate > 0 else 0
            eta = timedelta(seconds=int(eta_seconds))
        else:
            elapsed = 0
            rate = 0
            eta = timedelta(0)
            
        # Progress bar
        bar_width = 30
        filled = int(bar_width * progress / 100)
        bar = f"[green]{'█' * filled}[/green][dim]{'░' * (bar_width - filled)}[/dim]"
        
        # Recent logs
        recent_logs = "\n".join([
            f"[dim]{log['timestamp'].strftime('%H:%M:%S')}[/dim] [{self._get_level_color(log['level'])}]{log['message'][:60]}[/]"
            for log in list(self.log_buffer)[-5:]
        ])
        
        content = f"""[bold]Progress:[/bold] {bar} {progress:>5.1f}%

[bold]Statistics:[/bold]
Total:      {self.stats['total']:>6}  Rate:     {rate:>6.1f} files/s
Processed:  [green]{self.stats['processed']:>6}[/green]  Elapsed:  {str(timedelta(seconds=int(elapsed))):>12}
Failed:     [red]{self.stats['failed']:>6}[/red]  ETA:      {str(eta):>12}
Skipped:    [yellow]{self.stats['skipped']:>6}[/yellow]

[bold]Current:[/bold] {self.stats.get('current_file', 'Waiting...')[:60]}

[bold]Recent Activity:[/bold]
{recent_logs}"""
        
        return Panel(
            content,
            title="[bold]Processing Monitor[/bold]",
            box=box.ROUNDED,
            style=COLORS['box_primary']
        )
        
    def _get_level_color(self, level: str) -> str:
        """Get color for log level"""
        return {
            'DEBUG': 'dim',
            'INFO': 'white',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold red'
        }.get(level, 'white')
        
    def export_report(self, path: Path = None) -> Path:
        """Export detailed processing report"""
        if not path:
            path = self.output_dir / f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            
        with open(path, 'w') as f:
            f.write("PythonMetaMap Processing Report\n")
            f.write("=" * 50 + "\n\n")
            
            # Summary
            f.write("Summary\n")
            f.write("-" * 20 + "\n")
            f.write(f"Total Files: {self.stats['total']}\n")
            f.write(f"Processed: {self.stats['processed']}\n")
            f.write(f"Failed: {self.stats['failed']}\n")
            f.write(f"Skipped: {self.stats['skipped']}\n")
            
            if self.stats['start_time']:
                duration = datetime.now() - self.stats['start_time']
                f.write(f"Duration: {duration}\n")
                
            # Errors
            if self.stats['errors']:
                f.write("\n\nError Summary\n")
                f.write("-" * 20 + "\n")
                for error, count in sorted(self.stats['errors'].items(), key=lambda x: x[1], reverse=True):
                    f.write(f"{error}: {count}\n")
                    
            # Processing times
            if self.stats['processing_times']:
                f.write("\n\nPerformance Statistics\n")
                f.write("-" * 20 + "\n")
                times = self.stats['processing_times']
                f.write(f"Average: {sum(times) / len(times):.2f}s\n")
                f.write(f"Min: {min(times):.2f}s\n")
                f.write(f"Max: {max(times):.2f}s\n")
                
        return path


class AnalysisTools:
    """Advanced analysis tools for medical concepts"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.analyzer = ConceptAnalyzer(output_dir)
        self.clinical_analyzer = EnhancedConceptAnalyzer(output_dir)
        
    def concept_frequency_analysis(self) -> Table:
        """Analyze concept frequencies across all files"""
        concepts = defaultdict(int)
        semantic_types = defaultdict(int)
        sources = defaultdict(int)
        
        # Process all CSV files
        csv_files = list(self.output_dir.glob("*.csv"))
        
        with Progress() as progress:
            task = progress.add_task("Analyzing concepts...", total=len(csv_files))
            
            for csv_file in csv_files:
                try:
                    import pandas as pd
                    df = pd.read_csv(csv_file)
                    
                    for _, row in df.iterrows():
                        concept = row.get('Preferred_Name', '')
                        if concept:
                            concepts[concept] += 1
                            
                        sem_type = row.get('Semantic_Types', '')
                        if sem_type:
                            semantic_types[sem_type] += 1
                            
                        source = row.get('Sources', '')
                        if source:
                            sources[source] += 1
                            
                except Exception:
                    pass
                    
                progress.advance(task)
                
        # Create frequency table
        table = Table(title="Top Concepts by Frequency", box=box.ROUNDED)
        table.add_column("Rank", style="dim", width=6)
        table.add_column("Concept", style="cyan")
        table.add_column("Count", justify="right", style="green")
        table.add_column("Percentage", justify="right", style="yellow")
        
        total = sum(concepts.values())
        for i, (concept, count) in enumerate(sorted(concepts.items(), key=lambda x: x[1], reverse=True)[:20], 1):
            percentage = (count / total * 100) if total > 0 else 0
            table.add_row(
                str(i),
                concept[:50],
                str(count),
                f"{percentage:.1f}%"
            )
            
        return table
        
    def semantic_type_distribution(self) -> Panel:
        """Analyze semantic type distribution"""
        semantic_types = defaultdict(int)
        
        csv_files = list(self.output_dir.glob("*.csv"))
        for csv_file in csv_files:
            try:
                import pandas as pd
                df = pd.read_csv(csv_file)
                
                for sem_types in df['Semantic_Types'].dropna():
                    for sem_type in sem_types.split(','):
                        semantic_types[sem_type.strip()] += 1
                        
            except Exception:
                pass
                
        # Create visual distribution
        sorted_types = sorted(semantic_types.items(), key=lambda x: x[1], reverse=True)[:15]
        max_count = max(count for _, count in sorted_types) if sorted_types else 1
        
        content = ""
        for sem_type, count in sorted_types:
            bar_length = int(30 * count / max_count)
            bar = f"[cyan]{'█' * bar_length}[/cyan]"
            content += f"{sem_type:<30} {bar} {count:>6}\n"
            
        return Panel(
            content,
            title="[bold]Semantic Type Distribution[/bold]",
            box=box.ROUNDED,
            style=COLORS['box_secondary']
        )
        
    def generate_word_cloud(self) -> bool:
        """Generate word cloud from concepts"""
        try:
            from wordcloud import WordCloud
            import matplotlib.pyplot as plt
            
            # Collect all concepts
            text = ""
            csv_files = list(self.output_dir.glob("*.csv"))
            
            for csv_file in csv_files:
                try:
                    import pandas as pd
                    df = pd.read_csv(csv_file)
                    concepts = df['Preferred_Name'].dropna().tolist()
                    text += " ".join(concepts) + " "
                except Exception:
                    pass
                    
            if text:
                # Generate word cloud
                wordcloud = WordCloud(
                    width=800,
                    height=400,
                    background_color='white',
                    colormap='viridis'
                ).generate(text)
                
                # Save image
                output_path = self.output_dir / f"wordcloud_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                plt.figure(figsize=(10, 5))
                plt.imshow(wordcloud, interpolation='bilinear')
                plt.axis('off')
                plt.tight_layout()
                plt.savefig(output_path, dpi=300, bbox_inches='tight')
                plt.close()
                
                console.print(f"[green]Word cloud saved to: {output_path}[/green]")
                return True
                
        except ImportError:
            console.print("[yellow]Word cloud generation requires: pip install wordcloud matplotlib[/yellow]")
            
        return False
        
    def search_concepts(self, search_term: str) -> Table:
        """Search for specific concepts across files"""
        results = []
        search_lower = search_term.lower()
        
        csv_files = list(self.output_dir.glob("*.csv"))
        
        for csv_file in csv_files:
            try:
                import pandas as pd
                df = pd.read_csv(csv_file)
                
                # Search in multiple columns - handle different column name formats
                # Check which column names are present
                cui_col = 'CUI' if 'CUI' in df.columns else None
                concept_col = 'ConceptName' if 'ConceptName' in df.columns else 'Concept_Name' if 'Concept_Name' in df.columns else None
                pref_col = 'PrefName' if 'PrefName' in df.columns else 'Preferred_Name' if 'Preferred_Name' in df.columns else 'preferred_name' if 'preferred_name' in df.columns else None
                sem_col = 'SemTypes' if 'SemTypes' in df.columns else 'Semantic_Types' if 'Semantic_Types' in df.columns else 'semantic_types' if 'semantic_types' in df.columns else None
                
                if not all([cui_col, pref_col]):
                    continue
                    
                # Build search condition
                search_conditions = []
                if cui_col:
                    search_conditions.append(df[cui_col].str.lower().str.contains(search_lower, na=False))
                if concept_col:
                    search_conditions.append(df[concept_col].str.lower().str.contains(search_lower, na=False))
                if pref_col:
                    search_conditions.append(df[pref_col].str.lower().str.contains(search_lower, na=False))
                
                if search_conditions:
                    if len(search_conditions) == 1:
                        matches = df[search_conditions[0]]
                    else:
                        # Combine multiple conditions with OR
                        combined_condition = search_conditions[0]
                        for condition in search_conditions[1:]:
                            combined_condition = combined_condition | condition
                        matches = df[combined_condition]
                else:
                    continue
                
                for _, row in matches.iterrows():
                    results.append({
                        'file': csv_file.name,
                        'concept': row.get(pref_col, ''),
                        'cui': row.get(cui_col, ''),
                        'semantic_types': row.get(sem_col, '') if sem_col else '',
                        'score': row.get('Score', '')
                    })
                    
            except Exception:
                pass
                
        # Create results table
        table = Table(title=f"Search Results for '{search_term}'", box=box.ROUNDED)
        table.add_column("File", style="cyan")
        table.add_column("Concept", style="white")
        table.add_column("CUI", style="dim")
        table.add_column("Semantic Types", style="yellow")
        table.add_column("Score", justify="right", style="green")
        
        for result in results[:50]:  # Show first 50
            table.add_row(
                result['file'][:30],
                result['concept'][:40],
                result['cui'],
                str(result['semantic_types'])[:30],
                str(result['score'])
            )
            
        if len(results) > 50:
            table.add_row("", f"[dim]... and {len(results) - 50} more matches[/dim]", "", "", "")
            
        return table
        
    def co_occurrence_analysis(self) -> Panel:
        """Analyze concept co-occurrence patterns"""
        co_occurrences = defaultdict(lambda: defaultdict(int))
        
        csv_files = list(self.output_dir.glob("*.csv"))
        
        for csv_file in csv_files:
            try:
                import pandas as pd
                df = pd.read_csv(csv_file)
                
                # Get concepts per file
                concepts = df['Preferred_Name'].dropna().unique()
                
                # Count co-occurrences
                for i, concept1 in enumerate(concepts):
                    for concept2 in concepts[i+1:]:
                        if concept1 != concept2:
                            co_occurrences[concept1][concept2] += 1
                            co_occurrences[concept2][concept1] += 1
                            
            except Exception:
                pass
                
        # Find top co-occurring pairs
        pairs = []
        for concept1, related in co_occurrences.items():
            for concept2, count in related.items():
                if concept1 < concept2:  # Avoid duplicates
                    pairs.append((concept1, concept2, count))
                    
        pairs.sort(key=lambda x: x[2], reverse=True)
        
        # Create visualization
        content = "[bold]Top Co-occurring Concept Pairs:[/bold]\n\n"
        
        for concept1, concept2, count in pairs[:15]:
            bar_length = int(20 * count / pairs[0][2]) if pairs else 0
            bar = f"[green]{'█' * bar_length}[/green]"
            content += f"{concept1[:25]} ↔ {concept2[:25]}\n"
            content += f"  {bar} {count} files\n\n"
            
        return Panel(
            content,
            title="[bold]Concept Co-occurrence Analysis[/bold]",
            box=box.ROUNDED,
            style=COLORS['box_secondary']
        )


class UltimateInteractiveNavigator:
    """Ultimate interactive TUI with all features"""
    
    def __init__(self):
        self.config = PyMMConfig()
        self.server_manager = ServerManager(self.config)
        self.pool_manager = AdaptivePoolManager(self.config)
        self.resource_monitor = EnhancedResourceMonitor()
        self.background_processor = BackgroundProcessor(self.config)
        output_dir = self.config.get("pymm_output_dir", "output")
        self.state_manager = AtomicStateManager(Path(output_dir))
        self.running = True
        
        # Check requirements
        self._check_requirements()
        
        # Start monitoring
        self.resource_monitor.start()
        
        # Setup defaults
        self._setup_defaults()
        
    def _check_requirements(self):
        """Check system requirements"""
        if not self.server_manager.java_available:
            console.print("\n[bold red]Java Not Found[/bold red]")
            console.print("[yellow]MetaMap requires Java to run.[/yellow]\n")
            console.print("To install Java:")
            console.print("  Ubuntu/Debian: sudo apt-get install openjdk-11-jre-headless")
            console.print("  macOS: brew install openjdk@11")
            console.print("  Windows: Download from https://adoptium.net/\n")
            self.running = False
            
    def _setup_defaults(self):
        """Setup default configuration"""
        # Auto-detect system capabilities
        cpu_count = psutil.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # Smart defaults
        if not self.config.get('max_parallel_workers'):
            default_workers = min(max(cpu_count // 2, 2), 4)
            self.config.set('max_parallel_workers', default_workers)
            
        if not self.config.get('chunk_size'):
            if memory_gb < 8:
                chunk_size = 100
            elif memory_gb < 16:
                chunk_size = 250
            else:
                chunk_size = 500
            self.config.set('chunk_size', chunk_size)
            
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
        
    def main_menu(self):
        """Main menu with Claude Code style"""
        if not self.running:
            return
            
        while self.running:
            self.clear_screen()
            
            # Claude Code style header
            console.print(CLAUDE_BANNER)
            console.print()
            
            # Compact resource status
            console.print(self.resource_monitor.get_compact_status_panel())
            
            # Server status line
            server_status = "[green]● Running[/green]" if self.server_manager.is_running() else "[red]● Stopped[/red]"
            metamap_path = self.config.get('metamap_binary_path', 'Not configured')
            console.print(f"Server: {server_status}  MetaMap: {metamap_path[:50]}")
            console.print()
            
            # Menu grid with smaller boxes
            menu_grid = Table(show_header=False, box=None, padding=(0, 1))
            menu_grid.add_column()
            menu_grid.add_column()
            menu_grid.add_column()
            
            menu_items = [
                ("1", "Quick Process", COLORS['success']),
                ("2", "File Explorer", COLORS['info']),
                ("3", "Monitor", "cyan"),
                ("4", "Batch Process", COLORS['primary']),
                ("5", "View Results", COLORS['secondary']),
                ("6", "Analysis Tools", "magenta"),
                ("7", "Configuration", COLORS['warning']),
                ("8", "Server Control", COLORS['error']),
                ("9", "Resume/Retry", "yellow"),
                ("0", "Help", "dim"),
                ("Q", "Quit", "dim")
            ]
            
            # Create 3x4 grid
            for row in range(4):
                items = []
                for col in range(3):
                    idx = row * 3 + col
                    if idx < len(menu_items):
                        key, title, color = menu_items[idx]
                        panel = Panel(
                            f"[bold]{title}[/bold]",
                            title=f"[{color}]{key}[/{color}]",
                            box=box.ROUNDED,
                            style=color,
                            width=24,
                            height=3
                        )
                        items.append(panel)
                    else:
                        items.append("")
                        
                menu_grid.add_row(*items)
                
            console.print(menu_grid)
            console.rule(style="dim")
            
            # Prompt
            choice = Prompt.ask(
                "[bold]Select option[/bold]",
                default="1"
            ).lower()
            
            self._handle_menu_choice(choice)
            
    def _handle_menu_choice(self, choice: str):
        """Handle menu selection"""
        if choice == "1":
            self.quick_process()
        elif choice == "2":
            self.file_explorer()
        elif choice == "3":
            self.monitor()
        elif choice == "4":
            self.batch_process()
        elif choice == "5":
            self.view_results()
        elif choice == "6":
            self.analysis_tools()
        elif choice == "7":
            self.configuration()
        elif choice == "8":
            self.server_control()
        elif choice == "9":
            self.resume_retry()
        elif choice == "0":
            self.show_help()
        elif choice == "q":
            if Confirm.ask("[yellow]Exit PythonMetaMap?[/yellow]", default=False):
                self.running = False
                console.print("\n[green]Thank you for using PythonMetaMap![/green]")
                
    def quick_process(self):
        """Quick processing with visual feedback"""
        self.clear_screen()
        console.print(Panel(
            "[bold]Quick Process[/bold]\nProcess files with optimized settings",
            box=box.DOUBLE,
            style=COLORS['success']
        ))
        
        # Check server
        if not self.server_manager.is_running():
            console.print("\n[yellow]MetaMap server not running[/yellow]")
            if Confirm.ask("Start server now?", default=True):
                self._start_server_visual()
            else:
                input("\nPress Enter to continue...")
                return
                
        # Get directories
        console.print("\n[bold]Select Directories[/bold]")
        
        input_dir = Prompt.ask(
            "Input directory",
            default=self.config.get('default_input_dir', './input_notes')
        )
        
        output_dir = Prompt.ask(
            "Output directory",
            default=self.config.get('default_output_dir', './output_csvs')
        )
        
        # Validate and scan
        input_path = Path(input_dir)
        if not input_path.exists():
            console.print(f"\n[error]Directory not found: {input_dir}[/error]")
            input("\nPress Enter to continue...")
            return
            
        # Scan for files with progress
        console.print("\n[cyan]Scanning for files...[/cyan]")
        files = []
        
        with Progress() as progress:
            task = progress.add_task("Scanning...", total=None)
            
            for ext in ['*.txt', '*.text', '*.input']:
                for file in input_path.glob(ext):
                    files.append(file)
                    progress.update(task, description=f"Found {len(files)} files...")
                    
        if not files:
            console.print("\n[warning]No text files found[/warning]")
            input("\nPress Enter to continue...")
            return
            
        # Show summary
        total_size = sum(f.stat().st_size for f in files)
        
        summary_table = Table(title="File Summary", box=box.ROUNDED)
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green")
        
        summary_table.add_row("Total Files", str(len(files)))
        summary_table.add_row("Total Size", self._format_size(total_size))
        summary_table.add_row("Average Size", self._format_size(total_size // len(files) if files else 0))
        summary_table.add_row("Processing Mode", "Ultra-optimized" if len(files) > 500 else "Standard")
        
        console.print(summary_table)
        
        # Show configuration
        self._show_processing_config(len(files))
        
        # Background option
        if len(files) > 100:
            if Confirm.ask("\nRun in background?", default=False):
                job_id = self.background_processor.start_background_process(input_dir, output_dir)
                console.print(f"\n[green]Started background job: {job_id}[/green]")
                console.print("Monitor progress in Background Jobs menu")
                input("\nPress Enter to continue...")
                return
                
        if not Confirm.ask("\nProceed with processing?", default=True):
            return
            
        # Process with visual feedback
        self._run_processing_visual(input_dir, output_dir, files)
        
    def _start_server_visual(self):
        """Start server with visual progress"""
        stages = [
            ("Checking Java environment", 0.2),
            ("Initializing MetaMap resources", 0.3),
            ("Starting SKR/Tagger server", 0.3),
            ("Starting WSD server", 0.15),
            ("Verifying services", 0.05)
        ]
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Starting MetaMap server...", total=1.0)
            
            success = True
            for stage, weight in stages:
                progress.update(task, description=f"[cyan]{stage}...[/cyan]")
                
                # Start server on first stage
                if weight == 0.2:
                    server_thread = threading.Thread(target=lambda: setattr(self, '_server_result', self.server_manager.start()))
                    server_thread.start()
                    
                time.sleep(weight * 5)  # Simulate time
                progress.advance(task, weight)
                
                # Check result
                if weight == 0.2:
                    server_thread.join(timeout=10)
                    success = getattr(self, '_server_result', False)
                    if not success:
                        break
                        
        if success:
            console.print("\n[green]✓ Server started successfully![/green]")
        else:
            console.print("\n[error]✗ Failed to start server[/error]")
            
    def _show_processing_config(self, file_count: int):
        """Show processing configuration"""
        recommendations = self.pool_manager.analyze_system()
        
        config_panels = []
        
        # System panel
        system_content = f"""CPU Cores: {recommendations['system']['cpu_count']}
CPU Usage: {recommendations['system']['cpu_percent']:.1f}%
Memory: {recommendations['memory']['available_gb']:.1f}GB / {recommendations['memory']['total_gb']:.1f}GB
Disk Free: {recommendations['system']['disk_free_gb']:.1f}GB"""
        
        config_panels.append(Panel(
            system_content,
            title="[bold]System[/bold]",
            box=box.ROUNDED,
            style=COLORS['box_primary']
        ))
        
        # Settings panel
        current_workers = self.config.get('max_parallel_workers')
        optimal_workers = recommendations['workers']['optimal']
        
        settings_content = f"""Workers: {current_workers} (Optimal: {optimal_workers})
Chunk Size: {self.config.get('chunk_size')} files
Timeout: {self.config.get('pymm_timeout')}s
Mode: {'Ultra' if file_count > 500 else 'Standard'}"""
        
        config_panels.append(Panel(
            settings_content,
            title="[bold]Settings[/bold]",
            box=box.ROUNDED,
            style=COLORS['box_secondary']
        ))
        
        # Recommendations
        if recommendations['recommendations']:
            rec_content = "\n".join(f"• {rec}" for rec in recommendations['recommendations'])
            config_panels.append(Panel(
                rec_content,
                title="[bold]Recommendations[/bold]",
                box=box.ROUNDED,
                style=COLORS['warning']
            ))
            
        console.print(Columns(config_panels))
        
    def _run_processing_visual(self, input_dir: str, output_dir: str, files: List[Path]):
        """Run processing with rich visual feedback"""
        self.clear_screen()
        
        # Setup
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        monitor = ProcessingMonitor(output_path)
        monitor.stats['total'] = len(files)
        monitor.stats['start_time'] = datetime.now()
        
        # Choose runner
        if len(files) > 500:
            runner = UltraOptimizedBatchRunner(input_dir, output_dir, self.config)
            monitor.log("Using ultra-optimized runner for large dataset")
        else:
            runner = OptimizedBatchRunner(input_dir, output_dir, self.config)
            monitor.log("Using standard optimized runner")
            
        # Create live display
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=1)
        )
        
        # Header
        header = Panel(
            "[bold]Processing Medical Texts[/bold]",
            box=box.DOUBLE,
            style=COLORS['primary']
        )
        layout["header"].update(header)
        
        # Footer
        footer = Text("[dim]Press Ctrl+C to cancel[/dim]", justify="center")
        layout["footer"].update(footer)
        
        # Run with live display
        with Live(layout, refresh_per_second=2) as live:
            def update_display(stats):
                # Update monitor stats
                monitor.update_stats(**stats)
                
                if 'current_file' in stats:
                    monitor.log(f"Processing: {stats['current_file']}", file=stats['current_file'])
                    
                # Update main display
                main_layout = Layout()
                main_layout.split_row(
                    Layout(monitor.get_progress_panel(), ratio=2),
                    Layout(self.resource_monitor.get_compact_status_panel(), ratio=1)
                )
                
                layout["main"].update(main_layout)
                
            try:
                results = runner.run(progress_callback=update_display)
                
                # Final update
                monitor.log("Processing completed", "INFO")
                
                # Show results
                self._show_processing_results(results, output_dir, monitor)
                
            except KeyboardInterrupt:
                monitor.log("Processing interrupted by user", "WARNING")
                console.print("\n\n[yellow]Processing interrupted[/yellow]")
            except Exception as e:
                monitor.log(f"Processing error: {e}", "ERROR")
                console.print(f"\n\n[error]Processing error: {e}[/error]")
                
        # Export report
        report_path = monitor.export_report()
        console.print(f"\n[dim]Report saved to: {report_path}[/dim]")
        
        input("\nPress Enter to continue...")
        
    def _show_processing_results(self, results: Dict, output_dir: str, monitor: ProcessingMonitor):
        """Show detailed processing results"""
        self.clear_screen()
        
        # Header
        if results.get('successful', 0) > 0:
            header_style = COLORS['success']
            status = "✓ Processing Complete"
        else:
            header_style = COLORS['error']
            status = "✗ Processing Failed"
            
        console.print(Panel(
            f"[bold]{status}[/bold]",
            box=box.DOUBLE,
            style=header_style
        ))
        
        # Results grid
        results_panels = []
        
        # Summary panel
        duration = results.get('duration', 0)
        throughput = results.get('throughput', 0)
        
        summary_content = f"""Total Files: {results.get('total', 0)}
Successful: [green]{results.get('successful', 0)}[/green]
Failed: [red]{results.get('failed', 0)}[/red]
Skipped: [yellow]{results.get('skipped', 0)}[/yellow]

Duration: {duration:.1f}s
Throughput: {throughput:.2f} files/s"""
        
        results_panels.append(Panel(
            summary_content,
            title="[bold]Summary[/bold]",
            box=box.ROUNDED,
            style=COLORS['box_primary']
        ))
        
        # Error summary if any
        if monitor.stats['errors']:
            error_content = ""
            for error, count in sorted(monitor.stats['errors'].items(), key=lambda x: x[1], reverse=True)[:5]:
                error_content += f"[red]{error[:40]}[/red]: {count}\n"
                
            results_panels.append(Panel(
                error_content.strip(),
                title="[bold]Top Errors[/bold]",
                box=box.ROUNDED,
                style=COLORS['error']
            ))
            
        console.print(Columns(results_panels))
        
        # Sample results
        if results.get('successful', 0) > 0:
            self._show_sample_results(Path(output_dir))
            
    def _show_sample_results(self, output_dir: Path):
        """Show sample of processed results"""
        csv_files = list(output_dir.glob("*.csv"))[:3]
        
        if csv_files:
            console.print("\n[bold]Sample Results:[/bold]")
            
            for csv_file in csv_files:
                try:
                    import pandas as pd
                    df = pd.read_csv(csv_file)
                    
                    # Show file info
                    console.print(f"\n[cyan]{csv_file.name}[/cyan] ({len(df)} concepts)")
                    
                    # Show top concepts
                    if not df.empty:
                        sample = df.head(3)
                        for _, row in sample.iterrows():
                            console.print(f"  • {row.get('Preferred_Name', 'Unknown')[:50]} [{row.get('Semantic_Types', '')[:30]}]")
                            
                except Exception:
                    pass
                    
    def file_explorer(self):
        """Launch advanced file explorer"""
        self.clear_screen()
        
        explorer = AdvancedFileExplorer()
        selected_files = explorer.navigate()
        
        if selected_files:
            console.print(f"\n[bold]Selected {len(selected_files)} files[/bold]")
            
            # Show selected files
            table = Table(box=box.SIMPLE)
            table.add_column("File", style="cyan")
            table.add_column("Size", justify="right")
            
            total_size = 0
            for file in selected_files[:10]:
                size = file.stat().st_size
                total_size += size
                table.add_row(file.name, self._format_size(size))
                
            if len(selected_files) > 10:
                table.add_row(f"... and {len(selected_files) - 10} more", "")
                
            console.print(table)
            console.print(f"\nTotal size: {self._format_size(total_size)}")
            
            if Confirm.ask("\nProcess selected files?", default=True):
                output_dir = Prompt.ask(
                    "Output directory",
                    default=self.config.get('default_output_dir', './output_csvs')
                )
                
                # Create temporary directory with selected files
                temp_dir = Path(tempfile.mkdtemp(prefix="pymm_selected_"))
                
                try:
                    for file in selected_files:
                        shutil.copy2(file, temp_dir / file.name)
                        
                    self._run_processing_visual(str(temp_dir), output_dir, selected_files)
                    
                finally:
                    # Cleanup
                    shutil.rmtree(temp_dir)
                    
    def _run_ultra_processing(self, input_dir: str, output_dir: str):
        """Run ultra-optimized processing"""
        try:
            from ..processing.ultra_optimized_runner import UltraOptimizedBatchRunner
            
            console.print("\n[bold cyan]Starting Ultra Processing...[/bold cyan]")
            
            runner = UltraOptimizedBatchRunner(input_dir, output_dir, self.config)
            
            # UltraOptimizedBatchRunner has its own progress display and doesn't support callbacks
            results = runner.run()
                
            console.print(f"\n[green]✓ Ultra processing complete![/green]")
            console.print(f"Processed: {results.get('processed', 0)} files")
            console.print(f"Failed: {results.get('failed', 0)} files")
            
        except ImportError:
            console.print("[yellow]Ultra optimizer not available, using optimized processing[/yellow]")
            self._run_optimized_processing_fallback(input_dir, output_dir, 100)
        except Exception as e:
            console.print(f"[red]Ultra processing error: {e}[/red]")
            console.print("[yellow]Falling back to optimized processing[/yellow]")
            self._run_optimized_processing_fallback(input_dir, output_dir, 100)
            
    def _run_chunked_processing(self, input_dir: str, output_dir: str, chunk_size: int):
        """Run memory-efficient chunked processing"""
        try:
            from ..processing.chunked_batch_runner import ChunkedBatchRunner
            
            console.print(f"\n[bold cyan]Starting Chunked Processing (chunks of {chunk_size})...[/bold cyan]")
            
            # Set chunk size in config before creating runner
            old_chunk_size = self.config.get('chunk_size', 500)
            self.config.set('chunk_size', chunk_size)
            
            runner = ChunkedBatchRunner(input_dir, output_dir, self.config)
            
            # ChunkedBatchRunner doesn't support progress callbacks, so just run it
            results = runner.run()
                
            # Restore original chunk size
            self.config.set('chunk_size', old_chunk_size)
                
            console.print(f"\n[green]✓ Chunked processing complete![/green]")
            console.print(f"Processed: {results.get('processed', 0)} files")
            console.print(f"Failed: {results.get('failed', 0)} files")
            console.print(f"Chunks: {results.get('chunks', 0)}")
            
        except ImportError:
            console.print("[yellow]Chunked processor not available, using optimized processing[/yellow]")
            self._run_optimized_processing_fallback(input_dir, output_dir, chunk_size)
        except Exception as e:
            console.print(f"[red]Chunked processing error: {e}[/red]")
            console.print("[yellow]Falling back to optimized processing[/yellow]")
            self._run_optimized_processing_fallback(input_dir, output_dir, chunk_size)
        finally:
            # Always restore original chunk size
            if 'old_chunk_size' in locals():
                self.config.set('chunk_size', old_chunk_size)
            
    def _run_optimized_processing_fallback(self, input_dir: str, output_dir: str, chunk_size: int):
        """Fallback chunked processing using OptimizedBatchRunner"""
        try:
            from ..processing.optimized_batch_runner import OptimizedBatchRunner
            
            console.print(f"\n[bold cyan]Starting Optimized Processing...[/bold cyan]")
            
            # OptimizedBatchRunner manages its own chunk size dynamically
            runner = OptimizedBatchRunner(input_dir, output_dir, self.config)
            
            # Just run it - it has its own progress display
            results = runner.run()
                
            console.print(f"\n[green]✓ Optimized processing complete![/green]")
            console.print(f"Processed: {results.get('processed', 0)} files")
            console.print(f"Failed: {results.get('failed', 0)} files")
            
        except ImportError:
            console.print("[yellow]Optimized processor not available, using standard processing[/yellow]")
            self._run_standard_processing(input_dir, output_dir)
        except Exception as e:
            console.print(f"[red]Optimized processing error: {e}[/red]")
            self._run_standard_processing(input_dir, output_dir)
            
    def _run_standard_processing(self, input_dir: str, output_dir: str):
        """Fallback to standard processing"""
        try:
            from ..processing.batch_runner import BatchRunner
            
            console.print("\n[bold cyan]Starting Standard Processing...[/bold cyan]")
            
            runner = BatchRunner(input_dir, output_dir, self.config)
            
            # BatchRunner has its own progress display
            results = runner.run()
            
            console.print(f"\n[green]✓ Standard processing complete![/green]")
            console.print(f"Processed: {results.get('processed', 0)} files")
            console.print(f"Failed: {results.get('failed', 0)} files")
            
        except Exception as e:
            console.print(f"\n[red]Standard processing error: {e}[/red]")
            console.print("[red]All processing methods failed. Please check your configuration.[/red]")
            input("\nPress Enter to continue...")
                    
    def batch_process(self):
        """Advanced batch processing with full control"""
        self.clear_screen()
        console.print(Panel(
            "[bold]Batch Processing[/bold]\nFull control over MetaMap processing",
            box=box.DOUBLE,
            style=COLORS['success']
        ))
        
        # Check server
        if not self.server_manager.is_running():
            console.print("\n[yellow]MetaMap server not running[/yellow]")
            if Confirm.ask("Start server now?", default=True):
                self._start_server_visual()
            else:
                input("\nPress Enter to continue...")
                return
                
        # Get directories
        console.print("\n[bold]Processing Configuration[/bold]")
        
        input_dir = Prompt.ask(
            "Input directory",
            default=self.config.get('default_input_dir', './input_notes')
        )
        
        output_dir = Prompt.ask(
            "Output directory", 
            default=self.config.get('default_output_dir', './output_csvs')
        )
        
        # Advanced options
        console.print("\n[bold]Advanced Options[/bold]")
        
        workers = IntPrompt.ask(
            "Number of workers",
            default=self.config.get('max_parallel_workers', 4)
        )
        
        timeout = IntPrompt.ask(
            "Timeout per file (seconds)",
            default=self.config.get('pymm_timeout', 300)
        )
        
        chunk_size = IntPrompt.ask(
            "Files per chunk",
            default=100
        )
        
        use_pool = Confirm.ask(
            "Use instance pooling?",
            default=self.config.get('use_instance_pool', True)
        )
        
        # Ask about background processing
        run_background = Confirm.ask(
            "\nRun in background? (allows you to close terminal)",
            default=False
        )
        
        # Get list of files to process (needed for auto-select)
        input_path = Path(input_dir)
        files = list(input_path.glob("*.txt")) + list(input_path.glob("*.TXT"))
        
        if not files:
            console.print("[yellow]No text files found in input directory[/yellow]")
            input("\nPress Enter to continue...")
            return
        
        # Processing options with descriptions
        console.print("\n[bold]Processing Mode Selection[/bold]")
        console.print(Panel(
            "[bold cyan]Standard Mode (OptimizedBatchRunner)[/bold cyan]\n"
            "[green]✓[/green] Best for: 10-500 files\n"
            "[green]✓[/green] Balanced performance and memory usage\n"
            "[green]✓[/green] Smart retry handling\n"
            "[dim]Recommended for most use cases[/dim]",
            title="[1]",
            box=box.ROUNDED
        ))
        
        console.print(Panel(
            "[bold cyan]Ultra Mode (UltraOptimizedBatchRunner)[/bold cyan]\n"
            "[green]✓[/green] Best for: 500-5000 files\n"
            "[green]✓[/green] Advanced worker management\n"
            "[green]✓[/green] Health monitoring & auto-recovery\n"
            "[green]✓[/green] Adaptive timeout adjustment\n"
            "[dim]For large datasets with mixed file sizes[/dim]",
            title="[2]",
            box=box.ROUNDED
        ))
        
        console.print(Panel(
            "[bold cyan]Memory-Efficient Mode (Chunked)[/bold cyan]\n"
            "[green]✓[/green] Best for: 5000+ files\n"
            "[green]✓[/green] Processes files in small batches\n"
            "[green]✓[/green] Minimal memory footprint\n"
            "[green]✓[/green] Prevents OOM errors\n"
            "[dim]For very large datasets or limited RAM[/dim]",
            title="[3]",
            box=box.ROUNDED
        ))
        
        console.print(Panel(
            "[bold cyan]Auto-Select (Recommended)[/bold cyan]\n"
            "[green]✓[/green] Analyzes your dataset\n"
            "[green]✓[/green] Checks available memory\n"
            "[green]✓[/green] Picks optimal mode\n"
            "[dim]Let PyMM choose the best mode for you[/dim]",
            title="[A]",
            box=box.DOUBLE
        ))
        
        mode = Prompt.ask("\nSelect mode", choices=["1", "2", "3", "a", "A"], default="a").lower()
        
        # Auto-select mode based on file count and memory
        if mode == "a":
            file_count = len(files)
            available_memory = psutil.virtual_memory().available / (1024**3)  # GB
            
            console.print(f"\n[cyan]Analyzing: {file_count} files, {available_memory:.1f}GB available RAM[/cyan]")
            
            if file_count <= 100 and available_memory >= 4:
                mode = "1"
                console.print("[green]→ Selected: Standard Mode (optimal for small datasets)[/green]")
            elif file_count <= 1000 and available_memory >= 8:
                mode = "2"
                console.print("[green]→ Selected: Ultra Mode (optimal for medium datasets)[/green]")
            elif file_count > 5000 or available_memory < 4:
                mode = "3"
                console.print("[green]→ Selected: Memory-Efficient Mode (optimal for large datasets)[/green]")
            elif file_count > 1000:
                mode = "3"
                console.print("[green]→ Selected: Memory-Efficient Mode (safer for 1000+ files)[/green]")
            else:
                mode = "2"
                console.print("[green]→ Selected: Ultra Mode (best performance/memory balance)[/green]")
        
        # Start processing
        if Confirm.ask("\nStart processing?", default=True):
            # Update config temporarily
            old_workers = self.config.get('max_parallel_workers')
            old_timeout = self.config.get('pymm_timeout')
            old_pool = self.config.get('use_instance_pool')
            
            self.config.set('max_parallel_workers', workers)
            self.config.set('pymm_timeout', timeout)
            self.config.set('use_instance_pool', use_pool)
            
            try:
                if run_background:
                    # Run in background mode
                    console.print("\n[yellow]Starting background processing...[/yellow]")
                    
                    # Import job manager
                    from ..core.job_manager import get_job_manager, JobType
                    job_manager = get_job_manager()
                    
                    # Determine job type based on mode
                    job_type_map = {
                        "1": JobType.OPTIMIZED,
                        "2": JobType.ULTRA,
                        "3": JobType.CHUNKED
                    }
                    job_type = job_type_map.get(mode, JobType.BATCH)
                    
                    # Create job
                    job_id = job_manager.create_job(
                        job_type=job_type,
                        input_dir=input_dir,
                        output_dir=output_dir,
                        config={
                            'workers': workers,
                            'timeout': timeout,
                            'chunk_size': chunk_size,
                            'use_pool': use_pool
                        }
                    )
                    
                    # Start background process
                    cmd = [
                        sys.executable, "-m", "pymm", "process",
                        input_dir, output_dir,
                        "--background",
                        "--job-id", job_id
                    ]
                    
                    log_file = Path(output_dir) / "logs" / f"{job_id}.log"
                    log_file.parent.mkdir(exist_ok=True)
                    
                    with open(log_file, 'w') as f:
                        process = subprocess.Popen(
                            cmd,
                            stdout=f,
                            stderr=subprocess.STDOUT,
                            text=True
                        )
                    
                    # Update job with PID
                    job_manager.start_job(job_id, process.pid)
                    
                    console.print(f"\n[green]✓ Background job started with ID: {job_id}[/green]")
                    console.print("[cyan]Monitor progress in the Job Monitor (option 7)[/cyan]")
                    console.print("[cyan]Or use: pymm monitor[/cyan]")
                else:
                    # Run in foreground mode
                    if mode == "1":
                        self._run_processing_visual(input_dir, output_dir, files)
                    elif mode == "2":
                        self._run_ultra_processing(input_dir, output_dir)
                    elif mode == "3":
                        self._run_chunked_processing(input_dir, output_dir, chunk_size)
            finally:
                # Restore config
                self.config.set('max_parallel_workers', old_workers)
                self.config.set('pymm_timeout', old_timeout)
                self.config.set('use_instance_pool', old_pool)
                
        input("\nPress Enter to continue...")
                    
    def view_results(self):
        """View and analyze results"""
        self.clear_screen()
        console.print(Panel(
            "[bold]View Results[/bold]\nAnalyze processed medical concepts",
            box=box.DOUBLE,
            style=COLORS['secondary']
        ))
        
        output_dir = Prompt.ask(
            "\nOutput directory",
            default=self.config.get('default_output_dir', './output_csvs')
        )
        
        output_path = Path(output_dir)
        if not output_path.exists():
            console.print(f"\n[error]Directory not found: {output_dir}[/error]")
            input("\nPress Enter to continue...")
            return
            
        # Count files
        csv_files = list(output_path.glob("*.csv"))
        if not csv_files:
            console.print("\n[warning]No CSV files found[/warning]")
            input("\nPress Enter to continue...")
            return
            
        # Show summary
        total_concepts = 0
        total_size = 0
        
        with Progress() as progress:
            task = progress.add_task("Analyzing results...", total=len(csv_files))
            
            for csv_file in csv_files:
                try:
                    size = csv_file.stat().st_size
                    total_size += size
                    
                    import pandas as pd
                    df = pd.read_csv(csv_file)
                    total_concepts += len(df)
                    
                except Exception:
                    pass
                    
                progress.advance(task)
                
        # Summary panels
        summary_panels = []
        
        # Files panel
        files_content = f"""Total Files: {len(csv_files)}
Total Size: {self._format_size(total_size)}
Average Size: {self._format_size(total_size // len(csv_files) if csv_files else 0)}"""
        
        summary_panels.append(Panel(
            files_content,
            title="[bold]Files[/bold]",
            box=box.ROUNDED,
            style=COLORS['box_primary']
        ))
        
        # Concepts panel
        concepts_content = f"""Total Concepts: {total_concepts:,}
Average per File: {total_concepts // len(csv_files) if csv_files else 0:,}
Output Directory: {output_path}"""
        
        summary_panels.append(Panel(
            concepts_content,
            title="[bold]Concepts[/bold]",
            box=box.ROUNDED,
            style=COLORS['box_secondary']
        ))
        
        console.print(Columns(summary_panels))
        
        # Recent files
        recent_files = sorted(csv_files, key=lambda f: f.stat().st_mtime, reverse=True)[:10]
        
        recent_table = Table(title="Recent Files", box=box.ROUNDED)
        recent_table.add_column("File", style="cyan")
        recent_table.add_column("Modified", style="dim")
        recent_table.add_column("Size", justify="right")
        recent_table.add_column("Concepts", justify="right", style="green")
        
        for csv_file in recent_files:
            try:
                import pandas as pd
                df = pd.read_csv(csv_file)
                concepts = len(df)
            except:
                concepts = "?"
                
            mtime = datetime.fromtimestamp(csv_file.stat().st_mtime)
            recent_table.add_row(
                csv_file.name[:40],
                mtime.strftime("%Y-%m-%d %H:%M"),
                self._format_size(csv_file.stat().st_size),
                str(concepts)
            )
            
        console.print("\n", recent_table)
        
        # Options
        console.print("\n[bold]Actions:[/bold]")
        console.print("[1] View specific file")
        console.print("[2] Search concepts")
        console.print("[3] Export summary report")
        console.print("[4] Open in file explorer")
        console.print("[B] Back")
        
        choice = Prompt.ask("Select action", choices=["1", "2", "3", "4", "b"], default="b").lower()
        
        if choice == "1":
            self._view_specific_file(output_path)
        elif choice == "2":
            term = Prompt.ask("Search term")
            analyzer = AnalysisTools(output_path)
            console.print(analyzer.search_concepts(term))
            input("\nPress Enter to continue...")
        elif choice == "3":
            self._export_summary_report(output_path)
        elif choice == "4":
            self._open_file_explorer(output_path)
                
    def _open_file_explorer(self, path: Path):
        """Open file explorer with WSL support"""
        try:
            if sys.platform == "win32":
                os.startfile(path)
            elif sys.platform == "darwin":
                subprocess.run(["open", path])
            else:
                # Check if we're running in WSL
                if 'microsoft' in platform.uname().release.lower() or 'WSL' in os.environ.get('WSL_DISTRO_NAME', ''):
                    # We're in WSL, use Windows explorer
                    # Convert WSL path to Windows path
                    windows_path = subprocess.run(['wslpath', '-w', str(path)], 
                                                capture_output=True, text=True).stdout.strip()
                    subprocess.run(['explorer.exe', windows_path])
                else:
                    # Regular Linux, use xdg-open
                    subprocess.run(["xdg-open", path])
        except Exception as e:
            console.print(f"[yellow]Could not open file explorer: {e}[/yellow]")
            console.print(f"[dim]Path: {path}[/dim]")
    
    def _view_specific_file(self, output_dir: Path):
        """View specific CSV file"""
        csv_files = sorted(output_dir.glob("*.csv"))
        
        # Show file list
        console.print("\n[bold]Select file:[/bold]")
        for i, csv_file in enumerate(csv_files[:20], 1):
            console.print(f"[{i:2d}] {csv_file.name}")
            
        if len(csv_files) > 20:
            console.print(f"[dim]... and {len(csv_files) - 20} more files[/dim]")
            
        try:
            choice = IntPrompt.ask("File number", default=1)
            if 1 <= choice <= len(csv_files):
                self._display_csv_details(csv_files[choice - 1])
        except:
            pass
            
    def _display_csv_details(self, csv_file: Path):
        """Display detailed CSV content"""
        self.clear_screen()
        
        try:
            import pandas as pd
            df = pd.read_csv(csv_file)
            
            console.print(Panel(
                f"[bold]{csv_file.name}[/bold]\n{len(df)} concepts extracted",
                box=box.DOUBLE,
                style=COLORS['info']
            ))
            
            # Show columns
            console.print("\n[bold]Columns:[/bold]")
            for col in df.columns:
                console.print(f"  • {col}")
                
            # Show sample data
            console.print("\n[bold]Sample Data:[/bold]")
            
            table = Table(box=box.ROUNDED)
            
            # Add first 5 columns
            for col in df.columns[:5]:
                table.add_column(col, overflow="fold")
                
            # Add rows
            for _, row in df.head(15).iterrows():
                table.add_row(*[str(row[col])[:30] for col in df.columns[:5]])
                
            console.print(table)
            
            # Statistics
            if 'Score' in df.columns:
                console.print(f"\n[bold]Score Statistics:[/bold]")
                console.print(f"  Mean: {df['Score'].mean():.2f}")
                console.print(f"  Min: {df['Score'].min():.2f}")
                console.print(f"  Max: {df['Score'].max():.2f}")
                
            # Semantic types distribution
            if 'Semantic_Types' in df.columns:
                console.print("\n[bold]Top Semantic Types:[/bold]")
                sem_types = df['Semantic_Types'].dropna().str.split(',').explode().str.strip()
                for sem_type, count in sem_types.value_counts().head(10).items():
                    console.print(f"  • {sem_type}: {count}")
                    
        except Exception as e:
            console.print(f"[error]Error reading file: {e}[/error]")
            
        input("\nPress Enter to continue...")
        
    def _export_summary_report(self, output_dir: Path):
        """Export comprehensive summary report"""
        console.print("\n[cyan]Generating summary report...[/cyan]")
        
        report_path = output_dir / f"summary_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_path, 'w') as f:
            f.write("PythonMetaMap Processing Summary Report\n")
            f.write("=" * 60 + "\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write(f"Output Directory: {output_dir}\n\n")
            
            # File summary
            csv_files = list(output_dir.glob("*.csv"))
            f.write(f"Total Files: {len(csv_files)}\n")
            
            # Concept summary
            total_concepts = 0
            for csv_file in csv_files:
                try:
                    import pandas as pd
                    df = pd.read_csv(csv_file)
                    total_concepts += len(df)
                except:
                    pass
                    
            f.write(f"Total Concepts: {total_concepts:,}\n")
            f.write(f"Average Concepts per File: {total_concepts // len(csv_files) if csv_files else 0:,}\n")
            
        console.print(f"[green]Report saved to: {report_path}[/green]")
        input("\nPress Enter to continue...")
        
    def analysis_tools(self):
        """Advanced analysis tools menu"""
        self.clear_screen()
        console.print(Panel(
            "[bold]Analysis Tools[/bold]\nAdvanced medical concept analysis",
            box=box.DOUBLE,
            style="magenta"
        ))
        
        output_dir = Prompt.ask(
            "\nOutput directory with CSV files",
            default=self.config.get('default_output_dir', './output_csvs')
        )
        
        output_path = Path(output_dir)
        if not output_path.exists() or not list(output_path.glob("*.csv")):
            console.print("\n[warning]No CSV files found in directory[/warning]")
            input("\nPress Enter to continue...")
            return
            
        analyzer = AnalysisTools(output_path)
        
        while True:
            self.clear_screen()
            console.print(Panel(
                "[bold]Analysis Tools[/bold]",
                box=box.DOUBLE,
                style="magenta"
            ))
            
            # Menu
            console.print("\n[1] Concept Frequency Analysis")
            console.print("[2] Semantic Type Distribution")
            console.print("[3] Concept Co-occurrence Analysis")
            console.print("[4] Search Concepts")
            console.print("[5] Generate Word Cloud")
            console.print("[6] Template-based Analysis")
            console.print("[7] Clinical Analysis (Note Types)")
            console.print("[8] Export Analysis Report")
            console.print("[B] Back to main menu")
            
            choice = Prompt.ask("\nSelect tool", default="b").lower()
            
            if choice == "b":
                break
            elif choice == "1":
                console.print("\n[cyan]Analyzing concept frequencies...[/cyan]\n")
                table = analyzer.concept_frequency_analysis()
                console.print(table)
            elif choice == "2":
                panel = analyzer.semantic_type_distribution()
                console.print("\n", panel)
            elif choice == "3":
                panel = analyzer.co_occurrence_analysis()
                console.print("\n", panel)
            elif choice == "4":
                term = Prompt.ask("Search term")
                table = analyzer.search_concepts(term)
                console.print("\n", table)
            elif choice == "5":
                console.print("\n[cyan]Generating word cloud...[/cyan]")
                if analyzer.generate_word_cloud():
                    console.print("[green]Word cloud generated successfully![/green]")
                else:
                    console.print("[red]Failed to generate word cloud[/red]")
            elif choice == "6":
                self._template_based_analysis(analyzer, output_path)
            elif choice == "7":
                self._clinical_analysis(output_path)
            elif choice == "8":
                self._export_analysis_report(analyzer, output_path)
                
            if choice != "b":
                input("\nPress Enter to continue...")
                
    def _template_based_analysis(self, analyzer: AnalysisTools, output_dir: Path):
        """Perform template-based analysis"""
        self.clear_screen()
        console.print(Panel(
            "[bold]Template-based Analysis[/bold]\nPre-configured analysis templates for common use cases",
            box=box.DOUBLE,
            style="cyan"
        ))
        
        # Show available templates
        console.print("\n[bold]Available Templates:[/bold]\n")
        
        template_table = Table(box=box.ROUNDED)
        template_table.add_column("ID", style="cyan", width=3)
        template_table.add_column("Template", style="green", width=25)
        template_table.add_column("Description", style="white", width=50)
        
        template_ids = []
        for i, (template_id, template) in enumerate(ANALYSIS_TEMPLATES.items(), 1):
            template_table.add_row(
                str(i),
                template['name'],
                template['description']
            )
            template_ids.append(template_id)
        
        console.print(template_table)
        
        # Select template
        choice = IntPrompt.ask("\nSelect template (0 to cancel)", default=0)
        
        if choice == 0 or choice > len(template_ids):
            return
            
        selected_template_id = template_ids[choice - 1]
        selected_template = ANALYSIS_TEMPLATES[selected_template_id]
        
        console.print(f"\n[cyan]Running {selected_template['name']}...[/cyan]\n")
        
        # Perform filtered analysis based on template
        results = self._run_template_analysis(analyzer, selected_template)
        
        # Display results
        console.print(Panel(
            f"[bold]{selected_template['name']} Results[/bold]",
            box=box.DOUBLE,
            style="green"
        ))
        
        # Show filtered concepts
        if 'concepts' in results:
            console.print("\n[bold]Relevant Concepts Found:[/bold]")
            concept_table = Table(box=box.ROUNDED)
            concept_table.add_column("Concept", style="cyan")
            concept_table.add_column("Count", style="yellow")
            concept_table.add_column("Semantic Type", style="dim")
            
            for concept_info in results['concepts'][:20]:
                concept_table.add_row(
                    concept_info['name'],
                    str(concept_info['count']),
                    concept_info['semantic_type']
                )
            
            console.print(concept_table)
        
        # Generate template-specific report
        self._export_template_report(results, selected_template, output_dir)
    
    def _run_template_analysis(self, analyzer: AnalysisTools, template: Dict) -> Dict:
        """Run analysis based on template configuration"""
        results = {
            'template_name': template['name'],
            'timestamp': datetime.now(),
            'concepts': [],
            'statistics': {}
        }
        
        # Analyze all CSV files with template filters
        csv_files = list(analyzer.output_dir.glob("*.csv"))
        filtered_concepts = defaultdict(lambda: {'count': 0, 'semantic_types': set()})
        
        for csv_file in csv_files:
            try:
                import pandas as pd
                df = pd.read_csv(csv_file)
                
                # Get column names
                cui_col = 'CUI' if 'CUI' in df.columns else None
                pref_col = 'PrefName' if 'PrefName' in df.columns else 'Preferred_Name' if 'Preferred_Name' in df.columns else None
                sem_col = 'SemTypes' if 'SemTypes' in df.columns else 'Semantic_Types' if 'Semantic_Types' in df.columns else None
                
                if not cui_col or not pref_col:
                    continue
                
                for _, row in df.iterrows():
                    concept_name = row.get(pref_col, '').lower()
                    semantic_types = row.get(sem_col, '') if sem_col else ''
                    
                    # Check if concept matches template filters
                    matches_filter = any(filter_term in concept_name for filter_term in template['filters'])
                    
                    # Check semantic type if specified
                    if template.get('semantic_types') and semantic_types:
                        sem_types_list = [st.strip() for st in str(semantic_types).strip('[]').split(',')]
                        matches_semantic = any(st in template['semantic_types'] for st in sem_types_list)
                    else:
                        matches_semantic = True
                    
                    if matches_filter or (template.get('semantic_types') and matches_semantic):
                        cui = row.get(cui_col, '')
                        key = f"{row.get(pref_col, '')} ({cui})"
                        filtered_concepts[key]['count'] += 1
                        if semantic_types:
                            filtered_concepts[key]['semantic_types'].add(str(semantic_types))
                            
            except Exception as e:
                console.print(f"[yellow]Warning: Error processing {csv_file.name}: {e}[/yellow]")
        
        # Convert to results format
        for concept, info in sorted(filtered_concepts.items(), key=lambda x: x[1]['count'], reverse=True):
            results['concepts'].append({
                'name': concept,
                'count': info['count'],
                'semantic_type': ', '.join(list(info['semantic_types'])[:2])  # First 2 semantic types
            })
        
        results['statistics'] = {
            'total_files': len(csv_files),
            'total_relevant_concepts': len(filtered_concepts),
            'total_occurrences': sum(info['count'] for info in filtered_concepts.values())
        }
        
        return results
    
    def _export_template_report(self, results: Dict, template: Dict, output_dir: Path):
        """Export template-based analysis report"""
        report_name = f"{template['name'].lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        report_path = output_dir / report_name
        
        with open(report_path, 'w') as f:
            f.write(f"# {template['name']} Report\n\n")
            f.write(f"**Generated:** {results['timestamp']}\n\n")
            f.write(f"**Description:** {template['description']}\n\n")
            
            f.write("## Summary Statistics\n\n")
            for key, value in results['statistics'].items():
                f.write(f"- **{key.replace('_', ' ').title()}:** {value}\n")
            
            f.write("\n## Top Concepts\n\n")
            f.write("| Concept | Count | Semantic Type |\n")
            f.write("|---------|-------|---------------|\n")
            
            for concept in results['concepts'][:30]:
                f.write(f"| {concept['name']} | {concept['count']} | {concept['semantic_type']} |\n")
            
            if template.get('report_sections'):
                f.write("\n## Analysis Sections\n\n")
                for section in template['report_sections']:
                    f.write(f"### {section}\n\n")
                    f.write(f"*Section analysis would be populated here based on specific {section.lower()} concepts*\n\n")
        
        console.print(f"\n[green]✓ Report saved to: {report_path}[/green]")
    
    def _clinical_analysis(self, output_dir: Path):
        """Perform clinical analysis"""
        console.print("\n[cyan]Performing clinical analysis...[/cyan]")
        
        # Create analyzer instance
        analyzer = AnalysisTools(output_dir)
        
        # Select analysis preset
        presets = {
            "1": ("General Clinical", None),
            "2": ("Kidney Stone Comprehensive", "kidney_stone_comprehensive"),
            "3": ("Stone Procedures - Removal", "stone_procedures_removal"),
            "4": ("Stone Procedures - Drainage", "stone_procedures_drainage"),
            "5": ("Stone Characteristics", "stone_characteristics"),
            "6": ("Treatment Outcomes", "stone_outcomes")
        }
        
        console.print("\n[bold]Select Analysis Preset:[/bold]")
        for key, (name, _) in presets.items():
            console.print(f"[{key}] {name}")
        
        choice = console.input("\nSelect preset (1-6) [1]: ").strip() or "1"
        preset_name, preset = presets.get(choice, presets["1"])
        
        console.print(f"\n[yellow]Running {preset_name} analysis...[/yellow]")
        
        with Progress() as progress:
            task = progress.add_task("Analyzing clinical data...", total=None)
            
            # Run enhanced analysis
            results = analyzer.clinical_analyzer.analyze_directory_enhanced(
                filter_preset=preset,
                export_validation=True
            )
            
            progress.update(task, completed=100)
        
        # Display results summary
        console.print("\n[bold green]Clinical Analysis Complete![/bold green]")
        
        # Note type distribution
        if results.get('note_types'):
            table = Table(title="Note Type Distribution", box=box.ROUNDED)
            table.add_column("Note Type", style="cyan")
            table.add_column("Count", style="yellow")
            table.add_column("Percentage", style="green")
            
            total = sum(results['note_types'].values())
            for note_type, count in sorted(results['note_types'].items(), 
                                         key=lambda x: x[1], reverse=True):
                percentage = (count / total * 100) if total > 0 else 0
                table.add_row(note_type, str(count), f"{percentage:.1f}%")
            
            console.print("\n", table)
        
        # Demographics summary
        if results.get('demographics'):
            demo_table = Table(title="Patient Demographics", box=box.ROUNDED)
            demo_table.add_column("Category", style="cyan")
            demo_table.add_column("Value", style="yellow")
            
            # Age statistics
            ages = results['demographics'].get('ages', [])
            if ages:
                demo_table.add_row("Patients with Age", str(len(ages)))
                demo_table.add_row("Mean Age", f"{np.mean(ages):.1f} years")
                demo_table.add_row("Age Range", f"{min(ages)}-{max(ages)} years")
            
            # Sex distribution
            sex_dist = results['demographics'].get('sex_distribution', {})
            for sex, count in sex_dist.items():
                demo_table.add_row(f"{sex.capitalize()} Patients", str(count))
            
            console.print("\n", demo_table)
        
        # Procedure summary (if applicable)
        if results.get('procedure_classifications'):
            proc_table = Table(title="Procedure Classifications", box=box.ROUNDED)
            proc_table.add_column("Category", style="cyan")
            proc_table.add_column("Count", style="yellow")
            
            proc_counts = Counter(results['procedure_classifications'].values())
            for category, count in proc_counts.most_common():
                proc_table.add_row(category.capitalize(), str(count))
            
            console.print("\n", proc_table)
        
        # Stone phenotypes (if applicable)
        if results.get('stone_phenotypes'):
            pheno_table = Table(title="Stone Phenotype Summary", box=box.ROUNDED)
            pheno_table.add_column("Feature", style="cyan")
            pheno_table.add_column("Found", style="yellow")
            
            # Count non-null features
            feature_counts = defaultdict(int)
            for phenotype in results['stone_phenotypes'].values():
                for feature, value in phenotype.items():
                    if value is not None:
                        feature_counts[feature] += 1
            
            for feature, count in sorted(feature_counts.items()):
                pheno_table.add_row(feature.replace('_', ' ').title(), str(count))
            
            console.print("\n", pheno_table)
        
        # Generate visualizations
        console.print("\n[yellow]Generating visualizations...[/yellow]")
        
        # Generate comparative charts
        analyzer.clinical_analyzer.generate_comparative_visualizations(results)
        
        # Generate chord diagram for concept relationships
        if results.get('cooccurrence_matrix'):
            analyzer.clinical_analyzer.generate_chord_diagram(
                results['cooccurrence_matrix'],
                results.get('semantic_types', {})
            )
        
        # Generate comprehensive HTML report
        report_path = analyzer.clinical_analyzer.generate_comprehensive_html_report(results)
        
        console.print(f"\n[green]✓ Clinical analysis complete![/green]")
        console.print(f"[green]✓ Report saved to: {report_path}[/green]")
        console.print(f"[green]✓ Visualizations saved to: {output_dir / 'clinical_visualizations'}[/green]")
        
        # Validation set info
        if results.get('validation_exported'):
            console.print(f"[green]✓ Validation set exported to: {output_dir / 'validation_samples.xlsx'}[/green]")
        
    def _export_analysis_report(self, analyzer: AnalysisTools, output_dir: Path):
        """Export comprehensive analysis report"""
        console.print("\n[cyan]Generating comprehensive analysis report...[/cyan]")
        
        # Gather all analysis data
        with Progress() as progress:
            task = progress.add_task("Collecting analysis data...", total=4)
            
            # Get concept frequency data
            freq_table = analyzer.concept_frequency_analysis()
            progress.update(task, advance=1)
            
            # Get semantic type analysis
            semantic_data = analyzer.semantic_type_analysis()
            progress.update(task, advance=1)
            
            # Get co-occurrence data
            cooccurrence_data = analyzer.co_occurrence_analysis()
            progress.update(task, advance=1)
            
            # Get clinical insights if available
            clinical_insights = None
            try:
                clinical_insights = analyzer.clinical_analyzer.analyze_directory_enhanced(
                    export_validation=False
                )
            except:
                pass
            progress.update(task, advance=1)
        
        # Create comprehensive report
        report_path = output_dir / f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        with open(report_path, 'w') as f:
            # Header
            f.write("# PythonMetaMap Comprehensive Analysis Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Output Directory:** {output_dir}\n")
            f.write(f"**Files Analyzed:** {len(list(output_dir.glob('*.csv')))}\n\n")
            
            # Executive Summary
            f.write("## Executive Summary\n\n")
            total_concepts = sum(analyzer.analyzer.concept_freq.values())
            unique_concepts = len(analyzer.analyzer.concept_freq)
            total_types = len(analyzer.analyzer.semantic_types)
            
            f.write(f"- **Total Concepts Extracted:** {total_concepts:,}\n")
            f.write(f"- **Unique Concepts:** {unique_concepts:,}\n")
            f.write(f"- **Semantic Types:** {total_types}\n")
            f.write(f"- **Processing Coverage:** {analyzer.analyzer.files_processed} files\n\n")
            
            # Top Concepts
            f.write("## Top 20 Most Frequent Concepts\n\n")
            f.write("| Rank | Concept | CUI | Semantic Type | Frequency |\n")
            f.write("|------|---------|-----|---------------|----------|\n")
            
            top_concepts = sorted(analyzer.analyzer.concept_freq.items(), 
                                key=lambda x: x[1], reverse=True)[:20]
            
            for i, ((concept, cui), freq) in enumerate(top_concepts, 1):
                sem_type = analyzer.analyzer.concept_details.get((concept, cui), {}).get('semantic_type', 'N/A')
                f.write(f"| {i} | {concept} | {cui} | {sem_type} | {freq:,} |\n")
            
            f.write("\n")
            
            # Semantic Type Distribution
            f.write("## Semantic Type Distribution\n\n")
            f.write("| Semantic Type | Count | Percentage |\n")
            f.write("|---------------|-------|------------|\n")
            
            total_sem_types = sum(analyzer.analyzer.semantic_types.values())
            for sem_type, count in sorted(analyzer.analyzer.semantic_types.items(), 
                                         key=lambda x: x[1], reverse=True)[:15]:
                percentage = (count / total_sem_types * 100) if total_sem_types > 0 else 0
                f.write(f"| {sem_type} | {count:,} | {percentage:.1f}% |\n")
            
            f.write("\n")
            
            # Co-occurrence Patterns
            if cooccurrence_data:
                f.write("## Top Co-occurrence Patterns\n\n")
                f.write("| Concept 1 | Concept 2 | Co-occurrence Count |\n")
                f.write("|-----------|-----------|--------------------|\n")
                
                # Get top 15 co-occurrences
                cooccurrence_list = []
                for (c1, cui1), related in cooccurrence_data.items():
                    for (c2, cui2), count in related.items():
                        if (c1, cui1) < (c2, cui2):  # Avoid duplicates
                            cooccurrence_list.append(((c1, c2), count))
                
                top_cooccurrences = sorted(cooccurrence_list, key=lambda x: x[1], reverse=True)[:15]
                
                for (c1, c2), count in top_cooccurrences:
                    f.write(f"| {c1} | {c2} | {count} |\n")
                
                f.write("\n")
            
            # Clinical Insights (if available)
            if clinical_insights:
                f.write("## Clinical Analysis Summary\n\n")
                
                # Note types
                if clinical_insights.get('note_types'):
                    f.write("### Note Type Distribution\n\n")
                    for note_type, count in sorted(clinical_insights['note_types'].items(), 
                                                  key=lambda x: x[1], reverse=True):
                        f.write(f"- **{note_type}:** {count} files\n")
                    f.write("\n")
                
                # Demographics
                if clinical_insights.get('demographics'):
                    f.write("### Patient Demographics\n\n")
                    ages = clinical_insights['demographics'].get('ages', [])
                    if ages:
                        f.write(f"- **Total Patients with Age Data:** {len(ages)}\n")
                        f.write(f"- **Mean Age:** {np.mean(ages):.1f} years\n")
                        f.write(f"- **Age Range:** {min(ages)}-{max(ages)} years\n")
                    
                    sex_dist = clinical_insights['demographics'].get('sex_distribution', {})
                    if sex_dist:
                        f.write("\n**Sex Distribution:**\n")
                        for sex, count in sex_dist.items():
                            f.write(f"- {sex.capitalize()}: {count}\n")
                    f.write("\n")
            
            # Data Quality Metrics
            f.write("## Data Quality Metrics\n\n")
            
            # Calculate coverage
            total_files = len(list(output_dir.glob('*.csv')))
            non_empty_files = analyzer.analyzer.files_processed
            coverage = (non_empty_files / total_files * 100) if total_files > 0 else 0
            
            f.write(f"- **File Coverage:** {non_empty_files}/{total_files} ({coverage:.1f}%)\n")
            f.write(f"- **Average Concepts per File:** {total_concepts/non_empty_files if non_empty_files > 0 else 0:.1f}\n")
            f.write(f"- **Concept Diversity Index:** {unique_concepts/total_concepts if total_concepts > 0 else 0:.3f}\n\n")
            
            # Methodology
            f.write("## Methodology\n\n")
            f.write("This report was generated using PythonMetaMap's comprehensive analysis tools:\n\n")
            f.write("1. **Concept Extraction:** MetaMap NLP processing of clinical text\n")
            f.write("2. **Frequency Analysis:** Statistical analysis of concept occurrence\n")
            f.write("3. **Semantic Grouping:** UMLS semantic type categorization\n")
            f.write("4. **Co-occurrence Analysis:** Identification of related concepts\n")
            if clinical_insights:
                f.write("5. **Clinical Analysis:** Enhanced clinical feature extraction\n")
            f.write("\n")
            
            # Footer
            f.write("---\n")
            f.write("*Report generated by PythonMetaMap - Advanced Medical Concept Analysis*\n")
        
        # Also generate an Excel version with multiple sheets
        excel_path = output_dir / f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            # Concept frequency sheet
            freq_df = pd.DataFrame([
                {
                    'Concept': concept,
                    'CUI': cui,
                    'Semantic Type': analyzer.analyzer.concept_details.get((concept, cui), {}).get('semantic_type', 'N/A'),
                    'Frequency': freq
                }
                for (concept, cui), freq in sorted(analyzer.analyzer.concept_freq.items(), 
                                                 key=lambda x: x[1], reverse=True)
            ])
            freq_df.to_excel(writer, sheet_name='Concept Frequency', index=False)
            
            # Semantic types sheet
            sem_df = pd.DataFrame([
                {'Semantic Type': sem_type, 'Count': count}
                for sem_type, count in sorted(analyzer.analyzer.semantic_types.items(), 
                                             key=lambda x: x[1], reverse=True)
            ])
            sem_df.to_excel(writer, sheet_name='Semantic Types', index=False)
            
            # Summary sheet
            summary_data = {
                'Metric': ['Total Concepts', 'Unique Concepts', 'Semantic Types', 
                          'Files Processed', 'File Coverage %'],
                'Value': [total_concepts, unique_concepts, total_types, 
                         non_empty_files, f"{coverage:.1f}%"]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        console.print(f"\n[green]✓ Analysis report saved to:[/green]")
        console.print(f"  [cyan]Markdown:[/cyan] {report_path}")
        console.print(f"  [cyan]Excel:[/cyan] {excel_path}")
        
    def configuration(self):
        """Configuration management"""
        while True:
            self.clear_screen()
            console.print(Panel(
                "[bold]Configuration[/bold]\nManage PythonMetaMap settings",
                box=box.DOUBLE,
                style=COLORS['warning']
            ))
            
            # Current configuration
            config_items = [
                ("MetaMap Binary", self.config.get('metamap_binary_path', 'Not set')),
                ("Java Home", self.config.get('java_home', 'Auto-detect')),
                ("Workers", str(self.config.get('max_parallel_workers', 4))),
                ("Chunk Size", str(self.config.get('chunk_size', 500))),
                ("Timeout", f"{self.config.get('pymm_timeout', 300)}s"),
                ("Input Dir", self.config.get('default_input_dir', './input_notes')),
                ("Output Dir", self.config.get('default_output_dir', './output_csvs'))
            ]
            
            config_table = Table(title="Current Configuration", box=box.ROUNDED)
            config_table.add_column("Setting", style="cyan")
            config_table.add_column("Value", style="green")
            
            for setting, value in config_items:
                # Truncate long paths
                if len(value) > 50:
                    value = "..." + value[-47:]
                config_table.add_row(setting, value)
                
            console.print(config_table)
            
            # Menu
            console.print("\n[1] Quick Setup (Auto-configure)")
            console.print("[2] Directory Settings")
            console.print("[3] Processing Settings")
            console.print("[4] Server Settings")
            console.print("[5] Save Configuration")
            console.print("[6] Load Configuration")
            console.print("[7] Reset to Defaults")
            console.print("[B] Back")
            
            choice = Prompt.ask("\nSelect option", default="b").lower()
            
            if choice == "b":
                break
            elif choice == "1":
                self._quick_setup()
            elif choice == "2":
                self._configure_directories()
            elif choice == "3":
                self._configure_processing()
            elif choice == "4":
                self._configure_server()
            elif choice == "5":
                self._save_configuration()
            elif choice == "6":
                self._load_configuration()
            elif choice == "7":
                self._reset_configuration()
                
    def _quick_setup(self):
        """Quick automatic setup"""
        console.print("\n[bold]Quick Setup[/bold]")
        console.print("[cyan]Auto-configuring PythonMetaMap...[/cyan]\n")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            results = {}
            
            # 1. Detect Java
            task1 = progress.add_task("Detecting Java installation", total=None)
            java_home = self._detect_java()
            if java_home:
                results["java"] = "Found"
                self.config.set('java_home', java_home)
            else:
                results["java"] = "Not found"
            progress.update(task1, completed=True)
            
            # 2. Detect MetaMap
            task2 = progress.add_task("Locating MetaMap", total=None)
            metamap_paths = self._detect_metamap()
            if metamap_paths:
                results["metamap"] = "Found"
                if metamap_paths.get('home'):
                    self.config.set('metamap_home', metamap_paths['home'])
                if metamap_paths.get('binary'):
                    self.config.set('metamap_binary_path', metamap_paths['binary'])
            else:
                results["metamap"] = "Not found"
            progress.update(task2, completed=True)
            
            # 3. Setup directories
            task3 = progress.add_task("Finding data directories", total=None)
            self.config.set('default_input_dir', '/home/ubuntu/input_notes')
            self.config.set('default_output_dir', '/home/ubuntu/output_csvs')
            results["directories"] = "Configured"
            progress.update(task3, completed=True)
            
            # 4. Analyze system
            task4 = progress.add_task("Analyzing system resources", total=None)
            try:
                recommendations = self.pool_manager.analyze_system()
                workers = recommendations['workers']['optimal']
                self.config.set('max_parallel_workers', workers)
                results["resources"] = "Optimized"
            except:
                # Fallback
                cpu_count = psutil.cpu_count(logical=False) or 4
                self.config.set('max_parallel_workers', min(cpu_count * 2, 16))
                results["resources"] = "Optimized"
            progress.update(task4, completed=True)
            
        # Save
        self.config.save()
        
        console.print("\n[green]✓ Configuration complete![/green]")
        
        # Show what was configured
        summary = Table(box=box.ROUNDED)
        summary.add_column("Component", style="cyan")
        summary.add_column("Status", style="green")
        
        summary.add_row("Java", results.get("java", "Not found"))
        summary.add_row("MetaMap", results.get("metamap", "Not found"))
        summary.add_row("Directories", results.get("directories", "Default"))
        summary.add_row("Resources", results.get("resources", "Default"))
        
        console.print(summary)
        
        input("\nPress Enter to continue...")
        
    def _detect_java(self) -> str:
        """Detect Java installation"""
        # Common Java paths
        java_paths = [
            '/usr/lib/jvm/java-11-openjdk-amd64',
            '/usr/lib/jvm/java-8-openjdk-amd64',
            '/usr/lib/jvm/default-java',
            '/opt/java',
            os.environ.get('JAVA_HOME', '')
        ]
        
        for path in java_paths:
            if path and Path(path).exists():
                return path
                
        # Try to find via which command
        try:
            result = subprocess.run(['which', 'java'], capture_output=True, text=True)
            if result.returncode == 0:
                java_bin = result.stdout.strip()
                # Follow symlinks and find JAVA_HOME
                real_java = os.path.realpath(java_bin)
                # Go up directories to find JAVA_HOME
                java_home = Path(real_java).parent.parent
                return str(java_home)
        except:
            pass
            
        return None
        
    def _detect_metamap(self) -> Dict[str, str]:
        """Detect MetaMap installation"""
        # Common MetaMap paths
        metamap_paths = [
            '/opt/public_mm',
            '/usr/local/public_mm', 
            './metamap_install/public_mm',
            '../metamap_install/public_mm',
            os.path.expanduser('~/metamap_install/public_mm'),
            os.path.expanduser('~/public_mm'),
        ]
        
        # Also check current config
        current_binary = self.config.get('metamap_binary_path')
        if current_binary and Path(current_binary).exists():
            metamap_home = Path(current_binary).parent.parent
            return {
                'home': str(metamap_home),
                'binary': current_binary
            }
        
        # Check common paths
        for path in metamap_paths:
            if Path(path).exists():
                # Look for binary
                bin_path = Path(path) / 'bin' / 'metamap'
                if bin_path.exists():
                    return {
                        'home': path,
                        'binary': str(bin_path)
                    }
                # Also check for .sh version
                bin_sh_path = Path(path) / 'bin' / 'metamap.sh'
                if bin_sh_path.exists():
                    return {
                        'home': path,
                        'binary': str(bin_sh_path)
                    }
                    
        # Check python site-packages (if installed via pip)
        try:
            import site
            for site_dir in site.getsitepackages():
                metamap_dir = Path(site_dir) / 'metamap_install' / 'public_mm'
                if metamap_dir.exists():
                    bin_path = metamap_dir / 'bin' / 'metamap'
                    if bin_path.exists():
                        return {
                            'home': str(metamap_dir),
                            'binary': str(bin_path)
                        }
        except:
            pass
            
        return None
        
    def _configure_directories(self):
        """Configure directories"""
        console.print("\n[bold]Directory Configuration[/bold]")
        
        input_dir = Prompt.ask(
            "Input directory",
            default=self.config.get('default_input_dir', './input_notes')
        )
        
        output_dir = Prompt.ask(
            "Output directory",
            default=self.config.get('default_output_dir', './output_csvs')
        )
        
        # Validate directories
        for name, path in [("Input", input_dir), ("Output", output_dir)]:
            p = Path(path)
            if not p.exists():
                if Confirm.ask(f"{name} directory doesn't exist. Create it?", default=True):
                    p.mkdir(parents=True, exist_ok=True)
                    console.print(f"[green]Created {path}[/green]")
                    
        self.config.set('default_input_dir', input_dir)
        self.config.set('default_output_dir', output_dir)
        self.config.save()
        
        console.print("\n[green]Directories configured![/green]")
        input("\nPress Enter to continue...")
        
    def _configure_processing(self):
        """Configure processing settings"""
        console.print("\n[bold]Processing Configuration[/bold]")
        
        workers = IntPrompt.ask(
            "Number of parallel workers",
            default=self.config.get('max_parallel_workers', 4)
        )
        
        timeout = IntPrompt.ask(
            "Timeout per file (seconds)",
            default=self.config.get('pymm_timeout', 300)
        )
        
        chunk_size = IntPrompt.ask(
            "Files per chunk",
            default=self.config.get('chunk_size', 100)
        )
        
        use_pool = Confirm.ask(
            "Use instance pooling?",
            default=self.config.get('use_instance_pool', True)
        )
        
        self.config.set('max_parallel_workers', workers)
        self.config.set('pymm_timeout', timeout)
        self.config.set('chunk_size', chunk_size)
        self.config.set('use_instance_pool', use_pool)
        self.config.save()
        
        console.print("\n[green]Processing settings configured![/green]")
        input("\nPress Enter to continue...")
        
    def _configure_server(self):
        """Configure server settings"""
        console.print("\n[bold]Server Configuration[/bold]")
        
        # MetaMap home
        metamap_home = Prompt.ask(
            "MetaMap home directory",
            default=self.config.get('metamap_home', '')
        )
        
        # Java home
        java_home = Prompt.ask(
            "Java home directory",
            default=self.config.get('java_home', '/usr/lib/jvm/java-11-openjdk-amd64')
        )
        
        # Server ports
        console.print("\n[bold]Server Ports[/bold]")
        tagger_port = IntPrompt.ask(
            "Tagger server port",
            default=self.config.get('tagger_server_port', 1795)
        )
        
        wsd_port = IntPrompt.ask(
            "WSD server port",
            default=self.config.get('wsd_server_port', 5554)
        )
        
        # Server options
        console.print("\n[bold]Server Options[/bold]")
        relaxed_model = Confirm.ask(
            "Use relaxed model?",
            default=self.config.get('relaxed_model', True)
        )
        
        self.config.set('metamap_home', metamap_home)
        self.config.set('java_home', java_home)
        self.config.set('tagger_server_port', tagger_port)
        self.config.set('wsd_server_port', wsd_port)
        self.config.set('relaxed_model', relaxed_model)
        self.config.save()
        
        console.print("\n[green]Server settings configured![/green]")
        input("\nPress Enter to continue...")
        
    def _save_configuration(self):
        """Save configuration to file"""
        filename = Prompt.ask("Configuration filename", default="pymm_config.json")
        
        config_data = {
            'metamap_binary_path': self.config.get('metamap_binary_path'),
            'java_home': self.config.get('java_home'),
            'max_parallel_workers': self.config.get('max_parallel_workers'),
            'chunk_size': self.config.get('chunk_size'),
            'pymm_timeout': self.config.get('pymm_timeout'),
            'default_input_dir': self.config.get('default_input_dir'),
            'default_output_dir': self.config.get('default_output_dir'),
            'use_instance_pool': self.config.get('use_instance_pool'),
            'tagger_server_port': self.config.get('tagger_server_port'),
            'wsd_server_port': self.config.get('wsd_server_port'),
            'relaxed_model': self.config.get('relaxed_model')
        }
        
        with open(filename, 'w') as f:
            json.dump(config_data, f, indent=2)
            
        console.print(f"\n[green]Configuration saved to {filename}[/green]")
        input("\nPress Enter to continue...")
        
    def _load_configuration(self):
        """Load configuration from file"""
        filename = Prompt.ask("Configuration filename", default="pymm_config.json")
        
        if not Path(filename).exists():
            console.print(f"[red]File {filename} not found[/red]")
            input("\nPress Enter to continue...")
            return
            
        with open(filename) as f:
            config_data = json.load(f)
            
        for key, value in config_data.items():
            if value is not None:
                self.config.set(key, value)
                
        self.config.save()
        console.print(f"\n[green]Configuration loaded from {filename}[/green]")
        input("\nPress Enter to continue...")
        
    def _reset_configuration(self):
        """Reset to default configuration"""
        if Confirm.ask("\n[yellow]Reset all settings to defaults?[/yellow]", default=False):
            # Reset to defaults
            defaults = {
                'max_parallel_workers': 4,
                'chunk_size': 100,
                'pymm_timeout': 300,
                'use_instance_pool': True,
                'relaxed_model': True,
                'tagger_server_port': 1795,
                'wsd_server_port': 5554
            }
            
            for key, value in defaults.items():
                self.config.set(key, value)
                
            self.config.save()
            console.print("\n[green]Configuration reset to defaults![/green]")
        
        input("\nPress Enter to continue...")
        
    def server_control(self):
        """Server control panel"""
        while True:
            self.clear_screen()
            console.print(Panel(
                "[bold]Server Control[/bold]\nManage MetaMap services",
                box=box.DOUBLE,
                style=COLORS['error']
            ))
            
            # Server status
            status_panels = []
            
            # Overall status
            overall_running = self.server_manager.is_running()
            overall_status = "[green]● Running[/green]" if overall_running else "[red]● Stopped[/red]"
            
            overall_panel = Panel(
                f"Status: {overall_status}\nBinary: {self.config.get('metamap_binary_path', 'Not configured')[:50]}",
                title="[bold]MetaMap Server[/bold]",
                box=box.ROUNDED,
                style=COLORS['box_primary']
            )
            status_panels.append(overall_panel)
            
            # Component status
            components = [
                ("Tagger Server", self.server_manager.is_tagger_server_running(), "1795"),
                ("WSD Server", self.server_manager.is_wsd_server_running(), "5554"),
                ("MetaMap Server", self.server_manager.is_mmserver_running(), "8066")
            ]
            
            component_content = ""
            for name, running, port in components:
                status = "[green]●[/green]" if running else "[red]●[/red]"
                component_content += f"{status} {name:<15} Port: {port}\n"
                
            component_panel = Panel(
                component_content.strip(),
                title="[bold]Components[/bold]",
                box=box.ROUNDED,
                style=COLORS['box_secondary']
            )
            status_panels.append(component_panel)
            
            console.print(Columns(status_panels))
            
            # Actions
            console.print("\n[bold]Actions:[/bold]")
            
            if overall_running:
                console.print("[1] Stop Server")
                console.print("[2] Restart Server")
                console.print("[3] View Server Logs")
                console.print("[4] Server Statistics")
            else:
                console.print("[1] Start Server")
                console.print("[2] Start with Custom Options")
                console.print("[3] Check Configuration")
                
            console.print("[B] Back")
            
            choice = Prompt.ask("\nSelect action", default="b").lower()
            
            if choice == "b":
                break
            elif choice == "1":
                if overall_running:
                    if Confirm.ask("\n[yellow]Stop MetaMap server?[/yellow]"):
                        with console.status("[cyan]Stopping server...[/cyan]"):
                            self.server_manager.stop()
                        console.print("[green]Server stopped[/green]")
                else:
                    self._start_server_visual()
            elif choice == "2":
                if overall_running:
                    console.print("\n[cyan]Restarting server...[/cyan]")
                    with console.status("[cyan]Stopping...[/cyan]"):
                        self.server_manager.stop()
                    time.sleep(2)
                    self._start_server_visual()
                else:
                    self._start_server_custom()
            elif choice == "3":
                if overall_running:
                    self._view_server_logs()
                else:
                    self._check_server_config()
            elif choice == "4" and overall_running:
                self._show_server_statistics()
                
            if choice != "b":
                input("\nPress Enter to continue...")
                
    def _show_server_statistics(self):
        """Show server statistics"""
        console.print("\n[bold]Server Statistics[/bold]")
        
        # Get server status
        tagger_running = self.server_manager.is_tagger_server_running()
        wsd_running = self.server_manager.is_wsd_server_running()
        status = {
            'tagger': tagger_running,
            'wsd': wsd_running
        }
        
        # Create statistics table
        stats_table = Table(box=box.ROUNDED)
        stats_table.add_column("Component", style="cyan")
        stats_table.add_column("Status", style="green")
        stats_table.add_column("Port", style="yellow")
        stats_table.add_column("PID", style="magenta")
        
        # Check each component
        components = [
            ("Tagger Server", "tagger", 1795),
            ("WSD Server", "wsd", 5554),
            ("MetaMap Server", "metamap", 8066)
        ]
        
        for name, key, port in components:
            if status.get(key, False):
                status_text = "[green]● Running[/green]"
                # Try to get PID
                pid = "N/A"
                try:
                    import psutil
                    for proc in psutil.process_iter(['pid', 'name', 'connections']):
                        try:
                            for conn in proc.connections():
                                if conn.laddr.port == port:
                                    pid = str(proc.pid)
                                    break
                        except:
                            pass
                except:
                    pass
            else:
                status_text = "[red]● Stopped[/red]"
                pid = "-"
                
            stats_table.add_row(name, status_text, str(port), pid)
            
        console.print(stats_table)
        
        # System resource usage
        try:
            import psutil
            console.print("\n[bold]Resource Usage:[/bold]")
            
            # Find MetaMap processes
            metamap_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_info']):
                try:
                    if any(term in proc.info['name'].lower() for term in ['java', 'metamap', 'mmserver']):
                        metamap_processes.append(proc)
                except:
                    pass
                    
            if metamap_processes:
                total_cpu = sum(p.cpu_percent() for p in metamap_processes)
                total_memory = sum(p.memory_info().rss for p in metamap_processes) / 1024 / 1024
                
                console.print(f"Total CPU Usage: {total_cpu:.1f}%")
                console.print(f"Total Memory Usage: {total_memory:.1f} MB")
                console.print(f"Active Processes: {len(metamap_processes)}")
            else:
                console.print("[dim]No MetaMap processes found[/dim]")
                
        except Exception as e:
            console.print(f"[yellow]Could not get resource statistics: {e}[/yellow]")
            
        # Configuration info
        console.print("\n[bold]Configuration:[/bold]")
        console.print(f"MetaMap Binary: {self.config.get('metamap_binary_path', 'Not set')}")
        console.print(f"MetaMap Home: {self.config.get('metamap_home', 'Not set')}")
        console.print(f"Processing Options: {self.config.get('metamap_processing_options', 'Default')}")
        
    def _view_server_logs(self):
        """View server logs"""
        console.print("\n[bold]Server Logs[/bold]")
        
        # Look for log files
        metamap_home = self.config.get('metamap_home')
        if metamap_home:
            log_dir = Path(metamap_home) / 'logs'
            if log_dir.exists():
                log_files = list(log_dir.glob("*.log"))
                
                if log_files:
                    # Show recent log entries
                    recent_log = max(log_files, key=lambda f: f.stat().st_mtime)
                    console.print(f"\nViewing: {recent_log.name}")
                    console.print("[dim]Last 20 lines:[/dim]\n")
                    
                    with open(recent_log) as f:
                        lines = f.readlines()[-20:]
                        for line in lines:
                            console.print(f"  {line.rstrip()}")
                else:
                    console.print("[yellow]No log files found[/yellow]")
            else:
                console.print("[yellow]Log directory not found[/yellow]")
        else:
            console.print("[yellow]MetaMap home not configured[/yellow]")
            
    def background_jobs(self):
        """Manage background processing jobs"""
        while True:
            self.clear_screen()
            console.print(Panel(
                "[bold]Background Jobs[/bold]\nManage background processing tasks",
                box=box.DOUBLE,
                style="cyan"
            ))
            
            # List jobs
            jobs = self.background_processor.list_jobs()
            
            if jobs:
                job_table = Table(title="Active Jobs", box=box.ROUNDED)
                job_table.add_column("ID", style="cyan")
                job_table.add_column("Status", style="green")
                job_table.add_column("Input", style="dim")
                job_table.add_column("Duration", justify="right")
                
                for job in jobs:
                    status_color = {
                        'running': 'yellow',
                        'completed': 'green',
                        'failed': 'red'
                    }.get(job['status'], 'white')
                    
                    job_table.add_row(
                        job['id'],
                        f"[{status_color}]{job['status'].upper()}[/{status_color}]",
                        job['input'][:30] + "..." if len(job['input']) > 30 else job['input'],
                        job['duration']
                    )
                    
                console.print(job_table)
            else:
                console.print("[dim]No background jobs[/dim]")
                
            # Menu
            console.print("\n[1] Start New Job")
            console.print("[2] View Job Details")
            console.print("[3] Cancel Job")
            console.print("[4] Clear Completed")
            console.print("[B] Back")
            
            choice = Prompt.ask("\nSelect option", default="b").lower()
            
            if choice == "b":
                break
            elif choice == "1":
                self._start_background_job()
            elif choice == "2" and jobs:
                job_id = Prompt.ask("Job ID")
                self._view_job_details(job_id)
            elif choice == "3" and jobs:
                job_id = Prompt.ask("Job ID to cancel")
                self._cancel_job(job_id)
            elif choice == "4":
                self._clear_completed_jobs()
                
    def _start_background_job(self):
        """Start a new background job"""
        console.print("\n[bold]Start Background Job[/bold]")
        
        input_dir = Prompt.ask("Input directory", default=self.config.get('default_input_dir'))
        output_dir = Prompt.ask("Output directory", default=self.config.get('default_output_dir'))
        
        # Validate
        if not Path(input_dir).exists():
            console.print("[error]Input directory not found[/error]")
            return
            
        # Start job
        job_id = self.background_processor.start_background_process(input_dir, output_dir)
        console.print(f"\n[green]Started background job: {job_id}[/green]")
    
    def monitor(self):
        """Launch unified monitor with all features"""
        self.clear_screen()
        console.print(Panel(
            "[bold]Unified Monitor[/bold]\nAll-in-one monitoring and management interface",
            box=box.DOUBLE,
            style="cyan"
        ))
        
        console.print("\n[yellow]Launching unified monitor...[/yellow]")
        console.print("\n[bold]Features:[/bold]")
        console.print("• [cyan]Dashboard view[/cyan] - See everything at once")
        console.print("• [cyan]Job monitoring[/cyan] - View and manage all jobs")
        console.print("• [cyan]File explorer[/cyan] - Browse and quick process files")
        console.print("• [cyan]Log viewer[/cyan] - View all system logs")
        console.print("• [cyan]System resources[/cyan] - Real-time resource monitoring")
        console.print("\n[bold]Navigation:[/bold]")
        console.print("• Press [cyan]1-5[/cyan] to switch views")
        console.print("• Press [cyan]Tab[/cyan] to switch panes in dashboard")
        console.print("• Press [cyan]Q[/cyan] to return to main menu")
        console.print("\n[dim]Starting in 2 seconds...[/dim]")
        time.sleep(2)
        
        try:
            # Launch unified monitor
            monitor = UnifiedMonitor(self.config)
            monitor.run()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            console.print(f"\n[red]Error: {e}[/red]")
            input("\nPress Enter to continue...")
    
    def job_monitor(self):
        """Unified job monitoring system"""
        from ..cli.monitor import JobMonitor
        from ..core.job_manager import get_job_manager, JobStatus
        
        while True:
            self.clear_screen()
            console.print(Panel(
                "[bold]Job Monitor[/bold]\nLive monitoring of all processing jobs",
                box=box.DOUBLE,
                style="bright_blue"
            ))
            
            job_manager = get_job_manager()
            jobs = job_manager.list_jobs(limit=20)
            
            # Summary stats
            active = len([j for j in jobs if j.status == JobStatus.RUNNING])
            completed = len([j for j in jobs if j.status == JobStatus.COMPLETED])
            failed = len([j for j in jobs if j.status == JobStatus.FAILED])
            
            stats_table = Table(box=box.SIMPLE)
            stats_table.add_column("Active", style="green", justify="center")
            stats_table.add_column("Completed", style="blue", justify="center")
            stats_table.add_column("Failed", style="red", justify="center")
            stats_table.add_column("Total", style="yellow", justify="center")
            
            stats_table.add_row(
                f"[bold]{active}[/bold]",
                f"[bold]{completed}[/bold]",
                f"[bold]{failed}[/bold]",
                f"[bold]{len(jobs)}[/bold]"
            )
            
            console.print(Align.center(stats_table))
            console.print()
            
            # Jobs table
            if jobs:
                job_table = Table(title="Recent Jobs", box=box.ROUNDED)
                job_table.add_column("ID", style="cyan", width=25)
                job_table.add_column("Type", style="yellow", width=10)
                job_table.add_column("Status", width=12)
                job_table.add_column("Progress", width=20)
                job_table.add_column("Duration", style="dim", width=10)
                
                for job in jobs[:10]:  # Show top 10
                    # Status color
                    status_color = {
                        JobStatus.RUNNING: "green",
                        JobStatus.COMPLETED: "blue",
                        JobStatus.FAILED: "red",
                        JobStatus.CANCELLED: "yellow",
                        JobStatus.QUEUED: "cyan"
                    }.get(job.status, "white")
                    
                    status_text = f"[{status_color}]{job.status.value}[/{status_color}]"
                    
                    # Progress
                    progress = job.progress or {}
                    if job.status == JobStatus.RUNNING and progress.get('percentage', 0) > 0:
                        percentage = progress['percentage']
                        bar_width = 15
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
                    
                    duration_str = str(duration).split('.')[0]  # Remove microseconds
                    
                    job_table.add_row(
                        job.job_id[:25],
                        job.job_type.value,
                        status_text,
                        progress_bar,
                        duration_str
                    )
                
                console.print(job_table)
            else:
                console.print("[dim]No jobs found[/dim]")
            
            # Menu options
            console.print("\n[bold]Options:[/bold]")
            console.print("[1] Live Monitor (full screen)")
            console.print("[2] View Job Details")
            console.print("[3] Cancel Job")
            console.print("[4] Clear Completed Jobs")
            console.print("[5] Export Job Report")
            console.print("[R] Refresh")
            console.print("[B] Back")
            
            choice = Prompt.ask("\nSelect option", default="b").lower()
            
            if choice == "b":
                break
            elif choice == "1":
                # Launch full screen monitor
                console.print("\n[yellow]Launching live monitor...[/yellow]")
                console.print("[dim]Press Ctrl+C to return[/dim]")
                time.sleep(1)
                
                monitor = JobMonitor()
                try:
                    monitor.run()
                except KeyboardInterrupt:
                    pass
                
            elif choice == "2":
                # View job details
                job_id = Prompt.ask("Enter job ID")
                job = job_manager.get_job(job_id)
                
                if job:
                    details = f"""
[bold]Job Details[/bold]
ID: {job.job_id}
Type: {job.job_type.value}
Status: {job.status.value}
Started: {job.start_time}
Input: {job.input_dir}
Output: {job.output_dir}
"""
                    if job.progress:
                        details += f"\nProgress: {job.progress}"
                    if job.error:
                        details += f"\n[red]Error: {job.error}[/red]"
                    
                    console.print(Panel(details, box=box.ROUNDED))
                else:
                    console.print("[red]Job not found[/red]")
                
                input("\nPress Enter to continue...")
                
            elif choice == "3":
                # Cancel job
                job_id = Prompt.ask("Enter job ID to cancel")
                if Confirm.ask(f"Cancel job {job_id}?"):
                    if job_manager.cancel_job(job_id):
                        console.print("[green]Job cancelled[/green]")
                    else:
                        console.print("[red]Failed to cancel job[/red]")
                    time.sleep(1)
                    
            elif choice == "4":
                # Clear completed
                if Confirm.ask("Clear all completed jobs?"):
                    job_manager.cleanup_old_jobs(0)  # Clear all completed
                    console.print("[green]Completed jobs cleared[/green]")
                    time.sleep(1)
                    
            elif choice == "5":
                # Export report
                report_path = Path.home() / f"pymm_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                
                report_data = {
                    'generated': datetime.now().isoformat(),
                    'summary': {
                        'active': active,
                        'completed': completed,
                        'failed': failed,
                        'total': len(jobs)
                    },
                    'jobs': [job.to_dict() for job in jobs]
                }
                
                with open(report_path, 'w') as f:
                    json.dump(report_data, f, indent=2)
                
                console.print(f"[green]Report exported to: {report_path}[/green]")
                input("\nPress Enter to continue...")
                
            elif choice == "r":
                continue  # Refresh
        
    def resume_retry(self):
        """Resume or retry failed processing"""
        self.clear_screen()
        console.print(Panel(
            "[bold]Resume/Retry[/bold]\nContinue interrupted processing",
            box=box.DOUBLE,
            style="yellow"
        ))
        
        # Look for state files
        output_dir = Path(self.config.get('default_output_dir', './output_csvs'))
        state_files = list(output_dir.glob(".processing_state*.json"))
        
        if not state_files:
            console.print("\n[yellow]No interrupted sessions found[/yellow]")
            
            # Check for failed files in logs
            log_dir = output_dir / "logs"
            if log_dir.exists():
                console.print("\n[dim]Checking logs for failed files...[/dim]")
                
                # Analyze logs for failures
                failed_files = []
                log_files = sorted(log_dir.glob("batch_run_*.log"), reverse=True)[:5]  # Check last 5 runs
                
                for log_file in log_files:
                    try:
                        with open(log_file, 'r') as f:
                            content = f.read()
                            # Look for error patterns
                            for line in content.splitlines():
                                if "ERROR" in line and ".txt" in line:
                                    # Extract filename from error message
                                    import re
                                    match = re.search(r'(\w+\.txt)', line)
                                    if match:
                                        failed_files.append(match.group(1))
                                elif "Failed to process" in line:
                                    match = re.search(r'Failed to process (\S+\.txt)', line)
                                    if match:
                                        failed_files.append(match.group(1))
                    except:
                        continue
                
                if failed_files:
                    failed_files = list(set(failed_files))  # Remove duplicates
                    console.print(f"\n[yellow]Found {len(failed_files)} failed files in recent logs[/yellow]")
                    
                    # Show failed files
                    failed_table = Table(title="Failed Files", box=box.ROUNDED)
                    failed_table.add_column("Filename", style="red")
                    
                    for file in failed_files[:10]:  # Show first 10
                        failed_table.add_row(file)
                    
                    if len(failed_files) > 10:
                        failed_table.add_row(f"... and {len(failed_files) - 10} more")
                    
                    console.print(failed_table)
                    
                    # Ask if they want to retry these files
                    if Confirm.ask("\nRetry these failed files?", default=True):
                        # Create a retry list file
                        retry_file = output_dir / "retry_files.txt"
                        with open(retry_file, 'w') as f:
                            for file in failed_files:
                                f.write(f"{file}\n")
                        
                        console.print(f"\n[green]Created retry file list: {retry_file}[/green]")
                        console.print("[cyan]Use this with batch processing to retry only failed files[/cyan]")
                else:
                    console.print("[green]No failed files found in recent logs[/green]")
                
            input("\nPress Enter to continue...")
            return
            
        # Show sessions
        session_table = Table(title="Interrupted Sessions", box=box.ROUNDED)
        session_table.add_column("#", width=3)
        session_table.add_column("Session", style="cyan")
        session_table.add_column("Processed", justify="right", style="green")
        session_table.add_column("Failed", justify="right", style="red")
        session_table.add_column("Last Update", style="dim")
        
        sessions = []
        for i, state_file in enumerate(state_files, 1):
            try:
                with open(state_file) as f:
                    state = json.load(f)
                    
                sessions.append((state_file, state))
                
                session_table.add_row(
                    str(i),
                    state_file.name[:30],
                    str(len(state.get('processed', []))),
                    str(len(state.get('failed', []))),
                    state.get('timestamp', 'Unknown')[:16]
                )
            except:
                pass
                
        console.print(session_table)
        
        # Options
        console.print("\n[1] Resume Processing")
        console.print("[2] Retry Failed Files Only")
        console.print("[3] View Session Details")
        console.print("[4] Clear Session")
        console.print("[B] Back")
        
        choice = Prompt.ask("\nSelect option", default="b").lower()
        
        if choice != "b" and sessions:
            try:
                session_num = IntPrompt.ask("Session number", default=1)
                if 1 <= session_num <= len(sessions):
                    state_file, state = sessions[session_num - 1]
                    
                    if choice == "1":
                        self._resume_session(state_file, state)
                    elif choice == "2":
                        self._retry_failed_files(state_file, state)
                    elif choice == "3":
                        self._view_session_details(state)
                    elif choice == "4":
                        if Confirm.ask("\n[yellow]Clear this session?[/yellow]"):
                            os.remove(state_file)
                            console.print("[green]Session cleared[/green]")
            except:
                pass
                
        input("\nPress Enter to continue...")
        
    def logs_monitor(self):
        """View logs and monitoring data"""
        self.clear_screen()
        console.print(Panel(
            "[bold]Logs & Monitoring[/bold]\nView system and processing logs",
            box=box.DOUBLE,
            style="white"
        ))
        
        # Log sources
        log_sources = []
        
        # Processing logs
        output_dir = Path(self.config.get('default_output_dir', './output_csvs'))
        log_dir = output_dir / "logs"
        
        if log_dir.exists():
            processing_logs = list(log_dir.glob("*.log"))
            if processing_logs:
                log_sources.append(("Processing Logs", processing_logs))
                
        # Server logs
        metamap_home = self.config.get('metamap_home')
        if metamap_home:
            server_log_dir = Path(metamap_home) / "logs"
            if server_log_dir.exists():
                server_logs = list(server_log_dir.glob("*.log"))
                if server_logs:
                    log_sources.append(("Server Logs", server_logs))
                    
        if not log_sources:
            console.print("\n[yellow]No log files found[/yellow]")
            input("\nPress Enter to continue...")
            return
            
        # Show log sources
        console.print("\n[bold]Available Logs:[/bold]")
        
        all_logs = []
        for source_name, logs in log_sources:
            console.print(f"\n[cyan]{source_name}:[/cyan]")
            
            # Show recent logs
            recent_logs = sorted(logs, key=lambda f: f.stat().st_mtime, reverse=True)[:5]
            
            for log in recent_logs:
                all_logs.append(log)
                mtime = datetime.fromtimestamp(log.stat().st_mtime)
                size = self._format_size(log.stat().st_size)
                console.print(f"  [{len(all_logs)}] {log.name:<40} {size:>10} {mtime.strftime('%Y-%m-%d %H:%M')}")
                
        # Options
        console.print("\n[#] View log by number")
        console.print("[T] Tail most recent log")
        console.print("[S] Search in logs")
        console.print("[E] Export logs")
        console.print("[B] Back")
        
        choice = Prompt.ask("\nSelect option", default="b").lower()
        
        if choice.isdigit():
            log_num = int(choice)
            if 1 <= log_num <= len(all_logs):
                self._view_log_file(all_logs[log_num - 1])
        elif choice == "t":
            if all_logs:
                self._tail_log(max(all_logs, key=lambda f: f.stat().st_mtime))
        elif choice == "s":
            pattern = Prompt.ask("Search pattern")
            self._search_logs(all_logs, pattern)
        elif choice == "e":
            self._export_logs(all_logs)
            
    def _view_log_file(self, log_file: Path):
        """View log file with pagination"""
        self.clear_screen()
        console.print(f"[bold]Viewing: {log_file.name}[/bold]")
        console.rule(style="dim")
        
        try:
            with open(log_file) as f:
                lines = f.readlines()
                
            # Show stats
            console.print(f"[dim]Lines: {len(lines)} | Size: {self._format_size(log_file.stat().st_size)}[/dim]")
            console.print()
            
            # Paginate
            page_size = 30
            page = 0
            
            while True:
                start = page * page_size
                end = min(start + page_size, len(lines))
                
                # Show page
                for i, line in enumerate(lines[start:end], start + 1):
                    # Color by log level
                    if '[ERROR]' in line or 'ERROR' in line:
                        console.print(f"[red]{i:5d} {line.rstrip()}[/red]")
                    elif '[WARNING]' in line or 'WARNING' in line:
                        console.print(f"[yellow]{i:5d} {line.rstrip()}[/yellow]")
                    elif '[DEBUG]' in line:
                        console.print(f"[dim]{i:5d} {line.rstrip()}[/dim]")
                    else:
                        console.print(f"{i:5d} {line.rstrip()}")
                        
                # Navigation
                console.print(f"\n[dim]Page {page + 1}/{(len(lines) - 1) // page_size + 1}[/dim]")
                console.print("[dim]N:Next P:Previous F:First L:Last S:Search Q:Quit[/dim]")
                
                nav = console.input().lower()
                
                if nav == 'q':
                    break
                elif nav == 'n' and end < len(lines):
                    page += 1
                elif nav == 'p' and page > 0:
                    page -= 1
                elif nav == 'f':
                    page = 0
                elif nav == 'l':
                    page = (len(lines) - 1) // page_size
                elif nav == 's':
                    pattern = Prompt.ask("Search")
                    # Search functionality
                    
                self.clear_screen()
                console.print(f"[bold]Viewing: {log_file.name}[/bold]")
                console.rule(style="dim")
                
        except Exception as e:
            console.print(f"[error]Error reading log: {e}[/error]")
            
        input("\nPress Enter to continue...")
        
    def _tail_log(self, log_file: Path):
        """Tail log file in real-time"""
        self.clear_screen()
        console.print(f"[bold]Tailing: {log_file.name}[/bold]")
        console.print("[dim]Press Ctrl+C to stop[/dim]")
        console.rule(style="dim")
        
        try:
            import subprocess
            process = subprocess.Popen(
                ['tail', '-f', str(log_file)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            try:
                for line in process.stdout:
                    # Color code log lines
                    if 'ERROR' in line:
                        console.print(f"[red]{line.rstrip()}[/red]")
                    elif 'WARNING' in line:
                        console.print(f"[yellow]{line.rstrip()}[/yellow]")
                    elif 'SUCCESS' in line or 'DONE' in line:
                        console.print(f"[green]{line.rstrip()}[/green]")
                    else:
                        console.print(line.rstrip())
            except KeyboardInterrupt:
                process.terminate()
                console.print("\n[yellow]Stopped tailing[/yellow]")
                
        except Exception as e:
            console.print(f"[error]Error tailing log: {e}[/error]")
            
        input("\nPress Enter to continue...")
        
    def _search_logs(self, log_files: List[Path], pattern: str):
        """Search across multiple log files"""
        self.clear_screen()
        console.print(f"[bold]Searching for: {pattern}[/bold]")
        console.rule(style="dim")
        
        matches = []
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Searching logs...", total=len(log_files))
            
            for log_file in log_files:
                try:
                    with open(log_file) as f:
                        for line_num, line in enumerate(f, 1):
                            if pattern.lower() in line.lower():
                                matches.append((log_file, line_num, line.strip()))
                except:
                    pass
                progress.update(task, advance=1)
                
        # Show results
        if matches:
            console.print(f"\n[green]Found {len(matches)} matches:[/green]\n")
            
            for log_file, line_num, line in matches[:50]:
                console.print(f"[cyan]{log_file.name}:{line_num}[/cyan]")
                console.print(f"  {line[:100]}{'...' if len(line) > 100 else ''}\n")
                
            if len(matches) > 50:
                console.print(f"[dim]... and {len(matches) - 50} more matches[/dim]")
        else:
            console.print("[yellow]No matches found[/yellow]")
            
        input("\nPress Enter to continue...")
        
    def _export_logs(self, log_files: List[Path]):
        """Export logs to a single file"""
        output_file = Prompt.ask("Export filename", default="logs_export.txt")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Exporting logs...", total=len(log_files))
            
            with open(output_file, 'w') as out:
                for log_file in log_files:
                    out.write(f"\n{'=' * 80}\n")
                    out.write(f"LOG: {log_file.name}\n")
                    out.write(f"{'=' * 80}\n\n")
                    
                    try:
                        with open(log_file) as f:
                            out.write(f.read())
                    except:
                        out.write(f"Error reading {log_file}\n")
                        
                    progress.update(task, advance=1)
                    
        console.print(f"\n[green]Logs exported to: {output_file}[/green]")
        input("\nPress Enter to continue...")
        
    def show_help(self):
        """Show comprehensive help"""
        self.clear_screen()
        
        help_content = """# PythonMetaMap Ultimate Help Guide

## Quick Start
1. **Install Java**: OpenJDK 11 or later required
2. **Install MetaMap**: Download from NLM website
3. **Configure**: Use Configuration > Quick Setup
4. **Process**: Place .txt files in input directory and use Quick Process

## Features

### File Processing
- **Quick Process**: Smart defaults for immediate processing
- **Monitor**: Unified interface with file explorer, job management, and system monitoring
- **Batch Process**: Handle large datasets efficiently with background support
- **Resume/Retry**: Continue interrupted processing or retry failed files

### Analysis Tools
- **Concept Frequency**: Analyze most common medical concepts
- **Semantic Types**: Distribution of concept categories
- **Co-occurrence**: Find related concepts
- **Search**: Find specific concepts across files
- **Word Cloud**: Visual representation of concepts

### Monitor Features (Press 2)
- **Dashboard View**: See everything at once in split-screen
- **Job Management**: View active jobs, cancel/kill operations
- **File Explorer**: Browse directories, select and quick process files
- **Log Viewer**: View processing and server logs in real-time
- **System Resources**: CPU, memory, disk, and network monitoring

### Advanced Features
- **Server Control**: Manage MetaMap services
- **Configuration**: System setup and optimization

## Keyboard Shortcuts
- **Arrow Keys**: Navigate in file browser
- **Space**: Select/deselect files
- **Enter**: Open/confirm
- **Q**: Go back/quit
- **Ctrl+C**: Cancel operation

## Tips
- Process files in batches of 500-1000 for best performance
- Use background jobs for datasets over 1000 files
- Check logs if processing fails
- Optimize settings based on your system resources

## Troubleshooting
- **Java not found**: Install OpenJDK 11
- **Server won't start**: Check MetaMap installation path
- **Out of memory**: Reduce chunk size or workers
- **Slow processing**: Check system resources, reduce workers
"""
        
        # Use pager for long content
        lines = help_content.split('\n')
        page_size = 25
        page = 0
        
        while True:
            self.clear_screen()
            console.print(Panel(
                "[bold]Help & Documentation[/bold]",
                box=box.DOUBLE,
                style=COLORS['dim']
            ))
            
            # Show page
            start = page * page_size
            end = min(start + page_size, len(lines))
            
            for line in lines[start:end]:
                if line.startswith('#'):
                    console.print(f"[bold cyan]{line}[/bold cyan]")
                elif line.startswith('-'):
                    console.print(f"[green]{line}[/green]")
                else:
                    console.print(line)
                    
            # Navigation
            console.print(f"\n[dim]Page {page + 1}/{(len(lines) - 1) // page_size + 1}[/dim]")
            console.print("[dim]N:Next P:Previous Q:Quit[/dim]")
            
            nav = console.input().lower()
            
            if nav == 'q':
                break
            elif nav == 'n' and end < len(lines):
                page += 1
            elif nav == 'p' and page > 0:
                page -= 1
                
    def _format_size(self, size: int) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f}{unit}"
            size /= 1024
        return f"{size:.1f}TB"
        
    def run(self):
        """Main entry point"""
        try:
            # Install readchar if needed
            try:
                import readchar
            except ImportError:
                console.print("[yellow]Installing required package: readchar[/yellow]")
                subprocess.check_call([sys.executable, "-m", "pip", "install", "readchar"])
                
            self.main_menu()
        except KeyboardInterrupt:
            console.print("\n\n[yellow]Interrupted by user[/yellow]")
        except Exception as e:
            console.print(f"\n\n[error]Unexpected error: {e}[/error]")
            if self.config.get('debug'):
                import traceback
                traceback.print_exc()
        finally:
            # Cleanup
            self.resource_monitor.stop()
            if self.server_manager.is_running() and self.config.get('auto_stop_server'):
                console.print("\n[dim]Stopping server...[/dim]")
                self.server_manager.stop()


def interactive_ultimate():
    """Launch ultimate interactive mode"""
    navigator = UltimateInteractiveNavigator()
    navigator.run()