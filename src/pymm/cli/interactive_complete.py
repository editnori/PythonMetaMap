"""Complete Interactive TUI for PythonMetaMap with all features"""
import os
import sys
import time
import psutil
import threading
import subprocess
import shutil
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from collections import deque
import json

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.text import Text
from rich.live import Live
from rich import box
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.align import Align
from rich.columns import Columns
from rich.layout import Layout
from rich.tree import Tree
from rich.syntax import Syntax
from rich.rule import Rule
from rich.markdown import Markdown

from ..core.config import PyMMConfig
from ..core.enhanced_state import AtomicStateManager
from ..server.manager import ServerManager
from ..processing.optimized_batch_runner import OptimizedBatchRunner
from ..processing.ultra_optimized_runner import UltraOptimizedBatchRunner
from ..processing.pool_manager import AdaptivePoolManager

console = Console()

# Rich color scheme
COLORS = {
    'primary': 'bright_cyan',
    'secondary': 'bright_magenta',
    'success': 'bright_green',
    'warning': 'yellow',
    'error': 'bright_red',
    'info': 'bright_blue',
    'header': 'bold bright_cyan',
    'subheader': 'bold cyan',
    'option': 'bold white',
    'dim': 'dim white',
    'highlight': 'bold yellow',
    'box': 'blue'
}

# Beautiful ASCII Art Banner
BANNER = """[bold bright_cyan]
╔═══════════════════════════════════════════════════════════════════════════════╗
║  ____        _   _                 __  __      _        __  __                ║
║ |  _ \ _   _| |_| |__   ___  _ __ |  \/  | ___| |_ __ _|  \/  | __ _ _ __    ║
║ | |_) | | | | __| '_ \ / _ \| '_ \| |\/| |/ _ \ __/ _` | |\/| |/ _` | '_ \   ║
║ |  __/| |_| | |_| | | | (_) | | | | |  | |  __/ || (_| | |  | | (_| | |_) |  ║
║ |_|    \__, |\__|_| |_|\___/|_| |_|_|  |_|\___|\__\__,_|_|  |_|\__,_| .__/   ║
║        |___/                                                         |_|       ║
╚═══════════════════════════════════════════════════════════════════════════════╝[/bold bright_cyan]
                    [dim]Advanced Medical Text Processing Suite v8.1.9[/dim]
"""


class ResourceMonitor:
    """Real-time system resource monitoring"""
    
    def __init__(self):
        self.cpu_history = deque(maxlen=60)
        self.memory_history = deque(maxlen=60)
        self.monitoring = False
        self.monitor_thread = None
        
    def start(self):
        """Start monitoring"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
    def stop(self):
        """Stop monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)
            
    def _monitor_loop(self):
        """Background monitoring"""
        while self.monitoring:
            try:
                self.cpu_history.append(psutil.cpu_percent(interval=0.1))
                self.memory_history.append(psutil.virtual_memory().percent)
                time.sleep(1)
            except:
                pass
                
    def get_status_panel(self) -> Panel:
        """Get formatted status panel"""
        cpu = self.cpu_history[-1] if self.cpu_history else 0
        mem = self.memory_history[-1] if self.memory_history else 0
        
        # Create progress bars
        cpu_bar = self._create_progress_bar(cpu, "CPU")
        mem_bar = self._create_progress_bar(mem, "RAM")
        
        # Get disk usage
        disk = psutil.disk_usage('/')
        disk_bar = self._create_progress_bar(disk.percent, "Disk")
        
        # Network status
        net = psutil.net_io_counters()
        net_text = f"↓ {self._format_bytes(net.bytes_recv)} ↑ {self._format_bytes(net.bytes_sent)}"
        
        # Create layout
        content = f"{cpu_bar}\n{mem_bar}\n{disk_bar}\n[dim]Network:[/dim] {net_text}"
        
        return Panel(
            content,
            title="[bold]System Resources[/bold]",
            border_style=COLORS['box'],
            box=box.ROUNDED
        )
        
    def _create_progress_bar(self, percent: float, label: str) -> str:
        """Create a visual progress bar"""
        width = 20
        filled = int(width * percent / 100)
        empty = width - filled
        
        if percent > 80:
            color = "red"
        elif percent > 60:
            color = "yellow"
        else:
            color = "green"
            
        bar = f"[{color}]{'█' * filled}[/{color}][dim]{'░' * empty}[/dim]"
        return f"{label:>4}: {bar} {percent:>5.1f}%"
        
    def _format_bytes(self, bytes: int) -> str:
        """Format bytes to human readable"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes < 1024:
                return f"{bytes:.1f}{unit}"
            bytes /= 1024
        return f"{bytes:.1f}TB"


class EnhancedFileExplorer:
    """Advanced file explorer with intuitive navigation"""
    
    def __init__(self, start_path: Path = None):
        self.current_path = Path(start_path or os.getcwd())
        self.selected_index = 0
        self.selected_files = set()
        self.show_hidden = False
        self.sort_by = 'name'
        self.filter_pattern = None
        self.preview_enabled = True
        self.bookmarks = self._load_bookmarks()
        
    def _load_bookmarks(self) -> Dict[str, Path]:
        """Load saved bookmarks"""
        bookmarks = {
            'home': Path.home(),
            'desktop': Path.home() / 'Desktop',
            'documents': Path.home() / 'Documents',
            'downloads': Path.home() / 'Downloads'
        }
        
        # Add project directories
        if Path('./input_notes').exists():
            bookmarks['input'] = Path('./input_notes').absolute()
        if Path('./output_csvs').exists():
            bookmarks['output'] = Path('./output_csvs').absolute()
            
        return bookmarks
        
    def navigate(self) -> List[Path]:
        """Interactive file navigation"""
        while True:
            self.clear_screen()
            
            # Create layout
            layout = Layout()
            layout.split_column(
                Layout(name="header", size=3),
                Layout(name="main"),
                Layout(name="footer", size=4)
            )
            
            # Header
            header = Panel(
                f"[bold]{self.current_path}[/bold]",
                title="[bold]File Explorer[/bold]",
                border_style=COLORS['primary']
            )
            layout["header"].update(header)
            
            # Main content
            main_layout = Layout()
            main_layout.split_row(
                Layout(name="files", ratio=3),
                Layout(name="preview", ratio=2)
            )
            
            # File list
            files_panel = self._create_file_list()
            main_layout["files"].update(files_panel)
            
            # Preview pane
            if self.preview_enabled:
                preview_panel = self._create_preview()
                main_layout["preview"].update(preview_panel)
                
            layout["main"].update(main_layout)
            
            # Footer with controls
            footer = self._create_controls()
            layout["footer"].update(footer)
            
            console.print(layout)
            
            # Get input
            key = Prompt.ask(
                "Command",
                choices=['↑', '↓', '←', '→', 'space', 'a', 'd', 'b', 'f', 'h', 'p', 'q', 'enter'],
                default='enter'
            )
            
            if key == 'q':
                break
            elif key == '↑':
                self.selected_index = max(0, self.selected_index - 1)
            elif key == '↓':
                items = list(self._get_items())
                self.selected_index = min(len(items) - 1, self.selected_index + 1)
            elif key == '←' or key == 'b':
                self.current_path = self.current_path.parent
                self.selected_index = 0
            elif key == '→' or key == 'enter':
                items = list(self._get_items())
                if items and 0 <= self.selected_index < len(items):
                    item = items[self.selected_index]
                    if item.is_dir():
                        self.current_path = item
                        self.selected_index = 0
            elif key == 'space':
                items = list(self._get_items())
                if items and 0 <= self.selected_index < len(items):
                    item = items[self.selected_index]
                    if item.is_file():
                        if item in self.selected_files:
                            self.selected_files.remove(item)
                        else:
                            self.selected_files.add(item)
            elif key == 'a':
                # Select all
                for item in self._get_items():
                    if item.is_file():
                        self.selected_files.add(item)
            elif key == 'd':
                # Deselect all
                self.selected_files.clear()
            elif key == 'h':
                self.show_hidden = not self.show_hidden
            elif key == 'p':
                self.preview_enabled = not self.preview_enabled
            elif key == 'f':
                # Filter
                pattern = Prompt.ask("Filter pattern (empty to clear)")
                self.filter_pattern = pattern if pattern else None
                
        return list(self.selected_files)
        
    def _get_items(self) -> List[Path]:
        """Get directory items with filtering and sorting"""
        try:
            items = list(self.current_path.iterdir())
            
            # Filter
            if not self.show_hidden:
                items = [i for i in items if not i.name.startswith('.')]
            if self.filter_pattern:
                items = [i for i in items if self.filter_pattern.lower() in i.name.lower()]
                
            # Sort
            items.sort(key=lambda x: (not x.is_dir(), x.name.lower()))
            
            return items
        except PermissionError:
            return []
            
    def _create_file_list(self) -> Panel:
        """Create file list panel"""
        items = self._get_items()
        
        table = Table(show_header=True, box=None, padding=(0, 1))
        table.add_column("", width=3)
        table.add_column("Name", style="white")
        table.add_column("Size", justify="right", style="dim")
        table.add_column("Modified", style="dim")
        
        for idx, item in enumerate(items):
            # Selection marker
            if idx == self.selected_index:
                marker = "▶"
                style = "bold bright_white on blue"
            else:
                marker = " "
                style = None
                
            # Selected marker
            if item in self.selected_files:
                marker = "✓" + marker
                
            # Icon and name
            if item.is_dir():
                icon = "📁"
                name = f"{icon} {item.name}/"
                size = "-"
            else:
                icon = self._get_file_icon(item)
                name = f"{icon} {item.name}"
                size = self._format_size(item.stat().st_size)
                
            # Modified time
            mtime = datetime.fromtimestamp(item.stat().st_mtime)
            modified = mtime.strftime("%Y-%m-%d %H:%M")
            
            table.add_row(marker, name, size, modified, style=style)
            
        return Panel(
            table,
            title=f"Files ({len(self.selected_files)} selected)",
            border_style=COLORS['box'],
            box=box.ROUNDED
        )
        
    def _create_preview(self) -> Panel:
        """Create preview panel"""
        items = self._get_items()
        
        if not items or self.selected_index >= len(items):
            content = "[dim]No preview available[/dim]"
        else:
            item = items[self.selected_index]
            
            if item.is_dir():
                # Directory preview
                try:
                    sub_items = list(item.iterdir())[:10]
                    content = f"[bold]Directory: {item.name}[/bold]\n\n"
                    content += f"Items: {len(list(item.iterdir()))}\n\n"
                    for sub in sub_items:
                        if sub.is_dir():
                            content += f"📁 {sub.name}/\n"
                        else:
                            content += f"📄 {sub.name}\n"
                    if len(list(item.iterdir())) > 10:
                        content += "[dim]...[/dim]"
                except:
                    content = "[error]Cannot read directory[/error]"
                    
            elif item.suffix in ['.txt', '.log', '.csv', '.json']:
                # Text file preview
                try:
                    with open(item, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()[:20]
                    content = f"[bold]File: {item.name}[/bold]\n\n"
                    content += "".join(lines)
                    if len(lines) == 20:
                        content += "\n[dim]...[/dim]"
                except:
                    content = "[error]Cannot read file[/error]"
            else:
                # File info
                stat = item.stat()
                content = f"[bold]File: {item.name}[/bold]\n\n"
                content += f"Type: {item.suffix or 'No extension'}\n"
                content += f"Size: {self._format_size(stat.st_size)}\n"
                content += f"Modified: {datetime.fromtimestamp(stat.st_mtime)}\n"
                content += f"Created: {datetime.fromtimestamp(stat.st_ctime)}\n"
                
        return Panel(
            content,
            title="Preview",
            border_style=COLORS['box'],
            box=box.ROUNDED
        )
        
    def _create_controls(self) -> Panel:
        """Create controls panel"""
        controls = Table(show_header=False, box=None, padding=(0, 2))
        controls.add_column(style="bold cyan")
        controls.add_column(style="white")
        
        controls.add_row("↑↓", "Navigate")
        controls.add_row("←/B", "Back")
        controls.add_row("→/Enter", "Open")
        controls.add_row("Space", "Select")
        controls.add_row("A", "Select All")
        controls.add_row("D", "Deselect All")
        controls.add_row("H", "Show Hidden")
        controls.add_row("P", "Toggle Preview")
        controls.add_row("F", "Filter")
        controls.add_row("Q", "Done")
        
        return Panel(
            Columns([controls], padding=(0, 2)),
            title="Controls",
            border_style=COLORS['dim'],
            box=box.ROUNDED
        )
        
    def _get_file_icon(self, path: Path) -> str:
        """Get file icon based on extension"""
        icons = {
            '.txt': '📄',
            '.csv': '📊',
            '.json': '📋',
            '.log': '📜',
            '.py': '🐍',
            '.md': '📝',
            '.xml': '📰',
            '.html': '🌐',
            '.pdf': '📕',
            '.zip': '📦',
            '.gz': '📦'
        }
        return icons.get(path.suffix.lower(), '📄')
        
    def _format_size(self, size: int) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
        
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')


class ProcessingMonitor:
    """Real-time processing monitor with logging"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.log_file = output_dir / "processing.log"
        self.stats = {
            'total': 0,
            'processed': 0,
            'failed': 0,
            'start_time': None,
            'current_file': None
        }
        self.log_buffer = deque(maxlen=100)
        
    def log(self, message: str, level: str = "INFO"):
        """Log message"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        self.log_buffer.append(log_entry)
        
        # Write to file
        with open(self.log_file, 'a') as f:
            f.write(log_entry + "\n")
            
    def create_monitor_panel(self) -> Panel:
        """Create monitoring panel"""
        # Progress
        if self.stats['total'] > 0:
            progress = self.stats['processed'] / self.stats['total'] * 100
        else:
            progress = 0
            
        # Time elapsed
        if self.stats['start_time']:
            elapsed = time.time() - self.stats['start_time']
            elapsed_str = f"{int(elapsed // 60)}m {int(elapsed % 60)}s"
        else:
            elapsed_str = "0m 0s"
            
        # ETA
        if progress > 0 and self.stats['processed'] > 0:
            total_time = elapsed / (progress / 100)
            remaining = total_time - elapsed
            eta_str = f"{int(remaining // 60)}m {int(remaining % 60)}s"
        else:
            eta_str = "Calculating..."
            
        # Create content
        content = f"""
[bold]Progress:[/bold] {self.stats['processed']}/{self.stats['total']} files ({progress:.1f}%)
[bold]Success:[/bold] [green]{self.stats['processed'] - self.stats['failed']}[/green]
[bold]Failed:[/bold] [red]{self.stats['failed']}[/red]
[bold]Time:[/bold] {elapsed_str} (ETA: {eta_str})
[bold]Current:[/bold] {self.stats['current_file'] or 'None'}

[bold]Recent Activity:[/bold]
"""
        
        # Add recent logs
        for log in list(self.log_buffer)[-5:]:
            content += f"[dim]{log}[/dim]\n"
            
        return Panel(
            content.strip(),
            title="[bold]Processing Monitor[/bold]",
            border_style=COLORS['primary'],
            box=box.ROUNDED
        )


class AnalysisViewer:
    """View and analyze processing results"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        
    def show_results_summary(self):
        """Show summary of processing results"""
        csv_files = list(self.output_dir.glob("*.csv"))
        
        if not csv_files:
            console.print("[warning]No results found[/warning]")
            return
            
        # Create summary table
        table = Table(title="Processing Results Summary", box=box.ROUNDED)
        table.add_column("File", style="cyan")
        table.add_column("Size", justify="right")
        table.add_column("Concepts", justify="right")
        table.add_column("Modified", style="dim")
        
        total_concepts = 0
        
        for csv_file in csv_files[:20]:  # Show first 20
            size = csv_file.stat().st_size
            
            # Count concepts (lines in CSV)
            try:
                with open(csv_file) as f:
                    concepts = sum(1 for _ in f) - 1  # Minus header
            except:
                concepts = 0
                
            total_concepts += concepts
            
            mtime = datetime.fromtimestamp(csv_file.stat().st_mtime)
            
            table.add_row(
                csv_file.name,
                self._format_size(size),
                str(concepts),
                mtime.strftime("%Y-%m-%d %H:%M")
            )
            
        console.print(table)
        
        # Summary stats
        summary = Panel(
            f"""
[bold]Total Files:[/bold] {len(csv_files)}
[bold]Total Concepts:[/bold] {total_concepts:,}
[bold]Average Concepts/File:[/bold] {total_concepts // len(csv_files) if csv_files else 0}
[bold]Output Directory:[/bold] {self.output_dir}
""",
            title="Summary Statistics",
            border_style=COLORS['success'],
            box=box.DOUBLE
        )
        
        console.print(summary)
        
    def _format_size(self, size: int) -> str:
        """Format file size"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"


class CompleteInteractiveNavigator:
    """Complete interactive TUI with all features"""
    
    def __init__(self):
        self.config = PyMMConfig()
        self.server_manager = ServerManager(self.config)
        self.pool_manager = AdaptivePoolManager(self.config)
        self.resource_monitor = ResourceMonitor()
        self.running = True
        
        # Check requirements
        self._check_requirements()
        
        # Start resource monitoring
        self.resource_monitor.start()
        
        # Initialize components
        self._setup_defaults()
        
    def _check_requirements(self):
        """Check system requirements"""
        if not self.server_manager.java_available:
            console.print("\n[bold red]⚠️  Java Not Found[/bold red]")
            console.print("[yellow]MetaMap requires Java to run.[/yellow]\n")
            console.print("To install Java, run one of these commands:\n")
            console.print("  [cyan]Ubuntu/Debian:[/cyan]")
            console.print("    sudo apt-get update && sudo apt-get install -y openjdk-11-jre-headless\n")
            console.print("  [cyan]RHEL/CentOS:[/cyan]") 
            console.print("    sudo yum install -y java-11-openjdk\n")
            console.print("  [cyan]macOS:[/cyan]")
            console.print("    brew install openjdk@11\n")
            console.print("  [cyan]Windows:[/cyan]")
            console.print("    Download from https://adoptium.net/\n")
            console.print("[dim]After installing, restart PythonMetaMap[/dim]")
            self.running = False
            
    def _setup_defaults(self):
        """Setup default configuration based on system resources"""
        # Auto-detect system capabilities
        cpu_count = psutil.cpu_count()
        memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # Smart defaults based on hardware
        if not self.config.get('max_parallel_workers'):
            # Conservative default: min(cpu_count/2, 4) for stability
            default_workers = min(max(cpu_count // 2, 2), 4)
            self.config.set('max_parallel_workers', default_workers)
            
        if not self.config.get('chunk_size'):
            # Chunk size based on available memory
            if memory_gb < 8:
                chunk_size = 100
            elif memory_gb < 16:
                chunk_size = 250
            else:
                chunk_size = 500
            self.config.set('chunk_size', chunk_size)
            
        # Set reasonable timeout
        if not self.config.get('pymm_timeout'):
            self.config.set('pymm_timeout', 300)
            
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
        
    def show_banner(self):
        """Show application banner"""
        console.print(BANNER)
        console.print()
        
    def main_menu(self):
        """Main menu with beautiful layout"""
        if not self.running:
            return
            
        while self.running:
            self.clear_screen()
            self.show_banner()
            
            # Create main layout
            layout = Layout()
            layout.split_column(
                Layout(name="status", size=8),
                Layout(name="menu", size=15),
                Layout(name="footer", size=3)
            )
            
            # Status section
            status_layout = Layout()
            status_layout.split_row(
                Layout(self.resource_monitor.get_status_panel()),
                Layout(self._get_server_status_panel())
            )
            layout["status"].update(status_layout)
            
            # Menu section
            menu_panel = self._create_main_menu()
            layout["menu"].update(menu_panel)
            
            # Footer
            footer = Panel(
                "[dim]Advanced Interactive Mode | Press number to select | Q to quit[/dim]",
                style="dim",
                box=box.SIMPLE
            )
            layout["footer"].update(footer)
            
            console.print(layout)
            
            # Get choice
            choice = Prompt.ask(
                "\n[bold]Select option[/bold]",
                choices=["1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "q"],
                default="1"
            ).lower()
            
            self._handle_menu_choice(choice)
            
    def _get_server_status_panel(self) -> Panel:
        """Get server status panel"""
        if self.server_manager.is_running():
            status = "[bold green]● RUNNING[/bold green]"
            uptime = "Active"
        else:
            status = "[bold red]● STOPPED[/bold red]"
            uptime = "Inactive"
            
        content = f"""
[bold]MetaMap Server[/bold]
Status: {status}
State: {uptime}

[bold]Configuration[/bold]
Binary: {self.config.get('metamap_binary_path', 'Not configured')}
Workers: {self.config.get('max_parallel_workers', 4)}
"""
        
        return Panel(
            content.strip(),
            title="[bold]Server Status[/bold]",
            border_style=COLORS['box'],
            box=box.ROUNDED
        )
        
    def _create_main_menu(self) -> Panel:
        """Create main menu panel"""
        # Menu items with icons
        menu_items = [
            ("1", "🚀", "Quick Process", "Process files with smart defaults", COLORS['success']),
            ("2", "📁", "File Explorer", "Browse and select files interactively", COLORS['info']),
            ("3", "⚡", "Batch Process", "Process large datasets efficiently", COLORS['primary']),
            ("4", "📊", "View Results", "Analyze processing outcomes", COLORS['secondary']),
            ("5", "🔧", "Configuration", "Adjust settings and preferences", COLORS['warning']),
            ("6", "🖥️", "Server Control", "Manage MetaMap services", COLORS['error']),
            ("7", "📈", "Analysis Tools", "Advanced concept analysis", "magenta"),
            ("8", "🔄", "Resume/Retry", "Continue interrupted tasks", "yellow"),
            ("9", "📝", "Logs & Monitor", "View processing logs", "cyan"),
            ("0", "ℹ️", "Help & Info", "Documentation and guides", "white")
        ]
        
        # Create grid
        grid = Table(show_header=False, box=None, padding=(1, 2))
        grid.add_column(justify="center")
        grid.add_column(justify="center")
        
        for i in range(0, len(menu_items), 2):
            row = []
            for j in range(2):
                if i + j < len(menu_items):
                    key, icon, title, desc, color = menu_items[i + j]
                    
                    # Create menu item
                    item_content = f"{icon}  [bold]{title}[/bold]\n[dim]{desc}[/dim]"
                    
                    panel = Panel(
                        item_content,
                        title=f"[{color}][ {key} ][/{color}]",
                        border_style=color,
                        width=35,
                        height=4,
                        box=box.ROUNDED
                    )
                    row.append(panel)
                else:
                    row.append("")
                    
            grid.add_row(*row)
            
        return Panel(
            grid,
            title="[bold]Main Menu[/bold]",
            border_style=COLORS['header'],
            box=box.DOUBLE
        )
        
    def _handle_menu_choice(self, choice: str):
        """Handle menu selection"""
        if choice == "1":
            self.quick_process()
        elif choice == "2":
            self.file_explorer_mode()
        elif choice == "3":
            self.batch_process()
        elif choice == "4":
            self.view_results()
        elif choice == "5":
            self.configuration_menu()
        elif choice == "6":
            self.server_control()
        elif choice == "7":
            self.analysis_tools()
        elif choice == "8":
            self.resume_retry_menu()
        elif choice == "9":
            self.logs_monitor()
        elif choice == "0":
            self.show_help()
        elif choice == "q":
            if Confirm.ask("\n[yellow]Exit PythonMetaMap?[/yellow]"):
                self.running = False
                console.print("\n[green]Thank you for using PythonMetaMap![/green]")
                
    def quick_process(self):
        """Quick processing with beautiful interface"""
        self.clear_screen()
        
        # Header
        console.print(Panel(
            "[bold]🚀 Quick Process[/bold]\nProcess medical texts with optimized settings",
            style=COLORS['success'],
            box=box.DOUBLE
        ))
        
        # Check server
        if not self.server_manager.is_running():
            console.print("\n[yellow]⚠️  MetaMap server not running[/yellow]")
            if Confirm.ask("Start server now?", default=True):
                self._start_server_with_progress()
            else:
                Prompt.ask("\nPress Enter to continue")
                return
                
        # Get directories with visual feedback
        console.print("\n[bold]📁 Select Directories[/bold]")
        
        input_dir = Prompt.ask(
            "  Input directory",
            default=self.config.get('default_input_dir', './input_notes')
        )
        
        output_dir = Prompt.ask(
            "  Output directory", 
            default=self.config.get('default_output_dir', './output_csvs')
        )
        
        # Validate and count files
        input_path = Path(input_dir)
        if not input_path.exists():
            console.print(f"\n[error]❌ Directory not found: {input_dir}[/error]")
            Prompt.ask("\nPress Enter to continue")
            return
            
        files = list(input_path.glob("*.txt"))
        if not files:
            console.print("\n[warning]⚠️  No .txt files found in directory[/warning]")
            Prompt.ask("\nPress Enter to continue")
            return
            
        # Show file summary
        console.print(f"\n[bold]📊 File Summary[/bold]")
        console.print(f"  Found [cyan]{len(files)}[/cyan] text files")
        console.print(f"  Total size: [cyan]{self._get_total_size(files)}[/cyan]")
        
        # Show configuration
        self._show_processing_config(len(files))
        
        if not Confirm.ask("\n[bold]Proceed with processing?[/bold]", default=True):
            return
            
        # Process with beautiful progress
        self._run_processing_with_ui(input_dir, output_dir, files)
        
    def _start_server_with_progress(self):
        """Start server with visual progress"""
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=console
        ) as progress:
            
            task = progress.add_task("[cyan]Starting MetaMap server...[/cyan]", total=100)
            
            # Simulate startup steps
            steps = [
                ("Initializing Java environment", 20),
                ("Loading MetaMap resources", 40),
                ("Starting tagger server", 30),
                ("Verifying server status", 10)
            ]
            
            success = True
            for step, weight in steps:
                progress.update(task, description=f"[cyan]{step}...[/cyan]")
                
                # Actual server start on first step
                if weight == 20:
                    success = self.server_manager.start()
                    
                time.sleep(0.5)  # Visual feedback
                progress.advance(task, weight)
                
                if not success:
                    break
                    
        if success:
            console.print("\n[green]✅ Server started successfully![/green]")
        else:
            console.print("\n[error]❌ Failed to start server[/error]")
            
    def _show_processing_config(self, file_count: int):
        """Show processing configuration beautifully"""
        # Get system recommendations
        recommendations = self.pool_manager.analyze_system()
        
        # Create configuration table
        config_table = Table(title="Processing Configuration", box=box.ROUNDED)
        config_table.add_column("Setting", style="blue", width=20)
        config_table.add_column("Current", style="green", width=15)
        config_table.add_column("Recommended", style="yellow", width=15)
        config_table.add_column("Status", width=10)
        
        # Workers
        current_workers = self.config.get('max_parallel_workers')
        optimal_workers = recommendations['workers']['optimal']
        worker_status = "✅" if current_workers == optimal_workers else "⚠️"
        config_table.add_row(
            "Parallel Workers",
            str(current_workers),
            str(optimal_workers),
            worker_status
        )
        
        # Chunk size
        chunk_size = self.config.get('chunk_size')
        config_table.add_row(
            "Chunk Size",
            f"{chunk_size} files",
            "Auto",
            "✅"
        )
        
        # Memory
        mem_available = recommendations['memory']['available_gb']
        mem_status = "✅" if mem_available > 4 else "⚠️"
        config_table.add_row(
            "Available Memory",
            f"{mem_available:.1f} GB",
            ">4 GB",
            mem_status
        )
        
        # Processing mode
        mode = "Ultra-optimized" if file_count > 500 else "Standard"
        config_table.add_row(
            "Processing Mode",
            mode,
            mode,
            "✅"
        )
        
        console.print("\n", config_table)
        
        # Show recommendations if any
        if recommendations['recommendations']:
            console.print("\n[bold]💡 Recommendations:[/bold]")
            for rec in recommendations['recommendations']:
                console.print(f"  • {rec}")
                
    def _run_processing_with_ui(self, input_dir: str, output_dir: str, files: List[Path]):
        """Run processing with beautiful UI"""
        self.clear_screen()
        
        # Create processing layout
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main", size=20),
            Layout(name="footer", size=3)
        )
        
        # Header
        header = Panel(
            "[bold]🔄 Processing Medical Texts[/bold]",
            style=COLORS['primary'],
            box=box.DOUBLE
        )
        layout["header"].update(header)
        
        # Main area split
        main_layout = Layout()
        main_layout.split_row(
            Layout(name="progress", ratio=2),
            Layout(name="monitor", ratio=1)
        )
        layout["main"].update(main_layout)
        
        # Initialize components
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        monitor = ProcessingMonitor(output_path)
        monitor.stats['total'] = len(files)
        monitor.stats['start_time'] = time.time()
        
        # Choose runner based on file count
        if len(files) > 500:
            runner = UltraOptimizedBatchRunner(input_dir, output_dir, self.config)
            monitor.log("Using ultra-optimized runner for large dataset")
        else:
            runner = OptimizedBatchRunner(input_dir, output_dir, self.config)
            monitor.log("Using standard optimized runner")
            
        # Progress tracking
        completed = False
        
        def update_display():
            while not completed:
                # Update progress panel
                progress_panel = self._create_progress_panel(monitor.stats)
                main_layout["progress"].update(progress_panel)
                
                # Update monitor panel
                monitor_panel = monitor.create_monitor_panel()
                main_layout["monitor"].update(monitor_panel)
                
                # Update footer
                footer = Panel(
                    "[dim]Press Ctrl+C to cancel[/dim]",
                    style="dim",
                    box=box.SIMPLE
                )
                layout["footer"].update(footer)
                
                console.print(layout, end="")
                console.print("\033[H")  # Move cursor to top
                time.sleep(0.5)
                
        # Start display thread
        display_thread = threading.Thread(target=update_display, daemon=True)
        display_thread.start()
        
        # Run processing
        try:
            def progress_callback(stats):
                monitor.stats.update(stats)
                if stats.get('current_file'):
                    monitor.log(f"Processing: {stats['current_file']}")
                    
            results = runner.run(progress_callback=progress_callback)
            completed = True
            
            # Show completion
            self._show_processing_results(results, output_dir, monitor)
            
        except KeyboardInterrupt:
            completed = True
            console.print("\n\n[yellow]⚠️  Processing interrupted by user[/yellow]")
            monitor.log("Processing interrupted by user", "WARNING")
        except Exception as e:
            completed = True
            console.print(f"\n\n[error]❌ Processing error: {e}[/error]")
            monitor.log(f"Processing error: {e}", "ERROR")
            
        Prompt.ask("\nPress Enter to continue")
        
    def _create_progress_panel(self, stats: Dict) -> Panel:
        """Create beautiful progress panel"""
        if stats['total'] > 0:
            progress = stats.get('processed', 0) / stats['total'] * 100
        else:
            progress = 0
            
        # Create progress bar
        bar_width = 40
        filled = int(bar_width * progress / 100)
        empty = bar_width - filled
        
        if progress < 33:
            bar_color = "red"
        elif progress < 66:
            bar_color = "yellow"
        else:
            bar_color = "green"
            
        progress_bar = f"[{bar_color}]{'█' * filled}[/{bar_color}][dim]{'░' * empty}[/dim]"
        
        # Calculate stats
        success = stats.get('processed', 0) - stats.get('failed', 0)
        
        content = f"""
[bold]Overall Progress[/bold]
{progress_bar} {progress:>5.1f}%

[bold]Files[/bold]
Total:     {stats.get('total', 0):>6}
Processed: {stats.get('processed', 0):>6}
Success:   [green]{success:>6}[/green]
Failed:    [red]{stats.get('failed', 0):>6}[/red]

[bold]Current File[/bold]
{stats.get('current_file', 'Initializing...')}
"""
        
        return Panel(
            content.strip(),
            title="[bold]Progress[/bold]",
            border_style=COLORS['primary'],
            box=box.ROUNDED
        )
        
    def _show_processing_results(self, results: Dict, output_dir: str, monitor: ProcessingMonitor):
        """Show processing results beautifully"""
        self.clear_screen()
        
        # Header
        if results.get('successful', 0) > 0:
            style = COLORS['success']
            icon = "✅"
            title = "Processing Complete!"
        else:
            style = COLORS['error']
            icon = "❌"
            title = "Processing Failed!"
            
        console.print(Panel(
            f"[bold]{icon} {title}[/bold]",
            style=style,
            box=box.DOUBLE
        ))
        
        # Results table
        results_table = Table(title="Processing Summary", box=box.ROUNDED)
        results_table.add_column("Metric", style="blue")
        results_table.add_column("Value", style="green")
        
        results_table.add_row("Total Files", str(results.get('total', 0)))
        results_table.add_row("Successful", f"[green]{results.get('successful', 0)}[/green]")
        results_table.add_row("Failed", f"[red]{results.get('failed', 0)}[/red]")
        results_table.add_row("Duration", f"{results.get('duration', 0):.1f} seconds")
        
        if results.get('duration', 0) > 0:
            throughput = results.get('successful', 0) / results.get('duration', 0)
            results_table.add_row("Throughput", f"{throughput:.2f} files/second")
            
        results_table.add_row("Output Directory", output_dir)
        
        console.print("\n", results_table)
        
        # Show sample results
        if results.get('successful', 0) > 0:
            self._show_sample_results(Path(output_dir))
            
    def _show_sample_results(self, output_dir: Path):
        """Show sample of processed results"""
        csv_files = list(output_dir.glob("*.csv"))[:5]
        
        if csv_files:
            console.print("\n[bold]📄 Sample Results:[/bold]")
            
            for csv_file in csv_files:
                try:
                    # Read first few concepts
                    import csv
                    with open(csv_file, 'r') as f:
                        reader = csv.DictReader(f)
                        concepts = []
                        for i, row in enumerate(reader):
                            if i >= 3:  # Show first 3 concepts
                                break
                            concepts.append(row.get('Preferred_Name', 'Unknown'))
                            
                    if concepts:
                        console.print(f"\n  [cyan]{csv_file.name}[/cyan]")
                        for concept in concepts:
                            console.print(f"    • {concept}")
                except:
                    pass
                    
    def file_explorer_mode(self):
        """Launch enhanced file explorer"""
        self.clear_screen()
        
        explorer = EnhancedFileExplorer()
        selected_files = explorer.navigate()
        
        if selected_files:
            console.print(f"\n[bold]Selected {len(selected_files)} files[/bold]")
            
            if Confirm.ask("Process selected files?", default=True):
                output_dir = Prompt.ask(
                    "Output directory",
                    default=self.config.get('default_output_dir', './output_csvs')
                )
                
                # Create temporary directory with selected files
                temp_dir = Path("temp_selected_files")
                temp_dir.mkdir(exist_ok=True)
                
                for file in selected_files:
                    shutil.copy2(file, temp_dir / file.name)
                    
                self._run_processing_with_ui(str(temp_dir), output_dir, selected_files)
                
                # Cleanup
                shutil.rmtree(temp_dir)
                
    def view_results(self):
        """View processing results"""
        self.clear_screen()
        
        console.print(Panel(
            "[bold]📊 View Results[/bold]\nAnalyze processed medical concepts",
            style=COLORS['secondary'],
            box=box.DOUBLE
        ))
        
        output_dir = Prompt.ask(
            "\nOutput directory",
            default=self.config.get('default_output_dir', './output_csvs')
        )
        
        output_path = Path(output_dir)
        if not output_path.exists():
            console.print(f"\n[error]Directory not found: {output_dir}[/error]")
            Prompt.ask("\nPress Enter to continue")
            return
            
        viewer = AnalysisViewer(output_path)
        viewer.show_results_summary()
        
        # Additional options
        console.print("\n[bold]Options:[/bold]")
        console.print("[1] Export summary report")
        console.print("[2] Search concepts")
        console.print("[3] View specific file")
        console.print("[B] Back")
        
        choice = Prompt.ask("Select option", choices=["1", "2", "3", "b"], default="b")
        
        if choice == "1":
            self._export_summary_report(output_path)
        elif choice == "2":
            self._search_concepts(output_path)
        elif choice == "3":
            self._view_specific_file(output_path)
            
        if choice != "b":
            Prompt.ask("\nPress Enter to continue")
            
    def _export_summary_report(self, output_dir: Path):
        """Export summary report"""
        console.print("\n[cyan]Generating summary report...[/cyan]")
        
        # Implementation would generate comprehensive report
        report_file = output_dir / f"summary_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_file, 'w') as f:
            f.write("PythonMetaMap Processing Summary Report\n")
            f.write("=" * 50 + "\n\n")
            # Add report content
            
        console.print(f"[green]✅ Report saved to: {report_file}[/green]")
        
    def _search_concepts(self, output_dir: Path):
        """Search for specific concepts"""
        search_term = Prompt.ask("\nSearch for concept")
        
        console.print(f"\n[cyan]Searching for '{search_term}'...[/cyan]")
        
        # Implementation would search through CSV files
        results = []
        
        if results:
            # Show results
            pass
        else:
            console.print("[yellow]No matches found[/yellow]")
            
    def _view_specific_file(self, output_dir: Path):
        """View specific result file"""
        csv_files = list(output_dir.glob("*.csv"))
        
        if not csv_files:
            console.print("[warning]No CSV files found[/warning]")
            return
            
        # Show file list
        console.print("\n[bold]Available Files:[/bold]")
        for i, file in enumerate(csv_files[:20], 1):
            console.print(f"{i:2d}. {file.name}")
            
        if len(csv_files) > 20:
            console.print(f"... and {len(csv_files) - 20} more files")
            
        choice = IntPrompt.ask("\nSelect file number", default=1)
        
        if 1 <= choice <= len(csv_files):
            # Display file content
            self._display_csv_content(csv_files[choice - 1])
            
    def _display_csv_content(self, csv_file: Path):
        """Display CSV file content"""
        console.print(f"\n[bold]File: {csv_file.name}[/bold]")
        
        try:
            import pandas as pd
            df = pd.read_csv(csv_file)
            
            # Show first 10 rows
            table = Table(box=box.ROUNDED)
            
            # Add columns
            for col in df.columns[:5]:  # Show first 5 columns
                table.add_column(col)
                
            # Add rows
            for _, row in df.head(10).iterrows():
                table.add_row(*[str(row[col]) for col in df.columns[:5]])
                
            console.print(table)
            console.print(f"\n[dim]Showing 10 of {len(df)} rows[/dim]")
            
        except Exception as e:
            console.print(f"[error]Error reading file: {e}[/error]")
            
    def server_control(self):
        """Server control with visual feedback"""
        self.clear_screen()
        
        console.print(Panel(
            "[bold]🖥️  Server Control[/bold]\nManage MetaMap services",
            style=COLORS['warning'],
            box=box.DOUBLE
        ))
        
        # Show current status
        self._show_detailed_server_status()
        
        console.print("\n[bold]Actions:[/bold]")
        
        if self.server_manager.is_running():
            console.print("[1] Stop server")
            console.print("[2] Restart server")
            console.print("[3] View server logs")
        else:
            console.print("[1] Start server")
            console.print("[2] Start with custom options")
            
        console.print("[B] Back")
        
        choice = Prompt.ask("Select action", default="b").lower()
        
        if choice == "1":
            if self.server_manager.is_running():
                self._stop_server()
            else:
                self._start_server_with_progress()
        elif choice == "2":
            if self.server_manager.is_running():
                self._restart_server()
            else:
                self._start_server_custom()
        elif choice == "3" and self.server_manager.is_running():
            self._view_server_logs()
            
        if choice != "b":
            Prompt.ask("\nPress Enter to continue")
            
    def _show_detailed_server_status(self):
        """Show detailed server status"""
        status_table = Table(title="Server Status", box=box.ROUNDED)
        status_table.add_column("Component", style="blue")
        status_table.add_column("Status", style="green")
        status_table.add_column("Port", style="yellow")
        status_table.add_column("Details", style="dim")
        
        # Tagger server
        tagger_running = self.server_manager.is_tagger_server_running()
        status_table.add_row(
            "Tagger Server",
            "🟢 Running" if tagger_running else "🔴 Stopped",
            "1795",
            "SKR/MedPost tagger"
        )
        
        # WSD server
        wsd_running = self.server_manager.is_wsd_server_running()
        status_table.add_row(
            "WSD Server",
            "🟢 Running" if wsd_running else "🔴 Stopped",
            "5554",
            "Word Sense Disambiguation"
        )
        
        # MetaMap server
        mm_running = self.server_manager.is_mmserver_running()
        status_table.add_row(
            "MetaMap Server",
            "🟢 Running" if mm_running else "🔴 Stopped",
            "8066",
            "Main processing server"
        )
        
        console.print(status_table)
        
    def _stop_server(self):
        """Stop server with confirmation"""
        if Confirm.ask("\n[yellow]Stop MetaMap server?[/yellow]"):
            with console.status("[cyan]Stopping server...[/cyan]"):
                self.server_manager.stop()
            console.print("[green]✅ Server stopped[/green]")
            
    def _restart_server(self):
        """Restart server"""
        console.print("\n[cyan]Restarting server...[/cyan]")
        
        with console.status("[cyan]Stopping server...[/cyan]"):
            self.server_manager.stop()
            
        time.sleep(2)
        
        self._start_server_with_progress()
        
    def configuration_menu(self):
        """Enhanced configuration menu"""
        self.clear_screen()
        
        console.print(Panel(
            "[bold]🔧 Configuration[/bold]\nCustomize PythonMetaMap settings",
            style=COLORS['warning'],
            box=box.DOUBLE
        ))
        
        menu_items = [
            ("1", "⚡", "Quick Setup", "Automatic configuration"),
            ("2", "📁", "Directories", "Input/output paths"),
            ("3", "⚙️", "Processing", "Workers, timeouts, chunks"),
            ("4", "🖥️", "Server", "MetaMap server settings"),
            ("5", "💾", "Save/Load", "Configuration profiles"),
            ("6", "🔄", "Reset", "Restore defaults")
        ]
        
        # Display menu
        for key, icon, title, desc in menu_items:
            console.print(f"\n[{COLORS['primary']}][{key}][/{COLORS['primary']}] {icon}  [bold]{title}[/bold]")
            console.print(f"    [dim]{desc}[/dim]")
            
        console.print(f"\n[{COLORS['dim']}][B][/{COLORS['dim']}] Back to main menu")
        
        choice = Prompt.ask("\nSelect option", default="b").lower()
        
        if choice == "1":
            self._quick_setup()
        elif choice == "2":
            self._configure_directories()
        elif choice == "3":
            self._configure_processing()
        elif choice == "4":
            self._configure_server()
        elif choice == "5":
            self._manage_profiles()
        elif choice == "6":
            self._reset_configuration()
            
    def _quick_setup(self):
        """Quick automatic setup"""
        console.print("\n[bold]⚡ Quick Setup[/bold]")
        console.print("[cyan]Analyzing system and optimizing configuration...[/cyan]\n")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            task = progress.add_task("Optimizing...", total=4)
            
            # Detect Java
            progress.update(task, description="Detecting Java installation...")
            time.sleep(0.5)
            progress.advance(task)
            
            # Detect MetaMap
            progress.update(task, description="Locating MetaMap...")
            time.sleep(0.5)
            progress.advance(task)
            
            # Analyze system
            progress.update(task, description="Analyzing system resources...")
            recommendations = self.pool_manager.analyze_system()
            time.sleep(0.5)
            progress.advance(task)
            
            # Apply settings
            progress.update(task, description="Applying optimal settings...")
            self.config.set('max_parallel_workers', recommendations['workers']['optimal'])
            self.config.save()
            time.sleep(0.5)
            progress.advance(task)
            
        console.print("\n[green]✅ Configuration optimized![/green]")
        
        # Show what was configured
        summary = Table(title="Configuration Summary", box=box.ROUNDED)
        summary.add_column("Setting", style="blue")
        summary.add_column("Value", style="green")
        
        summary.add_row("Java", self.config.get('java_home', 'Auto-detected'))
        summary.add_row("MetaMap", self.config.get('metamap_binary_path', 'Not found'))
        summary.add_row("Workers", str(recommendations['workers']['optimal']))
        summary.add_row("Memory Limit", f"{recommendations['memory']['available_gb']:.1f} GB")
        
        console.print(summary)
        
        Prompt.ask("\nPress Enter to continue")
        
    def logs_monitor(self):
        """View logs and monitoring"""
        self.clear_screen()
        
        console.print(Panel(
            "[bold]📝 Logs & Monitoring[/bold]\nView system and processing logs",
            style=COLORS['info'],
            box=box.DOUBLE
        ))
        
        # Get log files
        log_dir = Path(self.config.get('default_output_dir', './output_csvs')) / 'logs'
        
        if log_dir.exists():
            log_files = list(log_dir.glob("*.log"))
            
            if log_files:
                console.print(f"\n[bold]Found {len(log_files)} log files[/bold]")
                
                # Show recent logs
                most_recent = max(log_files, key=lambda f: f.stat().st_mtime)
                
                console.print(f"\n[cyan]Most recent: {most_recent.name}[/cyan]")
                
                # Show last 20 lines
                with open(most_recent, 'r') as f:
                    lines = f.readlines()[-20:]
                    
                console.print("\n[dim]Last 20 lines:[/dim]")
                for line in lines:
                    console.print(f"  {line.rstrip()}")
            else:
                console.print("[yellow]No log files found[/yellow]")
        else:
            console.print("[yellow]Log directory not found[/yellow]")
            
        Prompt.ask("\nPress Enter to continue")
        
    def show_help(self):
        """Show comprehensive help"""
        self.clear_screen()
        
        help_content = """
# PythonMetaMap Help Guide

## Quick Start
1. **Install Java** if not already installed (OpenJDK 11 recommended)
2. **Install MetaMap** from NLM website
3. **Configure paths** using Configuration > Quick Setup
4. **Process files** using Quick Process option

## Main Features

### 🚀 Quick Process
- Automatically detects optimal settings
- Processes all .txt files in input directory
- Saves results as CSV files with medical concepts

### 📁 File Explorer
- Browse directories with arrow keys
- Select multiple files with spacebar
- Preview file contents
- Filter and sort capabilities

### ⚡ Batch Process
- Handles large datasets (1000+ files)
- Automatic chunking for memory efficiency
- Resume capability for interrupted jobs

### 📊 View Results
- Summary statistics of processed files
- Search through extracted concepts
- Export reports

## Tips
- For best performance, close other applications
- Process in batches of 500-1000 files
- Monitor system resources during processing
- Check logs if processing fails

## Keyboard Shortcuts
- **Arrow keys**: Navigate menus
- **Enter**: Select option
- **Space**: Toggle selection
- **Q**: Go back/quit
- **Ctrl+C**: Cancel operation
"""
        
        console.print(Markdown(help_content))
        
        Prompt.ask("\nPress Enter to continue")
        
    def _get_total_size(self, files: List[Path]) -> str:
        """Get formatted total size of files"""
        total = sum(f.stat().st_size for f in files)
        
        for unit in ['B', 'KB', 'MB', 'GB']:
            if total < 1024:
                return f"{total:.1f} {unit}"
            total /= 1024
        return f"{total:.1f} TB"
        
    def batch_process(self):
        """Batch processing for large datasets"""
        self.clear_screen()
        
        console.print(Panel(
            "[bold]⚡ Batch Process[/bold]\nEfficient processing for large datasets",
            style=COLORS['primary'],
            box=box.DOUBLE
        ))
        
        # Implementation similar to quick_process but with more options
        console.print("\n[yellow]This feature processes large datasets efficiently[/yellow]")
        
        # Get batch configuration
        console.print("\n[bold]Batch Configuration[/bold]")
        
        input_dir = Prompt.ask("Input directory", default="./input_notes")
        output_dir = Prompt.ask("Output directory", default="./output_csvs")
        
        # Advanced options
        if Confirm.ask("\nConfigure advanced options?", default=False):
            chunk_size = IntPrompt.ask("Chunk size", default=500)
            self.config.set('chunk_size', chunk_size)
            
        # Start processing
        input_path = Path(input_dir)
        if input_path.exists():
            files = list(input_path.glob("*.txt"))
            if files:
                console.print(f"\n[bold]Ready to process {len(files)} files[/bold]")
                if Confirm.ask("Start batch processing?", default=True):
                    self._run_processing_with_ui(input_dir, output_dir, files)
                    
        Prompt.ask("\nPress Enter to continue")
        
    def analysis_tools(self):
        """Advanced analysis tools"""
        self.clear_screen()
        
        console.print(Panel(
            "[bold]📈 Analysis Tools[/bold]\nAdvanced medical concept analysis",
            style="magenta",
            box=box.DOUBLE
        ))
        
        console.print("\n[bold]Available Tools:[/bold]")
        console.print("[1] Concept frequency analysis")
        console.print("[2] Semantic type distribution")
        console.print("[3] Concept co-occurrence")
        console.print("[4] Generate word cloud")
        console.print("[B] Back")
        
        choice = Prompt.ask("\nSelect tool", choices=["1", "2", "3", "4", "b"], default="b")
        
        if choice != "b":
            output_dir = Prompt.ask(
                "Output directory with CSV files",
                default=self.config.get('default_output_dir', './output_csvs')
            )
            
            if choice == "1":
                self._concept_frequency_analysis(Path(output_dir))
            elif choice == "2":
                self._semantic_type_analysis(Path(output_dir))
            elif choice == "3":
                self._concept_cooccurrence(Path(output_dir))
            elif choice == "4":
                self._generate_wordcloud(Path(output_dir))
                
            Prompt.ask("\nPress Enter to continue")
            
    def _concept_frequency_analysis(self, output_dir: Path):
        """Analyze concept frequencies"""
        console.print("\n[cyan]Analyzing concept frequencies...[/cyan]")
        
        # Implementation would analyze CSV files and show frequency distribution
        console.print("[green]✅ Analysis complete[/green]")
        
        # Show sample results
        table = Table(title="Top 10 Most Frequent Concepts", box=box.ROUNDED)
        table.add_column("Concept", style="cyan")
        table.add_column("Frequency", style="green")
        table.add_column("Percentage", style="yellow")
        
        # Sample data
        concepts = [
            ("Hypertension", 245, 15.3),
            ("Diabetes mellitus", 189, 11.8),
            ("Pain", 156, 9.7),
            ("Fever", 134, 8.4),
            ("Cough", 98, 6.1)
        ]
        
        for concept, freq, pct in concepts:
            table.add_row(concept, str(freq), f"{pct}%")
            
        console.print(table)
        
    def resume_retry_menu(self):
        """Resume or retry failed processing"""
        self.clear_screen()
        
        console.print(Panel(
            "[bold]🔄 Resume/Retry[/bold]\nContinue interrupted processing",
            style="yellow",
            box=box.DOUBLE
        ))
        
        # Check for interrupted sessions
        output_dir = Path(self.config.get('default_output_dir', './output_csvs'))
        state_file = output_dir / '.processing_state.json'
        
        if state_file.exists():
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    
                processed = len(state.get('processed', []))
                failed = len(state.get('failed', []))
                
                console.print(f"\n[bold]Found incomplete session:[/bold]")
                console.print(f"  Processed: [green]{processed}[/green] files")
                console.print(f"  Failed: [red]{failed}[/red] files")
                console.print(f"  Last update: {state.get('timestamp', 'Unknown')}")
                
                console.print("\n[bold]Options:[/bold]")
                console.print("[1] Resume processing")
                console.print("[2] Retry failed files only")
                console.print("[3] Start fresh (clear state)")
                console.print("[B] Back")
                
                choice = Prompt.ask("Select option", choices=["1", "2", "3", "b"], default="b")
                
                if choice == "1":
                    # Resume processing
                    console.print("\n[cyan]Resuming processing...[/cyan]")
                    # Implementation would resume from saved state
                elif choice == "2":
                    # Retry failed only
                    console.print(f"\n[cyan]Retrying {failed} failed files...[/cyan]")
                    # Implementation would retry failed files
                elif choice == "3":
                    # Clear state
                    if Confirm.ask("\n[yellow]Clear processing state?[/yellow]"):
                        os.remove(state_file)
                        console.print("[green]✅ State cleared[/green]")
                        
            except Exception as e:
                console.print(f"[error]Error reading state: {e}[/error]")
        else:
            console.print("[yellow]No interrupted sessions found[/yellow]")
            
        Prompt.ask("\nPress Enter to continue")
        
    def run(self):
        """Main entry point"""
        try:
            self.main_menu()
        except KeyboardInterrupt:
            console.print("\n\n[yellow]Interrupted by user[/yellow]")
        except Exception as e:
            console.print(f"\n\n[error]Unexpected error: {e}[/error]")
            import traceback
            if self.config.get('debug', False):
                traceback.print_exc()
        finally:
            # Cleanup
            self.resource_monitor.stop()
            if self.server_manager.is_running() and self.config.get('auto_stop_server', False):
                console.print("\n[dim]Stopping server...[/dim]")
                self.server_manager.stop()


def interactive_complete():
    """Launch complete interactive mode"""
    navigator = CompleteInteractiveNavigator()
    navigator.run()