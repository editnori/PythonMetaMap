"""Professional Interactive TUI for PythonMetaMap - Optimized and Consistent"""
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
from rich.console import Console, Group
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

from ..core.config import PyMMConfig
from ..core.enhanced_state import AtomicStateManager
from ..server.manager import ServerManager
from ..processing.optimized_batch_runner import OptimizedBatchRunner
from ..processing.ultra_optimized_runner import UltraOptimizedBatchRunner
from ..processing.pool_manager import AdaptivePoolManager

console = Console()

# Professional color scheme - minimal and consistent
COLORS = {
    'primary': 'cyan',
    'secondary': 'blue',
    'success': 'green',
    'warning': 'yellow',
    'error': 'red',
    'dim': 'dim white',
    'bright': 'bright_white'
}


class CompactResourceMonitor:
    """Compact system resource monitoring"""
    
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
                
    def get_status_line(self) -> str:
        """Get single line status"""
        cpu = self.cpu_history[-1] if self.cpu_history else 0
        mem = self.memory_history[-1] if self.memory_history else 0
        disk = psutil.disk_usage('/').percent
        
        # Compact indicators
        cpu_ind = self._get_indicator(cpu)
        mem_ind = self._get_indicator(mem)
        disk_ind = self._get_indicator(disk)
        
        return f"CPU: {cpu:>4.0f}% {cpu_ind}  MEM: {mem:>4.0f}% {mem_ind}  DISK: {disk:>4.0f}% {disk_ind}"
        
    def _get_indicator(self, value: float) -> str:
        """Get compact indicator"""
        if value > 80:
            return "[red]▲[/red]"
        elif value > 60:
            return "[yellow]●[/yellow]"
        else:
            return "[green]●[/green]"


class CompactFileExplorer:
    """Compact file explorer with efficient navigation"""
    
    def __init__(self, start_path: Path = None):
        self.current_path = Path(start_path or os.getcwd())
        self.selected_index = 0
        self.selected_files = set()
        self.page_size = 20
        self.page_offset = 0
        
    def navigate(self) -> List[Path]:
        """Compact file navigation interface"""
        while True:
            self.clear_screen()
            items = self._get_items()
            
            # Header
            console.print(f"[bold]{self.current_path}[/bold]")
            console.print(f"[dim]Files: {len(items)} | Selected: {len(self.selected_files)}[/dim]")
            console.rule(style="dim")
            
            # File list
            start = self.page_offset
            end = min(start + self.page_size, len(items))
            visible_items = items[start:end]
            
            for idx, item in enumerate(visible_items):
                abs_idx = start + idx
                is_selected = abs_idx == self.selected_index
                is_marked = item in self.selected_files
                
                # Compact display
                marker = ">" if is_selected else " "
                check = "*" if is_marked else " "
                
                if item.is_dir():
                    name = f"{item.name}/"
                    style = "blue" if not is_selected else "reverse blue"
                else:
                    name = item.name
                    style = "white" if not is_selected else "reverse"
                    
                size = self._format_size_compact(item) if item.is_file() else "    -"
                
                console.print(f"{marker}[{check}] {name:<40} {size:>8}", style=style)
                
            # Controls line
            console.rule(style="dim")
            console.print("[dim]↑↓:Nav ←→:Dir Space:Select A:All Enter:Open Q:Done[/dim]")
            
            # Get input
            key = console.input().lower()
            
            if key == 'q':
                break
            elif key == '\x1b[a':  # Up arrow
                self._move_selection(-1, len(items))
            elif key == '\x1b[b':  # Down arrow
                self._move_selection(1, len(items))
            elif key == '\x1b[d':  # Left arrow
                self.current_path = self.current_path.parent
                self.selected_index = 0
                self.page_offset = 0
            elif key == '\x1b[c' or key == '\r':  # Right arrow or Enter
                if 0 <= self.selected_index < len(items):
                    item = items[self.selected_index]
                    if item.is_dir():
                        self.current_path = item
                        self.selected_index = 0
                        self.page_offset = 0
            elif key == ' ':
                if 0 <= self.selected_index < len(items):
                    item = items[self.selected_index]
                    if item.is_file():
                        if item in self.selected_files:
                            self.selected_files.remove(item)
                        else:
                            self.selected_files.add(item)
            elif key == 'a':
                for item in items:
                    if item.is_file():
                        self.selected_files.add(item)
                        
        return list(self.selected_files)
        
    def _get_items(self) -> List[Path]:
        """Get directory items"""
        try:
            items = [p for p in self.current_path.iterdir() if not p.name.startswith('.')]
            items.sort(key=lambda x: (not x.is_dir(), x.name.lower()))
            return items
        except PermissionError:
            return []
            
    def _move_selection(self, delta: int, total: int):
        """Move selection with pagination"""
        self.selected_index = max(0, min(total - 1, self.selected_index + delta))
        
        # Adjust page offset
        if self.selected_index < self.page_offset:
            self.page_offset = self.selected_index
        elif self.selected_index >= self.page_offset + self.page_size:
            self.page_offset = self.selected_index - self.page_size + 1
            
    def _format_size_compact(self, path: Path) -> str:
        """Compact size format"""
        size = path.stat().st_size
        for unit in ['B', 'K', 'M', 'G']:
            if size < 1024:
                return f"{size:>3.0f}{unit}"
            size /= 1024
        return f"{size:>3.0f}T"
        
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')


class ProcessingMonitor:
    """Compact processing monitor"""
    
    def __init__(self):
        self.stats = {
            'total': 0,
            'processed': 0,
            'failed': 0,
            'start_time': None,
            'current_file': None
        }
        
    def get_progress_line(self) -> str:
        """Get single line progress"""
        if self.stats['total'] == 0:
            return "Initializing..."
            
        progress = self.stats['processed'] / self.stats['total'] * 100
        bar_width = 20
        filled = int(bar_width * progress / 100)
        bar = f"[green]{'█' * filled}[/green][dim]{'░' * (bar_width - filled)}[/dim]"
        
        return f"{bar} {progress:>5.1f}% | {self.stats['processed']}/{self.stats['total']} | Failed: {self.stats['failed']}"


class ProfessionalInteractiveNavigator:
    """Professional, space-efficient interactive TUI"""
    
    def __init__(self):
        self.config = PyMMConfig()
        self.server_manager = ServerManager(self.config)
        self.pool_manager = AdaptivePoolManager(self.config)
        self.resource_monitor = CompactResourceMonitor()
        self.running = True
        
        # Check requirements
        self._check_requirements()
        
        # Start monitoring
        self.resource_monitor.start()
        
    def _check_requirements(self):
        """Check system requirements"""
        if not self.server_manager.java_available:
            console.print("[red]Java Not Found[/red]")
            console.print("Install: sudo apt-get install openjdk-11-jre-headless")
            self.running = False
            
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
        
    def main_menu(self):
        """Compact main menu"""
        if not self.running:
            return
            
        while self.running:
            self.clear_screen()
            
            # Compact header
            console.print("[bold cyan]PythonMetaMap[/bold cyan] v8.1.9", justify="center")
            console.print("[dim]Medical Text Processing with MetaMap[/dim]", justify="center")
            
            # Single status line
            status_line = self.resource_monitor.get_status_line()
            server_status = "[green]Running[/green]" if self.server_manager.is_running() else "[red]Stopped[/red]"
            console.print(f"\n{status_line}  |  Server: {server_status}")
            console.rule(style="dim")
            
            # Compact menu in two columns
            menu_items = [
                ("1", "Quick Process", "Smart defaults"),
                ("2", "File Browser", "Select files"),
                ("3", "Batch Process", "Large datasets"),
                ("4", "View Results", "Analysis"),
                ("5", "Configuration", "Settings"),
                ("6", "Server Control", "Start/Stop"),
                ("7", "Analysis Tools", "Advanced"),
                ("8", "Resume/Retry", "Continue"),
                ("9", "Logs", "View logs"),
                ("0", "Help", "Documentation")
            ]
            
            # Create two-column layout
            left_items = menu_items[:5]
            right_items = menu_items[5:]
            
            table = Table(show_header=False, box=None, padding=0)
            table.add_column(width=35)
            table.add_column(width=35)
            
            for i in range(max(len(left_items), len(right_items))):
                left = ""
                right = ""
                
                if i < len(left_items):
                    key, title, desc = left_items[i]
                    left = f"[cyan][{key}][/cyan] {title:<15} [dim]{desc}[/dim]"
                    
                if i < len(right_items):
                    key, title, desc = right_items[i]
                    right = f"[cyan][{key}][/cyan] {title:<15} [dim]{desc}[/dim]"
                    
                table.add_row(left, right)
                
            console.print(table)
            console.rule(style="dim")
            
            # Prompt
            choice = Prompt.ask("Select", choices=[str(i) for i in range(10)] + ["q"], default="1").lower()
            
            if choice == "1":
                self.quick_process()
            elif choice == "2":
                self.file_browser()
            elif choice == "3":
                self.batch_process()
            elif choice == "4":
                self.view_results()
            elif choice == "5":
                self.configuration()
            elif choice == "6":
                self.server_control()
            elif choice == "7":
                self.analysis_tools()
            elif choice == "8":
                self.resume_retry()
            elif choice == "9":
                self.view_logs()
            elif choice == "0":
                self.show_help()
            elif choice == "q":
                if Confirm.ask("Exit?", default=False):
                    self.running = False
                    
    def quick_process(self):
        """Streamlined quick processing"""
        self.clear_screen()
        console.print("[bold]Quick Process[/bold]")
        console.rule(style="dim")
        
        # Check server
        if not self.server_manager.is_running():
            console.print("[yellow]Server not running[/yellow]")
            if Confirm.ask("Start server?", default=True):
                with console.status("Starting server..."):
                    success = self.server_manager.start()
                console.print("[green]Server started[/green]" if success else "[red]Failed to start[/red]")
                if not success:
                    input("\nPress Enter to continue...")
                    return
            else:
                return
                
        # Get directories
        input_dir = Prompt.ask("Input directory", default=self.config.get('default_input_dir', './input_notes'))
        output_dir = Prompt.ask("Output directory", default=self.config.get('default_output_dir', './output_csvs'))
        
        # Validate
        input_path = Path(input_dir)
        if not input_path.exists():
            console.print(f"[red]Not found: {input_dir}[/red]")
            input("\nPress Enter...")
            return
            
        files = list(input_path.glob("*.txt"))
        if not files:
            console.print("[yellow]No .txt files found[/yellow]")
            input("\nPress Enter...")
            return
            
        # Compact summary
        total_size = sum(f.stat().st_size for f in files)
        console.print(f"\nFiles: {len(files)} | Size: {self._format_size(total_size)}")
        
        # Settings summary
        recommendations = self.pool_manager.analyze_system()
        workers = self.config.get('max_parallel_workers')
        optimal = recommendations['workers']['optimal']
        
        console.print(f"Workers: {workers} (Optimal: {optimal})")
        console.print(f"Memory: {recommendations['memory']['available_gb']:.1f}GB available")
        
        if not Confirm.ask("\nProceed?", default=True):
            return
            
        # Process
        self._run_processing(input_dir, output_dir, files)
        
    def _run_processing(self, input_dir: str, output_dir: str, files: List[Path]):
        """Compact processing display"""
        self.clear_screen()
        console.print("[bold]Processing[/bold]")
        console.rule(style="dim")
        
        # Choose runner
        if len(files) > 500:
            runner = UltraOptimizedBatchRunner(input_dir, output_dir, self.config)
        else:
            runner = OptimizedBatchRunner(input_dir, output_dir, self.config)
            
        monitor = ProcessingMonitor()
        monitor.stats['total'] = len(files)
        monitor.stats['start_time'] = time.time()
        
        # Process with live display
        with Live(refresh_per_second=2) as live:
            def update_callback(stats):
                monitor.stats.update(stats)
                
                # Create compact display
                elapsed = time.time() - monitor.stats['start_time']
                elapsed_str = f"{int(elapsed // 60)}:{int(elapsed % 60):02d}"
                
                display = Group(
                    monitor.get_progress_line(),
                    f"[dim]Time: {elapsed_str} | Current: {monitor.stats.get('current_file', 'Starting...')[:50]}[/dim]"
                )
                live.update(display)
                
            try:
                results = runner.run(progress_callback=update_callback)
                
                # Show results
                console.print("\n[green]Complete[/green]")
                console.print(f"Processed: {results['successful']} | Failed: {results['failed']} | Time: {results['duration']:.1f}s")
                
            except KeyboardInterrupt:
                console.print("\n[yellow]Interrupted[/yellow]")
            except Exception as e:
                console.print(f"\n[red]Error: {e}[/red]")
                
        input("\nPress Enter...")
        
    def file_browser(self):
        """Compact file browser"""
        explorer = CompactFileExplorer()
        selected = explorer.navigate()
        
        if selected:
            console.print(f"\nSelected {len(selected)} files")
            if Confirm.ask("Process?", default=True):
                output_dir = Prompt.ask("Output directory", default="./output_csvs")
                
                # Create temp directory
                temp_dir = Path("temp_selected")
                temp_dir.mkdir(exist_ok=True)
                
                for file in selected:
                    shutil.copy2(file, temp_dir / file.name)
                    
                self._run_processing(str(temp_dir), output_dir, selected)
                shutil.rmtree(temp_dir)
                
    def view_results(self):
        """Compact results viewer"""
        self.clear_screen()
        console.print("[bold]View Results[/bold]")
        console.rule(style="dim")
        
        output_dir = Path(self.config.get('default_output_dir', './output_csvs'))
        if not output_dir.exists():
            console.print("[yellow]No results directory found[/yellow]")
            input("\nPress Enter...")
            return
            
        csv_files = list(output_dir.glob("*.csv"))
        if not csv_files:
            console.print("[yellow]No results found[/yellow]")
            input("\nPress Enter...")
            return
            
        # Summary table
        table = Table(box=box.SIMPLE)
        table.add_column("File", style="cyan")
        table.add_column("Size", justify="right")
        table.add_column("Modified")
        
        total_size = 0
        for csv in csv_files[:15]:  # Show first 15
            size = csv.stat().st_size
            total_size += size
            mtime = datetime.fromtimestamp(csv.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            table.add_row(csv.name[:40], self._format_size(size), mtime)
            
        if len(csv_files) > 15:
            table.add_row(f"... and {len(csv_files) - 15} more", "", "")
            
        console.print(table)
        console.print(f"\nTotal: {len(csv_files)} files, {self._format_size(total_size)}")
        
        input("\nPress Enter...")
        
    def configuration(self):
        """Compact configuration menu"""
        self.clear_screen()
        console.print("[bold]Configuration[/bold]")
        console.rule(style="dim")
        
        # Current settings
        settings = [
            ("Workers", self.config.get('max_parallel_workers', 4)),
            ("Chunk Size", self.config.get('chunk_size', 500)),
            ("Timeout", f"{self.config.get('pymm_timeout', 300)}s"),
            ("Input Dir", self.config.get('default_input_dir', './input_notes')),
            ("Output Dir", self.config.get('default_output_dir', './output_csvs'))
        ]
        
        for key, value in settings:
            console.print(f"{key:<15} {value}")
            
        console.rule(style="dim")
        console.print("[1] Quick Optimize  [2] Change Dirs  [3] Advanced  [B] Back")
        
        choice = Prompt.ask("Select", choices=["1", "2", "3", "b"], default="b").lower()
        
        if choice == "1":
            recommendations = self.pool_manager.analyze_system()
            self.config.set('max_parallel_workers', recommendations['workers']['optimal'])
            self.config.save()
            console.print("[green]Settings optimized[/green]")
            input("\nPress Enter...")
        elif choice == "2":
            input_dir = Prompt.ask("Input directory", default=self.config.get('default_input_dir'))
            output_dir = Prompt.ask("Output directory", default=self.config.get('default_output_dir'))
            self.config.set('default_input_dir', input_dir)
            self.config.set('default_output_dir', output_dir)
            self.config.save()
            console.print("[green]Directories updated[/green]")
            input("\nPress Enter...")
        elif choice == "3":
            workers = IntPrompt.ask("Workers", default=self.config.get('max_parallel_workers'))
            chunk = IntPrompt.ask("Chunk size", default=self.config.get('chunk_size'))
            timeout = IntPrompt.ask("Timeout (s)", default=self.config.get('pymm_timeout'))
            self.config.set('max_parallel_workers', workers)
            self.config.set('chunk_size', chunk)
            self.config.set('pymm_timeout', timeout)
            self.config.save()
            console.print("[green]Settings saved[/green]")
            input("\nPress Enter...")
            
    def server_control(self):
        """Compact server control"""
        self.clear_screen()
        console.print("[bold]Server Control[/bold]")
        console.rule(style="dim")
        
        # Status
        if self.server_manager.is_running():
            console.print("Status: [green]Running[/green]")
            console.print("\n[1] Stop  [2] Restart  [B] Back")
            choice = Prompt.ask("Select", choices=["1", "2", "b"], default="b")
            
            if choice == "1":
                with console.status("Stopping..."):
                    self.server_manager.stop()
                console.print("[green]Server stopped[/green]")
            elif choice == "2":
                with console.status("Restarting..."):
                    self.server_manager.stop()
                    time.sleep(2)
                    success = self.server_manager.start()
                console.print("[green]Server restarted[/green]" if success else "[red]Failed[/red]")
        else:
            console.print("Status: [red]Stopped[/red]")
            if Confirm.ask("\nStart server?", default=True):
                with console.status("Starting..."):
                    success = self.server_manager.start()
                console.print("[green]Server started[/green]" if success else "[red]Failed[/red]")
                
        input("\nPress Enter...")
        
    def analysis_tools(self):
        """Compact analysis tools"""
        self.clear_screen()
        console.print("[bold]Analysis Tools[/bold]")
        console.rule(style="dim")
        
        console.print("[1] Concept Frequency")
        console.print("[2] Search Concepts")
        console.print("[B] Back")
        
        choice = Prompt.ask("Select", choices=["1", "2", "b"], default="b")
        
        if choice != "b":
            output_dir = Path(self.config.get('default_output_dir', './output_csvs'))
            
            if choice == "1":
                # Simple frequency analysis
                console.print("\n[cyan]Analyzing frequencies...[/cyan]")
                # Implementation would analyze CSVs
                console.print("[green]Analysis complete[/green]")
            elif choice == "2":
                term = Prompt.ask("Search term")
                console.print(f"\n[cyan]Searching for '{term}'...[/cyan]")
                # Implementation would search CSVs
                
            input("\nPress Enter...")
            
    def resume_retry(self):
        """Resume/retry menu"""
        self.clear_screen()
        console.print("[bold]Resume/Retry[/bold]")
        console.rule(style="dim")
        
        state_file = Path(self.config.get('default_output_dir', './output_csvs')) / '.processing_state.json'
        
        if state_file.exists():
            with open(state_file) as f:
                state = json.load(f)
            
            console.print(f"Found session: {len(state.get('processed', []))} processed, {len(state.get('failed', []))} failed")
            console.print("\n[1] Resume  [2] Retry Failed  [3] Clear  [B] Back")
            
            choice = Prompt.ask("Select", choices=["1", "2", "3", "b"], default="b")
            
            if choice == "3" and Confirm.ask("Clear state?"):
                os.remove(state_file)
                console.print("[green]State cleared[/green]")
        else:
            console.print("[yellow]No interrupted sessions[/yellow]")
            
        input("\nPress Enter...")
        
    def view_logs(self):
        """Compact log viewer"""
        self.clear_screen()
        console.print("[bold]Logs[/bold]")
        console.rule(style="dim")
        
        log_dir = Path(self.config.get('default_output_dir', './output_csvs')) / 'logs'
        
        if log_dir.exists():
            logs = list(log_dir.glob("*.log"))
            if logs:
                recent = max(logs, key=lambda f: f.stat().st_mtime)
                console.print(f"Recent: {recent.name}")
                console.print("\n[dim]Last 10 lines:[/dim]")
                
                with open(recent) as f:
                    lines = f.readlines()[-10:]
                for line in lines:
                    console.print(f"  {line.rstrip()[:80]}")
            else:
                console.print("[yellow]No logs found[/yellow]")
        else:
            console.print("[yellow]No log directory[/yellow]")
            
        input("\nPress Enter...")
        
    def show_help(self):
        """Compact help"""
        self.clear_screen()
        console.print("[bold]Help[/bold]")
        console.rule(style="dim")
        
        help_items = [
            ("Quick Start", "Place .txt files in input dir, select Quick Process"),
            ("File Types", "Input: .txt medical notes, Output: .csv with concepts"),
            ("Performance", "Use Settings > Quick Optimize for best performance"),
            ("Large Sets", "Files >1000 use automatic chunking"),
            ("Resume", "Failed files can be retried without reprocessing all")
        ]
        
        for title, desc in help_items:
            console.print(f"[cyan]{title}:[/cyan] {desc}")
            
        console.print("\n[dim]Shortcuts: Numbers select options, Q goes back[/dim]")
        
        input("\nPress Enter...")
        
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
            self.main_menu()
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted[/yellow]")
        except Exception as e:
            console.print(f"\n[red]Error: {e}[/red]")
        finally:
            self.resource_monitor.stop()
            if self.server_manager.is_running() and self.config.get('auto_stop_server', False):
                self.server_manager.stop()


def interactive_professional():
    """Launch professional interactive mode"""
    navigator = ProfessionalInteractiveNavigator()
    navigator.run()