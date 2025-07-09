"""Streamlined Interactive CLI for PythonMetaMap"""
import os
import sys
import time
import psutil
import threading
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import subprocess

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.text import Text
from rich.live import Live
from rich import box
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.align import Align
from rich.columns import Columns

from ..core.config import PyMMConfig
from ..core.enhanced_state import AtomicStateManager
from ..server.manager import ServerManager
from ..processing.optimized_batch_runner import OptimizedBatchRunner
from ..processing.ultra_optimized_runner import UltraOptimizedBatchRunner
from ..processing.pool_manager import AdaptivePoolManager

console = Console()

# Clean color scheme
COLORS = {
    'primary': 'cyan',
    'success': 'green',
    'warning': 'yellow',
    'error': 'red',
    'info': 'blue',
    'dim': 'dim white'
}

class InteractiveNavigator:
    """Streamlined interactive CLI for PythonMetaMap"""
    
    def __init__(self):
        self.config = PyMMConfig()
        self.server_manager = ServerManager(self.config)
        self.pool_manager = AdaptivePoolManager(self.config)
        self.running = True
        
        # Check Java installation
        self._check_requirements()
        
        # Initialize with sensible defaults
        self._setup_defaults()
    
    def _check_requirements(self):
        """Check system requirements"""
        if not self.server_manager.java_available:
            console.print("\n[bold red]Java Not Found[/bold red]")
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
        
    def show_header(self):
        """Show minimal header"""
        console.print(f"[bold cyan]PythonMetaMap[/bold cyan] v8.1.9", justify="center")
        console.print("[dim]Medical Text Processing with MetaMap[/dim]", justify="center")
        console.print()
        
    def show_system_status(self):
        """Display current system status"""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        
        status = Table(show_header=False, box=None, padding=(0, 1))
        status.add_column(style="dim")
        status.add_column(style="dim")
        
        # Server status
        server_status = "Running" if self.server_manager.is_running() else "Stopped"
        server_color = "green" if server_status == "Running" else "red"
        
        status.add_row(
            f"Server: [{server_color}]{server_status}[/{server_color}]",
            f"CPU: {cpu_percent:.1f}% | RAM: {memory.percent:.1f}%"
        )
        
        console.print(Panel(status, height=3, style="dim"))
        
    def main_menu(self):
        """Simplified main menu with only essential options"""
        if not self.running:
            return
            
        while self.running:
            self.clear_screen()
            self.show_header()
            self.show_system_status()
            
            # Simple, focused menu
            menu = Table(show_header=False, box=box.SIMPLE)
            menu.add_column("Option", style="cyan")
            menu.add_column("Description")
            
            menu.add_row("1", "Process Files - Quick start with smart defaults")
            menu.add_row("2", "Select Files - Choose specific files to process")
            menu.add_row("3", "Server - Start/stop MetaMap server")
            menu.add_row("4", "Settings - Configure processing options")
            menu.add_row("5", "Help - Show usage guide")
            menu.add_row("Q", "Quit")
            
            console.print(menu)
            console.print()
            
            choice = Prompt.ask("Select option", choices=["1", "2", "3", "4", "5", "q"], default="1").lower()
            
            if choice == "1":
                self.quick_process()
            elif choice == "2":
                self.file_browser()
            elif choice == "3":
                self.server_control()
            elif choice == "4":
                self.settings()
            elif choice == "5":
                self.show_help()
            elif choice == "q":
                if Confirm.ask("[yellow]Exit PythonMetaMap?[/yellow]"):
                    self.running = False
                    console.print("[green]Goodbye![/green]")
                    
    def quick_process(self):
        """Streamlined quick processing with auto-configuration"""
        self.clear_screen()
        console.print(Panel("[bold]Quick Process[/bold]", style="cyan"))
        
        # Check server
        if not self.server_manager.is_running():
            console.print("[yellow]MetaMap server not running.[/yellow]")
            if Confirm.ask("Start server now?", default=True):
                self._start_server()
            else:
                return
                
        # Get directories
        input_dir = Prompt.ask(
            "Input directory",
            default=self.config.get('default_input_dir', './input_notes')
        )
        
        output_dir = Prompt.ask(
            "Output directory", 
            default=self.config.get('default_output_dir', './output_csvs')
        )
        
        # Count files
        input_path = Path(input_dir)
        if not input_path.exists():
            console.print(f"[error]Directory not found: {input_dir}[/error]")
            Prompt.ask("Press Enter to continue")
            return
            
        files = list(input_path.glob("*.txt"))
        if not files:
            console.print("[warning]No .txt files found in directory[/warning]")
            Prompt.ask("Press Enter to continue")
            return
            
        console.print(f"\nFound [cyan]{len(files)}[/cyan] files to process")
        
        # Show optimized settings
        recommendations = self.pool_manager.analyze_system()
        
        settings = Table(title="Processing Configuration", box=box.ROUNDED)
        settings.add_column("Setting", style="blue")
        settings.add_column("Value", style="green")
        settings.add_column("Recommendation", style="dim")
        
        workers = self.config.get('max_parallel_workers')
        chunk_size = self.config.get('chunk_size')
        
        settings.add_row(
            "Workers", 
            str(workers),
            f"Optimal: {recommendations['workers']['optimal']}"
        )
        settings.add_row(
            "Chunk Size",
            str(chunk_size),
            f"Files per batch"
        )
        settings.add_row(
            "Memory Limit",
            f"{recommendations['memory']['available_gb']:.1f} GB",
            "Available RAM"
        )
        
        console.print(settings)
        
        if not Confirm.ask("\nProceed with processing?", default=True):
            return
            
        # Process files
        self._run_processing(input_dir, output_dir, files)
        
    def _run_processing(self, input_dir: str, output_dir: str, files: List[Path]):
        """Run the actual processing with progress tracking"""
        console.print("\n[cyan]Starting processing...[/cyan]")
        
        # Use ultra-optimized runner for large datasets, regular for smaller ones
        if len(files) > 500:
            runner = UltraOptimizedBatchRunner(input_dir, output_dir, self.config)
        else:
            runner = OptimizedBatchRunner(input_dir, output_dir, self.config)
        
        # Setup progress display
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            task = progress.add_task("Processing files...", total=len(files))
            
            def update_progress(stats):
                progress.update(task, completed=stats['processed'])
                
            # Run with progress callback
            try:
                results = runner.run(progress_callback=update_progress)
                
                # Show results
                console.print("\n[green]Processing complete![/green]")
                
                result_table = Table(title="Results", box=box.ROUNDED)
                result_table.add_column("Metric", style="blue")
                result_table.add_column("Value", style="green")
                
                result_table.add_row("Total Files", str(results['total']))
                result_table.add_row("Successful", str(results['successful']))
                result_table.add_row("Failed", str(results['failed']))
                result_table.add_row("Time Taken", f"{results['duration']:.1f}s")
                result_table.add_row("Output Directory", output_dir)
                
                console.print(result_table)
                
            except KeyboardInterrupt:
                console.print("\n[yellow]Processing interrupted by user[/yellow]")
            except Exception as e:
                console.print(f"\n[error]Processing error: {e}[/error]")
                
        Prompt.ask("\nPress Enter to continue")
        
    def file_browser(self):
        """Simple file browser for selecting specific files"""
        self.clear_screen()
        console.print(Panel("[bold]File Browser[/bold]", style="cyan"))
        
        current_dir = Path(self.config.get('default_input_dir', '.'))
        selected_files = []
        
        while True:
            # List files
            console.print(f"\nCurrent directory: [blue]{current_dir}[/blue]")
            
            try:
                items = list(current_dir.iterdir())
                items.sort(key=lambda x: (not x.is_dir(), x.name.lower()))
                
                # Show items
                for i, item in enumerate(items[:20]):  # Limit display
                    if item.is_dir():
                        console.print(f"{i+1:2d}. [blue]📁 {item.name}/[/blue]")
                    elif item.suffix == '.txt':
                        marker = "✓" if item in selected_files else " "
                        console.print(f"{i+1:2d}. [{marker}] 📄 {item.name}")
                    else:
                        console.print(f"{i+1:2d}.     {item.name}", style="dim")
                        
                if len(items) > 20:
                    console.print(f"... and {len(items)-20} more items", style="dim")
                    
            except PermissionError:
                console.print("[error]Permission denied[/error]")
                
            console.print(f"\nSelected: [cyan]{len(selected_files)}[/cyan] files")
            console.print("\n[dim]Enter number to toggle file, 'u' for up, 'p' to process, 'q' to quit[/dim]")
            
            choice = Prompt.ask("Choice").lower()
            
            if choice == 'q':
                break
            elif choice == 'u':
                current_dir = current_dir.parent
            elif choice == 'p' and selected_files:
                self._process_selected_files(selected_files)
                break
            elif choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(items):
                    item = items[idx]
                    if item.is_dir():
                        current_dir = item
                    elif item.suffix == '.txt':
                        if item in selected_files:
                            selected_files.remove(item)
                        else:
                            selected_files.append(item)
                            
    def _process_selected_files(self, files: List[Path]):
        """Process specifically selected files"""
        output_dir = Prompt.ask(
            "Output directory",
            default=self.config.get('default_output_dir', './output_csvs')
        )
        
        # Create a temporary input directory with links to selected files
        temp_input = Path("temp_selected_files")
        temp_input.mkdir(exist_ok=True)
        
        for file in files:
            (temp_input / file.name).write_text(file.read_text())
            
        self._run_processing(str(temp_input), output_dir, files)
        
        # Cleanup
        import shutil
        shutil.rmtree(temp_input)
        
    def server_control(self):
        """Simple server control menu"""
        self.clear_screen()
        console.print(Panel("[bold]Server Control[/bold]", style="cyan"))
        
        status = "Running" if self.server_manager.is_running() else "Stopped"
        color = "green" if status == "Running" else "red"
        
        console.print(f"\nServer Status: [{color}]{status}[/{color}]")
        
        if self.server_manager.is_running():
            if Confirm.ask("\nStop server?"):
                self._stop_server()
        else:
            if Confirm.ask("\nStart server?", default=True):
                self._start_server()
                
        Prompt.ask("\nPress Enter to continue")
        
    def _start_server(self):
        """Start MetaMap server with progress"""
        with console.status("[cyan]Starting MetaMap server...[/cyan]"):
            success = self.server_manager.start()
            
        if success:
            console.print("[green]✓ Server started successfully[/green]")
        else:
            console.print("[error]✗ Failed to start server[/error]")
            console.print("[dim]Check logs for details[/dim]")
            
    def _stop_server(self):
        """Stop MetaMap server"""
        with console.status("[cyan]Stopping MetaMap server...[/cyan]"):
            self.server_manager.stop()
            
        console.print("[green]✓ Server stopped[/green]")
        
    def settings(self):
        """Simplified settings menu"""
        self.clear_screen()
        console.print(Panel("[bold]Settings[/bold]", style="cyan"))
        
        # Show current settings
        settings = Table(title="Current Configuration", box=box.ROUNDED)
        settings.add_column("Setting", style="blue")
        settings.add_column("Value", style="green")
        settings.add_column("Description", style="dim")
        
        settings.add_row(
            "Workers",
            str(self.config.get('max_parallel_workers')),
            "Parallel processing threads"
        )
        settings.add_row(
            "Chunk Size",
            str(self.config.get('chunk_size')),
            "Files per processing batch"
        )
        settings.add_row(
            "Timeout",
            f"{self.config.get('pymm_timeout')}s",
            "Max time per file"
        )
        settings.add_row(
            "Input Directory",
            self.config.get('default_input_dir', './input_notes'),
            "Default input location"
        )
        settings.add_row(
            "Output Directory", 
            self.config.get('default_output_dir', './output_csvs'),
            "Default output location"
        )
        
        console.print(settings)
        
        # Options
        console.print("\n[1] Optimize for current system")
        console.print("[2] Change directories")
        console.print("[3] Advanced settings")
        console.print("[Q] Back to main menu")
        
        choice = Prompt.ask("Select option", choices=["1", "2", "3", "q"], default="q").lower()
        
        if choice == "1":
            self._optimize_settings()
        elif choice == "2":
            self._change_directories()
        elif choice == "3":
            self._advanced_settings()
            
    def _optimize_settings(self):
        """Auto-optimize settings for current system"""
        console.print("\n[cyan]Analyzing system...[/cyan]")
        
        recommendations = self.pool_manager.analyze_system()
        
        # Apply recommendations
        self.config.set('max_parallel_workers', recommendations['workers']['optimal'])
        
        # Set chunk size based on memory
        memory_gb = recommendations['memory']['available_gb']
        if memory_gb < 8:
            chunk_size = 100
        elif memory_gb < 16:
            chunk_size = 250
        else:
            chunk_size = 500
        self.config.set('chunk_size', chunk_size)
        
        self.config.save()
        
        console.print("[green]✓ Settings optimized for your system[/green]")
        Prompt.ask("\nPress Enter to continue")
        
    def _change_directories(self):
        """Change default directories"""
        console.print("\n[cyan]Directory Settings[/cyan]")
        
        input_dir = Prompt.ask(
            "Default input directory",
            default=self.config.get('default_input_dir', './input_notes')
        )
        
        output_dir = Prompt.ask(
            "Default output directory",
            default=self.config.get('default_output_dir', './output_csvs')
        )
        
        self.config.set('default_input_dir', input_dir)
        self.config.set('default_output_dir', output_dir)
        self.config.save()
        
        console.print("[green]✓ Directories updated[/green]")
        Prompt.ask("\nPress Enter to continue")
        
    def _advanced_settings(self):
        """Advanced settings for power users"""
        console.print("\n[cyan]Advanced Settings[/cyan]")
        
        workers = IntPrompt.ask(
            "Max parallel workers",
            default=self.config.get('max_parallel_workers')
        )
        
        chunk_size = IntPrompt.ask(
            "Chunk size (files per batch)",
            default=self.config.get('chunk_size')
        )
        
        timeout = IntPrompt.ask(
            "Timeout per file (seconds)",
            default=self.config.get('pymm_timeout')
        )
        
        self.config.set('max_parallel_workers', workers)
        self.config.set('chunk_size', chunk_size)
        self.config.set('pymm_timeout', timeout)
        self.config.save()
        
        console.print("[green]✓ Settings saved[/green]")
        Prompt.ask("\nPress Enter to continue")
        
    def show_help(self):
        """Display help information"""
        self.clear_screen()
        console.print(Panel("[bold]Help & Usage Guide[/bold]", style="cyan"))
        
        help_text = """
[bold cyan]Quick Start:[/bold cyan]
1. Place your .txt files in the input directory
2. Select 'Process Files' from the main menu
3. Results will be saved as CSV files in the output directory

[bold cyan]Processing Tips:[/bold cyan]
• The system automatically optimizes settings for your hardware
• For large datasets (>1000 files), processing is done in chunks
• Failed files can be retried by running the process again

[bold cyan]File Formats:[/bold cyan]
• Input: Plain text files (.txt) containing medical notes
• Output: CSV files with extracted medical concepts
• One output file per input file

[bold cyan]Troubleshooting:[/bold cyan]
• If processing fails, check that the MetaMap server is running
• For memory issues, reduce the chunk size in Settings
• Check the logs directory for detailed error messages

[bold cyan]Keyboard Shortcuts:[/bold cyan]
• Use number keys to select menu options
• Press 'q' to go back or quit
• Press Ctrl+C to interrupt processing
        """
        
        console.print(help_text)
        Prompt.ask("\nPress Enter to continue")
        
    def run(self):
        """Main entry point"""
        try:
            self.main_menu()
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted by user[/yellow]")
        except Exception as e:
            console.print(f"\n[error]Unexpected error: {e}[/error]")
        finally:
            # Cleanup
            if self.server_manager.is_running():
                console.print("\n[dim]Stopping server...[/dim]")
                self.server_manager.stop()


def interactive_mode():
    """Launch interactive mode"""
    navigator = InteractiveNavigator()
    navigator.run()