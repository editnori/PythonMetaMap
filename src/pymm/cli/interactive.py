"""Interactive CLI Navigator for PythonMetaMap"""
import os
import sys
from pathlib import Path
from typing import Optional, List, Tuple
import time
import subprocess
from datetime import datetime

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt, Confirm, IntPrompt
from rich.text import Text
from rich.layout import Layout
from rich.live import Live
from rich import box
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.columns import Columns
from rich.align import Align

from ..core.config import PyMMConfig
from ..server.manager import ServerManager
from ..processing.batch_runner import BatchRunner
from ..core.state import StateManager

console = Console()

# Color scheme
COLORS = {
    'primary': 'cyan',
    'secondary': 'magenta', 
    'success': 'green',
    'warning': 'yellow',
    'error': 'red',
    'info': 'blue',
    'header': 'bold cyan',
    'option': 'bold white',
    'dim': 'dim white'
}

# ASCII Banner with colors
BANNER = r"""[bold cyan]
╔═══════════════════════════════════════════════════════════════╗
║  ____        __  __  __  __     _   _             _           ║
║ |  _ \ _   _|  \/  ||  \/  |   | \ | | __ ___   _(_) __ _     ║
║ | |_) | | | | |\/| || |\/| |   |  \|  / _` \ \ / / |/ _` |    ║
║ |  __/| |_| | |  | || |  | |   | |\  | (_| |\ V /| | (_| |    ║
║ |_|    \__, |_|  |_||_|  |_|   |_| \_|\__,_| \_/ |_|\__, |    ║
║        |___/                                         |___/    ║
╚═══════════════════════════════════════════════════════════════╝[/bold cyan]
           [dim]MetaMap Orchestrator v8.1.8 - Interactive Mode[/dim]
"""

class InteractiveNavigator:
    """Interactive CLI Navigator with intuitive menu system"""
    
    def __init__(self):
        self.config = PyMMConfig()
        self.server_manager = ServerManager(self.config)
        self.running = True
        self.current_menu = "main"
        self.history = []
        
    def clear_screen(self):
        """Clear the console screen"""
        console.clear()
        
    def show_banner(self):
        """Display the banner"""
        console.print(BANNER)
        console.print()
        
    def show_status_bar(self):
        """Show a status bar with current info"""
        server_status = self.server_manager.get_status()
        
        # Build status indicators
        status_items = []
        
        # Server status
        tagger_status = "●" if server_status['tagger']['status'] == 'RUNNING' else "○"
        wsd_status = "●" if server_status['wsd']['status'] == 'RUNNING' else "○"
        
        status_items.append(f"[{COLORS['info']}]Servers:[/{COLORS['info']}] Tagger {tagger_status} WSD {wsd_status}")
        
        # Config status
        if self.config.get('metamap_binary_path'):
            status_items.append(f"[{COLORS['success']}]✓ Configured[/{COLORS['success']}]")
        else:
            status_items.append(f"[{COLORS['warning']}]⚠ Not Configured[/{COLORS['warning']}]")
        
        # Create status bar
        status_text = " │ ".join(status_items)
        console.print(Panel(status_text, style="dim", box=box.MINIMAL))
        console.print()
    
    def main_menu(self):
        """Display main menu"""
        self.clear_screen()
        self.show_banner()
        self.show_status_bar()
        
        # Menu options
        menu = Table(show_header=False, box=None, padding=(0, 2))
        menu.add_column("Option", style=COLORS['option'])
        menu.add_column("Description", style=COLORS['dim'])
        
        menu.add_row("1", "Process Files - Run MetaMap on input files")
        menu.add_row("2", "Server Management - Start/stop/check servers")
        menu.add_row("3", "Configuration - Setup and manage settings")
        menu.add_row("4", "View Results - Check status and manage outputs")
        menu.add_row("5", "Quick Process - Process with current settings")
        menu.add_row("6", "Install MetaMap - Download and install MetaMap")
        menu.add_row("", "")
        menu.add_row("H", "Help - Show usage guide")
        menu.add_row("Q", "Quit - Exit the program")
        
        console.print(Panel(menu, title="[bold]Main Menu[/bold]", border_style=COLORS['primary']))
        
        choice = Prompt.ask("\n[bold cyan]Select option[/bold cyan]", 
                           choices=["1", "2", "3", "4", "5", "6", "h", "q"],
                           default="1").lower()
        
        if choice == "1":
            self.process_files_menu()
        elif choice == "2":
            self.server_menu()
        elif choice == "3":
            self.config_menu()
        elif choice == "4":
            self.results_menu()
        elif choice == "5":
            self.quick_process()
        elif choice == "6":
            self.install_metamap()
        elif choice == "h":
            self.show_help()
        elif choice == "q":
            self.quit()
    
    def process_files_menu(self):
        """Process files menu"""
        self.clear_screen()
        console.print(Panel("[bold]Process Files[/bold]", style=COLORS['header']))
        
        # Get current settings
        current_input = self.config.get('default_input_dir', './input_notes')
        current_output = self.config.get('default_output_dir', './output_csvs')
        current_workers = self.config.get('max_parallel_workers', 4)
        current_timeout = self.config.get('pymm_timeout', 300)
        
        # Display current settings
        settings_table = Table(title="Current Settings", box=box.ROUNDED)
        settings_table.add_column("Setting", style=COLORS['info'])
        settings_table.add_column("Value", style=COLORS['option'])
        
        settings_table.add_row("Input Directory", current_input)
        settings_table.add_row("Output Directory", current_output)
        settings_table.add_row("Parallel Workers", str(current_workers))
        settings_table.add_row("Timeout (seconds)", str(current_timeout))
        settings_table.add_row("Instance Pool", "Yes" if self.config.get('use_instance_pool') else "No")
        
        console.print(settings_table)
        console.print()
        
        # Options
        menu = Table(show_header=False, box=None)
        menu.add_column("Option", style=COLORS['option'])
        menu.add_column("Description")
        
        menu.add_row("1", "Start processing with current settings")
        menu.add_row("2", "Change input directory")
        menu.add_row("3", "Change output directory") 
        menu.add_row("4", "Change processing options")
        menu.add_row("5", "Resume previous processing")
        menu.add_row("B", "Back to main menu")
        
        console.print(menu)
        
        choice = Prompt.ask("\nSelect option", choices=["1", "2", "3", "4", "5", "b"], default="1").lower()
        
        if choice == "1":
            self.start_processing(current_input, current_output)
        elif choice == "2":
            new_input = Prompt.ask("Enter input directory", default=current_input)
            if Path(new_input).exists():
                self.config.set('default_input_dir', new_input)
                console.print(f"[{COLORS['success']}]✓ Input directory updated[/{COLORS['success']}]")
            else:
                console.print(f"[{COLORS['error']}]✗ Directory not found[/{COLORS['error']}]")
            console.input("\nPress Enter to continue...")
            self.process_files_menu()
        elif choice == "3":
            new_output = Prompt.ask("Enter output directory", default=current_output)
            self.config.set('default_output_dir', new_output)
            console.print(f"[{COLORS['success']}]✓ Output directory updated[/{COLORS['success']}]")
            console.input("\nPress Enter to continue...")
            self.process_files_menu()
        elif choice == "4":
            self.processing_options_menu()
        elif choice == "5":
            self.resume_processing()
        elif choice == "b":
            return
    
    def processing_options_menu(self):
        """Processing options submenu"""
        self.clear_screen()
        console.print(Panel("[bold]Processing Options[/bold]", style=COLORS['header']))
        
        # Current options
        options = Table(title="Current Options", box=box.ROUNDED)
        options.add_column("Option", style=COLORS['info'])
        options.add_column("Current Value", style=COLORS['option'])
        
        options.add_row("Workers", str(self.config.get('max_parallel_workers')))
        options.add_row("Timeout", f"{self.config.get('pymm_timeout')}s")
        options.add_row("Retry Attempts", str(self.config.get('retry_max_attempts')))
        options.add_row("Instance Pool", "Yes" if self.config.get('use_instance_pool') else "No")
        options.add_row("Java Heap Size", self.config.get('java_heap_size'))
        
        console.print(options)
        console.print()
        
        menu = Table(show_header=False, box=None)
        menu.add_column("Option", style=COLORS['option'])
        menu.add_column("Description")
        
        menu.add_row("1", "Change number of workers")
        menu.add_row("2", "Change timeout")
        menu.add_row("3", "Change retry attempts")
        menu.add_row("4", "Toggle instance pool")
        menu.add_row("5", "Change Java heap size")
        menu.add_row("B", "Back")
        
        console.print(menu)
        
        choice = Prompt.ask("\nSelect option", choices=["1", "2", "3", "4", "5", "b"]).lower()
        
        if choice == "1":
            workers = IntPrompt.ask("Number of parallel workers", default=self.config.get('max_parallel_workers'))
            self.config.set('max_parallel_workers', workers)
            console.print(f"[{COLORS['success']}]✓ Workers set to {workers}[/{COLORS['success']}]")
        elif choice == "2":
            timeout = IntPrompt.ask("Timeout per file (seconds)", default=self.config.get('pymm_timeout'))
            self.config.set('pymm_timeout', timeout)
            console.print(f"[{COLORS['success']}]✓ Timeout set to {timeout}s[/{COLORS['success']}]")
        elif choice == "3":
            retries = IntPrompt.ask("Max retry attempts", default=self.config.get('retry_max_attempts'))
            self.config.set('retry_max_attempts', retries)
            console.print(f"[{COLORS['success']}]✓ Retries set to {retries}[/{COLORS['success']}]")
        elif choice == "4":
            current = self.config.get('use_instance_pool')
            self.config.set('use_instance_pool', not current)
            console.print(f"[{COLORS['success']}]✓ Instance pool {'enabled' if not current else 'disabled'}[/{COLORS['success']}]")
        elif choice == "5":
            heap = Prompt.ask("Java heap size", default=self.config.get('java_heap_size'))
            self.config.set('java_heap_size', heap)
            console.print(f"[{COLORS['success']}]✓ Heap size set to {heap}[/{COLORS['success']}]")
        
        if choice != "b":
            console.input("\nPress Enter to continue...")
            self.processing_options_menu()
    
    def start_processing(self, input_dir: str, output_dir: str):
        """Start processing with progress display"""
        self.clear_screen()
        
        # Check if servers are running
        if not self.check_servers():
            if Confirm.ask("[yellow]Servers not running. Start them now?[/yellow]"):
                self.start_servers()
            else:
                return
        
        # Create runner
        runner = BatchRunner(input_dir, output_dir, self.config)
        
        # Gather files
        try:
            files = runner._collect_input_files()
            total_files = len(files)
            
            if total_files == 0:
                console.print(f"[{COLORS['warning']}]No input files found in {input_dir}[/{COLORS['warning']}]")
                console.input("\nPress Enter to continue...")
                return
                
            console.print(Panel(f"Found [bold]{total_files}[/bold] files to process", 
                              title="Processing", style=COLORS['info']))
            
            if not Confirm.ask(f"\nProcess {total_files} files?"):
                return
                
        except Exception as e:
            console.print(f"[{COLORS['error']}]Error: {e}[/{COLORS['error']}]")
            console.input("\nPress Enter to continue...")
            return
        
        # Ask for processing mode BEFORE creating progress bar
        console.print("\n[bold]Processing Mode:[/bold]")
        console.print("  1. Frontend (interactive with progress bar)")
        console.print("  2. Backend (nohup - runs in background)")
        
        mode = Prompt.ask("\nSelect mode", choices=["1", "2"], default="1")
        
        if mode == "2":
            # Backend processing
            console.print(f"\n[{COLORS['info']}]Starting background processing...[/{COLORS['info']}]")
            
            # Create background command - don't force server start if already running
            cmd = [
                "nohup",
                sys.executable,
                "-m", "pymm",
                "process",
                str(input_dir),
                str(output_dir),
                "--workers", str(runner.max_workers),
                "--timeout", str(runner.timeout),
                "--background"  # Add background flag
            ]
            
            # Only add --no-start-servers if servers are already running
            if self.check_servers():
                cmd.append("--no-start-servers")
            
            # Start background process
            log_file = runner.logs_dir / f"background_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            
            with open(log_file, 'w') as log:
                process = subprocess.Popen(
                    cmd,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    preexec_fn=os.setsid if os.name != 'nt' else None
                )
            
            console.print(f"[{COLORS['success']}]✓ Background processing started![/{COLORS['success']}]")
            console.print(f"  PID: {process.pid}")
            console.print(f"  Log: {log_file}")
            console.print(f"\n[{COLORS['info']}]Monitor progress with:[/{COLORS['info']}]")
            console.print(f"  tail -f {log_file}")
            console.print(f"\n[{COLORS['info']}]Check status with:[/{COLORS['info']}]")
            console.print(f"  pymm status {output_dir}")
            
            # Save PID for monitoring
            pid_file = Path(output_dir) / ".background_pid"
            pid_file.write_text(str(process.pid))
            
            console.input("\nPress Enter to continue...")
            return
        
        # Frontend processing - NOW create progress bar
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console
        ) as progress:
            
            task = progress.add_task("Processing files...", total=total_files)
            
            # Run processing
            start_time = time.time()
            try:
                results = runner.run()
                
                if results.get('success'):
                    elapsed = time.time() - start_time
                    console.print(f"\n[{COLORS['success']}]✓ Processing complete![/{COLORS['success']}]")
                    
                    # Show summary
                    summary = Table(title="Processing Summary", box=box.ROUNDED)
                    summary.add_column("Metric", style=COLORS['info'])
                    summary.add_column("Value", style=COLORS['success'])
                    
                    summary.add_row("Total Files", str(results.get('total_files', 0)))
                    summary.add_row("Processed", str(results.get('processed', 0)))
                    summary.add_row("Failed", str(results.get('failed', 0)))
                    summary.add_row("Time", f"{elapsed:.1f}s")
                    summary.add_row("Throughput", f"{results.get('throughput', 0):.2f} files/s")
                    
                    console.print(summary)
                else:
                    console.print(f"\n[{COLORS['error']}]✗ Processing failed: {results.get('error')}[/{COLORS['error']}]")
                    
            except Exception as e:
                console.print(f"\n[{COLORS['error']}]Error: {e}[/{COLORS['error']}]")
        
        console.input("\nPress Enter to continue...")
    
    def server_menu(self):
        """Server management menu"""
        self.clear_screen()
        console.print(Panel("[bold]Server Management[/bold]", style=COLORS['header']))
        
        # Get current status
        status = self.server_manager.get_status()
        
        # Status table
        status_table = Table(title="Server Status", box=box.ROUNDED)
        status_table.add_column("Service", style=COLORS['info'])
        status_table.add_column("Status", style="bold")
        status_table.add_column("Port")
        status_table.add_column("PID", style=COLORS['dim'])
        
        for service, info in status.items():
            status_color = COLORS['success'] if info['status'] == 'RUNNING' else COLORS['error']
            status_table.add_row(
                service.upper(),
                f"[{status_color}]{info['status']}[/{status_color}]",
                str(info.get('port', '-')),
                str(info.get('pid', '-'))
            )
        
        console.print(status_table)
        console.print()
        
        # Menu options
        menu = Table(show_header=False, box=None)
        menu.add_column("Option", style=COLORS['option'])
        menu.add_column("Description")
        
        menu.add_row("1", "Start all servers")
        menu.add_row("2", "Stop all servers")
        menu.add_row("3", "Restart all servers")
        menu.add_row("4", "Restart specific server")
        menu.add_row("5", "Check connectivity")
        menu.add_row("6", "View server logs")
        menu.add_row("B", "Back to main menu")
        
        console.print(menu)
        
        choice = Prompt.ask("\nSelect option", choices=["1", "2", "3", "4", "5", "6", "b"]).lower()
        
        if choice == "1":
            self.start_servers()
        elif choice == "2":
            self.stop_servers()
        elif choice == "3":
            self.restart_servers()
        elif choice == "4":
            self.restart_specific_server()
        elif choice == "5":
            self.check_connectivity()
        elif choice == "6":
            self.view_server_logs()
        
        if choice != "b":
            console.input("\nPress Enter to continue...")
            self.server_menu()
    
    def config_menu(self):
        """Configuration menu"""
        self.clear_screen()
        console.print(Panel("[bold]Configuration[/bold]", style=COLORS['header']))
        
        # Key settings
        settings = Table(title="Current Configuration", box=box.ROUNDED)
        settings.add_column("Setting", style=COLORS['info'])
        settings.add_column("Value", style=COLORS['option'])
        
        key_settings = [
            ("MetaMap Binary", self.config.get('metamap_binary_path', 'Not set')),
            ("Input Directory", self.config.get('default_input_dir', 'Not set')),
            ("Output Directory", self.config.get('default_output_dir', 'Not set')),
            ("Workers", str(self.config.get('max_parallel_workers', 4))),
            ("Instance Pool", "Yes" if self.config.get('use_instance_pool') else "No")
        ]
        
        for setting, value in key_settings:
            # Truncate long paths
            display_value = value if len(value) < 50 else "..." + value[-47:]
            settings.add_row(setting, display_value)
        
        console.print(settings)
        console.print()
        
        # Menu
        menu = Table(show_header=False, box=None)
        menu.add_column("Option", style=COLORS['option'])
        menu.add_column("Description")
        
        menu.add_row("1", "Run configuration wizard")
        menu.add_row("2", "View all settings")
        menu.add_row("3", "Change specific setting")
        menu.add_row("4", "Reset configuration")
        menu.add_row("5", "Validate configuration")
        menu.add_row("B", "Back to main menu")
        
        console.print(menu)
        
        choice = Prompt.ask("\nSelect option", choices=["1", "2", "3", "4", "5", "b"]).lower()
        
        if choice == "1":
            self.config_wizard()
        elif choice == "2":
            self.view_all_settings()
        elif choice == "3":
            self.change_setting()
        elif choice == "4":
            if Confirm.ask("[yellow]Reset all settings to defaults?[/yellow]"):
                self.config.reset()
                console.print(f"[{COLORS['success']}]✓ Configuration reset[/{COLORS['success']}]")
        elif choice == "5":
            self.validate_config()
        
        if choice != "b":
            console.input("\nPress Enter to continue...")
            self.config_menu()
    
    def quick_process(self):
        """Quick process with current settings"""
        input_dir = self.config.get('default_input_dir')
        output_dir = self.config.get('default_output_dir')
        
        if not input_dir or not output_dir:
            console.print(f"[{COLORS['error']}]Please configure input/output directories first[/{COLORS['error']}]")
            console.input("\nPress Enter to continue...")
            return
            
        self.start_processing(input_dir, output_dir)
    
    def check_servers(self) -> bool:
        """Check if servers are running"""
        status = self.server_manager.get_status()
        tagger_running = status['tagger']['status'] == 'RUNNING'
        wsd_running = status['wsd']['status'] == 'RUNNING'
        return tagger_running  # WSD is optional
    
    def start_servers(self):
        """Start MetaMap servers"""
        with console.status("Starting servers...") as status:
            if self.server_manager.start_all():
                console.print(f"[{COLORS['success']}]✓ Servers started[/{COLORS['success']}]")
            else:
                console.print(f"[{COLORS['error']}]✗ Failed to start servers[/{COLORS['error']}]")
    
    def stop_servers(self):
        """Stop MetaMap servers"""
        with console.status("Stopping servers...") as status:
            self.server_manager.stop_all()
        console.print(f"[{COLORS['success']}]✓ Servers stopped[/{COLORS['success']}]")
    
    def restart_servers(self):
        """Restart all servers"""
        with console.status("Restarting servers...") as status:
            if self.server_manager.restart_service('all'):
                console.print(f"[{COLORS['success']}]✓ Servers restarted[/{COLORS['success']}]")
            else:
                console.print(f"[{COLORS['error']}]✗ Failed to restart servers[/{COLORS['error']}]")
    
    def config_wizard(self):
        """Run configuration wizard"""
        self.clear_screen()
        console.print(Panel("[bold]Configuration Wizard[/bold]", style=COLORS['header']))
        self.config.configure_interactive()
    
    def show_help(self):
        """Show help information"""
        self.clear_screen()
        
        help_text = """
[bold]PythonMetaMap Help[/bold]

[cyan]Overview:[/cyan]
PythonMetaMap orchestrates the NLM MetaMap tool for processing clinical text.
It manages servers, handles batch processing, and provides retry mechanisms.

[cyan]Quick Start:[/cyan]
1. Configure: Select option 3 from main menu
2. Start servers: Select option 2 → 1
3. Process files: Select option 1 or 5

[cyan]Key Features:[/cyan]
• Parallel processing with configurable workers
• Automatic retry with exponential backoff
• Server health monitoring
• Progress tracking and resumable processing
• Instance pooling for better performance

[cyan]Common Issues:[/cyan]
• WSD Server: May have startup issues - try restarting
• Empty results: Usually indicates server connectivity problems
• Port conflicts: Use server menu to check and restart

[cyan]Tips:[/cyan]
• Use Quick Process (option 5) for repeated runs
• Enable instance pooling for large batches
• Check server status before processing
• Monitor logs for detailed error information
        """
        
        console.print(Panel(help_text, title="Help", border_style=COLORS['info']))
        console.input("\nPress Enter to continue...")
    
    def quit(self):
        """Quit the application"""
        if Confirm.ask("\n[yellow]Really quit?[/yellow]"):
            console.print(f"\n[{COLORS['info']}]Thank you for using PythonMetaMap![/{COLORS['info']}]")
            self.running = False
        
    def run(self):
        """Main run loop"""
        while self.running:
            try:
                self.main_menu()
            except KeyboardInterrupt:
                console.print(f"\n[{COLORS['warning']}]Use 'Q' to quit[/{COLORS['warning']}]")
                time.sleep(1)
            except Exception as e:
                console.print(f"\n[{COLORS['error']}]Error: {e}[/{COLORS['error']}]")
                console.input("\nPress Enter to continue...")

    def results_menu(self):
        """Results and output management menu"""
        self.clear_screen()
        console.print(Panel("[bold]View Results & Manage Outputs[/bold]", style=COLORS['header']))
        
        # Get output directories
        default_output = self.config.get('default_output_dir', './output_csvs')
        output_path = Path(default_output)
        
        # Quick stats
        if output_path.exists():
            csv_files = list(output_path.glob("*.csv"))
            csv_files = [f for f in csv_files if not f.name.startswith('.')]
            total_size = sum(f.stat().st_size for f in csv_files) / 1024 / 1024  # MB
            
            console.print(f"[{COLORS['info']}]Output Directory:[/{COLORS['info']}] {output_path}")
            console.print(f"[{COLORS['info']}]Total Files:[/{COLORS['info']}] {len(csv_files)}")
            console.print(f"[{COLORS['info']}]Total Size:[/{COLORS['info']}] {total_size:.1f} MB\n")
        
        # Menu options
        menu = Table(show_header=False, box=None)
        menu.add_column("Option", style=COLORS['option'])
        menu.add_column("Description")
        
        menu.add_row("1", "View recent processing sessions")
        menu.add_row("2", "Analyze concepts (like kidney stone analysis)")
        menu.add_row("3", "Session statistics and performance")
        menu.add_row("4", "Monitor background processing")
        menu.add_row("5", "Clear failed outputs")
        menu.add_row("6", "Clear all outputs")
        menu.add_row("7", "Export analysis report")
        menu.add_row("8", "Retry failed files")
        menu.add_row("B", "Back to main menu")
        
        console.print(menu)
        
        choice = Prompt.ask("\nSelect option", choices=["1", "2", "3", "4", "5", "6", "7", "8", "b"]).lower()
        
        if choice == "1":
            self.view_recent_sessions()
        elif choice == "2":
            self.analyze_concepts_interactive()
        elif choice == "3":
            self.view_session_statistics()
        elif choice == "4":
            self.monitor_background()
        elif choice == "5":
            self.clear_failed_outputs()
        elif choice == "6":
            self.clear_all_outputs()
        elif choice == "7":
            self.export_analysis_report()
        elif choice == "8":
            self.retry_failed_files()
        elif choice == "b":
            return
            
        if choice != "b":
            console.input("\nPress Enter to continue...")
            self.results_menu()
    
    def view_recent_sessions(self):
        """View recent processing sessions"""
        # List recent output directories
        output_base = Path(self.config.get('default_output_dir', '.'))
        if output_base.exists():
            # Find directories with state files
            state_dirs = []
            for item in output_base.parent.iterdir():
                if item.is_dir() and (item / '.pymm_state.json').exists():
                    state_dirs.append(item)
            
            if state_dirs:
                # Sort by modification time
                state_dirs.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                
                table = Table(title="Recent Processing Sessions", box=box.ROUNDED)
                table.add_column("#", style=COLORS['dim'])
                table.add_column("Directory", style=COLORS['info'])
                table.add_column("Last Modified", style=COLORS['dim'])
                
                for i, dir_path in enumerate(state_dirs[:10], 1):
                    mtime = time.strftime('%Y-%m-%d %H:%M', time.localtime(dir_path.stat().st_mtime))
                    table.add_row(str(i), dir_path.name, mtime)
                
                console.print(table)
                console.print()
                
                choice = Prompt.ask("Select session to view (or 'B' to go back)", default="1")
                
                if choice.lower() != 'b' and choice.isdigit():
                    idx = int(choice) - 1
                    if 0 <= idx < len(state_dirs):
                        self.view_session_details(state_dirs[idx])
                        return
        
        console.print(f"[{COLORS['warning']}]No processing sessions found[/{COLORS['warning']}]")
    
    def clear_failed_outputs(self):
        """Clear failed outputs"""
        output_dir = self.config.get('default_output_dir', './output_csvs')
        
        if not Path(output_dir).exists():
            console.print(f"[{COLORS['warning']}]Output directory not found: {output_dir}[/{COLORS['warning']}]")
            return
            
        # First show summary of what will be cleared
        from ..processing.batch_runner import BatchRunner
        
        runner = BatchRunner('input_notes', output_dir, self.config)
        
        with console.status("Analyzing failed outputs..."):
            # Count failed outputs
            failed_count = 0
            incomplete_count = 0
            
            for csv_file in Path(output_dir).glob("*.csv"):
                if csv_file.stem.startswith('.'):
                    continue
                    
                try:
                    content = csv_file.read_text()
                    # Check for error marker
                    if ":ERROR" in content:
                        failed_count += 1
                    # Check if file is incomplete (no end marker)
                    elif "META_BATCH_END" not in content:
                        incomplete_count += 1
                except Exception:
                    incomplete_count += 1
        
        if failed_count == 0 and incomplete_count == 0:
            console.print(f"[{COLORS['success']}]No failed or incomplete outputs found[/{COLORS['success']}]")
            return
            
        console.print(f"\n[{COLORS['warning']}]Found:[/{COLORS['warning']}]")
        console.print(f"  • {failed_count} failed outputs (with errors)")
        console.print(f"  • {incomplete_count} incomplete outputs (empty or partial)")
        
        if Confirm.ask(f"\n[yellow]Clear {failed_count + incomplete_count} failed/incomplete outputs?[/yellow]"):
            with console.status("Clearing failed outputs..."):
                result = runner.clear_failed_outputs()
                
            console.print(f"\n[{COLORS['success']}]✓ Cleared:[/{COLORS['success']}]")
            console.print(f"  • {result['removed_files']} CSV files")
            console.print(f"  • {result['reset_states']} state entries")
            console.print(f"  • {result['cleared_retries']} retry queue entries")
    
    def clear_all_outputs(self):
        """Clear all outputs"""
        output_dir = self.config.get('default_output_dir', './output_csvs')
        
        if not Path(output_dir).exists():
            console.print(f"[{COLORS['warning']}]Output directory not found: {output_dir}[/{COLORS['warning']}]")
            return
            
        # Count files
        csv_count = len(list(Path(output_dir).glob("*.csv")))
        state_files = len(list(Path(output_dir).glob(".*json")))
        
        console.print(f"\n[{COLORS['warning']}]This will delete:[/{COLORS['warning']}]")
        console.print(f"  • {csv_count} CSV output files")
        console.print(f"  • {state_files} state/configuration files")
        console.print(f"  • All processing history")
        
        if Confirm.ask(f"\n[red]Are you sure you want to clear ALL outputs?[/red]"):
            if Confirm.ask(f"[red]This action cannot be undone. Continue?[/red]"):
                from ..processing.batch_runner import BatchRunner
                
                runner = BatchRunner('input_notes', output_dir, self.config)
                
                with console.status("Clearing all outputs..."):
                    result = runner.clear_all_outputs(confirm=True)
                    
                console.print(f"\n[{COLORS['success']}]✓ Cleared:[/{COLORS['success']}]")
                console.print(f"  • {result['removed_csv_files']} CSV files")
                console.print(f"  • {result['removed_state_files']} state files")
                console.print(f"  • {result['total_removed']} total files removed")

    def view_session_details(self, output_dir: Path):
        """View details of a specific session"""
        self.clear_screen()
        
        try:
            state_mgr = StateManager(str(output_dir))
            
            # Session info
            session = state_mgr.get_session_info()
            console.print(Panel(
                f"[bold]Session ID:[/bold] {session['session_id']}\n"
                f"[bold]Started:[/bold] {session['started']}\n"
                f"[bold]Last Updated:[/bold] {session['last_updated']}",
                title=f"Session: {output_dir.name}",
                style=COLORS['info']
            ))
            
            # Statistics
            stats = state_mgr.get_statistics()
            
            stats_table = Table(box=box.ROUNDED)
            stats_table.add_column("Metric", style=COLORS['info'])
            stats_table.add_column("Count", style=COLORS['option'])
            
            stats_table.add_row("Total Files", str(stats['total_files']))
            stats_table.add_row("Completed", f"[{COLORS['success']}]{stats['completed']}[/{COLORS['success']}]")
            stats_table.add_row("Failed", f"[{COLORS['error']}]{stats['failed']}[/{COLORS['error']}]")
            stats_table.add_row("In Progress", f"[{COLORS['warning']}]{stats['in_progress']}[/{COLORS['warning']}]")
            
            console.print(stats_table)
            
            # Failed files if any
            if state_mgr._state.get('failed_files'):
                console.print(f"\n[{COLORS['error']}]Failed Files:[/{COLORS['error']}]")
                for file, info in list(state_mgr._state['failed_files'].items())[:5]:
                    console.print(f"  • {Path(file).name}: {info['error'][:50]}...")
                    
        except Exception as e:
            console.print(f"[{COLORS['error']}]Error reading session: {e}[/{COLORS['error']}]")

    def restart_specific_server(self):
        """Restart a specific server"""
        service = Prompt.ask("Which server to restart?", choices=["tagger", "wsd", "all"])
        
        with console.status(f"Restarting {service}..."):
            if self.server_manager.restart_service(service):
                console.print(f"[{COLORS['success']}]✓ {service} restarted[/{COLORS['success']}]")
            else:
                console.print(f"[{COLORS['error']}]✗ Failed to restart {service}[/{COLORS['error']}]")

    def check_connectivity(self):
        """Check MetaMap connectivity"""
        with console.status("Testing connectivity..."):
            if self.server_manager.verify_connectivity():
                console.print(f"[{COLORS['success']}]✓ MetaMap connectivity verified[/{COLORS['success']}]")
            else:
                console.print(f"[{COLORS['error']}]✗ Connectivity test failed[/{COLORS['error']}]")

    def view_server_logs(self):
        """View server logs"""
        console.print(f"[{COLORS['info']}]Server logs location:[/{COLORS['info']}]")
        console.print(f"  • {self.config.get('server_scripts_dir', 'Not configured')}")
        console.print("\nCheck the following files:")
        console.print("  • skrmedpostagger.log")
        console.print("  • wsdserver.log")

    def resume_processing(self):
        """Resume interrupted processing"""
        output_dir = Prompt.ask("Enter output directory to resume", 
                               default=self.config.get('default_output_dir'))
        
        if Path(output_dir).exists() and (Path(output_dir) / '.pymm_state.json').exists():
            try:
                results = BatchRunner.resume(output_dir, self.config)
                if results.get('success'):
                    console.print(f"[{COLORS['success']}]✓ Processing resumed and completed[/{COLORS['success']}]")
                else:
                    console.print(f"[{COLORS['error']}]✗ Resume failed: {results.get('error')}[/{COLORS['error']}]")
            except Exception as e:
                console.print(f"[{COLORS['error']}]Error: {e}[/{COLORS['error']}]")
        else:
            console.print(f"[{COLORS['error']}]No resumable session found in {output_dir}[/{COLORS['error']}]")

    def view_all_settings(self):
        """View all configuration settings"""
        self.clear_screen()
        
        all_settings = Table(title="All Configuration Settings", box=box.ROUNDED)
        all_settings.add_column("Key", style=COLORS['info'])
        all_settings.add_column("Value", style=COLORS['option'])
        all_settings.add_column("Source", style=COLORS['dim'])
        
        # Get all config keys
        for key, value in self.config._config.items():
            source = "config file"
            if os.getenv(key.upper()):
                source = "environment"
            elif key in self.config.DEFAULTS and value == self.config.DEFAULTS[key]:
                source = "default"
                
            # Format value for display
            display_value = str(value)
            if len(display_value) > 60:
                display_value = display_value[:57] + "..."
                
            all_settings.add_row(key, display_value, source)
        
        console.print(all_settings)

    def change_setting(self):
        """Change a specific setting"""
        key = Prompt.ask("Enter setting key (e.g., max_parallel_workers)")
        
        current = self.config.get(key)
        if current is not None:
            console.print(f"Current value: {current}")
            new_value = Prompt.ask("Enter new value")
            
            # Try to convert to appropriate type
            if isinstance(current, int):
                try:
                    new_value = int(new_value)
                except ValueError:
                    console.print(f"[{COLORS['error']}]Invalid integer value[/{COLORS['error']}]")
                    return
                    
            self.config.set(key, new_value)
            console.print(f"[{COLORS['success']}]✓ {key} set to {new_value}[/{COLORS['success']}]")
        else:
            console.print(f"[{COLORS['warning']}]Setting '{key}' not found[/{COLORS['warning']}]")

    def validate_config(self):
        """Validate current configuration"""
        self.clear_screen()
        console.print(Panel("[bold]Configuration Validation[/bold]", style=COLORS['header']))
        
        errors = []
        warnings = []
        
        # Check MetaMap binary
        mm_path = self.config.get("metamap_binary_path")
        if not mm_path:
            errors.append("metamap_binary_path not set")
        elif not Path(mm_path).exists():
            errors.append(f"MetaMap binary not found: {mm_path}")
        elif not os.access(mm_path, os.X_OK):
            errors.append(f"MetaMap binary not executable: {mm_path}")
        else:
            console.print(f"[{COLORS['success']}]✓ MetaMap binary found and executable[/{COLORS['success']}]")
        
        # Check server scripts
        scripts_dir = self.config.get("server_scripts_dir")
        if scripts_dir:
            scripts_path = Path(scripts_dir)
            if not scripts_path.exists():
                warnings.append(f"Server scripts directory not found: {scripts_dir}")
            else:
                for script in ["skrmedpostctl", "wsdserverctl"]:
                    if not (scripts_path / script).exists():
                        warnings.append(f"Server script not found: {script}")
                if not warnings:
                    console.print(f"[{COLORS['success']}]✓ Server scripts found[/{COLORS['success']}]")
        
        # Check directories
        for key in ["default_input_dir", "default_output_dir"]:
            path = self.config.get(key)
            if path and not Path(path).exists():
                warnings.append(f"{key} does not exist: {path}")
        
        # Display results
        if errors:
            console.print(f"\n[{COLORS['error']}]Errors:[/{COLORS['error']}]")
            for error in errors:
                console.print(f"  ✗ {error}")
        
        if warnings:
            console.print(f"\n[{COLORS['warning']}]Warnings:[/{COLORS['warning']}]")
            for warning in warnings:
                console.print(f"  ⚠ {warning}")
        
        if not errors and not warnings:
            console.print(f"\n[{COLORS['success']}]✓ Configuration is valid[/{COLORS['success']}]")

    def install_metamap(self):
        """Install MetaMap"""
        self.clear_screen()
        console.print(Panel("[bold]Install MetaMap[/bold]", style=COLORS['header']))
        
        console.print("This will download and install MetaMap 2020 (~1GB)")
        
        # Check if already installed
        from ..install_metamap import EXPECTED_METAMAP_BINARY
        if os.path.exists(EXPECTED_METAMAP_BINARY):
            console.print(f"\n[{COLORS['warning']}]MetaMap appears to be already installed at:[/{COLORS['warning']}]")
            console.print(f"  {EXPECTED_METAMAP_BINARY}")
            console.print("\nOptions:")
            console.print("  1. Keep existing installation")
            console.print("  2. Reconfigure existing installation")
            console.print("  3. Complete reinstallation")
            
            choice = Prompt.ask("\nSelect option", choices=["1", "2", "3"], default="1")
            
            if choice == "1":
                console.print(f"\n[{COLORS['info']}]Keeping existing installation[/{COLORS['info']}]")
                return
            elif choice == "2":
                os.environ["PYMM_FORCE_REINSTALL"] = "no"
            else:  # choice == "3"
                os.environ["PYMM_FORCE_REINSTALL"] = "yes"
        
        if not Confirm.ask("\nProceed with installation?"):
            return
            
        try:
            from ..install_metamap import main as install_main
            
            # Call install_main without spinner to allow prompts
            console.print(f"\n[{COLORS['info']}]Starting MetaMap installation process...[/{COLORS['info']}]\n")
            
            # Capture the installation in a more controlled way
            original_stdin = sys.stdin
            try:
                result = install_main()
            finally:
                sys.stdin = original_stdin
                # Clean up environment variable
                os.environ.pop("PYMM_FORCE_REINSTALL", None)
            
            if result:
                console.print(f"\n[{COLORS['success']}]✓ MetaMap installed at: {result}[/{COLORS['success']}]")
                self.config.set('metamap_binary_path', result)
                
                # Also set server scripts directory
                from ..install_metamap import META_INSTALL_DIR
                scripts_dir = os.path.join(META_INSTALL_DIR, "public_mm", "bin")
                if os.path.exists(scripts_dir):
                    self.config.set('server_scripts_dir', scripts_dir)
                    
                console.print(f"[{COLORS['info']}]Configuration updated[/{COLORS['info']}]")
                
                # Run setup verification
                console.print(f"\n[{COLORS['info']}]Running setup verification...[/{COLORS['info']}]")
                from ..utils.setup_verifier import verify_setup
                verify_setup()
                
            else:
                console.print(f"\n[{COLORS['error']}]✗ Installation failed[/{COLORS['error']}]")
                console.print("Check the output above for errors")
                
        except FileExistsError as e:
            console.print(f"\n[{COLORS['error']}]Installation directory already exists[/{COLORS['error']}]")
            console.print("Please run the installation again and choose option 3 for complete reinstallation")
        except KeyboardInterrupt:
            console.print(f"\n[{COLORS['warning']}]Installation cancelled by user[/{COLORS['warning']}]")
        except Exception as e:
            console.print(f"\n[{COLORS['error']}]Installation error: {e}[/{COLORS['error']}]")
            import traceback
            if self.config.get('debug', False):
                traceback.print_exc()

    def analyze_concepts_interactive(self):
        """Interactive concept analysis"""
        from .analysis import ConceptAnalyzer, FILTER_PRESETS
        
        output_dir = self.config.get('default_output_dir', './output_csvs')
        output_path = Path(output_dir)
        
        if not output_path.exists() or not list(output_path.glob("*.csv")):
            console.print(f"[{COLORS['error']}]No output files found to analyze[/{COLORS['error']}]")
            return
        
        # Show analysis options
        console.print(Panel("[bold]Concept Analysis Options[/bold]", style=COLORS['header']))
        
        # Preset options
        console.print("\n[bold]Preset Filters:[/bold]")
        preset_table = Table(show_header=False, box=None)
        preset_table.add_column("Option", style=COLORS['option'])
        preset_table.add_column("Preset", style=COLORS['info'])
        preset_table.add_column("Description")
        
        preset_table.add_row("1", "kidney_stone", "Complete kidney stone analysis (concepts, symptoms, procedures)")
        preset_table.add_row("2", "kidney_symptoms", "Kidney stone symptoms only")
        preset_table.add_row("3", "kidney_procedures", "Kidney stone procedures only")
        preset_table.add_row("4", "diabetes", "Diabetes related concepts")
        preset_table.add_row("5", "hypertension", "Hypertension related concepts")
        preset_table.add_row("6", "pain", "Pain related concepts")
        preset_table.add_row("7", "custom", "Custom filter terms")
        preset_table.add_row("8", "all", "Analyze all concepts (no filter)")
        
        console.print(preset_table)
        
        choice = Prompt.ask("\nSelect analysis type", choices=["1", "2", "3", "4", "5", "6", "7", "8"], default="8")
        
        preset = None
        filter_terms = None
        
        if choice == "1":
            preset = "kidney_stone"
        elif choice == "2":
            preset = "kidney_symptoms"
        elif choice == "3":
            preset = "kidney_procedures"
        elif choice == "4":
            preset = "diabetes"
        elif choice == "5":
            preset = "hypertension"
        elif choice == "6":
            preset = "pain"
        elif choice == "7":
            # Custom filter
            terms = Prompt.ask("Enter filter terms (comma-separated)")
            filter_terms = [t.strip() for t in terms.split(',')]
        
        # Analysis options
        visualize = Confirm.ask("\nGenerate visualizations?", default=True)
        excel = Confirm.ask("Export to Excel?", default=False)
        
        # Run analysis
        analyzer = ConceptAnalyzer(output_path)
        
        filter_name = preset if preset else ("_".join(filter_terms) if filter_terms else "all")
        
        with console.status("Analyzing concepts...") as status:
            report = analyzer.analyze_directory(filter_terms, preset=preset)
        
        # Display results
        summary = report['summary']
        console.print(f"\n[{COLORS['success']}]✓ Analysis complete![/{COLORS['success']}]")
        console.print(f"  Files analyzed: {summary['files_analyzed']}")
        console.print(f"  Total concepts: {summary['total_concepts']:,}")
        console.print(f"  Total occurrences: {summary['total_occurrences']:,}")
        
        # Show top concepts
        if report['top_concepts']:
            console.print(f"\n[bold]Top 10 Concepts:[/bold]")
            for i, (concept, count) in enumerate(report['top_concepts'][:10], 1):
                console.print(f"  {i}. {concept}: {count}")
        
        # Generate outputs
        if visualize:
            with console.status("Generating visualizations..."):
                analyzer.generate_visualizations(output_path, filter_name)
            console.print(f"[{COLORS['success']}]✓ Visualizations saved to {output_path / 'visualizations'}[/{COLORS['success']}]")
        
        if excel:
            excel_file = output_path / f"{filter_name}_analysis.xlsx"
            with console.status("Exporting to Excel..."):
                analyzer.export_to_excel(excel_file, filter_name)
            console.print(f"[{COLORS['success']}]✓ Excel report saved to {excel_file}[/{COLORS['success']}]")
    
    def view_session_statistics(self):
        """View detailed session statistics"""
        from .analysis import ProcessingSessionAnalyzer
        
        output_dir = self.config.get('default_output_dir', './output_csvs')
        output_path = Path(output_dir)
        
        if not output_path.exists():
            console.print(f"[{COLORS['error']}]Output directory not found[/{COLORS['error']}]")
            return
        
        analyzer = ProcessingSessionAnalyzer(output_path)
        
        # Get session stats
        stats = analyzer.get_session_stats()
        
        # Display session info
        if stats['session_info']:
            console.print(Panel(
                f"[bold]Session ID:[/bold] {stats['session_info'].get('session_id', 'Unknown')}\n"
                f"[bold]Started:[/bold] {stats['session_info'].get('started', 'Unknown')}\n"
                f"[bold]Last Updated:[/bold] {stats['session_info'].get('last_updated', 'Unknown')}",
                title="Session Information",
                style=COLORS['info']
            ))
        
        # File statistics
        file_stats = stats['file_stats']
        if file_stats['total'] > 0:
            console.print("\n[bold]File Processing Statistics:[/bold]")
            console.print(f"  Total Files: {file_stats['total']}")
            console.print(f"  Completed: {file_stats['completed']} ({file_stats['completed']/file_stats['total']*100:.1f}%)")
            console.print(f"  Failed: {file_stats['failed']} ({file_stats['failed']/file_stats['total']*100:.1f}%)")
            console.print(f"  In Progress: {file_stats['in_progress']}")
            console.print(f"  Success Rate: {file_stats['success_rate']:.1f}%")
        
        # Performance metrics
        perf = stats['performance']
        if perf['avg_processing_time'] > 0:
            console.print("\n[bold]Performance Metrics:[/bold]")
            console.print(f"  Average Processing Time: {perf['avg_processing_time']:.1f} seconds/file")
            console.print(f"  Throughput: {perf['throughput']:.2f} files/second")
            console.print(f"  Total Processing Time: {perf['total_time']/60:.1f} minutes")
        
        # Failed file analysis
        if stats['failed_analysis']['by_error_type']:
            console.print("\n[bold]Failed File Analysis:[/bold]")
            for error_type, count in stats['failed_analysis']['by_error_type'].items():
                console.print(f"  {error_type}: {count}")
            
            if stats['failed_analysis']['retry_candidates']:
                console.print(f"\n[yellow]ℹ {len(stats['failed_analysis']['retry_candidates'])} files can be retried[/yellow]")
        
        # Output statistics
        output_stats = stats['output_stats']
        if output_stats['total_size'] > 0:
            console.print("\n[bold]Output Statistics:[/bold]")
            console.print(f"  Total Size: {output_stats['total_size'] / 1024 / 1024:.1f} MB")
            console.print(f"  Average File Size: {output_stats['avg_file_size'] / 1024:.1f} KB")
            console.print(f"  Total Concepts: {output_stats['total_concepts']:,}")
        
        # Options
        console.print("\n[bold]Options:[/bold]")
        if stats['failed_analysis']['retry_candidates']:
            if Confirm.ask("Retry failed files?"):
                # Run retry command
                from ..cli.commands import retry_failed
                ctx = click.Context(retry_failed)
                ctx.invoke(retry_failed, output_dir=str(output_path), dry_run=False)
        
        if Confirm.ask("\nSync state with filesystem?"):
            sync_stats = analyzer.sync_with_filesystem()
            if any(v > 0 for k, v in sync_stats.items() if k.startswith('outputs_not') or k.startswith('state_not')):
                console.print("\n[yellow]⚠ Discrepancies found - state file may be out of sync[/yellow]")
            else:
                console.print("[green]✓ State is synchronized with filesystem[/green]")

    def monitor_background(self):
        """Monitor background processing"""
        self.clear_screen()
        console.print(Panel("[bold]Monitor Background Process[/bold]", style=COLORS['header']))
        
        output_dir = self.config.get('default_output_dir', './output_csvs')
        output_path = Path(output_dir)
        
        # Check for background PID file
        pid_file = output_path / ".background_pid"
        if not pid_file.exists():
            console.print(f"[{COLORS['error']}]No background process found[/{COLORS['error']}]")
            return
        
        try:
            pid = int(pid_file.read_text().strip())
            
            # Check if process is still running
            import psutil
            try:
                proc = psutil.Process(pid)
                if proc.is_running():
                    console.print(f"[{COLORS['success']}]Background process running[/{COLORS['success']}] (PID: {pid})")
                    
                    # Show process info
                    info_table = Table(box=box.ROUNDED)
                    info_table.add_column("Property", style=COLORS['info'])
                    info_table.add_column("Value", style=COLORS['success'])
                    
                    info_table.add_row("CPU Usage", f"{proc.cpu_percent(interval=1)}%")
                    info_table.add_row("Memory Usage", f"{proc.memory_info().rss / 1024 / 1024:.1f} MB")
                    info_table.add_row("Running Time", str(datetime.now() - datetime.fromtimestamp(proc.create_time())))
                    
                    console.print(info_table)
                else:
                    console.print(f"[{COLORS['warning']}]Process {pid} is not running[/{COLORS['warning']}]")
                    pid_file.unlink()
            except psutil.NoSuchProcess:
                console.print(f"[{COLORS['error']}]Process {pid} not found[/{COLORS['error']}]")
                pid_file.unlink()
                
        except Exception as e:
            console.print(f"[{COLORS['error']}]Error reading PID file: {e}[/{COLORS['error']}]")
        
        # Show processing statistics
        console.print(f"\n[bold]Processing Statistics:[/bold]")
        try:
            state_mgr = StateManager(str(output_path))
            stats = state_mgr.get_statistics()
            
            stats_table = Table(box=box.ROUNDED)
            stats_table.add_column("Metric", style=COLORS['info'])
            stats_table.add_column("Value", style=COLORS['success'])
            
            stats_table.add_row("Total Files", str(stats['total_files']))
            stats_table.add_row("Completed", str(stats['completed']))
            stats_table.add_row("Failed", str(stats['failed']))
            stats_table.add_row("In Progress", str(stats['in_progress']))
            
            if stats['total_files'] > 0:
                progress = (stats['completed'] / stats['total_files']) * 100
                stats_table.add_row("Progress", f"{progress:.1f}%")
                
                # Add progress bar
                completed_blocks = int(progress / 5)  # 20 blocks total
                progress_bar = "█" * completed_blocks + "░" * (20 - completed_blocks)
                stats_table.add_row("", f"[{COLORS['success']}]{progress_bar}[/{COLORS['success']}]")
            
            console.print(stats_table)
            
            # Show concept statistics - handle gracefully if not available
            try:
                concept_stats = state_mgr.get_concept_statistics()
                if concept_stats.get('total_concepts', 0) > 0:
                    console.print(f"\n[bold]Concept Extraction:[/bold]")
                    
                    concept_table = Table(box=box.ROUNDED)
                    concept_table.add_column("Metric", style=COLORS['info'])
                    concept_table.add_column("Value", style=COLORS['success'])
                    
                    concept_table.add_row("Total Concepts", f"{concept_stats.get('total_concepts', 0):,}")
                    concept_table.add_row("Unique Concepts", f"{concept_stats.get('unique_concepts', 0):,}")
                    concept_table.add_row("Semantic Types", str(concept_stats.get('total_semantic_types', 0)))
                    
                    console.print(concept_table)
                    
                    # Show top 5 concepts
                    if concept_stats.get('top_concepts'):
                        console.print(f"\n[bold]Top 5 Concepts:[/bold]")
                        top_table = Table(box=box.SIMPLE)
                        top_table.add_column("#", style=COLORS['dim'])
                        top_table.add_column("Concept", style=COLORS['info'])
                        top_table.add_column("Count", style=COLORS['warning'])
                        
                        for i, (cui, name, count) in enumerate(concept_stats['top_concepts'][:5], 1):
                            top_table.add_row(str(i), f"{name} ({cui})", str(count))
                        
                        console.print(top_table)
            except Exception as e:
                # Concept stats not available yet
                pass
                
        except Exception as e:
            console.print(f"[{COLORS['error']}]Error reading state: {e}[/{COLORS['error']}]")
        
        # Options
        console.print(f"\n[bold]Options:[/bold]")
        console.print("  1. Follow log output")
        console.print("  2. Kill background process")
        console.print("  3. Refresh (live monitor)")
        console.print("  4. Back")
        
        choice = Prompt.ask("\nSelect option", choices=["1", "2", "3", "4"], default="3")
        
        if choice == "1":
            # Find the latest log file
            logs_dir = output_path / "logs"
            if logs_dir.exists():
                log_files = sorted(logs_dir.glob("background_*.log"), key=lambda x: x.stat().st_mtime)
                if log_files:
                    latest_log = log_files[-1]
                    console.print(f"\n[{COLORS['info']}]Following log: {latest_log}[/{COLORS['info']}]")
                    console.print(f"[{COLORS['dim']}]Press Ctrl+C to stop[/{COLORS['dim']}]\n")
                    
                    try:
                        import subprocess
                        subprocess.run(["tail", "-f", str(latest_log)])
                    except KeyboardInterrupt:
                        console.print(f"\n[{COLORS['warning']}]Stopped following log[/{COLORS['warning']}]")
                else:
                    console.print(f"[{COLORS['warning']}]No log files found[/{COLORS['warning']}]")
            else:
                console.print(f"[{COLORS['warning']}]No logs directory found[/{COLORS['warning']}]")
                
        elif choice == "2":
            if Confirm.ask(f"[{COLORS['warning']}]Kill background process?[/{COLORS['warning']}]"):
                try:
                    import psutil
                    proc = psutil.Process(pid)
                    proc.terminate()
                    time.sleep(1)
                    if proc.is_running():
                        proc.kill()
                    pid_file.unlink()
                    console.print(f"[{COLORS['success']}]✓ Background process terminated[/{COLORS['success']}]")
                except Exception as e:
                    console.print(f"[{COLORS['error']}]Error killing process: {e}[/{COLORS['error']}]")
                    
        elif choice == "3":
            # Live refresh mode
            console.print(f"\n[{COLORS['info']}]Live monitoring mode - Press Ctrl+C to stop[/{COLORS['info']}]")
            time.sleep(1)
            
            try:
                while True:
                    self.clear_screen()
                    console.print(Panel("[bold]Monitor Background Process - Live[/bold]", style=COLORS['header']))
                    
                    # Check process status
                    try:
                        proc = psutil.Process(pid)
                        if proc.is_running():
                            console.print(f"[{COLORS['success']}]● Process running[/{COLORS['success']}] (PID: {pid})")
                            console.print(f"CPU: {proc.cpu_percent(interval=0.1)}% | Memory: {proc.memory_info().rss / 1024 / 1024:.1f} MB")
                        else:
                            console.print(f"[{COLORS['error']}]Process stopped[/{COLORS['error']}]")
                            break
                    except:
                        console.print(f"[{COLORS['error']}]Process not found[/{COLORS['error']}]")
                        break
                    
                    # Show live stats
                    try:
                        state_mgr = StateManager(str(output_path))
                        stats = state_mgr.get_statistics()
                        
                        if stats['total_files'] > 0:
                            progress = (stats['completed'] / stats['total_files']) * 100
                            completed_blocks = int(progress / 5)
                            progress_bar = "█" * completed_blocks + "░" * (20 - completed_blocks)
                            
                            console.print(f"\nProgress: [{COLORS['success']}]{progress_bar}[/{COLORS['success']}] {progress:.1f}%")
                            console.print(f"Files: {stats['completed']}/{stats['total_files']} (Failed: {stats['failed']})")
                            
                            # Show concept count if available
                            try:
                                concept_stats = state_mgr.get_concept_statistics()
                                if concept_stats.get('total_concepts', 0) > 0:
                                    console.print(f"Concepts: {concept_stats.get('total_concepts', 0):,} total, {concept_stats.get('unique_concepts', 0):,} unique")
                            except:
                                pass
                    except:
                        pass
                    
                    time.sleep(2)  # Refresh every 2 seconds
                    
            except KeyboardInterrupt:
                console.print(f"\n[{COLORS['warning']}]Stopped live monitoring[/{COLORS['warning']}]")
        
        if choice not in ["3", "4"]:
            console.input("\nPress Enter to continue...")
            self.monitor_background()

    def export_analysis_report(self):
        """Export comprehensive analysis report"""
        from .analysis import ConceptAnalyzer, ProcessingSessionAnalyzer, FILTER_PRESETS
        
        output_dir = self.config.get('default_output_dir', './output_csvs')
        output_path = Path(output_dir)
        
        if not output_path.exists():
            console.print(f"[{COLORS['error']}]Output directory not found[/{COLORS['error']}]")
            return
        
        console.print(Panel("[bold]Export Analysis Report[/bold]", style=COLORS['header']))
        
        # Ask what type of report
        console.print("\n[bold]Report Type:[/bold]")
        console.print("  1. Comprehensive report (all concepts + session stats)")
        console.print("  2. Kidney stone analysis report")
        console.print("  3. Custom filtered report")
        
        report_type = Prompt.ask("Select report type", choices=["1", "2", "3"], default="1")
        
        # Initialize analyzers
        concept_analyzer = ConceptAnalyzer(output_path)
        session_analyzer = ProcessingSessionAnalyzer(output_path)
        
        # Create report structure
        report = {
            'generated': datetime.now().isoformat(),
            'output_directory': str(output_path),
            'report_type': '',
            'concept_analysis': {},
            'session_statistics': {}
        }
        
        # Generate report based on type
        with console.status("Generating report...") as status:
            # Always include session statistics
            report['session_statistics'] = session_analyzer.get_session_stats()
            
            if report_type == "1":
                # Comprehensive report
                report['report_type'] = 'comprehensive'
                report['concept_analysis'] = concept_analyzer.analyze_directory()
                filter_name = "comprehensive"
                
            elif report_type == "2":
                # Kidney stone analysis
                report['report_type'] = 'kidney_stone_analysis'
                report['concept_analysis'] = concept_analyzer.analyze_directory(preset='kidney_stone')
                filter_name = "kidney_stone"
                
            elif report_type == "3":
                # Custom filter
                terms = Prompt.ask("Enter filter terms (comma-separated)")
                filter_terms = [t.strip() for t in terms.split(',')]
                report['report_type'] = f'custom_filter: {", ".join(filter_terms)}'
                report['concept_analysis'] = concept_analyzer.analyze_directory(filter_terms)
                filter_name = "_".join(filter_terms)
        
        # Export options
        console.print("\n[bold]Export Options:[/bold]")
        
        # JSON report
        json_path = output_path / f"{filter_name}_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        import json
        with open(json_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        console.print(f"[{COLORS['success']}]✓ JSON report saved to {json_path}[/{COLORS['success']}]")
        
        # Excel report
        if Confirm.ask("\nExport to Excel?"):
            excel_path = output_path / f"{filter_name}_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            concept_analyzer.export_to_excel(excel_path, filter_name)
            console.print(f"[{COLORS['success']}]✓ Excel report saved to {excel_path}[/{COLORS['success']}]")
        
        # Visualizations
        if Confirm.ask("\nGenerate visualizations?"):
            concept_analyzer.generate_visualizations(output_path, filter_name)
            console.print(f"[{COLORS['success']}]✓ Visualizations saved to {output_path / 'visualizations'}[/{COLORS['success']}]")
        
        # Summary
        summary = report['concept_analysis']['summary']
        console.print(f"\n[bold]Report Summary:[/bold]")
        console.print(f"  Files Analyzed: {summary['files_analyzed']}")
        console.print(f"  Total Concepts: {summary['total_concepts']:,}")
        console.print(f"  Total Occurrences: {summary['total_occurrences']:,}")
        console.print(f"  Success Rate: {report['session_statistics']['file_stats']['success_rate']:.1f}%")

    def retry_failed_files(self):
        """Retry failed files with interactive options"""
        output_dir = self.config.get('default_output_dir', './output_csvs')
        output_path = Path(output_dir)
        
        if not output_path.exists():
            console.print(f"[{COLORS['error']}]Output directory not found: {output_dir}[/{COLORS['error']}]")
            return
        
        # Check for state file
        state_file = output_path / ".pymm_state.json"
        if not state_file.exists():
            console.print(f"[{COLORS['error']}]No processing state found. Cannot retry without previous session data.[/{COLORS['error']}]")
            return
        
        # Load state
        import json
        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
        except Exception as e:
            console.print(f"[{COLORS['error']}]Error reading state file: {e}[/{COLORS['error']}]")
            return
        
        # Get failed files
        failed_files = state.get('failed_files', {})
        if not failed_files:
            console.print(f"[{COLORS['success']}]No failed files to retry![/{COLORS['success']}]")
            return
        
        # Show failed files summary
        self.clear_screen()
        console.print(Panel("[bold]Retry Failed Files[/bold]", style=COLORS['header']))
        
        # Categorize errors
        error_types = {}
        for file_path, error_info in failed_files.items():
            error_msg = error_info.get('error', 'Unknown error')
            error_type = 'Timeout' if 'timeout' in error_msg.lower() else \
                        'Memory' if 'memory' in error_msg.lower() else \
                        'Java' if 'java' in error_msg.lower() else \
                        'Other'
            
            if error_type not in error_types:
                error_types[error_type] = []
            error_types[error_type].append({
                'path': file_path,
                'name': Path(file_path).name,
                'attempts': error_info.get('attempts', 1),
                'error': error_msg
            })
        
        # Show summary
        summary_table = Table(title="Failed Files Summary", box=box.ROUNDED)
        summary_table.add_column("Error Type", style=COLORS['info'])
        summary_table.add_column("Count", style=COLORS['warning'])
        summary_table.add_column("Example", style=COLORS['dim'])
        
        total_failed = len(failed_files)
        for error_type, files in error_types.items():
            example = files[0]['name'] if files else '-'
            summary_table.add_row(error_type, str(len(files)), example)
        
        console.print(summary_table)
        console.print(f"\n[bold]Total failed files: {total_failed}[/bold]")
        
        # Show retry options
        console.print("\n[bold]Retry Options:[/bold]")
        options_table = Table(show_header=False, box=None)
        options_table.add_column("Option", style=COLORS['option'])
        options_table.add_column("Description")
        
        options_table.add_row("1", "Retry all failed files")
        options_table.add_row("2", "Retry specific error type")
        options_table.add_row("3", "View detailed error list")
        options_table.add_row("4", "Configure retry settings")
        options_table.add_row("5", "Dry run (preview what would be retried)")
        options_table.add_row("B", "Back")
        
        console.print(options_table)
        
        choice = Prompt.ask("\nSelect option", choices=["1", "2", "3", "4", "5", "b"]).lower()
        
        if choice == "1":
            # Retry all
            self._do_retry(output_dir, failed_files)
        elif choice == "2":
            # Retry specific error type
            error_type = Prompt.ask(
                "Select error type to retry",
                choices=list(error_types.keys())
            )
            filtered_files = {
                f['path']: failed_files[f['path']] 
                for f in error_types[error_type]
            }
            self._do_retry(output_dir, filtered_files)
        elif choice == "3":
            # View detailed list
            self._show_detailed_errors(failed_files)
            console.input("\nPress Enter to continue...")
            self.retry_failed_files()
        elif choice == "4":
            # Configure retry settings
            self._configure_retry_settings()
            self.retry_failed_files()
        elif choice == "5":
            # Dry run
            self._do_retry(output_dir, failed_files, dry_run=True)
            console.input("\nPress Enter to continue...")
            self.retry_failed_files()
    
    def _do_retry(self, output_dir: str, failed_files: dict, dry_run: bool = False):
        """Execute retry with given files"""
        max_attempts = self.config.get('retry_max_attempts', 3)
        delay = self.config.get('retry_base_delay', 5)
        
        # Filter candidates based on max attempts
        retry_candidates = []
        for file_path, error_info in failed_files.items():
            attempts = error_info.get('attempts', 1)
            if attempts < max_attempts:
                retry_candidates.append({
                    'path': file_path,
                    'name': Path(file_path).name,
                    'attempts': attempts,
                    'error': error_info.get('error', 'Unknown')
                })
        
        if not retry_candidates:
            console.print(f"[{COLORS['warning']}]No files eligible for retry (all exceeded max attempts)[/{COLORS['warning']}]")
            return
        
        # Show what will be retried
        console.print(f"\n[bold]Will retry {len(retry_candidates)} files[/bold]")
        console.print(f"Max attempts: {max_attempts}")
        console.print(f"Delay between retries: {delay}s")
        
        if dry_run:
            console.print(f"\n[{COLORS['warning']}]DRY RUN - No files will be processed[/{COLORS['warning']}]")
            # Show first 10 files
            for i, candidate in enumerate(retry_candidates[:10]):
                console.print(f"  • {candidate['name']} (attempt {candidate['attempts'] + 1})")
            if len(retry_candidates) > 10:
                console.print(f"  ... and {len(retry_candidates) - 10} more")
            return
        
        if not Confirm.ask(f"\nProceed with retry?"):
            return
        
        # Check servers
        if not self.check_servers():
            if Confirm.ask("[yellow]Servers not running. Start them now?[/yellow]"):
                self.start_servers()
            else:
                return
        
        # Execute retry using the CLI command
        try:
            from ..processing.batch_runner import BatchRunner
            from ..core.state import StateManager
            
            # Get input directory from state
            state_file = Path(output_dir) / ".pymm_state.json"
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            input_dir = state.get('input_dir')
            if not input_dir or not Path(input_dir).exists():
                console.print(f"[{COLORS['error']}]Original input directory not found[/{COLORS['error']}]")
                return
            
            # Create runner for retry
            runner = BatchRunner(input_dir, output_dir, self.config)
            
            # Process with progress
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=console
            ) as progress:
                task = progress.add_task("Retrying failed files...", total=len(retry_candidates))
                
                successful = 0
                for candidate in retry_candidates:
                    file_path = candidate['path']
                    progress.update(task, description=f"Retrying {candidate['name']}...")
                    
                    try:
                        # Add delay between retries
                        if successful > 0:
                            time.sleep(delay)
                        
                        # Process file
                        runner._process_single_file(Path(file_path))
                        successful += 1
                        
                    except Exception as e:
                        console.print(f"[{COLORS['error']}]Failed: {candidate['name']} - {str(e)}[/{COLORS['error']}]")
                    
                    progress.update(task, advance=1)
            
            # Show results
            console.print(f"\n[bold]Retry Results:[/bold]")
            console.print(f"  [{COLORS['success']}]✓ Successfully processed: {successful}[/{COLORS['success']}]")
            console.print(f"  [{COLORS['error']}]✗ Still failing: {len(retry_candidates) - successful}[/{COLORS['error']}]")
            
        except Exception as e:
            console.print(f"[{COLORS['error']}]Retry error: {e}[/{COLORS['error']}]")
    
    def _show_detailed_errors(self, failed_files: dict):
        """Show detailed error information"""
        error_table = Table(title="Failed Files Details", box=box.ROUNDED)
        error_table.add_column("File", style=COLORS['cyan'])
        error_table.add_column("Attempts", style=COLORS['yellow'])
        error_table.add_column("Error", style=COLORS['error'], max_width=50)
        error_table.add_column("Timestamp", style=COLORS['dim'])
        
        for file_path, error_info in list(failed_files.items())[:20]:
            error_table.add_row(
                Path(file_path).name,
                str(error_info.get('attempts', 1)),
                error_info.get('error', 'Unknown')[:50] + '...' if len(error_info.get('error', '')) > 50 else error_info.get('error', 'Unknown'),
                error_info.get('timestamp', '-')[:19] if error_info.get('timestamp') else '-'
            )
        
        console.print(error_table)
        
        if len(failed_files) > 20:
            console.print(f"\n[{COLORS['dim']}]Showing first 20 of {len(failed_files)} failed files[/{COLORS['dim']}]")
    
    def _configure_retry_settings(self):
        """Configure retry-specific settings"""
        console.print("\n[bold]Retry Configuration[/bold]")
        
        current_max_attempts = self.config.get('retry_max_attempts', 3)
        current_delay = self.config.get('retry_base_delay', 5)
        current_timeout = self.config.get('pymm_timeout', 300)
        
        settings_table = Table(box=box.ROUNDED)
        settings_table.add_column("Setting", style=COLORS['info'])
        settings_table.add_column("Current Value", style=COLORS['option'])
        settings_table.add_column("Description", style=COLORS['dim'])
        
        settings_table.add_row("Max Attempts", str(current_max_attempts), "Maximum retry attempts per file")
        settings_table.add_row("Base Delay", f"{current_delay}s", "Delay between retries")
        settings_table.add_row("Timeout", f"{current_timeout}s", "Timeout per file")
        
        console.print(settings_table)
        
        if Confirm.ask("\nModify settings?"):
            new_attempts = IntPrompt.ask("Max retry attempts", default=current_max_attempts)
            new_delay = IntPrompt.ask("Delay between retries (seconds)", default=current_delay)
            new_timeout = IntPrompt.ask("Timeout per file (seconds)", default=current_timeout)
            
            self.config.set('retry_max_attempts', new_attempts)
            self.config.set('retry_base_delay', new_delay)
            self.config.set('pymm_timeout', new_timeout)
            
            console.print(f"[{COLORS['success']}]✓ Retry settings updated[/{COLORS['success']}]")


def interactive_mode():
    """Launch interactive mode"""
    navigator = InteractiveNavigator()
    navigator.run()


# Add to main CLI
@click.command()
def interactive():
    """Launch interactive mode with intuitive navigator"""
    interactive_mode()


if __name__ == "__main__":
    interactive_mode()