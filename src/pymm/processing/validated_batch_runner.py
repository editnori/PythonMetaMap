"""Batch runner with comprehensive validation"""
import os
import sys
import time
import shutil
import subprocess
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any
import psutil

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from ..core.config import PyMMConfig
from ..server.manager import ServerManager
from .optimized_batch_runner import OptimizedBatchRunner

console = Console()


class ValidationResult:
    """Validation result container"""
    def __init__(self):
        self.passed = True
        self.checks = []
        self.warnings = []
        self.errors = []
        
    def add_check(self, name: str, status: bool, message: str = ""):
        self.checks.append({
            'name': name,
            'status': status,
            'message': message
        })
        if not status:
            self.passed = False
            
    def add_warning(self, message: str):
        self.warnings.append(message)
        
    def add_error(self, message: str):
        self.errors.append(message)
        self.passed = False


class ValidatedBatchRunner(OptimizedBatchRunner):
    """Batch runner with comprehensive pre-run validation"""
    
    def __init__(self, input_dir: str, output_dir: str, config: PyMMConfig):
        super().__init__(input_dir, output_dir, config)
        self.server_manager = ServerManager(config)
        
    def validate_environment(self) -> ValidationResult:
        """Comprehensive validation of environment and prerequisites"""
        result = ValidationResult()
        
        console.print("\n[bold cyan]Running Pre-Processing Validation...[/bold cyan]\n")
        
        # 1. Check Java
        java_ok, java_version = self._check_java()
        result.add_check(
            "Java Runtime", 
            java_ok, 
            f"Found: {java_version}" if java_ok else "Java not found - MetaMap requires Java 8+"
        )
        
        # 2. Check MetaMap installation
        metamap_ok, metamap_path = self._check_metamap()
        result.add_check(
            "MetaMap Binary",
            metamap_ok,
            f"Found at: {metamap_path}" if metamap_ok else "MetaMap not found at configured path"
        )
        
        # 3. Check server binaries
        servers_ok, server_msg = self._check_server_binaries()
        result.add_check(
            "MetaMap Servers",
            servers_ok,
            server_msg
        )
        
        # 4. Check input directory
        input_ok, input_files = self._check_input_directory()
        result.add_check(
            "Input Directory",
            input_ok,
            f"Found {len(input_files)} text files" if input_ok else "No valid input files found"
        )
        
        # 5. Check output directory
        output_ok, output_msg = self._check_output_directory()
        result.add_check(
            "Output Directory",
            output_ok,
            output_msg
        )
        
        # 6. Check system resources
        resources_ok, resource_msg = self._check_system_resources()
        if not resources_ok:
            result.add_warning(resource_msg)
        result.add_check(
            "System Resources",
            True,  # Don't fail on resources, just warn
            resource_msg
        )
        
        # 7. Check port availability
        ports_ok, ports_msg = self._check_ports()
        result.add_check(
            "Network Ports",
            ports_ok,
            ports_msg
        )
        
        # 8. Check if servers are running
        servers_running, servers_status = self._check_servers_running()
        result.add_check(
            "Server Status",
            servers_running,
            servers_status
        )
        
        return result
        
    def _check_java(self) -> Tuple[bool, str]:
        """Check if Java is installed and get version"""
        try:
            result = subprocess.run(
                ['java', '-version'], 
                capture_output=True, 
                text=True
            )
            if result.returncode == 0:
                # Java version is in stderr
                version_line = result.stderr.split('\n')[0]
                return True, version_line
        except:
            pass
        return False, "Not installed"
        
    def _check_metamap(self) -> Tuple[bool, str]:
        """Check if MetaMap binary exists"""
        metamap_home = self.config.get('metamap_home')
        if not metamap_home:
            return False, "METAMAP_HOME not configured"
            
        # Check for metamap binary
        metamap_bin = Path(metamap_home) / "bin" / "metamap"
        if metamap_bin.exists():
            return True, str(metamap_bin)
            
        # Also check with .bat extension on Windows
        metamap_bat = Path(metamap_home) / "bin" / "metamap.bat"
        if metamap_bat.exists():
            return True, str(metamap_bat)
            
        return False, f"Not found in {metamap_home}/bin"
        
    def _check_server_binaries(self) -> Tuple[bool, str]:
        """Check if server binaries exist"""
        metamap_home = self.config.get('metamap_home')
        if not metamap_home:
            return False, "METAMAP_HOME not configured"
            
        bin_dir = Path(metamap_home) / "bin"
        
        # Check for required server binaries
        required_servers = ['skrmedpostctl', 'wsdserverctl']
        found = []
        missing = []
        
        for server in required_servers:
            server_path = bin_dir / server
            if server_path.exists() or (bin_dir / f"{server}.bat").exists():
                found.append(server)
            else:
                missing.append(server)
                
        if missing:
            return False, f"Missing: {', '.join(missing)}"
        else:
            return True, f"All servers found: {', '.join(found)}"
            
    def _check_input_directory(self) -> Tuple[bool, List[Path]]:
        """Check input directory and count valid files"""
        if not self.input_dir.exists():
            return False, []
            
        # Find all text files
        text_files = []
        for ext in ['*.txt', '*.text']:
            text_files.extend(self.input_dir.glob(ext))
            
        return len(text_files) > 0, text_files
        
    def _check_output_directory(self) -> Tuple[bool, str]:
        """Check output directory"""
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            
            # Check write permissions
            test_file = self.output_dir / ".test_write"
            test_file.write_text("test")
            test_file.unlink()
            
            # Check if directory already has files
            existing_files = list(self.output_dir.glob("*.csv"))
            if existing_files:
                return True, f"Directory exists with {len(existing_files)} existing CSV files"
            else:
                return True, "Directory ready for output"
        except Exception as e:
            return False, f"Cannot write to directory: {str(e)}"
            
    def _check_system_resources(self) -> Tuple[bool, str]:
        """Check system resources"""
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage(str(self.output_dir))
        
        warnings = []
        
        # Check memory (warn if less than 4GB available)
        if mem.available < 4 * 1024 * 1024 * 1024:
            warnings.append(f"Low memory: {mem.available / (1024**3):.1f}GB available")
            
        # Check disk space (warn if less than 1GB)
        if disk.free < 1024 * 1024 * 1024:
            warnings.append(f"Low disk space: {disk.free / (1024**3):.1f}GB free")
            
        if warnings:
            return False, "; ".join(warnings)
        else:
            return True, f"Memory: {mem.available / (1024**3):.1f}GB, Disk: {disk.free / (1024**3):.1f}GB"
            
    def _check_ports(self) -> Tuple[bool, str]:
        """Check if required ports are available"""
        ports_to_check = [
            (self.config.get('tagger_server_port', 1795), 'Tagger Server'),
            (self.config.get('wsd_server_port', 5554), 'WSD Server')
        ]
        
        blocked = []
        for port, name in ports_to_check:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                result = sock.connect_ex(('localhost', port))
                if result == 0:
                    # Port is in use - check if it's our server
                    if name == 'Tagger Server' and self.server_manager.is_tagger_server_running():
                        continue
                    elif name == 'WSD Server' and self.server_manager.is_wsd_server_running():
                        continue
                    else:
                        blocked.append(f"{name} port {port}")
            finally:
                sock.close()
                
        if blocked:
            return False, f"Ports in use: {', '.join(blocked)}"
        else:
            return True, "All ports available"
            
    def _check_servers_running(self) -> Tuple[bool, str]:
        """Check if MetaMap servers are running"""
        try:
            tagger_running = self.server_manager.is_tagger_server_running()
            wsd_running = self.server_manager.is_wsd_server_running()
            
            if tagger_running and wsd_running:
                return True, "Both servers running"
            elif tagger_running:
                return False, "Only Tagger server running (WSD server not running)"
            elif wsd_running:
                return False, "Only WSD server running (Tagger server not running)"
            else:
                return False, "No servers running (will start automatically)"
        except Exception as e:
            return False, f"Unable to check server status: {e}"
            
    def display_validation_results(self, result: ValidationResult) -> bool:
        """Display validation results in a nice table"""
        table = Table(title="Validation Results", box="rounded")
        table.add_column("Check", style="cyan")
        table.add_column("Status", style="bold")
        table.add_column("Details", style="dim")
        
        for check in result.checks:
            status_icon = "[PASS]" if check['status'] else "[FAIL]"
            status_color = "green" if check['status'] else "red"
            table.add_row(
                check['name'],
                f"[{status_color}]{status_icon}[/{status_color}]",
                check['message']
            )
            
        console.print(table)
        
        # Show warnings
        if result.warnings:
            console.print("\n[yellow]Warnings:[/yellow]")
            for warning in result.warnings:
                console.print(f"   - {warning}")
                
        # Show errors
        if result.errors:
            console.print("\n[red]Errors:[/red]")
            for error in result.errors:
                console.print(f"   - {error}")
                
        return result.passed
        
    def run_with_validation(self, force: bool = False) -> Dict[str, Any]:
        """Run batch processing with validation"""
        # Run validation
        validation = self.validate_environment()
        
        # Display results
        passed = self.display_validation_results(validation)
        
        if not passed and not force:
            console.print("\n[red]Validation failed. Please fix the issues above and try again.[/red]")
            return {"status": "failed", "reason": "validation_failed"}
            
        # Get file count for confirmation
        input_files = list(self.input_dir.glob("*.txt"))
        input_files.extend(self.input_dir.glob("*.text"))
        
        # Show confirmation prompt
        console.print(f"\n[bold]Ready to process {len(input_files)} files[/bold]")
        console.print(f"Input: {self.input_dir}")
        console.print(f"Output: {self.output_dir}")
        console.print(f"Output Format: CSV (MetaMap structured output)")
        
        if not force and not Confirm.ask("\nProceed with processing?"):
            return {"status": "cancelled", "reason": "user_cancelled"}
            
        # Start servers
        console.print("\n[cyan]Starting MetaMap servers...[/cyan]")
        if not self.server_manager.is_running():
            try:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console
                ) as progress:
                    task = progress.add_task("Starting servers...", total=None)
                    self.server_manager.start_all()
                    time.sleep(3)  # Give servers time to initialize
                    progress.update(task, completed=1)
                    
                console.print("[green]MetaMap servers started successfully[/green]")
            except Exception as e:
                console.print(f"[red]Failed to start servers: {e}[/red]")
                return {"status": "failed", "reason": "server_start_failed"}
        else:
            console.print("[green]MetaMap servers already running[/green]")
            
        # Run the actual processing
        console.print("\n[bold cyan]Starting batch processing...[/bold cyan]\n")
        return super().run(progress_callback=None)