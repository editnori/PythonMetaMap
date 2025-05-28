"""CLI sub-commands for server and config management"""
import click
import sys
import os
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt, Confirm
from pathlib import Path
from datetime import datetime
from rich import box
import subprocess

from ..core.config import PyMMConfig
from ..server.manager import ServerManager
from ..server.port_guard import PortGuard
from ..core.state import StateManager

console = Console()

# Server management commands
@click.group()
def server_group():
    """Manage MetaMap servers"""
    pass

@server_group.command(name='start')
@click.option('--timeout', '-t', default=60, help='Timeout for server startup')
def server_start(timeout):
    """Start MetaMap servers"""
    config = PyMMConfig()
    
    if not config.get("metamap_binary_path"):
        console.print("[red]MetaMap not configured. Run 'pymm config setup' first.[/red]")
        sys.exit(1)
    
    manager = ServerManager(config)
    
    with console.status("Starting servers...") as status:
        if manager.start_all():
            console.print("[green]✓ Servers started successfully[/green]")
        else:
            console.print("[red]✗ Failed to start servers[/red]")
            sys.exit(1)

@server_group.command(name='stop')
def server_stop():
    """Stop MetaMap servers"""
    config = PyMMConfig()
    manager = ServerManager(config)
    
    with console.status("Stopping servers...") as status:
        manager.stop_all()
    
    console.print("[green]✓ Servers stopped[/green]")

@server_group.command(name='restart')
@click.argument('service', required=False, type=click.Choice(['tagger', 'wsd', 'all']))
def server_restart(service):
    """Restart MetaMap servers"""
    config = PyMMConfig()
    manager = ServerManager(config)
    
    service = service or 'all'
    
    with console.status(f"Restarting {service}...") as status:
        if manager.restart_service(service):
            console.print(f"[green]✓ {service} restarted successfully[/green]")
        else:
            console.print(f"[red]✗ Failed to restart {service}[/red]")
            sys.exit(1)

@server_group.command(name='status')
@click.option('--detailed', '-d', is_flag=True, help='Show detailed status')
@click.option('--pool', '-p', is_flag=True, help='Show server pool status')
def server_status(detailed, pool):
    """Show server status"""
    config = PyMMConfig()
    manager = ServerManager(config)
    
    status = manager.get_status()
    
    table = Table(title="MetaMap Server Status")
    table.add_column("Service", style="cyan")
    table.add_column("Status", style="bold")
    table.add_column("Port", style="yellow")
    table.add_column("PID", style="dim")
    
    for service, info in status.items():
        status_style = "green" if info['status'] == "RUNNING" else "red"
        table.add_row(
            service.upper(),
            f"[{status_style}]{info['status']}[/{status_style}]",
            str(info.get('port', '-')),
            str(info.get('pid', '-'))
        )
    
    console.print(table)
    
    if detailed:
        # Port status
        port_status = PortGuard.get_port_status()
        
        port_table = Table(title="Port Details")
        port_table.add_column("Service", style="cyan")
        port_table.add_column("Port", style="yellow")
        port_table.add_column("Available", style="bold")
        port_table.add_column("Blocking Process")
        
        for service, info in port_status.items():
            available = "[green]Yes[/green]" if info['available'] else "[red]No[/red]"
            blocker = "-"
            
            if not info['available'] and info['blocking_process']:
                proc = info['blocking_process']
                blocker = f"{proc['name']} (PID: {proc['pid']})"
            
            port_table.add_row(service, str(info['port']), available, blocker)
        
        console.print("\n")
        console.print(port_table)
    
    if pool:
        # Show server pool status
        pool_status = manager.get_server_pool_status()
        
        console.print("\n")
        pool_table = Table(title="Server Pool Status")
        pool_table.add_column("Type", style="cyan")
        pool_table.add_column("Port", style="yellow")
        pool_table.add_column("Status", style="bold")
        pool_table.add_column("PID", style="dim")
        
        for port_info in pool_status['tagger_pool']:
            if port_info['running']:
                pool_table.add_row(
                    "Tagger",
                    str(port_info['port']),
                    "[green]RUNNING[/green]",
                    str(port_info.get('pid', '-'))
                )
        
        for port_info in pool_status['wsd_pool']:
            if port_info['running']:
                pool_table.add_row(
                    "WSD",
                    str(port_info['port']),
                    "[green]RUNNING[/green]",
                    str(port_info.get('pid', '-'))
                )
        
        console.print(pool_table)

@server_group.command(name='kill')
@click.option('--force', '-f', is_flag=True, help='Force kill without confirmation')
def server_kill(force):
    """Kill all MetaMap processes"""
    if not force:
        if not Confirm.ask("[yellow]Kill all MetaMap processes?[/yellow]"):
            console.print("Cancelled")
            return
    
    config = PyMMConfig()
    manager = ServerManager(config)
    
    with console.status("Killing processes...") as status:
        killed = manager.force_kill_all()
    
    console.print(f"[green]✓ Killed {killed} process groups[/green]")

@server_group.command(name='force-kill')
def server_force_kill():
    """Force kill all MetaMap processes and clean up"""
    config = PyMMConfig()
    manager = ServerManager(config)
    
    console.print("[yellow]Force killing all MetaMap processes...[/yellow]")
    
    with console.status("Cleaning up...") as status:
        killed = manager.force_kill_all()
    
    console.print(f"[green]✓ Force killed {killed} processes and cleaned up[/green]")

@server_group.command(name='fix-scripts')
def server_fix_scripts():
    """Fix server control scripts with correct paths"""
    config = PyMMConfig()
    manager = ServerManager(config)
    
    console.print("[cyan]Fixing server control scripts...[/cyan]")
    
    # The fix happens automatically in __init__, but we can force it
    manager._fix_server_scripts()
    
    console.print("[green]✓ Server scripts updated with correct paths[/green]")

@server_group.command(name='pool')
@click.option('--tagger', '-t', default=3, help='Number of tagger instances')
@click.option('--wsd', '-w', default=3, help='Number of WSD instances')
@click.option('--stop', is_flag=True, help='Stop server pool')
def server_pool(tagger, wsd, stop):
    """Manage server pool for parallel processing"""
    config = PyMMConfig()
    manager = ServerManager(config)
    
    if stop:
        console.print("[yellow]Stopping server pool...[/yellow]")
        # Stop all servers on pool ports
        for port in range(1795, 1795 + 10):
            manager._kill_process_on_port(port)
        for port in range(5554, 5554 + 10):
            manager._kill_process_on_port(port)
        console.print("[green]✓ Server pool stopped[/green]")
        return
    
    console.print(f"[cyan]Starting server pool: {tagger} tagger(s), {wsd} WSD(s)[/cyan]")
    
    with console.status("Starting server pool...") as status:
        results = manager.start_server_pool(tagger, wsd)
    
    # Show results
    table = Table(title="Server Pool Status")
    table.add_column("Type", style="cyan")
    table.add_column("Port", style="yellow")
    table.add_column("Status", style="bold")
    
    for i, success in enumerate(results['tagger_servers']):
        port = 1795 + i
        status = "[green]STARTED[/green]" if success else "[red]FAILED[/red]"
        table.add_row("Tagger", str(port), status)
    
    for i, success in enumerate(results['wsd_servers']):
        port = 5554 + i
        status = "[green]STARTED[/green]" if success else "[red]FAILED[/red]"
        table.add_row("WSD", str(port), status)
    
    console.print(table)
    
    # Summary
    tagger_ok = sum(results['tagger_servers'])
    wsd_ok = sum(results['wsd_servers'])
    console.print(f"\n[bold]Summary:[/bold] {tagger_ok}/{tagger} taggers, {wsd_ok}/{wsd} WSD servers started")

@server_group.command(name='check-java')
def server_check_java():
    """Check Java installation and configuration"""
    try:
        config = PyMMConfig()
        manager = ServerManager(config)
        
        console.print("[bold]Java Configuration Check[/bold]\n")
        
        # Check detected Java
        console.print(f"Detected Java path: [cyan]{manager.java_path}[/cyan]")
        
        # Check if Java is executable
        try:
            result = subprocess.run(
                [manager.java_path, '-version'],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0 or result.stderr:  # Java outputs version to stderr
                console.print("[green]✓ Java is executable[/green]")
                # Parse version info (Java outputs to stderr)
                version_lines = (result.stderr or result.stdout).strip().split('\n')
                if version_lines:
                    console.print(f"  Version: {version_lines[0]}")
            else:
                console.print("[red]✗ Java is not executable[/red]")
                if result.stdout:
                    console.print(f"  Error: {result.stdout}")
        except Exception as e:
            console.print(f"[red]✗ Failed to run Java: {e}[/red]")
        
        # Check JAVA_HOME
        java_home = os.environ.get('JAVA_HOME')
        if java_home:
            console.print(f"\nJAVA_HOME: [cyan]{java_home}[/cyan]")
            if Path(java_home).exists():
                console.print("[green]✓ JAVA_HOME directory exists[/green]")
            else:
                console.print("[red]✗ JAVA_HOME directory does not exist[/red]")
        else:
            console.print("\n[yellow]JAVA_HOME not set[/yellow]")
        
        # Check common Java locations
        console.print("\n[bold]Common Java locations:[/bold]")
        common_paths = [
            '/usr/bin/java',
            '/usr/local/bin/java',
            '/opt/java/bin/java',
            '/usr/lib/jvm/default/bin/java',
        ]
        
        found_any = False
        for path in common_paths:
            if Path(path).exists():
                console.print(f"  [green]✓[/green] {path}")
                found_any = True
            else:
                console.print(f"  [dim]✗ {path}[/dim]")
        
        if not found_any:
            console.print("\n[red]No Java installation found in common locations![/red]")
            console.print("[yellow]Please install Java 8 or later:[/yellow]")
            console.print("  Ubuntu/Debian: sudo apt-get install openjdk-8-jdk")
            console.print("  RHEL/CentOS: sudo yum install java-1.8.0-openjdk")
            console.print("  macOS: brew install openjdk@8")
            
    except Exception as e:
        console.print(f"[red]Error checking Java: {e}[/red]")
        sys.exit(1)

# Configuration management commands
@click.group()
def config_group():
    """Manage configuration"""
    pass

@config_group.command(name='setup')
@click.option('--reset', is_flag=True, help='Reset existing configuration')
def config_setup(reset):
    """Interactive configuration setup"""
    config = PyMMConfig()
    
    console.print("[bold]PythonMetaMap Configuration Setup[/bold]\n")
    
    if reset:
        config.reset()
        console.print("[yellow]Configuration reset[/yellow]\n")
    
    config.configure_interactive(reset=False)

@config_group.command(name='show')
@click.option('--raw', is_flag=True, help='Show raw JSON')
def config_show(raw):
    """Show current configuration"""
    config = PyMMConfig()
    
    if raw:
        import json
        console.print(json.dumps(config._config, indent=2))
    else:
        table = Table(title="Current Configuration")
        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="green")
        table.add_column("Source", style="dim")
        
        # Key settings to display
        keys = [
            "metamap_binary_path",
            "server_scripts_dir",
            "default_input_dir",
            "default_output_dir",
            "metamap_processing_options",
            "max_parallel_workers",
            "use_instance_pool",
            "metamap_instance_count",
            "pymm_timeout",
            "java_heap_size",
            "retry_max_attempts"
        ]
        
        for key in keys:
            value = config.get(key)
            if value is not None:
                # Determine source
                if key in config._config:
                    source = "config file"
                elif os.getenv(key.upper()):
                    source = "environment"
                else:
                    source = "default"
                
                # Truncate long values
                display_value = str(value)
                if len(display_value) > 50:
                    display_value = display_value[:47] + "..."
                
                table.add_row(key, display_value, source)
        
        console.print(table)
        console.print(f"\nConfig file: [dim]{config.CONFIG_FILE}[/dim]")

@config_group.command(name='get')
@click.argument('key')
def config_get(key):
    """Get a configuration value"""
    config = PyMMConfig()
    value = config.get(key)
    
    if value is not None:
        console.print(f"{key} = {value}")
    else:
        console.print(f"[yellow]No value set for '{key}'[/yellow]")

@config_group.command(name='set')
@click.argument('key')
@click.argument('value')
def config_set(key, value):
    """Set a configuration value"""
    config = PyMMConfig()
    
    # Type conversion for known numeric fields
    if key in ['max_parallel_workers', 'pymm_timeout', 'retry_max_attempts', 
               'metamap_instance_count', 'health_check_interval']:
        try:
            value = int(value)
        except ValueError:
            console.print(f"[red]Error: {key} must be a number[/red]")
            sys.exit(1)
    
    config.set(key, value)
    console.print(f"[green]✓ {key} = {value}[/green]")

@config_group.command(name='unset')
@click.argument('key')
def config_unset(key):
    """Remove a configuration value"""
    config = PyMMConfig()
    config.remove(key)
    console.print(f"[green]✓ Removed '{key}'[/green]")

@config_group.command(name='validate')
def config_validate():
    """Validate configuration"""
    config = PyMMConfig()
    
    errors = []
    warnings = []
    
    # Check MetaMap binary
    mm_path = config.get("metamap_binary_path")
    if not mm_path:
        errors.append("metamap_binary_path not set")
    elif not Path(mm_path).exists():
        errors.append(f"MetaMap binary not found: {mm_path}")
    elif not os.access(mm_path, os.X_OK):
        errors.append(f"MetaMap binary not executable: {mm_path}")
    
    # Check server scripts
    scripts_dir = config.get("server_scripts_dir")
    if scripts_dir:
        scripts_path = Path(scripts_dir)
        if not scripts_path.exists():
            warnings.append(f"Server scripts directory not found: {scripts_dir}")
        else:
            for script in ["skrmedpostctl", "wsdserverctl"]:
                if not (scripts_path / script).exists():
                    warnings.append(f"Server script not found: {script}")
    
    # Check directories
    for key in ["default_input_dir", "default_output_dir"]:
        path = config.get(key)
        if path and not Path(path).exists():
            warnings.append(f"{key} does not exist: {path}")
    
    # Display results
    if errors:
        console.print("[red]Configuration Errors:[/red]")
        for error in errors:
            console.print(f"  ✗ {error}")
    
    if warnings:
        console.print("\n[yellow]Configuration Warnings:[/yellow]")
        for warning in warnings:
            console.print(f"  ⚠ {warning}")
    
    if not errors and not warnings:
        console.print("[green]✓ Configuration is valid[/green]")
    
    sys.exit(1 if errors else 0)

# The setup and install_metamap commands will be defined in main.py where cli is available

# Add concept statistics command group
@click.group()
def stats_group():
    """View processing statistics and insights"""
    pass

@stats_group.command(name='concepts')
@click.argument('output_dir', type=click.Path(exists=True))
@click.option('--top', '-n', default=20, help='Number of top concepts to show')
@click.option('--min-count', '-m', default=2, help='Minimum occurrence count')
def concept_stats(output_dir, top, min_count):
    """Show top extracted concepts from processed files"""
    from collections import Counter
    import csv
    from pathlib import Path
    
    output_path = Path(output_dir)
    concept_counter = Counter()
    semantic_type_counter = Counter()
    file_count = 0
    
    # Read all CSV files
    for csv_file in output_path.glob("*.csv"):
        if csv_file.name.startswith('.'):
            continue
            
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'preferred_name' in row and 'cui' in row:
                        concept = f"{row['preferred_name']} ({row['cui']})"
                        concept_counter[concept] += 1
                        
                    if 'semantic_types' in row:
                        sem_types = row['semantic_types'].strip('[]').split(',')
                        for st in sem_types:
                            st = st.strip().strip("'\"")
                            if st:
                                semantic_type_counter[st] += 1
                                
            file_count += 1
        except Exception as e:
            console.print(f"[yellow]Warning: Error reading {csv_file.name}: {e}[/yellow]")
    
    if not concept_counter:
        console.print("[yellow]No concepts found in output files[/yellow]")
        return
    
    # Display results
    console.print(f"\n[bold]Concept Statistics from {file_count} files[/bold]\n")
    
    # Top concepts table
    concept_table = Table(title=f"Top {top} Concepts (min count: {min_count})")
    concept_table.add_column("Rank", style="cyan")
    concept_table.add_column("Concept (CUI)", style="green")
    concept_table.add_column("Count", style="yellow")
    concept_table.add_column("Frequency", style="dim")
    
    total_concepts = sum(concept_counter.values())
    
    for i, (concept, count) in enumerate(concept_counter.most_common(top), 1):
        if count < min_count:
            break
        freq = f"{(count/total_concepts)*100:.1f}%"
        concept_table.add_row(str(i), concept, str(count), freq)
    
    console.print(concept_table)
    
    # Semantic types table
    console.print(f"\n[bold]Top Semantic Types[/bold]\n")
    
    sem_table = Table(title="Semantic Type Distribution")
    sem_table.add_column("Semantic Type", style="blue")
    sem_table.add_column("Count", style="yellow")
    sem_table.add_column("Percentage", style="dim")
    
    total_sem_types = sum(semantic_type_counter.values())
    
    for sem_type, count in semantic_type_counter.most_common(10):
        pct = f"{(count/total_sem_types)*100:.1f}%"
        sem_table.add_row(sem_type, str(count), pct)
    
    console.print(sem_table)
    
    # Summary statistics
    console.print(f"\n[bold]Summary:[/bold]")
    console.print(f"  • Total unique concepts: {len(concept_counter):,}")
    console.print(f"  • Total concept occurrences: {total_concepts:,}")
    console.print(f"  • Average concepts per file: {total_concepts/file_count:.1f}")
    console.print(f"  • Total semantic types: {len(semantic_type_counter)}")

@click.command()
@click.option('--output-dir', '-o', type=click.Path(exists=True), 
              help='Output directory to monitor')
@click.option('--follow', '-f', is_flag=True, help='Follow log output')
@click.option('--stats', '-s', is_flag=True, help='Show detailed statistics')
def monitor(output_dir, follow, stats):
    """Monitor background processing status"""
    config = PyMMConfig()
    
    if not output_dir:
        output_dir = config.get('default_output_dir', './output_csvs')
    
    output_path = Path(output_dir)
    
    # Check for background PID file
    pid_file = output_path / ".background_pid"
    if not pid_file.exists():
        console.print("[red]No background process found[/red]")
        return
    
    try:
        pid = int(pid_file.read_text().strip())
        
        # Check if process is still running
        import psutil
        try:
            proc = psutil.Process(pid)
            if proc.is_running():
                console.print(f"[green]Background process running[/green] (PID: {pid})")
                
                # Show process info
                info_table = Table(box=box.ROUNDED)
                info_table.add_column("Property", style="cyan")
                info_table.add_column("Value", style="green")
                
                info_table.add_row("CPU Usage", f"{proc.cpu_percent(interval=1)}%")
                info_table.add_row("Memory Usage", f"{proc.memory_info().rss / 1024 / 1024:.1f} MB")
                info_table.add_row("Running Time", str(datetime.now() - datetime.fromtimestamp(proc.create_time())))
                
                console.print(info_table)
            else:
                console.print(f"[yellow]Process {pid} is not running[/yellow]")
                pid_file.unlink()
        except psutil.NoSuchProcess:
            console.print(f"[red]Process {pid} not found[/red]")
            pid_file.unlink()
            
    except Exception as e:
        console.print(f"[red]Error reading PID file: {e}[/red]")
        return
    
    # Show processing statistics
    if stats:
        state_mgr = StateManager(str(output_path))
        stats = state_mgr.get_statistics()
        
        console.print("\n[bold]Processing Statistics:[/bold]")
        stats_table = Table(box=box.ROUNDED)
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", style="green")
        
        stats_table.add_row("Total Files", str(stats['total_files']))
        stats_table.add_row("Completed", str(stats['completed']))
        stats_table.add_row("Failed", str(stats['failed']))
        stats_table.add_row("In Progress", str(stats['in_progress']))
        
        if stats['total_files'] > 0:
            progress = (stats['completed'] / stats['total_files']) * 100
            stats_table.add_row("Progress", f"{progress:.1f}%")
        
        console.print(stats_table)
    
    # Follow log output
    if follow:
        # Find the latest log file
        logs_dir = output_path / "logs"
        if logs_dir.exists():
            log_files = sorted(logs_dir.glob("background_*.log"), key=lambda x: x.stat().st_mtime)
            if log_files:
                latest_log = log_files[-1]
                console.print(f"\n[cyan]Following log: {latest_log}[/cyan]")
                console.print("[dim]Press Ctrl+C to stop[/dim]\n")
                
                try:
                    import subprocess
                    subprocess.run(["tail", "-f", str(latest_log)])
                except KeyboardInterrupt:
                    console.print("\n[yellow]Stopped following log[/yellow]")
            else:
                console.print("[yellow]No log files found[/yellow]")
        else:
            console.print("[yellow]No logs directory found[/yellow]")