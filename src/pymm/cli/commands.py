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
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
import time

from ..core.config import PyMMConfig, Config
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
    total_rows = 0
    
    # Read all CSV files
    for csv_file in output_path.glob("*.csv"):
        if csv_file.name.startswith('.'):
            continue
            
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                # Skip the start marker line
                first_line = f.readline()
                if not first_line.startswith("META_BATCH_START"):
                    f.seek(0)  # Reset if no marker
                
                reader = csv.DictReader(f)
                file_has_concepts = False
                
                for row in reader:
                    # Skip empty rows or end marker
                    if not row or 'META_BATCH' in str(row.get('CUI', '')):
                        continue
                    
                    total_rows += 1
                    
                    # Get concept info - check both possible column names
                    cui = row.get('CUI', '').strip()
                    pref_name = row.get('PrefName', row.get('preferred_name', '')).strip()
                    
                    if cui and pref_name:
                        concept = f"{pref_name} ({cui})"
                        concept_counter[concept] += 1
                        file_has_concepts = True
                    
                    # Get semantic types - check both possible column names
                    sem_types = row.get('SemTypes', row.get('semantic_types', '')).strip()
                    if sem_types:
                        # Handle different formats
                        sem_types = sem_types.strip('[]')
                        for st in sem_types.split(','):
                            st = st.strip().strip("'\"")
                            if st:
                                semantic_type_counter[st] += 1
                
                if file_has_concepts:
                    file_count += 1
                    
        except Exception as e:
            console.print(f"[yellow]Warning: Error reading {csv_file.name}: {e}[/yellow]")
    
    if not concept_counter:
        console.print("[yellow]No concepts found in output files[/yellow]")
        console.print(f"[dim]Checked {len(list(output_path.glob('*.csv')))} CSV files[/dim]")
        console.print(f"[dim]Found {total_rows} data rows[/dim]")
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
    
    shown = 0
    for i, (concept, count) in enumerate(concept_counter.most_common(), 1):
        if count < min_count:
            break
        if shown >= top:
            break
        freq = f"{(count/total_concepts)*100:.1f}%"
        concept_table.add_row(str(i), concept, str(count), freq)
        shown += 1
    
    console.print(concept_table)
    
    # Semantic types table
    if semantic_type_counter:
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

@stats_group.command(name='explore')
@click.argument('output_dir', type=click.Path(exists=True))
@click.option('--detailed', '-d', is_flag=True, help='Show detailed file analysis')
@click.option('--concepts', '-c', is_flag=True, help='Include concept analysis')
def explore_output(output_dir, detailed, concepts):
    """Comprehensive exploration of output directory"""
    from pathlib import Path
    import csv
    from collections import Counter
    from datetime import datetime
    import os
    
    output_path = Path(output_dir)
    console.print(f"\n[bold cyan]Output Directory Analysis[/bold cyan]")
    console.print(f"Directory: {output_path.absolute()}\n")
    
    # Collect all CSV files
    csv_files = list(output_path.glob("*.csv"))
    csv_files = [f for f in csv_files if not f.name.startswith('.')]
    
    if not csv_files:
        console.print("[red]No CSV output files found![/red]")
        return
    
    # Analyze files
    complete_files = []
    partial_files = []
    failed_files = []
    empty_files = []
    
    total_size = 0
    total_concepts = 0
    concept_counter = Counter()
    semantic_type_counter = Counter()
    
    for csv_file in csv_files:
        file_size = csv_file.stat().st_size
        total_size += file_size
        
        # Check file status
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                content = f.read()
                
                # Check for markers
                has_start = "META_BATCH_START_NOTE_ID:" in content
                has_end = "META_BATCH_END_NOTE_ID:" in content
                has_error = ":ERROR" in content
                
                # Count concepts
                f.seek(0)
                reader = csv.DictReader(f)
                concept_count = 0
                
                for row in reader:
                    # Skip marker lines
                    if not row or 'META_BATCH' in str(row.get('CUI', '')):
                        continue
                        
                    concept_count += 1
                    
                    # Track concepts for analysis
                    if concepts:
                        cui = row.get('CUI', '').strip()
                        pref_name = row.get('PrefName', '').strip()
                        if cui and pref_name:
                            concept_key = f"{pref_name} ({cui})"
                            concept_counter[concept_key] += 1
                        
                        # Track semantic types
                        sem_types = row.get('SemTypes', '').strip()
                        if sem_types:
                            for st in sem_types.split(','):
                                st = st.strip()
                                if st:
                                    semantic_type_counter[st] += 1
                
                total_concepts += concept_count
                
                # Categorize file
                if file_size < 100:
                    empty_files.append((csv_file, file_size))
                elif has_error or (has_start and not has_end):
                    failed_files.append((csv_file, file_size, concept_count))
                elif has_start and has_end:
                    complete_files.append((csv_file, file_size, concept_count))
                else:
                    partial_files.append((csv_file, file_size, concept_count))
                    
        except Exception as e:
            failed_files.append((csv_file, file_size, 0))
            console.print(f"[yellow]Warning: Error reading {csv_file.name}: {e}[/yellow]")
    
    # Display summary statistics
    summary_table = Table(title="Output File Summary", box=box.ROUNDED)
    summary_table.add_column("Category", style="cyan")
    summary_table.add_column("Count", style="green")
    summary_table.add_column("Percentage", style="yellow")
    
    total_files = len(csv_files)
    summary_table.add_row("Total Files", str(total_files), "100.0%")
    summary_table.add_row("Complete", str(len(complete_files)), f"{len(complete_files)/total_files*100:.1f}%")
    summary_table.add_row("Partial", str(len(partial_files)), f"{len(partial_files)/total_files*100:.1f}%")
    summary_table.add_row("Failed", str(len(failed_files)), f"{len(failed_files)/total_files*100:.1f}%")
    summary_table.add_row("Empty", str(len(empty_files)), f"{len(empty_files)/total_files*100:.1f}%")
    
    console.print(summary_table)
    
    # Storage statistics
    console.print(f"\n[bold]Storage Statistics:[/bold]")
    console.print(f"  Total Size: {total_size / 1024 / 1024:.2f} MB")
    console.print(f"  Average File Size: {total_size / total_files / 1024:.2f} KB")
    console.print(f"  Total Concepts Extracted: {total_concepts:,}")
    if complete_files:
        avg_concepts = sum(f[2] for f in complete_files) / len(complete_files)
        console.print(f"  Average Concepts per Complete File: {avg_concepts:.1f}")
    
    # Show detailed file lists if requested
    if detailed:
        # Failed files
        if failed_files:
            console.print(f"\n[bold red]Failed Files ({len(failed_files)}):[/bold red]")
            failed_table = Table(box=box.SIMPLE)
            failed_table.add_column("File", style="red")
            failed_table.add_column("Size", style="dim")
            failed_table.add_column("Concepts", style="dim")
            
            for f, size, concepts in sorted(failed_files)[:10]:
                failed_table.add_row(f.name, f"{size/1024:.1f} KB", str(concepts))
            
            console.print(failed_table)
            if len(failed_files) > 10:
                console.print(f"[dim]... and {len(failed_files) - 10} more[/dim]")
        
        # Empty files
        if empty_files:
            console.print(f"\n[bold yellow]Empty Files ({len(empty_files)}):[/bold yellow]")
            for f, size in sorted(empty_files)[:5]:
                console.print(f"  • {f.name} ({size} bytes)")
            if len(empty_files) > 5:
                console.print(f"[dim]... and {len(empty_files) - 5} more[/dim]")
    
    # Concept analysis if requested
    if concepts and concept_counter:
        console.print(f"\n[bold]Top Concepts Analysis:[/bold]")
        
        # Top concepts
        concept_table = Table(title="Top 15 Most Frequent Concepts", box=box.ROUNDED)
        concept_table.add_column("Rank", style="cyan")
        concept_table.add_column("Concept (CUI)", style="green")
        concept_table.add_column("Count", style="yellow")
        concept_table.add_column("Files", style="dim")
        
        # Track which files contain each concept
        concept_files = Counter()
        for csv_file in complete_files + partial_files:
            try:
                with open(csv_file[0], 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    file_concepts = set()
                    
                    for row in reader:
                        if not row or 'META_BATCH' in str(row.get('CUI', '')):
                            continue
                        cui = row.get('CUI', '').strip()
                        pref_name = row.get('PrefName', '').strip()
                        if cui and pref_name:
                            concept_key = f"{pref_name} ({cui})"
                            file_concepts.add(concept_key)
                    
                    for concept in file_concepts:
                        concept_files[concept] += 1
                        
            except:
                pass
        
        for i, (concept, count) in enumerate(concept_counter.most_common(15), 1):
            file_count = concept_files.get(concept, 0)
            concept_table.add_row(str(i), concept, str(count), f"{file_count} files")
        
        console.print(concept_table)
        
        # Semantic types
        if semantic_type_counter:
            sem_table = Table(title="Top 10 Semantic Types", box=box.ROUNDED)
            sem_table.add_column("Type", style="blue")
            sem_table.add_column("Count", style="yellow")
            sem_table.add_column("Percentage", style="dim")
            
            total_sem = sum(semantic_type_counter.values())
            for sem_type, count in semantic_type_counter.most_common(10):
                pct = f"{(count/total_sem)*100:.1f}%"
                sem_table.add_row(sem_type, str(count), pct)
            
            console.print(sem_table)
    
    # Check state file
    state_file = output_path / ".pymm_state.json"
    if state_file.exists():
        console.print(f"\n[bold]Processing State:[/bold]")
        try:
            import json
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            stats = state.get('statistics', {})
            state_table = Table(box=box.SIMPLE)
            state_table.add_column("Metric", style="cyan")
            state_table.add_column("Value", style="green")
            
            state_table.add_row("Session Started", state.get('started', 'Unknown'))
            state_table.add_row("Last Updated", state.get('last_updated', 'Unknown'))
            state_table.add_row("Total Files", str(stats.get('total_files', 0)))
            state_table.add_row("Completed", str(stats.get('completed', 0)))
            state_table.add_row("Failed", str(stats.get('failed', 0)))
            
            console.print(state_table)
            
            # Check for discrepancies
            state_completed = stats.get('completed', 0)
            actual_completed = len(complete_files)
            if state_completed != actual_completed:
                console.print(f"\n[yellow]⚠ Discrepancy detected:[/yellow]")
                console.print(f"  State shows {state_completed} completed files")
                console.print(f"  But found {actual_completed} complete CSV files")
                
        except Exception as e:
            console.print(f"[yellow]Could not read state file: {e}[/yellow]")
    
    # Recommendations
    if failed_files or empty_files:
        console.print(f"\n[bold]Recommendations:[/bold]")
        if failed_files:
            console.print(f"  • Retry {len(failed_files)} failed files: [cyan]pymm retry {output_dir}[/cyan]")
        if empty_files:
            console.print(f"  • Check {len(empty_files)} empty input files")

@click.command()
@click.option('--output-dir', '-o', type=click.Path(exists=True), 
              help='Output directory to monitor (deprecated - use job monitor instead)')
@click.option('--follow', '-f', is_flag=True, help='Follow log output')
@click.option('--stats', '-s', is_flag=True, help='Show detailed statistics')
@click.option('--live', '-l', is_flag=True, help='Launch live job monitor')
def monitor(output_dir, follow, stats, live):
    """Monitor processing jobs"""
    # Launch unified monitor if requested or by default
    if live or (not follow and not stats and not output_dir):
        # Use unified monitor with file tracking
        from ..monitoring.unified_monitor import UnifiedMonitor
        from ..core.config import PyMMConfig
        
        config = PyMMConfig()
        monitor = UnifiedMonitor(config=config)
        
        console.print("[cyan]Starting Unified Monitor...[/cyan]")
        console.print("[dim]Press Ctrl+C to exit[/dim]\n")
        
        try:
            monitor.start()
            # Keep the main thread alive
            while monitor._running:
                time.sleep(1)
        except KeyboardInterrupt:
            monitor.stop()
            console.print("\n[yellow]Monitor stopped[/yellow]")
        return
    
    # Legacy single-job monitoring
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

@click.command()
@click.argument('output_dir', type=click.Path(exists=True))
@click.option('--max-attempts', default=3, help='Maximum retry attempts')
@click.option('--workers', default=4, help='Number of parallel workers')
@click.pass_context
def retry(ctx, output_dir, max_attempts, workers):
    """Retry failed files from a previous run"""
    from ..processing.batch_runner import BatchRunner
    from ..core.state import StateManager
    from ..core.config import PyMMConfig
    
    # Load configuration
    config = ctx.obj['config']
    config['retry_max_attempts'] = max_attempts
    config['max_parallel_workers'] = workers
    
    # Load state to find failed files
    state_manager = StateManager(output_dir)
    failed_files = list(state_manager._state.get('failed_files', {}).keys())
    
    if not failed_files:
        click.echo("No failed files to retry")
        return
    
    click.echo(f"Found {len(failed_files)} failed files to retry")
    
    # Get input directory from first failed file
    if failed_files:
        first_file = Path(failed_files[0])
        if first_file.exists():
            input_dir = first_file.parent
        else:
            click.echo("Cannot determine input directory from failed files", err=True)
            return
    
    # Create runner and retry
    runner = BatchRunner(str(input_dir), output_dir, config)
    
    # Process only failed files
    from ..processing.retry import RetryManager
    retry_manager = RetryManager(config, state_manager)
    
    def process_func(file_path):
        file = Path(file_path)
        if runner.use_instance_pool:
            return runner._process_file_with_pool(file)
        else:
            return runner._process_file_direct(file)
    
    results = retry_manager.retry_failed_files(failed_files, process_func)
    
    # Show results
    click.echo(f"\nRetry Results:")
    click.echo(f"  Attempted: {results['attempted']}")
    click.echo(f"  Recovered: {results['recovered']}")
    click.echo(f"  Still Failed: {len(results['still_failed'])}")
    click.echo(f"  Skipped: {len(results['skipped'])}")
    
    if results['still_failed']:
        click.echo(f"\nFiles still failing after retry:")
        for file in results['still_failed'][:10]:
            click.echo(f"  - {Path(file).name}")
        if len(results['still_failed']) > 10:
            click.echo(f"  ... and {len(results['still_failed']) - 10} more")

@click.command(name='retry-failed')
@click.argument('output_dir', type=click.Path(exists=True))
@click.option('--config', '-c', type=click.Path(exists=True), help='Config file path')
@click.option('--max-attempts', default=3, help='Maximum retry attempts per file')
@click.option('--delay', default=5, help='Delay between retries in seconds')
@click.option('--filter-error', help='Only retry files with specific error type')
@click.option('--dry-run', is_flag=True, help='Show what would be retried without actually processing')
def retry_failed(output_dir, config, max_attempts, delay, filter_error, dry_run):
    """Retry failed files from a previous processing session
    
    This command reads the state file from a previous session and retries
    all failed files that haven't exceeded the maximum retry attempts.
    
    Examples:
    
        # Retry all failed files
        pymm retry-failed output_csvs/
        
        # Only retry timeout errors
        pymm retry-failed output_csvs/ --filter-error timeout
        
        # Dry run to see what would be retried
        pymm retry-failed output_csvs/ --dry-run
    """
    from pathlib import Path
    import json
    import time
    from ..processing.batch_runner import BatchRunner
    from ..core.config import Config
    
    output_path = Path(output_dir)
    state_file = output_path / ".pymm_state.json"
    
    if not state_file.exists():
        console.print("[red]No state file found. Cannot retry without previous session data.[/red]")
        return
    
    # Load state
    with open(state_file, 'r') as f:
        state = json.load(f)
    
    # Get failed files
    failed_files = state.get('failed_files', {})
    if not failed_files:
        console.print("[green]No failed files to retry![/green]")
        return
    
    # Filter candidates
    retry_candidates = []
    for file_path, error_info in failed_files.items():
        attempts = error_info.get('attempts', 1)
        error_msg = error_info.get('error', '')
        
        # Check if we should retry this file
        if attempts >= max_attempts:
            continue
            
        # Apply error filter if specified
        if filter_error and filter_error.lower() not in error_msg.lower():
            continue
            
        retry_candidates.append({
            'path': file_path,
            'name': Path(file_path).name,
            'attempts': attempts,
            'error': error_msg
        })
    
    if not retry_candidates:
        console.print("[yellow]No files match retry criteria.[/yellow]")
        return
    
    # Show what we'll retry
    console.print(f"\n[bold]Found {len(retry_candidates)} files to retry[/bold]")
    
    retry_table = Table(box=box.ROUNDED)
    retry_table.add_column("File", style="cyan")
    retry_table.add_column("Previous Attempts", style="yellow")
    retry_table.add_column("Error Type", style="red")
    
    for candidate in retry_candidates[:10]:
        error_type = 'Timeout' if 'timeout' in candidate['error'].lower() else \
                    'Memory' if 'memory' in candidate['error'].lower() else \
                    'Java' if 'java' in candidate['error'].lower() else \
                    'Other'
        retry_table.add_row(
            candidate['name'],
            str(candidate['attempts']),
            error_type
        )
    
    console.print(retry_table)
    
    if len(retry_candidates) > 10:
        console.print(f"\n[dim]... and {len(retry_candidates) - 10} more files[/dim]")
    
    if dry_run:
        console.print("\n[yellow]Dry run mode - no files will be processed[/yellow]")
        return
    
    # Confirm retry
    if not click.confirm(f"\nRetry {len(retry_candidates)} failed files?"):
        return
    
    # Load config
    if config:
        cfg = Config.from_file(config)
    else:
        cfg = Config()
        # Try to use config from original session
        if 'config' in state:
            for key, value in state['config'].items():
                setattr(cfg, key, value)
    
    # Get input directory from state
    input_dir = state.get('input_dir')
    if not input_dir or not Path(input_dir).exists():
        console.print("[red]Original input directory not found in state or doesn't exist[/red]")
        return
    
    # Create list of files to retry
    retry_files = [candidate['path'] for candidate in retry_candidates]
    
    # Initialize batch runner with retry files
    runner = BatchRunner(
        input_dir=input_dir,
        output_dir=output_dir,
        config=cfg,
        retry_files=retry_files  # Only process these specific files
    )
    
    console.print(f"\n[cyan]Starting retry with {delay}s delay between files...[/cyan]")
    
    # Run with retry logic
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        task = progress.add_task("Retrying failed files...", total=len(retry_files))
        
        for i, file_path in enumerate(retry_files):
            try:
                # Add delay between retries
                if i > 0:
                    time.sleep(delay)
                
                # Update progress
                progress.update(task, description=f"Retrying {Path(file_path).name}...")
                
                # Process file
                runner.process_single_file(file_path)
                
                # Update state to mark as completed
                if file_path in failed_files:
                    del failed_files[file_path]
                    
                progress.update(task, advance=1)
                
            except Exception as e:
                # Update attempt count
                if file_path not in failed_files:
                    failed_files[file_path] = {}
                    
                failed_files[file_path]['attempts'] = failed_files[file_path].get('attempts', 1) + 1
                failed_files[file_path]['error'] = str(e)
                failed_files[file_path]['timestamp'] = datetime.now().isoformat()
                
                progress.update(task, advance=1)
    
    # Save updated state
    state['failed_files'] = failed_files
    state['last_updated'] = datetime.now().isoformat()
    
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)
    
    # Show final results
    successful = len(retry_candidates) - len(failed_files)
    console.print(f"\n[bold]Retry Results:[/bold]")
    console.print(f"  [green]✓ Successfully processed: {successful}[/green]")
    console.print(f"  [red]✗ Still failing: {len(failed_files)}[/red]")
    
    if failed_files:
        console.print("\n[yellow]Some files still failing. Run again to retry or check logs for details.[/yellow]")

@click.command(name="process")
@click.argument("input_dir", type=click.Path(exists=True), required=False)
@click.argument("output_dir", type=click.Path(), required=False)
@click.option("-w", "--workers", type=int, help="Number of parallel workers")
@click.option("-t", "--timeout", type=int, help="Processing timeout per file (seconds)")
@click.option("-r", "--retry", type=int, help="Max retry attempts per file")
@click.option("--instance-pool/--no-instance-pool", default=None, help="Use MetaMap instance pooling (auto-detected if not specified)")
@click.option("--start-servers/--no-start-servers", default=True, help="Automatically start MetaMap servers")
@click.option("-m", "--interactive-monitor", is_flag=True, help="Enable interactive monitoring during processing")
@click.option("-b", "--background", is_flag=True, help="Run in background mode (for nohup)")
@click.option("--job-id", type=str, help="Job ID for tracking (used internally)")
@click.pass_context
def process_cmd(ctx, input_dir, output_dir, workers, timeout, retry, instance_pool, start_servers, interactive_monitor, background, job_id):
    """Process files through MetaMap
    
    Examples:
    
        pymm process input_notes/ output_csvs/
        
        pymm process data/notes/ results/ --workers 8 --timeout 600
        
        # Run in background
        nohup pymm process input_notes/ output_csvs/ --background &
    """
    config = ctx.obj
    
    # Use defaults if not provided
    if not input_dir:
        input_dir = config.get("default_input_dir", "./input_notes")
    if not output_dir:
        output_dir = config.get("default_output_dir", "./output_csvs")
    
    # Validate directories
    input_path = Path(input_dir)
    if not input_path.exists():
        console.print(f"[red]Error: Input directory '{input_dir}' does not exist[/red]")
        ctx.exit(1)
    
    # Update configuration
    if workers:
        config.set("max_parallel_workers", workers)
    if timeout:
        config.set("pymm_timeout", timeout)
    if retry is not None: # Handle None explicitly
        config.set("retry_max_attempts", retry)
    if instance_pool is not None: # Handle None explicitly
        config.set("use_instance_pool", instance_pool)
    if not start_servers: # Handle False explicitly
        config.set("start_servers", False)
    
    # Configure background mode
    if background:
        config.set("progress_bar", False)
    
    # Set job ID if provided
    if job_id:
        config.set("job_id", job_id)
    
    # Import and use chunked batch runner
    from ..processing.batch_runner import BatchRunner
    
    try:
        # Create and run chunked batch processor
        runner = BatchRunner(input_dir, output_dir, config)
        
        if not start_servers:
            console.print("[yellow]Skipping server start as per --no-start-servers flag.[/yellow]")
        elif runner.start_servers():
            console.print("[green]✓ Servers started successfully.[/green]")
        else:
            console.print("[red]✗ Failed to start servers. Please check logs.[/red]")
            ctx.exit(1)

        if interactive_monitor:
            console.print("\n[cyan]Interactive monitoring enabled.[/cyan]")
            # Run in foreground with interactive monitoring
            runner.run_with_interactive_monitor()
        else:
            # Run in foreground
            console.print("\n[cyan]Starting batch processing...[/cyan]")
            
            # Show configuration
            table = Table(title="Processing Configuration")
            table.add_column("Setting", style="cyan")
            table.add_column("Value", style="green")
            
            table.add_row("Input Directory", str(input_dir))
            table.add_row("Output Directory", str(output_dir))
            table.add_row("Max Workers", str(config.get("max_parallel_workers", 4)))
            table.add_row("Timeout (seconds)", str(config.get("pymm_timeout", 300)))
            table.add_row("Max Retry Attempts", str(config.get("retry_max_attempts", 3)))
            table.add_row("Instance Pool", "Enabled" if config.get("use_instance_pool", True) else "Disabled")
            
            console.print(table)
            
            # Run processing
            results = runner.run()
            
            # Show results
            if results.get("success"):
                console.print(f"\n[green]✓ Processing complete![/green]")
                
                result_table = Table(title="Processing Summary")
                result_table.add_column("Metric", style="cyan")
                result_table.add_column("Value", style="green")
                
                result_table.add_row("Total Files", str(results.get("total_files", 0)))
                result_table.add_row("Processed", str(results.get("processed", 0)))
                result_table.add_row("Failed", str(results.get("failed", 0)))
                result_table.add_row("Chunks Processed", str(results.get("chunks_processed", 0)))
                result_table.add_row("Time Elapsed", f"{results.get('elapsed_time', 0):.2f}s")
                result_table.add_row("Throughput", f"{results.get('throughput', 0):.2f} files/s")
                
                console.print(result_table)
            else:
                console.print(f"\n[red]✗ Processing failed: {results.get('error', 'Unknown error')}[/red]")
                ctx.exit(1)
                
    except KeyboardInterrupt:
        console.print("\n[yellow]Processing interrupted by user[/yellow]")
        ctx.exit(130)
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        if config.get("debug"):
            import traceback
            traceback.print_exc()
        ctx.exit(1)

@click.command(name="chunked-process")
@click.argument("input_dir", type=click.Path(exists=True), required=False)
@click.argument("output_dir", type=click.Path(), required=False)
@click.option("-w", "--workers", type=int, help="Number of parallel workers")
@click.option("-t", "--timeout", type=int, help="Processing timeout per file (seconds)")
@click.option("-c", "--chunk-size", type=int, default=500, help="Files per chunk (default: 500)")
@click.option("--clear-state", is_flag=True, help="Clear previous processing state")
@click.option("-b", "--background", is_flag=True, help="Run in background mode")
@click.pass_context
def chunked_process_cmd(ctx, input_dir, output_dir, workers, timeout, chunk_size, clear_state, background):
    """Process large file sets efficiently with chunking
    
    Optimized for processing thousands of files without memory issues.
    
    Examples:
    
        pymm chunked-process input_notes/ output_csvs/
        
        pymm chunked-process data/ results/ --workers 6 --chunk-size 1000
        
        # Clear state and start fresh
        pymm chunked-process input_notes/ output_csvs/ --clear-state
    """
    config = ctx.obj
    
    # Use defaults if not provided
    if not input_dir:
        input_dir = config.get("default_input_dir", "./input_notes")
    if not output_dir:
        output_dir = config.get("default_output_dir", "./output_csvs")
    
    # Validate directories
    input_path = Path(input_dir)
    if not input_path.exists():
        console.print(f"[red]Error: Input directory '{input_dir}' does not exist[/red]")
        ctx.exit(1)
    
    # Update configuration
    if workers:
        config.set("max_parallel_workers", workers)
    if timeout:
        config.set("pymm_timeout", timeout)
    config.set("chunk_size", chunk_size)
    
    # Configure background mode
    if background:
        config.set("progress_bar", False)
    
    # Import and use chunked batch runner
    from ..processing.chunked_batch_runner import ChunkedBatchRunner
    
    try:
        # Create and run chunked batch processor
        runner = ChunkedBatchRunner(input_dir, output_dir, config)
        
        if clear_state:
            console.print("[yellow]Clearing previous processing state...[/yellow]")
            runner.clear_state()
        
        if background:
            console.print("\n[yellow]Starting chunked background processing...[/yellow]")
            # For background mode, we need to handle differently
            import subprocess
            import sys
            import os
            
            log_file = Path(output_dir) / "logs" / f"chunked_background_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            log_file.parent.mkdir(parents=True, exist_ok=True)
            
            cmd = [
                sys.executable, "-m", "pymm", "chunked-process",
                str(input_dir), str(output_dir),
                "--workers", str(config.get("max_parallel_workers", 4)),
                "--timeout", str(config.get("pymm_timeout", 300)),
                "--chunk-size", str(chunk_size)
            ]
            
            with open(log_file, 'w') as log:
                process = subprocess.Popen(
                    cmd,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    preexec_fn=os.setsid if os.name != 'nt' else None
                )
            
            pid_file = Path(output_dir) / ".background_pid"
            pid_file.write_text(str(process.pid))
            
            console.print(f"[green]✓ Background processing started![/green]")
            console.print(f"  PID: {process.pid}")
            console.print(f"  Log: {log_file}")
            console.print(f"\nMonitor progress with:")
            console.print(f"  tail -f {log_file}")
            console.print(f"\nCheck status with:")
            console.print(f"  pymm status {output_dir}")
        else:
            # Run in foreground
            console.print("\n[cyan]Starting chunked batch processing...[/cyan]")
            
            # Show configuration
            table = Table(title="Processing Configuration")
            table.add_column("Setting", style="cyan")
            table.add_column("Value", style="green")
            
            table.add_row("Input Directory", str(input_dir))
            table.add_row("Output Directory", str(output_dir))
            table.add_row("Max Workers", str(config.get("max_parallel_workers", 4)))
            table.add_row("Timeout (seconds)", str(config.get("pymm_timeout", 300)))
            table.add_row("Chunk Size", str(chunk_size))
            
            console.print(table)
            
            # Run processing
            results = runner.run()
            
            # Show results
            if results.get("success"):
                console.print(f"\n[green]✓ Processing complete![/green]")
                
                result_table = Table(title="Processing Summary")
                result_table.add_column("Metric", style="cyan")
                result_table.add_column("Value", style="green")
                
                result_table.add_row("Total Files", str(results.get("total_files", 0)))
                result_table.add_row("Processed", str(results.get("processed", 0)))
                result_table.add_row("Failed", str(results.get("failed", 0)))
                result_table.add_row("Chunks Processed", str(results.get("chunks_processed", 0)))
                result_table.add_row("Time Elapsed", f"{results.get('elapsed_time', 0):.2f}s")
                result_table.add_row("Throughput", f"{results.get('throughput', 0):.2f} files/s")
                
                console.print(result_table)
            else:
                console.print(f"\n[red]✗ Processing failed: {results.get('error', 'Unknown error')}[/red]")
                ctx.exit(1)
                
    except KeyboardInterrupt:
        console.print("\n[yellow]Processing interrupted by user[/yellow]")
        ctx.exit(130)
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        if config.get("debug"):
            import traceback
            traceback.print_exc()
        ctx.exit(1)