"""Main CLI entry point for PythonMetaMap"""
import click
import sys
import os
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel
from rich import print as rprint

from ..core.config import PyMMConfig
from ..server.manager import ServerManager
from ..processing.batch_runner import BatchRunner
from .commands import server_group, config_group, stats_group, monitor, retry, retry_failed, chunked_process_cmd
from .interactive import interactive_ultimate as interactive_mode
try:
    from .analysis import analysis_group
except ImportError:
    analysis_group = None
try:
    from .enhanced_analysis import enhanced_analysis_group
except ImportError:
    enhanced_analysis_group = None

console = Console()

# ASCII Banner
ASCII_BANNER = r"""[bold cyan]
  ____        __  __  __  __ 
 |  _ \ _   _|  \/  ||  \/  |
 | |_) | | | | |\/| || |\/| |
 |  __/| |_| | |  | || |  | |
 |_|    \__, |_|  |_||_|  |_|
        |___/                 [/bold cyan]
[dim]Python MetaMap Orchestrator v9.4.3[/dim]
"""

@click.group(invoke_without_command=True)
@click.version_option(version='9.4.3', prog_name='pymm')
@click.option('--interactive', '-i', is_flag=True, help='Launch interactive mode')
@click.pass_context
def cli(ctx, interactive):
    """PythonMetaMap - NLM MetaMap orchestration tool
    
    Process clinical text through MetaMap with parallel processing,
    automatic retry, and server management.
    """
    if interactive or (ctx.invoked_subcommand is None and not ctx.resilient_parsing):
        # Launch interactive mode
        interactive_mode()
        ctx.exit()
    elif ctx.invoked_subcommand is None:
        console.print(ASCII_BANNER)
        console.print("Use [bold]pymm --help[/bold] to see available commands")
        console.print("Or use [bold]pymm -i[/bold] for interactive mode")

@cli.command()
@click.argument('input_dir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument('output_dir', type=click.Path(file_okay=False, dir_okay=True))
@click.option('--workers', '-w', type=int, help='Number of parallel workers')
@click.option('--timeout', '-t', type=int, help='Processing timeout per file (seconds)')
@click.option('--retry', '-r', type=int, help='Max retry attempts per file')
@click.option('--instance-pool/--no-instance-pool', default=None, 
              help='Use MetaMap instance pooling (auto-detected if not specified)')
@click.option('--start-servers/--no-start-servers', default=True,
              help='Automatically start MetaMap servers')
@click.option('--interactive-monitor', '-m', is_flag=True,
              help='Enable interactive monitoring during processing')
@click.option('--background', '-b', is_flag=True,
              help='Run in background mode (for nohup)')
def process(input_dir, output_dir, workers, timeout, retry, instance_pool, start_servers, interactive_monitor, background):
    """Process files through MetaMap
    
    Examples:
    
        pymm process input_notes/ output_csvs/
        
        pymm process data/notes/ results/ --workers 8 --timeout 600
        
        # Run in background
        nohup pymm process input_notes/ output_csvs/ --background &
    """
    # In background mode, skip banner and use simpler output
    if not background:
        console.print(ASCII_BANNER)
    
    # Load and update config
    config = PyMMConfig()
    
    # Check if MetaMap is configured
    if not config.get("metamap_binary_path"):
        if background:
            print("ERROR: MetaMap binary not configured!")
        else:
            console.print("[red]MetaMap binary not configured![/red]")
            console.print("Run [bold]pymm config setup[/bold] first")
        sys.exit(1)
    
    # Apply command line overrides
    if workers is not None:
        config.set("max_parallel_workers", workers)
    if timeout is not None:
        config.set("pymm_timeout", timeout)
    if retry is not None:
        config.set("retry_max_attempts", retry)
    if instance_pool is not None:
        config.set("use_instance_pool", instance_pool)
    
    # In background mode, disable progress bar
    if background:
        config.set("progress_bar", False)
    
    # Show configuration (skip in background mode)
    if not background:
        table = Table(title="Processing Configuration")
        table.add_column("Setting", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Input Directory", input_dir)
        table.add_row("Output Directory", output_dir)
        table.add_row("Max Workers", str(config.get("max_parallel_workers")))
        table.add_row("Timeout (seconds)", str(config.get("pymm_timeout")))
        table.add_row("Max Retries", str(config.get("retry_max_attempts")))
        table.add_row("Instance Pool", "Yes" if config.get("use_instance_pool") else "No")
        
        console.print(table)
    
    # Start servers if requested
    if start_servers:
        server_mgr = ServerManager(config)
        
        # Check if servers are already running
        tagger_running = server_mgr.is_tagger_server_running()
        wsd_running = server_mgr.is_wsd_server_running()
        
        if tagger_running:
            if background:
                print("Tagger server already running")
            else:
                console.print("[green]✓ Tagger server already running[/green]")
        
        if not tagger_running:
            if background:
                print("Starting MetaMap servers...")
            else:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    console=console
                ) as progress:
                    task = progress.add_task("Starting MetaMap servers...", total=None)
                    
                    if not server_mgr.start_all():
                        if background:
                            print("ERROR: Failed to start MetaMap servers!")
                        else:
                            console.print("[red]Failed to start MetaMap servers![/red]")
                        sys.exit(1)
                    
                    progress.update(task, completed=True, description="Servers started ✓")
            
            # In background mode, start servers without progress bar
            if background and not tagger_running:
                if not server_mgr.start_all():
                    print("ERROR: Failed to start MetaMap servers!")
                    sys.exit(1)
                print("Servers started successfully")
    
    # Run batch processing
    try:
        runner = BatchRunner(input_dir, output_dir, config)
        
        if background:
            print(f"Starting batch processing: {input_dir} -> {output_dir}")
            results = runner.run()
        else:
            with console.status("Processing files...") as status:
                results = runner.run()
        
        # Show results
        if results.get("success"):
            if background:
                print(f"Processing complete! Processed: {results.get('processed', 0)}, Failed: {results.get('failed', 0)}, Time: {results.get('elapsed_time', 0):.1f}s")
            else:
                console.print("\n[green]✓ Processing complete![/green]")
                
                summary = Table(title="Processing Summary")
                summary.add_column("Metric", style="cyan")
                summary.add_column("Value", style="green")
                
                summary.add_row("Total Files", str(results.get("total_files", 0)))
                summary.add_row("Processed", str(results.get("processed", 0)))
                summary.add_row("Failed", str(results.get("failed", 0)))
                summary.add_row("Time Elapsed", f"{results.get('elapsed_time', 0):.1f}s")
                summary.add_row("Throughput", f"{results.get('throughput', 0):.2f} files/s")
                
                console.print(summary)
        else:
            error_msg = f"Processing failed: {results.get('error')}"
            if background:
                print(f"ERROR: {error_msg}")
            else:
                console.print(f"\n[red]✗ {error_msg}[/red]")
            sys.exit(1)
            
    except Exception as e:
        error_msg = f"Error: {e}"
        if background:
            print(f"ERROR: {error_msg}")
        else:
            console.print(f"\n[red]{error_msg}[/red]")
        sys.exit(1)

@cli.command()
@click.argument('output_dir', type=click.Path(exists=True, file_okay=False, dir_okay=True))
def resume(output_dir):
    """Resume interrupted processing
    
    Example:
    
        pymm resume output_csvs/
    """
    console.print(ASCII_BANNER)
    console.print(f"Resuming processing from: [cyan]{output_dir}[/cyan]")
    
    config = PyMMConfig()
    
    try:
        with console.status("Resuming batch processing...") as status:
            results = BatchRunner.resume(output_dir, config)
        
        if results.get("success"):
            console.print("\n[green]✓ Processing complete![/green]")
        else:
            console.print(f"\n[red]✗ Processing failed: {results.get('error')}[/red]")
            
    except Exception as e:
        console.print(f"\n[red]Error: {e}[/red]")
        sys.exit(1)

@cli.command()
@click.option('--failed-only', is_flag=True, help='Show only failed files')
@click.argument('output_dir', type=click.Path(exists=True))
def status(output_dir, failed_only):
    """Show processing status
    
    Example:
    
        pymm status output_csvs/
    """
    from ..core.state import StateManager
    
    try:
        state_mgr = StateManager(output_dir)
        
        # Session info
        session = state_mgr.get_session_info()
        console.print(Panel(
            f"[bold]Session ID:[/bold] {session['session_id']}\n"
            f"[bold]Started:[/bold] {session['started']}\n"
            f"[bold]Last Updated:[/bold] {session['last_updated']}",
            title="Session Information"
        ))
        
        # Statistics
        stats = state_mgr.get_statistics()
        
        table = Table(title="Processing Statistics")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="green")
        
        table.add_row("Total Files", str(stats['total_files']))
        table.add_row("Completed", str(stats['completed']))
        table.add_row("Failed", str(stats['failed']))
        table.add_row("In Progress", str(stats['in_progress']))
        
        console.print(table)
        
        # Failed files
        if state_mgr._state.get('failed_files'):
            failed_table = Table(title="Failed Files")
            failed_table.add_column("File", style="red")
            failed_table.add_column("Error", style="yellow")
            failed_table.add_column("Time", style="dim")
            
            for file, info in state_mgr._state['failed_files'].items():
                failed_table.add_row(
                    Path(file).name,
                    info['error'][:50] + "..." if len(info['error']) > 50 else info['error'],
                    info['timestamp']
                )
            
            console.print(failed_table)
            
    except Exception as e:
        console.print(f"[red]Error reading state: {e}[/red]")

@cli.command()
def interactive():
    """Launch interactive mode with intuitive menu navigation"""
    interactive_mode()

# Add sub-command groups
cli.add_command(server_group, name='server')
cli.add_command(config_group, name='config')
cli.add_command(stats_group, name='stats')
if analysis_group:
    cli.add_command(analysis_group, name='analysis')
if enhanced_analysis_group:
    cli.add_command(enhanced_analysis_group, name='enhanced-analysis')
cli.add_command(monitor)
cli.add_command(retry)
cli.add_command(retry_failed)
cli.add_command(chunked_process_cmd, name='chunked-process')

@cli.command()
def install():
    """Install MetaMap binaries
    
    Downloads and installs MetaMap 2020 from NLM.
    """
    console.print(ASCII_BANNER)
    
    try:
        from ..install_metamap import main as install_main
        
        # Call install_main without spinner to allow prompts
        console.print("[bold cyan]Starting MetaMap Installation Process...[/bold cyan]\n")
        result = install_main()
        
        if result:
            console.print(f"\n[green]✓ MetaMap installed at: {result}[/green]")
            console.print("\nRun [bold]pymm config setup[/bold] to configure")
        else:
            console.print("\n[red]✗ Installation failed[/red]")
            
    except Exception as e:
        console.print(f"\n[red]Installation error: {e}[/red]")
        sys.exit(1)

@cli.command(name='install-metamap')
def install_metamap():
    """Install MetaMap (downloads ~1GB)"""
    console.print(ASCII_BANNER)
    
    try:
        from ..install_metamap import main as install_main
        
        console.print("[bold cyan]Starting MetaMap Installation...[/bold cyan]")
        result = install_main()
        
        if result:
            console.print(f"\n[green]✓ MetaMap installed successfully at: {result}[/green]")
            console.print("\n[bold]Next steps:[/bold]")
            console.print("1. Run [cyan]pymm setup[/cyan] to verify installation")
            console.print("2. Run [cyan]pymm config setup[/cyan] to configure settings")
            console.print("3. Run [cyan]pymm server start[/cyan] to start servers")
        else:
            console.print("\n[red]✗ Installation failed. Check the output above for errors.[/red]")
            sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]Installation error: {e}[/red]")
        sys.exit(1)

@cli.command()
@click.option('--fix', is_flag=True, help='Attempt to fix common issues')
def setup(fix):
    """Verify and fix PythonMetaMap setup"""
    console.print(ASCII_BANNER)
    
    try:
        from ..utils.setup_verifier import verify_setup, fix_setup
        
        if fix:
            console.print("[bold cyan]Running setup verification with fixes...[/bold cyan]\n")
            is_valid = fix_setup()
        else:
            console.print("[bold cyan]Running setup verification...[/bold cyan]\n")
            is_valid = verify_setup()
        
        sys.exit(0 if is_valid else 1)
    except Exception as e:
        console.print(f"\n[red]Setup verification error: {e}[/red]")
        sys.exit(1)

def main():
    """Main entry point"""
    try:
        cli()
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        sys.exit(130)
    except Exception as e:
        console.print(f"\n[red]Unexpected error: {e}[/red]")
        sys.exit(1)

if __name__ == '__main__':
    main()