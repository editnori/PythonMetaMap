"""Unified monitoring interface integrating all monitoring components"""
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable
import asyncio
from concurrent.futures import ThreadPoolExecutor

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich.table import Table
from rich import box
from rich.align import Align
from rich.prompt import Prompt

from .realtime_progress import RealtimeProgressTracker
from .live_logger import LiveLogger, LoggerIntegration
from .resource_monitor import ResourceMonitor
from .output_explorer import OutputExplorer
from .statistics_dashboard import StatisticsDashboard

console = Console()


class UnifiedMonitor:
    """Unified monitoring system bringing together all monitoring components"""
    
    def __init__(self, output_dirs: List[Path] = None, config = None):
        # Initialize all components
        self.config = config
        self.progress_tracker = RealtimeProgressTracker(update_callback=self._on_progress_update)
        self.live_logger = LiveLogger(display_lines=15)
        self.resource_monitor = ResourceMonitor()
        
        # Use config for default output directory if not specified
        if not output_dirs and config:
            default_output = config.get('default_output_dir', './output_csvs')
            output_dirs = [Path(default_output)]
        elif not output_dirs:
            output_dirs = [Path("./output_csvs")]
            
        self.output_explorer = OutputExplorer(output_dirs)
        self.statistics_dashboard = StatisticsDashboard()
        
        # Current view
        self.current_view = "dashboard"  # dashboard, progress, logs, resources, files, stats
        self.views = ["dashboard", "progress", "logs", "resources", "files", "stats"]
        
        # Control
        self._running = False
        self._live = None
        self.refresh_rate = 0.5
        
        # Keyboard shortcuts
        self.shortcuts = {
            'd': 'dashboard',
            'p': 'progress',
            'l': 'logs',
            'r': 'resources',
            'f': 'files',
            's': 'stats',
            'q': 'quit',
            'h': 'help'
        }
        
        # Integration callbacks
        self.batch_callback: Optional[Callable] = None
        self.file_callback: Optional[Callable] = None
        
    def start(self):
        """Start the unified monitor"""
        if not self._running:
            self._running = True
            
            # Start all monitoring components
            self.resource_monitor.start()
            self.output_explorer.start_watching()
            
            # Start display thread
            self._display_thread = threading.Thread(target=self._run_display, daemon=True)
            self._display_thread.start()
    
    def stop(self):
        """Stop the unified monitor"""
        self._running = False
        
        # Stop all components
        self.progress_tracker.stop()
        self.resource_monitor.stop()
        self.output_explorer.stop_watching()
        self.statistics_dashboard.stop()
        
        # Wait for display thread
        if hasattr(self, '_display_thread') and self._display_thread.is_alive():
            self._display_thread.join(timeout=5)
    
    def _run_display(self):
        """Run the live display"""
        with Live(
            self._create_layout(),
            console=console,
            refresh_per_second=1/self.refresh_rate,
            screen=True
        ) as live:
            self._live = live
            
            while self._running:
                try:
                    # Update display
                    live.update(self._create_layout())
                    
                    # Check for keyboard input (would need async for real implementation)
                    time.sleep(self.refresh_rate)
                    
                except KeyboardInterrupt:
                    self._running = False
                except Exception as e:
                    console.print(f"[error]Display error: {e}[/error]")
    
    def _create_layout(self) -> Layout:
        """Create the main layout based on current view"""
        layout = Layout()
        
        # Header
        header = self._create_header()
        
        # Main content based on view
        if self.current_view == "dashboard":
            content = self._create_dashboard_view()
        elif self.current_view == "progress":
            content = self._create_progress_view()
        elif self.current_view == "logs":
            content = self._create_logs_view()
        elif self.current_view == "resources":
            content = self._create_resources_view()
        elif self.current_view == "files":
            content = self._create_files_view()
        elif self.current_view == "stats":
            content = self._create_stats_view()
        else:
            content = Panel("Unknown view", style="red")
        
        # Footer
        footer = self._create_footer()
        
        # Arrange layout
        layout.split_column(
            Layout(header, size=3),
            Layout(content, name="main"),
            Layout(footer, size=3)
        )
        
        return layout
    
    def _create_header(self) -> Panel:
        """Create header with navigation tabs"""
        # Create tabs
        tabs = []
        for view in self.views:
            if view == self.current_view:
                tabs.append(f"[bold cyan][ {view.upper()} ][/bold cyan]")
            else:
                shortcut = [k for k, v in self.shortcuts.items() if v == view][0]
                tabs.append(f"[dim]  {view}({shortcut})  [/dim]")
        
        tabs_text = "  ".join(tabs)
        
        # Add resource summary
        resource_summary = self.resource_monitor.get_summary()
        cpu = resource_summary['cpu_percent']
        mem = resource_summary['memory_percent']
        
        header_content = f"{tabs_text}\n[dim]CPU: {cpu:.1f}% | MEM: {mem:.1f}% | [/dim][green]Files: {self.statistics_dashboard.global_stats.total_files_processed}[/green]"
        
        return Panel(header_content, box=box.DOUBLE, style="bold cyan")
    
    def _create_footer(self) -> Panel:
        """Create footer with shortcuts and status"""
        shortcuts_text = " | ".join([
            f"[bold]{k}[/bold]:{v[:4]}" for k, v in sorted(self.shortcuts.items())
        ])
        
        # Add status info
        status_parts = []
        
        # Active batches
        active_batches = len(self.progress_tracker.batches)
        if active_batches > 0:
            status_parts.append(f"[green]Active Batches: {active_batches}[/green]")
        
        # Recent errors
        error_count = len(self.live_logger.entries) if hasattr(self.live_logger, 'entries') else 0
        if error_count > 0:
            recent_errors = sum(1 for e in list(self.live_logger.entries)[-100:] 
                              if e.level in ['ERROR', 'CRITICAL'])
            if recent_errors > 0:
                status_parts.append(f"[red]Recent Errors: {recent_errors}[/red]")
        
        footer_content = shortcuts_text
        if status_parts:
            footer_content += "\n" + " | ".join(status_parts)
        
        return Panel(footer_content, box=box.SIMPLE, style="dim")
    
    def _create_dashboard_view(self) -> Layout:
        """Create dashboard view combining multiple components"""
        layout = Layout()
        
        # Split into quadrants
        layout.split_column(
            Layout(name="top", size=15),
            Layout(name="bottom", size=15)
        )
        
        layout["top"].split_row(
            Layout(self._create_mini_progress(), name="progress"),
            Layout(self._create_mini_stats(), name="stats")
        )
        
        layout["bottom"].split_row(
            Layout(self._create_mini_logs(), name="logs"),
            Layout(self._create_mini_resources(), name="resources")
        )
        
        return layout
    
    def _create_progress_view(self) -> Layout:
        """Create detailed progress view"""
        return self.progress_tracker.get_progress_display()
    
    def _create_logs_view(self) -> Layout:
        """Create logs view with controls"""
        layout = Layout()
        
        # Main log display
        logs_panel = self.live_logger.get_display(height=25)
        
        # Controls panel
        controls = self._create_log_controls()
        
        layout.split_column(
            Layout(logs_panel, size=25),
            Layout(controls, size=5)
        )
        
        return layout
    
    def _create_resources_view(self) -> Layout:
        """Create resources view"""
        return self.resource_monitor.get_display()
    
    def _create_files_view(self) -> Layout:
        """Create files view"""
        return self.output_explorer.get_display()
    
    def _create_stats_view(self) -> Layout:
        """Create statistics view"""
        return self.statistics_dashboard.get_display()
    
    def _create_mini_progress(self) -> Panel:
        """Create mini progress panel for dashboard"""
        if not self.progress_tracker.current_batch:
            return Panel("No active processing", title="Progress", box=box.ROUNDED)
        
        batch = self.progress_tracker.batches.get(self.progress_tracker.current_batch)
        if not batch:
            return Panel("No batch data", title="Progress", box=box.ROUNDED)
        
        content = Text()
        
        # Overall progress
        progress = (batch.completed_files + batch.failed_files) / batch.total_files * 100
        content.append(f"Progress: {progress:.1f}%\n", style="bold")
        content.append(f"Files: {batch.completed_files}/{batch.total_files}\n")
        
        # Active files
        if batch.active_files:
            content.append("\nActive:\n", style="bold cyan")
            for filename in batch.active_files[:3]:
                content.append(f"  • {Path(filename).name}\n", style="yellow")
        
        # Throughput
        content.append(f"\nThroughput: {self.progress_tracker.files_per_second:.1f} files/s", style="green")
        
        return Panel(content, title="Progress Overview", box=box.ROUNDED, style="cyan")
    
    def _create_mini_stats(self) -> Panel:
        """Create mini stats panel for dashboard"""
        stats = self.statistics_dashboard.global_stats
        
        content = Text()
        content.append(f"Total Processed: {stats.total_files_processed:,}\n", style="bold green")
        content.append(f"Total Failed: {stats.total_files_failed:,}\n", style="red")
        content.append(f"Success Rate: {(stats.total_files_processed/(stats.total_files_processed+stats.total_files_failed)*100 if stats.total_files_processed+stats.total_files_failed > 0 else 0):.1f}%\n")
        content.append(f"\nConcepts: {stats.total_concepts_extracted:,}\n", style="magenta")
        content.append(f"Unique: {stats.total_unique_concepts:,}\n")
        content.append(f"\nAvg Time: {stats.avg_file_processing_time:.2f}s\n", style="yellow")
        content.append(f"Files/Min: {stats.files_per_minute:.1f}", style="cyan")
        
        return Panel(content, title="Statistics", box=box.ROUNDED, style="green")
    
    def _create_mini_logs(self) -> Panel:
        """Create mini logs panel for dashboard"""
        return self.live_logger.get_display(height=10)
    
    def _create_mini_resources(self) -> Panel:
        """Create mini resources panel for dashboard"""
        summary = self.resource_monitor.get_summary()
        
        content = Text()
        
        # CPU
        content.append("CPU: ", style="bold")
        cpu_bar = self._create_mini_bar(summary['cpu_percent'], 100, 20)
        content.append(cpu_bar)
        content.append(f" {summary['cpu_percent']:.1f}%\n")
        
        # Memory
        content.append("MEM: ", style="bold")
        mem_bar = self._create_mini_bar(summary['memory_percent'], 100, 20)
        content.append(mem_bar)
        content.append(f" {summary['memory_percent']:.1f}%\n")
        
        # Disk I/O
        content.append(f"\nDisk R/W: {summary['disk_read_mb_s']:.1f}/{summary['disk_write_mb_s']:.1f} MB/s\n")
        
        # Network
        content.append(f"Net R/S: {summary['network_recv_mb_s']:.1f}/{summary['network_sent_mb_s']:.1f} MB/s\n")
        
        # Top process
        content.append(f"\nTop Process: {summary['top_process']}", style="dim")
        
        return Panel(content, title="Resources", box=box.ROUNDED, style="blue")
    
    def _create_log_controls(self) -> Panel:
        """Create log control panel"""
        controls = Text()
        
        # Active filters
        controls.append("Filters: ", style="bold")
        
        # Level filters
        for level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']:
            if level in self.live_logger.filters['levels']:
                controls.append(f"[{level[0]}]", style={
                    'DEBUG': 'dim',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'bold red'
                }.get(level, 'white'))
            else:
                controls.append(f" {level[0]} ", style="dim strikethrough")
        
        # Search
        if self.live_logger.filters['search']:
            controls.append(f"\nSearch: '{self.live_logger.filters['search']}'", style="yellow")
        
        # Commands
        controls.append("\n\nCommands: ", style="bold")
        controls.append("[bold]f[/bold]:filter [bold]s[/bold]:search [bold]c[/bold]:clear [bold]e[/bold]:export")
        
        return Panel(controls, box=box.SINGLE, style="dim")
    
    def _create_mini_bar(self, value: float, max_value: float, width: int) -> Text:
        """Create a mini progress bar"""
        percent = (value / max_value) * 100 if max_value > 0 else 0
        filled = int((percent / 100) * width)
        
        # Color based on value
        if percent < 50:
            color = "green"
        elif percent < 80:
            color = "yellow"
        else:
            color = "red"
        
        bar = Text()
        bar.append("█" * filled, style=color)
        bar.append("░" * (width - filled), style="dim")
        
        return bar
    
    def _on_progress_update(self, event_type: str, data: Any):
        """Handle progress updates"""
        # Update statistics dashboard
        if event_type == "file_completed":
            self.statistics_dashboard.update_file_processed(
                data['batch'],
                data['file'],
                data['time'],
                data['concepts']
            )
        elif event_type == "file_failed":
            self.statistics_dashboard.update_file_failed(
                data['batch'],
                data['file'],
                data['error']
            )
    
    # Integration methods for batch processors
    def create_batch(self, batch_id: str, total_files: int):
        """Create a new batch for tracking"""
        self.progress_tracker.create_batch(batch_id, total_files)
        self.statistics_dashboard.start_batch(batch_id, total_files)
        self.live_logger.add_entry("INFO", "BatchManager", f"Started batch {batch_id} with {total_files} files")
    
    def update_file_progress(self, batch_id: str, filename: str, stage: str, progress: float):
        """Update file processing progress"""
        self.progress_tracker.update_file_stage(filename, stage, progress, batch_id)
    
    def complete_file(self, batch_id: str, filename: str, concepts_count: int):
        """Mark file as completed"""
        self.progress_tracker.complete_file(filename, concepts_count, batch_id)
        self.live_logger.add_entry("INFO", "FileProcessor", f"Completed {Path(filename).name} with {concepts_count} concepts")
    
    def fail_file(self, batch_id: str, filename: str, error: str):
        """Mark file as failed"""
        self.progress_tracker.fail_file(filename, error, batch_id)
        self.live_logger.add_entry("ERROR", "FileProcessor", f"Failed {Path(filename).name}: {error}")
    
    def log(self, level: str, source: str, message: str, **kwargs):
        """Add log entry"""
        self.live_logger.add_entry(level, source, message, **kwargs)
    
    def get_logger_handler(self):
        """Get Python logging handler for integration"""
        integration = LoggerIntegration(self.live_logger)
        return integration.create_handler()