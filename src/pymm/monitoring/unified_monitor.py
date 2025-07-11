"""Unified monitor combining all monitoring features

This module consolidates all monitoring capabilities:
- Basic monitoring (from monitor.py)
- Enhanced monitoring features (from enhanced_monitor.py)
- Unified display modes (from unified_monitor.py in cli)
- Enhanced unified features (from enhanced_unified_monitor.py)
- Resource monitoring
- Live progress tracking
- Statistics dashboard
- Output exploration
"""
import os
import time
import threading
import json
import psutil
from pathlib import Path
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from collections import deque

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.columns import Columns
from rich import box
from rich.align import Align
from rich.padding import Padding

from ..core.state import StateManager
from ..core.job_manager import get_job_manager
from ..core.file_tracker import UnifiedFileTracker
from ..theme import (
    ICONS, format_progress_bar, get_progress_color, get_panel_style
)

logger = None  # Avoid circular imports
console = Console()


class MonitoringMode:
    """Monitoring display modes"""
    COMPACT = "compact"
    DETAILED = "detailed"
    DASHBOARD = "dashboard"
    MINIMAL = "minimal"
    AUTO = "auto"


class UnifiedMonitor:
    """Unified monitor with all monitoring features"""

    def __init__(self, output_dir: Path, mode: str = MonitoringMode.AUTO):
        """Initialize unified monitor

        Args:
            output_dir: Output directory to monitor
            mode: Display mode (compact, detailed, dashboard, minimal, auto)
        """
        self.output_dir = Path(output_dir)
        self.mode = mode
        self.running = False
        self.update_interval = 1.0  # seconds

        # State management
        self.state_manager = StateManager(str(output_dir))
        self.file_tracker = None
        self.job_manager = None

        # Try to initialize enhanced features
        try:
            self.file_tracker = UnifiedFileTracker()
        except BaseException:
            pass

        try:
            self.job_manager = get_job_manager()
        except BaseException:
            pass

        # Resource monitoring
        self.resource_monitor = ResourceMonitor()

        # Progress tracking
        self.progress_tracker = ProgressTracker(output_dir)

        # Statistics
        self.stats = {
            "start_time": None,
            "files_processed": 0,
            "files_failed": 0,
            "total_files": 0,
            "concepts_found": 0,
            "processing_rate": 0,
            "avg_file_time": 0,
            "current_file": None,
            "eta": None
        }

        # Recent activity log
        self.activity_log = deque(maxlen=50)

        # Error tracking
        self.recent_errors = deque(maxlen=10)

        # Performance history
        self.performance_history = deque(maxlen=100)

        # Auto-detect best display mode
        if self.mode == MonitoringMode.AUTO:
            self._auto_detect_mode()

    def _auto_detect_mode(self):
        """Auto-detect best display mode based on terminal size"""
        try:
            width, height = os.get_terminal_size()

            if height < 20:
                self.mode = MonitoringMode.MINIMAL
            elif height < 30:
                self.mode = MonitoringMode.COMPACT
            elif width > 120 and height > 40:
                self.mode = MonitoringMode.DASHBOARD
            else:
                self.mode = MonitoringMode.DETAILED

        except BaseException:
            self.mode = MonitoringMode.COMPACT

    def start(self, live_display: bool = True):
        """Start monitoring

        Args:
            live_display: Whether to show live display
        """
        self.running = True
        self.stats["start_time"] = time.time()

        # Start resource monitoring
        self.resource_monitor.start()

        if live_display:
            # Start live display in separate thread
            self.display_thread = threading.Thread(
                target=self._run_live_display, daemon=True)
            self.display_thread.start()
        else:
            # Just update stats in background
            self.update_thread = threading.Thread(
                target=self._update_loop, daemon=True)
            self.update_thread.start()

    def stop(self):
        """Stop monitoring"""
        self.running = False
        self.resource_monitor.stop()

        # Wait for threads to finish
        if hasattr(self, 'display_thread'):
            self.display_thread.join(timeout=2)
        if hasattr(self, 'update_thread'):
            self.update_thread.join(timeout=2)

    def _run_live_display(self):
        """Run live display loop with smart refresh to prevent flicker"""
        # Previous state for change detection
        prev_stats = {}
        force_refresh = True
        refresh_countdown = 0

        # Adjust refresh rate based on mode
        if self.mode == MonitoringMode.DASHBOARD:
            refresh_per_second = 0.5  # Slower refresh for complex dashboard
        elif self.mode == MonitoringMode.MINIMAL:
            refresh_per_second = 2  # Faster for minimal display
        else:
            refresh_per_second = 1

        with Live(console=console, refresh_per_second=refresh_per_second, transient=True) as live:
            while self.running:
                try:
                    # Update statistics
                    self._update_stats()

                    # Check if stats have changed significantly
                    stats_changed = (
                        prev_stats.get("files_processed") != self.stats["files_processed"] or
                        prev_stats.get("files_failed") != self.stats["files_failed"] or
                        prev_stats.get("current_file") != self.stats["current_file"] or
                        force_refresh or
                        refresh_countdown <= 0
                    )

                    if stats_changed:
                        # Generate display based on mode
                        if self.mode == MonitoringMode.MINIMAL:
                            display = self._create_minimal_display()
                        elif self.mode == MonitoringMode.COMPACT:
                            display = self._create_compact_display()
                        elif self.mode == MonitoringMode.DETAILED:
                            display = self._create_detailed_display()
                        elif self.mode == MonitoringMode.DASHBOARD:
                            display = self._create_dashboard_display()
                        else:
                            display = self._create_compact_display()

                        live.update(display)
                        prev_stats = self.stats.copy()
                        force_refresh = False
                        refresh_countdown = 10  # Force refresh every 10 iterations

                    refresh_countdown -= 1
                    time.sleep(self.update_interval)

                except KeyboardInterrupt:
                    break
                except Exception as e:
                    console.print(f"[dim]Display error: {e}[/dim]")
                    break

    def _update_loop(self):
        """Update statistics in background without display"""
        while self.running:
            try:
                self._update_stats()
                time.sleep(self.update_interval)
            except BaseException:
                pass

    def _update_stats(self):
        """Update monitoring statistics"""
        # Get state info
        state_info = self.state_manager.get_statistics()
        self.stats["total_files"] = state_info.get("total_files", 0)
        self.stats["files_processed"] = state_info.get("completed", 0)
        self.stats["files_failed"] = state_info.get("failed", 0)

        # Calculate processing rate
        if self.stats["start_time"] and self.stats["files_processed"] > 0:
            elapsed = time.time() - self.stats["start_time"]
            self.stats["processing_rate"] = self.stats["files_processed"] / elapsed
            self.stats["avg_file_time"] = elapsed / \
                self.stats["files_processed"]

            # Calculate ETA
            remaining = self.stats["total_files"] - \
                self.stats["files_processed"] - self.stats["files_failed"]
            if remaining > 0 and self.stats["processing_rate"] > 0:
                eta_seconds = remaining / self.stats["processing_rate"]
                self.stats["eta"] = datetime.now(
                ) + timedelta(seconds=eta_seconds)

        # Get current processing files
        in_progress = state_info.get("in_progress_files", [])
        if in_progress:
            self.stats["current_file"] = Path(in_progress[0]).name

        # Count concepts from CSV files
        self._update_concept_count()

        # Check for new errors
        self._check_errors()

    def _update_concept_count(self):
        """Count total concepts from CSV files"""
        try:
            csv_files = list(self.output_dir.glob("*.csv"))
            total_concepts = 0

            for csv_file in csv_files[-10:]:  # Sample last 10 files
                try:
                    with open(csv_file, 'r') as f:
                        # Count non-header, non-empty lines
                        lines = sum(1 for line in f if line.strip()
                                    and not line.startswith('#'))
                        total_concepts += max(0, lines - 1)  # Subtract header
                except BaseException:
                    pass

            # Extrapolate if not all files checked
            if len(csv_files) > 10:
                avg_per_file = total_concepts / 10
                self.stats["concepts_found"] = int(
                    avg_per_file * len(csv_files))
            else:
                self.stats["concepts_found"] = total_concepts

        except BaseException:
            pass

    def _check_errors(self):
        """Check for recent errors"""
        if self.state_manager._state.get("failed_files"):
            for file_path, error_info in self.state_manager._state["failed_files"].items(
            ):
                error_entry = {
                    "file": Path(file_path).name,
                    "error": error_info.get("error", "Unknown error"),
                    "time": error_info.get("timestamp", "")
                }

                # Add if not already in recent errors
                if not any(e["file"] == error_entry["file"]
                           for e in self.recent_errors):
                    self.recent_errors.append(error_entry)
                    self.activity_log.append(
                        f"[dim]PROCESSING ERROR[/dim] {error_entry['file']}: {error_entry['error'][:50]}...")

    def _create_minimal_display(self) -> Panel:
        """Create minimal single-line display"""
        processed = self.stats["files_processed"]
        total = self.stats["total_files"]
        failed = self.stats["files_failed"]
        rate = self.stats["processing_rate"]

        if total > 0:
            percent = (processed + failed) / total * 100
        else:
            percent = 0

        # Create minimal progress bar
        bar_width = 20
        filled = int(bar_width * percent / 100)
        bar = f"[dim]{'═' * filled}[/dim][bright_black]{'─' * (bar_width - filled)}[/bright_black]"

        # Status line
        status = f"{bar} {percent:>3.0f}%   Processed: {processed}/{total}   Failed: {failed}   Rate: {rate:.1f}/sec"

        if self.stats["current_file"]:
            status += f"   Processing: {self.stats['current_file'][:30]}..."

        # Use colorful theme for minimal display
        border_color = get_progress_color(percent)
        
        return Panel(
            status, 
            box=box.ROUNDED, 
            style='', 
            border_style=border_color,
            padding=(0, 1)
        )

    def _create_compact_display(self) -> Layout:
        """Create compact display layout"""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="progress", size=5),
            Layout(name="stats", size=7),
            Layout(name="resources", size=4)
        )

        # Header with colorful theme
        header_style = get_panel_style('header')
        header = Panel(
            "[bold bright_blue]MetaMap Processing Monitor[/bold bright_blue]\n"
            f"[bright_cyan]Output Directory: {self.output_dir}[/bright_cyan]", 
            box=getattr(box, header_style['box']),
            style=header_style['style'],
            border_style=header_style['border_style'],
            padding=header_style['padding']
        )
        layout["header"].update(header)

        # Progress with colorful theme
        progress_content = self._create_progress_bars()
        layout["progress"].update(
            Panel(
                progress_content,
                title="[bold bright_blue]Processing Progress[/bold bright_blue]",
                box=box.ROUNDED,
                style='',
                border_style="bright_cyan",
                padding=(1, 2)
            )
        )

        # Statistics with colorful theme
        stats_table = self._create_stats_table()
        layout["stats"].update(
            Panel(
                stats_table,
                title="[bold bright_blue]Statistics[/bold bright_blue]",
                box=box.ROUNDED,
                style='',
                border_style="bright_green",
                padding=(1, 2)
            )
        )

        # Resources with colorful theme
        resources = self.resource_monitor.get_compact_display()
        layout["resources"].update(
            Panel(
                resources,
                title="[bold bright_blue]System Resources[/bold bright_blue]",
                box=box.ROUNDED,
                style='',
                border_style="bright_magenta",
                padding=(1, 2)
            )
        )

        return layout

    def _create_detailed_display(self) -> Layout:
        """Create detailed display layout"""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=3)
        )

        # Header
        header = self._create_header_panel()
        layout["header"].update(header)

        # Main area
        main = Layout()
        main.split_row(
            Layout(name="left", ratio=2),
            Layout(name="right", ratio=1)
        )

        # Left side - progress and stats
        left = Layout()
        left.split_column(
            Layout(name="progress", size=8),
            Layout(name="stats", size=10),
            Layout(name="activity", ratio=1)
        )

        left["progress"].update(
            Panel(
                self._create_detailed_progress(),
                title="[bold bright_blue]Processing Progress[/bold bright_blue]",
                box=box.ROUNDED,
                style='',
                border_style='bright_cyan',
                padding=(1, 2)
            )
        )
        left["stats"].update(
            Panel(
                self._create_detailed_stats(),
                title="[bold bright_blue]Detailed Statistics[/bold bright_blue]",
                box=box.ROUNDED,
                style='',
                border_style='bright_green',
                padding=(1, 2)
            )
        )
        left["activity"].update(
            Panel(
                self._create_activity_log(),
                title="[bold bright_blue]Recent Activity[/bold bright_blue]",
                box=box.ROUNDED,
                style='',
                border_style='bright_yellow',
                padding=(1, 2)
            )
        )

        # Right side - resources and errors
        right = Layout()
        right.split_column(
            Layout(name="resources", size=12),
            Layout(name="errors", ratio=1)
        )

        right["resources"].update(self.resource_monitor.get_detailed_panel())
        right["errors"].update(
            Panel(
                self._create_error_list(),
                title="[bold bright_red]Recent Errors[/bold bright_red]",
                box=box.ROUNDED,
                style='',
                border_style='bright_red',
                padding=(1, 2)
            )
        )

        main["left"].update(left)
        main["right"].update(right)
        layout["main"].update(main)

        # Footer
        footer = self._create_footer_panel()
        layout["footer"].update(footer)

        return layout

    def _create_dashboard_display(self) -> Layout:
        """Create full dashboard display"""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=4),
            Layout(name="overview", size=8),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=3)
        )

        # Header with colorful styling
        header = Panel(
            Align.center(
                "[bold bright_blue]PythonMetaMap Processing Dashboard[/bold bright_blue]\n\n"
                f"[bright_cyan]Output Directory: {self.output_dir}[/bright_cyan]\n"
                f"[bright_green]{ICONS['running']} Status: Processing Active[/bright_green]"
            ),
            box=box.DOUBLE,
            style='',
            border_style="bright_blue",
            padding=(1, 2)
        )
        layout["header"].update(header)

        # Overview with key metrics
        overview = self._create_overview_panel()
        layout["overview"].update(overview)

        # Main content area
        main = Layout()
        main.split_row(
            Layout(name="left", ratio=1),
            Layout(name="center", ratio=1),
            Layout(name="right", ratio=1)
        )

        # Left column with modern panels
        left = Layout()
        left.split_column(
            Layout(name="progress", size=12),
            Layout(name="performance", ratio=1)
        )
        left["progress"].update(
            Panel(
                Padding(self._create_detailed_progress(), (1, 2)),
                title="[bold bright_cyan]Progress Tracking[/bold bright_cyan]",
                box=box.ROUNDED,
                style='',
                border_style='bright_cyan',
                padding=(1, 2)
            )
        )
        left["performance"].update(
            Panel(
                Padding(self._create_performance_chart(), (1, 2)),
                title="[bold bright_magenta]Performance Metrics[/bold bright_magenta]",
                box=box.ROUNDED,
                style='',
                border_style='bright_magenta',
                padding=(1, 2)
            )
        )

        # Center column with modern panels
        center = Layout()
        center.split_column(
            Layout(name="stats", size=14),
            Layout(name="activity", ratio=1)
        )
        center["stats"].update(
            Panel(
                self._create_detailed_stats(),
                title="[bold bright_green]Statistics[/bold bright_green]",
                box=box.ROUNDED,
                style='',
                border_style='bright_green',
                padding=(1, 2)
            )
        )
        center["activity"].update(
            Panel(
                Padding(self._create_activity_log(), (1, 2)),
                title="[bold bright_yellow]Activity Log[/bold bright_yellow]",
                box=box.ROUNDED,
                style='',
                border_style='bright_yellow',
                padding=(1, 2)
            )
        )

        # Right column with modern panels
        right = Layout()
        right.split_column(
            Layout(name="resources", size=16),
            Layout(name="errors", ratio=1)
        )
        right["resources"].update(self.resource_monitor.get_detailed_panel())
        right["errors"].update(
            Panel(
                Padding(self._create_error_list(), (1, 2)),
                title="[bold bright_red]Error Log[/bold bright_red]",
                box=box.ROUNDED,
                style='',
                border_style='bright_red',
                padding=(1, 2)
            )
        )

        main["left"].update(left)
        main["center"].update(center)
        main["right"].update(right)
        layout["main"].update(main)

        # Footer
        footer = self._create_footer_panel()
        layout["footer"].update(footer)

        return layout

    def _create_header_panel(self) -> Panel:
        """Create header panel"""
        elapsed = time.time() - \
            self.stats["start_time"] if self.stats["start_time"] else 0
        elapsed_str = str(timedelta(seconds=int(elapsed)))

        content = "[black]MetaMap Processing Monitor[/black]  "
        content += f"[dim]Elapsed Time: {elapsed_str}[/dim]  "

        if self.stats["eta"]:
            eta_str = self.stats["eta"].strftime("%H:%M:%S")
            content += f"[dim]Estimated Completion: {eta_str}[/dim]"
        else:
            content += "[dim]Calculating estimated completion time...[/dim]"

        return Panel(
            content, 
            box=box.ROUNDED, 
            style='bold', 
            border_style="bright_cyan",
            padding=(0, 1)
        )

    def _create_footer_panel(self) -> Panel:
        """Create footer panel with minimal controls"""
        controls = (
            "[dim]Q[/dim] - Quit   "
            "[dim]P[/dim] - Pause   "
            "[dim]R[/dim] - Resume   "
            "[dim]M[/dim] - Change Mode   "
            "[dim]E[/dim] - Export Report   "
            "[dim]H[/dim] - Help"
        )

        return Panel(
            Align.center(controls),
            box=box.MINIMAL,  # Fixed: replaced None with MINIMAL
            style='',
            border_style='bright_black',
            padding=(1, 2)
        )

    def _create_progress_bars(self) -> str:
        """Create colorful progress bars for compact display"""
        processed = self.stats["files_processed"]
        failed = self.stats["files_failed"]
        total = self.stats["total_files"]

        if total > 0:
            progress_percent = processed / total * 100
            failed_percent = failed / total * 100
            overall_percent = (processed + failed) / total * 100
        else:
            progress_percent = failed_percent = overall_percent = 0

        # Overall progress with gradient colors
        overall_bar = format_progress_bar(overall_percent, 30)
        success_bar = format_progress_bar(progress_percent, 30, 'bright_green')
        failed_bar = format_progress_bar(failed_percent, 30, 'bright_red')
        
        content = f"[bold]Overall Progress:[/bold] {overall_bar} [bold bright_blue]{overall_percent:>3.0f}%[/bold bright_blue]\n"
        content += f"[bold]Successfully Processed:[/bold] {success_bar} [bright_green]{processed}/{total}[/bright_green]\n"
        content += f"[bold]Failed Processing:[/bold]  {failed_bar} [bright_red]{failed}/{total}[/bright_red]"

        return content

    def _create_bar(
            self,
            percent: float,
            width: int,
            color: str = None) -> str:
        """Create a colorful progress bar"""
        return format_progress_bar(percent, width, color)

    def _create_stats_table(self) -> Table:
        """Create colorful statistics table"""
        table = Table(
            show_header=False, 
            box=box.SIMPLE,  # Fixed: replaced None with SIMPLE
            padding=(0, 1),
            border_style='bright_cyan'
        )
        table.add_column("Metric", style="bold bright_blue")
        table.add_column("Value", justify="right", style="bright_green")

        # Add icon indicators
        table.add_row(
            f"{ICONS['arrow']} Processing Rate",
            f"{self.stats['processing_rate']:.2f} files/sec"
        )
        table.add_row(
            f"{ICONS['arrow']} Average Processing Time", 
            f"{self.stats['avg_file_time']:.2f} seconds"
        )
        table.add_row(
            f"{ICONS['arrow']} Total Concepts Identified", 
            f"{self.stats['concepts_found']:,}"
        )

        if self.stats["current_file"]:
            table.add_row(
                f"{ICONS['arrow']} Current File Processing", 
                f"[italic]{self.stats['current_file'][:40]}[/italic]"
            )

        return table
    
    def _create_ring_progress(self, percent: float) -> str:
        """Create a colorful progress indicator"""
        color = get_progress_color(percent)
        
        # Add icon based on completion
        if percent >= 100:
            return f"[{color}]{ICONS['success']} {int(percent)}%[/{color}]"
        elif percent >= 75:
            return f"[{color}]{ICONS['spinner'][0]} {int(percent)}%[/{color}]"
        else:
            return f"[{color}]{int(percent)}%[/{color}]"

    def _create_detailed_progress(self) -> str:
        """Create detailed colorful progress display"""
        processed = self.stats["files_processed"]
        failed = self.stats["files_failed"]
        total = self.stats["total_files"]
        remaining = total - processed - failed

        content = f"[bold bright_green]{ICONS['check']} Successfully Processed:[/bold bright_green] {processed:>6} files\n"
        content += f"[bold bright_red]{ICONS['cross']} Processing Failed:[/bold bright_red]    {failed:>6} files\n"
        content += f"[bold bright_yellow]{ICONS['dot']} Remaining Files:[/bold bright_yellow] {remaining:>6} files\n"
        content += f"[bold bright_blue]{ICONS['dot']} Total Files:[/bold bright_blue]     {total:>6} files\n\n"

        # Colorful progress bar with percentage
        if total > 0:
            percent = (processed + failed) / total * 100
            bar = format_progress_bar(percent, 40)
            color = get_progress_color(percent)
            content += f"{bar} [{color} bold]{percent:>5.1f}%[/{color} bold]\n"

        # Current file with animated indicator
        if self.stats["current_file"]:
            content += f"\n[bold bright_cyan]{ICONS['running']} Currently Processing:[/bold bright_cyan] [italic bright_blue]{self.stats['current_file']}[/italic bright_blue]"

        return content

    def _create_detailed_stats(self) -> Table:
        """Create detailed statistics table with colorful theme"""
        table = Table(
            show_header=True,
            box=box.ROUNDED,
            title_style='bold bright_blue',
            header_style='bold bright_cyan',
            border_style='bright_cyan',
            padding=(0, 2),
            expand=True
        )
        table.add_column("Metric", style='bold bright_blue', width=20)
        table.add_column("Value", justify="right", style='bright_green', width=15)
        table.add_column("Details", style='bright_magenta', width=25)

        # Time stats with colorful formatting
        elapsed = time.time() - self.stats["start_time"] if self.stats["start_time"] else 0
        elapsed_str = str(timedelta(seconds=int(elapsed)))
        table.add_row(
            f"{ICONS['arrow']} Elapsed Time",
            f"[bold bright_cyan]{elapsed_str}[/bold bright_cyan]",
            "Total elapsed processing time"
        )

        if self.stats["eta"]:
            remaining = (self.stats["eta"] - datetime.now()).total_seconds()
            remaining_str = str(timedelta(seconds=int(max(0, remaining))))
            eta_color = 'bright_green' if remaining < 300 else 'bright_yellow'
            table.add_row(
                f"{ICONS['arrow']} Time Remaining",
                f"[bold {eta_color}]{remaining_str}[/bold {eta_color}]",
                "Estimated time to completion"
            )

        # Processing stats with color indicators
        rate_color = 'bright_green' if self.stats['processing_rate'] > 2 else 'bright_yellow'
        table.add_row(
            f"{ICONS['arrow']} Processing Rate",
            f"[bold {rate_color}]{self.stats['processing_rate']:.2f}/sec[/bold {rate_color}]",
            "Files processed per second"
        )
        
        avg_time_color = 'bright_green' if self.stats['avg_file_time'] < 2 else 'bright_yellow'
        table.add_row(
            f"{ICONS['arrow']} Average Processing Time",
            f"[bold {avg_time_color}]{self.stats['avg_file_time']:.2f} seconds[/bold {avg_time_color}]",
            "Average time per file"
        )

        # Results with vibrant colors
        table.add_row(
            f"{ICONS['arrow']} Concepts Identified",
            f"[bold bright_blue]{self.stats['concepts_found']:,}[/bold bright_blue]",
            "Total medical concepts identified"
        )

        if self.stats['files_processed'] > 0:
            avg_concepts = self.stats['concepts_found'] / self.stats['files_processed']
            avg_color = 'bright_green' if avg_concepts > 10 else 'bright_yellow'
            table.add_row(
                f"{ICONS['arrow']} Average Concepts per File",
                f"[bold {avg_color}]{avg_concepts:.1f}[/bold {avg_color}]",
                "Average concepts identified per file"
            )

        return table

    def _create_activity_log(self) -> str:
        """Create colorful activity log display"""
        if not self.activity_log:
            return "[italic bright_cyan]No recent activity recorded[/italic bright_cyan]"

        # Show last 10 activities with colorful formatting
        content = ""
        for i, activity in enumerate(list(self.activity_log)[-10:]):
            # Apply colorful formatting to activities
            if "ERROR" in activity:
                icon = ICONS['error']
                color = 'bright_red'
            elif "SUCCESS" in activity or "COMPLETE" in activity:
                icon = ICONS['success']
                color = 'bright_green'
            elif "WARNING" in activity:
                icon = ICONS['warning']
                color = 'bright_yellow'
            else:
                icon = ICONS['info']
                color = 'bright_cyan'
            
            # Format with color and icon
            formatted = activity
            for old, new in [
                ("[red]", f"[{color}]"),
                ("[/red]", f"[/{color}]"),
                ("[green]", "[bright_green]"),
                ("[/green]", "[/bright_green]"),
                ("[yellow]", "[bright_yellow]"),
                ("[/yellow]", "[/bright_yellow]"),
                ("[dim]", "[bright_cyan]"),
                ("[/dim]", "[/bright_cyan]")
            ]:
                formatted = formatted.replace(old, new)
            
            content += f"{icon} {formatted}\n"

        return content.strip()

    def _create_error_list(self) -> str:
        """Create colorful error list display"""
        if not self.recent_errors:
            return f"[bright_green]{ICONS['success']} No processing errors detected[/bright_green]"

        content = ""
        for i, error in enumerate(list(self.recent_errors)[-5:], 1):
            # Error with colorful formatting
            content += f"[bold bright_red]{ICONS['error']} Processing Error {i}:[/bold bright_red] [bright_yellow]{error['file']}[/bright_yellow]\n"
            content += f"   [bright_red]{error['error'][:60]}...[/bright_red]\n"
            if error.get('time'):
                content += f"   [italic bright_cyan]Timestamp: {error['time']}[/italic bright_cyan]\n"
            content += "\n"

        return content.strip()

    def _create_overview_panel(self) -> Panel:
        """Create overview panel for dashboard"""
        # Calculate percentages
        processed = self.stats["files_processed"]
        failed = self.stats["files_failed"]
        total = self.stats["total_files"]

        success_rate = (processed / (processed + failed) *
                        100) if (processed + failed) > 0 else 0
        completion = ((processed + failed) / total * 100) if total > 0 else 0

        # Create minimal metric cards
        cards = []

        # Colorful metric cards with gradients
        card_style = get_panel_style('card')
        
        # Completion card with gradient color
        completion_color = get_progress_color(completion)
        cards.append(Panel(
            Align.center(
                f"[bold {completion_color}]{completion:.1f}%[/bold {completion_color}]\n[bright_cyan]Completion Rate[/bright_cyan]"
            ),
            box=box.ROUNDED,
            style='',
            width=22,
            border_style=completion_color,
            padding=(1, 1)
        ))

        # Success rate card
        success_color = 'bright_green' if success_rate > 90 else 'bright_yellow' if success_rate > 70 else 'bright_red'
        cards.append(Panel(
            Align.center(
                f"[bold {success_color}]{success_rate:.1f}%[/bold {success_color}]\n[bright_green]Success Rate[/bright_green]"
            ),
            box=box.ROUNDED,
            style='',
            width=22,
            border_style=success_color,
            padding=(1, 1)
        ))

        # Files/sec card
        rate_color = 'bright_green' if self.stats['processing_rate'] > 2 else 'bright_yellow'
        cards.append(Panel(
            Align.center(
                f"[bold {rate_color}]{self.stats['processing_rate']:.2f}[/bold {rate_color}]\n[bright_magenta]Processing Rate[/bright_magenta]"
            ),
            box=box.ROUNDED,
            style='',
            width=22,
            border_style='bright_magenta',
            padding=(1, 1)
        ))

        # Concepts card
        cards.append(Panel(
            Align.center(
                f"[bold bright_blue]{self.stats['concepts_found']:,}[/bold bright_blue]\n[bright_blue]Total Concepts[/bright_blue]"
            ),
            box=box.ROUNDED,
            style='',
            width=22,
            border_style='bright_blue',
            padding=(1, 1)
        ))

        # ETA card
        if self.stats["eta"]:
            eta_str = self.stats["eta"].strftime("%H:%M")
            cards.append(Panel(
                Align.center(
                    f"[bold bright_cyan]{eta_str}[/bold bright_cyan]\n[bright_yellow]Estimated Completion[/bright_yellow]"
                ),
                box=box.ROUNDED,
                style='',
                width=22,
                border_style='bright_yellow',
                padding=(1, 1)
            ))

        return Panel(
            Columns(cards, equal=True, expand=False),
            title="[bold bright_blue]Processing Overview[/bold bright_blue]",
            box=box.ROUNDED,
            style='',
            border_style="bright_blue",
            padding=(1, 2)
        )

    def _create_performance_chart(self) -> str:
        """Create colorful performance chart"""
        if not self.performance_history:
            return "[italic bright_cyan]Collecting performance metrics...[/italic bright_cyan]"

        # Chart dimensions
        chart_height = 8
        chart_width = 40

        # Get last N samples and add current processing rate
        self.performance_history.append(self.stats['processing_rate'])
        samples = list(self.performance_history)[-chart_width:]
        
        if not samples:
            return "[bright_yellow]No performance data available[/bright_yellow]"

        # Calculate stats
        max_val = max(samples) if samples else 1
        min_val = min(samples) if samples else 0
        avg_val = sum(samples) / len(samples) if samples else 0
        range_val = max_val - min_val or 1

        # Create colorful chart
        chart_lines = []
        
        # Add colorful header
        chart_lines.append(
            f"[bold bright_green]Maximum: {max_val:.2f} files/sec[/bold bright_green]   "
            f"[bold bright_yellow]Average: {avg_val:.2f} files/sec[/bold bright_yellow]   "
            f"[bold bright_red]Minimum: {min_val:.2f} files/sec[/bold bright_red]"
        )
        chart_lines.append("")  # Empty line
        
        # Build chart with gradient colors
        for y in range(chart_height, 0, -1):
            line = ""
            threshold = min_val + (range_val * y / chart_height)
            
            for x, val in enumerate(samples):
                if val >= threshold:
                    # Color based on performance level
                    if val > avg_val * 1.2:
                        char_color = 'bright_green'
                        char = '█'  # Full block
                    elif val > avg_val * 0.8:
                        char_color = 'bright_yellow'
                        char = '▓'  # Medium shade
                    else:
                        char_color = 'bright_red'
                        char = '░'  # Light shade
                    line += f"[{char_color}]{char}[/{char_color}]"
                else:
                    line += " "
            
            # Add y-axis label with color
            label_color = 'bright_cyan'
            chart_lines.append(
                f"{line} [{label_color}]{threshold:>5.1f}[/{label_color}]"
            )
        
        # Add colorful x-axis
        chart_lines.append(f"[bright_blue]{'━' * chart_width}[/bright_blue]")
        chart_lines.append(
            f"[bright_magenta]{' ' * (chart_width // 2 - 7) + 'Time Progression' + ' ' * (chart_width // 2 - 8)}[/bright_magenta]"
        )

        return "\n".join(chart_lines)

    def export_report(self, output_path: Optional[Path] = None) -> Path:
        """Export monitoring report"""
        if not output_path:
            output_path = self.output_dir / \
                f"monitor_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        report = {
            "output_directory": str(
                self.output_dir),
            "start_time": datetime.fromtimestamp(
                self.stats["start_time"]).isoformat() if self.stats["start_time"] else None,
            "end_time": datetime.now().isoformat(),
            "statistics": self.stats,
            "errors": list(
                self.recent_errors),
            "performance_metrics": {
                "avg_processing_time": self.stats["avg_file_time"],
                    "processing_rate": self.stats["processing_rate"],
                    "success_rate": (
                        self.stats["files_processed"] /
                        (
                            self.stats["files_processed"] +
                            self.stats["files_failed"]) *
                        100) if (
                        self.stats["files_processed"] +
                        self.stats["files_failed"]) > 0 else 0},
            "resource_usage": self.resource_monitor.get_summary()}

        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)

        return output_path

    def get_current_stats(self) -> Dict[str, Any]:
        """Get current monitoring statistics"""
        self._update_stats()
        return self.stats.copy()


class ResourceMonitor:
    """System resource monitoring"""

    def __init__(self):
        self.monitoring = False
        self.monitor_thread = None
        self.cpu_history = deque(maxlen=60)
        self.memory_history = deque(maxlen=60)
        self.disk_history = deque(maxlen=30)
        
        # Import the theme elements we need from parent imports

    def start(self):
        """Start resource monitoring"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

    def stop(self):
        """Stop resource monitoring"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)

    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.monitoring:
            try:
                # CPU
                self.cpu_history.append(psutil.cpu_percent(interval=0.1))

                # Memory
                mem = psutil.virtual_memory()
                self.memory_history.append(mem.percent)

                # Disk
                disk = psutil.disk_usage('/')
                self.disk_history.append(disk.percent)

                time.sleep(1)

            except Exception:
                pass

    def get_compact_display(self) -> str:
        """Get compact resource display with colorful theme"""
        cpu = self.cpu_history[-1] if self.cpu_history else 0
        mem = self.memory_history[-1] if self.memory_history else 0
        disk = self.disk_history[-1] if self.disk_history else 0

        # Create colorful resource display
        lines = []
        
        # CPU with vibrant color coding
        cpu_color = 'bright_red' if cpu > 80 else 'bright_yellow' if cpu > 50 else 'bright_green'
        lines.append(
            f"[bold bright_cyan]CPU Usage[/bold bright_cyan]    {self._create_mini_bar(cpu, 12)} "
            f"[bold {cpu_color}]{cpu:>3.0f}%[/bold {cpu_color}]"
        )
        
        # RAM with vibrant color coding
        mem_color = 'bright_red' if mem > 80 else 'bright_yellow' if mem > 50 else 'bright_green'
        lines.append(
            f"[bold bright_magenta]Memory Usage[/bold bright_magenta] {self._create_mini_bar(mem, 12)} "
            f"[bold {mem_color}]{mem:>3.0f}%[/bold {mem_color}]"
        )
        
        # Disk with vibrant color coding
        disk_color = 'bright_red' if disk > 90 else 'bright_yellow' if disk > 70 else 'bright_green'
        lines.append(
            f"[bold bright_blue]Disk Usage[/bold bright_blue]   {self._create_mini_bar(disk, 12)} "
            f"[bold {disk_color}]{disk:>3.0f}%[/bold {disk_color}]"
        )

        return "\n".join(lines)

    def get_detailed_panel(self) -> Panel:
        """Get detailed resource panel with colorful theme"""
        stats = self.get_summary()

        table = Table(
            show_header=False,
            box=box.SIMPLE,  # Fixed: replaced None with SIMPLE
            padding=(0, 1),
            expand=True,
            border_style='bright_cyan'
        )
        table.add_column("Resource", style='bold bright_blue', width=12)
        table.add_column("Usage", width=30)
        table.add_column("Value", justify="right", style='bright_green')

        # CPU with colorful bar - use current value from nested dict
        cpu_percent = stats['cpu']['current']
        cpu_color = 'bright_red' if cpu_percent > 80 else 'bright_yellow' if cpu_percent > 50 else 'bright_green'
        cpu_bar = self._create_bar(cpu_percent, 20, cpu_color)
        table.add_row(
            f"{ICONS['arrow']} CPU Utilization",
            cpu_bar,
            f"[bold {cpu_color}]{cpu_percent:.1f}%[/bold {cpu_color}]")

        # Memory with colorful bar - use current value from nested dict
        mem_percent = stats['memory']['current']
        mem_color = 'bright_red' if mem_percent > 80 else 'bright_yellow' if mem_percent > 50 else 'bright_green'
        mem_bar = self._create_bar(mem_percent, 20, mem_color)

        # Get memory info from psutil
        mem_info = psutil.virtual_memory()
        mem_used_gb = mem_info.used / (1024**3)
        mem_total_gb = mem_info.total / (1024**3)
        table.add_row(
            f"{ICONS['arrow']} Memory Utilization",
            mem_bar,
            f"[bold {mem_color}]{mem_used_gb:.1f}/{mem_total_gb:.1f} GB[/bold {mem_color}]"
        )

        # Disk with colorful bar - use current value from nested dict
        disk_percent = stats['disk']['current']
        disk_color = 'bright_red' if disk_percent > 90 else 'bright_yellow' if disk_percent > 70 else 'bright_green'
        disk_bar = self._create_bar(disk_percent, 20, disk_color)

        # Get disk info from psutil
        disk_info = psutil.disk_usage('/')
        disk_free_gb = disk_info.free / (1024**3)
        disk_total_gb = disk_info.total / (1024**3)
        table.add_row(
            f"{ICONS['arrow']} Disk Space Utilization",
            disk_bar,
            f"[bold {disk_color}]{disk_free_gb:.1f}/{disk_total_gb:.1f} GB available[/bold {disk_color}]"
        )

        # Network (if available)
        try:
            # Get network stats from psutil
            net_io = psutil.net_io_counters()
            if net_io:
                sent_mb = net_io.bytes_sent / (1024**2)
                recv_mb = net_io.bytes_recv / (1024**2)
                table.add_row(
                    f"{ICONS['up']} Network Upload",
                    "",
                    f"[bold bright_cyan]{sent_mb:.1f} MB[/bold bright_cyan]"
                )
                table.add_row(
                    f"{ICONS['down']} Network Download",
                    "",
                    f"[bold bright_cyan]{recv_mb:.1f} MB[/bold bright_cyan]"
                )
        except BaseException:
            # Network stats not available
            pass

        return Panel(
            table,
            title="[bold bright_magenta]System Resources[/bold bright_magenta]",
            box=box.ROUNDED,
            style='',
            border_style='bright_magenta',
            padding=(1, 2)
        )

    def _create_mini_bar(self, percent: float, width: int = 10) -> str:
        """Create mini progress bar with colorful theme"""
        # Use the format_progress_bar function from theme
        return format_progress_bar(percent, width)

    def _create_bar(self, percent: float, width: int, color: str = None) -> str:
        """Create a colorful progress bar"""
        return format_progress_bar(percent, width, color)

    def get_summary(self) -> Dict[str, Any]:
        """Get resource usage summary"""
        return {
            "cpu": {
                "current": self.cpu_history[-1] if self.cpu_history else 0,
                "average": sum(self.cpu_history) / len(self.cpu_history) if self.cpu_history else 0,
                "max": max(self.cpu_history) if self.cpu_history else 0
            },
            "memory": {
                "current": self.memory_history[-1] if self.memory_history else 0,
                "average": sum(self.memory_history) / len(self.memory_history) if self.memory_history else 0,
                "max": max(self.memory_history) if self.memory_history else 0
            },
            "disk": {
                "current": self.disk_history[-1] if self.disk_history else 0
            }
        }


class ProgressTracker:
    """Track processing progress"""

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.start_time = time.time()
        self.file_times = deque(maxlen=100)

    def add_file_time(self, processing_time: float):
        """Record file processing time"""
        self.file_times.append(processing_time)

    def get_average_time(self) -> float:
        """Get average processing time"""
        if not self.file_times:
            return 0
        return sum(self.file_times) / len(self.file_times)

    def estimate_remaining_time(self, files_remaining: int) -> float:
        """Estimate remaining processing time"""
        avg_time = self.get_average_time()
        if avg_time > 0:
            return files_remaining * avg_time
        return 0


# Standalone monitoring function
def monitor_output(
        output_dir: str,
        mode: str = MonitoringMode.AUTO,
        refresh_rate: float = 1.0) -> None:
    """Monitor MetaMap processing output directory

    Args:
        output_dir: Output directory to monitor
        mode: Display mode
        refresh_rate: Update frequency in seconds
    """
    monitor = UnifiedMonitor(Path(output_dir), mode)
    monitor.update_interval = refresh_rate

    try:
        monitor.start(live_display=True)

        # Keep running until interrupted
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        console.print("\n[dim]Monitoring terminated by user request[/dim]")
    finally:
        monitor.stop()

        # Export final report
        report_path = monitor.export_report()
        console.print(f"\n[bright_black]Processing report saved to: {report_path}[/bright_black]")
