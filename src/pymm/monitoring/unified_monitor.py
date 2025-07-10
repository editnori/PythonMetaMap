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
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from collections import deque, defaultdict
import csv

from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.text import Text
from rich.columns import Columns
from rich import box
from rich.align import Align
from rich.tree import Tree
from rich.rule import Rule

from ..core.state import StateManager
from ..core.job_manager import get_job_manager
from ..core.file_tracker import UnifiedFileTracker

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
        except:
            pass
            
        try:
            self.job_manager = get_job_manager()
        except:
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
                
        except:
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
            self.display_thread = threading.Thread(target=self._run_live_display, daemon=True)
            self.display_thread.start()
        else:
            # Just update stats in background
            self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
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
        """Run live display loop"""
        with Live(console=console, refresh_per_second=1) as live:
            while self.running:
                try:
                    # Update statistics
                    self._update_stats()
                    
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
                    time.sleep(self.update_interval)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    console.print(f"[red]Display error: {e}[/red]")
                    break
                    
    def _update_loop(self):
        """Update statistics in background without display"""
        while self.running:
            try:
                self._update_stats()
                time.sleep(self.update_interval)
            except:
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
            self.stats["avg_file_time"] = elapsed / self.stats["files_processed"]
            
            # Calculate ETA
            remaining = self.stats["total_files"] - self.stats["files_processed"] - self.stats["files_failed"]
            if remaining > 0 and self.stats["processing_rate"] > 0:
                eta_seconds = remaining / self.stats["processing_rate"]
                self.stats["eta"] = datetime.now() + timedelta(seconds=eta_seconds)
                
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
                        lines = sum(1 for line in f if line.strip() and not line.startswith('#'))
                        total_concepts += max(0, lines - 1)  # Subtract header
                except:
                    pass
                    
            # Extrapolate if not all files checked
            if len(csv_files) > 10:
                avg_per_file = total_concepts / 10
                self.stats["concepts_found"] = int(avg_per_file * len(csv_files))
            else:
                self.stats["concepts_found"] = total_concepts
                
        except:
            pass
            
    def _check_errors(self):
        """Check for recent errors"""
        if self.state_manager._state.get("failed_files"):
            for file_path, error_info in self.state_manager._state["failed_files"].items():
                error_entry = {
                    "file": Path(file_path).name,
                    "error": error_info.get("error", "Unknown error"),
                    "time": error_info.get("timestamp", "")
                }
                
                # Add if not already in recent errors
                if not any(e["file"] == error_entry["file"] for e in self.recent_errors):
                    self.recent_errors.append(error_entry)
                    self.activity_log.append(f"[red]ERROR[/red] {error_entry['file']}: {error_entry['error'][:50]}...")
                    
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
            
        # Create progress bar
        bar_width = 20
        filled = int(bar_width * percent / 100)
        bar = f"[green]{'█' * filled}[/green][dim]{'░' * (bar_width - filled)}[/dim]"
        
        # Status line
        status = f"{bar} {percent:>3.0f}% | Files: {processed}/{total} | Failed: {failed} | Rate: {rate:.1f}/s"
        
        if self.stats["current_file"]:
            status += f" | Current: {self.stats['current_file'][:30]}..."
            
        return Panel(status, box=box.MINIMAL)
        
    def _create_compact_display(self) -> Layout:
        """Create compact display layout"""
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="progress", size=5),
            Layout(name="stats", size=7),
            Layout(name="resources", size=4)
        )
        
        # Header
        header = Panel(
            f"[bold bright_cyan]MetaMap Processing Monitor[/bold bright_cyan]\n"
            f"[dim]Output: {self.output_dir}[/dim]",
            box=box.ROUNDED
        )
        layout["header"].update(header)
        
        # Progress
        progress_content = self._create_progress_bars()
        layout["progress"].update(Panel(progress_content, title="Progress", box=box.ROUNDED))
        
        # Statistics
        stats_table = self._create_stats_table()
        layout["stats"].update(Panel(stats_table, title="Statistics", box=box.ROUNDED))
        
        # Resources
        resources = self.resource_monitor.get_compact_display()
        layout["resources"].update(Panel(resources, title="System Resources", box=box.ROUNDED))
        
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
        
        left["progress"].update(Panel(self._create_detailed_progress(), title="Processing Progress", box=box.ROUNDED))
        left["stats"].update(Panel(self._create_detailed_stats(), title="Detailed Statistics", box=box.ROUNDED))
        left["activity"].update(Panel(self._create_activity_log(), title="Recent Activity", box=box.ROUNDED))
        
        # Right side - resources and errors
        right = Layout()
        right.split_column(
            Layout(name="resources", size=12),
            Layout(name="errors", ratio=1)
        )
        
        right["resources"].update(self.resource_monitor.get_detailed_panel())
        right["errors"].update(Panel(self._create_error_list(), title="Recent Errors", box=box.ROUNDED))
        
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
        
        # Header with title and summary
        header = Panel(
            Align.center(
                f"[bold bright_cyan]PythonMetaMap Processing Dashboard[/bold bright_cyan]\n"
                f"[bright_yellow]Output Directory:[/bright_yellow] {self.output_dir}\n"
                f"[bright_green]Mode:[/bright_green] Real-time Monitoring"
            ),
            box=box.DOUBLE
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
        
        # Left column
        left = Layout()
        left.split_column(
            Layout(name="progress", size=10),
            Layout(name="performance", ratio=1)
        )
        left["progress"].update(Panel(self._create_detailed_progress(), title="📊 Progress", box=box.ROUNDED))
        left["performance"].update(Panel(self._create_performance_chart(), title="📈 Performance", box=box.ROUNDED))
        
        # Center column
        center = Layout()
        center.split_column(
            Layout(name="stats", size=12),
            Layout(name="activity", ratio=1)
        )
        center["stats"].update(Panel(self._create_detailed_stats(), title="📋 Statistics", box=box.ROUNDED))
        center["activity"].update(Panel(self._create_activity_log(), title="🔄 Activity Log", box=box.ROUNDED))
        
        # Right column
        right = Layout()
        right.split_column(
            Layout(name="resources", size=14),
            Layout(name="errors", ratio=1)
        )
        right["resources"].update(self.resource_monitor.get_detailed_panel())
        right["errors"].update(Panel(self._create_error_list(), title="⚠️ Errors", box=box.ROUNDED))
        
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
        elapsed = time.time() - self.stats["start_time"] if self.stats["start_time"] else 0
        elapsed_str = str(timedelta(seconds=int(elapsed)))
        
        content = f"[bold bright_cyan]MetaMap Processing Monitor[/bold bright_cyan] | "
        content += f"[bright_yellow]Elapsed:[/bright_yellow] {elapsed_str} | "
        
        if self.stats["eta"]:
            eta_str = self.stats["eta"].strftime("%H:%M:%S")
            content += f"[bright_green]ETA:[/bright_green] {eta_str}"
        else:
            content += "[dim]Calculating ETA...[/dim]"
            
        return Panel(content, box=box.ROUNDED)
        
    def _create_footer_panel(self) -> Panel:
        """Create footer panel with controls"""
        controls = "[bright_cyan]Q[/bright_cyan]uit | [bright_cyan]P[/bright_cyan]ause | [bright_cyan]R[/bright_cyan]esume | "
        controls += "[bright_cyan]M[/bright_cyan]ode | [bright_cyan]E[/bright_cyan]xport | [bright_cyan]H[/bright_cyan]elp"
        
        return Panel(controls, box=box.MINIMAL)
        
    def _create_progress_bars(self) -> str:
        """Create progress bars for compact display"""
        processed = self.stats["files_processed"]
        failed = self.stats["files_failed"]
        total = self.stats["total_files"]
        
        if total > 0:
            progress_percent = processed / total * 100
            failed_percent = failed / total * 100
            overall_percent = (processed + failed) / total * 100
        else:
            progress_percent = failed_percent = overall_percent = 0
            
        # Overall progress
        content = f"Overall: {self._create_bar(overall_percent, 30)} {overall_percent:>3.0f}%\n"
        content += f"Success: {self._create_bar(progress_percent, 30, 'green')} {processed}/{total}\n"
        content += f"Failed:  {self._create_bar(failed_percent, 30, 'red')} {failed}/{total}"
        
        return content
        
    def _create_bar(self, percent: float, width: int, color: str = 'cyan') -> str:
        """Create a progress bar"""
        filled = int(width * percent / 100)
        empty = width - filled
        return f"[{color}]{'█' * filled}[/{color}][dim]{'░' * empty}[/dim]"
        
    def _create_stats_table(self) -> Table:
        """Create statistics table"""
        table = Table(show_header=False, box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")
        
        table.add_row("Processing Rate", f"{self.stats['processing_rate']:.2f} files/s")
        table.add_row("Avg Time/File", f"{self.stats['avg_file_time']:.2f}s")
        table.add_row("Concepts Found", f"{self.stats['concepts_found']:,}")
        
        if self.stats["current_file"]:
            table.add_row("Current File", self.stats["current_file"][:40])
            
        return table
        
    def _create_detailed_progress(self) -> str:
        """Create detailed progress display"""
        processed = self.stats["files_processed"]
        failed = self.stats["files_failed"]
        total = self.stats["total_files"]
        remaining = total - processed - failed
        
        content = f"[bright_green]Processed:[/bright_green] {processed:>5} files\n"
        content += f"[bright_red]Failed:[/bright_red]    {failed:>5} files\n"
        content += f"[bright_yellow]Remaining:[/bright_yellow] {remaining:>5} files\n"
        content += f"[bright_cyan]Total:[/bright_cyan]     {total:>5} files\n\n"
        
        # Progress bar
        if total > 0:
            percent = (processed + failed) / total * 100
            content += self._create_bar(percent, 40) + f" {percent:>3.0f}%\n"
            
        # Current file
        if self.stats["current_file"]:
            content += f"\n[dim]Processing:[/dim] {self.stats['current_file']}"
            
        return content
        
    def _create_detailed_stats(self) -> Table:
        """Create detailed statistics table"""
        table = Table(show_header=True, box=box.SIMPLE)
        table.add_column("Metric", style="bright_cyan")
        table.add_column("Value", justify="right", style="bright_white")
        table.add_column("Details", style="dim")
        
        # Time stats
        elapsed = time.time() - self.stats["start_time"] if self.stats["start_time"] else 0
        elapsed_str = str(timedelta(seconds=int(elapsed)))
        table.add_row("Elapsed Time", elapsed_str, "Total processing time")
        
        if self.stats["eta"]:
            remaining = (self.stats["eta"] - datetime.now()).total_seconds()
            remaining_str = str(timedelta(seconds=int(max(0, remaining))))
            table.add_row("Time Remaining", remaining_str, "Estimated time to complete")
        
        # Processing stats
        table.add_row("Processing Rate", f"{self.stats['processing_rate']:.2f}/s", "Files per second")
        table.add_row("Avg Time/File", f"{self.stats['avg_file_time']:.2f}s", "Average processing time")
        
        # Results
        table.add_row("Concepts Found", f"{self.stats['concepts_found']:,}", "Total medical concepts")
        
        if self.stats['files_processed'] > 0:
            avg_concepts = self.stats['concepts_found'] / self.stats['files_processed']
            table.add_row("Avg Concepts/File", f"{avg_concepts:.1f}", "Average per file")
            
        return table
        
    def _create_activity_log(self) -> str:
        """Create activity log display"""
        if not self.activity_log:
            return "[dim]No recent activity[/dim]"
            
        # Show last 10 activities
        content = ""
        for activity in list(self.activity_log)[-10:]:
            content += f"{activity}\n"
            
        return content.strip()
        
    def _create_error_list(self) -> str:
        """Create error list display"""
        if not self.recent_errors:
            return "[green]No errors[/green]"
            
        content = ""
        for i, error in enumerate(list(self.recent_errors)[-5:], 1):
            content += f"[red]{i}.[/red] {error['file']}\n"
            content += f"   [dim]{error['error'][:60]}...[/dim]\n"
            
        return content.strip()
        
    def _create_overview_panel(self) -> Panel:
        """Create overview panel for dashboard"""
        # Calculate percentages
        processed = self.stats["files_processed"]
        failed = self.stats["files_failed"]
        total = self.stats["total_files"]
        
        success_rate = (processed / (processed + failed) * 100) if (processed + failed) > 0 else 0
        completion = ((processed + failed) / total * 100) if total > 0 else 0
        
        # Create metric cards
        cards = []
        
        # Completion card
        completion_color = "green" if completion > 80 else "yellow" if completion > 50 else "red"
        cards.append(Panel(
            f"[bold {completion_color}]{completion:.1f}%[/bold {completion_color}]\n[dim]Complete[/dim]",
            box=box.ROUNDED,
            width=15
        ))
        
        # Success rate card
        success_color = "green" if success_rate > 95 else "yellow" if success_rate > 90 else "red"
        cards.append(Panel(
            f"[bold {success_color}]{success_rate:.1f}%[/bold {success_color}]\n[dim]Success[/dim]",
            box=box.ROUNDED,
            width=15
        ))
        
        # Files/sec card
        rate_color = "green" if self.stats["processing_rate"] > 2 else "yellow" if self.stats["processing_rate"] > 1 else "red"
        cards.append(Panel(
            f"[bold {rate_color}]{self.stats['processing_rate']:.2f}[/bold {rate_color}]\n[dim]Files/sec[/dim]",
            box=box.ROUNDED,
            width=15
        ))
        
        # Concepts card
        cards.append(Panel(
            f"[bold bright_cyan]{self.stats['concepts_found']:,}[/bold bright_cyan]\n[dim]Concepts[/dim]",
            box=box.ROUNDED,
            width=15
        ))
        
        # ETA card
        if self.stats["eta"]:
            eta_str = self.stats["eta"].strftime("%H:%M")
            cards.append(Panel(
                f"[bold bright_yellow]{eta_str}[/bold bright_yellow]\n[dim]ETA[/dim]",
                box=box.ROUNDED,
                width=15
            ))
        
        return Panel(Columns(cards, equal=True, expand=False), title="Overview", box=box.ROUNDED)
        
    def _create_performance_chart(self) -> str:
        """Create performance chart"""
        if not self.performance_history:
            return "[dim]Collecting performance data...[/dim]"
            
        # Simple ASCII chart
        chart_height = 8
        chart_width = 40
        
        # Get last N samples
        samples = list(self.performance_history)[-chart_width:]
        if not samples:
            return "[dim]No data[/dim]"
            
        # Normalize to chart height
        max_val = max(samples) if samples else 1
        min_val = min(samples) if samples else 0
        range_val = max_val - min_val or 1
        
        # Create chart
        chart = []
        for y in range(chart_height, 0, -1):
            line = ""
            threshold = min_val + (range_val * y / chart_height)
            
            for x, val in enumerate(samples):
                if val >= threshold:
                    line += "█"
                else:
                    line += " "
                    
            chart.append(f"{line} {threshold:.1f}")
            
        return "\n".join(chart)
        
    def export_report(self, output_path: Optional[Path] = None) -> Path:
        """Export monitoring report"""
        if not output_path:
            output_path = self.output_dir / f"monitor_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
        report = {
            "output_directory": str(self.output_dir),
            "start_time": datetime.fromtimestamp(self.stats["start_time"]).isoformat() if self.stats["start_time"] else None,
            "end_time": datetime.now().isoformat(),
            "statistics": self.stats,
            "errors": list(self.recent_errors),
            "performance_metrics": {
                "avg_processing_time": self.stats["avg_file_time"],
                "processing_rate": self.stats["processing_rate"],
                "success_rate": (self.stats["files_processed"] / (self.stats["files_processed"] + self.stats["files_failed"]) * 100) if (self.stats["files_processed"] + self.stats["files_failed"]) > 0 else 0
            },
            "resource_usage": self.resource_monitor.get_summary()
        }
        
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
        
    def start(self):
        """Start resource monitoring"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
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
        """Get compact resource display"""
        cpu = self.cpu_history[-1] if self.cpu_history else 0
        mem = self.memory_history[-1] if self.memory_history else 0
        disk = self.disk_history[-1] if self.disk_history else 0
        
        content = f"CPU:  {self._create_mini_bar(cpu)} {cpu:>3.0f}%\n"
        content += f"RAM:  {self._create_mini_bar(mem)} {mem:>3.0f}%\n"
        content += f"Disk: {self._create_mini_bar(disk)} {disk:>3.0f}%"
        
        return content
        
    def get_detailed_panel(self) -> Panel:
        """Get detailed resource panel"""
        # Current values
        cpu = self.cpu_history[-1] if self.cpu_history else 0
        mem = self.memory_history[-1] if self.memory_history else 0
        disk = self.disk_history[-1] if self.disk_history else 0
        
        # Memory details
        mem_info = psutil.virtual_memory()
        mem_used_gb = mem_info.used / (1024**3)
        mem_total_gb = mem_info.total / (1024**3)
        
        # Create content
        content = f"[bold]CPU Usage[/bold]\n"
        content += f"{self._create_bar(cpu, 25)} {cpu:>3.0f}%\n"
        content += f"[dim]Cores: {psutil.cpu_count()}[/dim]\n\n"
        
        content += f"[bold]Memory Usage[/bold]\n"
        content += f"{self._create_bar(mem, 25)} {mem:>3.0f}%\n"
        content += f"[dim]{mem_used_gb:.1f} / {mem_total_gb:.1f} GB[/dim]\n\n"
        
        content += f"[bold]Disk Usage[/bold]\n"
        content += f"{self._create_bar(disk, 25)} {disk:>3.0f}%"
        
        return Panel(content, title="📊 System Resources", box=box.ROUNDED)
        
    def _create_mini_bar(self, percent: float, width: int = 10) -> str:
        """Create mini progress bar"""
        filled = int(width * percent / 100)
        empty = width - filled
        
        if percent > 80:
            color = "red"
        elif percent > 60:
            color = "yellow"
        else:
            color = "green"
            
        return f"[{color}]{'█' * filled}[/{color}][dim]{'░' * empty}[/dim]"
        
    def _create_bar(self, percent: float, width: int) -> str:
        """Create progress bar"""
        filled = int(width * percent / 100)
        empty = width - filled
        
        if percent > 80:
            color = "red"
        elif percent > 60:
            color = "yellow"
        else:
            color = "green"
            
        return f"[{color}]{'█' * filled}[/{color}][dim]{'░' * empty}[/dim]"
        
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
def monitor_output(output_dir: str, mode: str = MonitoringMode.AUTO, refresh_rate: float = 1.0) -> None:
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
        console.print("\n[yellow]Monitoring stopped by user[/yellow]")
    finally:
        monitor.stop()
        
        # Export final report
        report_path = monitor.export_report()
        console.print(f"\n[green]Report saved to: {report_path}[/green]")