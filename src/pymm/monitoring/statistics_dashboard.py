"""Global statistics dashboard with real-time processing counters"""
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Deque, Any
from dataclasses import dataclass, field
from collections import deque, defaultdict
import json
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.layout import Layout
from rich.columns import Columns
from rich import box
from rich.align import Align
from rich.live import Live

console = Console()


@dataclass
class ProcessingStats:
    """Global processing statistics"""
    # Counters
    total_files_processed: int = 0
    total_files_failed: int = 0
    total_files_skipped: int = 0
    total_concepts_extracted: int = 0
    total_unique_concepts: int = 0
    total_processing_time: float = 0.0
    
    # Current session
    session_start_time: datetime = field(default_factory=datetime.now)
    session_files_processed: int = 0
    session_concepts_extracted: int = 0
    
    # Performance metrics
    avg_file_processing_time: float = 0.0
    avg_concepts_per_file: float = 0.0
    files_per_minute: float = 0.0
    concepts_per_second: float = 0.0
    
    # Time-based metrics
    files_by_hour: Dict[int, int] = field(default_factory=dict)
    files_by_day: Dict[str, int] = field(default_factory=dict)
    processing_time_history: Deque[float] = field(default_factory=lambda: deque(maxlen=1000))
    

@dataclass
class BatchStats:
    """Statistics for a single batch"""
    batch_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_files: int = 0
    processed_files: int = 0
    failed_files: int = 0
    concepts_extracted: int = 0
    processing_times: List[float] = field(default_factory=list)
    error_types: Dict[str, int] = field(default_factory=dict)
    

class StatisticsDashboard:
    """Comprehensive statistics dashboard for monitoring"""
    
    def __init__(self):
        self._lock = threading.Lock()
        
        # Global statistics
        self.global_stats = ProcessingStats()
        
        # Batch tracking
        self.active_batches: Dict[str, BatchStats] = {}
        self.completed_batches: List[BatchStats] = []
        
        # Real-time metrics (per second)
        self.realtime_files_processed = deque(maxlen=60)  # Last 60 seconds
        self.realtime_concepts_extracted = deque(maxlen=60)
        self.realtime_errors = deque(maxlen=60)
        
        # Concept tracking
        self.concept_frequency = defaultdict(int)
        self.semantic_type_frequency = defaultdict(int)
        self.recent_concepts = deque(maxlen=100)
        
        # Error tracking
        self.error_counts = defaultdict(int)
        self.recent_errors = deque(maxlen=50)
        
        # Performance tracking
        self.server_response_times = deque(maxlen=100)
        self.file_sizes = deque(maxlen=1000)
        
        # Milestones
        self.milestones = []
        self._check_milestones()
        
        # Start metrics calculation
        self._running = True
        self._metrics_thread = threading.Thread(target=self._calculate_metrics, daemon=True)
        self._metrics_thread.start()
    
    def update_file_processed(self, batch_id: str, filename: str, 
                            processing_time: float, concepts_count: int,
                            file_size: int = 0):
        """Update statistics for a successfully processed file"""
        with self._lock:
            # Update global stats
            self.global_stats.total_files_processed += 1
            self.global_stats.session_files_processed += 1
            self.global_stats.total_concepts_extracted += concepts_count
            self.global_stats.session_concepts_extracted += concepts_count
            self.global_stats.total_processing_time += processing_time
            self.global_stats.processing_time_history.append(processing_time)
            
            # Update time-based metrics
            hour = datetime.now().hour
            day = datetime.now().strftime("%Y-%m-%d")
            self.global_stats.files_by_hour[hour] = self.global_stats.files_by_hour.get(hour, 0) + 1
            self.global_stats.files_by_day[day] = self.global_stats.files_by_day.get(day, 0) + 1
            
            # Update batch stats
            if batch_id in self.active_batches:
                batch = self.active_batches[batch_id]
                batch.processed_files += 1
                batch.concepts_extracted += concepts_count
                batch.processing_times.append(processing_time)
            
            # Track file size
            if file_size > 0:
                self.file_sizes.append(file_size)
            
            # Check milestones
            self._check_milestones()
    
    def update_file_failed(self, batch_id: str, filename: str, error: str):
        """Update statistics for a failed file"""
        with self._lock:
            # Update global stats
            self.global_stats.total_files_failed += 1
            
            # Update batch stats
            if batch_id in self.active_batches:
                batch = self.active_batches[batch_id]
                batch.failed_files += 1
                
                # Categorize error
                error_type = self._categorize_error(error)
                batch.error_types[error_type] = batch.error_types.get(error_type, 0) + 1
            
            # Track errors
            self.error_counts[error] += 1
            self.recent_errors.append({
                'timestamp': datetime.now(),
                'filename': filename,
                'error': error
            })
    
    def update_concept(self, concept: str, semantic_type: str = None):
        """Update concept statistics"""
        with self._lock:
            self.concept_frequency[concept] += 1
            if semantic_type:
                self.semantic_type_frequency[semantic_type] += 1
            
            self.recent_concepts.append({
                'concept': concept,
                'semantic_type': semantic_type,
                'timestamp': datetime.now()
            })
            
            # Update unique concepts count
            self.global_stats.total_unique_concepts = len(self.concept_frequency)
    
    def start_batch(self, batch_id: str, total_files: int):
        """Start tracking a new batch"""
        with self._lock:
            batch = BatchStats(
                batch_id=batch_id,
                start_time=datetime.now(),
                total_files=total_files
            )
            self.active_batches[batch_id] = batch
    
    def complete_batch(self, batch_id: str):
        """Mark a batch as completed"""
        with self._lock:
            if batch_id in self.active_batches:
                batch = self.active_batches[batch_id]
                batch.end_time = datetime.now()
                self.completed_batches.append(batch)
                del self.active_batches[batch_id]
    
    def _calculate_metrics(self):
        """Background thread to calculate real-time metrics"""
        last_files = self.global_stats.total_files_processed
        last_concepts = self.global_stats.total_concepts_extracted
        last_errors = self.global_stats.total_files_failed
        
        while self._running:
            try:
                time.sleep(1)  # Update every second
                
                with self._lock:
                    # Calculate rates
                    files_delta = self.global_stats.total_files_processed - last_files
                    concepts_delta = self.global_stats.total_concepts_extracted - last_concepts
                    errors_delta = self.global_stats.total_files_failed - last_errors
                    
                    # Update realtime metrics
                    self.realtime_files_processed.append(files_delta)
                    self.realtime_concepts_extracted.append(concepts_delta)
                    self.realtime_errors.append(errors_delta)
                    
                    # Update last values
                    last_files = self.global_stats.total_files_processed
                    last_concepts = self.global_stats.total_concepts_extracted
                    last_errors = self.global_stats.total_files_failed
                    
                    # Calculate averages
                    if self.global_stats.total_files_processed > 0:
                        self.global_stats.avg_file_processing_time = (
                            self.global_stats.total_processing_time / 
                            self.global_stats.total_files_processed
                        )
                        self.global_stats.avg_concepts_per_file = (
                            self.global_stats.total_concepts_extracted / 
                            self.global_stats.total_files_processed
                        )
                    
                    # Calculate rates
                    if len(self.realtime_files_processed) > 0:
                        self.global_stats.files_per_minute = sum(self.realtime_files_processed) * 60
                    
                    if len(self.realtime_concepts_extracted) > 0:
                        self.global_stats.concepts_per_second = sum(self.realtime_concepts_extracted) / len(self.realtime_concepts_extracted)
                        
            except Exception as e:
                console.print(f"[error]Metrics calculation error: {e}[/error]")
    
    def _categorize_error(self, error: str) -> str:
        """Categorize error messages"""
        error_lower = error.lower()
        if 'timeout' in error_lower:
            return 'Timeout'
        elif 'connection' in error_lower or 'network' in error_lower:
            return 'Network'
        elif 'memory' in error_lower:
            return 'Memory'
        elif 'permission' in error_lower or 'access' in error_lower:
            return 'Permission'
        elif 'file not found' in error_lower:
            return 'File Not Found'
        else:
            return 'Other'
    
    def _check_milestones(self):
        """Check and record milestones"""
        milestones_thresholds = [100, 500, 1000, 5000, 10000, 50000, 100000]
        
        for threshold in milestones_thresholds:
            if (self.global_stats.total_files_processed >= threshold and 
                not any(m['threshold'] == threshold for m in self.milestones)):
                self.milestones.append({
                    'threshold': threshold,
                    'timestamp': datetime.now(),
                    'type': 'files_processed'
                })
    
    def get_display(self) -> Layout:
        """Get rich Layout for statistics display"""
        layout = Layout()
        
        # Create sections
        overview_section = self._create_overview_section()
        performance_section = self._create_performance_section()
        activity_section = self._create_activity_section()
        errors_section = self._create_errors_section()
        
        # Arrange layout
        layout.split_column(
            Layout(overview_section, size=10),
            Layout(name="middle", size=12),
            Layout(name="bottom", size=8)
        )
        
        layout["middle"].split_row(
            Layout(performance_section),
            Layout(activity_section)
        )
        
        layout["bottom"] = errors_section
        
        return layout
    
    def _create_overview_section(self) -> Panel:
        """Create overview statistics panel"""
        with self._lock:
            # Calculate session duration
            session_duration = datetime.now() - self.global_stats.session_start_time
            
            # Create cards
            cards = []
            
            # Total files card
            total_card = Panel(
                Align.center(
                    Text(f"{self.global_stats.total_files_processed:,}\n", style="bold cyan") +
                    Text("Total Files", style="dim")
                ),
                box=box.ROUNDED,
                style="cyan"
            )
            cards.append(total_card)
            
            # Success rate card
            total_attempted = (self.global_stats.total_files_processed + 
                             self.global_stats.total_files_failed)
            success_rate = (self.global_stats.total_files_processed / total_attempted * 100 
                          if total_attempted > 0 else 0)
            
            success_card = Panel(
                Align.center(
                    Text(f"{success_rate:.1f}%\n", style="bold green") +
                    Text("Success Rate", style="dim")
                ),
                box=box.ROUNDED,
                style="green"
            )
            cards.append(success_card)
            
            # Concepts card
            concepts_card = Panel(
                Align.center(
                    Text(f"{self.global_stats.total_concepts_extracted:,}\n", style="bold magenta") +
                    Text("Total Concepts", style="dim")
                ),
                box=box.ROUNDED,
                style="magenta"
            )
            cards.append(concepts_card)
            
            # Files/minute card
            fpm_card = Panel(
                Align.center(
                    Text(f"{self.global_stats.files_per_minute:.1f}\n", style="bold yellow") +
                    Text("Files/Minute", style="dim")
                ),
                box=box.ROUNDED,
                style="yellow"
            )
            cards.append(fpm_card)
            
            # Session time card
            session_card = Panel(
                Align.center(
                    Text(f"{str(session_duration).split('.')[0]}\n", style="bold blue") +
                    Text("Session Time", style="dim")
                ),
                box=box.ROUNDED,
                style="blue"
            )
            cards.append(session_card)
            
            overview = Columns(cards, equal=True, expand=True)
            
            return Panel(overview, title="Processing Overview", box=box.ROUNDED, style="bold")
    
    def _create_performance_section(self) -> Panel:
        """Create performance metrics panel"""
        with self._lock:
            table = Table(box=box.SIMPLE, show_header=False)
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="yellow")
            
            # Add metrics
            table.add_row("Avg Processing Time", f"{self.global_stats.avg_file_processing_time:.2f}s")
            table.add_row("Avg Concepts/File", f"{self.global_stats.avg_concepts_per_file:.1f}")
            table.add_row("Concepts/Second", f"{self.global_stats.concepts_per_second:.1f}")
            
            # Active batches
            active_count = len(self.active_batches)
            table.add_row("Active Batches", str(active_count))
            
            # Calculate throughput trend
            if len(self.realtime_files_processed) >= 2:
                recent = sum(list(self.realtime_files_processed)[-10:])
                older = sum(list(self.realtime_files_processed)[-20:-10])
                if older > 0:
                    trend = ((recent - older) / older) * 100
                    trend_text = f"{'↑' if trend > 0 else '↓'} {abs(trend):.1f}%"
                    table.add_row("Throughput Trend", trend_text)
            
            return Panel(table, title="Performance Metrics", box=box.ROUNDED)
    
    def _create_activity_section(self) -> Panel:
        """Create recent activity panel"""
        with self._lock:
            content = Text()
            
            # Recent concepts
            content.append("Recent Concepts\n", style="bold cyan")
            recent = list(self.recent_concepts)[-5:]
            for item in reversed(recent):
                content.append(f"  • {item['concept'][:30]}", style="yellow")
                if item['semantic_type']:
                    content.append(f" [{item['semantic_type']}]", style="dim")
                content.append("\n")
            
            # Top concepts today
            content.append("\nTop Concepts (Session)\n", style="bold cyan")
            top_concepts = sorted(
                self.concept_frequency.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
            
            for concept, count in top_concepts:
                content.append(f"  • {concept[:25]:25} {count:4}\n")
            
            return Panel(content, title="Activity Feed", box=box.ROUNDED)
    
    def _create_errors_section(self) -> Panel:
        """Create errors summary panel"""
        with self._lock:
            if not self.recent_errors:
                return Panel("No errors recorded", title="Errors", box=box.ROUNDED, style="green")
            
            table = Table(box=box.SIMPLE)
            table.add_column("Time", style="dim", width=8)
            table.add_column("File", style="cyan", width=30)
            table.add_column("Error", style="red", width=40)
            
            for error_info in list(self.recent_errors)[-5:]:
                time_str = error_info['timestamp'].strftime("%H:%M:%S")
                filename = Path(error_info['filename']).name[:30]
                error_msg = error_info['error'][:40]
                
                table.add_row(time_str, filename, error_msg)
            
            # Add error summary
            error_summary = Text()
            error_summary.append(f"\nTotal Errors: {self.global_stats.total_files_failed}", style="red")
            
            return Panel(
                Group(table, error_summary),
                title=f"Recent Errors ({len(self.recent_errors)} total)",
                box=box.ROUNDED,
                style="red"
            )
    
    def export_statistics(self, filepath: Path):
        """Export comprehensive statistics"""
        with self._lock:
            stats = {
                'timestamp': datetime.now().isoformat(),
                'global_stats': {
                    'total_files_processed': self.global_stats.total_files_processed,
                    'total_files_failed': self.global_stats.total_files_failed,
                    'total_concepts_extracted': self.global_stats.total_concepts_extracted,
                    'total_unique_concepts': self.global_stats.total_unique_concepts,
                    'avg_file_processing_time': self.global_stats.avg_file_processing_time,
                    'avg_concepts_per_file': self.global_stats.avg_concepts_per_file,
                    'files_per_minute': self.global_stats.files_per_minute,
                    'session_duration': str(datetime.now() - self.global_stats.session_start_time)
                },
                'batches': {
                    'active': len(self.active_batches),
                    'completed': len(self.completed_batches)
                },
                'top_concepts': dict(sorted(
                    self.concept_frequency.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:100]),
                'error_summary': dict(self.error_counts),
                'milestones': self.milestones
            }
            
            with open(filepath, 'w') as f:
                json.dump(stats, f, indent=2)
    
    def stop(self):
        """Stop the dashboard"""
        self._running = False
        if self._metrics_thread.is_alive():
            self._metrics_thread.join(timeout=2)