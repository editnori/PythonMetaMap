"""Real-time progress tracking system with live updates"""
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from collections import deque, defaultdict
from pathlib import Path
import json

from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn, MofNCompleteColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.console import Console
from rich.text import Text
from rich import box

console = Console()


@dataclass
class FileProgress:
    """Track individual file processing progress"""
    filename: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "pending"  # pending, processing, completed, failed
    progress: float = 0.0
    concepts_found: int = 0
    error_message: Optional[str] = None
    retry_count: int = 0
    file_size: int = 0
    processing_stage: str = ""  # reading, metamap, writing, etc.


@dataclass
class BatchProgress:
    """Track overall batch processing progress"""
    batch_id: str
    total_files: int
    start_time: datetime
    end_time: Optional[datetime] = None
    files: Dict[str, FileProgress] = field(default_factory=dict)
    completed_files: int = 0
    failed_files: int = 0
    active_files: List[str] = field(default_factory=list)
    throughput_history: deque = field(default_factory=lambda: deque(maxlen=60))
    

class RealtimeProgressTracker:
    """Enhanced progress tracking with real-time updates"""
    
    def __init__(self, update_callback: Optional[Callable] = None):
        self.batches: Dict[str, BatchProgress] = {}
        self.current_batch: Optional[str] = None
        self.update_callback = update_callback
        self._lock = threading.Lock()
        
        # Global statistics
        self.total_processed = 0
        self.total_failed = 0
        self.total_concepts = 0
        self.processing_times = deque(maxlen=1000)
        
        # Real-time metrics
        self.files_per_second = 0.0
        self.avg_file_time = 0.0
        self.estimated_remaining = timedelta(0)
        
        # Stage-specific metrics
        self.stage_times = defaultdict(list)
        
        # Start metrics calculation thread
        self._running = True
        self._metrics_thread = threading.Thread(target=self._calculate_metrics, daemon=True)
        self._metrics_thread.start()
    
    def create_batch(self, batch_id: str, total_files: int) -> str:
        """Create a new batch tracking session"""
        with self._lock:
            batch = BatchProgress(
                batch_id=batch_id,
                total_files=total_files,
                start_time=datetime.now()
            )
            self.batches[batch_id] = batch
            self.current_batch = batch_id
            self._notify_update("batch_created", batch_id)
            return batch_id
    
    def start_file(self, filename: str, file_size: int = 0, batch_id: Optional[str] = None):
        """Mark a file as started"""
        batch_id = batch_id or self.current_batch
        if not batch_id:
            return
            
        with self._lock:
            batch = self.batches.get(batch_id)
            if batch:
                file_progress = FileProgress(
                    filename=filename,
                    start_time=datetime.now(),
                    status="processing",
                    file_size=file_size
                )
                batch.files[filename] = file_progress
                batch.active_files.append(filename)
                self._notify_update("file_started", {"batch": batch_id, "file": filename})
    
    def update_file_stage(self, filename: str, stage: str, progress: float = None, batch_id: Optional[str] = None):
        """Update file processing stage"""
        batch_id = batch_id or self.current_batch
        if not batch_id:
            return
            
        with self._lock:
            batch = self.batches.get(batch_id)
            if batch and filename in batch.files:
                file_progress = batch.files[filename]
                file_progress.processing_stage = stage
                if progress is not None:
                    file_progress.progress = progress
                self._notify_update("file_stage_updated", {
                    "batch": batch_id, 
                    "file": filename, 
                    "stage": stage,
                    "progress": progress
                })
    
    def complete_file(self, filename: str, concepts_found: int = 0, batch_id: Optional[str] = None):
        """Mark a file as completed"""
        batch_id = batch_id or self.current_batch
        if not batch_id:
            return
            
        with self._lock:
            batch = self.batches.get(batch_id)
            if batch and filename in batch.files:
                file_progress = batch.files[filename]
                file_progress.end_time = datetime.now()
                file_progress.status = "completed"
                file_progress.concepts_found = concepts_found
                file_progress.progress = 100.0
                
                # Update batch stats
                batch.completed_files += 1
                if filename in batch.active_files:
                    batch.active_files.remove(filename)
                
                # Update global stats
                self.total_processed += 1
                self.total_concepts += concepts_found
                
                # Track processing time
                processing_time = (file_progress.end_time - file_progress.start_time).total_seconds()
                self.processing_times.append(processing_time)
                
                self._notify_update("file_completed", {
                    "batch": batch_id,
                    "file": filename,
                    "concepts": concepts_found,
                    "time": processing_time
                })
    
    def fail_file(self, filename: str, error: str, batch_id: Optional[str] = None):
        """Mark a file as failed"""
        batch_id = batch_id or self.current_batch
        if not batch_id:
            return
            
        with self._lock:
            batch = self.batches.get(batch_id)
            if batch and filename in batch.files:
                file_progress = batch.files[filename]
                file_progress.end_time = datetime.now()
                file_progress.status = "failed"
                file_progress.error_message = error
                
                # Update batch stats
                batch.failed_files += 1
                if filename in batch.active_files:
                    batch.active_files.remove(filename)
                
                # Update global stats
                self.total_failed += 1
                
                self._notify_update("file_failed", {
                    "batch": batch_id,
                    "file": filename,
                    "error": error
                })
    
    def get_progress_display(self) -> Layout:
        """Get rich Layout for progress display"""
        layout = Layout()
        
        # Main progress section
        progress_section = self._create_progress_section()
        
        # Active files section
        active_section = self._create_active_files_section()
        
        # Statistics section
        stats_section = self._create_statistics_section()
        
        # Arrange layout
        layout.split_column(
            Layout(progress_section, size=8),
            Layout(active_section, size=10),
            Layout(stats_section, size=8)
        )
        
        return layout
    
    def _create_progress_section(self) -> Panel:
        """Create overall progress display"""
        if not self.current_batch or self.current_batch not in self.batches:
            return Panel("No active batch", title="Progress")
            
        batch = self.batches[self.current_batch]
        
        # Create progress bar
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console,
            expand=True
        )
        
        task = progress.add_task(
            f"Batch {batch.batch_id}",
            total=batch.total_files,
            completed=batch.completed_files + batch.failed_files
        )
        
        # Add sub-tasks for different stages
        if batch.active_files:
            for filename in batch.active_files[:3]:  # Show top 3 active files
                file_prog = batch.files.get(filename)
                if file_prog:
                    progress.add_task(
                        f"  {Path(filename).name} - {file_prog.processing_stage}",
                        total=100,
                        completed=int(file_prog.progress)
                    )
        
        return Panel(progress, title="Overall Progress", box=box.ROUNDED)
    
    def _create_active_files_section(self) -> Panel:
        """Create active files display"""
        if not self.current_batch or self.current_batch not in self.batches:
            return Panel("No active files", title="Active Files")
            
        batch = self.batches[self.current_batch]
        
        table = Table(box=box.SIMPLE)
        table.add_column("File", style="cyan", width=40)
        table.add_column("Stage", style="yellow", width=15)
        table.add_column("Progress", style="green", width=10)
        table.add_column("Duration", style="magenta", width=10)
        
        for filename in batch.active_files:
            file_prog = batch.files.get(filename)
            if file_prog:
                duration = (datetime.now() - file_prog.start_time).total_seconds()
                table.add_row(
                    Path(filename).name,
                    file_prog.processing_stage,
                    f"{file_prog.progress:.1f}%",
                    f"{duration:.1f}s"
                )
        
        return Panel(table, title=f"Active Files ({len(batch.active_files)})", box=box.ROUNDED)
    
    def _create_statistics_section(self) -> Panel:
        """Create statistics display"""
        stats_text = Text()
        
        # Global stats
        stats_text.append("Global Statistics\n", style="bold cyan")
        stats_text.append(f"Total Processed: {self.total_processed}\n")
        stats_text.append(f"Total Failed: {self.total_failed}\n")
        stats_text.append(f"Total Concepts: {self.total_concepts:,}\n")
        stats_text.append(f"Files/Second: {self.files_per_second:.2f}\n")
        stats_text.append(f"Avg Time/File: {self.avg_file_time:.2f}s\n")
        
        # Current batch stats
        if self.current_batch and self.current_batch in self.batches:
            batch = self.batches[self.current_batch]
            stats_text.append(f"\nCurrent Batch\n", style="bold cyan")
            stats_text.append(f"Progress: {batch.completed_files}/{batch.total_files} ")
            stats_text.append(f"({batch.completed_files/batch.total_files*100:.1f}%)\n")
            stats_text.append(f"Failed: {batch.failed_files}\n")
            
            # Time estimates
            elapsed = (datetime.now() - batch.start_time).total_seconds()
            if batch.completed_files > 0:
                rate = batch.completed_files / elapsed
                remaining_files = batch.total_files - batch.completed_files - batch.failed_files
                eta = remaining_files / rate if rate > 0 else 0
                stats_text.append(f"ETA: {timedelta(seconds=int(eta))}\n")
        
        return Panel(stats_text, title="Statistics", box=box.ROUNDED)
    
    def _calculate_metrics(self):
        """Background thread to calculate real-time metrics"""
        while self._running:
            try:
                with self._lock:
                    # Calculate files per second
                    if self.processing_times:
                        recent_times = list(self.processing_times)[-60:]  # Last minute
                        self.avg_file_time = sum(recent_times) / len(recent_times)
                        self.files_per_second = 1.0 / self.avg_file_time if self.avg_file_time > 0 else 0
                    
                    # Update throughput for current batch
                    if self.current_batch and self.current_batch in self.batches:
                        batch = self.batches[self.current_batch]
                        batch.throughput_history.append(self.files_per_second)
                
                time.sleep(1)  # Update every second
            except Exception as e:
                console.print(f"[error]Metrics calculation error: {e}[/error]")
    
    def _notify_update(self, event_type: str, data: Any):
        """Notify callback of updates"""
        if self.update_callback:
            try:
                self.update_callback(event_type, data)
            except Exception as e:
                console.print(f"[error]Update callback error: {e}[/error]")
    
    def stop(self):
        """Stop the progress tracker"""
        self._running = False
        if self._metrics_thread.is_alive():
            self._metrics_thread.join(timeout=2)
    
    def export_metrics(self, filepath: Path):
        """Export metrics to JSON file"""
        with self._lock:
            metrics = {
                "batches": {},
                "global_stats": {
                    "total_processed": self.total_processed,
                    "total_failed": self.total_failed,
                    "total_concepts": self.total_concepts,
                    "avg_file_time": self.avg_file_time,
                    "files_per_second": self.files_per_second
                }
            }
            
            for batch_id, batch in self.batches.items():
                metrics["batches"][batch_id] = {
                    "total_files": batch.total_files,
                    "completed_files": batch.completed_files,
                    "failed_files": batch.failed_files,
                    "start_time": batch.start_time.isoformat(),
                    "end_time": batch.end_time.isoformat() if batch.end_time else None,
                    "files": {
                        filename: {
                            "status": fp.status,
                            "concepts_found": fp.concepts_found,
                            "error": fp.error_message,
                            "processing_time": (fp.end_time - fp.start_time).total_seconds() if fp.end_time else None
                        }
                        for filename, fp in batch.files.items()
                    }
                }
            
            with open(filepath, 'w') as f:
                json.dump(metrics, f, indent=2)