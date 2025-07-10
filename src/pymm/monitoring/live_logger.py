"""Live logging system with filtering and search capabilities"""
import threading
import time
from datetime import datetime
from typing import List, Optional, Dict, Set, Callable
from dataclasses import dataclass
from collections import deque
from pathlib import Path
import re
import json

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.syntax import Syntax
from rich import box
from rich.live import Live
from rich.layout import Layout

console = Console()


@dataclass
class LogEntry:
    """Single log entry"""
    timestamp: datetime
    level: str  # DEBUG, INFO, WARNING, ERROR, CRITICAL
    source: str  # Component that generated the log
    message: str
    file_context: Optional[str] = None  # File being processed
    extra_data: Optional[Dict] = None
    
    def matches_filter(self, filters: Dict[str, any]) -> bool:
        """Check if entry matches filters"""
        # Level filter
        if 'levels' in filters and self.level not in filters['levels']:
            return False
            
        # Source filter
        if 'sources' in filters and self.source not in filters['sources']:
            return False
            
        # Text search
        if 'search' in filters and filters['search']:
            search_text = filters['search'].lower()
            if search_text not in self.message.lower():
                # Also check extra data
                if self.extra_data:
                    extra_str = json.dumps(self.extra_data).lower()
                    if search_text not in extra_str:
                        return False
                else:
                    return False
        
        # Regex search
        if 'regex' in filters and filters['regex']:
            try:
                if not re.search(filters['regex'], self.message, re.IGNORECASE):
                    return False
            except re.error:
                pass
                
        # Time range filter
        if 'start_time' in filters and self.timestamp < filters['start_time']:
            return False
        if 'end_time' in filters and self.timestamp > filters['end_time']:
            return False
            
        return True


class LiveLogger:
    """Live logging system with real-time display and filtering"""
    
    def __init__(self, max_entries: int = 10000, display_lines: int = 20):
        self.max_entries = max_entries
        self.display_lines = display_lines
        
        # Log storage
        self.entries = deque(maxlen=max_entries)
        self.filtered_entries: List[LogEntry] = []
        
        # Filters
        self.filters = {
            'levels': {'INFO', 'WARNING', 'ERROR', 'CRITICAL'},  # Default: hide DEBUG
            'sources': set(),  # Empty = all sources
            'search': '',
            'regex': None,
            'start_time': None,
            'end_time': None
        }
        
        # Statistics
        self.level_counts = {
            'DEBUG': 0,
            'INFO': 0,
            'WARNING': 0,
            'ERROR': 0,
            'CRITICAL': 0
        }
        self.source_counts = {}
        
        # Threading
        self._lock = threading.Lock()
        self.update_callback: Optional[Callable] = None
        
        # Auto-refresh
        self.auto_refresh = True
        self.refresh_interval = 0.5
        
    def add_entry(self, level: str, source: str, message: str, 
                  file_context: Optional[str] = None, extra_data: Optional[Dict] = None):
        """Add a new log entry"""
        with self._lock:
            entry = LogEntry(
                timestamp=datetime.now(),
                level=level.upper(),
                source=source,
                message=message,
                file_context=file_context,
                extra_data=extra_data
            )
            
            self.entries.append(entry)
            
            # Update statistics
            if level.upper() in self.level_counts:
                self.level_counts[level.upper()] += 1
            
            if source not in self.source_counts:
                self.source_counts[source] = 0
            self.source_counts[source] += 1
            
            # Update filtered entries if matches
            if entry.matches_filter(self.filters):
                self.filtered_entries.append(entry)
                # Keep filtered list bounded
                if len(self.filtered_entries) > self.max_entries:
                    self.filtered_entries = self.filtered_entries[-self.max_entries:]
            
            # Notify update
            if self.update_callback:
                self.update_callback('new_entry', entry)
    
    def set_filter(self, filter_type: str, value: any):
        """Set a filter and reapply to all entries"""
        with self._lock:
            self.filters[filter_type] = value
            self._reapply_filters()
    
    def toggle_level(self, level: str):
        """Toggle visibility of a log level"""
        with self._lock:
            level = level.upper()
            if level in self.filters['levels']:
                self.filters['levels'].remove(level)
            else:
                self.filters['levels'].add(level)
            self._reapply_filters()
    
    def search(self, text: str):
        """Search logs for text"""
        with self._lock:
            self.filters['search'] = text
            self._reapply_filters()
    
    def set_regex(self, pattern: Optional[str]):
        """Set regex pattern for filtering"""
        with self._lock:
            if pattern:
                try:
                    re.compile(pattern)
                    self.filters['regex'] = pattern
                except re.error:
                    return False
            else:
                self.filters['regex'] = None
            self._reapply_filters()
            return True
    
    def _reapply_filters(self):
        """Reapply filters to all entries"""
        self.filtered_entries = [
            entry for entry in self.entries
            if entry.matches_filter(self.filters)
        ]
    
    def get_display(self, height: Optional[int] = None) -> Panel:
        """Get rich Panel for log display"""
        height = height or self.display_lines
        
        with self._lock:
            # Get latest entries
            display_entries = self.filtered_entries[-height:] if self.filtered_entries else []
            
            # Create table
            table = Table(box=box.SIMPLE, show_header=True, padding=0)
            table.add_column("Time", style="dim", width=8)
            table.add_column("Level", width=8)
            table.add_column("Source", style="cyan", width=15)
            table.add_column("Message", style="white", no_wrap=False)
            
            # Add entries
            for entry in display_entries:
                # Color based on level
                level_style = {
                    'DEBUG': 'dim',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'bold red'
                }.get(entry.level, 'white')
                
                # Format time
                time_str = entry.timestamp.strftime("%H:%M:%S")
                
                # Truncate source if needed
                source = entry.source[:15]
                
                # Add file context to message if present
                message = entry.message
                if entry.file_context:
                    message = f"[{Path(entry.file_context).name}] {message}"
                
                table.add_row(
                    time_str,
                    Text(entry.level, style=level_style),
                    source,
                    message
                )
            
            # Create header with stats and filters
            header_parts = []
            
            # Level counts
            for level, count in self.level_counts.items():
                if count > 0:
                    style = "dim" if level not in self.filters['levels'] else {
                        'DEBUG': 'dim',
                        'INFO': 'green',
                        'WARNING': 'yellow',
                        'ERROR': 'red',
                        'CRITICAL': 'bold red'
                    }.get(level, 'white')
                    header_parts.append(f"[{style}]{level}: {count}[/{style}]")
            
            # Active filters
            if self.filters['search']:
                header_parts.append(f"[yellow]Search: '{self.filters['search']}'[/yellow]")
            if self.filters['regex']:
                header_parts.append(f"[yellow]Regex: '{self.filters['regex']}'[/yellow]")
            
            header = " | ".join(header_parts) if header_parts else "No entries"
            
            return Panel(
                table,
                title=f"Live Logs ({len(self.filtered_entries)} shown / {len(self.entries)} total)",
                subtitle=header,
                box=box.ROUNDED,
                style="dim"
            )
    
    def get_detailed_view(self, entry_index: int) -> Panel:
        """Get detailed view of a specific log entry"""
        with self._lock:
            if 0 <= entry_index < len(self.filtered_entries):
                entry = self.filtered_entries[entry_index]
                
                # Build detailed view
                details = Text()
                details.append("Timestamp: ", style="bold")
                details.append(f"{entry.timestamp.isoformat()}\n")
                
                details.append("Level: ", style="bold")
                level_style = {
                    'DEBUG': 'dim',
                    'INFO': 'green',
                    'WARNING': 'yellow',
                    'ERROR': 'red',
                    'CRITICAL': 'bold red'
                }.get(entry.level, 'white')
                details.append(f"{entry.level}\n", style=level_style)
                
                details.append("Source: ", style="bold")
                details.append(f"{entry.source}\n", style="cyan")
                
                if entry.file_context:
                    details.append("File: ", style="bold")
                    details.append(f"{entry.file_context}\n", style="blue")
                
                details.append("\nMessage:\n", style="bold")
                details.append(entry.message)
                
                if entry.extra_data:
                    details.append("\n\nExtra Data:\n", style="bold")
                    # Pretty print JSON
                    json_str = json.dumps(entry.extra_data, indent=2)
                    details.append(Syntax(json_str, "json", theme="monokai", line_numbers=False))
                
                return Panel(details, title="Log Entry Details", box=box.ROUNDED)
            else:
                return Panel("Invalid entry index", title="Error", style="red")
    
    def export_logs(self, filepath: Path, format: str = 'json'):
        """Export logs to file"""
        with self._lock:
            if format == 'json':
                logs_data = []
                for entry in self.filtered_entries:
                    logs_data.append({
                        'timestamp': entry.timestamp.isoformat(),
                        'level': entry.level,
                        'source': entry.source,
                        'message': entry.message,
                        'file_context': entry.file_context,
                        'extra_data': entry.extra_data
                    })
                
                with open(filepath, 'w') as f:
                    json.dump(logs_data, f, indent=2)
                    
            elif format == 'text':
                with open(filepath, 'w') as f:
                    for entry in self.filtered_entries:
                        f.write(f"{entry.timestamp.isoformat()} [{entry.level}] {entry.source}: {entry.message}\n")
                        if entry.file_context:
                            f.write(f"  File: {entry.file_context}\n")
                        if entry.extra_data:
                            f.write(f"  Extra: {json.dumps(entry.extra_data)}\n")
                        f.write("\n")
    
    def clear_logs(self):
        """Clear all logs"""
        with self._lock:
            self.entries.clear()
            self.filtered_entries.clear()
            self.level_counts = {level: 0 for level in self.level_counts}
            self.source_counts.clear()
            
            if self.update_callback:
                self.update_callback('cleared', None)


class LoggerIntegration:
    """Integration helper for connecting LiveLogger to Python logging"""
    
    def __init__(self, live_logger: LiveLogger):
        self.live_logger = live_logger
        
    def create_handler(self):
        """Create a logging handler that feeds into LiveLogger"""
        import logging
        
        class LiveLoggerHandler(logging.Handler):
            def __init__(self, live_logger: LiveLogger):
                super().__init__()
                self.live_logger = live_logger
                
            def emit(self, record):
                try:
                    # Extract source from logger name
                    source = record.name.split('.')[-1] if record.name else 'unknown'
                    
                    # Get extra data
                    extra_data = {}
                    if hasattr(record, 'extra'):
                        extra_data = record.extra
                    
                    # Add file context if available
                    file_context = None
                    if hasattr(record, 'filename'):
                        file_context = record.filename
                    
                    self.live_logger.add_entry(
                        level=record.levelname,
                        source=source,
                        message=self.format(record),
                        file_context=file_context,
                        extra_data=extra_data
                    )
                except Exception:
                    self.handleError(record)
        
        return LiveLoggerHandler(self.live_logger)