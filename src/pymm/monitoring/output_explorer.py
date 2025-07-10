"""Real-time output file explorer with live updates and counts"""
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Callable
from dataclasses import dataclass, field
from collections import defaultdict
import csv
import json

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.tree import Tree
from rich.layout import Layout
from rich import box
from rich.syntax import Syntax
from rich.columns import Columns

console = Console()


@dataclass
class OutputFile:
    """Information about an output file"""
    path: Path
    created_time: datetime
    modified_time: datetime
    size: int
    row_count: int = 0
    concept_count: int = 0
    unique_concepts: Set[str] = field(default_factory=set)
    semantic_types: Dict[str, int] = field(default_factory=dict)
    status: str = "active"  # active, complete, error
    associated_input: Optional[str] = None
    

@dataclass
class DirectoryStats:
    """Statistics for an output directory"""
    total_files: int = 0
    active_files: int = 0
    completed_files: int = 0
    total_rows: int = 0
    total_concepts: int = 0
    unique_concepts: int = 0
    total_size_mb: float = 0.0
    files_by_hour: Dict[int, int] = field(default_factory=dict)
    

class OutputExplorer:
    """Real-time explorer for output files with live updates"""
    
    def __init__(self, output_dirs: List[Path], update_interval: float = 2.0):
        self.output_dirs = [Path(d) for d in output_dirs]
        self.update_interval = update_interval
        
        # File tracking
        self.files: Dict[Path, OutputFile] = {}
        self.directory_stats: Dict[Path, DirectoryStats] = {}
        self.new_files: List[Path] = []
        self.updated_files: List[Path] = []
        
        # Concept analysis
        self.global_concepts: Set[str] = set()
        self.concept_frequency: Dict[str, int] = defaultdict(int)
        self.semantic_type_stats: Dict[str, int] = defaultdict(int)
        
        # Watch settings
        self.watch_active = False
        self.update_callback: Optional[Callable] = None
        self._lock = threading.Lock()
        self._watch_thread: Optional[threading.Thread] = None
        
        # Display settings
        self.show_preview = True
        self.sort_by = "modified"  # name, size, modified, concepts
        self.filter_pattern = "*.csv"
        
    def start_watching(self):
        """Start watching directories for changes"""
        if not self.watch_active:
            self.watch_active = True
            self._watch_thread = threading.Thread(target=self._watch_loop, daemon=True)
            self._watch_thread.start()
    
    def stop_watching(self):
        """Stop watching directories"""
        self.watch_active = False
        if self._watch_thread and self._watch_thread.is_alive():
            self._watch_thread.join(timeout=5)
    
    def _watch_loop(self):
        """Main watching loop"""
        while self.watch_active:
            try:
                self._scan_directories()
                time.sleep(self.update_interval)
            except Exception as e:
                console.print(f"[error]Watch error: {e}[/error]")
    
    def _scan_directories(self):
        """Scan output directories for changes"""
        with self._lock:
            self.new_files.clear()
            self.updated_files.clear()
            
            # Track seen files
            seen_files = set()
            
            for output_dir in self.output_dirs:
                if not output_dir.exists():
                    continue
                
                # Initialize directory stats
                if output_dir not in self.directory_stats:
                    self.directory_stats[output_dir] = DirectoryStats()
                
                dir_stats = DirectoryStats()
                
                # Scan for CSV files
                for csv_file in output_dir.glob(self.filter_pattern):
                    seen_files.add(csv_file)
                    
                    # Check if new or updated
                    if csv_file not in self.files:
                        # New file
                        self._analyze_file(csv_file)
                        self.new_files.append(csv_file)
                    else:
                        # Check if updated
                        stat = csv_file.stat()
                        if stat.st_mtime > self.files[csv_file].modified_time.timestamp():
                            self._analyze_file(csv_file)
                            self.updated_files.append(csv_file)
                    
                    # Update directory stats
                    if csv_file in self.files:
                        file_info = self.files[csv_file]
                        dir_stats.total_files += 1
                        dir_stats.total_rows += file_info.row_count
                        dir_stats.total_concepts += file_info.concept_count
                        dir_stats.unique_concepts += len(file_info.unique_concepts)
                        dir_stats.total_size_mb += file_info.size / (1024 * 1024)
                        
                        # Track by hour
                        hour = file_info.created_time.hour
                        dir_stats.files_by_hour[hour] = dir_stats.files_by_hour.get(hour, 0) + 1
                        
                        if file_info.status == "active":
                            dir_stats.active_files += 1
                        else:
                            dir_stats.completed_files += 1
                
                self.directory_stats[output_dir] = dir_stats
            
            # Remove files that no longer exist
            for file_path in list(self.files.keys()):
                if file_path not in seen_files:
                    del self.files[file_path]
            
            # Notify updates
            if self.update_callback and (self.new_files or self.updated_files):
                self.update_callback('files_updated', {
                    'new': self.new_files,
                    'updated': self.updated_files
                })
    
    def _analyze_file(self, file_path: Path):
        """Analyze a CSV output file"""
        try:
            stat = file_path.stat()
            
            # Create file info
            file_info = OutputFile(
                path=file_path,
                created_time=datetime.fromtimestamp(stat.st_ctime),
                modified_time=datetime.fromtimestamp(stat.st_mtime),
                size=stat.st_size
            )
            
            # Determine associated input file
            # Assuming output filename format: input_filename_timestamp.csv
            base_name = file_path.stem
            if '_' in base_name:
                parts = base_name.rsplit('_', 1)
                if len(parts) == 2:
                    file_info.associated_input = parts[0]
            
            # Quick analysis of CSV content
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    
                    for row in reader:
                        file_info.row_count += 1
                        
                        # Extract concept info
                        if 'preferred_name' in row:
                            concept = row['preferred_name']
                            file_info.unique_concepts.add(concept)
                            self.global_concepts.add(concept)
                            self.concept_frequency[concept] += 1
                        
                        # Extract semantic type
                        if 'semantic_types' in row:
                            sem_types = row['semantic_types'].split(',')
                            for st in sem_types:
                                st = st.strip()
                                if st:
                                    file_info.semantic_types[st] = file_info.semantic_types.get(st, 0) + 1
                                    self.semantic_type_stats[st] += 1
                    
                    file_info.concept_count = len(file_info.unique_concepts)
                    
                    # Determine status based on file activity
                    if (datetime.now() - file_info.modified_time).seconds < 60:
                        file_info.status = "active"
                    else:
                        file_info.status = "complete"
                        
            except Exception as e:
                file_info.status = "error"
                console.print(f"[error]Error analyzing {file_path.name}: {e}[/error]")
            
            self.files[file_path] = file_info
            
        except Exception as e:
            console.print(f"[error]Error processing {file_path}: {e}[/error]")
    
    def get_display(self) -> Layout:
        """Get rich Layout for output explorer display"""
        layout = Layout()
        
        # Create sections
        summary_section = self._create_summary_section()
        files_section = self._create_files_section()
        details_section = self._create_details_section()
        
        # Arrange layout
        layout.split_column(
            Layout(summary_section, size=8),
            Layout(name="main", size=20)
        )
        
        layout["main"].split_row(
            Layout(files_section, ratio=2),
            Layout(details_section, ratio=1)
        )
        
        return layout
    
    def _create_summary_section(self) -> Panel:
        """Create summary statistics panel"""
        with self._lock:
            # Calculate totals
            total_files = len(self.files)
            active_files = sum(1 for f in self.files.values() if f.status == "active")
            total_concepts = len(self.global_concepts)
            total_rows = sum(f.row_count for f in self.files.values())
            total_size = sum(f.size for f in self.files.values()) / (1024 * 1024)
            
            # Create display
            content = Columns([
                Panel(f"[bold cyan]{total_files}[/bold cyan]\nTotal Files", box=box.ROUNDED),
                Panel(f"[bold green]{active_files}[/bold green]\nActive", box=box.ROUNDED),
                Panel(f"[bold yellow]{total_rows:,}[/bold yellow]\nTotal Rows", box=box.ROUNDED),
                Panel(f"[bold magenta]{total_concepts:,}[/bold magenta]\nUnique Concepts", box=box.ROUNDED),
                Panel(f"[bold blue]{total_size:.1f} MB[/bold blue]\nTotal Size", box=box.ROUNDED),
            ])
            
            # Add recent activity
            activity = Text()
            if self.new_files:
                activity.append(f"New files: {len(self.new_files)} ", style="green")
            if self.updated_files:
                activity.append(f"Updated: {len(self.updated_files)}", style="yellow")
            
            if activity:
                content = Group(content, Text(), activity)
            
            return Panel(content, title="Output Summary", box=box.ROUNDED, style="cyan")
    
    def _create_files_section(self) -> Panel:
        """Create files listing panel"""
        with self._lock:
            table = Table(box=box.SIMPLE)
            table.add_column("File", style="cyan", ratio=3)
            table.add_column("Status", width=8)
            table.add_column("Rows", style="yellow", width=8)
            table.add_column("Concepts", style="magenta", width=8)
            table.add_column("Size", style="blue", width=8)
            table.add_column("Modified", style="dim", width=12)
            
            # Sort files
            sorted_files = sorted(
                self.files.items(),
                key=lambda x: {
                    'name': x[0].name,
                    'size': x[1].size,
                    'modified': x[1].modified_time,
                    'concepts': x[1].concept_count
                }.get(self.sort_by, x[1].modified_time),
                reverse=True
            )
            
            # Add rows
            for file_path, file_info in sorted_files[:20]:  # Show top 20
                # Status indicator
                status_style = {
                    'active': 'green',
                    'complete': 'dim',
                    'error': 'red'
                }.get(file_info.status, 'white')
                
                # Format modified time
                if file_info.status == "active":
                    mod_time = f"{(datetime.now() - file_info.modified_time).seconds}s ago"
                else:
                    mod_time = file_info.modified_time.strftime("%H:%M:%S")
                
                table.add_row(
                    file_path.name[:40],
                    Text(file_info.status, style=status_style),
                    str(file_info.row_count),
                    str(file_info.concept_count),
                    f"{file_info.size / 1024:.1f}K",
                    mod_time
                )
            
            return Panel(table, title=f"Output Files ({len(self.files)} total)", box=box.ROUNDED)
    
    def _create_details_section(self) -> Panel:
        """Create details panel for selected file or top concepts"""
        with self._lock:
            content = Text()
            
            # Top concepts
            content.append("Top Concepts\n", style="bold cyan")
            top_concepts = sorted(
                self.concept_frequency.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
            
            for concept, count in top_concepts:
                content.append(f"  {concept[:30]:30} ")
                bar_width = int((count / max(self.concept_frequency.values())) * 20) if self.concept_frequency else 0
                content.append("█" * bar_width, style="green")
                content.append(f" {count}\n", style="dim")
            
            # Semantic types
            content.append("\nTop Semantic Types\n", style="bold cyan")
            top_types = sorted(
                self.semantic_type_stats.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
            
            for sem_type, count in top_types:
                content.append(f"  {sem_type:10} {count:6}\n")
            
            return Panel(content, title="Analysis", box=box.ROUNDED, style="magenta")
    
    def get_file_preview(self, file_path: Path, lines: int = 10) -> Panel:
        """Get preview of a specific file"""
        if file_path not in self.files:
            return Panel("File not found", title="Error", style="red")
        
        file_info = self.files[file_path]
        
        # Read first few lines
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                preview_lines = []
                reader = csv.DictReader(f)
                
                for i, row in enumerate(reader):
                    if i >= lines:
                        break
                    
                    # Format row for display
                    row_text = Text()
                    row_text.append(f"Row {i+1}: ", style="dim")
                    
                    if 'preferred_name' in row:
                        row_text.append(row['preferred_name'], style="cyan")
                        row_text.append(" | ")
                    
                    if 'semantic_types' in row:
                        row_text.append(row['semantic_types'], style="yellow")
                    
                    preview_lines.append(row_text)
                
                content = Text()
                content.append(f"File: {file_path.name}\n", style="bold")
                content.append(f"Status: {file_info.status}\n", style="green" if file_info.status == "active" else "dim")
                content.append(f"Rows: {file_info.row_count} | Concepts: {file_info.concept_count}\n")
                content.append(f"Size: {file_info.size / 1024:.1f} KB\n")
                content.append("\n")
                
                for line in preview_lines:
                    content.append(line)
                    content.append("\n")
                
                return Panel(content, title="File Preview", box=box.ROUNDED)
                
        except Exception as e:
            return Panel(f"Error reading file: {e}", title="Error", style="red")
    
    def export_statistics(self, output_path: Path):
        """Export current statistics to file"""
        with self._lock:
            stats = {
                'timestamp': datetime.now().isoformat(),
                'directories': {},
                'global_stats': {
                    'total_files': len(self.files),
                    'total_concepts': len(self.global_concepts),
                    'total_rows': sum(f.row_count for f in self.files.values())
                },
                'top_concepts': dict(sorted(
                    self.concept_frequency.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:50]),
                'semantic_types': dict(self.semantic_type_stats)
            }
            
            # Add directory stats
            for dir_path, dir_stats in self.directory_stats.items():
                stats['directories'][str(dir_path)] = {
                    'total_files': dir_stats.total_files,
                    'active_files': dir_stats.active_files,
                    'total_rows': dir_stats.total_rows,
                    'total_size_mb': dir_stats.total_size_mb
                }
            
            with open(output_path, 'w') as f:
                json.dump(stats, f, indent=2)