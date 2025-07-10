"""Live resource monitoring with detailed system metrics"""
import threading
import time
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Deque
from dataclasses import dataclass
from collections import deque
import platform

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.progress import Progress, BarColumn
from rich.layout import Layout
from rich.columns import Columns
from rich import box
# from rich.chart import LineChart  # Not available in all rich versions

console = Console()


@dataclass
class ResourceSnapshot:
    """Single snapshot of system resources"""
    timestamp: datetime
    cpu_percent: float
    cpu_per_core: List[float]
    memory_percent: float
    memory_used_gb: float
    memory_available_gb: float
    swap_percent: float
    disk_read_bytes: int
    disk_write_bytes: int
    disk_read_count: int
    disk_write_count: int
    network_recv_bytes: int
    network_sent_bytes: int
    process_count: int
    thread_count: int
    open_files: int
    

class ResourceMonitor:
    """Monitor system resources in real-time"""
    
    def __init__(self, history_size: int = 300, update_interval: float = 1.0):
        self.history_size = history_size
        self.update_interval = update_interval
        
        # Resource history
        self.cpu_history: Deque[float] = deque(maxlen=history_size)
        self.memory_history: Deque[float] = deque(maxlen=history_size)
        self.disk_read_history: Deque[float] = deque(maxlen=history_size)
        self.disk_write_history: Deque[float] = deque(maxlen=history_size)
        self.network_recv_history: Deque[float] = deque(maxlen=history_size)
        self.network_sent_history: Deque[float] = deque(maxlen=history_size)
        
        # Per-core CPU history
        self.cpu_cores = psutil.cpu_count()
        self.cpu_per_core_history: Dict[int, Deque[float]] = {
            i: deque(maxlen=history_size) for i in range(self.cpu_cores)
        }
        
        # Process-specific monitoring
        self.process_metrics: Dict[int, Dict] = {}
        self.top_processes: List[Dict] = []
        
        # Alerts and thresholds
        self.alerts: List[Dict] = []
        self.thresholds = {
            'cpu_percent': 90.0,
            'memory_percent': 90.0,
            'disk_usage_percent': 95.0,
            'swap_percent': 80.0
        }
        
        # Previous values for rate calculation
        self._last_disk_io = psutil.disk_io_counters()
        self._last_net_io = psutil.net_io_counters()
        self._last_update = time.time()
        
        # Control
        self._running = False
        self._monitor_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        
    def start(self):
        """Start monitoring"""
        if not self._running:
            self._running = True
            self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self._monitor_thread.start()
    
    def stop(self):
        """Stop monitoring"""
        self._running = False
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=5)
    
    def _monitor_loop(self):
        """Main monitoring loop"""
        while self._running:
            try:
                self._collect_metrics()
                time.sleep(self.update_interval)
            except Exception as e:
                console.print(f"[error]Resource monitoring error: {e}[/error]")
    
    def _collect_metrics(self):
        """Collect current system metrics"""
        with self._lock:
            now = time.time()
            time_delta = now - self._last_update
            self._last_update = now
            
            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=0.1)
            cpu_per_core = psutil.cpu_percent(interval=0.1, percpu=True)
            
            self.cpu_history.append(cpu_percent)
            for i, percent in enumerate(cpu_per_core):
                if i < len(self.cpu_per_core_history):
                    self.cpu_per_core_history[i].append(percent)
            
            # Memory metrics
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            self.memory_history.append(memory.percent)
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            if disk_io and self._last_disk_io:
                read_rate = (disk_io.read_bytes - self._last_disk_io.read_bytes) / time_delta
                write_rate = (disk_io.write_bytes - self._last_disk_io.write_bytes) / time_delta
                self.disk_read_history.append(read_rate / (1024 * 1024))  # MB/s
                self.disk_write_history.append(write_rate / (1024 * 1024))  # MB/s
                self._last_disk_io = disk_io
            
            # Network I/O
            net_io = psutil.net_io_counters()
            if net_io and self._last_net_io:
                recv_rate = (net_io.bytes_recv - self._last_net_io.bytes_recv) / time_delta
                sent_rate = (net_io.bytes_sent - self._last_net_io.bytes_sent) / time_delta
                self.network_recv_history.append(recv_rate / (1024 * 1024))  # MB/s
                self.network_sent_history.append(sent_rate / (1024 * 1024))  # MB/s
                self._last_net_io = net_io
            
            # Process metrics
            self._update_process_metrics()
            
            # Check thresholds
            self._check_alerts(cpu_percent, memory.percent, swap.percent)
    
    def _update_process_metrics(self):
        """Update metrics for top processes"""
        try:
            processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'num_threads']):
                try:
                    info = proc.info
                    if info['cpu_percent'] > 0 or info['memory_percent'] > 0.5:
                        processes.append(info)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            
            # Sort by CPU usage
            self.top_processes = sorted(processes, key=lambda x: x['cpu_percent'], reverse=True)[:10]
            
        except Exception as e:
            console.print(f"[error]Process metrics error: {e}[/error]")
    
    def _check_alerts(self, cpu: float, memory: float, swap: float):
        """Check for threshold violations"""
        alerts = []
        
        if cpu > self.thresholds['cpu_percent']:
            alerts.append({
                'type': 'cpu',
                'message': f'High CPU usage: {cpu:.1f}%',
                'severity': 'warning' if cpu < 95 else 'critical'
            })
        
        if memory > self.thresholds['memory_percent']:
            alerts.append({
                'type': 'memory',
                'message': f'High memory usage: {memory:.1f}%',
                'severity': 'warning' if memory < 95 else 'critical'
            })
        
        if swap > self.thresholds['swap_percent']:
            alerts.append({
                'type': 'swap',
                'message': f'High swap usage: {swap:.1f}%',
                'severity': 'warning'
            })
        
        # Keep recent alerts
        self.alerts = alerts + self.alerts[:10]
    
    def get_display(self) -> Layout:
        """Get rich Layout for resource display"""
        layout = Layout()
        
        # Create sections
        cpu_section = self._create_cpu_section()
        memory_section = self._create_memory_section()
        io_section = self._create_io_section()
        process_section = self._create_process_section()
        
        # Arrange layout
        layout.split_column(
            Layout(name="top", size=12),
            Layout(name="middle", size=8),
            Layout(name="bottom", size=10)
        )
        
        layout["top"].split_row(
            Layout(cpu_section),
            Layout(memory_section)
        )
        
        layout["middle"] = io_section
        layout["bottom"] = process_section
        
        return layout
    
    def _create_cpu_section(self) -> Panel:
        """Create CPU usage display"""
        with self._lock:
            # Current values
            current_cpu = self.cpu_history[-1] if self.cpu_history else 0
            
            # Create content
            content = Text()
            
            # Overall CPU bar
            content.append("Overall CPU Usage\n", style="bold")
            cpu_bar = self._create_usage_bar(current_cpu, 100)
            content.append(cpu_bar)
            content.append(f" {current_cpu:.1f}%\n\n")
            
            # Per-core display
            if len(self.cpu_per_core_history) > 0:
                content.append("Per-Core Usage\n", style="bold")
                
                # Create mini bars for each core
                for i in range(min(self.cpu_cores, 8)):  # Show max 8 cores
                    if i in self.cpu_per_core_history and self.cpu_per_core_history[i]:
                        core_usage = self.cpu_per_core_history[i][-1]
                        content.append(f"Core {i}: ")
                        mini_bar = self._create_mini_bar(core_usage, 100, width=20)
                        content.append(mini_bar)
                        content.append(f" {core_usage:5.1f}%\n")
            
            # CPU graph (simple ASCII)
            if len(self.cpu_history) > 10:
                content.append("\nHistory (last 60s)\n", style="bold")
                graph = self._create_simple_graph(list(self.cpu_history)[-60:], height=5)
                content.append(graph)
            
            return Panel(content, title="CPU Monitor", box=box.ROUNDED, style="cyan")
    
    def _create_memory_section(self) -> Panel:
        """Create memory usage display"""
        with self._lock:
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()
            
            content = Text()
            
            # RAM usage
            content.append("RAM Usage\n", style="bold")
            ram_bar = self._create_usage_bar(memory.percent, 100)
            content.append(ram_bar)
            content.append(f" {memory.percent:.1f}%\n")
            content.append(f"Used: {memory.used / (1024**3):.1f}GB / {memory.total / (1024**3):.1f}GB\n\n")
            
            # Swap usage
            content.append("Swap Usage\n", style="bold")
            swap_bar = self._create_usage_bar(swap.percent, 100)
            content.append(swap_bar)
            content.append(f" {swap.percent:.1f}%\n")
            content.append(f"Used: {swap.used / (1024**3):.1f}GB / {swap.total / (1024**3):.1f}GB\n")
            
            # Memory graph
            if len(self.memory_history) > 10:
                content.append("\nHistory (last 60s)\n", style="bold")
                graph = self._create_simple_graph(list(self.memory_history)[-60:], height=5)
                content.append(graph)
            
            return Panel(content, title="Memory Monitor", box=box.ROUNDED, style="green")
    
    def _create_io_section(self) -> Panel:
        """Create I/O usage display"""
        with self._lock:
            content = Text()
            
            # Disk I/O
            content.append("Disk I/O (MB/s)\n", style="bold")
            
            read_rate = self.disk_read_history[-1] if self.disk_read_history else 0
            write_rate = self.disk_write_history[-1] if self.disk_write_history else 0
            
            content.append(f"Read:  {read_rate:6.1f} MB/s  ")
            content.append(self._create_mini_bar(min(read_rate, 100), 100, width=30), style="green")
            content.append(f"\nWrite: {write_rate:6.1f} MB/s  ")
            content.append(self._create_mini_bar(min(write_rate, 100), 100, width=30), style="red")
            content.append("\n\n")
            
            # Network I/O
            content.append("Network I/O (MB/s)\n", style="bold")
            
            recv_rate = self.network_recv_history[-1] if self.network_recv_history else 0
            sent_rate = self.network_sent_history[-1] if self.network_sent_history else 0
            
            content.append(f"Recv: {recv_rate:6.1f} MB/s  ")
            content.append(self._create_mini_bar(min(recv_rate, 10), 10, width=30), style="blue")
            content.append(f"\nSent: {sent_rate:6.1f} MB/s  ")
            content.append(self._create_mini_bar(min(sent_rate, 10), 10, width=30), style="yellow")
            
            return Panel(content, title="I/O Monitor", box=box.ROUNDED, style="magenta")
    
    def _create_process_section(self) -> Panel:
        """Create top processes display"""
        with self._lock:
            table = Table(box=box.SIMPLE)
            table.add_column("PID", style="dim", width=8)
            table.add_column("Name", style="cyan", width=25)
            table.add_column("CPU %", style="yellow", width=8)
            table.add_column("Memory %", style="green", width=10)
            table.add_column("Threads", style="blue", width=8)
            
            for proc in self.top_processes[:8]:
                table.add_row(
                    str(proc['pid']),
                    proc['name'][:25],
                    f"{proc['cpu_percent']:.1f}",
                    f"{proc['memory_percent']:.1f}",
                    str(proc.get('num_threads', 0))
                )
            
            return Panel(table, title="Top Processes", box=box.ROUNDED, style="blue")
    
    def _create_usage_bar(self, value: float, max_value: float, width: int = 40) -> Text:
        """Create a colored usage bar"""
        percent = (value / max_value) * 100 if max_value > 0 else 0
        filled = int((percent / 100) * width)
        
        # Color based on usage
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
    
    def _create_mini_bar(self, value: float, max_value: float, width: int = 20) -> Text:
        """Create a mini usage bar"""
        percent = (value / max_value) * 100 if max_value > 0 else 0
        filled = int((percent / 100) * width)
        
        bar = Text()
        bar.append("▓" * filled, style="bright_white")
        bar.append("░" * (width - filled), style="dim")
        
        return bar
    
    def _create_simple_graph(self, values: List[float], height: int = 5) -> Text:
        """Create a simple ASCII graph"""
        if not values:
            return Text("No data")
        
        # Normalize values
        max_val = max(values) if values else 1
        min_val = min(values) if values else 0
        range_val = max_val - min_val if max_val != min_val else 1
        
        # Create graph
        graph = Text()
        
        for h in range(height, 0, -1):
            threshold = min_val + (h / height) * range_val
            line = ""
            for val in values[-40:]:  # Show last 40 values
                if val >= threshold:
                    line += "█"
                else:
                    line += " "
            graph.append(line + "\n", style="cyan")
        
        # Add axis
        graph.append("─" * min(40, len(values)), style="dim")
        
        return graph
    
    def get_summary(self) -> Dict:
        """Get current resource summary"""
        with self._lock:
            return {
                'cpu_percent': self.cpu_history[-1] if self.cpu_history else 0,
                'memory_percent': self.memory_history[-1] if self.memory_history else 0,
                'disk_read_mb_s': self.disk_read_history[-1] if self.disk_read_history else 0,
                'disk_write_mb_s': self.disk_write_history[-1] if self.disk_write_history else 0,
                'network_recv_mb_s': self.network_recv_history[-1] if self.network_recv_history else 0,
                'network_sent_mb_s': self.network_sent_history[-1] if self.network_sent_history else 0,
                'top_process': self.top_processes[0]['name'] if self.top_processes else 'None',
                'alerts': len(self.alerts)
            }