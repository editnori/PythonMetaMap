"""Clean batch processing with simplified UI"""
import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout

from ..core.config import PyMMConfig
from ..cli.ultra_fast_batch import process_file_simple

console = Console()


class CleanBatchProcessor:
    """Clean batch processor with simplified UI"""
    
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir).resolve()
        self.output_dir = Path(output_dir).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get config
        config = PyMMConfig()
        self.mm_path = config.get("metamap_binary_path")
        
        # Calculate workers
        cpu_count = mp.cpu_count()
        self.workers = min(int(cpu_count * 0.8), 16)
    
    def collect_files(self) -> List[Path]:
        """Collect input files"""
        files = []
        for pattern in ['*.txt', '*.text']:
            files.extend(self.input_dir.glob(pattern))
        return sorted(files)
    
    def process(self) -> Dict[str, Any]:
        """Process files with clean UI"""
        files = self.collect_files()
        if not files:
            console.print("[yellow]No input files found![/yellow]")
            return {"success": True, "processed": 0}
        
        # Show header
        console.print("\n")
        console.print(Panel.fit(
            f"[bold cyan]Processing {len(files)} files[/bold cyan]\n"
            f"Workers: {self.workers} • Timeout: 60s/file",
            title="⚡ Batch Processing ⚡"
        ))
        
        # Prepare tasks
        tasks = []
        for f in files:
            output_path = self.output_dir / f"{f.stem}.csv"
            tasks.append((str(f), str(output_path), self.mm_path, 0))
        
        # Process with clean progress
        results = {
            "processed": 0,
            "failed": 0,
            "start_time": datetime.now()
        }
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("{task.completed}/{task.total}"),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            
            # Single progress bar
            main_task = progress.add_task(
                "[cyan]Processing files", 
                total=len(files)
            )
            
            # Simple progress without complex layout
            # Process files
            with ProcessPoolExecutor(max_workers=self.workers) as executor:
                futures = {executor.submit(process_file_simple, task): task[0] for task in tasks}
                
                completed = 0
                for future in as_completed(futures):
                    try:
                        _, success, elapsed, concepts = future.result()
                        if success:
                            results["processed"] += 1
                        else:
                            results["failed"] += 1
                    except:
                        results["failed"] += 1
                    
                    completed += 1
                    # Update progress
                    progress.update(main_task, advance=1)
                    
                    # Update description with rate
                    elapsed_total = (datetime.now() - results["start_time"]).total_seconds()
                    if elapsed_total > 0:
                        rate = completed / elapsed_total
                        progress.update(main_task, description=f"[cyan]Processing files ({rate:.1f}/sec)")
        
        # Final results
        elapsed = (datetime.now() - results["start_time"]).total_seconds()
        results["time"] = elapsed
        results["rate"] = len(files) / elapsed if elapsed > 0 else 0
        
        # Show summary
        self._show_results(results, len(files))
        
        return results
    
    def _show_results(self, results: Dict[str, Any], total_files: int):
        """Show final results"""
        console.print("\n")
        table = Table(title="✅ Processing Complete", box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Files Processed", f"{results['processed']:,}")
        table.add_row("Files Failed", f"{results['failed']:,}")
        table.add_row("Total Time", f"{results['time']:.1f} seconds")
        table.add_row("Average Rate", f"{results['rate']:.2f} files/sec")
        
        if results['rate'] > 0:
            # Estimate for 30k
            time_30k = 30000 / results['rate']
            if time_30k < 3600:
                estimate = f"{time_30k/60:.0f} minutes"
            elif time_30k < 86400:
                estimate = f"{time_30k/3600:.1f} hours"
            else:
                estimate = f"{time_30k/86400:.1f} days"
            table.add_row("30k Files Estimate", estimate)
        
        console.print(table)


def main():
    """Simple entry point"""
    # Default to pymm_data directories
    input_dir = "./pymm_data/input"
    output_dir = "./pymm_data/output"
    
    if len(sys.argv) > 2:
        input_dir = sys.argv[1]
        output_dir = sys.argv[2]
    
    processor = CleanBatchProcessor(input_dir, output_dir)
    results = processor.process()
    
    return 0 if results.get("processed", 0) > 0 else 1


if __name__ == "__main__":
    sys.exit(main())