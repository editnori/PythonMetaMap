"""Ultra-fast batch processing - optimized for speed"""
import os
import sys
import time
import multiprocessing as mp
from pathlib import Path
from typing import List, Dict, Any, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import logging

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn, MofNCompleteColumn
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout

from ..pymm import Metamap
from ..core.config import PyMMConfig
from ..processing.worker import FileProcessor

console = Console()
logger = logging.getLogger(__name__)


def process_file_simple(args: Tuple[str, str, str, int]) -> Tuple[str, bool, float, int]:
    """Simple file processor for parallel execution"""
    input_path, output_path, mm_path, worker_id = args
    start_time = time.time()
    
    try:
        # Create output directory if needed
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Read input
        with open(input_path, 'r', encoding='utf-8') as f:
            text = f.read()
        
        # Create MetaMap instance (each worker gets its own)
        mm = Metamap(mm_path)
        
        # Process with timeout
        mmos = mm.parse([text], timeout=60)  # 60 second timeout per file
        
        # Extract concepts quickly
        concepts = []
        if mmos:
            for mmo in mmos:
                for concept in mmo:
                    concepts.append({
                        'CUI': getattr(concept, 'cui', ''),
                        'Score': getattr(concept, 'score', ''),
                        'ConceptName': getattr(concept, 'matched', ''),
                        'PrefName': getattr(concept, 'pref_name', ''),
                        'Phrase': getattr(concept, 'phrase_text', ''),
                        'SemTypes': str(getattr(concept, 'semtypes', [])),
                        'Sources': str(getattr(concept, 'sources', [])),
                        'Position': ''
                    })
        
        # Write CSV quickly
        import csv
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            if concepts:
                writer = csv.DictWriter(f, fieldnames=concepts[0].keys())
                writer.writeheader()
                writer.writerows(concepts)
            else:
                # Write empty CSV with headers
                writer = csv.writer(f)
                writer.writerow(['CUI', 'Score', 'ConceptName', 'PrefName', 'Phrase', 'SemTypes', 'Sources', 'Position'])
        
        elapsed = time.time() - start_time
        return input_path, True, elapsed, len(concepts)
        
    except Exception as e:
        elapsed = time.time() - start_time
        # Write error file
        with open(output_path + '.error', 'w') as f:
            f.write(str(e))
        return input_path, False, elapsed, 0


class UltraFastBatchProcessor:
    """Ultra-fast batch processor using true parallelization"""
    
    def __init__(self, input_dir: str, output_dir: str, workers: int = None):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Determine optimal workers
        cpu_count = mp.cpu_count()
        self.workers = workers or min(cpu_count - 1, 16)  # Leave one CPU free, max 16
        
        # Get MetaMap path
        config = PyMMConfig()
        self.mm_path = config.get("metamap_binary_path")
    
    def collect_files(self) -> List[Path]:
        """Collect all input files"""
        files = []
        for pattern in ['*.txt', '*.text']:
            files.extend(self.input_dir.glob(pattern))
        return sorted(files)
    
    def process_parallel(self, show_progress: bool = True) -> Dict[str, Any]:
        """Process files in parallel with all workers"""
        files = self.collect_files()
        if not files:
            return {"success": True, "processed": 0, "failed": 0, "time": 0}
        
        # Prepare arguments for parallel processing
        args_list = []
        for f in files:
            output_path = self.output_dir / f"{f.stem}.csv"
            args_list.append((str(f), str(output_path), self.mm_path, 0))
        
        results = {
            "processed": 0,
            "failed": 0,
            "total_concepts": 0,
            "start_time": datetime.now()
        }
        
        if show_progress:
            # Process with progress bar
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TextColumn("• {task.fields[rate]:.1f} files/min"),
                TimeRemainingColumn(),
                console=console
            ) as progress:
                
                task = progress.add_task(
                    "[cyan]Processing files", 
                    total=len(files),
                    rate=0.0
                )
                
                # Use ProcessPoolExecutor for true parallelization
                with ProcessPoolExecutor(max_workers=self.workers) as executor:
                    # Submit all tasks
                    future_to_file = {
                        executor.submit(process_file_simple, args): args[0] 
                        for args in args_list
                    }
                    
                    # Process completed tasks
                    for future in as_completed(future_to_file):
                        try:
                            input_path, success, elapsed, concepts = future.result()
                            
                            if success:
                                results["processed"] += 1
                                results["total_concepts"] += concepts
                            else:
                                results["failed"] += 1
                            
                            # Update progress
                            completed = results["processed"] + results["failed"]
                            progress.update(task, advance=1)
                            
                            # Calculate rate
                            elapsed_total = (datetime.now() - results["start_time"]).total_seconds()
                            rate = (completed / elapsed_total) * 60 if elapsed_total > 0 else 0
                            progress.update(task, rate=rate)
                            
                        except Exception as e:
                            results["failed"] += 1
                            progress.update(task, advance=1)
        
        else:
            # Process without progress (for background)
            with ProcessPoolExecutor(max_workers=self.workers) as executor:
                futures = [executor.submit(process_file_simple, args) for args in args_list]
                
                for future in as_completed(futures):
                    try:
                        _, success, _, concepts = future.result()
                        if success:
                            results["processed"] += 1
                            results["total_concepts"] += concepts
                        else:
                            results["failed"] += 1
                    except:
                        results["failed"] += 1
        
        # Calculate final stats
        elapsed = (datetime.now() - results["start_time"]).total_seconds()
        results["time"] = elapsed
        results["rate"] = len(files) / elapsed if elapsed > 0 else 0
        results["success"] = results["failed"] == 0
        
        return results
    
    def show_results(self, results: Dict[str, Any]):
        """Display results"""
        table = Table(title="⚡ Ultra-Fast Processing Results", box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Files Processed", f"{results['processed']:,}")
        table.add_row("Files Failed", f"{results['failed']:,}")
        table.add_row("Total Concepts", f"{results['total_concepts']:,}")
        table.add_row("Time Elapsed", f"{results['time']:.1f}s")
        table.add_row("Processing Rate", f"{results['rate']:.1f} files/sec")
        table.add_row("Effective Rate", f"{results['rate'] * 60:.0f} files/min")
        
        # Projection for 30k files
        if results['rate'] > 0:
            time_30k = 30000 / results['rate']
            if time_30k < 3600:
                projection = f"{time_30k/60:.1f} minutes"
            else:
                projection = f"{time_30k/3600:.1f} hours"
            table.add_row("Projected for 30k", projection)
        
        console.print("\n")
        console.print(table)


def main():
    """CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ultra-fast MetaMap batch processing")
    parser.add_argument("input_dir", help="Input directory")
    parser.add_argument("output_dir", help="Output directory") 
    parser.add_argument("--workers", type=int, help="Number of parallel workers")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar")
    
    args = parser.parse_args()
    
    # Show intro
    if not args.no_progress:
        console.print("\n[bold cyan]⚡ Ultra-Fast Batch Processing[/bold cyan]")
        console.print(f"Workers: {args.workers or mp.cpu_count() - 1}")
        console.print(f"Input: {args.input_dir}")
        console.print(f"Output: {args.output_dir}\n")
    
    # Process
    processor = UltraFastBatchProcessor(
        args.input_dir, 
        args.output_dir,
        args.workers
    )
    
    results = processor.process_parallel(show_progress=not args.no_progress)
    
    if not args.no_progress:
        processor.show_results(results)
    
    return 0 if results["success"] else 1


if __name__ == "__main__":
    sys.exit(main())