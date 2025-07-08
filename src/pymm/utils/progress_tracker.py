"""Progress tracking utilities for batch processing"""
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from pathlib import Path
import threading

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Track processing progress with detailed statistics"""
    
    def __init__(self, total_files: int, log_dir: Optional[Path] = None):
        self.total_files = total_files
        self.start_time = time.time()
        self.log_dir = log_dir
        
        # Counters (thread-safe)
        self._lock = threading.Lock()
        self.processed = 0
        self.failed = 0
        self.skipped = 0
        self.retried = 0
        self.in_progress = 0
        
        # Detailed tracking
        self.file_times = []  # List of processing times
        self.errors = {}  # Map of file -> error
        self.concept_counts = []  # List of concept counts per file
        
        # Setup progress log
        if log_dir:
            self._setup_progress_log()
    
    def _setup_progress_log(self):
        """Setup dedicated progress log file"""
        if not self.log_dir:
            return
            
        self.log_dir.mkdir(exist_ok=True)
        progress_log = self.log_dir / f"progress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # Create file handler for progress
        handler = logging.FileHandler(progress_log)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        handler.setFormatter(formatter)
        
        # Create dedicated progress logger
        self.progress_logger = logging.getLogger('progress')
        self.progress_logger.addHandler(handler)
        self.progress_logger.setLevel(logging.INFO)
        
        # Log header
        self.progress_logger.info(f"Progress tracking started for {self.total_files} files")
        self.progress_logger.info("-" * 80)
    
    def start_file(self, filename: str):
        """Mark file as started"""
        with self._lock:
            self.in_progress += 1
    
    def complete_file(self, filename: str, elapsed_time: float, concept_count: int = 0):
        """Mark file as completed"""
        with self._lock:
            self.processed += 1
            self.in_progress = max(0, self.in_progress - 1)
            self.file_times.append(elapsed_time)
            self.concept_counts.append(concept_count)
            
        # Log progress
        self._log_progress(f"COMPLETED: {filename} in {elapsed_time:.2f}s with {concept_count} concepts")
    
    def fail_file(self, filename: str, error: str):
        """Mark file as failed"""
        with self._lock:
            self.failed += 1
            self.in_progress = max(0, self.in_progress - 1)
            self.errors[filename] = error
            
        # Log failure
        self._log_progress(f"FAILED: {filename} - {error}")
    
    def skip_file(self, filename: str, reason: str = "already processed"):
        """Mark file as skipped"""
        with self._lock:
            self.skipped += 1
            
        # Log skip
        self._log_progress(f"SKIPPED: {filename} - {reason}")
    
    def retry_file(self, filename: str):
        """Mark file as being retried"""
        with self._lock:
            self.retried += 1
            
        # Log retry
        self._log_progress(f"RETRYING: {filename}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics"""
        with self._lock:
            elapsed = time.time() - self.start_time
            completed = self.processed + self.failed + self.skipped
            remaining = self.total_files - completed
            
            # Calculate rates
            if elapsed > 0:
                rate = self.processed / elapsed
                avg_time = sum(self.file_times) / len(self.file_times) if self.file_times else 0
                eta = remaining / rate if rate > 0 else 0
            else:
                rate = 0
                avg_time = 0
                eta = 0
            
            # Calculate concept statistics
            total_concepts = sum(self.concept_counts)
            avg_concepts = total_concepts / len(self.concept_counts) if self.concept_counts else 0
            
            return {
                "total_files": self.total_files,
                "processed": self.processed,
                "failed": self.failed,
                "skipped": self.skipped,
                "retried": self.retried,
                "in_progress": self.in_progress,
                "remaining": remaining,
                "elapsed_time": elapsed,
                "rate": rate,
                "avg_time_per_file": avg_time,
                "eta_seconds": eta,
                "eta_formatted": str(timedelta(seconds=int(eta))),
                "total_concepts": total_concepts,
                "avg_concepts_per_file": avg_concepts,
                "error_count": len(self.errors),
                "progress_percentage": (completed / self.total_files * 100) if self.total_files > 0 else 0
            }
    
    def _log_progress(self, message: str):
        """Log progress message"""
        if hasattr(self, 'progress_logger'):
            stats = self.get_stats()
            progress_msg = (
                f"{message} | "
                f"Progress: {stats['processed'] + stats['failed'] + stats['skipped']}/{self.total_files} "
                f"({stats['progress_percentage']:.1f}%) | "
                f"Success: {stats['processed']} | "
                f"Failed: {stats['failed']} | "
                f"Rate: {stats['rate']:.2f} files/s"
            )
            self.progress_logger.info(progress_msg)
    
    def get_summary(self) -> str:
        """Get formatted summary"""
        stats = self.get_stats()
        
        summary = f"""
Processing Summary
==================
Total Files: {stats['total_files']}
Processed: {stats['processed']}
Failed: {stats['failed']}
Skipped: {stats['skipped']}
Retried: {stats['retried']}

Performance
-----------
Elapsed Time: {str(timedelta(seconds=int(stats['elapsed_time'])))}
Processing Rate: {stats['rate']:.2f} files/second
Average Time per File: {stats['avg_time_per_file']:.2f} seconds

Concept Extraction
------------------
Total Concepts: {stats['total_concepts']:,}
Average Concepts per File: {stats['avg_concepts_per_file']:.1f}
"""
        
        if stats['failed'] > 0:
            summary += f"\nFailed Files ({stats['failed']}):\n"
            summary += "-" * 40 + "\n"
            for filename, error in list(self.errors.items())[:10]:  # Show first 10
                summary += f"  - {filename}: {error}\n"
            if len(self.errors) > 10:
                summary += f"  ... and {len(self.errors) - 10} more\n"
        
        return summary
    
    def save_summary(self, output_path: Path):
        """Save summary to file"""
        summary = self.get_summary()
        with open(output_path, 'w') as f:
            f.write(summary)
            f.write("\n\nDetailed Error Log:\n")
            f.write("=" * 80 + "\n")
            for filename, error in self.errors.items():
                f.write(f"\nFile: {filename}\n")
                f.write(f"Error: {error}\n")
                f.write("-" * 40 + "\n") 