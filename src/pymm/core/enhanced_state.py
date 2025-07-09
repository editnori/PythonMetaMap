"""Enhanced thread-safe state management for PythonMetaMap"""
import json
import time
import threading
import logging
from pathlib import Path
from typing import Dict, List, Set, Optional, Any
from datetime import datetime
import fcntl
import os
import tempfile

logger = logging.getLogger(__name__)


class AtomicStateManager:
    """Thread-safe state management with atomic operations"""
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.state_file = self.output_dir / ".processing_state.json"
        self.lock_file = self.output_dir / ".state.lock"
        self._local_lock = threading.Lock()
        
        # In-memory cache
        self._cache = {
            'processed': set(),
            'failed': set(),
            'in_progress': set(),
            'stats': {
                'total_files': 0,
                'start_time': None,
                'last_update': None
            }
        }
        
        # Load initial state
        self._load_state()
    
    def _acquire_file_lock(self, timeout: float = 5.0) -> Optional[Any]:
        """Acquire file-based lock for cross-process safety"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Create lock file if it doesn't exist
                lock_fd = os.open(str(self.lock_file), os.O_CREAT | os.O_WRONLY)
                
                # Try to acquire exclusive lock
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return lock_fd
                
            except (IOError, OSError):
                # Lock is held by another process
                time.sleep(0.1)
                try:
                    os.close(lock_fd)
                except:
                    pass
        
        return None
    
    def _release_file_lock(self, lock_fd: Any):
        """Release file-based lock"""
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
        except:
            pass
    
    def _load_state(self):
        """Load state from disk"""
        if not self.state_file.exists():
            return
        
        lock_fd = self._acquire_file_lock()
        if lock_fd is None:
            logger.warning("Failed to acquire lock for state loading")
            return
        
        try:
            with open(self.state_file, 'r') as f:
                data = json.load(f)
                
            self._cache['processed'] = set(data.get('processed', []))
            self._cache['failed'] = set(data.get('failed', []))
            self._cache['in_progress'] = set(data.get('in_progress', []))
            self._cache['stats'] = data.get('stats', self._cache['stats'])
            
            logger.info(f"Loaded state: {len(self._cache['processed'])} processed, "
                       f"{len(self._cache['failed'])} failed")
                       
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
        finally:
            self._release_file_lock(lock_fd)
    
    def _save_state_atomic(self):
        """Save state atomically to prevent corruption"""
        # Create temporary file
        temp_file = self.state_file.with_suffix('.tmp')
        
        try:
            # Prepare data
            data = {
                'processed': list(self._cache['processed']),
                'failed': list(self._cache['failed']),
                'in_progress': list(self._cache['in_progress']),
                'stats': self._cache['stats'],
                'last_update': datetime.now().isoformat()
            }
            
            # Write to temporary file
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Atomically replace old file
            temp_file.replace(self.state_file)
            
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            # Clean up temp file
            if temp_file.exists():
                temp_file.unlink()
    
    def mark_processed(self, file_id: str):
        """Mark file as processed"""
        with self._local_lock:
            self._cache['processed'].add(file_id)
            self._cache['in_progress'].discard(file_id)
            self._cache['failed'].discard(file_id)
            
            # Batch saves - only save every 10 files or 5 seconds
            if len(self._cache['processed']) % 10 == 0:
                self._save_state_atomic()
    
    def mark_failed(self, file_id: str):
        """Mark file as failed"""
        with self._local_lock:
            self._cache['failed'].add(file_id)
            self._cache['in_progress'].discard(file_id)
            
            # Save immediately for failures
            self._save_state_atomic()
    
    def mark_in_progress(self, file_id: str):
        """Mark file as in progress"""
        with self._local_lock:
            self._cache['in_progress'].add(file_id)
    
    def is_processed(self, file_id: str) -> bool:
        """Check if file is already processed"""
        with self._local_lock:
            return file_id in self._cache['processed']
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        with self._local_lock:
            return {
                'processed': len(self._cache['processed']),
                'failed': len(self._cache['failed']),
                'in_progress': len(self._cache['in_progress']),
                **self._cache['stats']
            }
    
    def update_stats(self, **kwargs):
        """Update statistics"""
        with self._local_lock:
            self._cache['stats'].update(kwargs)
            self._save_state_atomic()
    
    def clear_state(self):
        """Clear all state"""
        with self._local_lock:
            self._cache['processed'].clear()
            self._cache['failed'].clear()
            self._cache['in_progress'].clear()
            self._save_state_atomic()
    
    def get_pending_files(self, all_files: List[str]) -> List[str]:
        """Get list of files that still need processing"""
        with self._local_lock:
            all_set = set(all_files)
            processed_set = self._cache['processed']
            return list(all_set - processed_set)
    
    def checkpoint(self):
        """Force save current state"""
        with self._local_lock:
            self._save_state_atomic()


class FileTracker:
    """Efficient file tracking using bloom filters for large datasets"""
    
    def __init__(self, expected_files: int = 100000):
        self.expected_files = expected_files
        self.processed = set()
        self.failed = set()
        self._lock = threading.Lock()
        
        # For very large datasets, consider using a bloom filter
        self._use_bloom = expected_files > 50000
        
        if self._use_bloom:
            try:
                from pybloom_live import BloomFilter
                self.bloom_processed = BloomFilter(
                    capacity=expected_files,
                    error_rate=0.001
                )
                self.bloom_failed = BloomFilter(
                    capacity=expected_files // 10,
                    error_rate=0.001
                )
            except ImportError:
                logger.warning("pybloom_live not available, using sets")
                self._use_bloom = False
    
    def mark_processed(self, file_id: str):
        """Mark file as processed"""
        with self._lock:
            if self._use_bloom:
                self.bloom_processed.add(file_id)
            self.processed.add(file_id)
    
    def mark_failed(self, file_id: str):
        """Mark file as failed"""
        with self._lock:
            if self._use_bloom:
                self.bloom_failed.add(file_id)
            self.failed.add(file_id)
    
    def is_processed(self, file_id: str) -> bool:
        """Check if file is processed"""
        with self._lock:
            if self._use_bloom:
                # Bloom filter for quick negative checks
                if file_id not in self.bloom_processed:
                    return False
            return file_id in self.processed
    
    def get_stats(self) -> Dict[str, int]:
        """Get tracking statistics"""
        with self._lock:
            return {
                'processed': len(self.processed),
                'failed': len(self.failed)
            } 