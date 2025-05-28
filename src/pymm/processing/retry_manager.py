"""Retry management with exponential backoff"""
import time
import logging
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from pathlib import Path

@dataclass
class RetryRecord:
    """Record of retry attempts for a file"""
    file_path: str
    attempts: int = 0
    last_error: Optional[str] = None
    last_attempt_time: Optional[float] = None
    backoff_until: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RetryRecord':
        """Create from dictionary"""
        return cls(**data)

class RetryManager:
    """Manages retry queue with exponential backoff"""
    
    def __init__(self, config, state_manager):
        self.config = config
        self.state_manager = state_manager
        self.retry_queue: Dict[str, RetryRecord] = {}
        self.max_attempts = config.get("retry_max_attempts", 3)
        self.backoff_base = config.get("retry_backoff_base", 2)
        self.max_backoff = 3600  # 1 hour max
        self.logger = logging.getLogger("RetryManager")
        self._load_retry_state()
    
    def _load_retry_state(self):
        """Load retry state from persistent storage"""
        state = self.state_manager._state
        retry_data = state.get('retry_queue', {})
        
        self.retry_queue = {
            path: RetryRecord.from_dict(record) 
            for path, record in retry_data.items()
        }
        
        self.logger.info(f"Loaded {len(self.retry_queue)} files in retry queue")
    
    def should_retry(self, file_path: str) -> bool:
        """Check if file should be retried"""
        if file_path not in self.retry_queue:
            return True
            
        record = self.retry_queue[file_path]
        
        # Check max attempts
        if record.attempts >= self.max_attempts:
            self.logger.debug(f"{file_path}: Max attempts ({self.max_attempts}) reached")
            return False
            
        # Check backoff period
        if record.backoff_until and time.time() < record.backoff_until:
            remaining = record.backoff_until - time.time()
            self.logger.debug(f"{file_path}: In backoff period ({remaining:.1f}s remaining)")
            return False
            
        return True
    
    def get_retry_files(self) -> list:
        """Get list of files ready for retry"""
        ready_files = []
        
        for file_path, record in self.retry_queue.items():
            if self.should_retry(file_path):
                ready_files.append(file_path)
                
        return ready_files
    
    def record_attempt(self, file_path: str):
        """Record that we're attempting to process a file"""
        if file_path not in self.retry_queue:
            self.retry_queue[file_path] = RetryRecord(file_path)
            
        record = self.retry_queue[file_path]
        record.attempts += 1
        record.last_attempt_time = time.time()
        
        self.logger.info(f"{file_path}: Attempt {record.attempts}/{self.max_attempts}")
    
    def record_failure(self, file_path: str, error: str):
        """Record a processing failure"""
        if file_path not in self.retry_queue:
            self.retry_queue[file_path] = RetryRecord(file_path)
            
        record = self.retry_queue[file_path]
        record.last_error = str(error)[:500]  # Truncate long errors
        record.last_attempt_time = time.time()
        
        # Calculate exponential backoff
        backoff_seconds = min(
            self.backoff_base ** record.attempts,
            self.max_backoff
        )
        record.backoff_until = time.time() + backoff_seconds
        
        self.logger.warning(
            f"{file_path}: Failed attempt {record.attempts}, "
            f"retry in {backoff_seconds}s. Error: {record.last_error}"
        )
        
        # Persist state
        self._save_retry_state()
    
    def record_success(self, file_path: str):
        """Record successful processing"""
        if file_path in self.retry_queue:
            del self.retry_queue[file_path]
            self.logger.info(f"{file_path}: Removed from retry queue after success")
            self._save_retry_state()
    
    def _save_retry_state(self):
        """Persist retry state"""
        self.state_manager.add_to_retry_queue(
            file_path=None,  # Not used in new implementation
            attempt=0,       # Not used
            error=""         # Not used
        )
        
        # Direct update of state
        self.state_manager._state['retry_queue'] = {
            path: record.to_dict()
            for path, record in self.retry_queue.items()
        }
        self.state_manager.save()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get retry queue statistics"""
        total_files = len(self.retry_queue)
        
        if not total_files:
            return {
                "total_files": 0,
                "ready_for_retry": 0,
                "in_backoff": 0,
                "exhausted": 0
            }
        
        ready = 0
        in_backoff = 0
        exhausted = 0
        
        for record in self.retry_queue.values():
            if record.attempts >= self.max_attempts:
                exhausted += 1
            elif record.backoff_until and time.time() < record.backoff_until:
                in_backoff += 1
            else:
                ready += 1
                
        return {
            "total_files": total_files,
            "ready_for_retry": ready,
            "in_backoff": in_backoff,
            "exhausted": exhausted,
            "avg_attempts": sum(r.attempts for r in self.retry_queue.values()) / total_files
        }
    
    def clear_exhausted(self) -> int:
        """Remove files that have exhausted retry attempts"""
        exhausted = [
            path for path, record in self.retry_queue.items()
            if record.attempts >= self.max_attempts
        ]
        
        for path in exhausted:
            del self.retry_queue[path]
            
        if exhausted:
            self._save_retry_state()
            self.logger.info(f"Cleared {len(exhausted)} exhausted files from retry queue")
            
        return len(exhausted)
    
    def reset_file(self, file_path: str):
        """Reset retry count for a specific file"""
        if file_path in self.retry_queue:
            self.retry_queue[file_path] = RetryRecord(file_path)
            self._save_retry_state()
            self.logger.info(f"Reset retry count for {file_path}")
    
    def clear_queue(self):
        """Clear all files from the retry queue"""
        count = len(self.retry_queue)
        self.retry_queue.clear()
        self._save_retry_state()
        self.logger.info(f"Cleared {count} files from retry queue")
        return count
    
    def clear_retry_queue(self):
        """Clear all files from the retry queue (alias for clear_queue)"""
        return self.clear_queue()