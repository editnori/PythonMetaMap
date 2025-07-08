"""Retry management for failed files"""
import time
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from ..core.state import StateManager

logger = logging.getLogger(__name__)

class RetryManager:
    """Manages retry logic for failed files"""
    
    def __init__(self, config: Dict[str, Any], state_manager: StateManager):
        self.config = config
        self.state_manager = state_manager
        self.max_attempts = config.get("retry_max_attempts", 3)
        self.base_delay = config.get("retry_base_delay", 5)
        self.max_delay = config.get("retry_max_delay", 60)
        self.exponential_backoff = config.get("retry_exponential_backoff", True)
        
    def should_retry(self, file_path: str) -> bool:
        """Check if file should be retried"""
        retry_info = self.state_manager.get_retry_info(file_path)
        
        if not retry_info:
            return True  # First retry
            
        attempts = retry_info.get("attempts", 0)
        
        # Check if we've exceeded max attempts
        if attempts >= self.max_attempts:
            logger.warning(f"File {file_path} has exceeded max retry attempts ({self.max_attempts})")
            return False
            
        # Check if enough time has passed for backoff
        if self.exponential_backoff:
            delay = min(self.base_delay * (2 ** attempts), self.max_delay)
            last_attempt = retry_info.get("last_attempt")
            if last_attempt:
                from datetime import datetime
                last_time = datetime.fromisoformat(last_attempt)
                elapsed = (datetime.now() - last_time).total_seconds()
                if elapsed < delay:
                    logger.debug(f"File {file_path} in backoff period (need {delay}s, elapsed {elapsed}s)")
                    return False
        
        return True
    
    def record_retry_attempt(self, file_path: str, error: str):
        """Record a retry attempt"""
        retry_info = self.state_manager.get_retry_info(file_path)
        attempts = retry_info.get("attempts", 0) + 1 if retry_info else 1
        
        self.state_manager.add_to_retry_queue(file_path, attempts, error)
        logger.info(f"Recorded retry attempt {attempts} for {file_path}")
    
    def get_retry_delay(self, file_path: str) -> float:
        """Get delay before next retry"""
        if not self.exponential_backoff:
            return self.base_delay
            
        retry_info = self.state_manager.get_retry_info(file_path)
        attempts = retry_info.get("attempts", 0) if retry_info else 0
        
        delay = min(self.base_delay * (2 ** attempts), self.max_delay)
        return delay
    
    def clear_retry_queue(self) -> int:
        """Clear all files from retry queue"""
        count = 0
        with self.state_manager._lock:
            retry_files = list(self.state_manager._state["retry_queue"].keys())
            for file_path in retry_files:
                self.state_manager._state["retry_queue"].pop(file_path, None)
                count += 1
            
            if count > 0:
                self.state_manager.save()
                
        logger.info(f"Cleared {count} files from retry queue")
        return count
    
    def get_retryable_files(self, failed_files: List[str]) -> List[str]:
        """Filter failed files to get those eligible for retry"""
        retryable = []
        
        for file_path in failed_files:
            if self.should_retry(file_path):
                retryable.append(file_path)
            else:
                logger.info(f"Skipping retry for {file_path} (max attempts reached or in backoff)")
        
        return retryable
    
    def retry_failed_files(self, failed_files: List[str], process_func) -> Dict[str, Any]:
        """Retry processing of failed files
        
        Args:
            failed_files: List of failed file paths
            process_func: Function to process a single file
            
        Returns:
            Dictionary with retry results
        """
        results = {
            "attempted": 0,
            "recovered": 0,
            "still_failed": [],
            "skipped": []
        }
        
        # Filter to get retryable files
        retryable_files = self.get_retryable_files(failed_files)
        
        # Track skipped files
        results["skipped"] = [f for f in failed_files if f not in retryable_files]
        
        for file_path in retryable_files:
            # Get delay and wait if needed
            delay = self.get_retry_delay(file_path)
            if delay > 0:
                logger.info(f"Waiting {delay}s before retrying {file_path}")
                time.sleep(delay)
            
            results["attempted"] += 1
            
            try:
                # Attempt to process the file
                success, elapsed, error = process_func(file_path)
                
                if success:
                    results["recovered"] += 1
                    # Clear from retry queue
                    self.state_manager._state["retry_queue"].pop(file_path, None)
                    logger.info(f"Successfully recovered {file_path} on retry")
                else:
                    results["still_failed"].append(file_path)
                    self.record_retry_attempt(file_path, error or "Unknown error")
                    
            except Exception as e:
                results["still_failed"].append(file_path)
                self.record_retry_attempt(file_path, str(e))
                logger.error(f"Exception during retry of {file_path}: {e}")
        
        return results
    
    def get_retry_summary(self) -> Dict[str, Any]:
        """Get summary of retry queue status"""
        with self.state_manager._lock:
            retry_queue = self.state_manager._state.get("retry_queue", {})
            
            total_files = len(retry_queue)
            max_attempts_reached = 0
            in_backoff = 0
            ready_for_retry = 0
            
            for file_path, info in retry_queue.items():
                attempts = info.get("attempts", 0)
                
                if attempts >= self.max_attempts:
                    max_attempts_reached += 1
                elif self.should_retry(file_path):
                    ready_for_retry += 1
                else:
                    in_backoff += 1
            
            return {
                "total_files": total_files,
                "max_attempts_reached": max_attempts_reached,
                "in_backoff": in_backoff,
                "ready_for_retry": ready_for_retry,
                "max_attempts_setting": self.max_attempts
            } 