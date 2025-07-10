"""Unified job management system for PythonMetaMap"""
import os
import json
import time
import psutil
import subprocess
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Job status enumeration"""
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class JobType(Enum):
    """Job type enumeration"""
    BATCH = "batch"
    QUICK = "quick"
    CHUNKED = "chunked"
    ULTRA = "ultra"
    OPTIMIZED = "optimized"
    ANALYSIS = "analysis"


@dataclass
class JobInfo:
    """Job information dataclass"""
    job_id: str
    job_type: JobType
    status: JobStatus
    input_dir: str
    output_dir: str
    start_time: datetime
    end_time: Optional[datetime] = None
    pid: Optional[int] = None
    progress: Dict[str, Any] = None
    error: Optional[str] = None
    config: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['job_type'] = self.job_type.value
        data['status'] = self.status.value
        data['start_time'] = self.start_time.isoformat()
        if self.end_time:
            data['end_time'] = self.end_time.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'JobInfo':
        """Create from dictionary"""
        data['job_type'] = JobType(data['job_type'])
        data['status'] = JobStatus(data['status'])
        data['start_time'] = datetime.fromisoformat(data['start_time'])
        if data.get('end_time'):
            data['end_time'] = datetime.fromisoformat(data['end_time'])
        return cls(**data)


class JobManager:
    """Centralized job management system"""
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize job manager"""
        if config_dir is None:
            config_dir = Path.home() / ".pymm"
        
        self.config_dir = Path(config_dir)
        self.jobs_dir = self.config_dir / "jobs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        
        self.jobs_file = self.jobs_dir / "jobs.json"
        self.lock_file = self.jobs_dir / ".lock"
        
        self._jobs: Dict[str, JobInfo] = {}
        self._lock = threading.Lock()
        self._monitor_thread = None
        self._monitoring = False
        
        # Load existing jobs
        self._load_jobs()
        
        # Start monitoring
        self._start_monitoring()
    
    def _load_jobs(self):
        """Load jobs from persistent storage"""
        if self.jobs_file.exists():
            try:
                with open(self.jobs_file, 'r') as f:
                    data = json.load(f)
                    for job_id, job_data in data.items():
                        try:
                            self._jobs[job_id] = JobInfo.from_dict(job_data)
                        except Exception as e:
                            logger.error(f"Failed to load job {job_id}: {e}")
            except Exception as e:
                logger.error(f"Failed to load jobs file: {e}")
    
    def _save_jobs(self):
        """Save jobs to persistent storage"""
        try:
            data = {
                job_id: job.to_dict() 
                for job_id, job in self._jobs.items()
            }
            
            # Write to temp file first
            temp_file = self.jobs_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Atomic rename
            temp_file.replace(self.jobs_file)
            
        except Exception as e:
            logger.error(f"Failed to save jobs: {e}")
    
    def create_job(
        self,
        job_type: JobType,
        input_dir: str,
        output_dir: str,
        config: Optional[Dict[str, Any]] = None,
        job_id: Optional[str] = None
    ) -> str:
        """Create a new job"""
        with self._lock:
            if not job_id:
                job_id = f"{job_type.value}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:17]}"
            
            job = JobInfo(
                job_id=job_id,
                job_type=job_type,
                status=JobStatus.QUEUED,
                input_dir=input_dir,
                output_dir=output_dir,
                start_time=datetime.now(),
                progress={
                    'total_files': 0,
                    'processed': 0,
                    'failed': 0,
                    'percentage': 0
                },
                config=config or {}
            )
            
            self._jobs[job_id] = job
            self._save_jobs()
            
            logger.info(f"Created job {job_id} of type {job_type.value}")
            return job_id
    
    def start_job(self, job_id: str, pid: Optional[int] = None) -> bool:
        """Start a job"""
        with self._lock:
            if job_id not in self._jobs:
                return False
            
            job = self._jobs[job_id]
            job.status = JobStatus.RUNNING
            job.pid = pid or os.getpid()
            
            self._save_jobs()
            logger.info(f"Started job {job_id} with PID {job.pid}")
            return True
    
    def update_progress(self, job_id: str, progress: Dict[str, Any]):
        """Update job progress"""
        with self._lock:
            if job_id not in self._jobs:
                return
            
            job = self._jobs[job_id]
            job.progress.update(progress)
            
            # Calculate percentage if not provided
            if 'percentage' not in progress and 'total_files' in progress and 'processed' in progress:
                total = progress.get('total_files', 0)
                processed = progress.get('processed', 0)
                if total > 0:
                    job.progress['percentage'] = int((processed / total) * 100)
            
            self._save_jobs()
    
    def complete_job(self, job_id: str, error: Optional[str] = None):
        """Complete a job"""
        with self._lock:
            if job_id not in self._jobs:
                return
            
            job = self._jobs[job_id]
            job.status = JobStatus.FAILED if error else JobStatus.COMPLETED
            job.end_time = datetime.now()
            job.error = error
            
            self._save_jobs()
            logger.info(f"Completed job {job_id} with status {job.status.value}")
    
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a job"""
        with self._lock:
            if job_id not in self._jobs:
                return False
            
            job = self._jobs[job_id]
            
            # Try to kill the process if running
            if job.pid and job.status == JobStatus.RUNNING:
                try:
                    process = psutil.Process(job.pid)
                    process.terminate()
                    time.sleep(1)
                    if process.is_running():
                        process.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            
            job.status = JobStatus.CANCELLED
            job.end_time = datetime.now()
            
            self._save_jobs()
            logger.info(f"Cancelled job {job_id}")
            return True
    
    def get_job(self, job_id: str) -> Optional[JobInfo]:
        """Get job information"""
        with self._lock:
            return self._jobs.get(job_id)
    
    def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        job_type: Optional[JobType] = None,
        limit: int = 50
    ) -> List[JobInfo]:
        """List jobs with optional filtering"""
        with self._lock:
            jobs = list(self._jobs.values())
            
            # Filter by status
            if status:
                jobs = [j for j in jobs if j.status == status]
            
            # Filter by type
            if job_type:
                jobs = [j for j in jobs if j.job_type == job_type]
            
            # Sort by start time (most recent first)
            jobs.sort(key=lambda j: j.start_time, reverse=True)
            
            return jobs[:limit]
    
    def get_active_jobs(self) -> List[JobInfo]:
        """Get all active (running) jobs"""
        return self.list_jobs(status=JobStatus.RUNNING)
    
    def get_job_stats(self, job_id: str) -> Dict[str, Any]:
        """Get detailed job statistics"""
        job = self.get_job(job_id)
        if not job:
            return {}
        
        stats = {
            'job_id': job.job_id,
            'type': job.job_type.value,
            'status': job.status.value,
            'duration': None,
            'progress': job.progress,
            'resource_usage': {}
        }
        
        # Calculate duration
        if job.end_time:
            duration = (job.end_time - job.start_time).total_seconds()
        else:
            duration = (datetime.now() - job.start_time).total_seconds()
        
        stats['duration'] = duration
        
        # Get resource usage if process is running
        if job.pid and job.status == JobStatus.RUNNING:
            try:
                process = psutil.Process(job.pid)
                stats['resource_usage'] = {
                    'cpu_percent': process.cpu_percent(interval=0.1),
                    'memory_mb': process.memory_info().rss / 1024 / 1024,
                    'num_threads': process.num_threads()
                }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        return stats
    
    def cleanup_old_jobs(self, days: int = 7):
        """Clean up old completed/failed jobs"""
        with self._lock:
            cutoff = datetime.now().timestamp() - (days * 24 * 3600)
            
            to_remove = []
            for job_id, job in self._jobs.items():
                if job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
                    if job.end_time and job.end_time.timestamp() < cutoff:
                        to_remove.append(job_id)
            
            for job_id in to_remove:
                del self._jobs[job_id]
            
            if to_remove:
                self._save_jobs()
                logger.info(f"Cleaned up {len(to_remove)} old jobs")
    
    def _start_monitoring(self):
        """Start background monitoring thread"""
        if self._monitoring:
            return
        
        self._monitoring = True
        self._monitor_thread = threading.Thread(target=self._monitor_jobs, daemon=True)
        self._monitor_thread.start()
    
    def _monitor_jobs(self):
        """Monitor running jobs"""
        while self._monitoring:
            try:
                with self._lock:
                    for job_id, job in list(self._jobs.items()):
                        if job.status == JobStatus.RUNNING and job.pid:
                            # Check if process is still running
                            try:
                                process = psutil.Process(job.pid)
                                if not process.is_running():
                                    # Process ended, check exit code
                                    job.status = JobStatus.COMPLETED
                                    job.end_time = datetime.now()
                                    self._save_jobs()
                            except psutil.NoSuchProcess:
                                # Process doesn't exist
                                job.status = JobStatus.FAILED
                                job.end_time = datetime.now()
                                job.error = "Process terminated unexpectedly"
                                self._save_jobs()
                
                # Clean up old jobs periodically
                if time.time() % 3600 < 30:  # Once per hour
                    self.cleanup_old_jobs()
                
            except Exception as e:
                logger.error(f"Error in job monitor: {e}")
            
            time.sleep(5)  # Check every 5 seconds
    
    def stop(self):
        """Stop the job manager"""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)


# Global job manager instance
_job_manager: Optional[JobManager] = None


def get_job_manager() -> JobManager:
    """Get the global job manager instance"""
    global _job_manager
    if _job_manager is None:
        _job_manager = JobManager()
    return _job_manager