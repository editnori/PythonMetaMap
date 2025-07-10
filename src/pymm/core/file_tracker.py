"""Unified file tracking system for PythonMetaMap"""
import os
import json
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field, asdict
from collections import defaultdict

from .config import PyMMConfig


@dataclass
class FileRecord:
    """Record of a processed file"""
    input_path: str
    output_path: str
    input_hash: str
    process_date: str
    file_size: int
    status: str  # 'completed', 'failed', 'in_progress'
    error_message: Optional[str] = None
    concepts_found: int = 0
    processing_time: float = 0.0
    

@dataclass 
class ProcessingManifest:
    """Manifest tracking all processed files"""
    version: str = "1.0"
    created: str = field(default_factory=lambda: datetime.now().isoformat())
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())
    files: Dict[str, FileRecord] = field(default_factory=dict)
    statistics: Dict[str, int] = field(default_factory=lambda: {
        "total_processed": 0,
        "total_failed": 0,
        "total_concepts": 0
    })


class UnifiedFileTracker:
    """Unified tracking system for input/output file management"""
    
    def __init__(self, config: PyMMConfig = None):
        self.config = config or PyMMConfig()
        
        # Set up unified directories
        self.base_dir = Path(self.config.get('base_data_dir', './pymm_data'))
        self.input_dir = self.base_dir / 'input'
        self.output_dir = self.base_dir / 'output'
        self.manifest_path = self.base_dir / 'processing_manifest.json'
        
        # Create directories
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load manifest
        self.manifest = self._load_manifest()
        
    def _load_manifest(self) -> ProcessingManifest:
        """Load or create processing manifest"""
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, 'r') as f:
                    data = json.load(f)
                    # Convert file records
                    files = {}
                    for path, record_data in data.get('files', {}).items():
                        files[path] = FileRecord(**record_data)
                    data['files'] = files
                    return ProcessingManifest(**data)
            except Exception:
                pass
        return ProcessingManifest()
        
    def save_manifest(self):
        """Save manifest to disk"""
        # Convert to dict for JSON serialization
        data = asdict(self.manifest)
        data['files'] = {
            path: asdict(record) 
            for path, record in self.manifest.files.items()
        }
        
        with open(self.manifest_path, 'w') as f:
            json.dump(data, f, indent=2)
            
    def get_file_hash(self, file_path: Path) -> str:
        """Calculate file hash for change detection"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
        
    def get_unprocessed_files(self, rescan: bool = False) -> List[Path]:
        """Get list of files that haven't been processed yet"""
        unprocessed = []
        
        # Scan input directory
        for file_path in self.input_dir.glob("**/*.txt"):
            relative_path = str(file_path.relative_to(self.input_dir))
            
            # Check if file is in manifest
            if relative_path not in self.manifest.files:
                unprocessed.append(file_path)
            elif rescan:
                # Check if file has changed
                current_hash = self.get_file_hash(file_path)
                record = self.manifest.files[relative_path]
                if current_hash != record.input_hash:
                    unprocessed.append(file_path)
                    
        return sorted(unprocessed)
        
    def get_processed_files(self) -> List[Tuple[Path, FileRecord]]:
        """Get list of successfully processed files"""
        processed = []
        
        for relative_path, record in self.manifest.files.items():
            if record.status == 'completed':
                input_path = self.input_dir / relative_path
                if input_path.exists():
                    processed.append((input_path, record))
                    
        return processed
        
    def get_failed_files(self) -> List[Tuple[Path, FileRecord]]:
        """Get list of failed files"""
        failed = []
        
        for relative_path, record in self.manifest.files.items():
            if record.status == 'failed':
                input_path = self.input_dir / relative_path
                if input_path.exists():
                    failed.append((input_path, record))
                    
        return failed
        
    def mark_file_started(self, input_path: Path) -> str:
        """Mark a file as started processing"""
        relative_path = str(input_path.relative_to(self.input_dir))
        
        # Calculate expected output path
        output_path = self.output_dir / relative_path.replace('.txt', '_processed.csv')
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Create record
        record = FileRecord(
            input_path=str(input_path),
            output_path=str(output_path),
            input_hash=self.get_file_hash(input_path),
            process_date=datetime.now().isoformat(),
            file_size=input_path.stat().st_size,
            status='in_progress'
        )
        
        self.manifest.files[relative_path] = record
        self.manifest.last_updated = datetime.now().isoformat()
        self.save_manifest()
        
        return str(output_path)
        
    def mark_file_completed(self, input_path: Path, concepts_found: int, processing_time: float):
        """Mark a file as successfully processed"""
        relative_path = str(input_path.relative_to(self.input_dir))
        
        if relative_path in self.manifest.files:
            record = self.manifest.files[relative_path]
            record.status = 'completed'
            record.concepts_found = concepts_found
            record.processing_time = processing_time
            
            self.manifest.statistics['total_processed'] += 1
            self.manifest.statistics['total_concepts'] += concepts_found
            self.manifest.last_updated = datetime.now().isoformat()
            self.save_manifest()
            
    def mark_file_failed(self, input_path: Path, error_message: str):
        """Mark a file as failed"""
        relative_path = str(input_path.relative_to(self.input_dir))
        
        if relative_path in self.manifest.files:
            record = self.manifest.files[relative_path]
            record.status = 'failed'
            record.error_message = error_message
            
            self.manifest.statistics['total_failed'] += 1
            self.manifest.last_updated = datetime.now().isoformat()
            self.save_manifest()
            
    def get_processing_summary(self) -> Dict[str, any]:
        """Get summary of processing status"""
        total_files = len(list(self.input_dir.glob("**/*.txt")))
        processed = len([r for r in self.manifest.files.values() if r.status == 'completed'])
        failed = len([r for r in self.manifest.files.values() if r.status == 'failed'])
        in_progress = len([r for r in self.manifest.files.values() if r.status == 'in_progress'])
        unprocessed = total_files - len(self.manifest.files)
        
        return {
            'total_files': total_files,
            'processed': processed,
            'failed': failed,
            'in_progress': in_progress,
            'unprocessed': unprocessed,
            'total_concepts': self.manifest.statistics['total_concepts'],
            'last_updated': self.manifest.last_updated
        }
        
    def suggest_batch_size(self, target_files: Optional[int] = None) -> Tuple[List[Path], str]:
        """Suggest files to process based on various strategies"""
        unprocessed = self.get_unprocessed_files()
        failed = [path for path, _ in self.get_failed_files()]
        
        if not unprocessed and not failed:
            return [], "All files have been processed successfully!"
            
        # Strategy 1: Process specific number of files
        if target_files and target_files > 0:
            files_to_process = unprocessed[:target_files]
            if len(files_to_process) < target_files and failed:
                # Add failed files if we need more
                files_to_process.extend(failed[:target_files - len(files_to_process)])
            message = f"Selected {len(files_to_process)} files to process"
            return files_to_process, message
            
        # Strategy 2: Process all unprocessed files
        if unprocessed:
            message = f"Found {len(unprocessed)} unprocessed files"
            return unprocessed, message
            
        # Strategy 3: Retry failed files
        if failed:
            message = f"Found {len(failed)} failed files to retry"
            return failed, message
            
        return [], "No files to process"
        
    def cleanup_orphaned_outputs(self) -> int:
        """Remove output files that don't have corresponding completed records"""
        cleaned = 0
        
        # Get all output files
        for output_file in self.output_dir.glob("**/*.csv"):
            found = False
            for record in self.manifest.files.values():
                if Path(record.output_path) == output_file and record.status == 'completed':
                    found = True
                    break
                    
            if not found:
                output_file.unlink()
                cleaned += 1
                
        return cleaned