"""State management for processing sessions"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from collections import Counter
import logging

class StateManager:
    """Manages persistent state for processing sessions"""
    
    STATE_FILE = ".pymm_state.json"
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.state_path = self.output_dir / self.STATE_FILE
        self._state = self._load_state()
    
    def _load_state(self) -> Dict[str, Any]:
        """Load state from file or create new"""
        if self.state_path.exists():
            try:
                with open(self.state_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logging.warning(f"Failed to load state: {e}, creating new")
        
        # Default state structure
        return {
            "started": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "completed_files": [],
            "failed_files": {},
            "retry_queue": {},
            "statistics": {
                "total_files": 0,
                "completed": 0,
                "failed": 0,
                "in_progress": 0,
                "total_concepts": 0,
                "unique_concepts": 0,
                "total_semantic_types": 0
            },
            "concept_tracking": {
                "concepts": {},  # {cui: {"name": str, "count": int}}
                "semantic_types": {},  # {type: count}
                "top_concepts": [],  # List of (cui, name, count) tuples
                "top_semantic_types": []  # List of (type, count) tuples
            },
            "session_id": os.getpid(),
            "version": "8.0.8"
        }
    
    def save(self):
        """Save current state to file"""
        self._state["last_updated"] = datetime.now().isoformat()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        with open(self.state_path, 'w') as f:
            json.dump(self._state, f, indent=2)
    
    def mark_completed(self, file_path: str):
        """Mark a file as completed"""
        if file_path not in self._state["completed_files"]:
            self._state["completed_files"].append(file_path)
            self._state["statistics"]["completed"] += 1
        
        # Remove from failed/retry if present
        self._state["failed_files"].pop(file_path, None)
        self._state["retry_queue"].pop(file_path, None)
        self.save()
    
    def mark_failed(self, file_path: str, error: str):
        """Mark a file as failed"""
        self._state["failed_files"][file_path] = {
            "error": error,
            "timestamp": datetime.now().isoformat()
        }
        self._state["statistics"]["failed"] += 1
        self.save()
    
    def add_to_retry_queue(self, file_path: str, attempt: int, error: str):
        """Add file to retry queue"""
        self._state["retry_queue"][file_path] = {
            "attempts": attempt,
            "last_error": error,
            "last_attempt": datetime.now().isoformat()
        }
        self.save()
    
    def get_retry_info(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Get retry information for a file"""
        return self._state["retry_queue"].get(file_path)
    
    def is_completed(self, file_path: str) -> bool:
        """Check if file is already completed"""
        return file_path in self._state["completed_files"]
    
    def get_statistics(self) -> Dict[str, int]:
        """Get processing statistics"""
        return self._state["statistics"].copy()
    
    def update_statistics(self, **kwargs):
        """Update statistics"""
        self._state["statistics"].update(kwargs)
        self.save()
    
    def track_concepts(self, concepts: List[Dict[str, Any]]):
        """Track concepts from processed file"""
        concept_data = self._state["concept_tracking"]
        
        for concept in concepts:
            cui = concept.get('cui', '')
            name = concept.get('preferred_name', '')
            semantic_types = concept.get('semantic_types', [])
            
            if cui:
                # Track concept
                if cui not in concept_data["concepts"]:
                    concept_data["concepts"][cui] = {"name": name, "count": 0}
                concept_data["concepts"][cui]["count"] += 1
                
                # Track semantic types
                for sem_type in semantic_types:
                    if sem_type:
                        concept_data["semantic_types"][sem_type] = concept_data["semantic_types"].get(sem_type, 0) + 1
        
        # Update statistics
        self._state["statistics"]["total_concepts"] = sum(c["count"] for c in concept_data["concepts"].values())
        self._state["statistics"]["unique_concepts"] = len(concept_data["concepts"])
        self._state["statistics"]["total_semantic_types"] = len(concept_data["semantic_types"])
        
        # Update top concepts (top 10)
        sorted_concepts = sorted(
            [(cui, data["name"], data["count"]) for cui, data in concept_data["concepts"].items()],
            key=lambda x: x[2],
            reverse=True
        )
        concept_data["top_concepts"] = sorted_concepts[:10]
        
        # Update top semantic types (top 10)
        sorted_types = sorted(
            concept_data["semantic_types"].items(),
            key=lambda x: x[1],
            reverse=True
        )
        concept_data["top_semantic_types"] = sorted_types[:10]
        
        self.save()
    
    def get_concept_statistics(self) -> Dict[str, Any]:
        """Get concept tracking statistics"""
        return {
            "total_concepts": self._state["statistics"]["total_concepts"],
            "unique_concepts": self._state["statistics"]["unique_concepts"],
            "total_semantic_types": self._state["statistics"]["total_semantic_types"],
            "top_concepts": self._state["concept_tracking"]["top_concepts"],
            "top_semantic_types": self._state["concept_tracking"]["top_semantic_types"]
        }
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get session information"""
        return {
            "started": self._state.get("started"),
            "last_updated": self._state.get("last_updated"),
            "session_id": self._state.get("session_id"),
            "version": self._state.get("version")
        }
    
    def clear(self):
        """Clear all state"""
        self._state = self._load_state()
        self.save()
    
    def reset_file_state(self, file_path: str):
        """Reset state for a specific file"""
        # Remove from completed files
        if file_path in self._state["completed_files"]:
            self._state["completed_files"].remove(file_path)
            self._state["statistics"]["completed"] = max(0, self._state["statistics"]["completed"] - 1)
        
        # Remove from failed files
        if file_path in self._state["failed_files"]:
            del self._state["failed_files"][file_path]
            self._state["statistics"]["failed"] = max(0, self._state["statistics"]["failed"] - 1)
        
        # Remove from retry queue
        if file_path in self._state["retry_queue"]:
            del self._state["retry_queue"][file_path]
        
        self.save()
    
    def reset(self):
        """Reset all state (alias for clear)"""
        self.clear()
    
    def export_summary(self) -> str:
        """Export a human-readable summary"""
        stats = self.get_statistics()
        session = self.get_session_info()
        concept_stats = self.get_concept_statistics()
        
        summary = f"""
PythonMetaMap Processing Summary
================================
Session ID: {session['session_id']}
Started: {session['started']}
Last Updated: {session['last_updated']}

Statistics:
-----------
Total Files: {stats['total_files']}
Completed: {stats['completed']}
Failed: {stats['failed']}
In Progress: {stats['in_progress']}

Concept Statistics:
------------------
Total Concepts: {concept_stats['total_concepts']}
Unique Concepts: {concept_stats['unique_concepts']}
Semantic Types: {concept_stats['total_semantic_types']}

Top 5 Concepts:
"""
        for i, (cui, name, count) in enumerate(concept_stats['top_concepts'][:5], 1):
            summary += f"{i}. {name} ({cui}): {count} occurrences\n"
        
        summary += "\nTop 5 Semantic Types:\n"
        for i, (sem_type, count) in enumerate(concept_stats['top_semantic_types'][:5], 1):
            summary += f"{i}. {sem_type}: {count} occurrences\n"
        
        summary += f"""
Failed Files: {len(self._state['failed_files'])}
Retry Queue: {len(self._state['retry_queue'])}
"""
        return summary