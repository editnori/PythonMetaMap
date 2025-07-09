"""MetaMap Instance Pool for efficient instance management"""
import os
import time
import logging
import threading
from typing import Tuple, Optional, Dict, Any
from queue import Queue, Empty
from pathlib import Path

from ..pymm import Metamap as PyMetaMap
from ..core.config import PyMMConfig

logger = logging.getLogger(__name__)


class MetaMapInstancePool:
    """Pool manager for MetaMap instances with dynamic scaling"""
    
    def __init__(self, config: PyMMConfig, max_instances: Optional[int] = None):
        self.config = config
        self.metamap_path = config.get("metamap_binary_path")
        
        # Calculate optimal pool size
        if max_instances is None:
            import psutil
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024**3)
            
            # Conservative: 1 instance per 2GB RAM, capped by CPU count
            max_instances = min(
                max(2, int(memory_gb / 2)),
                config.get("max_parallel_workers", cpu_count),
                cpu_count
            )
        
        self.max_instances = max_instances
        self.instances = Queue(maxsize=max_instances)
        self.instance_count = 0
        self.lock = threading.Lock()
        self.debug = config.get("debug", False)
        
        # Port management
        self.base_tagger_port = config.get("tagger_port", 1795)
        self.base_wsd_port = config.get("wsd_port", 5554)
        self.port_offset = 0
        
        # Stats
        self.stats = {
            "created": 0,
            "reused": 0,
            "errors": 0,
            "active": 0
        }
        
        logger.info(f"Initialized MetaMapInstancePool with max_instances={self.max_instances}")
    
    def _create_instance(self) -> Tuple[int, PyMetaMap]:
        """Create a new MetaMap instance with unique ports"""
        with self.lock:
            instance_id = self.instance_count
            self.instance_count += 1
            
            # Calculate unique ports for this instance
            tagger_port = self.base_tagger_port + (instance_id * 10)
            wsd_port = self.base_wsd_port + (instance_id * 10)
            
            self.stats["created"] += 1
        
        try:
            # Create MetaMap instance
            mm_instance = PyMetaMap(
                self.metamap_path,
                debug=self.debug,
                tagger_port=tagger_port,
                wsd_port=wsd_port
            )
            
            logger.debug(f"Created MetaMap instance {instance_id} (ports: {tagger_port}, {wsd_port})")
            return (instance_id, mm_instance)
            
        except Exception as e:
            logger.error(f"Failed to create MetaMap instance: {e}")
            with self.lock:
                self.stats["errors"] += 1
            raise
    
    def get_instance(self, timeout: float = 30.0) -> Tuple[int, PyMetaMap]:
        """Get an instance from the pool (create if needed)"""
        start_time = time.time()
        
        while True:
            try:
                # Try to get existing instance
                instance = self.instances.get(timeout=0.1)
                with self.lock:
                    self.stats["reused"] += 1
                    self.stats["active"] += 1
                return instance
                
            except Empty:
                # Check if we can create a new instance
                with self.lock:
                    if self.instance_count < self.max_instances:
                        # Create new instance
                        instance = self._create_instance()
                        self.stats["active"] += 1
                        return instance
                
                # Check timeout
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Could not get instance within {timeout}s")
                
                # Wait a bit before retrying
                time.sleep(0.1)
    
    def release_instance(self, instance_id: int, instance: Optional[PyMetaMap] = None):
        """Return an instance to the pool"""
        if instance is None:
            # Instance might have failed, just update stats
            with self.lock:
                self.stats["active"] -= 1
            return
        
        try:
            self.instances.put((instance_id, instance), timeout=1.0)
            with self.lock:
                self.stats["active"] -= 1
            logger.debug(f"Released instance {instance_id} back to pool")
            
        except:
            # Pool is full or instance is broken, let it be garbage collected
            with self.lock:
                self.stats["active"] -= 1
            logger.warning(f"Could not return instance {instance_id} to pool")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pool statistics"""
        with self.lock:
            return {
                "max_instances": self.max_instances,
                "created": self.stats["created"],
                "reused": self.stats["reused"],
                "errors": self.stats["errors"],
                "active": self.stats["active"],
                "available": self.instances.qsize()
            }
    
    def close(self):
        """Close all instances in the pool"""
        logger.info("Closing MetaMapInstancePool")
        
        # Drain the queue
        while not self.instances.empty():
            try:
                _, instance = self.instances.get_nowait()
                # MetaMap instances don't have a close method, 
                # but they clean up in __del__
            except:
                break
        
        logger.info(f"Pool closed. Final stats: {self.get_stats()}")