"""Scaled MetaMap server management for parallel processing with persistence"""
import os
import subprocess
import time
import json
import logging
import socket
import signal
import atexit
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from datetime import datetime, timedelta
import threading
import psutil

logger = logging.getLogger(__name__)


class ScaledServerManager:
    """Manages multiple MetaMap server instances with persistence and scaling"""
    
    def __init__(self, metamap_path: str, config: Optional[Dict] = None):
        self.metamap_path = Path(metamap_path)
        self.public_mm_dir = self.metamap_path / "public_mm"
        self.config = config or {}
        
        # Port configuration
        self.tagger_port_base = self.config.get("tagger_port_base", 1795)
        self.wsd_port_base = self.config.get("wsd_port_base", 5554)
        self.mmserver_port_base = self.config.get("mmserver_port_base", 8066)
        
        # Persistence configuration
        self.persistence_file = Path.home() / ".pymm" / "server_state.json"
        self.persistence_hours = self.config.get("server_persistence_hours", 24)
        
        # Server tracking
        self.servers = self._load_server_state()
        self.lock = threading.Lock()
        
        # Register cleanup on exit
        atexit.register(self._save_server_state)
        
        # Start persistence monitor
        self._start_persistence_monitor()
    
    def _load_server_state(self) -> Dict[str, Dict]:
        """Load persisted server state"""
        if self.persistence_file.exists():
            try:
                with open(self.persistence_file, 'r') as f:
                    state = json.load(f)
                    # Validate servers are still running
                    valid_servers = {}
                    for server_type, instances in state.items():
                        valid_servers[server_type] = {}
                        for instance_id, info in instances.items():
                            if self._is_process_running(info.get("pid")):
                                valid_servers[server_type][instance_id] = info
                            else:
                                logger.info(f"Server {server_type}:{instance_id} no longer running")
                    return valid_servers
            except Exception as e:
                logger.error(f"Failed to load server state: {e}")
        
        return {"tagger": {}, "wsd": {}, "mmserver": {}}
    
    def _save_server_state(self):
        """Save server state for persistence"""
        try:
            self.persistence_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.persistence_file, 'w') as f:
                json.dump(self.servers, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save server state: {e}")
    
    def _is_process_running(self, pid: Optional[int]) -> bool:
        """Check if a process is running"""
        if not pid:
            return False
        try:
            process = psutil.Process(pid)
            return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False
    
    def _find_free_port(self, base_port: int, max_instances: int = 10) -> int:
        """Find a free port starting from base_port"""
        for offset in range(max_instances):
            port = base_port + offset
            if self._is_port_free(port):
                return port
        raise RuntimeError(f"No free ports found in range {base_port}-{base_port + max_instances}")
    
    def _is_port_free(self, port: int) -> bool:
        """Check if a port is free"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1)
                s.bind(('', port))
                return True
        except:
            return False
    
    def _wait_for_port(self, port: int, timeout: int = 30) -> bool:
        """Wait for a port to become available"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)
                    s.connect(('localhost', port))
                    return True
            except:
                time.sleep(0.5)
        return False
    
    def start_tagger_instance(self, instance_id: int = 0) -> Tuple[int, int]:
        """Start a tagger server instance"""
        with self.lock:
            # Check if instance already running
            if str(instance_id) in self.servers.get("tagger", {}):
                info = self.servers["tagger"][str(instance_id)]
                if self._is_process_running(info["pid"]):
                    logger.info(f"Tagger instance {instance_id} already running on port {info['port']}")
                    return info["port"], info["pid"]
            
            # Find free port
            port = self._find_free_port(self.tagger_port_base + instance_id * 10)
            
            # Prepare environment
            env = os.environ.copy()
            env['TAGGER_SERVER_PORT'] = str(port)
            
            # Build command
            server_dir = self.public_mm_dir / "MedPost-SKR" / "Tagger_server"
            lib_dir = server_dir / "lib"
            data_dir = self.public_mm_dir / "MedPost-SKR" / "data"
            
            # Create log directory
            log_dir = self.metamap_path / "logs" / f"instance_{instance_id}"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / "tagger.log"
            
            # Classpath
            classpath = f"{lib_dir}/taggerServer.jar:{lib_dir}/mps.jar"
            
            # Data files
            lexdb_file = data_dir / "lexDB.serial"
            ngram_file = data_dir / "ngramOne.serial"
            
            cmd = [
                "java",
                f"-Dtaggerserver.port={port}",
                f"-DlexFile={lexdb_file}",
                f"-DngramOne={ngram_file}",
                "-Xmx1G",
                "-cp", classpath,
                "taggerServer"
            ]
            
            logger.info(f"Starting tagger instance {instance_id} on port {port}")
            
            # Start server
            with open(log_file, 'a') as log:
                proc = subprocess.Popen(
                    cmd,
                    cwd=str(server_dir),
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    env=env,
                    preexec_fn=os.setsid if os.name != 'nt' else None
                )
            
            # Wait for server to start
            if self._wait_for_port(port):
                # Store server info
                if "tagger" not in self.servers:
                    self.servers["tagger"] = {}
                self.servers["tagger"][str(instance_id)] = {
                    "port": port,
                    "pid": proc.pid,
                    "started": datetime.now().isoformat(),
                    "log_file": str(log_file)
                }
                self._save_server_state()
                logger.info(f"Tagger instance {instance_id} started successfully on port {port}")
                return port, proc.pid
            else:
                proc.terminate()
                raise RuntimeError(f"Failed to start tagger instance {instance_id}")
    
    def start_wsd_instance(self, instance_id: int = 0) -> Tuple[int, int]:
        """Start a WSD server instance"""
        with self.lock:
            # Check if instance already running
            if str(instance_id) in self.servers.get("wsd", {}):
                info = self.servers["wsd"][str(instance_id)]
                if self._is_process_running(info["pid"]):
                    logger.info(f"WSD instance {instance_id} already running on port {info['port']}")
                    return info["port"], info["pid"]
            
            # Find free port
            port = self._find_free_port(self.wsd_port_base + instance_id * 10)
            
            # Create instance-specific config
            server_dir = self.public_mm_dir / "WSD_Server"
            config_dir = self.metamap_path / "scaled_configs" / f"instance_{instance_id}"
            config_dir.mkdir(parents=True, exist_ok=True)
            
            # Copy and modify config file
            orig_config = server_dir / "config" / "disambServer.cfg"
            new_config = config_dir / "disambServer.cfg"
            
            if orig_config.exists():
                config_content = orig_config.read_text()
                # Update port in config
                config_content = config_content.replace(
                    f"DISAMB_SERVER_TCP_PORT={self.wsd_port_base}",
                    f"DISAMB_SERVER_TCP_PORT={port}"
                )
                new_config.write_text(config_content)
            
            # Create log directory
            log_dir = self.metamap_path / "logs" / f"instance_{instance_id}"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / "wsd.log"
            
            # Build command
            lib_dir = server_dir / "lib"
            
            # Classpath
            jar_files = [
                "metamapwsd.jar", "utils.jar", "lucene-core-3.0.1.jar",
                "monq-1.1.1.jar", "wsd.jar", "kss-api.jar",
                "thirdparty.jar", "db.jar", "log4j-1.2.8.jar"
            ]
            classpath = ":".join(str(lib_dir / jar) for jar in jar_files)
            
            cmd = [
                "java",
                "-Xmx2G",
                f"-Dserver.config.file={new_config}",
                "-classpath", classpath,
                "wsd.server.DisambiguatorServer"
            ]
            
            logger.info(f"Starting WSD instance {instance_id} on port {port}")
            
            # Set environment
            env = os.environ.copy()
            env['LD_LIBRARY_PATH'] = f"{lib_dir}:/usr/lib:{env.get('LD_LIBRARY_PATH', '')}"
            
            # Create WSD log directory
            wsd_log_dir = server_dir / "log"
            wsd_log_dir.mkdir(exist_ok=True)
            
            # Start server
            with open(log_file, 'a') as log:
                proc = subprocess.Popen(
                    cmd,
                    cwd=str(server_dir),
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    env=env,
                    preexec_fn=os.setsid if os.name != 'nt' else None
                )
            
            # Wait for server to start
            if self._wait_for_port(port, timeout=45):
                # Store server info
                if "wsd" not in self.servers:
                    self.servers["wsd"] = {}
                self.servers["wsd"][str(instance_id)] = {
                    "port": port,
                    "pid": proc.pid,
                    "started": datetime.now().isoformat(),
                    "log_file": str(log_file),
                    "config_file": str(new_config)
                }
                self._save_server_state()
                logger.info(f"WSD instance {instance_id} started successfully on port {port}")
                return port, proc.pid
            else:
                proc.terminate()
                raise RuntimeError(f"Failed to start WSD instance {instance_id}")
    
    def start_server_pool(self, num_instances: int = 4) -> Dict[str, List[int]]:
        """Start a pool of server instances"""
        logger.info(f"Starting server pool with {num_instances} instances")
        
        ports = {"tagger": [], "wsd": []}
        
        for i in range(num_instances):
            try:
                # Start tagger
                tagger_port, _ = self.start_tagger_instance(i)
                ports["tagger"].append(tagger_port)
                
                # Start WSD
                wsd_port, _ = self.start_wsd_instance(i)
                ports["wsd"].append(wsd_port)
                
            except Exception as e:
                logger.error(f"Failed to start instance {i}: {e}")
        
        return ports
    
    def get_available_ports(self) -> Dict[str, List[int]]:
        """Get all available server ports"""
        available = {"tagger": [], "wsd": []}
        
        with self.lock:
            for server_type in ["tagger", "wsd"]:
                for instance_id, info in self.servers.get(server_type, {}).items():
                    if self._is_process_running(info["pid"]):
                        available[server_type].append(info["port"])
        
        return available
    
    def stop_instance(self, server_type: str, instance_id: int):
        """Stop a specific server instance"""
        with self.lock:
            if str(instance_id) in self.servers.get(server_type, {}):
                info = self.servers[server_type][str(instance_id)]
                try:
                    if self._is_process_running(info["pid"]):
                        os.kill(info["pid"], signal.SIGTERM)
                        logger.info(f"Stopped {server_type} instance {instance_id}")
                    del self.servers[server_type][str(instance_id)]
                    self._save_server_state()
                except Exception as e:
                    logger.error(f"Failed to stop {server_type} instance {instance_id}: {e}")
    
    def stop_all(self):
        """Stop all server instances"""
        logger.info("Stopping all server instances")
        
        with self.lock:
            for server_type in ["tagger", "wsd"]:
                for instance_id in list(self.servers.get(server_type, {}).keys()):
                    self.stop_instance(server_type, int(instance_id))
    
    def _start_persistence_monitor(self):
        """Start background thread to monitor server persistence"""
        def monitor():
            while True:
                try:
                    cutoff_time = datetime.now() - timedelta(hours=self.persistence_hours)
                    
                    with self.lock:
                        for server_type in ["tagger", "wsd"]:
                            for instance_id, info in list(self.servers.get(server_type, {}).items()):
                                started = datetime.fromisoformat(info["started"])
                                if started < cutoff_time:
                                    logger.info(f"Stopping expired {server_type} instance {instance_id}")
                                    self.stop_instance(server_type, int(instance_id))
                
                except Exception as e:
                    logger.error(f"Persistence monitor error: {e}")
                
                time.sleep(3600)  # Check every hour
        
        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()
    
    def get_server_status(self) -> Dict[str, Dict]:
        """Get detailed status of all servers"""
        status = {}
        
        with self.lock:
            for server_type in ["tagger", "wsd"]:
                status[server_type] = {}
                for instance_id, info in self.servers.get(server_type, {}).items():
                    running = self._is_process_running(info["pid"])
                    status[server_type][instance_id] = {
                        "port": info["port"],
                        "pid": info["pid"],
                        "running": running,
                        "started": info["started"],
                        "uptime": str(datetime.now() - datetime.fromisoformat(info["started"])) if running else "N/A"
                    }
        
        return status