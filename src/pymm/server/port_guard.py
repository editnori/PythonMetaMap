"""Cross-platform port availability management"""
import socket
import time
import platform
import logging
import subprocess
from typing import Optional, Dict, List

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    logging.warning("psutil not available - port guard functionality limited")

class PortGuard:
    """Cross-platform port availability manager for MetaMap servers"""
    
    METAMAP_PORTS = {
        'tagger': 1795,
        'wsd': 5554
    }
    
    @classmethod
    def is_port_available(cls, port: int, host: str = 'localhost') -> bool:
        """Check if port is available for binding"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((host, port))
                return True
        except OSError:
            return False
    
    @classmethod
    def find_blocking_process(cls, port: int) -> Optional[Dict[str, any]]:
        """Find process holding a port"""
        if not HAS_PSUTIL:
            return None
            
        try:
            for conn in psutil.net_connections(kind='inet'):
                if conn.laddr.port == port and conn.status == 'LISTEN':
                    try:
                        proc = psutil.Process(conn.pid)
                        return {
                            'pid': conn.pid,
                            'name': proc.name(),
                            'cmdline': ' '.join(proc.cmdline()),
                            'create_time': proc.create_time()
                        }
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
        except Exception as e:
            logging.error(f"Error finding blocking process: {e}")
        
        return None
    
    @classmethod
    def is_stale_metamap_process(cls, process_info: Dict[str, any]) -> bool:
        """Check if a process is a stale MetaMap process"""
        if not process_info:
            return False
            
        # Check process name
        name = process_info.get('name', '').lower()
        cmdline = process_info.get('cmdline', '').lower()
        
        metamap_indicators = ['java', 'metamap', 'skrmedpost', 'wsdserver', 'disambserver']
        
        if any(indicator in name for indicator in metamap_indicators):
            return True
            
        if any(indicator in cmdline for indicator in metamap_indicators):
            return True
            
        # Check if process is old (> 1 day)
        if HAS_PSUTIL and 'create_time' in process_info:
            age = time.time() - process_info['create_time']
            if age > 86400:  # 24 hours
                return True
                
        return False
    
    @classmethod
    def kill_process(cls, pid: int, force: bool = False) -> bool:
        """Kill a process by PID"""
        try:
            if HAS_PSUTIL:
                proc = psutil.Process(pid)
                if force:
                    proc.kill()
                else:
                    proc.terminate()
                    time.sleep(2)
                    if proc.is_running():
                        proc.kill()
                return True
            else:
                # Fallback to OS commands
                if platform.system() == 'Windows':
                    cmd = ['taskkill', '/F', '/PID', str(pid)]
                else:
                    cmd = ['kill', '-9' if force else '-15', str(pid)]
                
                result = subprocess.run(cmd, capture_output=True)
                return result.returncode == 0
                
        except Exception as e:
            logging.error(f"Failed to kill process {pid}: {e}")
            return False
    
    @classmethod
    def ensure_ports_available(cls, timeout: int = 60, auto_kill_stale: bool = True) -> Dict[str, bool]:
        """Ensure MetaMap ports are available
        
        Args:
            timeout: Maximum time to wait for ports
            auto_kill_stale: Automatically kill stale MetaMap processes
            
        Returns:
            Dict mapping service names to availability status
        """
        start_time = time.time()
        results = {}
        
        for service, port in cls.METAMAP_PORTS.items():
            logging.info(f"Checking port {port} for {service}...")
            
            while not cls.is_port_available(port):
                if time.time() - start_time > timeout:
                    logging.error(f"Port {port} still occupied after {timeout}s")
                    results[service] = False
                    break
                
                blocker = cls.find_blocking_process(port)
                if blocker:
                    logging.warning(f"Port {port} blocked by {blocker['name']} (PID: {blocker['pid']})")
                    
                    if auto_kill_stale and cls.is_stale_metamap_process(blocker):
                        logging.warning(f"Terminating stale process {blocker['pid']}")
                        if cls.kill_process(blocker['pid']):
                            logging.info(f"Successfully terminated process {blocker['pid']}")
                            time.sleep(2)  # Wait for port release
                        else:
                            logging.error(f"Failed to terminate process {blocker['pid']}")
                else:
                    logging.warning(f"Port {port} is occupied but cannot identify blocking process")
                
                time.sleep(1)
            else:
                logging.info(f"Port {port} is available for {service}")
                results[service] = True
        
        return results
    
    @classmethod
    def cleanup_stale_processes(cls) -> int:
        """Clean up all stale MetaMap-related processes
        
        Returns:
            Number of processes cleaned up
        """
        if not HAS_PSUTIL:
            logging.warning("psutil not available - cannot clean up stale processes")
            return 0
            
        cleaned = 0
        
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
                try:
                    info = proc.info
                    process_info = {
                        'pid': info['pid'],
                        'name': info.get('name', ''),
                        'cmdline': ' '.join(info.get('cmdline', [])),
                        'create_time': info.get('create_time', 0)
                    }
                    
                    if cls.is_stale_metamap_process(process_info):
                        logging.info(f"Found stale process: {process_info['name']} (PID: {process_info['pid']})")
                        if cls.kill_process(process_info['pid']):
                            cleaned += 1
                            
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                    
        except Exception as e:
            logging.error(f"Error during process cleanup: {e}")
            
        return cleaned
    
    @classmethod
    def get_port_status(cls) -> Dict[str, Dict[str, any]]:
        """Get detailed status of all MetaMap ports
        
        Returns:
            Dict mapping service names to port status info
        """
        status = {}
        
        for service, port in cls.METAMAP_PORTS.items():
            info = {
                'port': port,
                'available': cls.is_port_available(port),
                'blocking_process': None
            }
            
            if not info['available']:
                info['blocking_process'] = cls.find_blocking_process(port)
                
            status[service] = info
            
        return status