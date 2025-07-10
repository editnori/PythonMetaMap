"""MetaMap server lifecycle management - Fixed for WSL/Linux"""
import os
import subprocess
import time
import logging
import socket
from pathlib import Path
from typing import Dict, Optional, Tuple, List
import re
import sys
import signal
import tempfile
import shutil

from ..core.config import PyMMConfig
from ..core.exceptions import ServerConnectionError
from .port_guard import PortGuard

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

class ServerManager:
    """Manages MetaMap server lifecycle with WSL/Linux compatibility"""
    
    def __init__(self, config: Optional[PyMMConfig] = None):
        self.config = config or PyMMConfig()
        self.server_scripts_dir = Path(self.config.get("server_scripts_dir", ""))
        self.metamap_binary_path = self.config.get("metamap_binary_path", "")
        
        # Derive public_mm directory from binary path
        if self.metamap_binary_path:
            self.public_mm_dir = Path(self.metamap_binary_path).parent.parent
        else:
            self.public_mm_dir = None
            
        self._setup_logging()
        self.java_path = self._find_java()
        self.java_available = self._check_java()
        self._fix_server_scripts()
    
    def _setup_logging(self):
        """Setup logging for server operations"""
        self.logger = logging.getLogger("ServerManager")
    
    def _find_java(self) -> str:
        """Find Java executable path"""
        # Check config first
        java_home = self.config.get('java_home') or os.environ.get('JAVA_HOME')
        if java_home:
            java_path = Path(java_home) / 'bin' / 'java'
            if java_path.exists():
                self.logger.debug(f"Found Java at JAVA_HOME: {java_path}")
                return str(java_path)
        
        # Try common Java locations
        common_paths = [
            '/usr/bin/java',
            '/usr/local/bin/java',
            '/opt/java/bin/java',
            '/usr/lib/jvm/default/bin/java',
            '/usr/lib/jvm/java-11-openjdk-amd64/bin/java',
            '/usr/lib/jvm/java-8-openjdk-amd64/bin/java',
        ]
        
        for path in common_paths:
            if Path(path).exists():
                self.logger.debug(f"Found Java at: {path}")
                return path
        
        # Try to find java in PATH
        try:
            result = subprocess.run(['which', 'java'], capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                java_path = result.stdout.strip()
                self.logger.debug(f"Found Java in PATH: {java_path}")
                return java_path
        except:
            pass
        
        # Java not found
        self.logger.warning("Java not found. MetaMap requires Java to run.")
        self.logger.info("To install Java:")
        self.logger.info("  Ubuntu/Debian: sudo apt-get install openjdk-11-jre-headless")
        self.logger.info("  RHEL/CentOS: sudo yum install java-11-openjdk")
        self.logger.info("  macOS: brew install openjdk@11")
        self.logger.info("  Windows: Download from https://adoptium.net/")
        return 'java'
    
    def _check_java(self) -> bool:
        """Check if Java is actually available and working"""
        try:
            result = subprocess.run([self.java_path, '-version'], 
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,
                                  text=True)
            return result.returncode == 0
        except (FileNotFoundError, OSError):
            return False
    
    def _fix_server_scripts(self):
        """Fix server control scripts with correct paths"""
        if not self.server_scripts_dir or not self.public_mm_dir:
            return
            
        # Fix skrmedpostctl
        skr_script = self.server_scripts_dir / "skrmedpostctl"
        if skr_script.exists():
            self._update_script_paths(skr_script, "skrmedpost")
            
        # Fix wsdserverctl
        wsd_script = self.server_scripts_dir / "wsdserverctl"
        if wsd_script.exists():
            self._update_script_paths(wsd_script, "wsd")
            
        # Fix other MetaMap scripts
        # self._fix_metamap_scripts()  # Method not implemented yet
        
        # Fix WSD server configuration
        # self._fix_wsd_config()  # Method not implemented yet
    
    def _update_script_paths(self, script_path: Path, script_type: str):
        """Update paths in server control scripts"""
        try:
            content = script_path.read_text()
            
            # Replace BASEDIR with actual path
            basedir_line = f"BASEDIR={self.public_mm_dir}"
            content = re.sub(r'^BASEDIR=.*$', basedir_line, content, flags=re.MULTILINE)
            
            # Update JAVA path with detected Java
            java_line = f"JAVA={self.java_path}"
            content = re.sub(r'^JAVA=.*$', java_line, content, flags=re.MULTILINE)
            
            # Write back
            script_path.write_text(content)
            
            # Make executable
            os.chmod(script_path, 0o755)
            
            self.logger.debug(f"Updated paths in {script_path}")
        except Exception as e:
            self.logger.warning(f"Failed to update {script_path}: {e}")
    
    def _fix_metamap_scripts(self):
        """Fix paths in MetaMap main scripts"""
        if not self.server_scripts_dir or not self.public_mm_dir:
            return
            
        scripts_to_fix = ["metamap", "metamap20", "SKRrun.20", "metamap2020.TEMPLATE"]
        
        for script_name in scripts_to_fix:
            script_path = self.server_scripts_dir / script_name
            if script_path.exists():
                try:
                    content = script_path.read_text()
                    
                    # Update BASEDIR/MM_DISTRIB_DIR to current installation path
                    content = re.sub(
                        r'^(BASEDIR|MM_DISTRIB_DIR)=.*$', 
                        f'\\1={self.public_mm_dir}', 
                        content, 
                        flags=re.MULTILINE
                    )
                    
                    script_path.write_text(content)
                    os.chmod(script_path, 0o755)
                    self.logger.debug(f"Updated paths in {script_name}")
                except Exception as e:
                    self.logger.warning(f"Failed to update {script_name}: {e}")
    
    def _fix_wsd_config(self):
        """Fix paths in WSD server configuration files"""
        if not self.public_mm_dir:
            return
            
        wsd_config_dir = self.public_mm_dir / "WSD_Server" / "config"
        if not wsd_config_dir.exists():
            return
            
        # Fix disambServer.cfg
        config_file = wsd_config_dir / "disambServer.cfg"
        if config_file.exists():
            try:
                content = config_file.read_text()
                
                # Replace all hardcoded paths with current installation path
                old_path_patterns = [
                    r'/mnt/c/Users/[^/]+/[^/]+/PythonMetaMap/metamap_install/public_mm',
                    r'/mnt/c/Users/Administrator/PythonMetaMap/metamap_install/public_mm',
                    r'/mnt/c/Users/Layth M Qassem/Desktop/PythonMetaMap/metamap_install/public_mm'
                ]
                
                for pattern in old_path_patterns:
                    content = re.sub(pattern, str(self.public_mm_dir), content)
                
                config_file.write_text(content)
                self.logger.debug("Updated paths in disambServer.cfg")
            except Exception as e:
                self.logger.warning(f"Failed to update disambServer.cfg: {e}")
    
    def _check_port_with_retry(self, port: int, max_retries: int = 30, delay: float = 1.0) -> bool:
        """Check if port is open with retries"""
        for i in range(max_retries):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1)
                result = sock.connect_ex(('127.0.0.1', port))
                sock.close()
                
                if result == 0:
                    return True
                    
            except Exception:
                pass
                
            if i < max_retries - 1:
                time.sleep(delay)
                
        return False
    
    def is_running(self) -> bool:
        """Check if MetaMap servers are running"""
        return self.is_tagger_server_running()
    
    def start(self) -> bool:
        """Start MetaMap servers"""
        return self.start_all()
    
    def stop(self):
        """Stop MetaMap servers"""
        return self.stop_all()
    
    def is_tagger_server_running(self) -> bool:
        """Check if SKR/MedPost tagger server is running"""
        return self._check_port_with_retry(1795, max_retries=1)
    
    def is_wsd_server_running(self) -> bool:
        """Check if WSD server is running"""
        return self._check_port_with_retry(5554, max_retries=1)
    
    def is_mmserver_running(self) -> bool:
        """Check if mmserver20 is running"""
        try:
            result = subprocess.run(["pgrep", "-f", "mmserver20"], 
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            return result.returncode == 0
        except:
            return False
    
    def _kill_process_on_port(self, port: int) -> bool:
        """Kill any process using the specified port"""
        try:
            # Find process using lsof
            result = subprocess.run(
                ["lsof", "-ti", f":{port}"],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0 and result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                for pid in pids:
                    try:
                        os.kill(int(pid), signal.SIGKILL)
                        self.logger.info(f"Killed process {pid} on port {port}")
                    except:
                        pass
                time.sleep(2)
                return True
        except:
            pass
            
        # Try fuser as fallback
        try:
            subprocess.run(["fuser", "-k", f"{port}/tcp"], 
                         stdout=subprocess.DEVNULL, 
                         stderr=subprocess.DEVNULL)
            time.sleep(2)
            return True
        except:
            pass
            
        return False
    
    def _start_server_direct(self, server_type: str, port: int) -> bool:
        """Start server directly using Java command"""
        if server_type == "tagger":
            return self._start_tagger_direct(port)
        elif server_type == "wsd":
            return self._start_wsd_direct(port)
        return False
    
    def _start_tagger_direct(self, port: int) -> bool:
        """Start tagger server directly"""
        if not self.public_mm_dir:
            return False
            
        medpost_dir = self.public_mm_dir / "MedPost-SKR"
        server_dir = medpost_dir / "Tagger_server"
        data_dir = medpost_dir / "data"
        
        # Create necessary directories
        log_dir = server_dir / "log"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Java command
        classpath = f"{server_dir}/lib/taggerServer.jar:{server_dir}/lib/mps.jar"
        lexdb = data_dir / "lexDB.serial"
        ngram = data_dir / "ngramOne.serial"
        
        java_opts = [
            f"-Dtaggerserver.port={port}",
            f"-DlexFile={lexdb}",
            f"-DngramOne={ngram}"
        ]
        
        cmd = [self.java_path] + java_opts + ["-cp", classpath, "taggerServer"]
        
        # Start server
        log_file = log_dir / "tagger.log"
        with open(log_file, 'a') as log:
            proc = subprocess.Popen(
                cmd,
                cwd=str(server_dir),
                stdout=log,
                stderr=subprocess.STDOUT,
                preexec_fn=os.setsid if os.name != 'nt' else None
            )
            
            # Save PID
            pid_file = server_dir / "log" / "pid"
            pid_file.write_text(str(proc.pid))
            
            self.logger.info(f"Started tagger server with PID {proc.pid}")
            
        return True
    
    def _start_wsd_direct(self, port: int) -> bool:
        """Start WSD server directly"""
        if not self.public_mm_dir:
            return False
            
        server_dir = self.public_mm_dir / "WSD_Server"
        
        # Create necessary directories
        log_dir = server_dir / "log"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Build classpath
        lib_dir = server_dir / "lib"
        jars = [
            "metamapwsd.jar", "utils.jar", "lucene-core-3.0.1.jar",
            "monq-1.1.1.jar", "wsd.jar", "kss-api.jar", 
            "thirdparty.jar", "db.jar", "log4j-1.2.8.jar"
        ]
        classpath = ":".join(str(lib_dir / jar) for jar in jars)
        
        java_opts = [
            "-Xmx2g",
            f"-Dserver.config.file={server_dir}/config/disambServer.cfg"
        ]
        
        cmd = [self.java_path] + java_opts + ["-classpath", classpath, "wsd.server.DisambiguatorServer"]
        
        # Set LD_LIBRARY_PATH
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = f"{server_dir}/lib:/usr/lib:{env.get('LD_LIBRARY_PATH', '')}"
        
        # Start server
        log_file = log_dir / "wsd.log"
        with open(log_file, 'a') as log:
            proc = subprocess.Popen(
                cmd,
                cwd=str(server_dir),
                stdout=log,
                stderr=subprocess.STDOUT,
                env=env,
                preexec_fn=os.setsid if os.name != 'nt' else None
            )
            
            # Save PID
            pid_file = server_dir / "log" / "pid"
            pid_file.write_text(str(proc.pid))
            
            self.logger.info(f"Started WSD server with PID {proc.pid}")
            
        return True
    
    def start_tagger_server(self) -> bool:
        """Start SKR/MedPost tagger server"""
        if self.is_tagger_server_running():
            self.logger.info("Tagger server already running")
            return True
            
        self.logger.info("Starting SKR/MedPost tagger...")
        
        # Kill any process on the port first
        self._kill_process_on_port(1795)
        time.sleep(2)
        
        # Try using the control script first
        skr_ctl = self.server_scripts_dir / "skrmedpostctl"
        if skr_ctl.exists():
            try:
                # Set up environment with Java path
                env = os.environ.copy()
                env['PATH'] = f"{Path(self.java_path).parent}:{env.get('PATH', '')}"
                
                result = subprocess.run(
                    [str(skr_ctl), "start"],
                    capture_output=True,
                    text=True,
                    cwd=str(self.server_scripts_dir),
                    timeout=10,
                    env=env
                )
                
                if result.returncode == 0 or "started" in result.stdout.lower():
                    self.logger.info("Tagger start command issued via script")
                    
                    # Wait for server to start
                    if self._check_port_with_retry(1795, max_retries=30, delay=1.0):
                        self.logger.info("Tagger server started successfully")
                        return True
                    else:
                        self.logger.warning("Tagger server did not start via script, trying direct start")
                        
            except Exception as e:
                self.logger.warning(f"Script start failed: {e}, trying direct start")
        
        # If script failed, start directly
        if self._start_tagger_direct(1795):
            # Wait for server to start
            if self._check_port_with_retry(1795, max_retries=30, delay=1.0):
                self.logger.info("Tagger server started successfully (direct)")
                return True
                
        self.logger.error("Failed to start tagger server")
        return False
    
    def start_wsd_server(self) -> bool:
        """Start WSD server"""
        if self.is_wsd_server_running():
            self.logger.info("WSD server already running")
            return True
            
        self.logger.info("Starting WSD server...")
        
        # Kill any process on the port first
        self._kill_process_on_port(5554)
        time.sleep(2)
        
        # Try using the control script first
        wsd_ctl = self.server_scripts_dir / "wsdserverctl"
        if wsd_ctl.exists():
            try:
                # Set up environment with Java path
                env = os.environ.copy()
                env['PATH'] = f"{Path(self.java_path).parent}:{env.get('PATH', '')}"
                
                result = subprocess.run(
                    [str(wsd_ctl), "start"],
                    capture_output=True,
                    text=True,
                    cwd=str(self.server_scripts_dir),
                    timeout=10,
                    env=env
                )
                
                if result.returncode == 0 or "started" in result.stdout.lower():
                    self.logger.info("WSD start command issued via script")
                    
                    # Wait for server to start
                    if self._check_port_with_retry(5554, max_retries=30, delay=1.0):
                        self.logger.info("WSD server started successfully")
                        return True
                    else:
                        self.logger.warning("WSD server did not start via script, trying direct start")
                        
            except Exception as e:
                self.logger.warning(f"Script start failed: {e}, trying direct start")
        
        # If script failed, start directly
        if self._start_wsd_direct(5554):
            # Wait for server to start
            if self._check_port_with_retry(5554, max_retries=30, delay=1.0):
                self.logger.info("WSD server started successfully (direct)")
                return True
                
        self.logger.error("Failed to start WSD server")
        return False
    
    def start_mmserver(self) -> bool:
        """Start mmserver20 (optional)"""
        mmserver = self.server_scripts_dir / "mmserver20"
        
        if not mmserver.exists():
            self.logger.info("mmserver20 not found - skipping")
            return True  # Not an error if missing
            
        if self.is_mmserver_running():
            self.logger.info("mmserver20 already running")
            return True
            
        self.logger.info("Starting mmserver20...")
        try:
            log_dir = self.public_mm_dir.parent / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / "mmserver20.log"
            
            with open(log_path, "ab") as outfile:
                subprocess.Popen(
                    [str(mmserver)],
                    cwd=str(self.server_scripts_dir),
                    stdout=outfile,
                    stderr=outfile
                )
            
            self.logger.info(f"mmserver20 launched, log: {log_path}")
            time.sleep(2)
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting mmserver20: {e}")
            return False
    
    def start_all(self) -> bool:
        """Start all MetaMap servers with improved reliability"""
        # First, clean up any stale processes
        self.logger.info("Cleaning up any stale processes...")
        self.force_kill_all()
        time.sleep(3)
        
        # Start servers
        tagger_ok = self.start_tagger_server()
        wsd_ok = self.start_wsd_server()
        mmserver_ok = self.start_mmserver()
        
        # Verify connectivity
        if self.metamap_binary_path and tagger_ok:
            time.sleep(5)  # Give servers time to fully initialize
            success = self.verify_connectivity()
            if not success:
                self.logger.warning("MetaMap connectivity test failed, servers may need more time")
        
        return tagger_ok  # Tagger is essential
    
    def stop_all(self):
        """Stop all MetaMap servers"""
        self.logger.info("Stopping MetaMap servers...")
        
        # Use control scripts if available
        skr_ctl = self.server_scripts_dir / "skrmedpostctl"
        wsd_ctl = self.server_scripts_dir / "wsdserverctl"
        
        if skr_ctl.exists():
            try:
                subprocess.run([str(skr_ctl), "stop"], capture_output=True)
            except:
                pass
                
        if wsd_ctl.exists():
            try:
                subprocess.run([str(wsd_ctl), "stop"], capture_output=True)
            except:
                pass
        
        # Force kill all processes to ensure clean stop
        killed = self.force_kill_all()
        
        if killed > 0:
            self.logger.info(f"Force stopped {killed} processes")
        else:
            self.logger.info("All servers stopped cleanly")
    
    def restart_service(self, service: str) -> bool:
        """Restart a specific service"""
        if service == "tagger":
            self.stop_tagger()
            time.sleep(2)
            return self.start_tagger_server()
        elif service == "wsd":
            self.stop_wsd()
            time.sleep(2)
            return self.start_wsd_server()
        elif service == "all":
            self.stop_all()
            time.sleep(5)
            return self.start_all()
        else:
            self.logger.error(f"Unknown service: {service}")
            return False
    
    def stop_tagger(self):
        """Stop tagger server only"""
        skr_ctl = self.server_scripts_dir / "skrmedpostctl"
        if skr_ctl.exists():
            try:
                subprocess.run([str(skr_ctl), "stop"], capture_output=True)
            except:
                pass
        self._kill_process_on_port(1795)
    
    def stop_wsd(self):
        """Stop WSD server only"""
        wsd_ctl = self.server_scripts_dir / "wsdserverctl"
        if wsd_ctl.exists():
            try:
                subprocess.run([str(wsd_ctl), "stop"], capture_output=True)
            except:
                pass
        self._kill_process_on_port(5554)
    
    def get_status(self) -> Dict[str, Dict[str, any]]:
        """Get detailed status of all servers"""
        return {
            "tagger": {
                "status": "RUNNING" if self.is_tagger_server_running() else "STOPPED",
                "port": 1795,
                "pid": self._get_service_pid("taggerServer")
            },
            "wsd": {
                "status": "RUNNING" if self.is_wsd_server_running() else "STOPPED",
                "port": 5554,
                "pid": self._get_service_pid("DisambiguatorServer")
            },
            "mmserver": {
                "status": "RUNNING" if self.is_mmserver_running() else "STOPPED",
                "pid": self._get_service_pid("mmserver20")
            }
        }
    
    def _get_service_pid(self, service_name: str) -> Optional[int]:
        """Get PID of a service"""
        try:
            result = subprocess.run(
                ["pgrep", "-f", service_name],
                capture_output=True,
                text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                return int(result.stdout.strip().split()[0])
        except:
            pass
        return None
    
    def verify_connectivity(self) -> bool:
        """Verify MetaMap connectivity"""
        if not self.metamap_binary_path:
            return False
            
        try:
            # Create a simple test file
            test_file = Path("/tmp/pymm_test.txt")
            test_file.write_text("test")
            
            # Run metamap with minimal options
            cmd = [
                self.metamap_binary_path,
                "-q",  # quiet
                "--silent",
                test_file.as_posix()
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            # Clean up
            test_file.unlink(missing_ok=True)
            
            # Check if successful
            return result.returncode == 0
            
        except Exception as e:
            self.logger.error(f"Connectivity test failed: {e}")
            return False
    
    def force_kill_all(self) -> int:
        """Force kill all MetaMap-related processes"""
        killed = 0
        
        # Kill by port
        for port in [1795, 5554]:
            if self._kill_process_on_port(port):
                killed += 1
        
        # Kill by process name
        process_names = ["taggerServer", "DisambiguatorServer", "mmserver20", "metamap20"]
        
        for proc_name in process_names:
            try:
                result = subprocess.run(
                    ["pkill", "-f", proc_name],
                    capture_output=True
                )
                if result.returncode == 0:
                    killed += 1
            except:
                pass
        
        # Clean up PID files
        if self.public_mm_dir:
            pid_files = [
                self.public_mm_dir / "MedPost-SKR" / "Tagger_server" / "log" / "pid",
                self.public_mm_dir / "WSD_Server" / "log" / "pid"
            ]
            for pid_file in pid_files:
                if pid_file.exists():
                    pid_file.unlink(missing_ok=True)
        
        return killed
    
    def get_server_pool_status(self) -> Dict[str, List[Dict]]:
        """Get status of server pool for multiple instances"""
        pool_status = {
            "tagger_pool": [],
            "wsd_pool": []
        }
        
        # Check tagger servers on ports 1795-1805
        for port in range(1795, 1806):
            status = {
                "port": port,
                "running": self._check_port_with_retry(port, max_retries=1),
                "pid": self._get_process_on_port(port)
            }
            pool_status["tagger_pool"].append(status)
        
        # Check WSD servers on ports 5554-5564
        for port in range(5554, 5565):
            status = {
                "port": port,
                "running": self._check_port_with_retry(port, max_retries=1),
                "pid": self._get_process_on_port(port)
            }
            pool_status["wsd_pool"].append(status)
        
        return pool_status
    
    def _get_process_on_port(self, port: int) -> Optional[int]:
        """Get PID of process listening on port"""
        try:
            result = subprocess.run(
                ["lsof", "-ti", f":{port}"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                return int(result.stdout.strip().split()[0])
        except:
            pass
        return None
    
    def start_server_pool(self, tagger_instances: int = 3, wsd_instances: int = 3) -> Dict[str, List[bool]]:
        """Start multiple server instances for parallel processing"""
        results = {
            "tagger_servers": [],
            "wsd_servers": []
        }
        
        # Start tagger servers
        for i in range(tagger_instances):
            port = 1795 + i
            self._kill_process_on_port(port)
            time.sleep(1)
            success = self._start_tagger_direct(port)
            if success:
                success = self._check_port_with_retry(port, max_retries=20, delay=1.0)
            results["tagger_servers"].append(success)
            self.logger.info(f"Tagger server on port {port}: {'started' if success else 'failed'}")
        
        # Start WSD servers
        for i in range(wsd_instances):
            port = 5554 + i
            self._kill_process_on_port(port)
            time.sleep(1)
            success = self._start_wsd_direct(port)
            if success:
                success = self._check_port_with_retry(port, max_retries=20, delay=1.0)
            results["wsd_servers"].append(success)
            self.logger.info(f"WSD server on port {port}: {'started' if success else 'failed'}")
        
        return results