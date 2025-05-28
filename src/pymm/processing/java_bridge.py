"""
Java API Bridge for MetaMap processing
"""
import os
import sys
import json
import subprocess
import tempfile
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import queue

from ..core.config import Config
from ..core.exceptions import PyMMError

logger = logging.getLogger(__name__)


class JavaAPIBridge:
    """Bridge between Python pymm and Java MetaMap API"""
    
    def __init__(self, config: Config):
        self.config = config
        self.java_impl_path = Path(__file__).parent.parent.parent.parent / "java_api_impl"
        self.lib_path = self.java_impl_path / "lib"
        self.src_path = self.java_impl_path / "src"
        self.output_path = self.java_impl_path / "output"
        self.classpath = self._build_classpath()
        self.compiled = False
        
    def _build_classpath(self) -> str:
        """Build Java classpath"""
        jars = [
            self.lib_path / "MetaMapApi.jar",
            self.lib_path / "metamap-api-2.0.jar",
            self.lib_path / "prologbeans.jar",
            # Add Gson for JSON support
            Path(self.config.metamap_path) / "src" / "javaapi" / "lib" / "gson-2.8.9.jar"
        ]
        
        # Add source path for compiled classes
        paths = [str(self.src_path)] + [str(jar) for jar in jars if jar.exists()]
        
        # On Windows, use semicolon separator
        separator = ";" if sys.platform.startswith("win") else ":"
        return separator.join(paths)
    
    def compile_java_sources(self) -> bool:
        """Compile Java source files"""
        if self.compiled:
            return True
            
        logger.info("Compiling Java sources...")
        
        # Download Gson if not present
        gson_path = self.lib_path / "gson-2.8.9.jar"
        if not gson_path.exists():
            logger.info("Downloading Gson library...")
            import urllib.request
            gson_url = "https://repo1.maven.org/maven2/com/google/code/gson/gson/2.8.9/gson-2.8.9.jar"
            urllib.request.urlretrieve(gson_url, str(gson_path))
        
        # Update classpath with Gson
        self.classpath = self._build_classpath()
        
        # Find all Java files
        java_files = list(self.src_path.glob("*.java"))
        
        # Compile command
        javac_cmd = [
            "javac",
            "-cp", self.classpath,
            "-d", str(self.src_path),
            "-Xlint:unchecked"
        ] + [str(f) for f in java_files]
        
        try:
            result = subprocess.run(javac_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Java compilation failed: {result.stderr}")
                return False
            
            logger.info("Java sources compiled successfully")
            self.compiled = True
            return True
            
        except Exception as e:
            logger.error(f"Failed to compile Java sources: {e}")
            return False
    
    def process_files(self, input_files: List[str], output_dir: str, 
                     options: Optional[str] = None, 
                     max_workers: int = 4) -> Dict[str, Dict]:
        """Process files using Java API"""
        
        # Ensure Java sources are compiled
        if not self.compile_java_sources():
            raise PyMMError("Failed to compile Java sources")
        
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Prepare Java command
        java_cmd = [
            "java",
            "-cp", self.classpath,
            "-Xmx4G",  # 4GB heap
            "MetaMapRunner",
            ",".join(input_files),  # Pass files as comma-separated list
            output_dir,
            "--metamapPath", str(self.config.metamap_path),
            "--concurrency", str(max_workers)
        ]
        
        if options:
            java_cmd.extend(["--options", options])
        
        logger.info(f"Starting Java API processing with {max_workers} workers...")
        
        try:
            # Run Java process
            process = subprocess.Popen(
                java_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            # Monitor output in real-time
            results = {}
            for line in iter(process.stdout.readline, ''):
                line = line.strip()
                if line:
                    logger.info(f"Java: {line}")
                    
                    # Parse processing results
                    if line.startswith("Processed:"):
                        parts = line.split()
                        if len(parts) >= 2:
                            filename = parts[1]
                            results[filename] = {"status": "success"}
            
            # Wait for completion
            process.wait()
            
            if process.returncode != 0:
                stderr = process.stderr.read()
                raise PyMMError(f"Java processing failed: {stderr}")
            
            # Load processing report if available
            report_file = Path(output_dir) / "processing_report.json"
            if report_file.exists():
                with open(report_file, 'r') as f:
                    report = json.load(f)
                    return report.get("results", results)
            
            return results
            
        except Exception as e:
            logger.error(f"Java API processing failed: {e}")
            raise PyMMError(f"Java API processing failed: {e}")
    
    def process_single_file(self, input_file: str, output_file: str,
                           options: Optional[str] = None) -> bool:
        """Process a single file using Java API"""
        
        output_dir = Path(output_file).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        results = self.process_files([input_file], str(output_dir), options, 1)
        
        # Rename output file to match expected name
        java_output = output_dir / (Path(input_file).name + ".csv")
        if java_output.exists() and java_output != Path(output_file):
            java_output.rename(output_file)
        
        return input_file in results and results[input_file].get("status") == "success"
    
    def start_server_pool(self, num_servers: int = 4) -> List[int]:
        """Start a pool of MetaMap servers"""
        
        if not self.compile_java_sources():
            raise PyMMError("Failed to compile Java sources")
        
        logger.info(f"Starting {num_servers} MetaMap servers...")
        
        # Create a temporary Java program to start servers
        server_starter = f"""
        public class ServerStarter {{
            public static void main(String[] args) throws Exception {{
                MetaMapServerManager manager = new MetaMapServerManager("{self.config.metamap_path}");
                for (int i = 0; i < {num_servers}; i++) {{
                    int port = manager.startServer();
                    System.out.println("SERVER_PORT:" + port);
                }}
                // Keep running
                Thread.sleep(Long.MAX_VALUE);
            }}
        }}
        """
        
        # Write and compile server starter
        starter_file = self.src_path / "ServerStarter.java"
        starter_file.write_text(server_starter)
        
        compile_cmd = ["javac", "-cp", self.classpath, str(starter_file)]
        subprocess.run(compile_cmd, check=True)
        
        # Run server starter
        java_cmd = ["java", "-cp", self.classpath, "ServerStarter"]
        process = subprocess.Popen(java_cmd, stdout=subprocess.PIPE, text=True)
        
        # Collect server ports
        ports = []
        for line in iter(process.stdout.readline, ''):
            if line.startswith("SERVER_PORT:"):
                port = int(line.split(":")[1])
                ports.append(port)
                if len(ports) >= num_servers:
                    break
        
        logger.info(f"Started servers on ports: {ports}")
        return ports
    
    def check_server_health(self, port: int) -> bool:
        """Check if a MetaMap server is healthy"""
        import socket
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('localhost', port))
            sock.close()
            return result == 0
        except:
            return False