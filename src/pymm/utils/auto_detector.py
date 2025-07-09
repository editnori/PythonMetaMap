"""Auto-detection utilities for MetaMap paths and system configuration"""
import os
import sys
import platform
import subprocess
from pathlib import Path
from typing import Optional, Dict, Tuple
import logging

logger = logging.getLogger(__name__)


class AutoDetector:
    """Automatically detect MetaMap, JRE, and data directories"""
    
    def __init__(self):
        self.system = platform.system()
        self.is_windows = self.system == "Windows"
        self.is_mac = self.system == "Darwin"
        self.is_linux = self.system == "Linux"
    
    def detect_java(self) -> Optional[str]:
        """Detect Java installation"""
        # Check JAVA_HOME first
        java_home = os.environ.get('JAVA_HOME')
        if java_home:
            java_bin = os.path.join(java_home, 'bin', 'java')
            if self.is_windows:
                java_bin += '.exe'
            if os.path.exists(java_bin):
                return java_home
        
        # Try to find java in PATH
        try:
            result = subprocess.run(['java', '-version'], 
                                  capture_output=True, 
                                  text=True)
            if result.returncode == 0:
                # Java is in PATH, try to find JAVA_HOME
                if self.is_windows:
                    # Check common Windows locations
                    for path in [
                        r"C:\Program Files\Java",
                        r"C:\Program Files (x86)\Java",
                        r"C:\Java"
                    ]:
                        if os.path.exists(path):
                            # Find the latest JRE/JDK
                            subdirs = [d for d in os.listdir(path) 
                                     if d.startswith('jre') or d.startswith('jdk')]
                            if subdirs:
                                return os.path.join(path, sorted(subdirs)[-1])
                else:
                    # Unix-like systems
                    result = subprocess.run(['which', 'java'], 
                                          capture_output=True, 
                                          text=True)
                    if result.returncode == 0:
                        java_path = result.stdout.strip()
                        # Follow symlinks
                        java_path = os.path.realpath(java_path)
                        # Get JAVA_HOME (usually two levels up from bin/java)
                        java_home = os.path.dirname(os.path.dirname(java_path))
                        return java_home
        except Exception as e:
            logger.debug(f"Error detecting Java: {e}")
        
        return None
    
    def detect_metamap(self) -> Optional[str]:
        """Detect MetaMap installation"""
        # Check environment variable
        metamap_home = os.environ.get('METAMAP_HOME')
        if metamap_home and os.path.exists(metamap_home):
            return metamap_home
        
        # Common installation paths
        common_paths = []
        
        if self.is_windows:
            common_paths.extend([
                r"C:\metamap",
                r"C:\Program Files\metamap",
                r"C:\tools\metamap",
                os.path.expanduser(r"~\metamap"),
                os.path.expanduser(r"~\public_mm")
            ])
        else:
            common_paths.extend([
                "/opt/metamap",
                "/usr/local/metamap",
                os.path.expanduser("~/metamap"),
                os.path.expanduser("~/public_mm"),
                os.path.expanduser("~/tools/metamap")
            ])
        
        # Check each path
        for path in common_paths:
            if os.path.exists(path):
                # Verify it's a valid MetaMap installation
                bin_path = os.path.join(path, "bin", "metamap")
                if self.is_windows:
                    bin_path += ".bat"
                if os.path.exists(bin_path):
                    return path
        
        # Search in current directory tree
        current = Path.cwd()
        for parent in [current] + list(current.parents)[:3]:
            for pattern in ["*metamap*", "*public_mm*"]:
                for path in parent.glob(pattern):
                    if path.is_dir():
                        bin_path = path / "bin" / "metamap"
                        if self.is_windows:
                            bin_path = path / "bin" / "metamap.bat"
                        if bin_path.exists():
                            return str(path)
        
        return None
    
    def detect_metamap_binary(self, metamap_home: Optional[str] = None) -> Optional[str]:
        """Detect MetaMap binary path"""
        if not metamap_home:
            metamap_home = self.detect_metamap()
        
        if metamap_home:
            binary = os.path.join(metamap_home, "bin", "metamap")
            if self.is_windows:
                binary += ".bat"
            if os.path.exists(binary):
                return binary
        
        return None
    
    def detect_data_directories(self) -> Dict[str, str]:
        """Detect suitable input/output directories"""
        dirs = {}
        
        # Check for existing directories in project
        project_root = Path.cwd()
        
        # Input directories
        input_candidates = [
            "input_notes",
            "input",
            "data/input",
            "data/notes",
            "notes"
        ]
        
        for candidate in input_candidates:
            path = project_root / candidate
            if path.exists() and path.is_dir():
                dirs['input'] = str(path)
                break
        else:
            # Create default if none exists
            default_input = project_root / "input_notes"
            default_input.mkdir(exist_ok=True)
            dirs['input'] = str(default_input)
        
        # Output directories
        output_candidates = [
            "output_csvs",
            "output",
            "results",
            "data/output",
            "processed"
        ]
        
        for candidate in output_candidates:
            path = project_root / candidate
            if path.exists() and path.is_dir():
                dirs['output'] = str(path)
                break
        else:
            # Create default if none exists
            default_output = project_root / "output_csvs"
            default_output.mkdir(exist_ok=True)
            dirs['output'] = str(default_output)
        
        return dirs
    
    def check_system_requirements(self) -> Dict[str, bool]:
        """Check if system meets requirements"""
        requirements = {}
        
        # Check Java
        java_home = self.detect_java()
        requirements['java'] = java_home is not None
        
        # Check MetaMap
        metamap_home = self.detect_metamap()
        requirements['metamap'] = metamap_home is not None
        
        # Check memory (minimum 4GB recommended)
        try:
            import psutil
            memory_gb = psutil.virtual_memory().total / (1024**3)
            requirements['memory'] = memory_gb >= 4
            requirements['memory_gb'] = memory_gb
        except:
            requirements['memory'] = True  # Assume OK if can't check
            requirements['memory_gb'] = 0
        
        # Check disk space (minimum 2GB free)
        try:
            import shutil
            stat = shutil.disk_usage(".")
            free_gb = stat.free / (1024**3)
            requirements['disk_space'] = free_gb >= 2
            requirements['disk_gb'] = free_gb
        except:
            requirements['disk_space'] = True
            requirements['disk_gb'] = 0
        
        return requirements
    
    def get_optimal_settings(self) -> Dict[str, int]:
        """Get optimal settings based on system resources"""
        settings = {}
        
        try:
            import psutil
            import multiprocessing
            
            # CPU cores
            cpu_count = multiprocessing.cpu_count()
            
            # Memory
            memory_gb = psutil.virtual_memory().available / (1024**3)
            
            # Conservative worker count
            if memory_gb < 4:
                workers = min(2, cpu_count // 2)
            elif memory_gb < 8:
                workers = min(3, cpu_count // 2)
            elif memory_gb < 16:
                workers = min(4, (cpu_count // 2) + 1)
            else:
                workers = min(6, (cpu_count // 2) + 2)
            
            settings['workers'] = max(1, workers)
            
            # Chunk size based on memory
            if memory_gb < 4:
                settings['chunk_size'] = 50
            elif memory_gb < 8:
                settings['chunk_size'] = 100
            elif memory_gb < 16:
                settings['chunk_size'] = 250
            else:
                settings['chunk_size'] = 500
            
            # Timeout (longer for slower systems)
            if cpu_count < 4 or memory_gb < 8:
                settings['timeout'] = 600  # 10 minutes
            else:
                settings['timeout'] = 300  # 5 minutes
            
            # Instance pool size (very conservative)
            settings['pool_size'] = min(workers, max(2, int(memory_gb / 4)))
            
        except Exception as e:
            logger.debug(f"Error getting optimal settings: {e}")
            # Fallback defaults
            settings = {
                'workers': 2,
                'chunk_size': 100,
                'timeout': 300,
                'pool_size': 2
            }
        
        return settings
    
    def full_auto_detect(self) -> Dict[str, any]:
        """Perform full auto-detection"""
        result = {
            'system': self.system,
            'java_home': self.detect_java(),
            'metamap_home': self.detect_metamap(),
            'metamap_binary': None,
            'directories': self.detect_data_directories(),
            'requirements': self.check_system_requirements(),
            'optimal_settings': self.get_optimal_settings()
        }
        
        if result['metamap_home']:
            result['metamap_binary'] = self.detect_metamap_binary(result['metamap_home'])
        
        return result


def auto_configure():
    """Auto-configure PythonMetaMap"""
    detector = AutoDetector()
    config = detector.full_auto_detect()
    
    print("Auto-detection Results:")
    print(f"System: {config['system']}")
    print(f"Java: {'Found' if config['java_home'] else 'Not found'}")
    print(f"MetaMap: {'Found' if config['metamap_home'] else 'Not found'}")
    print(f"Input directory: {config['directories']['input']}")
    print(f"Output directory: {config['directories']['output']}")
    print(f"Recommended workers: {config['optimal_settings']['workers']}")
    print(f"Recommended chunk size: {config['optimal_settings']['chunk_size']}")
    
    return config


if __name__ == "__main__":
    auto_configure()