"""Java installation helper for PythonMetaMap"""
import os
import sys
import platform
import subprocess
import shutil
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class JavaInstaller:
    """Helper to install and configure Java for MetaMap"""
    
    def __init__(self):
        self.system = platform.system()
        self.is_windows = self.system == "Windows"
        self.is_mac = self.system == "Darwin"
        self.is_linux = self.system == "Linux"
        
    def check_java_installed(self) -> bool:
        """Check if Java is installed and accessible"""
        try:
            result = subprocess.run(['java', '-version'], 
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE,
                                  text=True)
            return result.returncode == 0
        except FileNotFoundError:
            return False
            
    def get_install_command(self) -> Optional[str]:
        """Get the appropriate Java installation command for the OS"""
        if self.is_linux:
            # Check which package manager is available
            if shutil.which('apt-get'):
                return "sudo apt-get update && sudo apt-get install -y openjdk-11-jre-headless"
            elif shutil.which('yum'):
                return "sudo yum install -y java-11-openjdk"
            elif shutil.which('dnf'):
                return "sudo dnf install -y java-11-openjdk"
            elif shutil.which('pacman'):
                return "sudo pacman -S jre11-openjdk-headless"
            elif shutil.which('zypper'):
                return "sudo zypper install -y java-11-openjdk"
        elif self.is_mac:
            if shutil.which('brew'):
                return "brew install openjdk@11"
            else:
                return "Please install Homebrew first, then run: brew install openjdk@11"
        elif self.is_windows:
            return None  # Windows requires manual installation
            
        return None
        
    def prompt_install(self) -> bool:
        """Prompt user to install Java"""
        print("\nJava is not installed. MetaMap requires Java to run.")
        print("Recommended: OpenJDK 11 or later\n")
        
        install_cmd = self.get_install_command()
        
        if self.is_windows:
            print("For Windows, please:")
            print("1. Download OpenJDK 11 from: https://adoptium.net/")
            print("2. Run the installer")
            print("3. Restart this program after installation")
            return False
            
        if install_cmd:
            print(f"To install Java on {self.system}, run:")
            print(f"\n  {install_cmd}\n")
            
            if 'sudo' in install_cmd:
                print("Note: You'll need administrator privileges (sudo password)")
                
            response = input("Would you like to install Java now? [y/N]: ").lower()
            
            if response == 'y':
                print("\nInstalling Java...")
                try:
                    # For sudo commands, we can't automate the password
                    if 'sudo' in install_cmd:
                        print("\nPlease enter your password when prompted:")
                        subprocess.run(install_cmd, shell=True, check=True)
                    else:
                        subprocess.run(install_cmd, shell=True, check=True)
                        
                    print("\nJava installation completed!")
                    return True
                    
                except subprocess.CalledProcessError as e:
                    print(f"\nError installing Java: {e}")
                    return False
                except KeyboardInterrupt:
                    print("\nInstallation cancelled.")
                    return False
        else:
            print(f"Please install Java manually for {self.system}")
            print("Visit: https://adoptium.net/ for OpenJDK downloads")
            
        return False
        
    def find_java_after_install(self) -> Optional[str]:
        """Find Java location after installation"""
        # First check if java is now in PATH
        if self.check_java_installed():
            # Try to find JAVA_HOME
            if self.is_linux or self.is_mac:
                try:
                    # Find java executable
                    result = subprocess.run(['which', 'java'], 
                                          capture_output=True, 
                                          text=True)
                    if result.returncode == 0:
                        java_path = result.stdout.strip()
                        # Follow symlinks
                        java_path = os.path.realpath(java_path)
                        # JAVA_HOME is typically two levels up from bin/java
                        java_home = os.path.dirname(os.path.dirname(java_path))
                        
                        # Verify it's a valid Java home
                        if os.path.exists(os.path.join(java_home, 'bin', 'java')):
                            return java_home
                            
                except Exception as e:
                    logger.debug(f"Error finding Java: {e}")
                    
            # Check common installation locations
            if self.is_linux:
                common_paths = [
                    "/usr/lib/jvm/java-11-openjdk-amd64",
                    "/usr/lib/jvm/java-11-openjdk",
                    "/usr/lib/jvm/default-java",
                    "/usr/java/latest"
                ]
            elif self.is_mac:
                common_paths = [
                    "/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home",
                    "/opt/homebrew/opt/openjdk@11",
                    "/usr/local/opt/openjdk@11"
                ]
            else:
                common_paths = []
                
            for path in common_paths:
                if os.path.exists(path) and os.path.exists(os.path.join(path, 'bin', 'java')):
                    return path
                    
        return None
        
    def setup_java_environment(self, java_home: str) -> bool:
        """Set up Java environment variables"""
        try:
            # Set for current process
            os.environ['JAVA_HOME'] = java_home
            
            # Add to PATH
            java_bin = os.path.join(java_home, 'bin')
            current_path = os.environ.get('PATH', '')
            if java_bin not in current_path:
                os.environ['PATH'] = f"{java_bin}{os.pathsep}{current_path}"
                
            print(f"\nJava environment configured:")
            print(f"  JAVA_HOME: {java_home}")
            print(f"  Added to PATH: {java_bin}")
            
            # Provide instructions for permanent setup
            if self.is_linux or self.is_mac:
                shell_rc = "~/.bashrc" if self.is_linux else "~/.zshrc"
                print(f"\nTo make this permanent, add to {shell_rc}:")
                print(f"  export JAVA_HOME=\"{java_home}\"")
                print(f"  export PATH=\"$JAVA_HOME/bin:$PATH\"")
                
            return True
            
        except Exception as e:
            logger.error(f"Error setting up Java environment: {e}")
            return False


def ensure_java_installed() -> Optional[str]:
    """Ensure Java is installed, prompt if not"""
    installer = JavaInstaller()
    
    # Check if already installed
    if installer.check_java_installed():
        # Try to find JAVA_HOME
        from .auto_detector import AutoDetector
        detector = AutoDetector()
        java_home = detector.detect_java()
        return java_home
        
    # Not installed, prompt user
    installed = installer.prompt_install()
    
    if installed:
        # Find where it was installed
        java_home = installer.find_java_after_install()
        if java_home:
            installer.setup_java_environment(java_home)
            return java_home
            
    return None