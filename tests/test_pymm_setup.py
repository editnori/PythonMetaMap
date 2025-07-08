#!/usr/bin/env python
"""
Comprehensive test script for PythonMetaMap setup
Tests all aspects of installation and configuration
"""
import os
import sys
import subprocess
import tempfile
from pathlib import Path
import shutil
import time

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def print_header(text):
    """Print a formatted header"""
    print("\n" + "="*70)
    print(f" {text}")
    print("="*70)

def run_command(cmd, capture=True):
    """Run a command and return output"""
    print(f"Running: {cmd}")
    if capture:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(f"STDERR: {result.stderr}")
        return result.returncode == 0, result.stdout, result.stderr
    else:
        return subprocess.call(cmd, shell=True) == 0, "", ""

def test_installation():
    """Test the complete installation process"""
    print_header("Testing PythonMetaMap Installation")
    
    # 1. Check Python version
    print("\n1. Checking Python version...")
    version = sys.version_info
    if version.major == 3 and version.minor >= 7:
        print(f"✓ Python {version.major}.{version.minor}.{version.micro} - OK")
    else:
        print(f"✗ Python {version.major}.{version.minor}.{version.micro} - Python 3.7+ required")
        return False
    
    # 2. Check package installation
    print("\n2. Checking package installation...")
    try:
        import pymm
        print("✓ PyMM package is installed")
    except ImportError:
        print("✗ PyMM package not installed")
        print("  Run: pip install -e .")
        return False
    
    # 3. Check dependencies
    print("\n3. Checking dependencies...")
    required = ['psutil', 'colorama', 'tqdm', 'rich']
    missing = []
    for pkg in required:
        try:
            __import__(pkg)
            print(f"✓ {pkg} installed")
        except ImportError:
            print(f"✗ {pkg} missing")
            missing.append(pkg)
    
    if missing:
        print(f"\nInstall missing packages: pip install {' '.join(missing)}")
        return False
    
    # 4. Check MetaMap installation
    print("\n4. Checking MetaMap installation...")
    from pymm.install_metamap import EXPECTED_METAMAP_BINARY, META_INSTALL_DIR
    
    if os.path.exists(EXPECTED_METAMAP_BINARY):
        print(f"✓ MetaMap binary found at: {EXPECTED_METAMAP_BINARY}")
        if os.access(EXPECTED_METAMAP_BINARY, os.X_OK):
            print("✓ MetaMap binary is executable")
        else:
            print("✗ MetaMap binary is not executable")
            print(f"  Run: chmod +x {EXPECTED_METAMAP_BINARY}")
    else:
        print(f"✗ MetaMap not installed at: {EXPECTED_METAMAP_BINARY}")
        print("  Run: pymm install-metamap")
        return False
    
    # 5. Check Java
    print("\n5. Checking Java installation...")
    success, stdout, stderr = run_command("java -version")
    if success or "version" in stderr.lower():
        print("✓ Java is installed")
    else:
        print("✗ Java not found")
        print("  Install Java 8 or higher")
        return False
    
    # 6. Run setup verification
    print("\n6. Running setup verification...")
    success, stdout, stderr = run_command("pymm setup")
    if success:
        print("✓ Setup verification passed")
    else:
        print("✗ Setup verification failed")
        print("  Run: pymm setup --fix")
        return False
    
    return True

def test_configuration():
    """Test configuration"""
    print_header("Testing Configuration")
    
    # Check if configuration exists
    from pymm.core.config import Config
    config = Config()
    
    print(f"Config file: {config.config_file}")
    
    if os.path.exists(config.config_file):
        print("✓ Configuration file exists")
    else:
        print("✗ Configuration file not found")
        print("  Run: pymm config")
        return False
    
    # Check key settings
    required_settings = ['metamap_binary_path', 'server_scripts_dir']
    for setting in required_settings:
        value = config.get(setting)
        if value:
            print(f"✓ {setting}: {value}")
        else:
            print(f"✗ {setting}: Not set")
            return False
    
    return True

def test_servers():
    """Test server functionality"""
    print_header("Testing Servers")
    
    print("\n1. Checking server status...")
    success, stdout, stderr = run_command("pymm server status")
    if not success:
        print("✗ Failed to check server status")
        return False
    
    print("\n2. Starting servers...")
    success, stdout, stderr = run_command("pymm server start", capture=False)
    if success:
        print("✓ Servers started successfully")
    else:
        print("✗ Failed to start servers")
        return False
    
    # Wait for servers to initialize
    print("Waiting for servers to initialize...")
    time.sleep(5)
    
    print("\n3. Verifying server connectivity...")
    success, stdout, stderr = run_command("pymm server status")
    if success and "RUNNING" in stdout:
        print("✓ Servers are running")
    else:
        print("✗ Servers not running properly")
        return False
    
    return True

def test_processing():
    """Test basic processing functionality"""
    print_header("Testing Processing")
    
    # Create test input
    test_dir = Path("test_pymm_temp")
    input_dir = test_dir / "input"
    output_dir = test_dir / "output"
    
    try:
        # Create directories
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create test file
        test_file = input_dir / "test.txt"
        test_file.write_text("The patient has diabetes and hypertension.")
        
        print(f"Created test file: {test_file}")
        
        # Run processing
        print("\nProcessing test file...")
        cmd = f"pymm process {input_dir} {output_dir} --workers 1"
        success, stdout, stderr = run_command(cmd, capture=False)
        
        if success:
            print("✓ Processing completed")
            
            # Check output
            output_file = output_dir / "test.csv"
            if output_file.exists():
                print(f"✓ Output file created: {output_file}")
                content = output_file.read_text()
                if "diabetes" in content.lower() or "C0011847" in content:
                    print("✓ MetaMap correctly identified concepts")
                    return True
                else:
                    print("✗ Output file doesn't contain expected concepts")
                    print(f"Content preview: {content[:200]}...")
            else:
                print("✗ Output file not created")
        else:
            print("✗ Processing failed")
            
    finally:
        # Cleanup
        if test_dir.exists():
            shutil.rmtree(test_dir)
            
    return False

def main():
    """Run all tests"""
    print("\n" + "="*70)
    print(" PythonMetaMap Setup Test Suite")
    print("="*70)
    
    tests = [
        ("Installation", test_installation),
        ("Configuration", test_configuration),
        ("Servers", test_servers),
        ("Processing", test_processing)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n✗ {test_name} test failed with error: {e}")
            results[test_name] = False
    
    # Summary
    print_header("Test Summary")
    
    all_passed = True
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n✓ All tests passed! PythonMetaMap is ready to use.")
        print("\nNext steps:")
        print("1. Place your input files in the input directory")
        print("2. Run: pymm process")
        print("   Or use interactive mode: pymm interactive")
    else:
        print("\n✗ Some tests failed. Please fix the issues above.")
        print("\nFor help:")
        print("- Run: pymm setup --fix")
        print("- Check documentation: https://github.com/your-repo/PythonMetaMap")
    
    # Stop servers
    print("\nStopping servers...")
    run_command("pymm server stop", capture=False)
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main()) 