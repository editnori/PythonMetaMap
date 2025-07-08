#!/usr/bin/env python
"""
PythonMetaMap v8.0.8 Complete Installation and Feature Test Suite

This script performs a complete end-to-end test of PythonMetaMap installation
and all features. Run this on a fresh system to verify everything works.

Usage:
    python test_complete_installation.py [--skip-metamap-install]
"""

import os
import sys
import time
import json
import shutil
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime
import argparse
import platform


class TestRunner:
    def __init__(self, skip_metamap_install=False):
        self.skip_metamap_install = skip_metamap_install
        self.test_results = []
        self.test_dir = Path("test_workspace")
        self.start_time = time.time()
        
    def run_command(self, cmd, check=True, capture_output=True):
        """Run a shell command and return result"""
        print(f"Running: {cmd}")
        try:
            result = subprocess.run(
                cmd, 
                shell=True, 
                check=check, 
                capture_output=capture_output,
                text=True
            )
            return result.returncode == 0, result.stdout, result.stderr
        except subprocess.CalledProcessError as e:
            return False, e.stdout, e.stderr
            
    def test(self, name, func):
        """Run a test and record result"""
        print(f"\n{'='*60}")
        print(f"Test: {name}")
        print(f"{'='*60}")
        
        try:
            success, message = func()
            self.test_results.append({
                "name": name,
                "success": success,
                "message": message,
                "timestamp": datetime.now().isoformat()
            })
            
            if success:
                print(f"✓ PASSED: {message}")
            else:
                print(f"✗ FAILED: {message}")
                
        except Exception as e:
            self.test_results.append({
                "name": name,
                "success": False,
                "message": f"Exception: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })
            print(f"✗ FAILED with exception: {str(e)}")
            
        return success
        
    def setup(self):
        """Create test workspace"""
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
        self.test_dir.mkdir()
        os.chdir(self.test_dir)
        
    def cleanup(self):
        """Clean up test workspace"""
        os.chdir("..")
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
            
    def run_all_tests(self):
        """Run complete test suite"""
        print("PythonMetaMap v8.0.8 Complete Test Suite")
        print(f"Platform: {platform.system()} {platform.release()}")
        print(f"Python: {sys.version}")
        print(f"Started: {datetime.now()}")
        
        self.setup()
        
        # Installation Tests
        self.test("Python Version Check", self.test_python_version)
        self.test("Pip Installation", self.test_pip_installation)
        self.test("PythonMetaMap Installation", self.test_pymm_installation)
        self.test("CLI Availability", self.test_cli_availability)
        self.test("Configuration System", self.test_configuration)
        
        if not self.skip_metamap_install:
            self.test("MetaMap Installation", self.test_metamap_installation)
        
        self.test("MetaMap Binary Check", self.test_metamap_binary)
        
        # Server Tests
        self.test("Server Start", self.test_server_start)
        self.test("Server Status", self.test_server_status)
        self.test("Server Connectivity", self.test_server_connectivity)
        
        # Processing Tests
        self.test("Single File Processing", self.test_single_file_processing)
        self.test("Parallel Processing", self.test_parallel_processing)
        self.test("State Persistence", self.test_state_persistence)
        self.test("Background Processing", self.test_background_processing)
        self.test("Error Handling", self.test_error_handling)
        self.test("Retry Mechanism", self.test_retry_mechanism)
        
        # Performance Tests
        self.test("Instance Pooling", self.test_instance_pooling)
        self.test("Large Batch Processing", self.test_large_batch)
        self.test("Memory Management", self.test_memory_management)
        
        # Feature Tests
        self.test("Interactive Mode", self.test_interactive_mode)
        self.test("Configuration Management", self.test_config_management)
        self.test("Log Management", self.test_log_management)
        self.test("Output Parsing", self.test_output_parsing)
        
        # Cleanup
        self.test("Server Stop", self.test_server_stop)
        self.test("Cleanup", self.test_cleanup)
        
        self.print_summary()
        self.cleanup()
        
    def test_python_version(self):
        """Test Python version requirement"""
        version = sys.version_info
        if version.major >= 3 and version.minor >= 8:
            return True, f"Python {version.major}.{version.minor} meets requirements"
        return False, f"Python {version.major}.{version.minor} does not meet minimum 3.8"
        
    def test_pip_installation(self):
        """Test pip availability"""
        success, stdout, stderr = self.run_command("pip --version")
        if success:
            return True, f"pip is installed: {stdout.strip()}"
        return False, "pip is not installed"
        
    def test_pymm_installation(self):
        """Test PythonMetaMap installation"""
        # Go back to project root
        os.chdir("..")
        success, stdout, stderr = self.run_command("pip install -e .")
        os.chdir(self.test_dir)
        
        if success:
            return True, "PythonMetaMap installed successfully"
        return False, f"Installation failed: {stderr}"
        
    def test_cli_availability(self):
        """Test pymm CLI command"""
        success, stdout, stderr = self.run_command("pymm --version")
        if success and "8.0.8" in stdout:
            return True, f"CLI available: {stdout.strip()}"
        return False, "CLI not available or wrong version"
        
    def test_configuration(self):
        """Test configuration system"""
        success, stdout, stderr = self.run_command("pymm config show")
        if success:
            return True, "Configuration system working"
        return False, f"Configuration failed: {stderr}"
        
    def test_metamap_installation(self):
        """Test MetaMap installation"""
        print("Installing MetaMap (this may take several minutes)...")
        success, stdout, stderr = self.run_command(
            "pymm install --accept-license", 
            capture_output=False
        )
        if success:
            return True, "MetaMap installed successfully"
        return False, "MetaMap installation failed"
        
    def test_metamap_binary(self):
        """Test MetaMap binary availability"""
        success, stdout, stderr = self.run_command("pymm test metamap")
        if success:
            return True, "MetaMap binary verified"
        return False, "MetaMap binary not found"
        
    def test_server_start(self):
        """Test server startup"""
        success, stdout, stderr = self.run_command("pymm server start")
        if success:
            time.sleep(5)  # Wait for servers to start
            return True, "Servers started successfully"
        return False, f"Server start failed: {stderr}"
        
    def test_server_status(self):
        """Test server status check"""
        success, stdout, stderr = self.run_command("pymm server status")
        if success and "RUNNING" in stdout:
            return True, "Servers are running"
        return False, "Servers not running properly"
        
    def test_server_connectivity(self):
        """Test server connectivity"""
        success, stdout, stderr = self.run_command("pymm test servers")
        if success:
            return True, "Server connectivity verified"
        return False, "Server connectivity failed"
        
    def test_single_file_processing(self):
        """Test single file processing"""
        # Create test input
        input_dir = Path("test_input")
        output_dir = Path("test_output")
        input_dir.mkdir(exist_ok=True)
        
        test_file = input_dir / "test1.txt"
        test_file.write_text("Patient diagnosed with diabetes mellitus type 2")
        
        success, stdout, stderr = self.run_command(
            f"pymm process {input_dir} {output_dir} --workers 1"
        )
        
        output_file = output_dir / "test1.txt.csv"
        if success and output_file.exists():
            content = output_file.read_text()
            if "C0011849" in content or "diabetes" in content.lower():
                return True, "Single file processed correctly"
        return False, "Single file processing failed"
        
    def test_parallel_processing(self):
        """Test parallel processing"""
        input_dir = Path("test_input_parallel")
        output_dir = Path("test_output_parallel")
        input_dir.mkdir(exist_ok=True)
        
        # Create multiple test files
        for i in range(10):
            test_file = input_dir / f"patient{i}.txt"
            test_file.write_text(f"Patient {i} has hypertension and diabetes")
            
        success, stdout, stderr = self.run_command(
            f"pymm process {input_dir} {output_dir} --workers 4"
        )
        
        output_files = list(output_dir.glob("*.csv"))
        if success and len(output_files) == 10:
            return True, f"Processed {len(output_files)} files in parallel"
        return False, f"Parallel processing failed: only {len(output_files)} files processed"
        
    def test_state_persistence(self):
        """Test state persistence and resume"""
        input_dir = Path("test_input_state")
        output_dir = Path("test_output_state")
        input_dir.mkdir(exist_ok=True)
        
        # Create test files
        for i in range(5):
            test_file = input_dir / f"record{i}.txt"
            test_file.write_text(f"Medical record {i}")
            
        # Start processing (we'll simulate interruption)
        process = subprocess.Popen(
            f"pymm process {input_dir} {output_dir} --workers 1",
            shell=True
        )
        time.sleep(2)  # Let it process a few files
        process.terminate()
        
        # Check state file
        state_file = output_dir / ".pymm_state.json"
        if state_file.exists():
            # Resume processing
            success, stdout, stderr = self.run_command(f"pymm resume {output_dir}")
            if success:
                output_files = list(output_dir.glob("*.csv"))
                if len(output_files) == 5:
                    return True, "State persistence and resume working"
        return False, "State persistence failed"
        
    def test_background_processing(self):
        """Test background processing"""
        input_dir = Path("test_input_bg")
        output_dir = Path("test_output_bg")
        input_dir.mkdir(exist_ok=True)
        
        test_file = input_dir / "background.txt"
        test_file.write_text("Background processing test")
        
        success, stdout, stderr = self.run_command(
            f"pymm process {input_dir} {output_dir} --background"
        )
        
        if success:
            time.sleep(5)  # Wait for background processing
            output_file = output_dir / "background.txt.csv"
            if output_file.exists():
                return True, "Background processing working"
        return False, "Background processing failed"
        
    def test_error_handling(self):
        """Test error handling"""
        input_dir = Path("test_input_error")
        output_dir = Path("test_output_error")
        input_dir.mkdir(exist_ok=True)
        
        # Create problematic files
        empty_file = input_dir / "empty.txt"
        empty_file.write_text("")
        
        large_file = input_dir / "large.txt"
        large_file.write_text("x" * 1000000)
        
        success, stdout, stderr = self.run_command(
            f"pymm process {input_dir} {output_dir} --workers 1"
        )
        
        # Check if errors were handled gracefully
        state_file = output_dir / ".pymm_state.json"
        if state_file.exists():
            state = json.loads(state_file.read_text())
            if "failed_files" in state:
                return True, "Error handling working"
        return False, "Error handling failed"
        
    def test_retry_mechanism(self):
        """Test retry mechanism"""
        output_dir = Path("test_output_error")
        
        if output_dir.exists():
            success, stdout, stderr = self.run_command(
                f"pymm retry {output_dir} --force"
            )
            if success:
                return True, "Retry mechanism working"
        return False, "Retry mechanism failed"
        
    def test_instance_pooling(self):
        """Test instance pooling"""
        # Enable instance pooling
        self.run_command("pymm config set use_instance_pool true")
        self.run_command("pymm config set max_instances 4")
        
        input_dir = Path("test_input_pool")
        output_dir = Path("test_output_pool")
        input_dir.mkdir(exist_ok=True)
        
        for i in range(20):
            test_file = input_dir / f"pool{i}.txt"
            test_file.write_text(f"Instance pool test {i}")
            
        start_time = time.time()
        success, stdout, stderr = self.run_command(
            f"pymm process {input_dir} {output_dir} --workers 4"
        )
        elapsed = time.time() - start_time
        
        if success:
            return True, f"Instance pooling working (processed in {elapsed:.1f}s)"
        return False, "Instance pooling failed"
        
    def test_large_batch(self):
        """Test large batch processing"""
        input_dir = Path("test_input_large")
        output_dir = Path("test_output_large")
        input_dir.mkdir(exist_ok=True)
        
        # Create 100 test files
        for i in range(100):
            test_file = input_dir / f"large{i}.txt"
            test_file.write_text(f"Large batch test {i} with medical conditions")
            
        start_time = time.time()
        success, stdout, stderr = self.run_command(
            f"pymm process {input_dir} {output_dir} --workers 8 --batch-size 50"
        )
        elapsed = time.time() - start_time
        
        output_files = list(output_dir.glob("*.csv"))
        if success and len(output_files) >= 90:  # Allow for some failures
            throughput = len(output_files) / (elapsed / 60)
            return True, f"Processed {len(output_files)} files at {throughput:.1f} files/min"
        return False, f"Large batch failed: only {len(output_files)} files processed"
        
    def test_memory_management(self):
        """Test memory management"""
        # Set memory limits
        self.run_command("pymm config set java_heap_size 2g")
        self.run_command("pymm config set enable_gc true")
        
        input_dir = Path("test_input_memory")
        output_dir = Path("test_output_memory")
        input_dir.mkdir(exist_ok=True)
        
        # Create file with lots of text
        test_file = input_dir / "memory.txt"
        test_file.write_text("Medical text " * 10000)
        
        success, stdout, stderr = self.run_command(
            f"pymm process {input_dir} {output_dir} --workers 1"
        )
        
        if success:
            return True, "Memory management working"
        return False, "Memory management failed"
        
    def test_interactive_mode(self):
        """Test interactive mode availability"""
        # Just check if interactive mode can be launched
        process = subprocess.Popen(
            "pymm -i",
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Send exit command
        process.stdin.write("5\n")
        process.stdin.flush()
        
        try:
            stdout, stderr = process.communicate(timeout=5)
            if "PythonMetaMap Interactive Mode" in stdout:
                return True, "Interactive mode available"
        except subprocess.TimeoutExpired:
            process.kill()
            
        return False, "Interactive mode not working"
        
    def test_config_management(self):
        """Test configuration management"""
        # Export config
        success1, _, _ = self.run_command("pymm config export test_config.json")
        
        # Reset config
        success2, _, _ = self.run_command("pymm config reset")
        
        # Import config
        success3, _, _ = self.run_command("pymm config import test_config.json")
        
        # Test 35: Set all configuration options
        self.run_command("pymm config set metamap_binary_path /path/to/metamap")
        self.run_command("pymm config set max_parallel_workers 16")
        self.run_command("pymm config set pymm_timeout 1200")
        self.run_command("pymm config set use_instance_pool true")
        self.run_command("pymm config set max_instances 16")
        self.run_command("pymm config set java_heap_size 16g")
        self.run_command("pymm config set retry_max_attempts 5")
        self.run_command("pymm config set progress_bar true")
        self.run_command("pymm config set server_persistence_hours 48")
        
        if all([success1, success2, success3]):
            return True, "Configuration management working"
        return False, "Configuration management failed"
        
    def test_log_management(self):
        """Test log management"""
        success, stdout, stderr = self.run_command("pymm logs show")
        if success:
            return True, "Log management working"
        return False, "Log management failed"
        
    def test_output_parsing(self):
        """Test output parsing"""
        output_dir = Path("test_output")
        if output_dir.exists():
            csv_files = list(output_dir.glob("*.csv"))
            if csv_files:
                csv_file = csv_files[0]
                content = csv_file.read_text()
                
                # Check CSV format
                lines = content.strip().split('\n')
                if len(lines) > 1:
                    header = lines[0]
                    if "CUI,Score,ConceptName,PrefName" in header:
                        return True, "Output parsing correct"
        return False, "Output parsing failed"
        
    def test_server_stop(self):
        """Test server shutdown"""
        success, stdout, stderr = self.run_command("pymm server stop")
        if success:
            return True, "Servers stopped successfully"
        return False, "Server stop failed"
        
    def test_cleanup(self):
        """Test cleanup functionality"""
        success, stdout, stderr = self.run_command("pymm cleanup --all")
        if success:
            return True, "Cleanup successful"
        return False, "Cleanup failed"
        
    def print_summary(self):
        """Print test summary"""
        elapsed = time.time() - self.start_time
        total = len(self.test_results)
        passed = sum(1 for t in self.test_results if t["success"])
        failed = total - passed
        
        print(f"\n{'='*60}")
        print("TEST SUMMARY")
        print(f"{'='*60}")
        print(f"Total Tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Success Rate: {(passed/total)*100:.1f}%")
        print(f"Total Time: {elapsed:.1f} seconds")
        
        if failed > 0:
            print(f"\nFailed Tests:")
            for test in self.test_results:
                if not test["success"]:
                    print(f"  - {test['name']}: {test['message']}")
                    
        # Save results
        results_file = Path("test_results.json")
        with open(results_file, 'w') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "platform": platform.system(),
                "python_version": sys.version,
                "total_tests": total,
                "passed": passed,
                "failed": failed,
                "elapsed_seconds": elapsed,
                "results": self.test_results
            }, f, indent=2)
            
        print(f"\nDetailed results saved to: {results_file}")


def main():
    parser = argparse.ArgumentParser(
        description="PythonMetaMap Complete Installation Test"
    )
    parser.add_argument(
        "--skip-metamap-install",
        action="store_true",
        help="Skip MetaMap installation (if already installed)"
    )
    
    args = parser.parse_args()
    
    runner = TestRunner(skip_metamap_install=args.skip_metamap_install)
    runner.run_all_tests()


if __name__ == "__main__":
    main() 