#!/usr/bin/env python3
"""Stress test for PythonMetaMap optimization"""
import os
import sys
import time
import shutil
import random
import string
import psutil
import logging
import threading
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pymm.core.config import PyMMConfig
from src.pymm.processing.optimized_batch_runner import OptimizedBatchRunner
from src.pymm.processing.ultra_optimized_runner import UltraOptimizedBatchRunner

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StressTester:
    """Stress test the MetaMap processing pipeline"""
    
    def __init__(self):
        self.test_dir = Path("stress_test_data")
        self.input_dir = self.test_dir / "input"
        self.output_dir = self.test_dir / "output"
        self.config = PyMMConfig()
        
    def setup_test_environment(self):
        """Create test environment"""
        print("Setting up test environment...")
        
        # Clean previous test data
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
            
        # Create directories
        self.input_dir.mkdir(parents=True)
        self.output_dir.mkdir(parents=True)
        
    def generate_test_files(self, count: int, size_range=(1, 100)):
        """Generate test files with medical text"""
        print(f"Generating {count} test files...")
        
        # Sample medical sentences
        medical_templates = [
            "Patient presents with {symptom} and {symptom}.",
            "Diagnosis: {condition} with {modifier} severity.",
            "Treatment plan includes {medication} {dosage} daily.",
            "History of {condition} diagnosed in {year}.",
            "Physical examination reveals {finding}.",
            "Laboratory results show {lab_test} {result}.",
            "Prescribed {medication} for management of {condition}.",
            "Follow-up recommended in {timeframe}.",
            "No known allergies to {medication_class}.",
            "Vital signs: BP {bp}, HR {hr}, Temp {temp}F."
        ]
        
        symptoms = ["fever", "cough", "headache", "fatigue", "nausea", "dizziness", "chest pain", "shortness of breath"]
        conditions = ["hypertension", "diabetes mellitus", "pneumonia", "asthma", "COPD", "heart failure", "arthritis"]
        medications = ["metformin", "lisinopril", "aspirin", "atorvastatin", "omeprazole", "levothyroxine", "metoprolol"]
        modifiers = ["mild", "moderate", "severe", "acute", "chronic", "stable", "progressive"]
        
        for i in range(count):
            filename = self.input_dir / f"patient_note_{i:04d}.txt"
            
            # Generate content
            num_sentences = random.randint(size_range[0], size_range[1])
            content = []
            
            for _ in range(num_sentences):
                template = random.choice(medical_templates)
                sentence = template.format(
                    symptom=random.choice(symptoms),
                    condition=random.choice(conditions),
                    medication=random.choice(medications),
                    modifier=random.choice(modifiers),
                    dosage=f"{random.randint(1, 4) * 5}mg",
                    year=random.randint(2010, 2023),
                    finding=f"{random.choice(modifiers)} {random.choice(symptoms)}",
                    lab_test=f"{'CBC' if random.random() > 0.5 else 'BMP'}",
                    result=f"{'normal' if random.random() > 0.3 else 'abnormal'}",
                    timeframe=f"{random.randint(1, 4)} weeks",
                    medication_class=random.choice(["penicillin", "sulfa", "NSAIDs"]),
                    bp=f"{random.randint(110, 140)}/{random.randint(70, 90)}",
                    hr=random.randint(60, 100),
                    temp=round(random.uniform(97.0, 99.5), 1)
                )
                content.append(sentence)
                
            # Write file
            filename.write_text(" ".join(content))
            
            if (i + 1) % 100 == 0:
                print(f"  Generated {i + 1} files...")
                
    def run_stress_test(self, file_counts=[100, 500, 1000, 2000]):
        """Run stress tests with different file counts"""
        results = []
        
        for count in file_counts:
            print(f"\n{'='*60}")
            print(f"STRESS TEST: {count} files")
            print(f"{'='*60}")
            
            # Clean and regenerate files
            if self.input_dir.exists():
                shutil.rmtree(self.input_dir)
            self.input_dir.mkdir()
            
            if self.output_dir.exists():
                shutil.rmtree(self.output_dir)
            self.output_dir.mkdir()
            
            # Generate test files
            self.generate_test_files(count, size_range=(10, 50))
            
            # Get baseline system stats
            baseline_cpu = psutil.cpu_percent(interval=1)
            baseline_memory = psutil.virtual_memory().percent
            
            print(f"\nBaseline - CPU: {baseline_cpu:.1f}%, Memory: {baseline_memory:.1f}%")
            
            # Test optimized runner
            print("\n--- Testing OptimizedBatchRunner ---")
            result1 = self._test_runner(OptimizedBatchRunner, count)
            
            # Clean output for next test
            shutil.rmtree(self.output_dir)
            self.output_dir.mkdir()
            
            # Test ultra-optimized runner
            print("\n--- Testing UltraOptimizedBatchRunner ---")
            result2 = self._test_runner(UltraOptimizedBatchRunner, count)
            
            results.append({
                'file_count': count,
                'optimized': result1,
                'ultra_optimized': result2
            })
            
        # Print summary
        self._print_summary(results)
        
    def _test_runner(self, runner_class, file_count):
        """Test a specific runner"""
        start_time = time.time()
        
        # Monitor resources in background
        max_cpu = 0
        max_memory = 0
        
        def monitor():
            nonlocal max_cpu, max_memory
            while not hasattr(monitor, 'stop'):
                max_cpu = max(max_cpu, psutil.cpu_percent(interval=0.1))
                max_memory = max(max_memory, psutil.virtual_memory().percent)
                time.sleep(0.5)
                
        monitor_thread = threading.Thread(target=monitor)
        monitor_thread.start()
        
        try:
            # Run processing
            runner = runner_class(str(self.input_dir), str(self.output_dir), self.config)
            
            def progress_callback(stats):
                if stats['total'] > 0:
                    percent = (stats['processed'] + stats['failed']) / stats['total'] * 100
                    print(f"\rProgress: {percent:.1f}% ({stats['processed']} successful, {stats['failed']} failed)", end='')
            
            result = runner.run(progress_callback=progress_callback)
            print()  # New line after progress
            
        finally:
            monitor.stop = True
            monitor_thread.join()
            
        elapsed = time.time() - start_time
        
        # Calculate statistics
        stats = {
            'runner': runner_class.__name__,
            'total_files': file_count,
            'successful': result.get('successful', 0),
            'failed': result.get('failed', 0),
            'duration': elapsed,
            'files_per_second': result.get('successful', 0) / elapsed if elapsed > 0 else 0,
            'max_cpu': max_cpu,
            'max_memory': max_memory,
            'throughput_mbps': result.get('throughput_mbps', 0)
        }
        
        # Print results
        print(f"\nResults for {runner_class.__name__}:")
        print(f"  Processed: {stats['successful']}/{stats['total_files']} files")
        print(f"  Failed: {stats['failed']} files")
        print(f"  Duration: {stats['duration']:.1f} seconds")
        print(f"  Throughput: {stats['files_per_second']:.2f} files/second")
        print(f"  Max CPU: {stats['max_cpu']:.1f}%")
        print(f"  Max Memory: {stats['max_memory']:.1f}%")
        
        return stats
        
    def _print_summary(self, results):
        """Print test summary"""
        print(f"\n{'='*80}")
        print("STRESS TEST SUMMARY")
        print(f"{'='*80}")
        
        print(f"\n{'Files':<10} {'Runner':<25} {'Success':<10} {'Duration':<10} {'Files/s':<10} {'Max CPU':<10} {'Max Mem':<10}")
        print("-" * 80)
        
        for test in results:
            count = test['file_count']
            
            # Optimized runner
            opt = test['optimized']
            print(f"{count:<10} {opt['runner']:<25} {opt['successful']:<10} {opt['duration']:<10.1f} {opt['files_per_second']:<10.2f} {opt['max_cpu']:<10.1f} {opt['max_memory']:<10.1f}")
            
            # Ultra-optimized runner
            ultra = test['ultra_optimized']
            print(f"{'':<10} {ultra['runner']:<25} {ultra['successful']:<10} {ultra['duration']:<10.1f} {ultra['files_per_second']:<10.2f} {ultra['max_cpu']:<10.1f} {ultra['max_memory']:<10.1f}")
            
            # Performance comparison
            if opt['duration'] > 0:
                improvement = ((opt['duration'] - ultra['duration']) / opt['duration']) * 100
                print(f"{'':<10} {'Performance Gain:':<25} {improvement:>10.1f}%")
            print()
        
        print("\nRecommendations:")
        
        # Find optimal configuration
        best_throughput = 0
        best_config = None
        
        for test in results:
            for runner_type in ['optimized', 'ultra_optimized']:
                stats = test[runner_type]
                if stats['files_per_second'] > best_throughput and stats['max_memory'] < 85:
                    best_throughput = stats['files_per_second']
                    best_config = {
                        'file_count': test['file_count'],
                        'runner': stats['runner'],
                        'throughput': stats['files_per_second']
                    }
        
        if best_config:
            print(f"  Best configuration: {best_config['runner']} with batch size around {best_config['file_count']}")
            print(f"  Achieved throughput: {best_config['throughput']:.2f} files/second")
            
        # Memory recommendations
        max_memory_seen = max(
            max(test['optimized']['max_memory'], test['ultra_optimized']['max_memory'])
            for test in results
        )
        
        if max_memory_seen > 80:
            print(f"\n  WARNING: High memory usage detected ({max_memory_seen:.1f}%)")
            print("  Consider reducing chunk size or worker count")
            
    def cleanup(self):
        """Clean up test data"""
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
            print("\nTest data cleaned up")


def main():
    """Run stress tests"""
    print("PythonMetaMap Stress Testing")
    print("=" * 60)
    
    tester = StressTester()
    
    try:
        # Setup
        tester.setup_test_environment()
        
        # Check if MetaMap is available
        config = PyMMConfig()
        if not config.get('metamap_binary_path'):
            print("ERROR: MetaMap not configured!")
            print("Run 'pymm config setup' first")
            sys.exit(1)
            
        # Run tests
        tester.run_stress_test(file_counts=[100, 500, 1000])
        
    except KeyboardInterrupt:
        print("\n\nStress test interrupted by user")
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        tester.cleanup()


if __name__ == "__main__":
    main()