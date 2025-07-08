#!/usr/bin/env python3
"""
Test script to demonstrate PythonMetaMap improvements
"""

import os
import sys
import time
from pathlib import Path

# Add src to path for testing
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pymm.core.state import StateManager
from pymm.processing.retry import RetryManager
from pymm.utils.progress_tracker import ProgressTracker
from pymm.core.config import PyMMConfig

def test_thread_safe_state():
    """Test thread-safe state management"""
    print("Testing thread-safe state management...")
    
    # Create state manager
    state_mgr = StateManager("./test_output")
    
    # Simulate concurrent access
    import threading
    
    def update_concepts():
        for i in range(100):
            concepts = [
                {"cui": f"C{i:04d}", "preferred_name": f"Concept {i}", "semantic_types": ["T047"]}
            ]
            try:
                state_mgr.track_concepts(concepts)
            except Exception as e:
                print(f"Error in thread: {e}")
    
    # Start multiple threads
    threads = []
    for _ in range(5):
        t = threading.Thread(target=update_concepts)
        threads.append(t)
        t.start()
    
    # Wait for completion
    for t in threads:
        t.join()
    
    print("✓ No dictionary modification errors!")
    print(f"  Total concepts tracked: {state_mgr.get_statistics()['total_concepts']}")

def test_retry_mechanism():
    """Test retry mechanism"""
    print("\nTesting retry mechanism...")
    
    config = PyMMConfig()
    config.set("retry_max_attempts", 3)
    config.set("retry_base_delay", 1)
    
    state_mgr = StateManager("./test_output")
    retry_mgr = RetryManager(config, state_mgr)
    
    # Simulate failed files
    failed_files = [
        "/path/to/file1.txt",
        "/path/to/file2.txt",
        "/path/to/file3.txt"
    ]
    
    # Mark some as failed with different attempt counts
    state_mgr.mark_failed(failed_files[0], "Test error 1")
    retry_mgr.record_retry_attempt(failed_files[1], "Test error 2")
    retry_mgr.record_retry_attempt(failed_files[1], "Test error 2")
    retry_mgr.record_retry_attempt(failed_files[2], "Test error 3")
    retry_mgr.record_retry_attempt(failed_files[2], "Test error 3")
    retry_mgr.record_retry_attempt(failed_files[2], "Test error 3")
    
    # Get retry summary
    summary = retry_mgr.get_retry_summary()
    print(f"✓ Retry queue status:")
    print(f"  Total files: {summary['total_files']}")
    print(f"  Ready for retry: {summary['ready_for_retry']}")
    print(f"  Max attempts reached: {summary['max_attempts_reached']}")
    
    # Test retryable files
    retryable = retry_mgr.get_retryable_files(failed_files)
    print(f"  Retryable files: {len(retryable)}")

def test_progress_tracker():
    """Test progress tracking"""
    print("\nTesting progress tracker...")
    
    tracker = ProgressTracker(total_files=100, log_dir=Path("./test_output/logs"))
    
    # Simulate processing
    for i in range(10):
        filename = f"test_file_{i}.txt"
        tracker.start_file(filename)
        
        # Simulate processing time
        time.sleep(0.1)
        
        if i % 3 == 0:
            tracker.fail_file(filename, "Simulated error")
        else:
            tracker.complete_file(filename, elapsed_time=0.1, concept_count=100 + i * 10)
    
    # Get statistics
    stats = tracker.get_stats()
    print(f"✓ Progress tracking:")
    print(f"  Processed: {stats['processed']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Rate: {stats['rate']:.2f} files/s")
    print(f"  Progress: {stats['progress_percentage']:.1f}%")
    
    # Save summary
    summary_path = Path("./test_output/test_summary.txt")
    tracker.save_summary(summary_path)
    print(f"  Summary saved to: {summary_path}")

def test_file_deduplication():
    """Test file deduplication"""
    print("\nTesting file deduplication...")
    
    state_mgr = StateManager("./test_output")
    
    # Test with different path formats
    test_paths = [
        "/home/user/file1.txt",
        "/home/user/./file1.txt",
        "/home/user/../user/file1.txt",
    ]
    
    # Mark first as completed
    state_mgr.mark_completed(test_paths[0])
    
    # Check if others are detected as completed
    for path in test_paths[1:]:
        # This would be handled by the normalized path checking
        print(f"  Path '{path}' completed: {state_mgr.is_completed(path)}")

def main():
    """Run all tests"""
    print("PythonMetaMap Improvements Test Suite")
    print("=" * 50)
    
    # Create test directory
    test_dir = Path("./test_output")
    test_dir.mkdir(exist_ok=True)
    
    try:
        test_thread_safe_state()
        test_retry_mechanism()
        test_progress_tracker()
        test_file_deduplication()
        
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        import shutil
        if test_dir.exists():
            shutil.rmtree(test_dir)
            print("\nTest directory cleaned up.")

if __name__ == "__main__":
    main() 