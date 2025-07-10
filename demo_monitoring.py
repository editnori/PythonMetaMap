#!/usr/bin/env python3
"""Demonstration of the enhanced monitoring system"""

import time
import threading
from pathlib import Path
from src.pymm.monitoring.unified_monitor import UnifiedMonitor
from src.pymm.core.config import PyMMConfig

def simulate_processing(monitor: UnifiedMonitor):
    """Simulate file processing to demonstrate monitoring"""
    
    # Create a batch
    batch_id = "demo_batch_001"
    monitor.create_batch(batch_id, 10)
    
    # Simulate processing 10 files
    for i in range(10):
        filename = f"test_file_{i+1}.txt"
        
        # Start file
        monitor.progress_tracker.start_file(filename, 1024 * (i + 1), batch_id)
        monitor.log("INFO", "Processor", f"Starting {filename}")
        
        # Simulate stages
        stages = [
            ("Reading file", 20),
            ("Analyzing content", 40),
            ("Processing with MetaMap", 60),
            ("Extracting concepts", 80),
            ("Writing results", 100)
        ]
        
        for stage, progress in stages:
            monitor.update_file_progress(batch_id, filename, stage, progress)
            time.sleep(0.5)  # Simulate work
            
            # Add some logs
            if progress == 40:
                monitor.log("DEBUG", "Analyzer", f"Found medical terms in {filename}")
            elif progress == 80:
                concepts = 10 + i * 2
                monitor.log("INFO", "Extractor", f"Extracted {concepts} concepts from {filename}")
        
        # Complete or fail some files
        if i % 4 == 3:  # Fail every 4th file
            monitor.fail_file(batch_id, filename, "Simulated error: timeout")
            monitor.log("ERROR", "Processor", f"Failed to process {filename}")
        else:
            concepts = 10 + i * 2
            monitor.complete_file(batch_id, filename, concepts)
            
            # Update concept statistics
            for j in range(concepts):
                monitor.statistics_dashboard.update_concept(
                    f"concept_{i}_{j}",
                    "dsyn" if j % 2 == 0 else "sosy"
                )
    
    # Complete the batch
    monitor.statistics_dashboard.complete_batch(batch_id)
    monitor.log("INFO", "BatchManager", "Demo batch completed")


def main():
    """Run the monitoring demonstration"""
    print("Enhanced Monitoring System Demo")
    print("=" * 50)
    print("\nThis demo will show:")
    print("- Real-time progress tracking")
    print("- Live logging with different levels")
    print("- Resource monitoring")
    print("- Statistics dashboard")
    print("\nThe monitor will run for about 30 seconds.")
    print("\nPress Enter to start...")
    input()
    
    # Create config
    config = PyMMConfig()
    
    # Create monitor
    monitor = UnifiedMonitor([Path("./output_csvs")])
    
    # Start monitor
    monitor.start()
    
    try:
        # Add initial logs
        monitor.log("INFO", "System", "Starting monitoring demonstration")
        monitor.log("INFO", "Config", f"Output directory: {config.get('default_output_dir', './output_csvs')}")
        
        # Start simulated processing in background
        processing_thread = threading.Thread(
            target=simulate_processing,
            args=(monitor,),
            daemon=True
        )
        processing_thread.start()
        
        # Add some resource-heavy simulation
        monitor.log("INFO", "Monitor", "Simulating resource usage...")
        
        # Keep running for demo duration
        start_time = time.time()
        while time.time() - start_time < 30:
            time.sleep(0.1)
            
            # Add periodic logs
            if int(time.time() - start_time) % 5 == 0:
                monitor.log("INFO", "Heartbeat", f"System running for {int(time.time() - start_time)}s")
        
        # Final summary
        monitor.log("INFO", "Demo", "Demonstration completed successfully")
        
        # Wait a bit for final updates
        time.sleep(2)
        
    except KeyboardInterrupt:
        monitor.log("WARNING", "Demo", "Demonstration interrupted by user")
    
    finally:
        # Stop monitor
        monitor.stop()
        
        print("\n" * 2)
        print("Demo completed!")
        print("\nKey features demonstrated:")
        print("✓ Real-time progress bars for each file")
        print("✓ Live logging with filtering capabilities")
        print("✓ Resource monitoring (CPU, memory, disk)")
        print("✓ Global statistics tracking")
        print("✓ Unified dashboard view")
        
        # Show final stats
        stats = monitor.statistics_dashboard.global_stats
        print(f"\nFinal Statistics:")
        print(f"- Files processed: {stats.total_files_processed}")
        print(f"- Files failed: {stats.total_files_failed}")
        print(f"- Total concepts: {stats.total_concepts_extracted}")
        print(f"- Processing rate: {stats.files_per_minute:.1f} files/minute")


if __name__ == "__main__":
    main()