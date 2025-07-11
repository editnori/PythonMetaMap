#!/usr/bin/env python3
"""
Production script for processing 30k medical notes
Optimized for maximum performance with clean UI
"""
import sys
import argparse
from pathlib import Path
from src.pymm.cli.clean_batch import CleanBatchProcessor

def main():
    parser = argparse.ArgumentParser(
        description="Process medical notes with MetaMap (optimized for 30k+ files)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process with default settings (80% CPU cores)
  python process_30k.py
  
  # Process with custom directories
  python process_30k.py --input /path/to/input --output /path/to/output
  
  # Run in background
  nohup python process_30k.py > processing.log 2>&1 &
  
Performance estimates (based on testing):
  - 8 workers:  ~33 hours for 30k files
  - 16 workers: ~17 hours for 30k files
  - 32 workers: ~8.5 hours for 30k files
        """
    )
    
    parser.add_argument(
        "--input", 
        default="./pymm_data/input",
        help="Input directory containing .txt files (default: ./pymm_data/input)"
    )
    parser.add_argument(
        "--output", 
        default="./pymm_data/output",
        help="Output directory for CSV files (default: ./pymm_data/output)"
    )
    
    args = parser.parse_args()
    
    # Validate directories
    input_dir = Path(args.input)
    if not input_dir.exists():
        print(f"Error: Input directory not found: {input_dir}")
        sys.exit(1)
    
    # Create processor and run
    processor = CleanBatchProcessor(args.input, args.output)
    
    # Check if we have files
    files = processor.collect_files()
    if not files:
        print("No input files found!")
        sys.exit(1)
    
    print(f"\nFound {len(files)} files to process")
    print(f"Using {processor.workers} parallel workers")
    print("\nStarting processing...\n")
    
    # Process
    results = processor.process()
    
    # Exit code based on success
    if results["processed"] > 0:
        print(f"\n✓ Successfully processed {results['processed']} files")
        return 0
    else:
        print(f"\n✗ Processing failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())