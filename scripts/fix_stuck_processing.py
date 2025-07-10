#!/usr/bin/env python3
"""Fix stuck processing by updating file status in the unified tracking manifest"""
import sys
import json
from pathlib import Path
from datetime import datetime


def fix_stuck_files(data_dir: str = "./pymm_data"):
    """Fix files stuck in 'in_progress' status"""
    manifest_file = Path(data_dir) / "processing_manifest.json"
    
    if not manifest_file.exists():
        print(f"No manifest found at {manifest_file}")
        return
    
    # Load manifest
    with open(manifest_file, 'r') as f:
        manifest = json.load(f)
    
    stuck_files = []
    
    # Find stuck files
    for file_path, record in manifest['files'].items():
        if record['status'] == 'in_progress':
            stuck_files.append(file_path)
    
    if not stuck_files:
        print("No stuck files found!")
        return
    
    print(f"Found {len(stuck_files)} stuck files:")
    for file in stuck_files:
        print(f"  - {file}")
    
    # Ask for confirmation
    response = input("\nMark these files as failed? (y/n): ")
    if response.lower() != 'y':
        print("Cancelled")
        return
    
    # Update status
    for file_path in stuck_files:
        manifest['files'][file_path]['status'] = 'failed'
        manifest['files'][file_path]['error_message'] = 'Processing timeout - marked as failed by fix script'
        manifest['files'][file_path]['process_date'] = datetime.now().isoformat()
    
    # Update manifest metadata
    manifest['last_updated'] = datetime.now().isoformat()
    
    # Save updated manifest
    with open(manifest_file, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    print(f"\nUpdated {len(stuck_files)} files to 'failed' status")
    print("You can now retry processing with: pymm process")


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "./pymm_data"
    fix_stuck_files(data_dir) 