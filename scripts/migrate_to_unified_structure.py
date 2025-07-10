#!/usr/bin/env python3
"""Migrate PythonMetaMap to unified directory structure

This script migrates files from scattered directories to the unified pymm_data structure:
- input_notes/* -> pymm_data/input/
- test_input/* -> pymm_data/input/test/
- output_csvs/* -> pymm_data/output/
- test_output/* -> pymm_data/output/test/

It also removes redundant files and directories.
"""
import os
import shutil
from pathlib import Path
import json
from datetime import datetime


def migrate_to_unified_structure(dry_run=True):
    """Migrate to unified pymm_data structure
    
    Args:
        dry_run: If True, only show what would be done without making changes
    """
    print(f"{'[DRY RUN] ' if dry_run else ''}Starting migration to unified structure...")
    
    # Define migrations
    migrations = [
        {
            "source": "input_notes",
            "target": "pymm_data/input",
            "pattern": "*.txt",
            "description": "input files"
        },
        {
            "source": "test_input",
            "target": "pymm_data/input/test",
            "pattern": "*",
            "description": "test input files"
        },
        {
            "source": "output_csvs",
            "target": "pymm_data/output",
            "pattern": "*",
            "description": "output files",
            "exclude": ["logs", ".pymm_state.json", ".nfs*"]
        },
        {
            "source": "test_output",
            "target": "pymm_data/output/test",
            "pattern": "*",
            "description": "test output files"
        }
    ]
    
    # Track operations
    operations = {
        "moved_files": 0,
        "moved_dirs": 0,
        "skipped": 0,
        "errors": 0,
        "operations": []
    }
    
    # Process each migration
    for migration in migrations:
        source_dir = Path(migration["source"])
        target_dir = Path(migration["target"])
        
        if not source_dir.exists():
            print(f"  Source directory {source_dir} does not exist, skipping")
            continue
            
        print(f"\nMigrating {migration['description']} from {source_dir} to {target_dir}")
        
        # Create target directory
        if not dry_run:
            target_dir.mkdir(parents=True, exist_ok=True)
        else:
            print(f"  Would create directory: {target_dir}")
            
        # Get files to move
        if migration["pattern"] == "*":
            items = list(source_dir.iterdir())
        else:
            items = list(source_dir.glob(migration["pattern"]))
            
        # Filter excludes
        if "exclude" in migration:
            exclude_patterns = migration["exclude"]
            filtered_items = []
            for item in items:
                skip = False
                for pattern in exclude_patterns:
                    if pattern.startswith(".nfs"):
                        if item.name.startswith(".nfs"):
                            skip = True
                            break
                    elif pattern == item.name:
                        skip = True
                        break
                if not skip:
                    filtered_items.append(item)
            items = filtered_items
            
        # Move items
        for item in items:
            target_path = target_dir / item.name
            
            # Check if already exists
            if target_path.exists():
                print(f"  Target already exists, skipping: {item.name}")
                operations["skipped"] += 1
                continue
                
            # Move item
            if dry_run:
                print(f"  Would move: {item} -> {target_path}")
            else:
                try:
                    shutil.move(str(item), str(target_path))
                    print(f"  Moved: {item.name}")
                    
                    if item.is_file():
                        operations["moved_files"] += 1
                    else:
                        operations["moved_dirs"] += 1
                        
                    operations["operations"].append({
                        "type": "move",
                        "source": str(item),
                        "target": str(target_path),
                        "timestamp": datetime.now().isoformat()
                    })
                    
                except Exception as e:
                    print(f"  ERROR moving {item}: {e}")
                    operations["errors"] += 1
                    
    # Clean up empty directories
    print("\nCleaning up empty directories...")
    empty_dirs = ["input_notes", "output_csvs", "test_input", "test_output", "examples"]
    
    for dir_name in empty_dirs:
        dir_path = Path(dir_name)
        if dir_path.exists():
            try:
                # Check if empty
                if not any(dir_path.iterdir()):
                    if dry_run:
                        print(f"  Would remove empty directory: {dir_path}")
                    else:
                        dir_path.rmdir()
                        print(f"  Removed empty directory: {dir_path}")
                        operations["operations"].append({
                            "type": "remove_dir",
                            "path": str(dir_path),
                            "timestamp": datetime.now().isoformat()
                        })
                else:
                    # Check what's left
                    remaining = list(dir_path.iterdir())
                    print(f"  Directory {dir_path} not empty, contains {len(remaining)} items:")
                    for item in remaining[:5]:
                        print(f"    - {item.name}")
                    if len(remaining) > 5:
                        print(f"    ... and {len(remaining) - 5} more")
            except Exception as e:
                print(f"  ERROR checking/removing {dir_path}: {e}")
                
    # Update configuration defaults
    print("\nUpdating configuration defaults...")
    config_updates = {
        "default_input_dir": "pymm_data/input",
        "default_output_dir": "pymm_data/output",
        "log_dir": "pymm_data/logs",
        "temp_dir": "pymm_data/temp"
    }
    
    if dry_run:
        print("  Would update PyMMConfig defaults:")
        for key, value in config_updates.items():
            print(f"    {key} = {value}")
    else:
        # This would be done in the actual config file
        print("  Configuration updates should be made in src/pymm/core/config.py")
        
    # Save migration log
    if not dry_run and operations["operations"]:
        log_file = Path("pymm_data/migration_log.json")
        log_file.parent.mkdir(exist_ok=True)
        
        with open(log_file, "w") as f:
            json.dump({
                "migration_date": datetime.now().isoformat(),
                "summary": {
                    "files_moved": operations["moved_files"],
                    "directories_moved": operations["moved_dirs"],
                    "skipped": operations["skipped"],
                    "errors": operations["errors"]
                },
                "operations": operations["operations"]
            }, f, indent=2)
            
        print(f"\nMigration log saved to: {log_file}")
        
    # Print summary
    print("\nMigration Summary:")
    print(f"  Files moved: {operations['moved_files']}")
    print(f"  Directories moved: {operations['moved_dirs']}")
    print(f"  Skipped (already exist): {operations['skipped']}")
    print(f"  Errors: {operations['errors']}")
    
    if dry_run:
        print("\nThis was a dry run. To perform the actual migration, run with dry_run=False")
        
    return operations


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate to unified pymm_data structure")
    parser.add_argument("--execute", action="store_true", help="Execute the migration (default is dry run)")
    parser.add_argument("--backup", action="store_true", help="Create backup before migration")
    
    args = parser.parse_args()
    
    if args.backup and args.execute:
        # Create backup
        backup_dir = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"Creating backup in {backup_dir}...")
        
        dirs_to_backup = ["input_notes", "output_csvs", "test_input", "test_output"]
        for dir_name in dirs_to_backup:
            if Path(dir_name).exists():
                shutil.copytree(dir_name, f"{backup_dir}/{dir_name}")
                print(f"  Backed up {dir_name}")
                
    # Run migration
    migrate_to_unified_structure(dry_run=not args.execute)
    

if __name__ == "__main__":
    main() 