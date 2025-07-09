#!/usr/bin/env python3
"""Migration script to transition to the clean interactive UI"""

import os
import shutil
import sys
from pathlib import Path

def main():
    """Run migration to clean UI"""
    print("PythonMetaMap UI Migration Script")
    print("=================================")
    
    # Get project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    cli_dir = project_root / "src" / "pymm" / "cli"
    
    # Files to migrate
    old_files = [
        cli_dir / "interactive.py",
        cli_dir / "enhanced_interactive.py"
    ]
    
    new_file = cli_dir / "interactive_clean.py"
    
    print(f"\nChecking files in {cli_dir}...")
    
    # Check if new file exists
    if not new_file.exists():
        print(f"ERROR: New clean interface not found at {new_file}")
        sys.exit(1)
    
    # Backup old files
    backup_dir = cli_dir / "backup"
    backup_dir.mkdir(exist_ok=True)
    
    for old_file in old_files:
        if old_file.exists():
            backup_path = backup_dir / f"{old_file.name}.backup"
            print(f"Backing up {old_file.name} -> {backup_path}")
            shutil.copy2(old_file, backup_path)
    
    # Rename new file to replace old one
    print(f"\nReplacing interactive.py with clean version...")
    
    # Remove old interactive.py
    if (cli_dir / "interactive.py").exists():
        os.remove(cli_dir / "interactive.py")
    
    # Rename clean version
    shutil.move(new_file, cli_dir / "interactive.py")
    print("✓ Replaced interactive.py with clean version")
    
    # Remove enhanced_interactive.py if exists
    enhanced_file = cli_dir / "enhanced_interactive.py"
    if enhanced_file.exists():
        os.remove(enhanced_file)
        print("✓ Removed enhanced_interactive.py")
    
    # Update main.py import back to normal
    main_file = cli_dir / "main.py"
    if main_file.exists():
        content = main_file.read_text()
        content = content.replace(
            "from .interactive_clean import interactive_mode",
            "from .interactive import interactive_mode"
        )
        main_file.write_text(content)
        print("✓ Updated imports in main.py")
    
    # Clean up __pycache__
    pycache_dir = cli_dir / "__pycache__"
    if pycache_dir.exists():
        shutil.rmtree(pycache_dir)
        print("✓ Cleaned __pycache__")
    
    print("\n✅ Migration complete!")
    print("\nBackups saved in:", backup_dir)
    print("\nYou can now use 'pymm -i' or 'pymm --interactive' to launch the streamlined UI")
    
if __name__ == "__main__":
    main()