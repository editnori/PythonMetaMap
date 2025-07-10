# PythonMetaMap Unified File Tracking System

## Overview

The unified tracking system consolidates all file management into a single, organized structure that automatically tracks which files have been processed, when they were processed, and their results.

## Key Benefits

1. **Automatic Detection**: No more manually checking which files have been processed
2. **Smart Processing**: Only process new or modified files
3. **Organized Structure**: All data in one location with clear input/output separation
4. **Processing History**: Complete manifest of all processing with statistics
5. **Failure Recovery**: Easily retry failed files

## Directory Structure

When using unified tracking, all files are organized as follows:

```
pymm_data/
├── input/              # Place your text files here
├── output/             # Processed CSV files appear here
└── processing_manifest.json  # Tracking metadata
```

## Usage

### 1. Enable Unified Tracking

From the main menu:
```
7. Configuration → 2. Directory Settings → Use unified file tracking? [Y]
```

### 2. Process Files with Smart Tracking

From the main menu:
```
4. Batch Process
```

You'll see:
- Current processing status (how many files processed, failed, etc.)
- Options to process all unprocessed files or select specific ones
- Ability to retry failed files

### Processing Options

1. **Process all unprocessed files** - Automatically finds and processes new files
2. **Process specific number** - Process a subset (e.g., first 10 files)
3. **Retry failed files** - Re-attempt files that previously failed
4. **Custom selection** - Choose specific files by number
5. **Show detailed file list** - View status of all files

### Example Workflow

1. Place your text files in `pymm_data/input/`
2. Run batch processing - it automatically detects new files
3. Check results in `pymm_data/output/`
4. Run batch processing again - it knows which files are already done!

## Traditional Mode

If you prefer the old separate directories approach:
- Set "Use unified file tracking?" to No in configuration
- Continue using separate input_notes/ and output_csvs/ directories
- Files won't be automatically tracked

## Manifest File

The `processing_manifest.json` tracks:
- Input file hash (to detect changes)
- Processing date and time
- Number of concepts found
- Processing time
- Error messages for failed files
- Overall statistics

## Migration

To migrate existing files to unified tracking:
1. Copy your text files to `pymm_data/input/`
2. The system will treat them as new files to process
3. Previous outputs remain in your old output directory