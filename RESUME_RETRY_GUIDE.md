# Enhanced Resume/Retry Manager Guide

## Overview

The new Resume/Retry Manager (Option 9) provides a comprehensive TUI for managing failed jobs, cleaning logs, and maintaining a clean processing state.

## Features

### 1. Failed Job Management
- View all failed jobs with detailed error information
- Retry specific jobs or all failed jobs
- Smart retry with adjusted settings based on failure patterns
- Export failure reports for analysis

### 2. Failed File Tracking
- Automatically tracks files that failed to process
- Shows error messages and processing timestamps
- Retry specific files or all failed files
- Remove failed records while keeping source files

### 3. Log File Management
- **Delete all logs** - Complete cleanup
- **Delete old logs** - Remove logs older than 7 or 30 days
- **Delete failed job logs** - Clean up only error logs
- **View specific logs** - Browse and read any log file
- **Archive logs** - Move logs to timestamped archive folders
- **Search in logs** - Find specific patterns across all logs

### 4. Clean State Reset
- **Complete reset** option to start fresh
- Removes all processing history and logs
- Clears job history and manifests
- Preserves input and output files
- Requires double confirmation for safety

### 5. In-Progress Job Management
- View currently running jobs
- Cancel stuck or hanging jobs
- See detailed progress information

## Menu Options

1. **View Failed Jobs** - See all failed jobs and files with error details
2. **Retry Failed Jobs** - Multiple retry strategies:
   - Retry all failed jobs
   - Retry specific jobs by selection
   - Retry all failed files
   - Retry specific files
   - Smart retry (analyzes patterns)
3. **Manage Log Files** - Complete log lifecycle management
4. **Clean Failed File Records** - Remove tracking without deleting files
5. **Reset to Clean State** - Start completely fresh
6. **Resume In-Progress Jobs** - Manage running jobs
7. **View Job Details** - Deep dive into any job
8. **Export Failure Report** - Generate detailed failure analysis

## Usage Examples

### Cleaning Up After Errors
```
1. Select "Manage Log Files"
2. Choose "Delete failed job logs only"
3. Confirm deletion
```

### Starting Fresh
```
1. Select "Reset to Clean State"
2. Review what will be deleted
3. Confirm with first prompt
4. Type "RESET" to confirm
```

### Retry Failed Processing
```
1. Select "View Failed Jobs" to see what failed
2. Select "Retry Failed Jobs"
3. Choose retry strategy:
   - All failed files
   - Specific selection
   - Smart retry (recommended)
```

### Investigating Failures
```
1. Select "View Job Details"
2. Choose a failed job
3. View error messages and log preview
4. Export failure report for documentation
```

## Benefits

- **No more manual log cleanup** - Automated log management
- **Smart failure handling** - Learn from errors and retry intelligently
- **Clean state option** - Easy way to start fresh without losing data
- **Comprehensive tracking** - Know exactly what failed and why
- **Safe operations** - Multiple confirmations for destructive actions

## Tips

1. Use "Smart Retry" for automatic optimization based on failure patterns
2. Archive logs before major cleanup to preserve history
3. Export failure reports before resetting to clean state
4. The clean state reset does NOT delete your input or output files

## File Locations

- Logs: `output_csvs/logs/`
- Processing manifest: `pymm_data/processing_manifest.json`
- State files: `.pymm_state/`
- Archives: `output_csvs/logs/archive/[timestamp]/`