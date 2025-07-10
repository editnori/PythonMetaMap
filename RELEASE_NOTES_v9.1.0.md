# PythonMetaMap v9.1.0 Release Notes

## Major Features Added

### 1. Enhanced Unified Monitor with Interactive Features
- **File Management**: Added ability to delete selected files and folders directly from the file explorer
- **Interactive Job Management**: Click on jobs to open their output directories
- **CSV Preview**: View processed CSV files directly in the monitor
- **Job Progress Tracking**: See file-by-file progress for all running jobs
- **Quick Process**: Select and process files instantly from the file explorer

### 2. Comprehensive Batch Process Validation
- **Pre-flight Checks**: Validates all requirements before processing starts
  - Java runtime verification
  - MetaMap binary path validation
  - Server binaries check
  - Input/output directory validation
  - System resources check (memory, disk space)
  - Network port availability
- **Detailed Validation Report**: Shows exactly what passed/failed
- **User Confirmation**: Shows file count and asks for confirmation before processing

### 3. Improved File Explorer
- Restored as standalone menu option (Option 2)
- Enhanced with delete functionality
- Quick process capability
- Better file type icons and visual indicators

## Key Improvements

1. **Better Error Handling**: All processing paths now check and start servers automatically
2. **CSV Output Guarantee**: All processing ensures CSV format output
3. **Interactive Features**: Monitor now allows direct interaction with jobs and files
4. **Validation First**: Batch processing validates environment before starting

## File Changes

### New Files
- `src/pymm/processing/validated_batch_runner.py` - Batch runner with comprehensive validation
- `src/pymm/cli/enhanced_unified_monitor.py` - Enhanced monitor with interactive features

### Modified Files
- `src/pymm/cli/interactive.py` - Updated to use validated batch runner and enhanced monitor
- `setup.py` - Version bumped to 9.1.0

## Usage

### Enhanced Monitor
```bash
pymm
# Select option 3 (Monitor)
# Use number keys 1-5 to switch views
# In Jobs view: Press Enter/O to open output directory
# In Files view: Press Delete/X to delete selected files
```

### Validated Batch Processing
```bash
pymm
# Select option 4 (Batch Process)
# Validation will run automatically
# Review validation results
# Confirm to proceed with processing
```

## Upgrade Instructions
```bash
pip install --upgrade pythonmetamap==9.1.0
```