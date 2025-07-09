# Release Notes - PythonMetaMap v8.3.8

## 🚀 Enhanced User Experience Release

### Summary
This release focuses on improving the user experience with better guidance for batch processing modes and fixing critical bugs in the interactive interface.

### ✨ New Features

#### Intelligent Batch Processing Mode Selection
- **Detailed Mode Descriptions**: Each processing mode now shows:
  - Recommended file count ranges
  - Key features and benefits
  - Use case scenarios
  
- **Auto-Select Mode**: New intelligent mode selection that:
  - Analyzes your dataset size
  - Checks available system memory
  - Automatically picks the optimal processing mode
  - Provides clear reasoning for the selection

#### Processing Modes Explained
1. **Standard Mode (OptimizedBatchRunner)**
   - Best for: 10-500 files
   - Balanced performance and memory usage
   - Smart retry handling
   
2. **Ultra Mode (UltraOptimizedBatchRunner)**
   - Best for: 500-5000 files
   - Advanced worker management
   - Health monitoring & auto-recovery
   - Adaptive timeout adjustment
   
3. **Memory-Efficient Mode (Chunked)**
   - Best for: 5000+ files
   - Processes files in small batches
   - Minimal memory footprint
   - Prevents OOM errors

### 🐛 Bug Fixes

- **Fixed**: `_configure_server` method missing error in configuration menu
- **Fixed**: `_run_processing_visual` missing files parameter in batch processing
- **Fixed**: Added all missing configuration submenu methods:
  - Processing settings configuration
  - Server settings configuration
  - Save/Load configuration options
  - Reset to defaults functionality

### 🔧 Improvements

- **Author Information**: Updated with full professional credentials
  - Dr. Layth Qassem, PharmD, MS
  - Contact: layth.qassem@vanderbilt.edu
  
- **Better Error Handling**: Added file existence checks before processing
- **User Feedback**: Clear messages when no files found in directory

### 📋 Technical Details

- Added comprehensive configuration management methods
- Improved file discovery before batch processing
- Enhanced mode selection with visual panels
- Auto-detection of optimal processing mode based on:
  - File count
  - Available RAM
  - System resources

### Installation
```bash
# Upgrade via pip
pip install --upgrade pythonmetamap

# Or from source
git pull origin master
pip install -e .
```

---

*Making medical text processing more intelligent and user-friendly!*