# PythonMetaMap Documentation

## Version 8.4.4

Welcome to the PythonMetaMap documentation! This guide provides comprehensive information about using PythonMetaMap for medical text processing.

## Quick Links

### Getting Started
- [Setup Guide](SETUP_GUIDE.md) - Installation and initial configuration
- [Java Installation Guide](JAVA_INSTALLATION_GUIDE.md) - Installing required Java dependencies
- [Complete Guide](PYTHONMETAMAP_COMPLETE_GUIDE.md) - Comprehensive usage documentation

### Features
- [Ultimate TUI Guide](ULTIMATE_TUI_COMPLETE.md) - Interactive terminal interface with PyMM CLI
- [Enhanced Analysis Guide](ENHANCED_ANALYSIS_GUIDE.md) - Advanced analysis features
- [Analysis Templates](ANALYSIS_TEMPLATES_GUIDE.md) - Pre-configured medical analysis templates

### Technical Details
- [Download Instructions](DOWNLOAD_INSTRUCTIONS.md) - MetaMap binary download guide
- [Improvements Summary](IMPROVEMENTS_SUMMARY.md) - Recent enhancements and optimizations

## What's New in v8.4.4

### PyMM CLI Interface
- Beautiful new banner with filled ASCII art
- Enhanced file explorer with color-coded files
- Full WSL support with Windows Explorer integration
- Advanced search functionality across CSV files

### Analysis Templates
- Clinical Summary Analysis
- Radiology Report Analysis
- Medication Review
- Symptom Analysis
- Laboratory Results Analysis

### Performance Improvements
- Optimized batch processing for 1000+ files
- Memory-efficient streaming file discovery
- Dynamic worker management
- Background processing capabilities

## Quick Start

```bash
# Install PythonMetaMap
pip install pythonmetamap

# Launch interactive CLI
pymm -i

# Process files
pymm process input_notes/ output_csvs/

# Analyze results with templates
pymm -i
# Select: 5 (Analysis Tools) → 6 (Template-based Analysis)
```

## Support

For issues or questions:
- GitHub Issues: https://github.com/editnori/PythonMetaMap/issues
- Documentation: This directory

---
*PythonMetaMap - Advanced Medical Text Processing Suite*