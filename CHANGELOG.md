# Changelog

All notable changes to PythonMetaMap will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [8.6.8] - 2025-01-09

### Added
- Proper `MetaMapInstancePool` implementation for efficient instance management
- Background processing option directly in batch processing menu
- Full implementation of Clinical Analysis feature with preset selection
- Comprehensive Export Analysis Report with markdown and Excel outputs
- Log analysis in Resume/Retry functionality to identify and retry failed files
- Dynamic instance pool sizing based on system resources
- Port management for parallel MetaMap instances

### Fixed
- Quick Setup status display now correctly shows "Found" when Java and MetaMap are detected
- Fixed `WorkerPool.__init__() got an unexpected keyword argument 'max_instances'` error
- Fixed all batch processing modes (Standard, Ultra, Memory-Efficient)
- Fixed release_instance calls to include proper parameters
- Removed all placeholder implementations and TODO comments

### Changed
- Improved separation between batch and background processing
- Enhanced error handling in instance pool management
- Better resource utilization with adaptive pool sizing

### Technical Details
- Created `instance_pool.py` with proper MetaMap instance lifecycle management
- Updated imports in `pool_manager.py` to use the new MetaMapInstancePool
- Fixed dictionary key mismatches in Quick Setup status display
- Implemented actual functionality for all previously placeholder features

## Previous Versions

[Previous version history to be added]