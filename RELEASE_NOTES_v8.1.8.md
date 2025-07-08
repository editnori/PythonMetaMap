Release Notes for PythonMetaMap v8.1.8

This release focuses on codebase cleanup, documentation improvements, and making the project more maintainable for future development. All functionality from previous versions remains intact while the project structure is now cleaner and more organized.

Major Changes

Project Structure Reorganization
- Created dedicated directories for documentation (docs/), tests (tests/), and examples
- Moved all test files from root directory to proper test directory
- Consolidated documentation files into docs/ folder
- Removed duplicate and temporary files cluttering the repository

Dependency Management
- Fixed requirements.txt to include all necessary dependencies including numpy
- Created requirements-dev.txt for development tools like pytest, black, and sphinx
- Removed unused dependencies (JPype1, colorama) from runtime requirements
- Properly commented all dependencies with their purpose

Documentation Overhaul
- Completely rewrote README.md in flowing prose style for better readability
- Documented all CLI commands including new analysis and visualization features
- Added comprehensive usage examples for every major feature
- Included performance optimization tips and troubleshooting guidance

Code Cleanup
- Removed backup directories and old code versions
- Cleaned up untracked files and build artifacts
- Enhanced .gitignore to prevent future clutter
- Maintained all existing functionality while improving organization

Command Updates
- All CLI commands remain the same with no breaking changes
- Interactive mode (pymm -i) continues as the primary interface
- Process, server, config, stats, and analysis commands work as before
- Background processing and retry mechanisms unchanged

What This Means for Users

If you're upgrading from a previous version, everything will work exactly as before. Your configuration files, processing workflows, and scripts will continue to function without modification. The main difference is that the codebase is now cleaner and the documentation is more comprehensive.

Installation remains simple with pip install pythonmetamap followed by pymm install to set up MetaMap. The interactive menu (pymm) provides the same friendly interface for processing medical text files.

Next Steps

Future releases will build on this clean foundation to add new features requested by the community. The organized structure makes it easier to contribute improvements and fix issues. Submit feedback through GitHub issues to help shape the project's direction.