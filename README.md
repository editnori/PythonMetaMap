# PythonMetaMap

A Python wrapper for NLM MetaMap that handles the complexity of medical text processing. This tool manages MetaMap servers automatically, processes files in parallel, recovers from failures, and provides clinical analysis with visualizations.

## Quick Start

```bash
# Install the package
pip install pythonmetamap

# Download and install MetaMap (about 1GB, takes a few minutes)
pymm install

# Start the interactive interface
pymm
```

## Installation Guide

### Prerequisites

- Python 3.8 or newer
- Java 8+ (MetaMap runs on JVM)
- 4GB+ RAM recommended
- Linux, macOS, or WSL on Windows

### Step-by-Step Installation

1. **Install PythonMetaMap**
   ```bash
   pip install pythonmetamap
   ```

2. **Install MetaMap**
   ```bash
   pymm install
   ```
   This downloads MetaMap 2020, compiles it, and saves the configuration automatically.

3. **Verify Installation**
   ```bash
   pymm setup
   ```
   If issues are found, use `pymm setup --fix` to resolve common problems.

4. **Check Java**
   ```bash
   pymm server check-java
   ```

## Interactive Mode

The easiest way to use PythonMetaMap is through the interactive interface:

```bash
pymm
# or
pymm -i
# or
pymm --interactive
```

This opens a menu system where you can:
- Configure settings
- Start/stop servers
- Process files with progress monitoring
- View results and statistics
- Retry failed files

## Command Line Interface

### Processing Commands

#### Process Files
```bash
# Basic processing
pymm process input_dir output_dir

# With options
pymm process input_dir output_dir --workers 8 --timeout 600

# Run in background (continues after terminal closes)
pymm process input_dir output_dir --background

# With interactive monitoring
pymm process input_dir output_dir --interactive-monitor
```

Options:
- `--workers, -w`: Number of parallel workers (default: auto-detect)
- `--timeout, -t`: Timeout per file in seconds (default: 300)
- `--retry, -r`: Max retry attempts (default: 3)
- `--instance-pool`: Reuse MetaMap instances for speed
- `--no-start-servers`: Don't start servers automatically

#### Resume Processing
```bash
# Resume interrupted processing
pymm resume output_dir
```

#### Check Status
```bash
# Show processing status
pymm status output_dir

# Show only failed files
pymm status output_dir --failed-only
```

#### Monitor Background Jobs
```bash
# Check if background process is running
pymm monitor

# Follow log output
pymm monitor --follow

# Show statistics
pymm monitor --stats
```

#### Retry Failed Files
```bash
# Basic retry
pymm retry output_dir

# Advanced retry with options
pymm retry-failed output_dir --max-attempts 5 --delay 30

# Preview what would be retried
pymm retry-failed output_dir --dry-run

# Retry only specific error types
pymm retry-failed output_dir --filter-error "timeout"
```

### Server Management

#### Server Control
```bash
# Check server status
pymm server status

# Detailed status with ports
pymm server status --detailed

# Start servers
pymm server start

# Stop servers
pymm server stop

# Restart servers
pymm server restart all
# or restart specific server
pymm server restart tagger
pymm server restart wsd
```

#### Server Pool (Multiple Instances)
```bash
# Start server pool for heavy processing
pymm server pool --tagger 4 --wsd 4

# Check pool status
pymm server status --pool

# Stop server pool
pymm server pool --stop
```

#### Force Kill Servers
```bash
# Kill all MetaMap processes
pymm server kill

# Force kill and clean up
pymm server force-kill
```

### Configuration

#### Interactive Setup
```bash
# Configure interactively
pymm config setup

# Reset to defaults
pymm config setup --reset
```

#### View Configuration
```bash
# Show configuration
pymm config show

# Show raw JSON
pymm config show --raw
```

#### Modify Settings
```bash
# Set a value
pymm config set max_parallel_workers 16
pymm config set pymm_timeout 600
pymm config set java_heap_size 8g

# Get a value
pymm config get metamap_binary_path

# Remove a value
pymm config unset custom_setting

# Validate configuration
pymm config validate
```

### Analysis and Statistics

#### Basic Statistics
```bash
# Show concept statistics
pymm stats concepts output_dir

# Top 50 concepts with minimum count 5
pymm stats concepts output_dir --top 50 --min-count 5

# Explore output directory
pymm stats explore output_dir --detailed
```

#### Advanced Analysis
```bash
# Analyze concepts with visualization
pymm analysis concepts output_dir --visualize

# Filter by specific terms
pymm analysis concepts output_dir --filter diabetes --filter insulin

# Use clinical presets
pymm analysis concepts output_dir --preset kidney_stone --visualize

# Export to Excel
pymm analysis concepts output_dir --excel report.xlsx

# Export to JSON
pymm analysis concepts output_dir --export analysis.json
```

Available presets:
- `kidney_stone`: Kidney stone related concepts
- `kidney_symptoms`: Kidney symptoms
- `kidney_procedures`: Kidney procedures
- `diabetes`: Diabetes concepts
- `hypertension`: Blood pressure concepts
- `pain`: Pain related concepts

#### Session Analysis
```bash
# Analyze session performance
pymm analysis session output_dir

# Check filesystem sync
pymm analysis session output_dir --sync

# Find retry candidates
pymm analysis session output_dir --retry-candidates
```

## Output Format

CSV files contain these columns:
- **CUI**: UMLS Concept Unique Identifier
- **Score**: MetaMap confidence score
- **ConceptName**: Matched concept
- **PrefName**: UMLS preferred name
- **Phrase**: Original text
- **SemTypes**: Semantic types (colon-separated)
- **Sources**: Source vocabularies (pipe-separated)
- **Position**: Location as start:length

Example output:
```csv
CUI,Score,ConceptName,PrefName,Phrase,SemTypes,Sources,Position
C0022646,1000,Kidney,Kidney structure,kidney,bpoc,MSH:MTH:NCI:SNOMEDCT_US,0:6
C0006736,1000,Calculi,Calculus,stone,patf,CHV:LNC:MTH:NCI:SNOMEDCT_US,7:5
```

## Python API

```python
from pymm import Metamap

# Initialize
mm = Metamap('/path/to/metamap20')

# Process text
concepts = mm.parse(['The patient has kidney stones'])

# Access results
for mmo in concepts:
    for concept in mmo:
        print(f"CUI: {concept.cui}")
        print(f"Score: {concept.score}")
        print(f"Name: {concept.matched}")
        print(f"Preferred: {concept.preferred}")
        print(f"Position: {concept.pos_start}:{concept.pos_length}")
```

## Configuration File

Located at `~/.pymm_controller_config.json`:

```json
{
    "metamap_binary_path": "/path/to/metamap20",
    "metamap_processing_options": "-Xd1g -y",
    "max_parallel_workers": 8,
    "pymm_timeout": 300,
    "java_heap_size": "4g",
    "default_input_dir": "/data/input",
    "default_output_dir": "/data/output"
}
```

## Performance Tips

1. **Worker Count**: Use 1-2x CPU cores for optimal performance
   ```bash
   pymm config set max_parallel_workers 16
   ```

2. **Instance Pool**: Reuse MetaMap processes for small files
   ```bash
   pymm process input output --instance-pool
   ```

3. **Java Memory**: Increase for large documents
   ```bash
   pymm config set java_heap_size 8g
   ```

4. **Timeout**: Adjust for complex documents
   ```bash
   pymm config set pymm_timeout 600
   ```

5. **Server Pool**: Use multiple servers for heavy loads
   ```bash
   pymm server pool --tagger 4 --wsd 4
   ```

## Troubleshooting

### Server Issues
```bash
# Check Java installation
pymm server check-java

# Restart all servers
pymm server restart all

# Force kill if stuck
pymm server force-kill
```

### Processing Issues
```bash
# Check failed files
pymm status output_dir --failed-only

# Retry with increased timeout
pymm retry output_dir --timeout 900

# Check logs
tail -f output_dir/mimic_controller.log
```

### Configuration Issues
```bash
# Validate setup
pymm setup

# Fix common issues
pymm setup --fix

# Reset configuration
pymm config setup --reset
```

## Advanced Features

### Background Processing
```bash
# Start in background
nohup pymm process input output --background &

# Monitor progress
pymm monitor --follow
```

### Clinical Analysis
The enhanced analysis module provides:
- Note type classification
- Demographics extraction
- Noise filtering (common terms)
- OMOP concept mapping
- Temporal analysis
- Patient-level aggregation

### Visualization
Analysis commands generate:
- Concept frequency bar charts
- Semantic type pie charts
- Co-occurrence heatmaps
- Comprehensive dashboards

## Project Structure

```
PythonMetaMap/
├── src/pymm/           # Core library
│   ├── cli/            # Command line interface
│   ├── core/           # Core functionality
│   ├── processing/     # Parallel processing
│   └── server/         # Server management
├── tests/              # Test files
├── docs/               # Documentation
└── requirements.txt    # Dependencies
```

## Contributing

Submit issues and pull requests on GitHub. The codebase follows Python conventions with type hints and docstrings.

## Citation

If you use PythonMetaMap in research:

```bibtex
@software{pythonmetamap,
  title = {PythonMetaMap: A Python Interface for MetaMap},
  author = {Qassem, Layth},
  year = {2025},
  url = {https://github.com/editnori/PythonMetaMap}
}
```

## License

MIT License - see LICENSE.txt

## Acknowledgments

- Original pymm library by Srikanth Mujjiga
- MetaMap by the National Library of Medicine
- All contributors and users