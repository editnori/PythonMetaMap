# PythonMetaMap

PythonMetaMap provides a lightweight Python interface for running [MetaMap](https://github.com/LHNCBC/MetaMap-src) and parsing its XML output. The project is maintained by **Dr. Layth Qassem** and takes inspiration from the original `pymm` library by Srikanth Mujjiga.

## Features

* **Automated MetaMap Installation** - Download and configure MetaMap 2020 automatically
* **Interactive CLI** - User-friendly command-line interface with menu-driven options  
* **Parallel Processing** - Process multiple files simultaneously with progress tracking
* **State Management** - Resume interrupted batch jobs automatically
* **Server Management** - Start/stop MetaMap servers (Tagger, WSD) from the CLI
* **Comprehensive Monitoring** - Real-time dashboard with CPU/RAM usage and progress
* **Flexible Output** - Export results as CSV with full UMLS concept information

## Prerequisites

* **Python ≥ 3.8**
* **Java 8+** (required by MetaMap)
* **4GB+ RAM** recommended for processing

## Installation

### Method 1: Install from PyPI (Recommended)

```bash
# Install the package
pip install pythonmetamap

# Download and install MetaMap automatically
pymm install

# Launch the interactive CLI
pymm
```

### Method 2: Install from Source

```bash
# Clone the repository
git clone https://github.com/editnori/PythonMetaMap.git
cd PythonMetaMap

# Install in development mode
pip install -e .

# Download and install MetaMap automatically  
pymm install

# Launch the interactive CLI
pymm
```

## Quick Start

### Interactive Mode (Recommended)

```bash
pymm
```

This opens an interactive menu where you can:
- Configure settings
- Start/stop MetaMap servers
- Process files with visual progress
- Retry failed files
- View results and statistics

### Command Line Mode

Process a directory of text files:

```bash
pymm start <input_dir> <output_dir>
```

Resume an interrupted job:

```bash
pymm resume <input_dir> <output_dir>
```

### Python API

```python
from pymm import Metamap

# Initialize with path to MetaMap binary
mm = Metamap('/path/to/metamap/bin/metamap20')

# Process text
mmos = mm.parse(['heart attack'])

# Extract concepts
for mmo in mmos:
    for concept in mmo:
        print(f"CUI: {concept.cui}")
        print(f"Name: {concept.matched}")
        print(f"Score: {concept.score}")
        print(f"Semantic Types: {concept.semtypes}")
```

## Output Format

CSV files contain the following columns:
- **CUI** - UMLS Concept Unique Identifier
- **Score** - MetaMap confidence score
- **ConceptName** - Matched concept name
- **PrefName** - UMLS preferred name
- **Phrase** - Original text phrase
- **SemTypes** - Semantic types (colon-separated)
- **Sources** - UMLS source vocabularies (pipe-separated)
- **Position** - Position in text (start:length)

## Configuration

Settings are stored in `~/.pymm_controller_config.json`:

```json
{
    "metamap_binary_path": "/path/to/metamap20",
    "metamap_processing_options": "-Xd1g -y",
    "max_parallel_workers": 4,
    "pymm_timeout": 300,
    "java_heap_size": "4g"
}
```

Edit via the interactive menu or manually with a text editor.

## Advanced Features

### Dashboard Views
- **Summary View** (s) - Overall progress and statistics
- **Worker View** (w) - Individual worker status and resource usage
- **File View** (f) - Detailed file processing status
- **Stats View** (t) - Performance metrics and throughput

### Keyboard Shortcuts
- **p** - Pause/resume auto-scrolling
- **q** - Quit dashboard
- **s/w/f/t** - Switch between views

### Retry Mechanism
Failed files are automatically tracked and can be retried with:
- Increased timeout values
- Background processing mode
- Detailed error analysis

## Development

### Running Tests

```bash
pytest tests/
```

### Building Documentation

```bash
cd docs/
make html
```

## Troubleshooting

### Common Issues

1. **Java Heap Space Error**
   - Increase `java_heap_size` in configuration
   - Reduce `max_parallel_workers`

2. **MetaMap Server Connection Issues**
   - Use the CLI to check server status
   - Restart servers from the interactive menu

3. **Processing Timeouts**
   - Increase `pymm_timeout` for complex documents
   - Use retry feature with extended timeout

## License

This project is licensed under the MIT License. See LICENSE.txt for details.

## Acknowledgments

- Original `pymm` project by Srikanth Mujjiga
- MetaMap by the National Library of Medicine
- Community contributors and testers

## Citation

If you use PythonMetaMap in your research, please cite:

```bibtex
@software{pythonmetamap,
  title = {PythonMetaMap: A Python Interface for MetaMap},
  author = {Qassem, Layth},
  year = {2025},
  url = {https://github.com/editnori/PythonMetaMap}
}
```