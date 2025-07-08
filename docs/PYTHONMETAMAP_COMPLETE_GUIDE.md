# PythonMetaMap v8.0.8 Complete Guide

## Overview

PythonMetaMap is a Python wrapper and orchestration system for the National Library of Medicine's MetaMap tool. It provides parallel processing capabilities, server management, and a comprehensive CLI for extracting medical concepts from text.

## System Requirements

- Python 3.8 or higher
- Java 8 or higher (for MetaMap)
- 16GB RAM minimum (32GB recommended for parallel processing)
- 20GB free disk space for MetaMap installation
- Windows, Linux, or macOS

## Installation Process

### Step 1: Clone Repository

```bash
git clone https://github.com/your-repo/PythonMetaMap.git
cd PythonMetaMap
```

### Step 2: Create Virtual Environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/macOS
source .venv/bin/activate
```

### Step 3: Install PythonMetaMap

```bash
pip install -e .
```

### Step 4: Verify Installation

```bash
pymm --version
# Should output: PythonMetaMap version 8.0.8
```

### Step 5: Install MetaMap

```bash
# Interactive installation
pymm install

# Or direct download
pymm install --accept-license --version 2020
```

The installation process will:
1. Download MetaMap 2020 (approximately 1.5GB)
2. Extract files to metamap_install directory
3. Configure paths automatically
4. Verify installation integrity

### Step 6: Configure PythonMetaMap

```bash
# Run configuration wizard
pymm config setup

# Or set manually
pymm config set metamap_binary_path /path/to/metamap20
pymm config set max_parallel_workers 8
pymm config set pymm_timeout 600
```

Configuration file location: `~/.pymm_controller_config.json`

## Core Features

### 1. Parallel Processing

PythonMetaMap processes multiple files concurrently using a worker pool system. Each worker manages its own MetaMap instance, enabling linear scaling with CPU cores.

```bash
# Process with 8 parallel workers
pymm process input_folder/ output_folder/ --workers 8
```

### 2. Server Management

The system manages MetaMap's required servers (WSD and Tagger) automatically:

```bash
# Start servers
pymm server start

# Check status
pymm server status

# Stop servers
pymm server stop
```

### 3. Instance Pooling

Pre-warmed MetaMap instances reduce startup overhead:

```bash
# Enable instance pooling
pymm config set use_instance_pool true
pymm config set max_instances 8
```

### 4. State Persistence

Processing state is saved automatically, allowing resumption of interrupted jobs:

```bash
# Resume processing
pymm resume output_folder/

# Check processing status
pymm status output_folder/
```

### 5. Background Processing

Long-running jobs can be executed in the background:

```bash
# Process in background with nohup
pymm process input/ output/ --background

# Monitor progress
pymm monitor output/
```

### 6. Interactive Mode

User-friendly terminal interface for all operations:

```bash
pymm -i
```

## Processing Workflow

### Input Preparation

Create a directory with text files to process:

```bash
mkdir input_notes
echo "Patient diagnosed with diabetes mellitus type 2" > input_notes/patient1.txt
echo "History of hypertension and cardiac arrhythmia" > input_notes/patient2.txt
```

### Basic Processing

```bash
pymm process input_notes/ output_csvs/
```

### Advanced Processing

```bash
pymm process input_notes/ output_csvs/ \
  --workers 8 \
  --timeout 600 \
  --retry-failed \
  --options "-Z 2020AA --lexicon db --word_sense_disambiguation"
```

### Output Format

Each input file generates a CSV with extracted concepts:

```csv
CUI,Score,ConceptName,PrefName,Phrase,SemTypes,Sources,Positions
C0011849,-1000,Diabetes Mellitus,Diabetes Mellitus,diabetes mellitus,dsyn:T047,MSH|NCI|SNOMEDCT_US,0:17
C0441730,-861,Type 2,Type 2,type 2,qnco:T081,CHV|LNC|MTH|NCI|SNOMEDCT_US,18:6
```

## Performance Optimization

### Memory Configuration

```bash
# Increase Java heap size
pymm config set java_heap_size 8g

# Set MetaMap memory options
pymm config set metamap_processing_options "-Xmx8g -Z 2020AA"
```

### Concurrency Settings

```bash
# Optimal for 16-core system
pymm config set max_parallel_workers 12
pymm config set max_instances 12
pymm config set pymm_timeout 600
```

### Batch Size Optimization

For large datasets, process in batches:

```bash
# Process 1000 files at a time
pymm process input/ output/ --batch-size 1000
```

## Troubleshooting

### Server Connection Issues

```bash
# Check server status
pymm server status

# Restart servers
pymm server restart

# Check ports
netstat -an | grep -E "1795|5554"
```

### Processing Failures

```bash
# View failed files
pymm status output/ --show-failed

# Retry failed files
pymm retry output/

# Process single file for debugging
pymm process-single input/problem_file.txt output/
```

### Memory Issues

```bash
# Reduce parallel workers
pymm config set max_parallel_workers 4

# Increase timeout
pymm config set pymm_timeout 1200

# Enable garbage collection
pymm config set enable_gc true
```

## API Usage

### Python API

```python
from pymm import Metamap, PyMMConfig, BatchRunner

# Single file processing
mm = Metamap("/path/to/metamap")
concepts = mm.extract_concepts("Patient has diabetes")

# Batch processing
config = PyMMConfig()
config.set("max_parallel_workers", 8)

runner = BatchRunner("input/", "output/", config)
results = runner.run()

# Access results
print(f"Processed: {results['processed']}")
print(f"Failed: {results['failed']}")
print(f"Throughput: {results['throughput']} files/min")
```

### Parsing Output

```python
from pymm import parse

# Parse MetaMap XML output
mmos = parse("output/file.xml")

for mmo in mmos:
    for concept in mmo:
        print(f"CUI: {concept.cui}")
        print(f"Name: {concept.pref_name}")
        print(f"Score: {concept.score}")
        print(f"Position: {concept.pos_start}:{concept.pos_length}")
```

## Complete Test Suite

### Installation Test

```bash
# Test 1: Verify Python installation
python --version

# Test 2: Verify pip installation
pip --version

# Test 3: Install PythonMetaMap
pip install -e .

# Test 4: Verify pymm command
pymm --version

# Test 5: Check configuration
pymm config show
```

### MetaMap Installation Test

```bash
# Test 6: Install MetaMap
pymm install --accept-license

# Test 7: Verify MetaMap binary
pymm test metamap

# Test 8: Check MetaMap version
pymm metamap-version
```

### Server Management Test

```bash
# Test 9: Start servers
pymm server start

# Test 10: Check server status
pymm server status

# Test 11: Test server connectivity
pymm test servers

# Test 12: Stop servers
pymm server stop
```

### Processing Test

```bash
# Test 13: Create test input
mkdir test_input
echo "diabetes mellitus type 2" > test_input/test1.txt
echo "hypertension" > test_input/test2.txt

# Test 14: Single worker processing
pymm process test_input/ test_output/ --workers 1

# Test 15: Verify output
ls test_output/*.csv

# Test 16: Check CSV content
cat test_output/test1.txt.csv
```

### Parallel Processing Test

```bash
# Test 17: Create multiple test files
for i in {1..20}; do
    echo "Patient $i has condition $i" > test_input/patient$i.txt
done

# Test 18: Parallel processing
pymm process test_input/ test_output_parallel/ --workers 8

# Test 19: Verify all files processed
ls test_output_parallel/*.csv | wc -l
# Should output: 20
```

### State Persistence Test

```bash
# Test 20: Start processing and interrupt
pymm process test_input/ test_output_state/ --workers 4
# Press Ctrl+C after a few files

# Test 21: Check state
pymm status test_output_state/

# Test 22: Resume processing
pymm resume test_output_state/

# Test 23: Verify completion
pymm status test_output_state/ --show-completed
```

### Background Processing Test

```bash
# Test 24: Process in background
pymm process test_input/ test_output_bg/ --background

# Test 25: Monitor progress
pymm monitor test_output_bg/

# Test 26: Check logs
cat test_output_bg/logs/batch_run_*.log
```

### Error Handling Test

```bash
# Test 27: Create problematic file
echo "" > test_input/empty.txt
echo "x" * 1000000 > test_input/large.txt

# Test 28: Process with retry
pymm process test_input/ test_output_retry/ --retry-failed

# Test 29: Check failed files
pymm status test_output_retry/ --show-failed

# Test 30: Force retry
pymm retry test_output_retry/ --force
```

### Performance Test

```bash
# Test 31: Create large dataset
mkdir perf_test
for i in {1..1000}; do
    echo "Medical record $i with various conditions" > perf_test/record$i.txt
done

# Test 32: Time single worker
time pymm process perf_test/ perf_output_single/ --workers 1

# Test 33: Time parallel processing
time pymm process perf_test/ perf_output_parallel/ --workers 8

# Test 34: Compare throughput
pymm compare perf_output_single/ perf_output_parallel/
```

### Configuration Test

```bash
# Test 35: Set all configuration options
pymm config set metamap_binary_path /path/to/metamap
pymm config set max_parallel_workers 16
pymm config set pymm_timeout 1200
pymm config set use_instance_pool true
pymm config set max_instances 16
pymm config set java_heap_size 16g
pymm config set retry_max_attempts 5
pymm config set progress_bar true
pymm config set server_persistence_hours 48

# Test 36: Verify configuration
pymm config show

# Test 37: Export configuration
pymm config export config_backup.json

# Test 38: Reset and import
pymm config reset
pymm config import config_backup.json
```

### Interactive Mode Test

```bash
# Test 39: Launch interactive mode
pymm -i

# Test through menu:
# - Process Files
# - Server Management
# - Configuration
# - View Statistics
# - Exit
```

### Cleanup Test

```bash
# Test 40: Clean up test files
rm -rf test_input test_output* perf_test perf_output*
pymm cleanup --all
```

## Maintenance

### Log Management

```bash
# View logs
pymm logs show

# Clean old logs
pymm logs clean --days 7

# Archive logs
pymm logs archive
```

### Cache Management

```bash
# Clear instance pool cache
pymm cache clear

# Show cache statistics
pymm cache stats
```

### Update Check

```bash
# Check for updates
pymm update check

# Update to latest version
pymm update install
```

## Support

For issues or questions:
- GitHub Issues: https://github.com/your-repo/PythonMetaMap/issues
- Documentation: https://pythonmetamap.readthedocs.io
- Email: support@pythonmetamap.org

## License

MIT License - see LICENSE.txt for details. 