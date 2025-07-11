# PythonMetaMap Performance Summary

## Current Performance Results

### Single File Processing
- **File size**: 7.6KB medical note
- **Processing time**: 32-35 seconds
- **Concepts found**: 3,125

### Parallel Processing (Tested)
- **10 small files (766 bytes each)**: 
  - Total time: ~4 seconds
  - Success rate: 9/10 files
  - Performance: ~150 files/minute for small files

### Bottlenecks
1. **MetaMap itself**: Minimum 3-5 seconds per document
2. **Chunking**: Actually makes it SLOWER (150s vs 32s)
3. **Java API**: Not available without mmserver running

## Realistic Projections for 30,000 Files

| Configuration | Time Estimate | Notes |
|--------------|---------------|-------|
| Single-threaded | 11 days | 32s per file |
| 8 workers | 33 hours | Tested configuration |
| 16 workers | 17 hours | Linear scaling estimate |
| 32 workers | 8.5 hours | With diminishing returns |

## Recommendations

### For Maximum Speed:
1. **Use the ultra-fast batch processor**:
   ```bash
   python3 -m pymm.cli.ultra_fast_batch input_dir output_dir --workers 16
   ```

2. **Disable chunking** - it makes things slower
3. **Use multiple workers** - true parallel processing
4. **Increase timeout** for complex documents
5. **Pre-filter files** to skip empty/short ones

### Hardware Requirements for <12 hours:
- CPU: 16+ cores
- RAM: 32GB minimum  
- Storage: SSD for I/O performance
- MetaMap: Multiple instances (1 per worker)

### Alternative Solutions:
- **MetaMap Lite**: 10x faster but less accurate
- **Cloud computing**: AWS/GCP with 64+ cores
- **Distributed processing**: Multiple machines
- **Other tools**: cTAKES, BioBERT, etc.

## Quick Start Commands

```bash
# Clean state
rm pymm_data/output/.*.json

# Process with 8 workers
python3 fast_batch.py

# Process with custom workers
python3 -m pymm.cli.ultra_fast_batch pymm_data/input pymm_data/output --workers 16

# Background processing
nohup python3 fast_batch.py > processing.log 2>&1 &
```

## The Reality
MetaMap is inherently slow - it's doing complex medical NLP. The best optimization is parallel processing across multiple files, not trying to speed up individual file processing.