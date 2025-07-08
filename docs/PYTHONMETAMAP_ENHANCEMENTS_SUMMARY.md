# PythonMetaMap Enhancement Summary

## New Advanced Analysis Features

We've enhanced PythonMetaMap with powerful analysis capabilities similar to the kidney stone analysis demonstration. These features provide comprehensive insights into MetaMap output data.

### 1. Advanced Concept Analysis (`pymm analysis concepts`)

Analyze extracted medical concepts with filtering, visualization, and detailed reporting:

```bash
# Basic concept analysis
pymm analysis concepts output_csvs/

# Filter for specific concepts (like kidney stone analysis)
pymm analysis concepts output_csvs/ --filter kidney --filter stone --visualize

# Export detailed report
pymm analysis concepts output_csvs/ --filter diabetes --export diabetes_analysis.json --top 50
```

Features include:
- **Concept Frequency Analysis**: Track most common medical concepts
- **Semantic Type Distribution**: Understand concept categories
- **Co-occurrence Analysis**: Find concepts that appear together
- **Score Distribution**: Analyze MetaMap confidence scores
- **Multi-format Export**: JSON, Excel, and visualizations
- **Predefined Filters**: Kidney stone, diabetes, hypertension presets

### 2. Session Statistics (`pymm analysis session`)

Get detailed insights into processing sessions:

```bash
# View session statistics
pymm analysis session output_csvs/

# Show retry candidates
pymm analysis session output_csvs/ --retry-candidates

# Sync state with filesystem
pymm analysis session output_csvs/ --sync
```

Provides:
- **File Processing Metrics**: Success rates, completion status
- **Performance Analysis**: Average processing time, throughput
- **Error Categorization**: Timeout, memory, Java errors
- **Retry Candidates**: Files that can be retried
- **Output Statistics**: Total size, concept counts

### 3. Dedicated Retry Command (`pymm retry-failed`)

Intelligently retry failed files from previous sessions:

```bash
# Retry all failed files
pymm retry-failed output_csvs/

# Only retry timeout errors
pymm retry-failed output_csvs/ --filter-error timeout

# Dry run to see what would be retried
pymm retry-failed output_csvs/ --dry-run

# Custom retry settings
pymm retry-failed output_csvs/ --max-attempts 5 --delay 10
```

Features:
- **Smart Retry Logic**: Only retries files that haven't exceeded max attempts
- **Error Filtering**: Retry specific error types
- **Progress Tracking**: Visual progress with time estimates
- **State Preservation**: Updates session state after each retry
- **Configurable Delays**: Prevent server overload

### 4. Enhanced Interactive Mode

The interactive mode now includes powerful analysis features:

```bash
pymm interactive
```

Navigate to **View Results & Manage Outputs** for:

1. **Analyze Concepts**: 
   - Choose from presets (kidney stone, diabetes, etc.)
   - Custom filters
   - Automatic visualizations
   - Excel export

2. **Session Statistics**:
   - Performance metrics
   - Failed file analysis
   - Direct retry option

3. **Export Analysis Report**:
   - Comprehensive reports
   - Kidney stone analysis
   - Custom filtered reports

### 5. Visualization Capabilities

All analysis commands generate professional visualizations:

- **Top Concepts Bar Chart**: Most frequent medical concepts
- **Semantic Type Pie Chart**: Distribution of concept categories
- **Co-occurrence Heatmap**: Concept relationships
- **Analysis Dashboard**: 9-panel comprehensive overview including:
  - Summary statistics
  - Top 10 concepts
  - Semantic types
  - Score distribution
  - Files per concept
  - Co-occurrence patterns

### 6. Excel Export

Comprehensive Excel workbooks with multiple sheets:

- **Summary**: Overall statistics
- **All Concepts**: Complete concept list with scores and frequencies
- **Semantic Types**: Type distribution
- **Co-occurrences**: Concept relationships
- **Failed Files**: Processing errors

## Usage Examples

### Complete Kidney Stone Analysis Workflow

```bash
# 1. Process clinical notes
pymm process input_notes/ output_csvs/ --workers 8

# 2. Check session status
pymm analysis session output_csvs/

# 3. Retry any failed files
pymm retry-failed output_csvs/

# 4. Run kidney stone analysis
pymm analysis concepts output_csvs/ --preset kidney_stone --visualize --excel kidney_analysis.xlsx

# 5. Or use interactive mode for guided analysis
pymm interactive
# Select: View Results & Manage Outputs → Analyze concepts → kidney_stone preset
```

### Custom Analysis for Research

```bash
# Analyze specific conditions
pymm analysis concepts output_csvs/ -f "heart failure" -f "ejection fraction" --visualize

# Export comprehensive report
pymm analysis concepts output_csvs/ --export research_data.json --excel research_analysis.xlsx
```

## Benefits

1. **Research-Ready Output**: Generate publication-quality visualizations and reports
2. **Efficient Retry Mechanism**: Don't reprocess successful files
3. **Comprehensive Insights**: Understand your data at multiple levels
4. **Flexible Filtering**: Focus on specific medical concepts
5. **Performance Monitoring**: Optimize processing workflows

These enhancements transform PythonMetaMap from a simple wrapper into a comprehensive medical text analysis platform, perfect for research projects like the kidney stone analysis demonstration.

## Technical Implementation

- **Modular Design**: New `