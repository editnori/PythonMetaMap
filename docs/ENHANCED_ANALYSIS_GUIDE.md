# PythonMetaMap Enhanced Analysis Guide

## Overview

The Enhanced Analysis module addresses clinical research feedback by providing advanced features for analyzing MetaMap output with a focus on clinical insights, patient deduplication, note type stratification, and comprehensive visualization capabilities.

## Key Features

### Noise Filtering
- Automatically filters out common generic terms (patient, doctor, hospital, etc.)
- Removes administrative and temporal concepts that add no clinical value
- Focuses analysis on clinically meaningful concepts
- Customizable noise term lists

### 1. Note Type Classification & Stratification
- Automatically classifies notes into categories:
  - Discharge summaries
  - Operative notes
  - Emergency department notes
  - Outpatient notes
  - Radiology reports
  - Pathology reports
- Enables comparative analysis across note types
- Identifies patterns specific to clinical settings

### 2. Patient-Level Deduplication
- Tracks unique patients across multiple notes
- Calculates average notes per patient
- Prevents double-counting in patient cohort analyses
- Maintains patient-note mappings for longitudinal studies

### 3. Demographic Extraction
- Extracts age from various formats (e.g., "45-year-old", "age: 67")
- Identifies sex/gender from text
- Provides demographic summaries and distributions
- Enables cohort stratification by demographics

### 4. Procedure Classification System
- Categorizes procedures into:
  - **Removal**: ureteroscopy, PCNL, lithotripsy, extraction
  - **Drainage**: stent placement, nephrostomy tubes
  - **Diagnostic**: cystoscopy, imaging studies
  - **Other**: unclassified procedures
- Tracks procedure distribution by note type

### 5. Validation Framework
- Random sampling for manual review
- Exports validation sets with:
  - Patient IDs
  - Note types
  - Top concepts for review
  - Empty columns for manual validation
- Enables quality assessment of NLP extraction

### 6. Stone-Specific Phenotype Extraction
- Extracts clinical characteristics:
  - Stone size (mm)
  - Location (kidney, ureter, bladder)
  - Composition (calcium oxalate, uric acid, etc.)
  - Laterality (left, right, bilateral)
  - Multiplicity (single, multiple)
  - Hounsfield units from CT scans

### 7. Advanced Visualizations

#### Interactive Chord Diagrams
- Visualize semantic type co-occurrences
- Interactive D3.js visualization showing relationships between medical concept types
- Thicker chords indicate stronger co-occurrence relationships
- Click on arcs to highlight specific semantic type connections
- Hover for detailed statistics

#### Comparative Visualizations
- Note type distribution charts
- Demographic breakdowns
- Procedure classification distributions
- Concept frequency by note type
- Stone phenotype distributions

### 8. Comprehensive Export Options
- Excel workbooks with multiple sheets:
  - Summary statistics
  - Note type distributions
  - Demographics
  - Procedure classifications
  - Concepts by note type
  - Stone phenotypes
- JSON reports for programmatic access
- Validation sets for quality control

## Usage Examples

### Interactive Mode (TUI)
The enhanced analysis is fully integrated into the interactive Text User Interface:

```bash
# Launch interactive mode
pymm -i

# Then navigate to:
# 4. View Results → 2. Analyze concepts → 2. Enhanced Clinical Analysis
```

### Command Line Interface

#### Basic Enhanced Analysis
```bash
pymm enhanced-analysis analyze output_csvs/
```

### Kidney Stone Analysis with Visualizations
```bash
pymm enhanced-analysis analyze output_csvs/ \
  --preset kidney_stone_comprehensive \
  --visualize \
  --chord \
  --excel kidney_stone_report.xlsx \
  --html comprehensive_methodology_report.html
```

### Generate Comprehensive Methodology Report
```bash
# This generates a detailed HTML report explaining the complete extraction methodology
pymm enhanced-analysis analyze output_csvs/ \
  --preset kidney_stone_comprehensive \
  --html kidney_stone_methodology_report.html
```

The HTML report includes:
- Executive summary of findings
- Detailed comparison of 1K vs 20K extraction methodologies
- OMOP concept-based extraction SQL queries
- MetaMap configuration and parameter explanations
- Noise filtering rationale and implementation
- Clinical research implications
- Technical implementation details
- Best practices and recommendations

### Analysis with Validation Export
```bash
pymm enhanced-analysis analyze output_csvs/ \
  --validation validation_set.xlsx \
  --sample-size 200
```

### Compare Multiple Datasets
```bash
pymm enhanced-analysis compare \
  dataset1/ dataset2/ dataset3/ \
  -p kidney_stone_comprehensive \
  -p stone_procedures_removal \
  -o comparison_radar.png
```

## Filter Presets

### kidney_stone_comprehensive
- Comprehensive kidney stone terminology
- Includes all stone-related concepts
- CUIs for nephrolithiasis, renal calculi, etc.

### stone_procedures_removal
- Stone removal procedures
- Ureteroscopy, PCNL, lithotripsy
- Extraction techniques

### stone_procedures_drainage
- Drainage procedures
- Stent placements
- Nephrostomy tubes

### stone_characteristics
- Stone composition terms
- Size measurements
- Imaging characteristics

### stone_outcomes
- Treatment outcomes
- Stone-free rates
- Complications
- Readmissions

## Clinical Research Applications

### 1. Cohort Characterization
- Identify patient populations
- Understand demographic distributions
- Track comorbidities across note types

### 2. Treatment Pattern Analysis
- Compare removal vs drainage procedures
- Identify practice variations
- Track temporal treatment sequences

### 3. Outcome Measurement
- Extract stone-free rates from notes
- Identify complications
- Track readmission patterns

### 4. Quality Improvement
- Validate ICD coding accuracy
- Identify documentation gaps
- Compare structured vs unstructured data

### 5. Comparative Effectiveness
- Compare treatment approaches
- Analyze outcomes by procedure type
- Stratify by patient characteristics

## Output Interpretation

### Enhanced Summary Metrics
- **Unique Patients**: Actual patient count (deduplicated)
- **Avg Notes/Patient**: Indicator of care complexity
- **Note Type Distribution**: Care setting patterns
- **Procedure Classifications**: Treatment approach distribution

### Validation Metrics
- Use exported validation sets for:
  - Precision/recall calculation
  - Inter-rater reliability
  - Concept accuracy assessment

### Phenotype Analysis
- Stone characteristics indicate disease severity
- Procedure patterns reveal treatment preferences
- Demographics show population characteristics

## Integration with Existing Workflow

1. **Run standard PythonMetaMap processing**
   ```bash
   pymm process input_notes/ output_csvs/
   ```

2. **Perform enhanced analysis**
   ```bash
   pymm enhanced-analysis analyze output_csvs/ -v -r -x report.xlsx
   ```

3. **Review validation set**
   - Open validation.xlsx
   - Manually review sampled concepts
   - Calculate accuracy metrics

4. **Compare cohorts**
   ```bash
   pymm enhanced-analysis compare cohort1/ cohort2/ -o comparison.png
   ```

## Future Enhancements (Planned)

### CPT Code Integration
- Map procedures to CPT codes
- Validate billing accuracy
- Compare documentation vs billing

### Temporal Analysis
- Extract procedure dates
- Calculate time to treatment
- Track disease progression

### Outcome Extraction
- Stone-free rate identification
- Complication detection
- Readmission prediction

## Troubleshooting

### Memory Issues with Large Datasets
- Process in batches
- Reduce sample size for validation
- Use filtering to focus analysis

### Visualization Errors
- Ensure matplotlib backend is configured
- Check for sufficient data in categories
- Verify output directory permissions

### Patient ID Extraction
- Customize patterns in code if needed
- Ensure consistent file naming
- Check for PHI compliance

## Best Practices

1. **Start with Presets**: Use filter presets for focused analysis
2. **Validate Early**: Export validation sets in initial runs
3. **Compare Note Types**: Different settings have different documentation
4. **Use Visualizations**: Radar charts reveal patterns quickly
5. **Export Everything**: Keep Excel reports for detailed review

## Support

For issues or feature requests related to enhanced analysis:
- Check existing analysis features with `pymm analysis --help`
- Review this guide for advanced features
- Submit issues with example data when possible