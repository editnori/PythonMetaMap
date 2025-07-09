# PythonMetaMap Analysis Templates Guide

## Overview

The Ultimate TUI now includes pre-configured analysis templates for common medical text analysis workflows. These templates provide optimized filters and settings for specific clinical documentation types.

## Available Templates

### 1. Clinical Summary Analysis
**Purpose**: Analyze discharge summaries and clinical notes
- **Focus**: Diagnoses, treatments, procedures, outcomes
- **Filters**: Disease/syndrome, treatments, clinical attributes
- **Use Case**: Understanding patient journey and clinical decisions

### 2. Radiology Report Analysis  
**Purpose**: Extract findings from imaging reports
- **Focus**: Imaging findings, anatomical structures, abnormalities
- **Filters**: Body parts, findings, spatial concepts
- **Use Case**: Identifying radiological patterns and anatomical mentions

### 3. Medication Review
**Purpose**: Analyze medication-related documentation
- **Focus**: Drugs, dosages, interactions, adherence
- **Filters**: Pharmacologic substances, clinical drugs
- **Use Case**: Medication reconciliation and drug safety analysis

### 4. Symptom Analysis
**Purpose**: Extract patient-reported symptoms
- **Focus**: Signs, symptoms, temporal patterns, severity
- **Filters**: Signs/symptoms, qualitative concepts, temporal concepts
- **Use Case**: Symptom tracking and pattern recognition

### 5. Laboratory Results Analysis
**Purpose**: Process lab reports and results
- **Focus**: Lab tests, values, abnormalities, trends
- **Filters**: Laboratory procedures, quantitative concepts
- **Use Case**: Lab value extraction and trend analysis

## How to Use Templates

1. Launch the interactive TUI:
   ```bash
   pymm -i
   ```

2. Navigate to Analysis Tools (option 5)

3. Select "Template-based Analysis" (option 6)

4. Choose a template based on your document type

5. The system will automatically:
   - Apply appropriate filters
   - Focus on relevant semantic types
   - Generate a customized report
   - Highlight key findings

## Template Components

Each template includes:

- **Semantic Type Filters**: Pre-selected UMLS semantic types relevant to the analysis
- **Concept Filters**: Keywords and patterns specific to the domain
- **Report Sections**: Customized output sections focusing on key information
- **Visualization Options**: Charts and graphs tailored to the data type

## Customizing Templates

While templates provide a great starting point, you can:

1. Modify filters after selection
2. Add custom concept searches
3. Adjust semantic type focus
4. Export results in various formats

## Best Practices

1. **Choose the Right Template**: Select based on your primary document type
2. **Review Filters**: Check the applied filters match your needs
3. **Iterate**: Run analysis, review results, and refine filters as needed
4. **Combine Templates**: Use multiple templates for comprehensive analysis

## Example Workflow

```
1. Process clinical notes with PythonMetaMap
2. Launch interactive TUI (pymm -i)
3. Select Analysis Tools → Template-based Analysis
4. Choose "Clinical Summary Analysis"
5. Review automated findings
6. Export report for further analysis
```

## Tips for WSL Users

The file explorer now works seamlessly in WSL:
- Automatically opens Windows Explorer when requested
- Proper path conversion between Linux and Windows
- No more "xdg-open" errors

## Search Functionality

The enhanced search now supports:
- Multiple column name formats
- Case-insensitive searching
- Partial matches
- Cross-file searching
- Concept relationship exploration

Enjoy the powerful analysis capabilities of PythonMetaMap Ultimate TUI!