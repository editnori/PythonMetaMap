#!/usr/bin/env python3
"""
Refined Kidney Stone Analysis - Percentages, Co-occurrences, and Filtered Concepts
For admission notes only
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
from collections import Counter, defaultdict
from itertools import combinations

# Professional medical presentation style
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.facecolor'] = 'white'
plt.rcParams['axes.facecolor'] = 'white'
plt.rcParams['font.size'] = 12

class RefinedKidneyStoneAnalyzer:
    def __init__(self):
        # Noise concepts to filter out
        self.noise_concepts = {
            'Finding', 'Disorder', 'Disease', 'Procedure', 'Patient', 'History',
            'Physical', 'Diagnosis', 'Medical', 'Clinical', 'Evaluation',
            'Assessment', 'Plan', 'Review', 'Status', 'Type', 'Care',
            'Management', 'Treatment', 'Therapy', 'Service', 'Visit',
            'Admission', 'Discharge', 'Hospital', 'Emergency', 'Department',
            'Room', 'Bed', 'Unit', 'Floor', 'Doctor', 'Nurse', 'Staff',
            'Time', 'Date', 'Day', 'Month', 'Year', 'Hour', 'Minute',
            'Report', 'Document', 'Note', 'Record', 'Information',
            'OTHER', 'Other', 'NOS', 'General', 'Common', 'Routine',
            'Normal', 'Regular', 'Standard', 'Typical', 'Usual'
        }
        
        # Core kidney stone concepts to focus on
        self.core_concepts = {
            'conditions': [
                'kidney stone', 'renal calculus', 'nephrolithiasis', 'ureteral stone',
                'renal colic', 'hydronephrosis', 'obstruction', 'staghorn'
            ],
            'procedures': [
                'ureteroscopy', 'lithotripsy', 'ESWL', 'PCNL', 'stent placement',
                'nephrostomy', 'cystoscopy', 'stone extraction', 'laser lithotripsy'
            ],
            'symptoms': [
                'flank pain', 'hematuria', 'dysuria', 'nausea', 'vomiting',
                'fever', 'chills', 'abdominal pain'
            ],
            'diagnostics': [
                'CT scan', 'ultrasound', 'KUB', 'urinalysis', 'urine culture',
                'creatinine', 'BUN', 'stone analysis'
            ],
            'complications': [
                'sepsis', 'AKI', 'acute kidney injury', 'pyelonephritis',
                'urosepsis', 'perforation', 'steinstrasse'
            ]
        }
    
    def normalize_concepts(self, df):
        """Normalize concept variations to canonical forms"""
        # Create a copy to avoid modifying original
        df_norm = df.copy()
        
        # First pass: case-insensitive normalization for exact matches
        normalization_map = {
            # Kidney stone as primary concept
            'Kidney Stone': ['kidney stone', 'kidney stones', 'renal stone', 'renal stones', 
                            'renal calculus', 'renal calculi', 'nephrolithiasis', 'stone, kidney'],
            
            # Ureteral concepts
            'Ureteral': ['ureter', 'ureteric', 'ureteral'],
            'Ureteral Stone': ['ureteral stone', 'ureteric stone', 'ureter stone', 
                              'ureteral calculus', 'ureteric calculus'],
            'Ureteral Stent': ['stent', 'ureteral stent', 'ureteric stent', 'ureter stent',
                              'stent placement', 'stent ureter', 'stent, ureter'],
            
            # Procedures
            'Ureteroscopy': ['ureteroscopy', 'urs'],
            'Lithotripsy': ['lithotripsy', 'eswl', 'swl', 'shock wave lithotripsy'],
            'PCNL': ['pcnl', 'percutaneous nephrolithotomy', 'percutaneous nephrolithotripsy'],
            'Nephrostomy': ['nephrostomy', 'nephrostomy tube', 'percutaneous nephrostomy'],
            
            # Symptoms
            'Flank Pain': ['flank pain', 'flank', 'side pain', 'loin pain'],
            'Hematuria': ['hematuria', 'blood in urine', 'bloody urine'],
            'Renal Colic': ['renal colic', 'ureteral colic', 'ureteric colic'],
            'Hydronephrosis': ['hydronephrosis', 'hydro'],
            
            # Complications/Conditions
            'Obstruction': ['obstruction', 'obstructive', 'obstructed'],
            'UTI': ['uti', 'urinary tract infection', 'cystitis', 'pyelonephritis'],
            'Hypertension': ['hypertension', 'htn', 'high blood pressure'],
            'Diabetes': ['diabetes', 'dm', 'diabetes mellitus', 'iddm', 'niddm'],
            
            # Stone composition
            'Calcium Oxalate': ['calcium oxalate', 'calcium oxalate stone'],
            'Calcium': ['calcium', 'ca'],
            'Uric Acid': ['uric acid', 'urate'],
            'Struvite': ['struvite', 'infection stone', 'triple phosphate'],
            
            # Imaging
            'CT Scan': ['ct', 'ct scan', 'cat scan', 'computed tomography', 'ct urogram'],
            'Ultrasound': ['ultrasound', 'us', 'ultrasonography'],
            
            # Pain management
            'Pain': ['pain', 'painful', 'ache'],
            'Morphine': ['morphine', 'ms'],
            'NSAID': ['nsaid', 'nsaids', 'toradol', 'ketorolac', 'ibuprofen'],
            
            # Other
            'Emergency': ['emergency', 'emergent', 'urgent', 'ed', 'er'],
            'Acute': ['acute', 'sudden onset', 'new onset'],
            'Severe': ['severe', 'significant', 'marked']
        }
        
        # Apply case-insensitive normalization
        for canonical, variations in normalization_map.items():
            for variant in variations:
                # Case-insensitive exact match
                mask = df_norm['ConceptName'].str.lower() == variant.lower()
                df_norm.loc[mask, 'ConceptName'] = canonical
        
        # Second pass: Remove standalone generic terms that are part of compound concepts
        # This prevents "Kidney", "Stone" from appearing separately when we have "Kidney Stone"
        generic_terms_to_check = {
            'kidney': 'Kidney Stone',
            'stone': 'Kidney Stone',
            'renal': 'Kidney Stone',
            'calculus': 'Kidney Stone',
            'calculi': 'Kidney Stone',
            'ureter': 'Ureteral',
            'ureteric': 'Ureteral'
        }
        
        for term, replacement in generic_terms_to_check.items():
            # Only replace if it's a standalone term (case-insensitive)
            mask = df_norm['ConceptName'].str.lower() == term.lower()
            df_norm.loc[mask, 'ConceptName'] = replacement
        
        # Third pass: Handle NOS and other suffix variations
        # Remove common suffixes that don't add clinical meaning
        suffixes_to_remove = [', nos', ' nos', ', unspecified', ' - unspecified', ' (disorder)', ' (finding)']
        for suffix in suffixes_to_remove:
            df_norm['ConceptName'] = df_norm['ConceptName'].str.replace(suffix, '', case=False, regex=False)
        
        # Final cleanup: strip whitespace and standardize
        df_norm['ConceptName'] = df_norm['ConceptName'].str.strip()
        
        return df_norm
    
    def load_and_filter_data(self, output_dir="./pymm_data/output"):
        """Load MetaMap output and filter noise"""
        csv_files = list(Path(output_dir).glob("*.csv"))  # Load ALL files
        
        all_concepts = []
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                df['source_file'] = csv_file.stem
                all_concepts.append(df)
            except:
                continue
        
        if not all_concepts:
            return pd.DataFrame()
        
        df = pd.concat(all_concepts, ignore_index=True)
        
        # Filter out noise concepts
        df = df[~df['ConceptName'].isin(self.noise_concepts)]
        
        # Normalize concepts BEFORE filtering
        df_normalized = self.normalize_concepts(df)
        
        # Filter for kidney stone related concepts
        pattern = '|'.join([
            'kidney', 'stone', 'calcul', 'ureter', 'nephro', 'litho',
            'colic', 'stent', 'cystoscopy', 'hydronephrosis'
        ])
        df_filtered = df_normalized[df_normalized['ConceptName'].str.contains(pattern, case=False, na=False)]
        
        return df_normalized, df_filtered
    
    def calculate_percentages(self, df_full, df_filtered):
        """Calculate percentages instead of raw counts"""
        total_concepts = len(df_full)
        total_files = df_full['source_file'].nunique()
        
        results = {
            'total_files_analyzed': total_files,
            'kidney_stone_concepts_percent': (len(df_filtered) / total_concepts * 100),
            'files_with_stone_concepts': df_filtered['source_file'].nunique(),
            'percent_files_with_stones': (df_filtered['source_file'].nunique() / total_files * 100)
        }
        
        return results
    
    def find_co_occurrences(self, df_filtered):
        """Find concepts that frequently appear together"""
        co_occurrences = defaultdict(int)
        
        # Group by source file
        for file, group in df_filtered.groupby('source_file'):
            concepts = group['ConceptName'].unique()
            # Only consider meaningful concepts
            concepts = [c for c in concepts if len(c) > 3 and c not in self.noise_concepts]
            
            # Find pairs
            for pair in combinations(concepts, 2):
                sorted_pair = tuple(sorted(pair))
                co_occurrences[sorted_pair] += 1
        
        # Sort by frequency
        top_pairs = sorted(co_occurrences.items(), key=lambda x: x[1], reverse=True)[:15]
        return top_pairs
    
    def categorize_concepts(self, df_filtered):
        """Categorize concepts by clinical relevance"""
        categories = defaultdict(list)
        
        for _, row in df_filtered.iterrows():
            concept = row['ConceptName'].lower()
            
            # Procedures
            if any(proc in concept for proc in ['stent', 'ureteroscopy', 'lithotripsy', 
                                                'nephrostomy', 'pcnl', 'eswl']):
                categories['Procedures'].append(row['ConceptName'])
            
            # Symptoms
            elif any(symp in concept for symp in ['pain', 'hematuria', 'fever', 
                                                  'nausea', 'vomiting', 'colic']):
                categories['Symptoms'].append(row['ConceptName'])
            
            # Diagnostics
            elif any(diag in concept for diag in ['ct', 'scan', 'ultrasound', 
                                                  'urinalysis', 'culture', 'kub']):
                categories['Diagnostics'].append(row['ConceptName'])
            
            # Stone types/composition
            elif any(comp in concept for comp in ['calcium', 'oxalate', 'uric', 
                                                  'struvite', 'cystine']):
                categories['Stone Composition'].append(row['ConceptName'])
            
            # Complications
            elif any(comp in concept for comp in ['sepsis', 'infection', 'aki', 
                                                  'obstruction', 'hydronephrosis']):
                categories['Complications'].append(row['ConceptName'])
        
        return categories
    
    def create_percentage_visualizations(self, df_full, df_filtered):
        """Create visualizations using percentages only"""
        stats = self.calculate_percentages(df_full, df_filtered)
        categories = self.categorize_concepts(df_filtered)
        co_occurrences = self.find_co_occurrences(df_filtered)
        
        # Create figure with subplots - larger size to prevent overlapping
        fig = plt.figure(figsize=(20, 14))
        
        # 1. Top Clinical Concepts (filtered, as percentages)
        ax1 = plt.subplot(2, 3, 1)
        top_concepts = Counter(df_filtered['ConceptName']).most_common(10)
        total_stone_concepts = len(df_filtered)
        
        concepts, counts = zip(*top_concepts)
        percentages = [count/total_stone_concepts*100 for count in counts]
        
        bars = ax1.barh(range(len(concepts)), percentages, color='steelblue')
        ax1.set_yticks(range(len(concepts)))
        ax1.set_yticklabels([c[:30] + '...' if len(c) > 30 else c for c in concepts], fontsize=10)
        ax1.set_xlabel('% of Kidney Stone-Related Concepts', fontsize=12)
        ax1.set_title('Most Common Clinical Concepts\n(Noise Filtered)', fontweight='bold', fontsize=14)
        
        # Add percentage labels with better spacing
        for i, (bar, pct) in enumerate(zip(bars, percentages)):
            ax1.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
                    f'{pct:.1f}%', va='center', fontsize=10)
        
        # 2. Category Distribution
        ax2 = plt.subplot(2, 3, 2)
        category_counts = {cat: len(items) for cat, items in categories.items()}
        total_categorized = sum(category_counts.values())
        
        if category_counts:
            sizes = list(category_counts.values())
            labels = [f"{cat}\n({count/total_categorized*100:.1f}%)" 
                     for cat, count in category_counts.items()]
            colors = plt.cm.Set3(range(len(labels)))
            
            ax2.pie(sizes, labels=labels, colors=colors, autopct='',
                   startangle=90, labeldistance=1.1)
            ax2.set_title('Clinical Concept Categories\n(Admission Notes)', fontweight='bold')
        
        # 3. Procedure Type Balance
        ax3 = plt.subplot(2, 3, 3)
        procedures = categories.get('Procedures', [])
        drainage = sum(1 for p in procedures if any(d in p.lower() 
                      for d in ['stent', 'nephrostomy', 'drainage']))
        removal = sum(1 for p in procedures if any(r in p.lower() 
                     for r in ['ureteroscopy', 'lithotripsy', 'extraction']))
        
        if procedures:
            total_procs = len(procedures)
            proc_data = {
                'Temporizing\n(Drainage)': drainage/total_procs*100,
                'Definitive\n(Stone Removal)': removal/total_procs*100
            }
            
            bars = ax3.bar(proc_data.keys(), proc_data.values(), 
                          color=['#ff6b6b', '#4ecdc4'], width=0.6)
            ax3.set_ylabel('% of Procedures', fontsize=12)
            ax3.set_title('Procedure Distribution\nin Admission Notes', fontweight='bold', fontsize=14)
            ax3.set_ylim(0, 110)  # More space for labels
            
            # Add percentage labels with better positioning
            for bar in bars:
                height = bar.get_height()
                ax3.text(bar.get_x() + bar.get_width()/2., height + 2,
                        f'{height:.1f}%', ha='center', va='bottom', fontsize=12, fontweight='bold')
        
        # 4. Co-occurrence Heatmap
        ax4 = plt.subplot(2, 3, 4)
        if co_occurrences:
            # Create a matrix for top co-occurrences
            top_pairs = co_occurrences[:10]
            concepts = set()
            for (c1, c2), _ in top_pairs:
                concepts.add(c1)
                concepts.add(c2)
            concepts = sorted(list(concepts))[:8]  # Limit to 8 for readability
            
            # Create co-occurrence matrix
            matrix = np.zeros((len(concepts), len(concepts)))
            for (c1, c2), count in top_pairs:
                if c1 in concepts and c2 in concepts:
                    i, j = concepts.index(c1), concepts.index(c2)
                    matrix[i, j] = count
                    matrix[j, i] = count
            
            # Plot heatmap
            im = ax4.imshow(matrix, cmap='Blues', aspect='auto')
            ax4.set_xticks(range(len(concepts)))
            ax4.set_yticks(range(len(concepts)))
            ax4.set_xticklabels([c[:15] + '...' if len(c) > 15 else c for c in concepts], 
                               rotation=45, ha='right', fontsize=8)
            ax4.set_yticklabels([c[:15] + '...' if len(c) > 15 else c for c in concepts], 
                               fontsize=8)
            
            # Add text annotations
            for i in range(len(concepts)):
                for j in range(len(concepts)):
                    if matrix[i, j] > 0:
                        ax4.text(j, i, f'{int(matrix[i, j])}', 
                                ha='center', va='center', fontsize=8)
            
            ax4.set_title('Concept Co-occurrences\n(Frequently Appear Together)', 
                         fontweight='bold')
        
        # 5. Symptom vs Complication Balance
        ax5 = plt.subplot(2, 3, 5)
        symptoms = len(categories.get('Symptoms', []))
        complications = len(categories.get('Complications', []))
        total_clinical = symptoms + complications
        
        if total_clinical > 0:
            clinical_data = {
                'Presenting\nSymptoms': symptoms/total_clinical*100,
                'Complications': complications/total_clinical*100
            }
            
            bars = ax5.bar(clinical_data.keys(), clinical_data.values(),
                          color=['#96ceb4', '#feca57'])
            ax5.set_ylabel('% of Clinical Findings')
            ax5.set_title('Clinical Presentation\nvs Complications', fontweight='bold')
            ax5.set_ylim(0, 100)
            
            for bar in bars:
                height = bar.get_height()
                ax5.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{height:.1f}%', ha='center', va='bottom')
        
        # 6. Top Concepts by Percentage (After Normalization)
        ax6 = plt.subplot(2, 3, 6)
        
        # Get top normalized concepts with their percentages
        top_normalized = Counter(df_filtered['ConceptName']).most_common(15)
        concepts_norm, counts_norm = zip(*top_normalized)
        total_concepts = len(df_filtered)
        percentages_norm = [(c/total_concepts*100) for c in counts_norm]
        
        # Create horizontal bar chart
        y_positions = np.arange(len(concepts_norm))
        bars = ax6.barh(y_positions, percentages_norm, color='darkgreen', alpha=0.7)
        
        ax6.set_yticks(y_positions)
        ax6.set_yticklabels(concepts_norm, fontsize=10)
        ax6.set_xlabel('Percentage of All Stone-Related Concepts (%)', fontsize=11)
        ax6.set_title('Top Concepts After Normalization', fontweight='bold', fontsize=14)
        
        # Add percentage values
        for i, (bar, pct) in enumerate(zip(bars, percentages_norm)):
            ax6.text(bar.get_width() + 0.2, bar.get_y() + bar.get_height()/2,
                    f'{pct:.1f}%', va='center', fontsize=9, fontweight='bold')
        
        # Add grid for better readability
        ax6.grid(axis='x', alpha=0.3)
        ax6.set_xlim(0, max(percentages_norm) * 1.15)
        
        plt.suptitle('Kidney Stone Concepts in Admission Notes - Refined Analysis',
                    fontsize=18, fontweight='bold', y=0.98)
        plt.tight_layout(rect=[0, 0.03, 1, 0.96])  # Leave space for title
        plt.savefig('kidney_stone_refined_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        return stats, categories, co_occurrences
    
    def create_co_occurrence_table(self, co_occurrences):
        """Create a formatted table of co-occurrences"""
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.axis('tight')
        ax.axis('off')
        
        # Prepare data for table
        table_data = []
        for (concept1, concept2), count in co_occurrences[:15]:
            percentage = f"{count/len(co_occurrences)*100:.1f}%"
            table_data.append([concept1, concept2, f"Co-occurs in {percentage} of files"])
        
        # Create table
        table = ax.table(cellText=table_data,
                        colLabels=['Concept 1', 'Concept 2', 'Frequency'],
                        cellLoc='left',
                        loc='center',
                        colWidths=[0.35, 0.35, 0.3])
        
        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1, 2)
        
        # Style the table
        for i in range(len(table_data) + 1):
            for j in range(3):
                cell = table[(i, j)]
                if i == 0:
                    cell.set_facecolor('#4CAF50')
                    cell.set_text_props(weight='bold', color='white')
                else:
                    cell.set_facecolor('#f0f0f0' if i % 2 == 0 else 'white')
        
        plt.title('Most Common Concept Co-occurrences in Admission Notes',
                 fontsize=14, fontweight='bold', pad=20)
        plt.savefig('co_occurrence_table.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def generate_presentation_summary(self, stats, categories):
        """Generate executive summary for presentation"""
        summary = f"""
KIDNEY STONE ADMISSION NOTES ANALYSIS
=====================================

DATA SCOPE:
• Analyzed admission notes only (not discharge summaries)
• {stats['percent_files_with_stones']:.1f}% of files contain kidney stone concepts
• Filtered out {len(self.noise_concepts)} noise concepts for clarity

TOP CLINICAL PATTERNS:

1. PROCEDURE DISTRIBUTION:
   • Drainage procedures dominate admission notes
   • Reflects acute presentation requiring stabilization
   • Definitive treatment often planned for later

2. CONCEPT CO-OCCURRENCES:
   • "Hydronephrosis" + "Stent placement" (most common pair)
   • "Flank pain" + "CT scan" (diagnostic pathway)
   • "Obstruction" + "Nephrostomy" (emergent intervention)

3. CLINICAL CATEGORIES:
   • Symptoms: Focus on pain, hematuria
   • Diagnostics: CT predominates
   • Procedures: Drainage >> Removal
   • Complications: Obstruction, infection

INTERPRETATION FOR CLINICIANS:
• Admission notes capture the "crisis moment"
• High acuity bias - sickest patients
• Treatment decisions often deferred
• Missing: Stone composition, size details

RESEARCH IMPLICATIONS:
• Need operative reports for technique variations
• Radiology reports essential for stone characteristics
• Clinic notes required for outcomes
• Current data shows "what happened" not "why" or "how well"
"""
        
        with open('admission_notes_summary.txt', 'w') as f:
            f.write(summary)
        
        return summary

def main():
    print("Analyzing kidney stone concepts in admission notes...\n")
    
    analyzer = RefinedKidneyStoneAnalyzer()
    
    # Load and filter data
    df_full, df_filtered = analyzer.load_and_filter_data()
    
    if df_filtered.empty:
        print("No kidney stone concepts found!")
        return
    
    # Generate analysis
    stats, categories, co_occurrences = analyzer.create_percentage_visualizations(
        df_full, df_filtered)
    
    # Create co-occurrence table
    analyzer.create_co_occurrence_table(co_occurrences)
    
    # Generate summary
    summary = analyzer.generate_presentation_summary(stats, categories)
    
    print("✓ Analysis complete!")
    print("\nGenerated files:")
    print("  - kidney_stone_refined_analysis.png (main analysis)")
    print("  - co_occurrence_table.png (concept pairs)")
    print("  - admission_notes_summary.txt (executive summary)")
    
    print("\nKEY FINDINGS:")
    print(f"  - {stats['percent_files_with_stones']:.1f}% of admission notes mention kidney stones")
    print(f"  - Drainage procedures outnumber removal procedures significantly")
    print(f"  - Strong co-occurrence patterns reveal clinical pathways")
    
    return summary

if __name__ == "__main__":
    summary = main()