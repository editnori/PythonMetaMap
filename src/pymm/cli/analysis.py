"""Advanced analysis features for PythonMetaMap CLI"""
import os
import csv
import json
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box
import matplotlib.pyplot as plt
import seaborn as sns

console = Console()

# Predefined filter sets for common analyses
FILTER_PRESETS = {
    'kidney_stone': {
        'terms': ['kidney stone', 'nephrolithiasis', 'renal calculi', 'ureteral calculi', 
                  'staghorn calculi', 'urolithiasis', 'kidney calculus', 'renal stone'],
        'cuis': ['C0022650', 'C0041952', 'C0022646', 'C0149904'],  # Common kidney stone CUIs
        'description': 'Kidney stone related concepts'
    },
    'kidney_symptoms': {
        'terms': ['flank pain', 'hematuria', 'dysuria', 'renal colic', 'urinary obstruction'],
        'cuis': ['C0016199', 'C0018965', 'C0013428', 'C0152169'],
        'description': 'Kidney stone symptoms'
    },
    'kidney_procedures': {
        'terms': ['lithotripsy', 'ureteroscopy', 'percutaneous nephrolithotomy', 'ureteral stent',
                  'cystoscopy', 'nephrostomy'],
        'cuis': ['C0023878', 'C0194261', 'C0162428', 'C0183518'],
        'description': 'Kidney stone procedures'
    },
    'diabetes': {
        'terms': ['diabetes', 'diabetic', 'glucose', 'insulin', 'hyperglycemia', 'A1C'],
        'description': 'Diabetes related concepts'
    },
    'hypertension': {
        'terms': ['hypertension', 'blood pressure', 'hypertensive', 'antihypertensive'],
        'description': 'Hypertension related concepts'
    },
    'pain': {
        'terms': ['pain', 'ache', 'discomfort', 'tenderness', 'soreness'],
        'description': 'Pain related concepts'
    }
}

class ConceptAnalyzer:
    """Advanced concept analysis similar to kidney stone analysis"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.concepts = Counter()
        self.semantic_types = Counter()
        self.concept_details = defaultdict(dict)
        self.file_count = 0
        self.total_rows = 0
        self.failed_files = []
        self.cooccurrence_matrix = defaultdict(Counter)
        self.file_concepts = defaultdict(set)  # Track concepts per file
        
    def analyze_directory(self, filter_terms: Optional[List[str]] = None, 
                         filter_cuis: Optional[List[str]] = None,
                         preset: Optional[str] = None):
        """Analyze all CSV files in the output directory"""
        # Apply preset filters if specified
        if preset and preset in FILTER_PRESETS:
            preset_config = FILTER_PRESETS[preset]
            if not filter_terms:
                filter_terms = preset_config.get('terms', [])
            if not filter_cuis:
                filter_cuis = preset_config.get('cuis', [])
        
        csv_files = list(self.output_dir.glob("*.csv"))
        csv_files = [f for f in csv_files if not f.name.startswith('.')]
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task(f"Analyzing {len(csv_files)} files...", total=len(csv_files))
            
            for csv_file in csv_files:
                try:
                    self._analyze_file(csv_file, filter_terms, filter_cuis)
                    self.file_count += 1
                    progress.update(task, advance=1)
                except Exception as e:
                    self.failed_files.append((csv_file.name, str(e)))
                    progress.update(task, advance=1)
        
        # Calculate co-occurrences
        self._calculate_cooccurrences()
        
        return self._generate_report()
    
    def _analyze_file(self, csv_file: Path, filter_terms: Optional[List[str]] = None,
                     filter_cuis: Optional[List[str]] = None):
        """Analyze a single CSV file"""
        file_concepts = set()
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            # Skip the start marker line if present
            first_line = f.readline()
            if not first_line.startswith("META_BATCH_START"):
                f.seek(0)
            
            reader = csv.DictReader(f)
            
            for row in reader:
                # Skip empty rows or end marker
                if not row or 'META_BATCH' in str(row.get('CUI', '')):
                    continue
                
                self.total_rows += 1
                
                # Extract concept information
                cui = row.get('CUI', '').strip()
                concept_name = row.get('ConceptName', '').strip()
                pref_name = row.get('PrefName', row.get('preferred_name', '')).strip()
                sem_types = row.get('SemTypes', row.get('semantic_types', '')).strip()
                score = row.get('Score', '').strip()
                
                # Apply filters if specified
                if filter_terms or filter_cuis:
                    # Check CUI filter
                    if filter_cuis and cui not in filter_cuis:
                        # Check term filter
                        if filter_terms:
                            if not any(term.lower() in (concept_name.lower() + ' ' + pref_name.lower()) 
                                      for term in filter_terms):
                                continue
                        else:
                            continue
                    # If CUI matches, include it regardless of term filter
                
                if cui and pref_name:
                    # Count concept occurrences
                    concept_key = f"{pref_name} ({cui})"
                    self.concepts[concept_key] += 1
                    file_concepts.add(cui)
                    
                    # Store concept details
                    if cui not in self.concept_details:
                        self.concept_details[cui] = {
                            'preferred_name': pref_name,
                            'concept_name': concept_name,
                            'semantic_types': set(),
                            'scores': [],
                            'count': 0,
                            'files': set()
                        }
                    
                    self.concept_details[cui]['count'] += 1
                    self.concept_details[cui]['files'].add(csv_file.name)
                    if score and score != '-':
                        try:
                            self.concept_details[cui]['scores'].append(float(score))
                        except ValueError:
                            pass
                
                # Count semantic types
                if sem_types:
                    sem_types = sem_types.strip('[]')
                    for st in sem_types.split(','):
                        st = st.strip().strip("'\"")
                        if st:
                            self.semantic_types[st] += 1
                            if cui in self.concept_details:
                                self.concept_details[cui]['semantic_types'].add(st)
        
        # Store file concepts for co-occurrence analysis
        self.file_concepts[csv_file.name] = file_concepts
    
    def _calculate_cooccurrences(self):
        """Calculate concept co-occurrences across files"""
        # For each file, count co-occurrences
        for file_name, concepts in self.file_concepts.items():
            concept_list = list(concepts)
            for i, cui1 in enumerate(concept_list):
                for cui2 in concept_list[i+1:]:
                    self.cooccurrence_matrix[cui1][cui2] += 1
                    self.cooccurrence_matrix[cui2][cui1] += 1
    
    def _generate_report(self) -> Dict:
        """Generate analysis report"""
        # Calculate average scores
        for cui, details in self.concept_details.items():
            if details['scores']:
                details['avg_score'] = np.mean(details['scores'])
                details['score_std'] = np.std(details['scores'])
            else:
                details['avg_score'] = 0
                details['score_std'] = 0
        
        return {
            'summary': {
                'files_analyzed': self.file_count,
                'total_concepts': len(self.concepts),
                'total_occurrences': sum(self.concepts.values()),
                'unique_semantic_types': len(self.semantic_types),
                'failed_files': len(self.failed_files),
                'total_rows': self.total_rows,
                'concepts_per_file': sum(self.concepts.values()) / self.file_count if self.file_count > 0 else 0
            },
            'top_concepts': self.concepts.most_common(50),
            'semantic_types': self.semantic_types.most_common(20),
            'concept_details': dict(self.concept_details),
            'failed_files': self.failed_files,
            'cooccurrence_matrix': dict(self.cooccurrence_matrix)
        }
    
    def generate_visualizations(self, output_path: Path, filter_name: str = ""):
        """Generate visualization plots similar to kidney stone analysis"""
        if not self.concepts:
            console.print("[yellow]No concepts to visualize[/yellow]")
            return
        
        # Create output directory
        viz_dir = output_path / "visualizations"
        viz_dir.mkdir(exist_ok=True)
        
        prefix = f"{filter_name}_" if filter_name else ""
        
        # Set style
        plt.style.use('seaborn-v0_8-darkgrid')
        
        # 1. Top concepts bar chart
        plt.figure(figsize=(12, 8))
        top_concepts = self.concepts.most_common(20)
        concepts, counts = zip(*top_concepts)
        
        # Shorten concept names for display
        short_concepts = [c.split('(')[0].strip()[:30] + '...' if len(c) > 35 else c.split('(')[0].strip() 
                         for c in concepts]
        
        colors = plt.cm.Blues(np.linspace(0.4, 0.8, len(short_concepts)))
        bars = plt.barh(range(len(short_concepts)), counts, color=colors)
        
        # Add value labels
        for i, (bar, count) in enumerate(zip(bars, counts)):
            plt.text(bar.get_width() + max(counts)*0.01, bar.get_y() + bar.get_height()/2, 
                    str(count), va='center')
        
        plt.yticks(range(len(short_concepts)), short_concepts)
        plt.xlabel('Occurrences')
        plt.title(f'Top 20 {filter_name.replace("_", " ").title() if filter_name else ""} Medical Concepts', 
                 fontsize=16, fontweight='bold')
        plt.tight_layout()
        plt.savefig(viz_dir / f'{prefix}top_concepts.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 2. Semantic type distribution pie chart
        plt.figure(figsize=(10, 8))
        top_sem_types = self.semantic_types.most_common(10)
        if top_sem_types:
            types, counts = zip(*top_sem_types)
            colors = plt.cm.Set3(np.linspace(0, 1, len(types)))
            
            plt.pie(counts, labels=types, autopct='%1.1f%%', startangle=90, colors=colors)
            plt.title(f'{filter_name.replace("_", " ").title() if filter_name else ""} Semantic Type Distribution',
                     fontsize=16, fontweight='bold')
            plt.tight_layout()
            plt.savefig(viz_dir / f'{prefix}semantic_types.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # 3. Concept co-occurrence network (top concepts)
        self._generate_cooccurrence_network(viz_dir / f'{prefix}cooccurrence_network.png', filter_name)
        
        # 4. Dashboard summary (similar to kidney stone analysis)
        self._generate_dashboard(viz_dir / f'{prefix}analysis_dashboard.png', filter_name)
        
        console.print(f"[green]✓ Visualizations saved to {viz_dir}[/green]")
    
    def _generate_cooccurrence_network(self, output_file: Path, filter_name: str):
        """Generate concept co-occurrence network visualization"""
        if not self.cooccurrence_matrix:
            return
        
        # Get top concepts for visualization
        top_concepts = [cui for concept, _ in self.concepts.most_common(15)]
        top_cuis = [concept.split('(')[1].rstrip(')') for concept, _ in self.concepts.most_common(15)]
        
        # Create adjacency matrix
        n = len(top_cuis)
        adj_matrix = np.zeros((n, n))
        
        for i, cui1 in enumerate(top_cuis):
            for j, cui2 in enumerate(top_cuis):
                if cui1 in self.cooccurrence_matrix and cui2 in self.cooccurrence_matrix[cui1]:
                    adj_matrix[i, j] = self.cooccurrence_matrix[cui1][cui2]
        
        # Create heatmap
        plt.figure(figsize=(12, 10))
        
        # Get concept names for labels
        labels = []
        for cui in top_cuis:
            if cui in self.concept_details:
                name = self.concept_details[cui]['preferred_name']
                labels.append(name[:25] + '...' if len(name) > 25 else name)
            else:
                labels.append(cui)
        
        sns.heatmap(adj_matrix, xticklabels=labels, yticklabels=labels, 
                   cmap='YlOrRd', annot=True, fmt='.0f', square=True)
        
        plt.title(f'{filter_name.replace("_", " ").title() if filter_name else ""} Concept Co-occurrence Matrix',
                 fontsize=16, fontweight='bold')
        plt.xticks(rotation=45, ha='right')
        plt.yticks(rotation=0)
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
    
    def _generate_dashboard(self, output_file: Path, filter_name: str):
        """Generate a comprehensive dashboard similar to kidney stone analysis"""
        fig = plt.figure(figsize=(16, 12))
        
        # Create grid
        gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
        
        # 1. Summary statistics (top left)
        ax1 = fig.add_subplot(gs[0, 0])
        ax1.axis('off')
        summary_text = f"""Analysis Summary
        
Files Analyzed: {self.file_count:,}
Total Concepts: {len(self.concepts):,}
Total Occurrences: {sum(self.concepts.values()):,}
Unique Semantic Types: {len(self.semantic_types)}
Average Concepts/File: {sum(self.concepts.values()) / self.file_count if self.file_count > 0 else 0:.1f}
        """
        ax1.text(0.1, 0.5, summary_text, fontsize=12, verticalalignment='center',
                bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.5))
        
        # 2. Top 10 concepts (top middle and right)
        ax2 = fig.add_subplot(gs[0, 1:])
        top_10 = self.concepts.most_common(10)
        if top_10:
            concepts, counts = zip(*top_10)
            short_concepts = [c.split('(')[0].strip()[:20] for c in concepts]
            
            bars = ax2.bar(range(len(short_concepts)), counts, color='steelblue')
            ax2.set_xticks(range(len(short_concepts)))
            ax2.set_xticklabels(short_concepts, rotation=45, ha='right')
            ax2.set_ylabel('Count')
            ax2.set_title('Top 10 Concepts', fontweight='bold')
            
            # Add value labels
            for bar, count in zip(bars, counts):
                ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(counts)*0.01,
                        str(count), ha='center', va='bottom')
        
        # 3. Semantic type pie chart (middle left)
        ax3 = fig.add_subplot(gs[1, 0])
        top_5_types = self.semantic_types.most_common(5)
        if top_5_types:
            types, counts = zip(*top_5_types)
            ax3.pie(counts, labels=types, autopct='%1.1f%%', startangle=90)
            ax3.set_title('Top 5 Semantic Types', fontweight='bold')
        
        # 4. Score distribution (middle center)
        ax4 = fig.add_subplot(gs[1, 1])
        all_scores = []
        for details in self.concept_details.values():
            all_scores.extend(details['scores'])
        
        if all_scores:
            ax4.hist(all_scores, bins=30, color='green', alpha=0.7, edgecolor='black')
            ax4.set_xlabel('MetaMap Score')
            ax4.set_ylabel('Frequency')
            ax4.set_title('Score Distribution', fontweight='bold')
            ax4.axvline(np.mean(all_scores), color='red', linestyle='--', 
                       label=f'Mean: {np.mean(all_scores):.0f}')
            ax4.legend()
        
        # 5. Files per concept distribution (middle right)
        ax5 = fig.add_subplot(gs[1, 2])
        files_per_concept = [len(details['files']) for details in self.concept_details.values()]
        if files_per_concept:
            ax5.hist(files_per_concept, bins=20, color='orange', alpha=0.7, edgecolor='black')
            ax5.set_xlabel('Number of Files')
            ax5.set_ylabel('Number of Concepts')
            ax5.set_title('Concept Distribution Across Files', fontweight='bold')
        
        # 6. Co-occurrence strength (bottom)
        ax6 = fig.add_subplot(gs[2, :])
        
        # Get top co-occurring pairs
        cooccur_pairs = []
        for cui1, others in self.cooccurrence_matrix.items():
            for cui2, count in others.items():
                if cui1 < cui2:  # Avoid duplicates
                    name1 = self.concept_details.get(cui1, {}).get('preferred_name', cui1)[:20]
                    name2 = self.concept_details.get(cui2, {}).get('preferred_name', cui2)[:20]
                    cooccur_pairs.append((f"{name1} - {name2}", count))
        
        cooccur_pairs.sort(key=lambda x: x[1], reverse=True)
        top_pairs = cooccur_pairs[:10]
        
        if top_pairs:
            pairs, counts = zip(*top_pairs)
            ax6.barh(range(len(pairs)), counts, color='purple', alpha=0.7)
            ax6.set_yticks(range(len(pairs)))
            ax6.set_yticklabels(pairs)
            ax6.set_xlabel('Co-occurrence Count')
            ax6.set_title('Top Concept Co-occurrences', fontweight='bold')
        
        # Main title
        fig.suptitle(f'{filter_name.replace("_", " ").title() if filter_name else "Medical Concept"} Analysis Dashboard',
                    fontsize=18, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
    
    def export_to_excel(self, output_file: Path, filter_name: str = ""):
        """Export comprehensive analysis to Excel file"""
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Summary sheet
            summary_df = pd.DataFrame([self._generate_report()['summary']])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Top concepts sheet
            concepts_data = []
            for concept, count in self.concepts.most_common():
                cui = concept.split('(')[1].rstrip(')')
                details = self.concept_details.get(cui, {})
                concepts_data.append({
                    'Concept': concept.split('(')[0].strip(),
                    'CUI': cui,
                    'Count': count,
                    'Avg Score': details.get('avg_score', 0),
                    'Files': len(details.get('files', [])),
                    'Semantic Types': ', '.join(details.get('semantic_types', []))
                })
            
            concepts_df = pd.DataFrame(concepts_data)
            concepts_df.to_excel(writer, sheet_name='All Concepts', index=False)
            
            # Semantic types sheet
            sem_types_df = pd.DataFrame(self.semantic_types.most_common(), 
                                       columns=['Semantic Type', 'Count'])
            sem_types_df.to_excel(writer, sheet_name='Semantic Types', index=False)
            
            # Co-occurrences sheet
            cooccur_data = []
            for cui1, others in self.cooccurrence_matrix.items():
                for cui2, count in others.items():
                    if cui1 < cui2:
                        cooccur_data.append({
                            'Concept 1': self.concept_details.get(cui1, {}).get('preferred_name', cui1),
                            'CUI 1': cui1,
                            'Concept 2': self.concept_details.get(cui2, {}).get('preferred_name', cui2),
                            'CUI 2': cui2,
                            'Co-occurrence Count': count
                        })
            
            if cooccur_data:
                cooccur_df = pd.DataFrame(cooccur_data)
                cooccur_df.sort_values('Co-occurrence Count', ascending=False, inplace=True)
                cooccur_df.to_excel(writer, sheet_name='Co-occurrences', index=False)
            
            # Failed files sheet
            if self.failed_files:
                failed_df = pd.DataFrame(self.failed_files, columns=['File', 'Error'])
                failed_df.to_excel(writer, sheet_name='Failed Files', index=False)
        
        console.print(f"[green]✓ Analysis exported to {output_file}[/green]")


class ProcessingSessionAnalyzer:
    """Analyze processing sessions with detailed statistics"""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.state_file = self.output_dir / ".pymm_state.json"
        self.failed_dir = self.output_dir / "failed_files"
        
    def get_session_stats(self) -> Dict:
        """Get detailed session statistics"""
        stats = {
            'session_info': {},
            'file_stats': {
                'total': 0,
                'completed': 0,
                'failed': 0,
                'in_progress': 0,
                'success_rate': 0.0
            },
            'performance': {
                'avg_processing_time': 0.0,
                'throughput': 0.0,
                'total_time': 0.0
            },
            'output_stats': {
                'total_size': 0,
                'avg_file_size': 0,
                'total_concepts': 0
            },
            'failed_analysis': {
                'by_error_type': Counter(),
                'retry_candidates': []
            }
        }
        
        # Read state file
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                
                # Session info
                stats['session_info'] = {
                    'session_id': state.get('session_id', 'Unknown'),
                    'started': state.get('started', 'Unknown'),
                    'last_updated': state.get('last_updated', 'Unknown'),
                    'input_dir': state.get('input_dir', 'Unknown'),
                    'config': state.get('config', {})
                }
                
                # File statistics
                files = state.get('files', {})
                stats['file_stats']['total'] = len(files)
                
                processing_times = []
                for file_path, file_info in files.items():
                    status = file_info.get('status', 'unknown')
                    if status == 'completed':
                        stats['file_stats']['completed'] += 1
                        # Calculate processing time if available
                        if 'start_time' in file_info and 'end_time' in file_info:
                            try:
                                start = datetime.fromisoformat(file_info['start_time'])
                                end = datetime.fromisoformat(file_info['end_time'])
                                processing_times.append((end - start).total_seconds())
                            except:
                                pass
                    elif status == 'failed':
                        stats['file_stats']['failed'] += 1
                    elif status == 'processing':
                        stats['file_stats']['in_progress'] += 1
                
                # Calculate success rate
                if stats['file_stats']['total'] > 0:
                    stats['file_stats']['success_rate'] = (
                        stats['file_stats']['completed'] / stats['file_stats']['total'] * 100
                    )
                
                # Performance metrics
                if processing_times:
                    stats['performance']['avg_processing_time'] = np.mean(processing_times)
                    stats['performance']['total_time'] = sum(processing_times)
                    if stats['performance']['total_time'] > 0:
                        stats['performance']['throughput'] = len(processing_times) / stats['performance']['total_time']
                
                # Failed file analysis
                failed_files = state.get('failed_files', {})
                for file_path, error_info in failed_files.items():
                    error_msg = error_info.get('error', 'Unknown error')
                    # Categorize errors
                    if 'timeout' in error_msg.lower():
                        error_type = 'Timeout'
                    elif 'memory' in error_msg.lower():
                        error_type = 'Memory Error'
                    elif 'java' in error_msg.lower():
                        error_type = 'Java Error'
                    elif 'connection' in error_msg.lower():
                        error_type = 'Connection Error'
                    else:
                        error_type = 'Other'
                    
                    stats['failed_analysis']['by_error_type'][error_type] += 1
                    
                    # Check if retry candidate
                    attempts = error_info.get('attempts', 1)
                    if attempts < 3:  # Could retry
                        stats['failed_analysis']['retry_candidates'].append({
                            'file': Path(file_path).name,
                            'attempts': attempts,
                            'error': error_msg
                        })
                
            except Exception as e:
                console.print(f"[yellow]Warning: Could not read state file: {e}[/yellow]")
        
        # Output statistics
        total_size = 0
        concept_count = 0
        output_files = list(self.output_dir.glob("*.csv"))
        output_files = [f for f in output_files if not f.name.startswith('.')]
        
        for output_file in output_files:
            try:
                size = output_file.stat().st_size
                total_size += size
                
                # Quick concept count
                with open(output_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip() and not line.startswith('META_BATCH'):
                            concept_count += 1
            except:
                pass
        
        stats['output_stats']['total_size'] = total_size
        stats['output_stats']['avg_file_size'] = total_size // len(output_files) if output_files else 0
        stats['output_stats']['total_concepts'] = concept_count
        
        return stats
    
    def get_retry_candidates(self) -> List[Dict]:
        """Get list of files that can be retried"""
        candidates = []
        
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            
            failed_files = state.get('failed_files', {})
            for file_path, error_info in failed_files.items():
                attempts = error_info.get('attempts', 1)
                if attempts < 3:
                    candidates.append({
                        'file_path': file_path,
                        'file_name': Path(file_path).name,
                        'attempts': attempts,
                        'error': error_info.get('error', 'Unknown'),
                        'timestamp': error_info.get('timestamp', 'Unknown')
                    })
        
        return candidates
    
    def sync_with_filesystem(self) -> Dict[str, int]:
        """Sync state with actual filesystem"""
        actual_outputs = set(f.stem for f in self.output_dir.glob("*.csv") 
                           if not f.name.startswith('.'))
        actual_failed = set(f.stem for f in self.failed_dir.glob("*.txt")) if self.failed_dir.exists() else set()
        
        # Read state
        state_outputs = set()
        state_failed = set()
        
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                
            for file_path, info in state.get('files', {}).items():
                file_stem = Path(file_path).stem
                if info.get('status') == 'completed':
                    state_outputs.add(file_stem)
                elif info.get('status') == 'failed':
                    state_failed.add(file_stem)
        
        # Find discrepancies
        return {
            'outputs_not_in_state': len(actual_outputs - state_outputs),
            'state_not_in_outputs': len(state_outputs - actual_outputs),
            'failed_not_in_state': len(actual_failed - state_failed),
            'state_not_in_failed': len(state_failed - actual_failed),
            'actual_outputs': len(actual_outputs),
            'actual_failed': len(actual_failed)
        }


@click.group()
def analysis_group():
    """Advanced analysis commands"""
    pass

@analysis_group.command(name='concepts')
@click.argument('output_dir', type=click.Path(exists=True))
@click.option('--filter', '-f', multiple=True, help='Filter concepts by terms (e.g., kidney, stone)')
@click.option('--preset', '-p', type=click.Choice(list(FILTER_PRESETS.keys())), 
              help='Use predefined filter preset')
@click.option('--visualize', '-v', is_flag=True, help='Generate visualization plots')
@click.option('--export', '-e', type=click.Path(), help='Export detailed report to JSON')
@click.option('--excel', '-x', type=click.Path(), help='Export to Excel file')
@click.option('--top', '-n', default=20, help='Number of top concepts to show')
def analyze_concepts(output_dir, filter, preset, visualize, export, excel, top):
    """Perform advanced concept analysis on output files
    
    Examples:
    
        # Basic analysis
        pymm analysis concepts output_csvs/
        
        # Kidney stone analysis with visualizations
        pymm analysis concepts output_csvs/ --preset kidney_stone --visualize
        
        # Custom filter with Excel export
        pymm analysis concepts output_csvs/ -f diabetes -f insulin --excel analysis.xlsx
        
        # Multiple analyses
        pymm analysis concepts output_csvs/ --preset kidney_symptoms --visualize --excel symptoms.xlsx
    """
    analyzer = ConceptAnalyzer(Path(output_dir))
    
    filter_terms = list(filter) if filter else None
    filter_name = preset if preset else ("_".join(filter) if filter else "all")
    
    # Show what we're analyzing
    if preset:
        preset_info = FILTER_PRESETS[preset]
        console.print(f"\n[bold cyan]Analyzing with preset: {preset_info['description']}[/bold cyan]")
        if preset_info.get('terms'):
            console.print(f"[dim]Terms: {', '.join(preset_info['terms'][:5])}{'...' if len(preset_info['terms']) > 5 else ''}[/dim]")
    elif filter:
        console.print(f"\n[bold cyan]Analyzing concepts with filters: {', '.join(filter)}[/bold cyan]")
    else:
        console.print(f"\n[bold cyan]Analyzing all concepts...[/bold cyan]")
    
    report = analyzer.analyze_directory(filter_terms, preset=preset)
    
    # Display summary
    summary = report['summary']
    summary_table = Table(title="Analysis Summary", box=box.ROUNDED)
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Value", style="green")
    
    summary_table.add_row("Files Analyzed", str(summary['files_analyzed']))
    summary_table.add_row("Total Unique Concepts", f"{summary['total_concepts']:,}")
    summary_table.add_row("Total Occurrences", f"{summary['total_occurrences']:,}")
    summary_table.add_row("Unique Semantic Types", str(summary['unique_semantic_types']))
    summary_table.add_row("Average Concepts/File", f"{summary['concepts_per_file']:.1f}")
    summary_table.add_row("Failed Files", str(summary['failed_files']))
    
    console.print(summary_table)
    
    # Display top concepts
    if report['top_concepts']:
        console.print(f"\n[bold]Top {min(top, len(report['top_concepts']))} Concepts[/bold]")
        
        concept_table = Table(box=box.ROUNDED)
        concept_table.add_column("Rank", style="cyan")
        concept_table.add_column("Concept (CUI)", style="green")
        concept_table.add_column("Count", style="yellow")
        concept_table.add_column("Frequency", style="dim")
        
        total = summary['total_occurrences']
        for i, (concept, count) in enumerate(report['top_concepts'][:top], 1):
            freq = f"{(count/total)*100:.2f}%"
            concept_table.add_row(str(i), concept, str(count), freq)
        
        console.print(concept_table)
    
    # Display semantic types
    if report['semantic_types']:
        console.print(f"\n[bold]Top Semantic Types[/bold]")
        
        sem_table = Table(box=box.ROUNDED)
        sem_table.add_column("Semantic Type", style="blue")
        sem_table.add_column("Count", style="yellow")
        
        for sem_type, count in report['semantic_types'][:10]:
            sem_table.add_row(sem_type, str(count))
        
        console.print(sem_table)
    
    # Generate visualizations
    if visualize:
        console.print("\n[cyan]Generating visualizations...[/cyan]")
        analyzer.generate_visualizations(Path(output_dir), filter_name)
    
    # Export to Excel
    if excel:
        console.print("\n[cyan]Exporting to Excel...[/cyan]")
        analyzer.export_to_excel(Path(excel), filter_name)
    
    # Export report
    if export:
        export_path = Path(export)
        with open(export_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        console.print(f"\n[green]✓ Detailed report exported to {export_path}[/green]")

@analysis_group.command(name='session')
@click.argument('output_dir', type=click.Path(exists=True))
@click.option('--sync', is_flag=True, help='Sync state with filesystem')
@click.option('--retry-candidates', is_flag=True, help='Show files that can be retried')
def analyze_session(output_dir, sync, retry_candidates):
    """Analyze processing session with detailed statistics"""
    analyzer = ProcessingSessionAnalyzer(Path(output_dir))
    
    # Get session stats
    stats = analyzer.get_session_stats()
    
    # Display session info
    if stats['session_info']:
        console.print(Panel(
            f"[bold]Session ID:[/bold] {stats['session_info']['session_id']}\n"
            f"[bold]Started:[/bold] {stats['session_info']['started']}\n"
            f"[bold]Last Updated:[/bold] {stats['session_info']['last_updated']}\n"
            f"[bold]Input Directory:[/bold] {stats['session_info']['input_dir']}",
            title="Session Information",
            style="cyan"
        ))
    
    # File statistics
    file_stats = stats['file_stats']
    file_table = Table(title="File Processing Statistics", box=box.ROUNDED)
    file_table.add_column("Status", style="cyan")
    file_table.add_column("Count", style="green")
    file_table.add_column("Percentage", style="yellow")
    
    total = file_stats['total']
    if total > 0:
        file_table.add_row("Completed", str(file_stats['completed']), 
                          f"{file_stats['completed']/total*100:.1f}%")
        file_table.add_row("Failed", str(file_stats['failed']), 
                          f"{file_stats['failed']/total*100:.1f}%")
        file_table.add_row("In Progress", str(file_stats['in_progress']), 
                          f"{file_stats['in_progress']/total*100:.1f}%")
        file_table.add_row("", "", "")
        file_table.add_row("[bold]Total[/bold]", f"[bold]{total}[/bold]", 
                          f"[bold]{file_stats['success_rate']:.1f}% success[/bold]")
    
    console.print(file_table)
    
    # Performance metrics
    perf = stats['performance']
    if perf['avg_processing_time'] > 0:
        console.print("\n[bold]Performance Metrics[/bold]")
        console.print(f"  Average Processing Time: {perf['avg_processing_time']:.1f} seconds/file")
        console.print(f"  Throughput: {perf['throughput']:.2f} files/second")
        console.print(f"  Total Processing Time: {perf['total_time']/60:.1f} minutes")
    
    # Failed file analysis
    if stats['failed_analysis']['by_error_type']:
        console.print("\n[bold]Failed File Analysis[/bold]")
        
        error_table = Table(box=box.ROUNDED)
        error_table.add_column("Error Type", style="red")
        error_table.add_column("Count", style="yellow")
        
        for error_type, count in stats['failed_analysis']['by_error_type'].items():
            error_table.add_row(error_type, str(count))
        
        console.print(error_table)
        
        if stats['failed_analysis']['retry_candidates']:
            console.print(f"\n[yellow]ℹ {len(stats['failed_analysis']['retry_candidates'])} files can be retried[/yellow]")
    
    # Output statistics
    output_stats = stats['output_stats']
    if output_stats['total_size'] > 0:
        console.print("\n[bold]Output Statistics[/bold]")
        console.print(f"  Total Size: {output_stats['total_size'] / 1024 / 1024:.1f} MB")
        console.print(f"  Average File Size: {output_stats['avg_file_size'] / 1024:.1f} KB")
        console.print(f"  Total Concepts: {output_stats['total_concepts']:,}")
    
    # Show retry candidates
    if retry_candidates:
        candidates = analyzer.get_retry_candidates()
        if candidates:
            console.print("\n[bold]Files Available for Retry[/bold]")
            retry_table = Table(box=box.ROUNDED)
            retry_table.add_column("File", style="cyan")
            retry_table.add_column("Attempts", style="yellow")
            retry_table.add_column("Error", style="red")
            
            for candidate in candidates[:10]:  # Show first 10
                retry_table.add_row(
                    candidate['file_name'],
                    str(candidate['attempts']),
                    candidate['error'][:50] + '...' if len(candidate['error']) > 50 else candidate['error']
                )
            
            console.print(retry_table)
            
            if len(candidates) > 10:
                console.print(f"\n[dim]... and {len(candidates) - 10} more files[/dim]")
            
            console.print(f"\n[green]Run 'pymm retry-failed {output_dir}' to retry these files[/green]")
    
    # Sync with filesystem
    if sync:
        console.print("\n[cyan]Syncing with filesystem...[/cyan]")
        sync_stats = analyzer.sync_with_filesystem()
        
        if any(v > 0 for k, v in sync_stats.items() if k.startswith('outputs_not') or k.startswith('state_not')):
            console.print("\n[yellow]⚠ Discrepancies found:[/yellow]")
            if sync_stats['outputs_not_in_state'] > 0:
                console.print(f"  • {sync_stats['outputs_not_in_state']} output files not tracked in state")
            if sync_stats['state_not_in_outputs'] > 0:
                console.print(f"  • {sync_stats['state_not_in_outputs']} completed files missing from outputs")
            if sync_stats['failed_not_in_state'] > 0:
                console.print(f"  • {sync_stats['failed_not_in_state']} failed files not tracked in state")
            
            console.print(f"\n[dim]Actual files: {sync_stats['actual_outputs']} outputs, {sync_stats['actual_failed']} failed[/dim]")
        else:
            console.print("[green]✓ State is synchronized with filesystem[/green]") 