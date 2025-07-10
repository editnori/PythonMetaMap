"""Individual file processing worker"""
import os
import csv
import time
import logging
import socket
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

from ..pymm import Metamap as PyMetaMap
from ..core.exceptions import MetamapStuck, ParseError
from ..cmdexecutor import verify_metamap_server_connectivity

# CSV output configuration
CSV_HEADER = [
    "CUI",
    "Score", 
    "ConceptName",
    "PrefName",
    "Phrase",
    "SemTypes",
    "Sources",
    "Position",
]

START_MARKER_PREFIX = "META_BATCH_START_NOTE_ID:"
END_MARKER_PREFIX = "META_BATCH_END_NOTE_ID:"

class FileProcessor:
    """Handles processing of individual files through MetaMap"""
    
    def __init__(self, metamap_binary_path: str, output_dir: str, 
                 metamap_options: str = "", timeout: int = 300,
                 metamap_instance=None, tagger_port=1795, wsd_port=5554,
                 worker_id=0, state_manager=None, file_tracker=None):
        self.metamap_binary_path = metamap_binary_path
        self.output_dir = Path(output_dir)
        self.metamap_options = metamap_options
        self.timeout = timeout
        self.metamap_instance = metamap_instance  # Reusable instance
        self.tagger_port = tagger_port
        self.wsd_port = wsd_port
        self.worker_id = worker_id
        self.state_manager = state_manager  # For tracking concepts
        self.file_tracker = file_tracker  # For unified tracking
        self.logger = logging.getLogger(f"FileProcessor-{worker_id}")
        
        # Setup file logging for this worker
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup file logging for this worker"""
        log_dir = self.output_dir / "logs"
        log_dir.mkdir(exist_ok=True)
        
        # Create worker-specific log file
        log_file = log_dir / f"worker_{self.worker_id}.log"
        
        # Create file handler
        handler = logging.FileHandler(log_file)
        handler.setLevel(logging.DEBUG)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Add handler to logger
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        
    def process_file(self, input_file_path: str) -> Tuple[bool, float, Optional[str]]:
        """Process a single file through MetaMap
        
        Args:
            input_file_path: Path to input text file
            
        Returns:
            Tuple of (success, processing_time, error_message)
        """
        start_time = time.time()
        input_path = Path(input_file_path)
        output_path = self._get_output_path(input_path.name)
        
        # Mark file as in progress in unified tracker
        if self.file_tracker:
            self.file_tracker.start_processing(input_path)
        
        try:
            # Read input file
            content = self._read_input_file(input_path)
            if not content:
                self._write_empty_output(output_path, input_path.name)
                # Mark as completed with 0 concepts
                if self.file_tracker:
                    self.file_tracker.complete_processing(input_path, concepts_found=0, processing_time=time.time() - start_time)
                return True, time.time() - start_time, None
            
            # Process through MetaMap
            try:
                concepts = self._process_content(content, input_path.name)
            except Exception as e:
                # Mark as failed in both trackers
                if self.state_manager:
                    self.state_manager.mark_failed(str(input_path), str(e))
                if self.file_tracker:
                    self.file_tracker.fail_processing(input_path, str(e))
                
                self.logger.error(f"Error processing {input_path.name}: {e}")
                raise
            
            # Write output
            self._write_output(output_path, input_path.name, concepts)
            
            # Track concepts if state manager is available
            if self.state_manager and concepts:
                try:
                    self.state_manager.track_concepts(concepts)
                except Exception as e:
                    # Log error but don't fail the file processing
                    self.logger.error(f"Failed to track concepts for {input_path.name}: {e}")
            
            processing_time = time.time() - start_time
            self.logger.info(f"Processed {input_path.name} in {processing_time:.2f}s, found {len(concepts)} concepts")
            
            # Mark as completed in unified tracker
            if self.file_tracker:
                self.file_tracker.complete_processing(input_path, concepts_found=len(concepts), processing_time=processing_time)
            
            return True, processing_time, None
            
        except MetamapStuck as e:
            error_msg = f"MetaMap timeout after {self.timeout}s"
            self._write_error_output(output_path, input_path.name, error_msg)
            # Mark as failed in unified tracker
            if self.file_tracker:
                self.file_tracker.fail_processing(input_path, error_msg)
            return False, time.time() - start_time, error_msg
            
        except ParseError as e:
            error_msg = f"Parse error: {e.details}"
            self._write_error_output(output_path, input_path.name, error_msg)
            # Mark as failed in unified tracker
            if self.file_tracker:
                self.file_tracker.fail_processing(input_path, error_msg)
            return False, time.time() - start_time, error_msg
            
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.logger.exception(f"Error processing {input_path.name}")
            self._write_error_output(output_path, input_path.name, error_msg)
            # Mark as failed in unified tracker
            if self.file_tracker:
                self.file_tracker.fail_processing(input_path, error_msg)
            return False, time.time() - start_time, error_msg
    
    def _get_output_path(self, input_basename: str) -> Path:
        """Get output CSV path for input file"""
        stem = input_basename[:-4] if input_basename.lower().endswith(".txt") else input_basename
        return self.output_dir / f"{stem}.csv"
    
    def _read_input_file(self, input_path: Path) -> str:
        """Read and validate input file"""
        try:
            with open(input_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            return content
        except Exception as e:
            raise ParseError(str(input_path), f"Failed to read file: {e}")
    
    def _process_content(self, content: str, filename: str) -> List[Dict[str, Any]]:
        """Process content through MetaMap"""
        # Set environment for MetaMap options
        if self.metamap_options:
            os.environ["METAMAP_PROCESSING_OPTIONS"] = self._deduplicate_options(self.metamap_options)
        
        # Use provided instance or create new one
        if self.metamap_instance:
            # Use provided instance (assumes it was created with correct ports)
            mm = self.metamap_instance
            try:
                # Parse content
                mmos = mm.parse([content], timeout=self.timeout)
                
                if not mmos:
                    self.logger.warning(f"No concepts found in {filename}")
                    return []
                
                # Extract concepts
                concepts = []
                for mmo in mmos:
                    for concept in mmo:
                        concepts.append(self._extract_concept_data(concept))
                
                return concepts
                
            except TimeoutError:
                raise MetamapStuck()
            except Exception as e:
                if "connection" in str(e).lower():
                    raise ParseError(filename, f"Server connection error: {e}")
                raise
        else:
            # Create new instance with context manager and custom ports
            with PyMetaMap(self.metamap_binary_path, debug=False, 
                          tagger_port=self.tagger_port, wsd_port=self.wsd_port) as mm:
                try:
                    # Parse content
                    mmos = mm.parse([content], timeout=self.timeout)
                    
                    if not mmos:
                        self.logger.warning(f"No concepts found in {filename}")
                        return []
                    
                    # Extract concepts
                    concepts = []
                    for mmo in mmos:
                        for concept in mmo:
                            concepts.append(self._extract_concept_data(concept))
                    
                    return concepts
                    
                except TimeoutError:
                    raise MetamapStuck()
                except Exception as e:
                    if "connection" in str(e).lower():
                        raise ParseError(filename, f"Server connection error: {e}")
                    raise
    
    def _extract_concept_data(self, concept) -> Dict[str, Any]:
        """Extract concept data into dictionary matching Java API format"""
        # Handle position information - based on mmoparser.py Concept class
        position = ""
        if hasattr(concept, 'pos_start') and hasattr(concept, 'pos_length'):
            if concept.pos_start is not None and concept.pos_length is not None:
                # Convert 1-based to 0-based position for consistency with Java output
                position = f"{concept.pos_start - 1}:{concept.pos_length}"
        
        # Extract matched positions if available
        elif hasattr(concept, 'matchedstart') and hasattr(concept, 'matchedend'):
            if concept.matchedstart and concept.matchedend:
                positions = []
                for start, end in zip(concept.matchedstart, concept.matchedend):
                    length = end - start + 1
                    positions.append(f"{start - 1}:{length}")
                position = ";".join(positions)
        
        # Get phrase text for the phrase column
        phrase_text = getattr(concept, 'phrase_text', '')
        if not phrase_text:
            # Fallback to matched text if phrase_text is empty
            phrase_text = getattr(concept, 'matched', '')
        
        # Extract semantic types properly
        sem_types = getattr(concept, 'semtypes', [])
        if isinstance(sem_types, str):
            # Parse string representation of list
            sem_types = [s.strip().strip("'\"[]") for s in sem_types.split(',') if s.strip()]
        
        return {
            'cui': getattr(concept, 'cui', ''),
            'score': getattr(concept, 'score', ''),
            'concept_name': getattr(concept, 'matched', ''),  # 'matched' is the matched text
            'preferred_name': getattr(concept, 'pref_name', ''),  # This should be populated from XML
            'phrase': phrase_text,  # Use phrase_text with fallback
            'sem_types': self._escape_csv(sem_types),
            'semantic_types': sem_types,  # Keep raw list for tracking
            'sources': self._escape_csv(getattr(concept, 'sources', [])),
            'position': position
        }
    
    def _escape_csv(self, field_data: Any) -> str:
        """Escape CSV field data"""
        if field_data is None:
            return ""
        
        s = str(field_data)
        # Replace newlines with spaces
        s = s.replace("\n", " ").replace("\r", " ")
        
        # Handle list/tuple semtypes
        if isinstance(field_data, (list, tuple)):
            s = ",".join(str(item) for item in field_data)
            
        return s
    
    def _deduplicate_options(self, options: str) -> str:
        """Remove duplicate MetaMap options"""
        seen = set()
        deduped = []
        
        for opt in options.split():
            if opt.startswith('--'):
                key = opt.split('=')[0] if '=' in opt else opt
                if key not in seen:
                    seen.add(key)
                    deduped.append(opt)
            else:
                deduped.append(opt)
                
        return ' '.join(deduped)
    
    def _write_output(self, output_path: Path, filename: str, concepts: List[Dict[str, Any]]):
        """Write concepts to CSV output"""
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            # Write start marker
            f.write(f"{START_MARKER_PREFIX}{filename}\n")
            
            # Write CSV
            writer = csv.writer(f, quoting=csv.QUOTE_ALL, doublequote=True)
            writer.writerow(CSV_HEADER)
            
            for concept in concepts:
                writer.writerow([
                    concept['cui'],
                    concept['score'],
                    concept['concept_name'],
                    concept['preferred_name'],
                    concept['phrase'],
                    concept['sem_types'],
                    concept['sources'],
                    concept['position']
                ])
            
            # Write end marker
            f.write(f"{END_MARKER_PREFIX}{filename}\n")
    
    def _write_empty_output(self, output_path: Path, filename: str):
        """Write output for empty input file"""
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            f.write(f"{START_MARKER_PREFIX}{filename}\n")
            writer = csv.writer(f, quoting=csv.QUOTE_ALL, doublequote=True)
            writer.writerow(CSV_HEADER)
            f.write(f"{END_MARKER_PREFIX}{filename}\n")
    
    def _write_error_output(self, output_path: Path, filename: str, error: str):
        """Write output for processing error"""
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            f.write(f"{START_MARKER_PREFIX}{filename}\n")
            writer = csv.writer(f, quoting=csv.QUOTE_ALL, doublequote=True)
            writer.writerow(CSV_HEADER)
            f.write(f"{END_MARKER_PREFIX}{filename}:ERROR\n")
            f.write(f"# Error: {error}\n")


def check_server_status() -> Dict[str, bool]:
    """Check if MetaMap servers are running"""
    return {
        'tagger': is_port_open('localhost', 1795),
        'wsd': is_port_open('localhost', 5554)
    }

def is_port_open(host: str, port: int) -> bool:
    """Check if a port is open"""
    # Try IPv4 first
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            result = s.connect_ex((host, port))
            if result == 0:
                return True
    except Exception:
        pass
    
    # Try IPv6
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            # Use ::1 for IPv6 localhost
            ipv6_host = '::1' if host in ['localhost', '127.0.0.1'] else host
            result = s.connect_ex((ipv6_host, port))
            return result == 0
    except Exception:
        return False