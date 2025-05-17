#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MIMIC Controller
----------------
Supervises MetaMap processing with validation, progress tracking, and
checkpointing for resume capability. Uses pymetamap for direct Python-based
MetaMap processing.

Usage:
    python3 mimic_controller.py start INPUT_DIR OUTPUT_DIR
    python3 mimic_controller.py resume INPUT_DIR OUTPUT_DIR

Creates/updates OUTPUT_DIR/.mimic_state.json and logs to OUTPUT_DIR/mimic.log
"""
from __future__ import print_function, division # For Python 2.7 compatibility

import json
import os
# import pathlib # Replaced by os.path
import subprocess
import sys
import time
from datetime import datetime # strptime will be used for fromisoformat
import random # Already imported
import logging
import shutil # For temporary directory handling
import tempfile  # Needed for temporary directories in run_java_worker_task
import fnmatch # For rglob_compat
# import select  # Non-blocking stdout polling
import re # For regular expressions

# --- Ensure the local patched version of pymm is imported ---
project_root = os.path.dirname(__file__)
# Adjust path to local pymm package (src/pymm)
pymm_src_path = os.path.join(project_root, "src")
if os.path.isdir(os.path.join(pymm_src_path, "pymm")) and pymm_src_path not in sys.path:
    sys.path.insert(0, pymm_src_path)
    # Optional debug print: which pymm will be imported
    # print("Using local pymm from", pymm_src_path)

try:
    from pymm import Metamap as MetaMap
except ImportError:
    print("ERROR: pymm library not found. Please install it (e.g., pip install pymm)")
    sys.exit(1)

from concurrent.futures import ProcessPoolExecutor, as_completed
import csv # For writing CSV files correctly

STATE_FILE = ".mimic_state.json"
# Adjusted to be specific for the output directory it's in
# LOG_FILE = "mimic.log" # This will be set dynamically based on output dir name
PID_FILE = ".mimic_pid"
CHECK_INTERVAL = 15  # Progress update interval.
SAMPLE_VALIDATION_LINES = 10
MIN_COLUMNS = 5
# MAX_PARALLEL_WORKERS = 1 # User was testing with 1
# Allow tuning from the environment so shell scripts can keep mmserver20 and controller in sync
MAX_PARALLEL_WORKERS = int(os.getenv("MAX_PARALLEL_WORKERS", "100"))
# WSL path to MetaMap executable provided by user
# METAMAP_BINARY_PATH = "/mnt/c/Users/Administrator/Pictures/MetamapWsl/metamap_install/public_mm/bin/metamap20"
METAMAP_BINARY_PATH = os.getenv("METAMAP_BINARY_PATH")
if METAMAP_BINARY_PATH is None:
    # print("ERROR: METAMAP_BINARY_PATH environment variable not set.") # Replaced by logging
    # sys.exit(1) # Exit will be handled by logging critical error and potential sys.exit in main() if needed
    pass # Logging will occur in main() if this is an issue

# Determine MetaMap server ports list (comma-separated env variable set by metamap_cli.sh)
MM_SERVER_PORTS_ENV = os.getenv("MM_SERVER_PORTS", "8066")
MM_SERVER_PORTS = [int(p.strip()) for p in MM_SERVER_PORTS_ENV.split(",") if p.strip()]
if not MM_SERVER_PORTS:
    MM_SERVER_PORTS = [8066]
# Consider the number of server threads set for mmserver20 (e.g., metamap_cli.sh suggests 4)

# ----- Batch-Runner compatibility constants -----
ERROR_FOLDER = "error_files"                         # mirrors BatchRunner01.ERROR_FOLDER
CHECKPOINT_BATCH_SIZE = int(os.getenv("CHECKPOINT_BATCH_SIZE", "100"))  # save state every N files
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))          # mirrors BatchRunner01.MAX_RETRY_ATTEMPTS

# Default MetaMap options string
METAMAP_PROCESSING_OPTIONS = os.getenv("METAMAP_OPTIONS", "-y -Z 2020AA --lexicon db")

# Expected markers
START_MARKER_PREFIX = "META_BATCH_START_NOTE_ID:"
END_MARKER_PREFIX = "META_BATCH_END_NOTE_ID:"

# Prefix of the CSV header line written by BatchRunner01
CSV_HEADER = ["CUI","Score","ConceptName","PrefName","Phrase","SemTypes","Sources","Positions"]
CSV_HEADER_PREFIX = CSV_HEADER[0] + "," + CSV_HEADER[1] + "," + CSV_HEADER[2] # Used to detect files still in progress

# Helper for os.path based rglob
def rglob_compat(directory, pattern):
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                filename = os.path.join(root, basename)
                yield filename

def load_state(out_dir): # out_dir is now a string path
    state_path = os.path.join(out_dir, STATE_FILE)
    if os.path.exists(state_path):
        with open(state_path, 'r') as f:
            return json.load(f)
    return None

def save_state(out_dir, state): # out_dir is now a string path
    state_path = os.path.join(out_dir, STATE_FILE)
    with open(state_path, 'w') as f:
        json.dump(state, f, indent=2)

def gather_inputs(inp_dir): # inp_dir is now a string path
    # return sorted([p for p in inp_dir.rglob("*.txt")]) # Pathlib version
    return sorted([p for p in rglob_compat(inp_dir, "*.txt")])

def validate_csv(csv_file): # csv_file is now a string path
    try:
        with open(csv_file, 'r') as f:
            line = f.readline()
            if line.count(",") >= MIN_COLUMNS - 1:
                return True
    except Exception:
        return False
    return False

def check_output_file_completion(output_csv_path, input_file_original_stem_for_marker):
    """Checks if an output CSV is fully processed based on start/end markers."""
    # output_csv_path is now a string path
    if not os.path.exists(output_csv_path) or os.stat(output_csv_path).st_size == 0:
        return False

    try:
        first_line_str = ""
        last_line_str = ""
        # Open in binary mode ('rb') for reliable seeking from SEEK_END
        with open(output_csv_path, 'rb') as f_bin:
            # Read first line
            first_line_bytes = f_bin.readline()
            first_line_str = first_line_bytes.decode('utf-8', 'replace').strip()

            # Efficiently get the last line using binary mode seeking
            f_bin.seek(0, os.SEEK_END)
            current_pos = f_bin.tell()

            if current_pos == 0: # Empty file
                 if not first_line_str: # Truly empty
                    logging.warning("File is empty (binary check): {path}".format(path=output_csv_path))
                    return False
                 # If first_line_str has content, it means file only had that one line
                 last_line_str = first_line_str
            elif current_pos == len(first_line_bytes): # File contains only the first line read
                last_line_str = first_line_str
            else:
                buffer_size = 1024 
                if current_pos < buffer_size:
                    buffer_size = current_pos
                
                # Seek back from the end. Ensure we don't seek past the beginning if buffer_size is too large for small files after first line.
                # The actual seek position must be >= 0.
                # We need to find the last newline.
                f_bin.seek(-buffer_size, os.SEEK_END)
                buffer_content_bytes = f_bin.read(buffer_size)
                
                lines_in_buffer = buffer_content_bytes.strip().splitlines()  # works on bytes, too
                if lines_in_buffer:
                    last_line_str = lines_in_buffer[-1].decode('utf-8', 'replace').strip()
                elif first_line_str:
                    logging.warning("Could not determine last line via buffer (binary mode) for {path}. First line was: '{fl}'".format(path=output_csv_path, fl=first_line_str))

        # Secondary fallback – scan the last 64 KB of the file if we still couldn't
        # decide on the logical last line.  This is cheap I/O and prevents false
        # negatives on very long final lines or files without a terminal newline.
        if not last_line_str:
            try:
                with open(output_csv_path, 'rb') as fh_tail:
                    fh_tail.seek(0, os.SEEK_END)
                    size = fh_tail.tell()
                    fh_tail.seek(max(size - 65536, 0))  # 64 KB
                    tail_bytes = fh_tail.read()
                    if tail_bytes:
                        last_line_str = tail_bytes.decode('utf-8', 'replace').strip().splitlines()[-1]
            except Exception:
                # Any issue here simply means we fall back to marker mismatch logic
                pass

        # Markers in the file content should refer to the original .txt filename for consistency with BatchRunner01's potential logging
        expected_start_marker = "{prefix}{stem}".format(prefix=START_MARKER_PREFIX, stem=input_file_original_stem_for_marker)
        expected_end_marker = "{prefix}{stem}".format(prefix=END_MARKER_PREFIX, stem=input_file_original_stem_for_marker)
        expected_end_marker_error = expected_end_marker + ":ERROR"
        
        # Early exit: if the last line is still the CSV header, the file is in progress – do not log a mismatch.
        # CSV_HEADER_PREFIX is a string. last_line_str could be the actual header array if not joined.
        # Ensure CSV_HEADER is a string for startswith if last_line_str is also a string.
        header_as_string = ",".join(CSV_HEADER) # Make sure we are comparing string with string
        if last_line_str.startswith(header_as_string):
            return False

        # For the first line, it should *be* the start marker, not just start with it, after stripping.
        # The issue in the log: Found: 'META_BATCH_START_NOTE_ID:K_0001_1549757_7224370079797268830.txt\n"CUI",...'
        # This implies first_line_str after .strip() was not just the marker. 
        # This can happen if readline() read more than one \n due to file encoding/line ending issues, 
        # or if the file literally had the header on the same line as the marker (unlikely).
        # Let's assume first_line_str, after decode & strip, IS the first logical line.

        start_marker_found = (first_line_str == expected_start_marker)
        end_marker_found = (last_line_str == expected_end_marker or last_line_str == expected_end_marker_error)

        if start_marker_found and end_marker_found:
            if last_line_str == expected_end_marker_error:
                 logging.warning("File processed with error marker: {path}".format(path=output_csv_path))
                 # Treat files ending with ERROR marker as INCOMPLETE so they will be retried.
                 return False
            return True
        else:
            logging.info(
                "Marker mismatch for {path}. Expected Start: '{exp_start}' (based on original stem '{marker_stem}'), Found: '{f_start}'. Expected End: '{exp_end}' or '{exp_end_err}', Found: '{f_end}'.".format(
                    path=output_csv_path, 
                    exp_start=expected_start_marker, 
                    marker_stem=input_file_original_stem_for_marker,
                    f_start=first_line_str, 
                    exp_end=expected_end_marker, 
                    exp_end_err=expected_end_marker_error,
                    f_end=last_line_str
                )
            )
        return False
    except Exception as e:
        # Log the full traceback for unexpected errors during file check
        logging.exception("Unhandled error during check_output_file_completion for {path}: {error}".format(path=output_csv_path, error=e))
        return False
    return False # Should be unreachable if logic above is complete

# Determine if CSV has real content (not just header)
def is_valid_csv(path): # path is now a string path
    try:
        if os.stat(path).st_size < 50: # Adjusted minimum size for markers + header. Still a heuristic.
            return False
        with open(path, 'r') as f:
            lines_read = 0
            for _ in f:
                lines_read += 1
                if lines_read >= 2: # Check for at least 2 lines (start_marker, header)
                    break 
            return lines_read >= 2
    except Exception: # Catches StopIteration if file is too short, and other read errors
        return False
    # Removed the final 'return True' as the logic is self-contained above.
    # If try completes and lines_read >=2, it would have returned True.
    # If try fails or lines_read < 2, it returns False.
    # This ensures a boolean is always returned from this path too.
    return False

# Helper to determine the dynamic log file name based on output directory
def get_dynamic_log_filename(base_output_dir):
    parent_dir_name_of_out = os.path.basename(os.path.dirname(base_output_dir))
    if "kidney" in parent_dir_name_of_out.lower():
        return "kidney_cont2roller.log"
    elif "mimic" in parent_dir_name_of_out.lower():
        return "mimic_controller.log"
    else:
        return "generic_controller.log"

# Helper to parse ISO format datetime strings for Python 2.7
def parse_iso_datetime_compat(dt_str):
    if not dt_str:
        return None
    # Strip microseconds if present, as strptime %f is tricky across Py2/3 for this specific format part
    return datetime.strptime(dt_str.split('.')[0], "%Y-%m-%dT%H:%M:%S")

def run_java_worker_task(worker_id, files_for_worker, main_out_dir, classpath, metamap_options_str):
    if os.getenv("MM_NO_COPY", "0") == "1":
        # Use original directory directly (faster, but input files must be readable by Java worker)
        worker_temp_inp_dir = os.path.dirname(files_for_worker[0]) if len(set(os.path.dirname(p) for p in files_for_worker)) == 1 else None
        if not worker_temp_inp_dir:
            # Fall back to copy if files spread across dirs
            worker_temp_inp_dir = tempfile.mkdtemp(prefix="metamap_worker_{}_".format(worker_id))
            for p in files_for_worker:
                shutil.copy(p, os.path.join(worker_temp_inp_dir, os.path.basename(p)))
        logging.info("[Worker-{}] Using in-place input dir: {}".format(worker_id, worker_temp_inp_dir))
    else:
        worker_temp_inp_dir = tempfile.mkdtemp(prefix="metamap_worker_{}_".format(worker_id))
        logging.info("[Worker-{}] Created temp input dir: {}".format(worker_id, worker_temp_inp_dir))
        for f_path_to_copy in files_for_worker:
            shutil.copy(f_path_to_copy, os.path.join(worker_temp_inp_dir, os.path.basename(f_path_to_copy)))
        logging.info("[Worker-{}] Copied {} files to {}".format(worker_id, len(files_for_worker), worker_temp_inp_dir))

    # Add MetaMap options as the third argument if provided
    java_cmd = ["java", "-cp", classpath, "demo.metamaprunner2020.BatchRunner01", worker_temp_inp_dir, main_out_dir]
    if metamap_options_str and metamap_options_str.strip():
        java_cmd.append(metamap_options_str)
        
    logging.info("[Worker-{}] Running command: {}".format(worker_id, " ".join(java_cmd)))
    
    # Assign server port to this worker (round-robin)
    chosen_port = MM_SERVER_PORTS[(int(worker_id.strip("W")) - 1) % len(MM_SERVER_PORTS)]
    env_for_worker = os.environ.copy()
    env_for_worker["MMSERVER_PORT"] = str(chosen_port)

    # Make sure stdout and stderr are captured
    proc = subprocess.Popen(java_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env_for_worker)
    return worker_id, proc, worker_temp_inp_dir

# Helper to derive output CSV path given the original .txt input filename (basename only)
# New convention: for input note 'note123.txt' --> 'note123.csv' in output dir.
# Older convention (now deprecated) appended an extra '.txt' producing 'note123.txt.csv'.
# To remain backward-compatible we accept both forms when checking completion.

def derive_output_csv_path(out_dir, input_basename):
    """Return full path to the CSV that should correspond to the given input .txt file.
    Handles both the current (<stem>.csv) and legacy (<stem>.txt.csv) conventions,
    preferring the current one when both exist.
    """
    stem = input_basename[:-4] if input_basename.lower().endswith(".txt") else input_basename
    new_path = os.path.join(out_dir, stem + ".csv")
    legacy_path = os.path.join(out_dir, input_basename + ".csv")  # legacy has .txt inside
    # Prefer new path if it exists, else legacy path, else return new path (for writing).
    if os.path.exists(new_path):
        return new_path
    if os.path.exists(legacy_path):
        return legacy_path
    return new_path

# Helper to recover the original input filename (with .txt) from an output CSV basename.
# Works for both new and legacy output naming.

def input_filename_from_csv_basename(csv_basename):
    if csv_basename.lower().endswith(".txt.csv"):
        return csv_basename[:-4]  # strip only .csv, still includes .txt
    elif csv_basename.lower().endswith(".csv"):
        return csv_basename[:-4] + ".txt"
    else:
        return csv_basename  # fallback (should not occur)

# Python worker function using pymm
def pymm_escape_csv_field(field_data):
    """Replicates Java's escaping for CSV fields."""
    if field_data is None:
        return "" # Return empty string for None, will be quoted by csv.writer if needed
    # Convert to string if not already
    s = str(field_data)
    # Replace newlines with spaces
    s = s.replace("\\n", " ").replace("\\r", " ")
    # CSV writer will handle quoting and escaping internal quotes if configured with doublequote=True
    return s

def process_files_with_pymm_worker(worker_id, files_for_worker, main_out_dir, metamap_options, metamap_binary_path, mm_server_port):
    logging.info("[{}] Starting. Processing {} files. Port: {}. Options: '{}'".format(
        worker_id, len(files_for_worker), mm_server_port, metamap_options))
    
    results = [] # List of (input_filename, duration_ms_str, error_occurred_bool)

    try:
        # Initialize MetaMap instance for this worker
        # pymm 0.2 does not accept 'options' here.
        # MetaMap options must be configured in the MetaMap installation itself.
        # Set MMSERVER_PORT environment variable for pymm's subprocess
        original_env_port = os.environ.get('MMSERVER_PORT')
        os.environ['MMSERVER_PORT'] = str(mm_server_port)
        logging.info(f"[{worker_id}] Set MMSERVER_PORT to {mm_server_port} for pymm instance.")

        mm = MetaMap(metamap_binary_path, debug=True)
         
    except Exception as e:
        logging.error("[{}] Failed to initialize MetaMap: {}".format(worker_id, e))
        # Mark all files for this worker as errored
        for f_path in files_for_worker:
            results.append((os.path.basename(f_path), "0ms", True))
        # Restore MMSERVER_PORT if it was changed during the try block
        if 'original_env_port' in locals(): # Check if it was set before an error
            if original_env_port is None:
                os.environ.pop('MMSERVER_PORT', None)
            else:
                os.environ['MMSERVER_PORT'] = original_env_port
        return results

    for input_file_path_str in files_for_worker:
        input_file_basename = os.path.basename(input_file_path_str)
        output_csv_path = derive_output_csv_path(main_out_dir, input_file_basename)
        processing_error_occurred = False
        duration_ms = 0

        logging.info("[{}] Processing: {} -> {}".format(worker_id, input_file_basename, output_csv_path))
        
        start_time = time.time()
        try:
            with open(input_file_path_str, 'r', encoding='utf-8') as f_in:
                # --- Java parity ---
                # BatchRunner01 feeds the ENTIRE note to MetaMap as one citation.
                # To reproduce that behaviour we send a single-element list that
                # contains the full file text (stripped of leading/trailing ws).
                whole_note = f_in.read().strip()
                if whole_note:
                    lines = [whole_note]
                else:
                    lines = []
            
            if not lines:
                logging.warning("[{}] Input file {} is empty. Skipping.".format(worker_id, input_file_basename))
                results.append((input_file_basename, "0ms", False)) # Not an error, but no concepts
                # Create an empty output file with markers for consistency
                with open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:
                    writer = csv.writer(f_out, quoting=csv.QUOTE_ALL, doublequote=True)
                    f_out.write(START_MARKER_PREFIX + input_file_basename + "\n")
                    writer.writerow(CSV_HEADER)
                    # No concepts
                # Add end marker in finally block
                continue # to next file

            # Generate IDs (1-based) – still required by pymm even with single citation
            sent_ids = [str(i+1) for i in range(len(lines))]
            
            # Extract concepts using pymm with extended timeout
            pymm_timeout = int(os.getenv("PYMM_TIMEOUT", "120"))  # seconds
            # The default in pymm is 10 seconds which may be too short for longer notes.
            mmos_iter = mm.parse(lines, timeout=pymm_timeout)
            concepts_list = []
            for mmo in mmos_iter:
                concepts_list.extend(list(mmo))

            error = None  # pymm raises exceptions rather than returning error string

            # Write to CSV
            with open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:
                # Write start marker
                f_out.write(START_MARKER_PREFIX + input_file_basename + "\n")
                
                writer = csv.writer(f_out, quoting=csv.QUOTE_ALL, doublequote=True)
                writer.writerow(CSV_HEADER)

                # Group concepts by utterance ID if we have any
                if concepts_list:
                    concepts_by_utterance = {}
                    for idx, c in enumerate(concepts_list):
                        # Only include mapping concepts (like Java BatchRunner)
                        if not c.ismapping:
                            continue
                        
                        # Get utterance ID (or use a position-based fallback)
                        utterance_id = getattr(c, 'utterance_id', None)
                        if utterance_id is None:
                            # If no utterance ID, try to infer one based on position
                            pos = None
                            for attr in ('phrase_start', 'pos_start'):
                                val = getattr(c, attr, None)
                                if isinstance(val, int) and val > 0:
                                    pos = val
                                    break
                            if pos is None and getattr(c, 'matchedstart', None):
                                pos = min(c.matchedstart)
                            
                            # Group by position blocks if no utterance ID
                            if pos is not None:
                                # Rough heuristic: divide position by 500 to group roughly by paragraph/major sections
                                utterance_id = pos // 500
                            else:
                                # Last resort - preserve original order
                                utterance_id = 999999
                        
                        # Save the concept with its original position for stable sort
                        if utterance_id not in concepts_by_utterance:
                            concepts_by_utterance[utterance_id] = []
                        concepts_by_utterance[utterance_id].append((idx, c))
                    
                    # Process utterances in order (like Java)
                    concepts_to_write = []
                    for utterance_id in sorted(concepts_by_utterance.keys()):
                        # Within each utterance, preserve the original order from MetaMap XML
                        # This mimics Java's processing which keeps the exact order from MetaMap
                        concepts_in_utterance = concepts_by_utterance[utterance_id]
                        concepts_in_utterance.sort(key=lambda pair: pair[0])  # sort by original index
                        concepts_to_write.extend(pair[1] for pair in concepts_in_utterance)
                    
                    if concepts_to_write:
                        logging.info(f"[{worker_id}] Processed {len(concepts_to_write)} concepts in {len(concepts_by_utterance)} utterances.")
                    
                    # Build utterance text segments for utterance-relative indexing
                    utterance_texts = {}
                    # Option 1: Use paragraphs (split by double newlines)
                    utterance_splits = re.split(r'\n\s*\n', whole_note)
                    for uidx, utt in enumerate(utterance_splits):
                        # Utterance IDs are 1-based
                        utterance_texts[uidx+1] = utt
                    
                    # Option 2: Also support single-line utterances for simpler texts
                    if len(utterance_splits) <= 1:
                        line_splits = whole_note.splitlines()
                        for lidx, line in enumerate(line_splits):
                            utterance_texts[lidx+1] = line

                    # --- NEW: derive per-utterance starting offset from XML pos_start values ---
                    utterance_offset_map = {}
                    for c_tmp in concepts_list:
                        uid_tmp = getattr(c_tmp, 'utterance_id', None)
                        pstart_tmp = getattr(c_tmp, 'pos_start', None)
                        if uid_tmp is not None and pstart_tmp is not None:
                            if uid_tmp not in utterance_offset_map or pstart_tmp < utterance_offset_map[uid_tmp]:
                                utterance_offset_map[uid_tmp] = pstart_tmp
                    # If we somehow have no offsets, nothing breaks – we just fall back to other logic.

                else:
                    concepts_to_write = []

                for concept in concepts_to_write:
                    try:
                        # ---- Column extraction mirroring Java ----
                        concept_name  = getattr(concept, 'concept_name',
                            getattr(concept, 'preferred_name', concept.matched))
                        pref_name     = getattr(concept, 'preferred_name', concept_name)
                        phrase_text   = getattr(concept, 'phrase_text', None) or getattr(concept, 'phrase', None) or getattr(concept, 'matched', '')

                        sem_types_formatted = ":".join(concept.semtypes) if getattr(concept, 'semtypes', None) else ""

                        # ---- Sources ----
                        # Use the parsed sources field from XML
                        sources_formatted = "|".join(concept.sources) if getattr(concept, 'sources', None) else ""

                        # ---- Positions ----
                        # Priority 1: Use position directly from XML if available (most like Java)
                        positions_formatted = ""
                        if hasattr(concept, 'pos_start') and concept.pos_start is not None:
                            try:
                                uid_cur = getattr(concept, 'utterance_id', None)
                                if uid_cur is not None and uid_cur in utterance_offset_map:
                                    rel_start = concept.pos_start - utterance_offset_map[uid_cur] + 1
                                    positions_formatted = f"{rel_start}:{concept.pos_length if concept.pos_length else len(phrase_text)}"
                                else:
                                    # Fall back to raw pos_start (document level) if we don't have an offset
                                    positions_formatted = f"{concept.pos_start}:{concept.pos_length if concept.pos_length else len(phrase_text)}"
                            except Exception as e_pos:
                                logging.debug(f"[{worker_id}] Error using direct position info: {e_pos}")
                                positions_formatted = ""
                        
                        # Priority 2: Utterance-local search if direct position not available
                        if not positions_formatted:
                            # Get utterance text for this concept
                            utt_text = utterance_texts.get(getattr(concept, 'utterance_id', None), whole_note)
                            
                            # Search within the utterance text
                            idx = utt_text.find(phrase_text)
                            if idx >= 0:
                                # Apply CR adjustment relative to the utterance text
                                cr_adjust = utt_text.count('\n', 0, idx)
                                java_idx = idx + cr_adjust
                                positions_formatted = f"{java_idx+1}:{len(phrase_text)}"
                            else:
                                # Basic fallback: try a normalized search within utt_text
                                try:
                                    normalized_phrase_to_find = re.sub(r'\s+', ' ', phrase_text).strip().lower()
                                    # Search in a similarly normalized version of utt_text 
                                    normalized_utt_text_lower = re.sub(r'\s+', ' ', utt_text).strip().lower()
                                    idx_norm = normalized_utt_text_lower.find(normalized_phrase_to_find)

                                    if idx_norm >= 0:
                                        # Try to map back to original text for CR counting
                                        temp_idx = utt_text.lower().find(normalized_phrase_to_find)
                                        if temp_idx != -1:
                                            cr_adjust = utt_text.count('\n', 0, temp_idx)
                                            java_idx = temp_idx + cr_adjust
                                            positions_formatted = f"{java_idx+1}:{len(phrase_text)}"
                                        else:
                                            # Use normalized position if can't map back
                                            cr_adjust = utt_text.count('\n', 0, idx_norm) 
                                            java_idx = idx_norm + cr_adjust
                                            positions_formatted = f"{java_idx+1}:{len(phrase_text)}"
                                except Exception:
                                    positions_formatted = "" # Error during fallback
                        
                        row_data = [
                            concept.cui,
                            concept.score,
                            concept_name,
                            pref_name,
                            phrase_text,
                            sem_types_formatted,
                            sources_formatted,
                            positions_formatted
                        ]
                        # Apply escaping similar to Java's esc function for each field before writerow
                        escaped_row_data = [pymm_escape_csv_field(field) for field in row_data]
                        writer.writerow(escaped_row_data)
                    except Exception as e_concept:
                        logging.error("[{}] Error processing concept for {}: {} - Concept: {}".format(
                            worker_id, input_file_basename, e_concept, concept))
                        processing_error_occurred = True # Mark error if individual concept fails

                if not concepts_list and not error:
                    logging.info("[{}] No concepts found by pymm for {}".format(worker_id, input_file_basename))
                elif error:
                    logging.warning("[{}] Pymm processing failed for {} (error flag set, no concepts)".format(worker_id, input_file_basename))
                    processing_error_occurred = True

        except Exception as e_file:
            logging.error("[{}] Unhandled error processing file {} or writing to {}: {}".format(
                worker_id, input_file_path_str, output_csv_path, e_file))
            logging.exception("Traceback for file processing error:") # Log full traceback
            processing_error_occurred = True
        finally:
            end_time = time.time()
            duration_ms = int((end_time - start_time) * 1000)
            
            # Always write end marker, even if file processing failed or output_csv_path wasn't fully written to.
            # If output_csv_path was never opened (e.g. error before that), this might fail.
            try:
                with open(output_csv_path, 'a', encoding='utf-8') as f_out_end: # Append mode
                    end_marker = END_MARKER_PREFIX + input_file_basename + (":ERROR" if processing_error_occurred else "")
                    f_out_end.write(end_marker + "\n")
                logging.info("[{}] Wrote END_MARKER for {} (Error: {})".format(worker_id, input_file_basename, processing_error_occurred))
            except Exception as e_marker:
                 logging.error("[{}] Failed to write END_MARKER for {}: {}".format(worker_id, input_file_basename, e_marker))
                 # If end marker fails, check_output_file_completion will likely fail for this file.
                 processing_error_occurred = True # Ensure this file is re-processed if end marker fails.

            results.append((input_file_basename, "{}ms".format(duration_ms), processing_error_occurred))
            logging.info("[{}] TIME_LOG_PYTHON:{}:{}:{}:Port{}".format(worker_id, input_file_basename, duration_ms, "ERROR" if processing_error_occurred else "OK", mm_server_port))
            
    logging.info("[{}] Finished processing batch of {} files.".format(worker_id, len(files_for_worker)))
    # Restore MMSERVER_PORT environment variable at the end of the worker function, 
    # if it was successfully set at the beginning of the function.
    if 'original_env_port' in locals(): 
        if original_env_port is None:
            os.environ.pop('MMSERVER_PORT', None)
            logging.info(f"[{worker_id}] Cleared MMSERVER_PORT.")
        else:
            os.environ['MMSERVER_PORT'] = original_env_port
            logging.info(f"[{worker_id}] Restored MMSERVER_PORT to {original_env_port}.")
    return results

def main():
    if len(sys.argv) < 3:
        print("Usage: mimic_controller.py <subcmd> ...\\n"
              "Subcommands:\\n"
              "  start  INPUT_DIR OUTPUT_DIR\\n"
              "  resume INPUT_DIR OUTPUT_DIR\\n"
              "  validate INPUT_DIR\\n"
              "  progress OUTPUT_DIR\\n"
              "  tail OUTPUT_DIR [LINES]\\n"
              "  pending INPUT_DIR OUTPUT_DIR\\n"
              "  completed OUTPUT_DIR\\n"
              "  clearout OUTPUT_DIR   # Delete all *.csv outputs (keeps logs, state)\\n"
              "  sample OUTPUT_DIR [N]" )
        sys.exit(1)

    subcmd = sys.argv[1]

    if subcmd in {"start", "resume"}:
        if len(sys.argv) != 4:
            print(f"Error: {subcmd} command requires INPUT_DIR and OUTPUT_DIR arguments.")
            print("Usage: mimic_controller.py {} INPUT_DIR OUTPUT_DIR".format(subcmd))
            sys.exit(1)
        # mode = subcmd # mode is already subcmd
        # inp_dir_orig_str, out_dir_str = sys.argv[2:4] # This will be handled later
    elif subcmd == "validate" and len(sys.argv) == 3:
        inp = os.path.abspath(os.path.expanduser(sys.argv[2]))
        files = gather_inputs(inp)
        bad_enc = []
        total_bytes = 0
        for f_path_str in files:
            try:
                with open(f_path_str, 'r') as f_obj: # Removed encoding for Py2 check, relies on locale
                    data = f_obj.read()
            except UnicodeDecodeError:
                bad_enc.append(f_path_str)
                continue
            total_bytes += len(data.encode('utf-8') if isinstance(data, str) else data) # Handle Py2 unicode/str -> Py3 str
        print("Total files: {total}  Bad encoding: {bad_len}  Size: {size:.2f} MB".format(
            total=len(files), bad_len=len(bad_enc), size=total_bytes/1e6))
        if bad_enc:
            print("Files with encoding issues:")
            for f_path_str in bad_enc[:20]:
                print("  ", f_path_str)
            if len(bad_enc) > 20:
                print("  ...")
        sys.exit(0)
    elif subcmd == "pid" and len(sys.argv)==3:
        out = os.path.abspath(os.path.expanduser(sys.argv[2]))
        pid_path = os.path.join(out, PID_FILE)
        if os.path.exists(pid_path):
            with open(pid_path, 'r') as f:
                print(f.read().strip())
        else:
            print("PID file not found")
            sys.exit(1)
        sys.exit(0)
    elif subcmd == "kill" and len(sys.argv)==3:
        out = os.path.abspath(os.path.expanduser(sys.argv[2]))
        pid_path = os.path.join(out, PID_FILE)
        if not os.path.exists(pid_path):
            print("PID file not found")
            sys.exit(1)
        with open(pid_path, 'r') as f:
            pid=int(f.read())
        try:
            os.kill(pid, 15)
            print("Sent SIGTERM to {pid_val}".format(pid_val=pid))
        except ProcessLookupError: # Python 3 specific, OSError in Py2
            print("Process already not running or OS error")
        except OSError: # Catch for Python 2
            print("Process already not running or OS error (OSError)")
        sys.exit(0)
    elif subcmd == "progress" and len(sys.argv) == 3:
        out = os.path.abspath(os.path.expanduser(sys.argv[2]))
        state = load_state(out)
        if not state:
            print("No state file present.")
            sys.exit(1)
        
        total_overall = state.get("total_overall", 0)
        completed_overall_markers = state.get("completed_overall_markers", 0)
        
        pct = (completed_overall_markers / total_overall * 100.0) if total_overall else 0.0 # Ensure float division
        print("Progress: {comp}/{total} ({pct:.1f}% completed based on markers)".format(
            comp=completed_overall_markers, total=total_overall, pct=pct))
        
        # Display active workers/files
        active_workers_info = state.get("active_workers_info")
        if active_workers_info and isinstance(active_workers_info, dict) and any(active_workers_info.values()):
            active_files_display = []
            for worker_id, info in active_workers_info.items():
                if isinstance(info, dict) and info.get("current_file"):
                    active_files_display.append("{}:{}".format(worker_id, info["current_file"]))
            if active_files_display:
                 print("  Active files (from state): {}".format(", ".join(active_files_display)))
            # else: # This case means active_workers_info exists but might be empty dicts or no current_file
                 # print("  Active workers info present in state, but no specific files listed as current.")
        elif not state.get("end_time"): # No active_workers_info and batch not ended
            print("  No active file information in current state (or batch just started).")


        if "start_time" in state:
            start_dt = parse_iso_datetime_compat(state["start_time"])
            end_ts_str = state.get("end_time")

            if end_ts_str: # Batch is marked as ended
                end_dt = parse_iso_datetime_compat(end_ts_str)
                pre_calculated_elapsed = state.get("elapsed_seconds")

                if pre_calculated_elapsed is not None:
                    try:
                        elapsed = int(pre_calculated_elapsed)
                        print("Total elapsed time: {min} min {sec} sec (Batch completed)".format(min=elapsed//60, sec=elapsed%60))
                    except ValueError:
                        print("Total elapsed time: Invalid pre-calculated elapsed_seconds in state. (Batch completed)")
                elif start_dt and end_dt:
                    if end_dt >= start_dt:
                        elapsed = int((end_dt - start_dt).total_seconds())
                        print("Total elapsed time: {min} min {sec} sec (Batch completed)".format(min=elapsed//60, sec=elapsed%60))
                    else: # end_dt < start_dt
                        print("Total elapsed time: Inconsistent (end time before start time). Batch marked as completed.")
                elif not start_dt:
                    print("Total elapsed time: Could not determine (start time missing/invalid). Batch marked as completed.")
                else: # not end_dt (but end_ts_str existed, so parse failed for end_dt)
                    print("Total elapsed time: Could not determine (end time invalid). Batch marked as completed.")

            else: # Batch is ongoing
                if start_dt:
                    now = datetime.now()
                    if now >= start_dt:
                        elapsed = int((now - start_dt).total_seconds())
                        print("Elapsed time so far: {min} min {sec} sec".format(min=elapsed//60, sec=elapsed%60))
                    else: # System clock might have jumped back or start_time is in future
                        print("Elapsed time so far: Start time is in the future. Check system clock or state file.")
                else:
                    print("Elapsed time so far: Could not determine (start time missing/invalid in state).")
        else: # No start_time in state
             print("Elapsed time: Not available (no start time in state).")

        file_timings = state.get("file_timings", {})
        if file_timings:
            # Python 2.7 dicts are unordered. To get "last", we need more robust logic, 
            # e.g. if file_timings stores (timestamp, duration) or if filenames imply order.
            # For simplicity, just show one example if any, or average.
            # last_timed_file, last_duration = list(file_timings.items())[-1] # Unreliable in Py2
            # Workaround: pick one entry (not guaranteed to be last)
            example_file = next(iter(file_timings.keys()), None)
            if example_file:
                 print("  Example file time ({}): {}".format(example_file, file_timings[example_file]))
            
            total_time_ms = 0
            num_timed_files = 0
            for duration_str in file_timings.values():
                try:
                    total_time_ms += int(duration_str.replace("ms", ""))
                    num_timed_files += 1
                except ValueError:
                    pass 
            
            if num_timed_files > 0:
                avg_time_ms = total_time_ms / num_timed_files # Float division due to __future__ import
                print("  Average time per file: {avg_ms:.0f} ms ({avg_s:.2f} s)".format(
                    avg_ms=avg_time_ms, avg_s=avg_time_ms/1000.0))
                
                remaining_files = total_overall - completed_overall_markers
                if remaining_files > 0 and not end_ts_str:
                    etr_seconds = (avg_time_ms / 1000.0) * remaining_files
                    etr_min = int(etr_seconds // 60)
                    etr_sec = int(etr_seconds % 60)
                    print("  Estimated time remaining (ETR): {min} min {sec} sec for {rem} files".format(
                        min=etr_min, sec=etr_sec, rem=remaining_files))
            else:
                print("  No file timing data available to calculate average or ETR.")
        else:
            print("  No file timing data recorded yet.")
        sys.exit(0)
    elif subcmd == "tail" and len(sys.argv) in {3,4}:
        out = os.path.abspath(os.path.expanduser(sys.argv[2]))
        lines_to_tail = int(sys.argv[3]) if len(sys.argv) == 4 else 20
        
        # Determine log file name (as in main start/resume section)
        # parent_dir_name = os.path.basename(os.path.dirname(out))
        # if "kidney" in parent_dir_name.lower(): dynamic_log_file = "kidney_controller.log"
        # elif "mimic" in parent_dir_name.lower(): dynamic_log_file = "mimic_controller.log"
        # else: dynamic_log_file = "generic_controller.log"
        # log_file = os.path.join(out, dynamic_log_file)
        log_file_name = get_dynamic_log_filename(out)
        log_file = os.path.join(out, log_file_name)
        
        if not os.path.exists(log_file):
            print("Log file not found at {}".format(log_file))
            sys.exit(1)
        with open(log_file, 'r') as f:
            print("\n".join(f.read().splitlines()[-lines_to_tail:]))
        sys.exit(0)
    elif subcmd == "pending" and len(sys.argv)==4:
        inp, out = sys.argv[2:]
        inp_dir = os.path.abspath(os.path.expanduser(inp)); out_dir = os.path.abspath(os.path.expanduser(out))
        
        all_input_files_str = gather_inputs(inp_dir)
        pending_files = []
        # logging.info("Checking for pending files. Input: {}, Output: {}".format(inp_dir, out_dir))
        print("Checking {num_total} total input files from {input_d} against {output_d}...".format(
            num_total=len(all_input_files_str), input_d=inp_dir, output_d=out_dir))

        for f_in_str in all_input_files_str:
            input_basename = os.path.basename(f_in_str)
            output_csv = derive_output_csv_path(out_dir, input_basename)
            if not check_output_file_completion(output_csv, input_basename):
                pending_files.append(f_in_str)
        
        print("Pending files ({}):".format(len(pending_files)))
        for p_str in pending_files[:30]:
            print("  ", os.path.basename(p_str))
        if len(pending_files)>30:
            print("  ...")
        sys.exit(0)
    elif subcmd == "badcsv" and len(sys.argv)==3: # This uses is_valid_csv, which is a quick check
        out = os.path.abspath(os.path.expanduser(sys.argv[2]))
        # bad = [f for f in out_dir.glob("*.csv") if not is_valid_csv(f)] # Pathlib
        bad = []
        for csv_f_str in rglob_compat(out, "*.csv"):
            if not is_valid_csv(csv_f_str):
                bad.append(csv_f_str)
        print("Header-only / superficially invalid CSVs: {}".format(len(bad)))
        for b_str in bad[:30]:
            print("  ", os.path.basename(b_str))
        if len(bad)>30:
            print("  ...")
        sys.exit(0)
    elif subcmd == "completed" and len(sys.argv)==3:
        out = os.path.abspath(os.path.expanduser(sys.argv[2]))
        output_csv_files_str = list(rglob_compat(out, "*.csv"))
        completed_by_markers = []
        print("Checking {num_csv} .csv files in {out_d} for completion markers...".format(
            num_csv=len(output_csv_files_str), out_d=out))

        for csv_file_str in output_csv_files_str:
            csv_basename = os.path.basename(csv_file_str)
            if not csv_basename.lower().endswith(".csv"):
                continue

            input_filename_stem = input_filename_from_csv_basename(csv_basename)

            if check_output_file_completion(csv_file_str, input_filename_stem):
                completed_by_markers.append(csv_file_str)

        print("Files confirmed complete by markers: {}".format(len(completed_by_markers)))
        for c_str in completed_by_markers[:30]:
            print("  ", os.path.basename(c_str))
        if len(completed_by_markers)>30:
            print("  ...")
        sys.exit(0)
    elif subcmd == "sample" and len(sys.argv) in {3,4}:
        out = os.path.abspath(os.path.expanduser(sys.argv[2]))
        num_sample = int(sys.argv[3]) if len(sys.argv)==4 else 1
        
        all_output_csvs_str = list(rglob_compat(out, "*.csv"))
        confirmed_complete_csvs = []
        for csv_file_str in all_output_csvs_str:
            csv_basename = os.path.basename(csv_file_str)
            if not csv_basename.lower().endswith(".csv"):
                continue

            input_filename_stem = input_filename_from_csv_basename(csv_basename)

            if check_output_file_completion(csv_file_str, input_filename_stem):
                confirmed_complete_csvs.append(csv_file_str)

        if not confirmed_complete_csvs:
            print("No CSV outputs confirmed complete by markers yet.")
            sys.exit(1)
        
        # random.sample requires a list or population, not an int for k if k > population size
        actual_num_to_sample = min(num_sample, len(confirmed_complete_csvs))
        sample_files_str = random.sample(confirmed_complete_csvs, actual_num_to_sample)
        
        for s_file_str in sample_files_str:
            print("===", os.path.basename(s_file_str), "(Confirmed Complete by Markers) ===")
            try:
                with open(s_file_str, 'r') as f:
                    lines_to_show = 5 
                    file_content_for_sample = [line.strip() for line in f.readlines()]
                    
                    for i_line in range(min(len(file_content_for_sample), lines_to_show)):
                        print(file_content_for_sample[i_line])
                    
                    if len(file_content_for_sample) > lines_to_show:
                        if len(file_content_for_sample) > lines_to_show +1 :
                             print("...")
                        print(file_content_for_sample[-1])

            except Exception as e:
                print("Error reading sample file {}: {}".format(os.path.basename(s_file_str), e))
        sys.exit(0)
    elif subcmd == "clearout" and len(sys.argv)==3:
        out_dir_clear = os.path.abspath(os.path.expanduser(sys.argv[2]))
        if not os.path.isdir(out_dir_clear):
            print("Output directory {} does not exist or is not a directory.".format(out_dir_clear))
            sys.exit(1)

        csv_files_to_remove = list(rglob_compat(out_dir_clear, "*.csv"))
        if not csv_files_to_remove:
            print("No *.csv files found in {}. Nothing to clear.".format(out_dir_clear))
            sys.exit(0)

        print("About to delete {} CSV files from {} (logs and .json state files will be preserved).".format(
            len(csv_files_to_remove), out_dir_clear))
        confirm = input("Type 'yes' to confirm: ").strip().lower()
        if confirm != 'yes':
            print("Aborted.")
            sys.exit(1)

        removed = 0
        for csv_path_rm in csv_files_to_remove:
            try:
                os.remove(csv_path_rm)
                removed += 1
            except Exception as rm_ex:
                print("Could not remove {}: {}".format(csv_path_rm, rm_ex))
        print("Removed {} CSV files from {}.".format(removed, out_dir_clear))
        sys.exit(0)
    else:
        print("Invalid arguments. Run without args for usage.")
        sys.exit(1)

    # --- Main start/resume logic for parallel processing ---
    # mode is already set to subcmd if it was start or resume
    if subcmd not in {"start", "resume"}:
        # For other subcommands that might have reached here inadvertently or if flow changes.
        # This block might be redundant if all other subcommands sys.exit() properly.
        # However, if a utility command failed before its own sys.exit(), it might fall through.
        # print(f"Subcommand '{subcmd}' not 'start' or 'resume', exiting main processing logic early.")
        return 
    
    inp_dir_orig_str, out_dir_str = sys.argv[2:4] # Args for start/resume confirmed by checks above
    
    inp_dir_orig = os.path.abspath(os.path.expanduser(inp_dir_orig_str))
    out_dir = os.path.abspath(os.path.expanduser(out_dir_str))

    # Ensure output directory exists
    if not os.path.exists(out_dir):
        try:
            os.makedirs(out_dir, exist_ok=True)
            print(f"Created output directory: {out_dir}") # Temporary print for diagnostics
        except Exception as e_mkdir:
            print(f"Error creating output directory {out_dir}: {e_mkdir}")
            sys.exit(1)

    # parent_dir_name_of_out = os.path.basename(os.path.dirname(out_dir))
    # if "kidney" in parent_dir_name_of_out.lower(): dynamic_log_file = "kidney_controller.log"
    # elif "mimic" in parent_dir_name_of_out.lower(): dynamic_log_file = "mimic_controller.log"
    # else: dynamic_log_file = "generic_controller.log"
    # log_path = os.path.join(out_dir, dynamic_log_file)
    log_file_name = get_dynamic_log_filename(out_dir)
    log_path = os.path.join(out_dir, log_file_name)

    pid_path = os.path.join(out_dir, PID_FILE)
    with open(pid_path, 'w') as f_pid: f_pid.write(str(os.getpid()))

    logging.basicConfig(filename=log_path, level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    logging.info("Python MetaMap Controller Started (pymm). Mode: {}. Max Workers: {}. MM Options: '{}'".format(
        subcmd, MAX_PARALLEL_WORKERS, METAMAP_PROCESSING_OPTIONS))
    
    if METAMAP_BINARY_PATH is None:
        logging.critical("CRITICAL ERROR: METAMAP_BINARY_PATH environment variable is not set. This is required for pymm.")
        print("CRITICAL ERROR: METAMAP_BINARY_PATH environment variable is not set. Check logs.")
        sys.exit(1)
    
    logging.info("MetaMap Binary: {}".format(METAMAP_BINARY_PATH))
    logging.info("MetaMap Server Ports: {}".format(MM_SERVER_PORTS))

    all_input_files_orig_str = gather_inputs(inp_dir_orig)
    if not all_input_files_orig_str:
        logging.warning("No input .txt files found in {}. Exiting.".format(inp_dir_orig))
        sys.exit(0)

    state = load_state(out_dir) or {}

    files_to_process_paths = []
    completed_files_count_initial = 0
    logging.info("Scanning for already completed files in {}...".format(out_dir))
    for input_file_str_path in all_input_files_orig_str:
        input_file_basename = os.path.basename(input_file_str_path)
        output_csv_path_str = derive_output_csv_path(out_dir, input_file_basename)
        if check_output_file_completion(output_csv_path_str, input_file_basename):
            completed_files_count_initial += 1
        else:
            # If a CSV exists but is incomplete/errored, move it aside so a new attempt doesn't overwrite silently
            if os.path.exists(output_csv_path_str):
                failed_dir = os.path.join(out_dir, ERROR_FOLDER)
                if not os.path.isdir(failed_dir):
                    try:
                        os.makedirs(failed_dir, exist_ok=True)
                    except Exception:
                        pass
                dest_failed = os.path.join(failed_dir, os.path.basename(output_csv_path_str))
                try:
                    shutil.move(output_csv_path_str, dest_failed)
                    logging.info("Moved incomplete CSV {} -> {}".format(output_csv_path_str, dest_failed))
                except Exception as mv_ex:
                    logging.warning("Could not move incomplete CSV {}: {}".format(output_csv_path_str, mv_ex))
            files_to_process_paths.append(input_file_str_path)
    
    logging.info("Found {} files already completed with markers.".format(completed_files_count_initial))
    logging.info("{} files pending processing.".format(len(files_to_process_paths)))

    if not files_to_process_paths:
        logging.info("No files to process. All tasks seem complete according to markers.")
        state["total_overall"] = len(all_input_files_orig_str)
        state["completed_overall_markers"] = completed_files_count_initial
        state["end_time"] = datetime.now().isoformat()
        if "start_time" not in state and state.get("total_overall", 0) > 0:
             state["start_time"] = datetime.now().isoformat()
        save_state(out_dir, state)
        sys.exit(0)

    if subcmd == "start" or not state.get("start_time"):
        state["start_time"] = datetime.now().isoformat()
    state["total_overall"] = len(all_input_files_orig_str)
    state["completed_overall_markers"] = completed_files_count_initial 
    state["file_timings"] = state.get("file_timings", {})
    state["active_workers_info"] = {} # Store info: {worker_id: {"assigned_count": N, "status": "running"}}
    save_state(out_dir, state)

    # Distribute files_to_process_paths among workers
    num_actual_workers = min(MAX_PARALLEL_WORKERS, len(files_to_process_paths))
    if num_actual_workers == 0 and files_to_process_paths: # Should not happen if logic above is correct
        logging.warning("No workers to assign, but files exist. This is unexpected.")
        num_actual_workers = 1 # Assign to at least one if files exist.

    # Prepare arguments for each worker
    worker_tasks_args = []
    if num_actual_workers > 0:
        base_files_per_worker = len(files_to_process_paths) // num_actual_workers
        extra_files = len(files_to_process_paths) % num_actual_workers
        current_file_idx = 0
        for i in range(num_actual_workers):
            files_for_this_worker_count = base_files_per_worker + (1 if i < extra_files else 0)
            if files_for_this_worker_count == 0:
                continue
            
            assigned_files = files_to_process_paths[current_file_idx : current_file_idx + files_for_this_worker_count]
            current_file_idx += files_for_this_worker_count
            
            worker_id_str = "W{}".format(i + 1)
            chosen_port = MM_SERVER_PORTS[i % len(MM_SERVER_PORTS)]
            
            worker_tasks_args.append((
                worker_id_str, assigned_files, out_dir, 
                METAMAP_PROCESSING_OPTIONS, METAMAP_BINARY_PATH, chosen_port
            ))
            state["active_workers_info"][worker_id_str] = {
                "assigned_count": len(assigned_files), 
                "status": "pending_launch"
            }
    save_state(out_dir, state) # Save state with pending worker info

    next_progress_check_time = time.time() + CHECK_INTERVAL
    active_futures = {}

    try:
        with ProcessPoolExecutor(max_workers=num_actual_workers) as executor:
            logging.info("Launching {} Python worker(s) via ProcessPoolExecutor.".format(len(worker_tasks_args)))
            for args in worker_tasks_args:
                worker_id = args[0]
                future = executor.submit(process_files_with_pymm_worker, *args)
                active_futures[future] = worker_id
                state["active_workers_info"][worker_id]["status"] = "running"
            save_state(out_dir, state)

            while active_futures:
                # Process completed futures
                # Use a non-blocking approach or short timeout for as_completed
                # to allow the loop to check time and active_futures count frequently.
                process_completed_futures_non_blocking(active_futures, state)

                if not active_futures:  # Check if all tasks are done
                    logging.info("All active futures completed.")
                    break # Exit the while active_futures loop

                # Periodic progress check
                if time.time() >= next_progress_check_time:
                    logging.info("Periodic progress check...")
                    current_completed_markers = 0
                    for input_f_path in all_input_files_orig_str: # Check against original full list
                        out_csv_p = derive_output_csv_path(out_dir, os.path.basename(input_f_path))
                        if check_output_file_completion(out_csv_p, os.path.basename(input_f_path)):
                            current_completed_markers +=1
                    
                    state["completed_overall_markers"] = current_completed_markers
                    save_state(out_dir, state) # Save state periodically
                    pct = current_completed_markers / state["total_overall"] * 100.0 if state["total_overall"] else 0.0
                    logging.info("Progress (Overall): {comp}/{total} ({pct:.1f}%)".format(
                        comp=current_completed_markers, total=state["total_overall"], pct=pct))
                    active_now = state.get("active_workers_info", {})
                    if active_now: logging.info("  Currently active (from state): {}".format(active_now))
                    next_progress_check_time = time.time() + CHECK_INTERVAL
                
                time.sleep(0.1) # Short sleep to prevent busy-waiting if no futures complete immediately

    except KeyboardInterrupt:
        logging.warning("KeyboardInterrupt received. Terminating worker processes...")
        # ProcessPoolExecutor handles shutdown on context exit, often sending SIGTERM.
        # If using `shutdown(wait=False, cancel_futures=True)` might be needed for quicker exit.
        # For now, rely on ProcessPoolExecutor's __exit__ behavior.
        logging.info("ProcessPoolExecutor will attempt to shut down workers.")
    finally:
        logging.info("Main processing loop finished or was interrupted.")
        # Final state update
        logging.info("Performing final state update...")
        final_completed_markers = 0
        for input_f_path_final in all_input_files_orig_str:
            out_csv_p_final = derive_output_csv_path(out_dir, os.path.basename(input_f_path_final))
            if check_output_file_completion(out_csv_p_final, os.path.basename(input_f_path_final)):
                final_completed_markers += 1
        state["completed_overall_markers"] = final_completed_markers
        state.pop("active_workers_info", None) # Clear active workers
        state["end_time"] = datetime.now().isoformat()
        if "start_time" in state:
            start_dt_final = parse_iso_datetime_compat(state["start_time"])
            end_dt_final = parse_iso_datetime_compat(state["end_time"])
            if start_dt_final and end_dt_final:
                 state["elapsed_seconds"] = int((end_dt_final - start_dt_final).total_seconds())
        save_state(out_dir, state)
        logging.info("Batch processing attempt finished. Total files marked as complete: {}/{}".format(final_completed_markers, len(all_input_files_orig_str)))

        # Final validation (optional, as before)
        # ...

# Helper function to process completed futures to avoid deep nesting in main loop
def process_completed_futures_non_blocking(active_futures_dict, current_state):
    # active_futures_dict is the shared dict {future: worker_id}
    # current_state is the main state dict
    done_futures_keys = []
    # Iterate over a copy of keys if modifying the dict during iteration,
    # or use as_completed with a very short timeout (0 or near 0)
    # For simplicity, let's use a non-blocking check if possible or iterate completed ones.
    # This is tricky with as_completed without blocking. Let's try checking future.done()
    
    # This approach checks status without blocking on as_completed for too long
    temp_done_keys = []
    for future_obj, worker_id_val in active_futures_dict.items():
        if future_obj.done():
            temp_done_keys.append(future_obj)
    
    for future_obj in temp_done_keys:
        worker_id_completed = active_futures_dict[future_obj]
        try:
            worker_results = future_obj.result() # Should not block if future_obj.done() is true
            logging.info("[{}] Worker completed. {} results.".format(worker_id_completed, len(worker_results)))
            for fname, dur_str, err_bool in worker_results:
                current_state["file_timings"][fname] = dur_str
            current_state["active_workers_info"].pop(worker_id_completed, None)
        except Exception as e_future:
            logging.error("[{}] Worker task raised an exception: {}".format(worker_id_completed, e_future))
            logging.exception("Traceback for worker future exception:")
            if worker_id_completed in current_state["active_workers_info"]:
                 current_state["active_workers_info"][worker_id_completed]["status"] = "error"
        active_futures_dict.pop(future_obj, None) # Remove from the main active_futures dict

if __name__ == "__main__":
    main() 