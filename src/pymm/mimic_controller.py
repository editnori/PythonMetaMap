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

import json
import os
import subprocess
import sys
import time
from datetime import datetime
import random
import logging
import shutil
import tempfile
import fnmatch
import re
from pathlib import Path
import csv
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- Configuration File Handling & Constants ---
CONFIG_FILE_NAME = ".pymm_controller_config.json"
METAMAP_PROCESSING_OPTIONS_DEFAULT = "-y -Z 2020AA --lexicon db" 
MAX_PARALLEL_WORKERS_DEFAULT = 4

def get_config_path():
    """Gets the path to the configuration file in the user's home directory."""
    return Path.home() / CONFIG_FILE_NAME

def load_config():
    """Loads configuration from the JSON file. Returns empty dict if not found or error."""
    config_path = get_config_path()
    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Warning: Error decoding config file: {config_path}. A new one may be created.", file=sys.stderr)
            return {}
        except IOError as e:
            print(f"Warning: Could not read config file {config_path}: {e}", file=sys.stderr)
            return {}
    return {}

def save_config(config_data):
    """Saves the given configuration data to the JSON file."""
    config_path = get_config_path()
    try:
        with open(config_path, 'w') as f:
            json.dump(config_data, f, indent=2)
    except IOError as e:
        print(f"Error: Error saving configuration to {config_path}: {e}", file=sys.stderr)

def get_config_value(key, default=None):
    """Gets a value from config file, falling back to environment variable, then default."""
    config = load_config()
    value = config.get(key)
    if value is None:
        env_value = os.getenv(key.upper()) 
        if env_value is not None:
            return env_value
    return value if value is not None else default

def set_config_value(key, value):
    """Sets a key-value pair in the configuration and saves it."""
    config = load_config()
    config[key] = value
    save_config(config)
    print(f"Info: Configuration updated: '{key}' saved in {get_config_path()}", file=sys.stderr)

def remove_config_value(key):
    config = load_config()
    if key in config:
        del config[key]
        save_config(config)
        print(f"Info: Config value '{key}' removed from {get_config_path()}", file=sys.stderr)
    else:
        print(f"Info: Config value '{key}' not found in config file.", file=sys.stderr)

def prompt_and_save_config_value(key, prompt_text, explanation="", is_essential=False, code_default=""):
    """Prompts user for a config value and saves it if provided or if a default/discovered value is used."""
    try:
        value_from_config_or_env = get_config_value(key)
        
        value_to_propose = None
        hint_type = ""

        if value_from_config_or_env is not None:
            value_to_propose = value_from_config_or_env
            hint_type = "current"
        else:
            if key == "metamap_binary_path":
                meta_install_dir = os.path.abspath("metamap_install")
                # Use helper function to simplify; break early if found
                pass
                auto_found = discover_metamap_binary(meta_install_dir)
                if auto_found:
                    value_to_propose = auto_found
                    hint_type = "found"
            
            if value_to_propose is None and code_default:
                value_to_propose = code_default
                hint_type = "default"

        display_hint_message = ""
        if hint_type == "current":
            display_hint_message = f" (current: '{value_to_propose}', Enter to keep)"
        elif hint_type == "found":
            display_hint_message = f" (found: '{value_to_propose}', Enter to use)"
        elif hint_type == "default":
            display_hint_message = f" (default: '{value_to_propose}', Enter to use default)"
        
        if explanation: print(f"  ({explanation})")
        user_input = input(f"{prompt_text}{display_hint_message}: ").strip()

        final_value_to_use = None
        if user_input:
            final_value_to_use = user_input
            set_config_value(key, final_value_to_use)
        elif value_to_propose is not None:
            final_value_to_use = value_to_propose
            set_config_value(key, final_value_to_use)

        if final_value_to_use is None and is_essential:
            if key == "metamap_binary_path": # Specific handling for metamap_binary_path
                try:
                    choice = input(f"MetaMap binary for '{key}' not found. Attempt to install MetaMap now? (yes/no): ").strip().lower()
                    if choice == 'yes':
                        print("Attempting MetaMap installation...")
                        # Ensure install_metamap.py is in the project root for this import to work
                        # when pymm-cli is run from the project root.
                        # Add project root to sys.path to allow importing install_metamap
                        # This assumes mimic_controller.py is in src/pymm 
                        # and install_metamap.py is in the parent of src/
                        try:
                            from . import install_metamap
                            installed_path = install_metamap.main()
                            if installed_path and os.path.isfile(installed_path):
                                print(f"MetaMap installation successful. Binary found at: {installed_path}")
                                final_value_to_use = installed_path
                                set_config_value(key, final_value_to_use) # Save the newly found path
                            else:
                                print("MetaMap installation did not succeed or binary not found post-install.")
                        except ImportError as e_imp:
                            print(f"Failed to import 'install_metamap' module from within the 'pymm' package. Error: {e_imp}", file=sys.stderr)
                        except Exception as e_install_run:
                            print(f"Error during MetaMap installation call: {e_install_run}", file=sys.stderr)
                    else:
                        print("Skipping MetaMap installation.")
                except Exception as e_prompt: # Catch errors from input() like EOFError
                     print(f"Error during installation prompt: {e_prompt}", file=sys.stderr)
            
            # After potential install attempt, check final_value_to_use again
            if final_value_to_use is None:
                print(f"Error: Essential setting '{key}' requires a value and none could be determined or installed.", file=sys.stderr)
                return None
        
        return final_value_to_use

    except EOFError:
        print(f"Warning: Cannot prompt for {key} (EOF). Trying to use a fallback value.", file=sys.stderr)
        eof_fallback_value = get_config_value(key)

        if eof_fallback_value is None and key == "metamap_binary_path":
            meta_install_dir = os.path.abspath("metamap_install")
            # Use helper function to simplify; break early if found
            pass
            auto_found = discover_metamap_binary(meta_install_dir)
            if auto_found:
                eof_fallback_value = auto_found
        
        if is_essential and eof_fallback_value is None:
            print(f"Error: Essential setting '{key}' could not be determined via EOF and no fallbacks exist.", file=sys.stderr)
            return None
        
        return eof_fallback_value

def configure_all_settings(is_reset=False):
    print("\n--- Configuring Settings ---")
    if is_reset:
        keys_to_reset = ["metamap_binary_path", "default_input_dir", "default_output_dir", 
                           "metamap_processing_options", "max_parallel_workers"]
        print("Resetting the following keys to allow re-prompt or use of defaults/env:")
        for key_r in keys_to_reset: 
            print(f"  - {key_r}")
            remove_config_value(key_r)
        print("Config values removed. Will now re-prompt for essentials.")

    mbp = prompt_and_save_config_value("metamap_binary_path", 
                                       "Full path to MetaMap binary (e.g., /opt/public_mm/bin/metamap)",
                                       explanation="This is the main MetaMap executable file.",
                                       is_essential=True)
    if not mbp:
        print("Error: METAMAP_BINARY_PATH is essential and was not configured. Some operations will fail.", file=sys.stderr)

    prompt_and_save_config_value("default_input_dir", "Default input directory for notes", 
                                   explanation="Directory containing .txt files to process (optional).",
                                   code_default="./input_notes")
    prompt_and_save_config_value("default_output_dir", "Default output directory for CSVs",
                                   explanation="Directory to save results (optional).", 
                                   code_default="./output_csvs")
    prompt_and_save_config_value("metamap_processing_options", "MetaMap processing options",
                                   explanation=f"Command-line flags for MetaMap (e.g., -y -Z ...).",
                                   code_default=METAMAP_PROCESSING_OPTIONS_DEFAULT)
    prompt_and_save_config_value("max_parallel_workers", "Max parallel workers",
                                   explanation=f"Number of files to process simultaneously. Consider your CPU cores.",
                                   code_default=str(MAX_PARALLEL_WORKERS_DEFAULT))
    print("--- Configuration Complete ---")

# --- PyMetaMap Import (from .pymm import Metamap as PyMetaMap) ---
try:
    from . import Metamap as PyMetaMap  # Metamap exposed in package __init__
except ImportError:
    try:
        from pymm import Metamap as PyMetaMap  # absolute fallback
    except ImportError as e:
        print(f"ERROR: Could not import Metamap class: {e}", file=sys.stderr)
        PyMetaMap = None

# --- Global Constants (STATE_FILE, PID_FILE, etc.) ---
STATE_FILE = ".mimic_state.json"
PID_FILE = ".mimic_pid"
CHECK_INTERVAL = 15  
ERROR_FOLDER = "error_files"
CSV_HEADER = ["CUI","Score","ConceptName","PrefName","Phrase","SemTypes","Sources","Positions"]
START_MARKER_PREFIX = "META_BATCH_START_NOTE_ID:"
END_MARKER_PREFIX = "META_BATCH_END_NOTE_ID:"

# --- Utility: search for a MetaMap binary under metamap_install (cross-platform friendly) ---
def discover_metamap_binary(search_root="metamap_install"):
    """Return path to first plausible MetaMap executable inside search_root, else None."""
    if not os.path.isdir(search_root):
        return None
    candidate_names = {
        "metamap", "metamap20",
        "metamap.exe", "metamap20.exe",
        "metamap.bat", "metamap20.bat",
    }
    for root, dirs, files in os.walk(search_root):
        for fname in files:
            if fname.lower() in candidate_names:
                return os.path.abspath(os.path.join(root, fname))
    return None

# --- Fancy ASCII Banner (imported from old bash script) ---
ASCII_BANNER = r"""
  __  __  _____  _____    _    __  __    _    ____     ____ _      ___
 |  \/  || ____||_   _|  / \  |  \/  |  / \  |  _ \   / ___| |    |_ _|
 | |\/| ||  _|    | |   / _ \ | |\/| | / _ \ | |_) | | |   | |     | |
 | |  | || |___   | |  / ___ \| |  | |/ ___ \|  __/  | |___| |___  | |
 |_|  |_||_____| |___|/_/   \_\_|  |_/_/   \_\_|     \____||_____||___|
"""

# --- MetaMap Server Management Helpers (lightweight wrappers) ---
# These simply shell out to underlying MetaMap control scripts if present.
# They are intentionally tolerant – they will not raise if the commands are missing.

def _run_quiet(cmd_list, cwd=None):
    try:
        subprocess.run(cmd_list, cwd=cwd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except Exception:
        return False

def start_metamap_servers(public_mm_dir):
    bin_path = os.path.join(public_mm_dir, "bin")
    skr_ctl = os.path.join(bin_path, "skrmedpostctl")
    wsd_ctl = os.path.join(bin_path, "wsdserverctl")
    mmserver = os.path.join(bin_path, "mmserver20")
    if not (os.path.isfile(skr_ctl) and os.path.isfile(wsd_ctl) and os.path.isfile(mmserver)):
        print("MetaMap server scripts not found in", bin_path)
        return
    print("Launching Tagger, WSD, and mmserver20…")
    _run_quiet([skr_ctl, "start"])
    _run_quiet([wsd_ctl, "start"])
    # Launch mmserver20 in background
    try:
        subprocess.Popen([mmserver])
    except Exception as e:
        print("Could not start mmserver20:", e)
    print("Start commands issued. Use 'Status' to verify.")

def stop_metamap_servers(public_mm_dir):
    bin_path = os.path.join(public_mm_dir, "bin")
    skr_ctl = os.path.join(bin_path, "skrmedpostctl")
    wsd_ctl = os.path.join(bin_path, "wsdserverctl")
    if os.path.isfile(skr_ctl): _run_quiet([skr_ctl, "stop"])
    if os.path.isfile(wsd_ctl): _run_quiet([wsd_ctl, "stop"])
    # Kill mmserver20 processes
    _run_quiet(["pkill", "-f", "mmserver20"])
    print("Stop commands sent.")

def status_metamap_servers(public_mm_dir):
    bin_path = os.path.join(public_mm_dir, "bin")
    skr_ctl = os.path.join(bin_path, "skrmedpostctl")
    wsd_ctl = os.path.join(bin_path, "wsdserverctl")
    mmserver_proc = subprocess.run(["pgrep", "-f", "mmserver20"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    running_mmservers = mmserver_proc.stdout.decode().strip().split() if mmserver_proc.returncode == 0 else []
    
    print("--- SKR/MedPost Tagger Status ---")
    if os.path.isfile(skr_ctl):
        subprocess.run([skr_ctl, "status"], check=False)
    else:
        print(f"skrmedpostctl not found at {skr_ctl}")

    print("--- WSD Server Status ---")
    if os.path.isfile(wsd_ctl):
        subprocess.run([wsd_ctl, "status"], check=False)
    else:
        print(f"wsdserverctl not found at {wsd_ctl}")

    print("--- MMServer20 Status (via pgrep) ---")
    if running_mmservers:
        print("mmserver20 running – PIDs:", ", ".join(running_mmservers))
    else:
        print("mmserver20 not detected in process list.")

# --- Auto-start helper -------------------------------------------------
def _is_server_process_running(command_path, server_name_in_output):
    """Helper to check if a server process (tagger/wsd) is running via its ctl script."""
    if not os.path.isfile(command_path):
        print(f"DEBUG: {os.path.basename(command_path)} not found at {command_path}")
        return False
    try:
        result = subprocess.run([command_path, "status"], capture_output=True, text=True, timeout=10, check=False)
        # Look for phrases like "is running" and avoid "is stopped" or "not running"
        output_lower = result.stdout.lower() + result.stderr.lower()
        # Crude check: MetaMap ctl scripts often include server name then "is running"
        # Example: "SKR/MedPost Part-of-Speech Tagger Server (pid 12345) is running."
        # Example: "WSD Server (pid 67890) is running."
        # Example: "SKR/MedPost Part-of-Speech Tagger Server is stopped."
        if server_name_in_output.lower() in output_lower and "is running" in output_lower and "stopped" not in output_lower:
            print(f"DEBUG: {server_name_in_output} detected as RUNNING from status output.")
            return True
        print(f"DEBUG: {server_name_in_output} NOT detected as running. Status output:\n{result.stdout}\n{result.stderr}")
        return False
    except subprocess.TimeoutExpired:
        print(f"DEBUG: Timeout checking status for {os.path.basename(command_path)}.")
        return False # Assume not running on timeout
    except Exception as e:
        print(f"DEBUG: Error checking status for {os.path.basename(command_path)}: {e}")
        return False # Assume not running on error

def ensure_servers_running():
    """Start Tagger, WSD, and mmserver20 if they are not running.

    NOTE: Disabled in lightweight configuration – function will exit immediately so that
    MetaMap runs without attempting to launch or manage additional servers.
    """
    print("Info: ensure_servers_running() disabled – skipping MetaMap server startup.")
    return  # <-- Early return prevents any server-management logic below
    # The original implementation is retained below for reference but will never execute.

# --- Helper Functions (rglob_compat, load_state, etc.) ---
def rglob_compat(directory, pattern):
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                filename = os.path.join(root, basename)
                yield filename

def load_state(out_dir): 
    state_path = os.path.join(out_dir, STATE_FILE)
    if os.path.exists(state_path):
        try:
            with open(state_path, 'r') as f: return json.load(f)
        except Exception as e:
            logging.error(f"Error loading state file {state_path}: {e}")
            return None
    return None

def save_state(out_dir, state): 
    state_path = os.path.join(out_dir, STATE_FILE)
    with open(state_path, 'w') as f: json.dump(state, f, indent=2)

def gather_inputs(inp_dir): 
    return sorted([p for p in rglob_compat(inp_dir, "*.txt")])

def check_output_file_completion(output_csv_path, input_file_original_stem_for_marker):
    """Checks if an output CSV is fully processed based on start/end markers."""
    if not os.path.exists(output_csv_path) or os.stat(output_csv_path).st_size == 0:
        return False

    try:
        first_line_str = ""
        last_line_str = ""
        with open(output_csv_path, 'rb') as f_bin:
            first_line_bytes = f_bin.readline()
            first_line_str = first_line_bytes.decode('utf-8', 'replace').strip()

            f_bin.seek(0, os.SEEK_END)
            current_pos = f_bin.tell()

            if current_pos == 0:
                 if not first_line_str:
                    logging.warning("File is empty (binary check): {path}".format(path=output_csv_path))
                    return False
                 last_line_str = first_line_str
            elif current_pos == len(first_line_bytes):
                last_line_str = first_line_str
            else:
                buffer_size = 1024 
                if current_pos < buffer_size:
                    buffer_size = current_pos
                
                f_bin.seek(-buffer_size, os.SEEK_END)
                buffer_content_bytes = f_bin.read(buffer_size)
                
                lines_in_buffer = buffer_content_bytes.strip().splitlines() 
                if lines_in_buffer:
                    last_line_str = lines_in_buffer[-1].decode('utf-8', 'replace').strip()
                elif first_line_str:
                    logging.warning("Could not determine last line via buffer (binary mode) for {path}. First line was: '{fl}'".format(path=output_csv_path, fl=first_line_str))

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

def get_dynamic_log_filename(base_output_dir):
    parent_dir_name_of_out = os.path.basename(os.path.dirname(base_output_dir))
    if "kidney" in parent_dir_name_of_out.lower():
        return "kidney_controller.log"
    elif "mimic" in parent_dir_name_of_out.lower():
        return "mimic_controller.log"
    else:
        return "generic_controller.log"

def parse_iso_datetime_compat(dt_str):
    if not dt_str:
        return None
    # Strip microseconds if present, as strptime %f is tricky across Py2/3 for this specific format part
    return datetime.strptime(dt_str.split('.')[0], "%Y-%m-%dT%H:%M:%S")

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

def input_filename_from_csv_basename(csv_basename):
    if csv_basename.lower().endswith(".txt.csv"):
        return csv_basename[:-4]  # strip only .csv, still includes .txt
    elif csv_basename.lower().endswith(".csv"):
        return csv_basename[:-4] + ".txt"
    else:
        return csv_basename  # fallback (should not occur)

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

def process_files_with_pymm_worker(worker_id, files_for_worker, main_out_dir, current_metamap_options, current_metamap_binary_path):
    logging.info(f"[{worker_id}] Starting worker. Files: {len(files_for_worker)}. Binary: '{current_metamap_binary_path}'. Options (for logging): '{current_metamap_options}'")
    results = []
    if PyMetaMap is None:
        logging.error(f"[{worker_id}] PyMetaMap class not loaded! Cannot initialize MetaMap. This worker will fail.")
        for f_path in files_for_worker: results.append((os.path.basename(f_path), "0ms", True))
        return results
    try:
        mm = PyMetaMap(current_metamap_binary_path, debug=False) 
    except Exception as e:
        logging.error(f"[{worker_id}] Failed to initialize PyMetaMap with binary '{current_metamap_binary_path}': {e}")
        for f_path in files_for_worker: results.append((os.path.basename(f_path), "0ms", True))
        return results
    
    for input_file_path_str in files_for_worker:
        input_file_basename = os.path.basename(input_file_path_str)
        output_csv_path = derive_output_csv_path(main_out_dir, input_file_basename)
        processing_error_occurred = False; duration_ms = 0
        start_time = time.time()
        try:
            with open(input_file_path_str, 'r', encoding='utf-8') as f_in: whole_note = f_in.read().strip()
            lines = [whole_note] if whole_note else []
            if not lines:
                logging.info(f"[{worker_id}] Input file {input_file_basename} is empty. Skipping.")
                results.append((input_file_basename, "0ms", False))
                with open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:
                    f_out.write(f"{START_MARKER_PREFIX}{input_file_basename}\n")
                    csv.writer(f_out, quoting=csv.QUOTE_ALL, doublequote=True).writerow(CSV_HEADER)
                # End marker written in finally
                continue
            
            pymm_timeout = int(os.getenv("PYMM_TIMEOUT", "120"))
            mmos_iter = mm.parse(lines, timeout=pymm_timeout)
            concepts_list = [concept for mmo_item in mmos_iter for concept in mmo_item]

            with open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:
                f_out.write(f"{START_MARKER_PREFIX}{input_file_basename}\n")
                writer = csv.writer(f_out, quoting=csv.QUOTE_ALL, doublequote=True)
                writer.writerow(CSV_HEADER)
                
                # This is the complex utterance and concept processing logic from your original file
                # It should be largely correct if it was working before.
                if concepts_list:
                    concepts_by_utterance = {}
                    for idx, c in enumerate(concepts_list):
                        if not c.ismapping: continue
                        utterance_id = getattr(c, 'utterance_id', None)
                        if utterance_id is None:
                            pos = None
                            for attr in ('phrase_start', 'pos_start'):
                                val = getattr(c, attr, None)
                                if isinstance(val, int) and val > 0: pos = val; break
                            if pos is None and getattr(c, 'matchedstart', None): pos = min(c.matchedstart)
                            utterance_id = (pos // 500) if pos is not None else 999999
                        if utterance_id not in concepts_by_utterance: concepts_by_utterance[utterance_id] = []
                        concepts_by_utterance[utterance_id].append((idx, c))
                    
                    concepts_to_write = []
                    for utterance_id in sorted(concepts_by_utterance.keys()):
                        concepts_in_utterance = sorted(concepts_by_utterance[utterance_id], key=lambda pair: pair[0])
                        concepts_to_write.extend(pair[1] for pair in concepts_in_utterance)
                    
                    # logging.debug(f"[{worker_id}] Processed {len(concepts_to_write)} concepts in {len(concepts_by_utterance)} utterances for {input_file_basename}.")
                    utterance_texts = {}; utterance_offset_map = {}
                    if whole_note: # Only build if there's text
                        utterance_splits = re.split(r'\n\s*\n', whole_note)
                        for uidx, utt_text_segment in enumerate(utterance_splits): utterance_texts[uidx+1] = utt_text_segment
                        if len(utterance_splits) <= 1: # fallback for single line/no double newline
                            line_splits = whole_note.splitlines()
                            for lidx, line_text in enumerate(line_splits): utterance_texts[lidx+1] = line_text
                        for c_tmp in concepts_list:
                            uid_tmp = getattr(c_tmp, 'utterance_id', None); pstart_tmp = getattr(c_tmp, 'pos_start', None)
                            if uid_tmp is not None and pstart_tmp is not None:
                                if uid_tmp not in utterance_offset_map or pstart_tmp < utterance_offset_map[uid_tmp]:
                                    utterance_offset_map[uid_tmp] = pstart_tmp

                    for concept in concepts_to_write:
                        try:
                            concept_name  = getattr(concept, 'concept_name', getattr(concept, 'preferred_name', concept.matched))
                            pref_name     = getattr(concept, 'preferred_name', concept_name)
                            phrase_text   = getattr(concept, 'phrase_text', None) or getattr(concept, 'phrase', None) or getattr(concept, 'matched', '')
                            sem_types_formatted = ":".join(concept.semtypes) if getattr(concept, 'semtypes', None) else ""
                            sources_formatted = "|".join(concept.sources) if getattr(concept, 'sources', None) else ""
                            positions_formatted = ""
                            if hasattr(concept, 'pos_start') and concept.pos_start is not None:
                                try:
                                    uid_cur = getattr(concept, 'utterance_id', None)
                                    if uid_cur is not None and uid_cur in utterance_offset_map:
                                        rel_start = concept.pos_start - utterance_offset_map[uid_cur] + 1
                                        positions_formatted = f"{rel_start}:{concept.pos_length if concept.pos_length else len(phrase_text)}"
                                    else:
                                        positions_formatted = f"{concept.pos_start}:{concept.pos_length if concept.pos_length else len(phrase_text)}"
                                except Exception: positions_formatted = "" # Error in complex pos logic
                            if not positions_formatted and phrase_text: # Fallback utterance search
                                utt_text_lookup = utterance_texts.get(getattr(concept, 'utterance_id', None), whole_note)
                                idx_f = utt_text_lookup.find(phrase_text)
                                if idx_f >= 0:
                                    cr_adjust = utt_text_lookup.count('\n', 0, idx_f)
                                    positions_formatted = f"{idx_f + cr_adjust + 1}:{len(phrase_text)}"
                            row_data = [concept.cui, concept.score, concept_name, pref_name, phrase_text, sem_types_formatted, sources_formatted, positions_formatted]
                            writer.writerow([pymm_escape_csv_field(field) for field in row_data])
                        except Exception as e_concept:
                            logging.error(f"[{worker_id}] Error processing/writing concept for {input_file_basename}: {e_concept} - Concept: {concept}")
                            processing_error_occurred = True 
        except Exception as e_file:
            logging.error(f"[{worker_id}] Error processing file {input_file_path_str} or writing to {output_csv_path}: {e_file}")
            logging.exception("Traceback for file processing error:")
            processing_error_occurred = True
        finally:
            end_time = time.time(); duration_ms = int((end_time - start_time) * 1000)
            try:
                start_marker_required = (not os.path.exists(output_csv_path)) or os.path.getsize(output_csv_path) == 0
                open_mode = 'a'
                with open(output_csv_path, open_mode, encoding='utf-8') as f_out_end:
                    if start_marker_required:
                        # Write start marker and CSV header for symmetry even if an early error occurred
                        f_out_end.write(f"{START_MARKER_PREFIX}{input_file_basename}\n")
                        csv.writer(f_out_end, quoting=csv.QUOTE_ALL, doublequote=True).writerow(CSV_HEADER)
                    end_marker = f"{END_MARKER_PREFIX}{input_file_basename}{':ERROR' if processing_error_occurred else ''}\n"
                    f_out_end.write(end_marker)
            except Exception as e_marker: 
                logging.error(f"[{worker_id}] Failed to write END_MARKER for {input_file_basename}: {e_marker}")
                processing_error_occurred = True
            results.append((input_file_basename, f"{duration_ms}ms", processing_error_occurred))
    if 'mm' in locals() and mm: mm.close()
    logging.info(f"[{worker_id}] Finished processing batch of {len(files_for_worker)} files.")
    return results

def execute_batch_processing(inp_dir_str, out_dir_str, mode, global_metamap_binary_path, global_metamap_options):
    logging.info(f"Executing batch processing. Mode: {mode}, Input: {inp_dir_str}, Output: {out_dir_str}")
    logging.info(f"  MetaMap Binary: {global_metamap_binary_path}")
    logging.info(f"  MetaMap Options: {global_metamap_options}")

    inp_dir_orig = os.path.abspath(os.path.expanduser(inp_dir_str))
    out_dir = os.path.abspath(os.path.expanduser(out_dir_str))
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    pid_path = os.path.join(out_dir, PID_FILE)
    with open(pid_path, 'w') as f_pid: f_pid.write(str(os.getpid()))

    all_input_files_orig_str = gather_inputs(inp_dir_orig)
    if not all_input_files_orig_str:
        logging.warning(f"No input .txt files found in {inp_dir_orig}. Nothing to do.")
        print(f"No input .txt files found in {inp_dir_orig}.")
        return

    state = load_state(out_dir) or {}
    files_to_process_paths = []
    completed_files_count_initial = 0
    logging.info(f"Scanning for already completed files in {out_dir}...")
    for input_file_str_path in all_input_files_orig_str:
        input_file_basename = os.path.basename(input_file_str_path)
        output_csv_path_str = derive_output_csv_path(out_dir, input_file_basename)
        if check_output_file_completion(output_csv_path_str, input_file_basename):
            completed_files_count_initial += 1
        else:
            if os.path.exists(output_csv_path_str):
                failed_dir = os.path.join(out_dir, ERROR_FOLDER)
                Path(failed_dir).mkdir(parents=True, exist_ok=True)
                dest_failed = os.path.join(failed_dir, os.path.basename(output_csv_path_str))
                try: shutil.move(output_csv_path_str, dest_failed); logging.info(f"Moved incomplete CSV {output_csv_path_str} to {dest_failed}")
                except Exception as mv_ex: logging.warning(f"Could not move incomplete CSV {output_csv_path_str}: {mv_ex}")
            files_to_process_paths.append(input_file_str_path)
    
    logging.info(f"Found {completed_files_count_initial} files already completed with markers.")
    logging.info(f"{len(files_to_process_paths)} files pending processing for this run.")

    if not files_to_process_paths:
        logging.info("No files to process. All tasks seem complete according to markers.")
        state["total_overall"] = len(all_input_files_orig_str); state["completed_overall_markers"] = completed_files_count_initial
        state["end_time"] = datetime.now().isoformat()
        if "start_time" not in state and state.get("total_overall", 0) > 0: state["start_time"] = datetime.now().isoformat()
        save_state(out_dir, state); return

    if mode == "start" or not state.get("start_time"):
        state["start_time"] = datetime.now().isoformat()
    state["total_overall"] = len(all_input_files_orig_str)
    state["completed_overall_markers"] = completed_files_count_initial 
    state["file_timings"] = state.get("file_timings", {})
    state["active_workers_info"] = {} 
    save_state(out_dir, state)

    current_max_workers = int(get_config_value("max_parallel_workers", os.getenv("MAX_PARALLEL_WORKERS", str(MAX_PARALLEL_WORKERS_DEFAULT))))
    num_actual_workers = min(current_max_workers, len(files_to_process_paths))
    if num_actual_workers == 0 and files_to_process_paths: num_actual_workers = 1

    worker_tasks_args = []
    if num_actual_workers > 0:
        base_files_per_worker = len(files_to_process_paths) // num_actual_workers
        extra_files = len(files_to_process_paths) % num_actual_workers
        current_file_idx = 0
        for i in range(num_actual_workers):
            files_for_this_worker_count = base_files_per_worker + (1 if i < extra_files else 0)
            if files_for_this_worker_count == 0: continue
            assigned_files = files_to_process_paths[current_file_idx : current_file_idx + files_for_this_worker_count]
            current_file_idx += files_for_this_worker_count
            worker_id_str = f"W{i + 1}"
            worker_tasks_args.append((worker_id_str, assigned_files, out_dir, global_metamap_options, global_metamap_binary_path))
            state["active_workers_info"][worker_id_str] = {"assigned_count": len(assigned_files), "status": "pending_launch"}
    save_state(out_dir, state)

    next_progress_check_time = time.time() + CHECK_INTERVAL
    active_futures = {}
    try:
        with ProcessPoolExecutor(max_workers=num_actual_workers) as executor:
            logging.info(f"Launching {len(worker_tasks_args)} Python worker(s) via ProcessPoolExecutor.")
            for args_tuple in worker_tasks_args:
                worker_id = args_tuple[0]
                future = executor.submit(process_files_with_pymm_worker, *args_tuple)
                active_futures[future] = worker_id
                state["active_workers_info"][worker_id]["status"] = "running"
            save_state(out_dir, state)
            while active_futures:
                temp_done_keys = [f for f, _ in active_futures.items() if f.done()]
                for future_obj_done in temp_done_keys:
                    worker_id_completed = active_futures.pop(future_obj_done, "UnknownWorker")
                    try:
                        worker_results = future_obj_done.result()
                        if worker_results: 
                            for fname, dur_str, err_bool in worker_results:
                                state["file_timings"][fname] = dur_str
                        if worker_id_completed != "UnknownWorker": state["active_workers_info"].pop(worker_id_completed, None)
                    except Exception as e_future:
                        logging.error(f"[{worker_id_completed}] Worker task raised an exception: {e_future}")
                        if worker_id_completed != "UnknownWorker" and worker_id_completed in state["active_workers_info"]:
                            state["active_workers_info"][worker_id_completed]["status"] = "error"
                if not active_futures: logging.info("All active futures completed."); break
                if time.time() >= next_progress_check_time:
                    current_completed_m = sum(1 for input_f_p in all_input_files_orig_str if check_output_file_completion(derive_output_csv_path(out_dir, os.path.basename(input_f_p)), os.path.basename(input_f_p)))
                    state["completed_overall_markers"] = current_completed_m
                    save_state(out_dir, state)
                    pct_prog = current_completed_m / state["total_overall"] * 100.0 if state["total_overall"] else 0.0
                    logging.info(f"Progress (Overall): {current_completed_m}/{state['total_overall']} ({pct_prog:.1f}%)")
                    next_progress_check_time = time.time() + CHECK_INTERVAL
                time.sleep(0.2)
    except KeyboardInterrupt: logging.warning("KeyboardInterrupt received. Terminating worker processes...")
    finally:
        logging.info("Main processing loop finished or was interrupted.")
        final_completed_m = sum(1 for input_f_path_f in all_input_files_orig_str if check_output_file_completion(derive_output_csv_path(out_dir, os.path.basename(input_f_path_f)), os.path.basename(input_f_path_f)))
        state["completed_overall_markers"] = final_completed_m; state.pop("active_workers_info", None); state["end_time"] = datetime.now().isoformat()
        if "start_time" in state:
            s_dt_f = parse_iso_datetime_compat(state["start_time"]); e_dt_f = parse_iso_datetime_compat(state["end_time"])
            if s_dt_f and e_dt_f: state["elapsed_seconds"] = int((e_dt_f - s_dt_f).total_seconds())
        save_state(out_dir, state)
        logging.info(f"Batch processing attempt finished. Total files marked as complete: {final_completed_m}/{len(all_input_files_orig_str)}")

    # --- Ensure log file for this output dir ---
    log_file_name = get_dynamic_log_filename(out_dir)
    log_path = os.path.join(out_dir, log_file_name)
    if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == log_path for h in logging.getLogger().handlers):
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        logging.getLogger().addHandler(fh)
    print(f"Logs will be written to: {log_path}")

def handle_configure_settings_menu(): # Renamed to avoid clash if main() has similar name
    configure_all_settings(is_reset=False)

def handle_reset_settings():
    print("\n--- Resetting Configuration ---")
    if input("Are you sure you want to remove all saved configurations? (yes/no): ").strip().lower() == 'yes':
        configure_all_settings(is_reset=True)
        print("Please re-run 'Configure Settings' (option 1) or restart pymm-cli to set essential paths.")
    else:
        print("Reset cancelled.")

def handle_run_batch_processing():
    print("\n--- Run MetaMap Batch Processing ---")
    metamap_binary_p = get_config_value("metamap_binary_path")
    if not metamap_binary_p:
        print("METAMAP_BINARY_PATH is not configured. Please run 'Configure Settings' first."); return

    default_input = get_config_value("default_input_dir", "./input_notes")
    default_output = get_config_value("default_output_dir", "./output_csvs")
    try:
        inp_dir = input(f"Enter input directory (default: {default_input}): ").strip() or default_input
        out_dir = input(f"Enter output directory (default: {default_output}): ").strip() or default_output
    except EOFError: inp_dir, out_dir = default_input, default_output; print("Using defaults.")
    Path(inp_dir).mkdir(parents=True, exist_ok=True); Path(out_dir).mkdir(parents=True, exist_ok=True)
    print(f"Using Input directory: {os.path.abspath(inp_dir)}")
    print(f"Using Output directory: {os.path.abspath(out_dir)}")

    metamap_opts = get_config_value("metamap_processing_options", METAMAP_PROCESSING_OPTIONS_DEFAULT)
    current_max_workers = int(get_config_value("max_parallel_workers", str(MAX_PARALLEL_WORKERS_DEFAULT)))
    # The global MAX_PARALLEL_WORKERS is used by execute_batch_processing, ensure it reflects current config
    global MAX_PARALLEL_WORKERS
    MAX_PARALLEL_WORKERS = current_max_workers

    print(f"MetaMap Options: '{metamap_opts}'")
    print(f"Max Parallel Workers: {MAX_PARALLEL_WORKERS}")

    # Auto-start MetaMap servers if needed (disabled)
    # ensure_servers_running()

    mode = "start" 
    if os.listdir(out_dir) and any(f.endswith('.csv') or f == STATE_FILE for f in os.listdir(out_dir)):
        if input("Output directory is not empty. Resume previous job? (yes/no, default: no): ").strip().lower() == 'yes':
            mode = "resume"
    print(f"Starting batch processing in '{mode}' mode...")
    execute_batch_processing(inp_dir, out_dir, mode, metamap_binary_p, metamap_opts)

def handle_view_progress(output_dir_str=None):
    print("\n--- View Batch Progress ---")
    if not output_dir_str:
        default_output = get_config_value("default_output_dir", "./output_csvs")
        try: output_dir_str = input(f"Enter output directory to check (default: {default_output}): ").strip() or default_output
        except EOFError: output_dir_str = default_output; print("Using default output dir.")
    
    out_abs = os.path.abspath(os.path.expanduser(output_dir_str))
    if not os.path.isdir(out_abs): print(f"Error: Output directory not found: {out_abs}"); return
    state = load_state(out_abs)
    if not state: print(f"No state file ({STATE_FILE}) present in {out_abs}."); return
    
    total_overall = state.get("total_overall", 0)
    completed_overall_markers = state.get("completed_overall_markers", 0)
    pct = (completed_overall_markers / total_overall * 100.0) if total_overall else 0.0
    print(f"Progress: {completed_overall_markers}/{total_overall} ({pct:.1f}% completed)")
    active_workers_info = state.get("active_workers_info")
    if active_workers_info and isinstance(active_workers_info, dict) and any(active_workers_info.values()):
        active_files_display = [f"{wid}:{info.get('current_file', 'N/A')}" for wid, info in active_workers_info.items() if isinstance(info, dict)]
        if active_files_display: print(f"  Active files (from state): {', '.join(active_files_display)}")
    elif not state.get("end_time"): print("  No active file information in current state (or batch just started).")
    if "start_time" in state:
        start_dt = parse_iso_datetime_compat(state["start_time"]); end_ts_str = state.get("end_time")
        if end_ts_str:
            end_dt = parse_iso_datetime_compat(end_ts_str); pre_calculated_elapsed = state.get("elapsed_seconds")
            if pre_calculated_elapsed is not None: elapsed = int(pre_calculated_elapsed)
            elif start_dt and end_dt and end_dt >= start_dt: elapsed = int((end_dt - start_dt).total_seconds())
            else: elapsed = None
            if elapsed is not None: print(f"Total elapsed time: {elapsed//60} min {elapsed%60} sec (Batch completed)")
            else: print("Total elapsed time: Could not determine (Batch completed, inconsistent times).")
        elif start_dt:
            now = datetime.now()
            if now >= start_dt: elapsed = int((now - start_dt).total_seconds()); print(f"Elapsed time so far: {elapsed//60} min {elapsed%60} sec")
            else: print("Elapsed time so far: Start time is in the future. Check system clock/state file.")
        else: print("Elapsed time: Could not determine (start time missing/invalid).")
    file_timings = state.get("file_timings", {})
    if file_timings:
        num_timed_files = len(file_timings); total_time_ms = sum(int(d.replace("ms","")) for d in file_timings.values() if d.replace("ms","").isdigit())
        if num_timed_files > 0:
            avg_time_ms = total_time_ms / num_timed_files
            print(f"  Average time per file: {avg_time_ms:.0f} ms ({avg_time_ms/1000.0:.2f} s) over {num_timed_files} timed files.")
            remaining_files = total_overall - completed_overall_markers
            if remaining_files > 0 and not state.get("end_time"):
                etr_seconds = (avg_time_ms / 1000.0) * remaining_files
                print(f"  Estimated time remaining (ETR): {int(etr_seconds // 60)} min {int(etr_seconds % 60)} sec for {remaining_files} files")
        else: print("  No valid file timing data to calculate average or ETR.")
    else: print("  No file timing data recorded yet.")

def handle_install_metamap():
    print("\n--- Install MetaMap ---")
    try:
        from . import install_metamap
    except ImportError as imp_err:
        print("Could not import install_metamap module:", imp_err)
        return
    installed_path = install_metamap.main()
    if installed_path and os.path.isfile(installed_path):
        set_config_value("metamap_binary_path", installed_path)
        print("MetaMap installed successfully at:", installed_path)
    else:
        print("MetaMap installation did not report a usable binary. Check above output.")

def handle_monitor_dashboard():
    print("\n--- Monitor Dashboard (live) ---")
    import psutil, time
    default_output = get_config_value("default_output_dir", "./output_csvs")
    out_dir = input(f"Output dir of running batch (default: {default_output}): ").strip() or default_output
    state_path = os.path.join(out_dir, STATE_FILE)
    pid_path = os.path.join(out_dir, PID_FILE)
    if not os.path.exists(state_path):
        print("State file not found – batch may not be running.")
        return
    try:
        pid = int(open(pid_path).read().strip()) if os.path.exists(pid_path) else None
    except Exception:
        pid = None
    try:
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            print("=== MetaMap Batch Dashboard ===")
            print(f"Output dir : {out_dir}")
            if pid:
                print(f"Main PID   : {pid}")
                if psutil and psutil.pid_exists(pid):
                    p = psutil.Process(pid)
                    cpu = p.cpu_percent(interval=0.5)
                    mem = p.memory_info().rss / (1024*1024)
                    print(f"CPU        : {cpu:.1f}%  |  RAM: {mem:.1f} MB")
                else:
                    print("Process not running.")
            with open(state_path) as f:
                state = json.load(f)
            total = state.get('total_overall', 0)
            done = state.get('completed_overall_markers', 0)
            pct = done/total*100 if total else 0
            print(f"Progress   : {done}/{total} ({pct:.1f}%)")
            active = state.get('active_workers_info', {})
            if active:
                print("Active workers:")
                for wid, info in active.items():
                    print(f"  {wid}: {info.get('status')} – {info.get('current_file','')}")
            else:
                print("No active workers recorded.")
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nExiting dashboard…")

def interactive_main_loop():
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stdout)
    
    # Initial config check only if config file doesn't exist or essential key is missing
    # This is less aggressive than always running configure_all_settings()
    if not get_config_path().exists() or not get_config_value("metamap_binary_path"):
        print("Performing initial essential configuration check...")
        configure_all_settings(is_reset=False) # Prompt for all if first time like
        # If still no metamap_binary_path, it would have exited in configure_all_settings

    while True:
        print("\n" + ASCII_BANNER)
        print("--- Python MetaMap Orchestrator ---")
        print("0. Install MetaMap (auto-download)")
        print("1. Configure All Settings (Re-prompt all)")
        print("2. Run MetaMap Batch Processing")
        print("3. Monitor Dashboard (live)")
        print("4. View Batch Progress")
        print("5. View/Tail Log File")
        print("6. List Pending/Completed Files")
        print("7. Reset All Saved Configurations to Defaults")
        print("8. Exit")
        try:
            choice = input("Enter your choice (0-8): ").strip()
        except EOFError:
            print("\nExiting due to EOF.")
            break
        if choice == '0':
            handle_install_metamap()
        elif choice == '1':
            configure_all_settings(is_reset=False)  # Re-prompt all
        elif choice == '2':
            handle_run_batch_processing()
        elif choice == '3':
            handle_monitor_dashboard()
        elif choice == '4':
            handle_view_progress()
        elif choice == '5':
            default_out = get_config_value("default_output_dir", "./output_csvs")
            log_out_dir = input(f"Enter output dir of the log (default: {default_out}): ").strip() or default_out
            if os.path.isdir(log_out_dir):
                log_fn = get_dynamic_log_filename(log_out_dir)
                log_fp = os.path.join(log_out_dir, log_fn)
                if os.path.exists(log_fp):
                    lines = input("How many lines to show from end (default 20)? ").strip() or "20"
                    if shutil.which("tail"):
                        subprocess.run(["tail", "-n", lines, log_fp])
                    else:
                        try:
                            with open(log_fp, "r", encoding="utf-8", errors="ignore") as _tf:
                                data_lines = _tf.readlines()[-int(lines):]
                            print("".join(data_lines))
                        except Exception as _tf_e:
                            print(f"Error displaying log tail: {_tf_e}")
                else:
                    print(f"Log file not found: {log_fp}")
            else:
                print(f"Directory not found: {log_out_dir}")
        elif choice == '6':
            default_out_p = get_config_value("default_output_dir", "./output_csvs")
            default_in_p = get_config_value("default_input_dir", "./input_notes")
            pend_out_dir = input(f"Output dir (def: {default_out_p}): ").strip() or default_out_p
            pend_in_dir = input(f"Input dir (def: {default_in_p}): ").strip() or default_in_p
            if os.path.isdir(pend_in_dir) and os.path.isdir(pend_out_dir):
                all_ins = gather_inputs(pend_in_dir)
                pending_fs = []
                completed_fs = []
                for f_i in all_ins:
                    base_f_i = os.path.basename(f_i)
                    out_csv_f_i = derive_output_csv_path(pend_out_dir, base_f_i)
                    if check_output_file_completion(out_csv_f_i, base_f_i):
                        completed_fs.append(base_f_i)
                    else:
                        pending_fs.append(base_f_i)
                print(f"\nPending ({len(pending_fs)}): ")
                [print(f"  {f}") for f in pending_fs[:10]]
                print("..." if len(pending_fs) > 10 else "")
                print(f"Completed ({len(completed_fs)}): ")
                [print(f"  {f}") for f in completed_fs[:10]]
                print("..." if len(completed_fs) > 10 else "")
            else:
                print("Invalid input/output dir for pending/completed.")
        elif choice == '7':
            handle_reset_settings()
        elif choice == '8':
            print("Exiting.")
            break
        else:
            print("Invalid choice. Please try again.")

def main():
    # Initial setup of basic console logging for any early messages or config prompts
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stdout)
    
    # Default to the interactive menu when no sub-command is supplied.
    if len(sys.argv) == 1:
        interactive_main_loop()
        sys.exit(0)

    # Legacy alias: `pymm-cli interactive` still works but is no longer required.
    if len(sys.argv) == 2 and sys.argv[1].lower() == 'interactive':
        interactive_main_loop()
        sys.exit(0)

    # --- Subcommand Processing --- 
    # Essential configs are checked first. This will prompt if they are missing.
    subcmd = sys.argv[1]

    # Defer METAMAP_BINARY_PATH check if the command is 'install'
    if subcmd.lower() != "install":
        configured_metamap_binary_path = get_config_value("metamap_binary_path")
        if not configured_metamap_binary_path: # Check after get_config_value for non-interactive
            print("Error: METAMAP_BINARY_PATH is not configured. Run 'pymm-cli' (interactive) or 'pymm-cli install' first.", file=sys.stderr)
            sys.exit(1)
    else:
        configured_metamap_binary_path = None # Will be set by install logic
    
    # Other global settings also fetched using get_config_value
    # This allows them to be influenced by config file or env vars before subcommand logic runs.
    global MAX_PARALLEL_WORKERS
    MAX_PARALLEL_WORKERS = int(get_config_value("max_parallel_workers", os.getenv("MAX_PARALLEL_WORKERS", str(MAX_PARALLEL_WORKERS_DEFAULT))))
    default_metamap_options = get_config_value("metamap_processing_options", METAMAP_PROCESSING_OPTIONS_DEFAULT)

    subcmd = sys.argv[1]
    output_dir_for_logging = None 
    if subcmd in {"start", "resume"} and len(sys.argv) == 4: output_dir_for_logging = os.path.abspath(os.path.expanduser(sys.argv[3]))
    elif subcmd in {"progress", "pid", "kill", "tail", "completed", "sample", "clearout", "badcsv"} and len(sys.argv) >= 3: output_dir_for_logging = os.path.abspath(os.path.expanduser(sys.argv[2]))
    elif subcmd == "pending" and len(sys.argv) == 4: output_dir_for_logging = os.path.abspath(os.path.expanduser(sys.argv[3]))

    if output_dir_for_logging:
        Path(output_dir_for_logging).mkdir(parents=True, exist_ok=True)
        log_file_name = get_dynamic_log_filename(output_dir_for_logging)
        log_path = os.path.join(output_dir_for_logging, log_file_name)
        for handler in logging.root.handlers[:]: logging.root.removeHandler(handler)
        logging.basicConfig(filename=log_path, level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    if subcmd in {"start", "resume"}:
        if len(sys.argv) != 4: logging.error(f"Error: {subcmd} command requires INPUT_DIR and OUTPUT_DIR arguments."); print(f"Usage: pymm-cli {subcmd} INPUT_DIR OUTPUT_DIR"); sys.exit(1)
        inp_dir_str, out_dir_str = sys.argv[2:4]
        # Auto-start MetaMap servers if needed (disabled)
        # ensure_servers_running()
        current_metamap_options = get_config_value("metamap_processing_options", default_metamap_options)
        execute_batch_processing(inp_dir_str, out_dir_str, subcmd, configured_metamap_binary_path, current_metamap_options)
    
    elif subcmd == "progress" and len(sys.argv) == 3: handle_view_progress(sys.argv[2]); sys.exit(0)
    
    elif subcmd == "pid" and len(sys.argv)==3:
        out = os.path.abspath(os.path.expanduser(sys.argv[2])); pid_path = os.path.join(out, PID_FILE)
        if os.path.exists(pid_path): print(open(pid_path, 'r').read().strip())
        else: logging.error("PID file not found"); print("PID file not found"); sys.exit(1)
        sys.exit(0)

    elif subcmd == "tail" and len(sys.argv) in {3,4}:
        out_dir_tail = os.path.abspath(os.path.expanduser(sys.argv[2])); lines_to_tail = sys.argv[3] if len(sys.argv) == 4 else "20"
        log_file_name_tail = get_dynamic_log_filename(out_dir_tail); log_file_path_tail = os.path.join(out_dir_tail, log_file_name_tail)
        if os.path.exists(log_file_path_tail):
            if shutil.which("tail"):
                subprocess.run(["tail", "-n", lines_to_tail, log_file_path_tail])
            else:
                try:
                    with open(log_file_path_tail, "r", encoding="utf-8", errors="ignore") as _tf:
                        data_lines = _tf.readlines()[-int(lines_to_tail):]
                    print("".join(data_lines))
                except Exception as _tf_e:
                    print(f"Error displaying log tail: {_tf_e}")
        else: print(f"Log file not found: {log_file_path_tail}"); sys.exit(1)
        sys.exit(0)
    
    elif subcmd == "kill" and len(sys.argv)==3:
        out_kill = os.path.abspath(os.path.expanduser(sys.argv[2]))
        pid_path_kill = os.path.join(out_kill, PID_FILE)
        if not os.path.exists(pid_path_kill): print("PID file not found, cannot kill."); sys.exit(1)
        try:
            with open(pid_path_kill, 'r') as f_pid_kill: pid_to_kill = int(f_pid_kill.read().strip())
            os.kill(pid_to_kill, 15) # SIGTERM
            print(f"Sent SIGTERM to process {pid_to_kill}")
        except ValueError: print(f"Invalid PID in PID file: {pid_path_kill}")
        except ProcessLookupError : print(f"Process {pid_to_kill if 'pid_to_kill' in locals() else 'from PID file'} not found.")
        except OSError as e_kill: print(f"Error killing process: {e_kill}")
        sys.exit(0)

    elif subcmd == "validate" and len(sys.argv) == 3: # Ensure this block exists or is re-added correctly
        inp = os.path.abspath(os.path.expanduser(sys.argv[2]))
        files = gather_inputs(inp)
        bad_files_summary = [] 
        total_bytes_read = 0; successfully_read_count = 0
        if not files: print(f"No .txt files found in: {inp}"); sys.exit(0)
        print(f"Validating {len(files)} files in {inp}...")
        for f_path_str in files:
            try: 
                with open(f_path_str, 'r', encoding='utf-8') as f_obj: content = f_obj.read()
                total_bytes_read += len(content.encode('utf-8')); successfully_read_count +=1
            except UnicodeDecodeError: logging.warning(f"Encoding error: {f_path_str}"); bad_files_summary.append(f_path_str)
            except IOError as e_io: logging.warning(f"IOError {f_path_str}: {e_io}"); bad_files_summary.append(f_path_str)
            except Exception as e_other: logging.warning(f"Unexpected error {f_path_str}: {e_other}"); bad_files_summary.append(f_path_str)
        print(f"\n--- Validation Summary for {inp} ---")
        print(f"  Total files: {len(files)}, Successfully read: {successfully_read_count}, Issues: {len(bad_files_summary)}")
        print(f"  Total size (read files): {total_bytes_read / 1e6:.2f} MB")
        if bad_files_summary: print("\n--- Files with Issues (first 20) ---"); [print(f"  {f}") for f in bad_files_summary[:20]];
        if len(bad_files_summary) > 20: print(f"  ...and {len(bad_files_summary) - 20} more.")
        sys.exit(0)

    elif subcmd == "install":
        # New sub-command: download & install MetaMap automatically using bundled helper script
        try:
            from . import install_metamap
        except ImportError as ie_imp:
            print(f"Error: Could not import install_metamap module: {ie_imp}", file=sys.stderr)
            sys.exit(1)
        installed_binary_path = install_metamap.main()
        if installed_binary_path and os.path.isfile(installed_binary_path):
            print(f"MetaMap installed successfully at: {installed_binary_path}")
            # Persist the discovered path into the user config so future runs pick it up automatically
            set_config_value("metamap_binary_path", installed_binary_path)
            sys.exit(0)
        else:
            print("MetaMap installation did not complete successfully or binary not found.", file=sys.stderr)
            sys.exit(1)

    else:
        if not (len(sys.argv) <= 1 or (len(sys.argv) == 2 and sys.argv[1].lower() == 'interactive')):
            logging.error(f"Invalid command or arguments: {' '.join(sys.argv)}")
            print(f"Invalid command or arguments: {' '.join(sys.argv)}")
            print("Run 'pymm-cli' or 'pymm-cli interactive' for interactive mode or help.")
        sys.exit(1)

if __name__ == "__main__":
    import multiprocessing as _mp
    # Required on Windows when using ProcessPoolExecutor
    _mp.freeze_support()
    main() 