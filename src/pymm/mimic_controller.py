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
import shlex
import socket

# Check and install dependencies
def check_and_install_dependencies():
    """Checks if required dependencies are installed and installs them if missing."""
    required_packages = ['psutil', 'colorama', 'tqdm', 'rich']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"Missing required dependencies: {', '.join(missing_packages)}")
        try:
            choice = input("Install missing dependencies now? (yes/no): ").strip().lower()
            if choice == 'yes':
                print("Installing missing dependencies...")
                subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing_packages)
                print("Dependencies installed successfully.")
                # Re-import the packages
                for package in missing_packages:
                    try:
                        __import__(package)
                    except ImportError as e:
                        print(f"Failed to import {package} after installation: {e}")
            else:
                print("Warning: Missing dependencies may cause functionality issues.")
        except Exception as e:
            print(f"Error installing dependencies: {e}")
    
    # Import psutil after ensuring it's installed
    try:
        global psutil
        import psutil
    except ImportError:
        print("Warning: psutil module is required for process monitoring.")

# --- Configuration File Handling & Constants ---
CONFIG_FILE_NAME = ".pymm_controller_config.json"
METAMAP_PROCESSING_OPTIONS_DEFAULT = "-y -Z 2020AA --lexicon db" 
MAX_PARALLEL_WORKERS_DEFAULT = 4
DEFAULT_PYMM_TIMEOUT = 300  # 5 minutes in seconds
DEFAULT_JAVA_HEAP_SIZE = "4g"  # Default to 4GB heap

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
    prompt_and_save_config_value("pymm_timeout", "Per-file MetaMap timeout (seconds)",
                                  explanation="How long MetaMap is allowed to run on a single note before being killed.",
                                  code_default=str(DEFAULT_PYMM_TIMEOUT))
    prompt_and_save_config_value("java_heap_size", "Java heap size for MetaMap (e.g., 4g, 16g, 100g)",
                                 explanation="Maximum memory allocation for MetaMap's Java process. For large notes, increase this value.",
                                 code_default=DEFAULT_JAVA_HEAP_SIZE)
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

# Position handling simplified: emit a single "Position" column which carries
# concept‐level coordinates when available, otherwise phrase‐level coordinates.

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
    
    print("Launching MetaMap servers...")
    
    # Start Tagger
    print("Starting SKR/MedPost tagger...")
    result_skr = subprocess.run([skr_ctl, "start"], capture_output=True, text=True)
    if result_skr.returncode == 0:
        print("SKR/MedPost tagger started successfully")
    else:
        print(f"Error starting SKR/MedPost tagger: {result_skr.stderr}")
    time.sleep(2)  # Give it time to start

    # Start WSD
    print("Starting WSD server...")
    result_wsd = subprocess.run([wsd_ctl, "start"], capture_output=True, text=True)
    if result_wsd.returncode == 0:
        print("WSD server start command issued")
    else:
        print(f"Error starting WSD server: {result_wsd.stderr}")
    
    # Wait for WSD server to actually start
    max_attempts = 5
    for attempt in range(max_attempts):
        time.sleep(2)  # Give it time to start
        if is_wsd_server_running():
            print("WSD server is running on port 5554")
            break
        elif attempt < max_attempts - 1:
            print(f"Waiting for WSD server to start (attempt {attempt+1}/{max_attempts})...")
        else:
            print("WARNING: WSD server does not appear to be running after multiple attempts")
            print("This may cause errors during MetaMap processing")
    
    # Start mmserver20
    print("Starting mmserver20...")
    try:
        # Check if already running
        existing = subprocess.run(["pgrep", "-f", "mmserver20"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        if existing.returncode != 0:
            # Start the server in background
            subprocess.Popen([mmserver], cwd=bin_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("mmserver20 launched in background")
            time.sleep(2)  # Give it time to start
        else:
            print("mmserver20 is already running")
    except Exception as e:
        print(f"Error starting mmserver20: {e}")
    
    print("Start commands issued. Use 'Status' to verify.")
    # Show status after starting
    status_metamap_servers(public_mm_dir)

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
        # Double-check if WSD server is actually accessible via network
        wsd_running = is_wsd_server_running()
        if wsd_running:
            print("WSD server is accessible on port 5554")
        else:
            print("WARNING: WSD server process might be running but is not accessible on port 5554")
            print("This will cause errors in MetaMap processing")
    else:
        print(f"wsdserverctl not found at {wsd_ctl}")

    print("--- MMServer20 Status (via pgrep) ---")
    if running_mmservers:
        print("mmserver20 running – PIDs:", ", ".join(running_mmservers))
    else:
        print("mmserver20 not detected in process list.")

def kill_all_metamap_processes():
    """Kill all MetaMap-related processes and Python workers.
    
    This function tries to kill:
    1. All metamap and mmserver processes
    2. Python processes running mimic_controller that are part of a batch
    3. Related worker processes
    
    Returns a count of processes terminated.
    """
    terminated_count = 0
    
    try:
        # First try Unix-style commands
        print("Attempting to kill all MetaMap processes...")
        
        # Kill all metamap processes
        try:
            result = subprocess.run(["pkill", "-f", "metamap"], 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE)
            if result.returncode == 0:
                print("Successfully terminated metamap processes")
                terminated_count += 1
        except Exception as e:
            print(f"Error killing metamap processes: {e}")

        # Kill all mmserver processes
        try:
            result = subprocess.run(["pkill", "-f", "mmserver"], 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE)
            if result.returncode == 0:
                print("Successfully terminated mmserver processes")
                terminated_count += 1
        except Exception as e:
            print(f"Error killing mmserver processes: {e}")
            
        # Kill Python processes running batch processing
        try:
            result = subprocess.run(["pkill", "-f", "mimic_controller.py"], 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE)
            if result.returncode == 0:
                print("Successfully terminated Python mimic_controller processes")
                terminated_count += 1
        except Exception as e:
            print(f"Error killing Python processes: {e}")
            
        # On Windows, try using taskkill
        if sys.platform == "win32":
            try:
                subprocess.run(["taskkill", "/F", "/IM", "metamap*"], 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE)
                subprocess.run(["taskkill", "/F", "/IM", "mmserver*"], 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE)
                print("Attempted to kill MetaMap processes using Windows taskkill")
                terminated_count += 1
            except Exception as e:
                print(f"Error using Windows taskkill: {e}")
                
        print(f"Kill operation completed. Terminated {terminated_count} process groups.")
        return terminated_count
        
    except Exception as e:
        print(f"Error during kill operation: {e}")
        return 0

def clear_output_directory(output_dir):
    """Clear all files in the output directory to start fresh.
    
    This removes:
    - All CSV files (the results)
    - PID file
    - State file (.mimic_state.json)
    - Log files
    
    Returns the count of files removed.
    """
    if not os.path.exists(output_dir):
        print(f"Output directory does not exist: {output_dir}")
        return 0
        
    if not os.path.isdir(output_dir):
        print(f"Not a directory: {output_dir}")
        return 0
    
    files_removed = 0
    
    try:
        print(f"Clearing output directory: {output_dir}")
        
        # First remove specific management files
        special_files = [
            os.path.join(output_dir, STATE_FILE),
            os.path.join(output_dir, PID_FILE),
            os.path.join(output_dir, "pymm_run.log"),
            os.path.join(output_dir, get_dynamic_log_filename(output_dir))
        ]
        
        for file_path in special_files:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"Removed: {file_path}")
                    files_removed += 1
                except Exception as e:
                    print(f"Error removing {file_path}: {e}")
        
        # Then remove all CSV files
        for file_name in os.listdir(output_dir):
            if file_name.endswith(".csv"):
                file_path = os.path.join(output_dir, file_name)
                try:
                    os.remove(file_path)
                    files_removed += 1
                except Exception as e:
                    print(f"Error removing {file_path}: {e}")
        
        # Also remove the error_files subdirectory if it exists
        error_dir = os.path.join(output_dir, ERROR_FOLDER)
        if os.path.exists(error_dir) and os.path.isdir(error_dir):
            try:
                shutil.rmtree(error_dir)
                print(f"Removed error directory: {error_dir}")
                files_removed += 1
            except Exception as e:
                print(f"Error removing error directory {error_dir}: {e}")
        
        print(f"Cleanup completed. Removed {files_removed} files/directories.")
        return files_removed
        
    except Exception as e:
        print(f"Error during cleanup: {e}")
        return files_removed

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
    """Ensure SKR tagger, WSD server and mmserver20 are running.

    If they are not running, try to start them.  Works only on Unix-like
    systems where the standard MetaMap control scripts are available.
    """

    public_mm_dir = None
    binary_path = get_config_value("metamap_binary_path") or os.getenv("METAMAP_BINARY_PATH")
    if binary_path:
        public_mm_dir = os.path.abspath(os.path.join(os.path.dirname(binary_path), os.pardir))
        if not os.path.isdir(public_mm_dir):
            public_mm_dir = None

    if not public_mm_dir:
        print("ensure_servers_running: cannot locate public_mm directory – skipping server check.")
        return

    bin_path = os.path.join(public_mm_dir, "bin")
    skr_ctl = os.path.join(bin_path, "skrmedpostctl")
    wsd_ctl = os.path.join(bin_path, "wsdserverctl")
    mmserver = os.path.join(bin_path, "mmserver20")

    # Always start servers that are needed rather than just checking
    print("Starting MetaMap servers...")
    
    # Tagger
    if os.path.isfile(skr_ctl):
        print("Starting SKR/MedPost tagger...")
        subprocess.run([skr_ctl, "start"], cwd=bin_path)
        time.sleep(2)  # Give it time to start

    # WSD
    if os.path.isfile(wsd_ctl):
        print("Starting WSD server...")
        subprocess.run([wsd_ctl, "start"], cwd=bin_path)
        time.sleep(2)  # Give it time to start

    # mmserver20 (fire-and-forget)
    if os.path.isfile(mmserver):
        print("Launching mmserver20 in background...")
        try:
            # Check if mmserver is already running
            existing = subprocess.run(["pgrep", "-f", "mmserver20"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            if existing.returncode != 0:
                # Start the server in background
                subprocess.Popen([mmserver], cwd=bin_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(2)  # Give it time to start
            else:
                print("mmserver20 already running")
        except Exception as e:
            print(f"Error starting mmserver20: {e}")
    
    # Verify all servers are running
    print("Verifying server status...")
    status_metamap_servers(public_mm_dir)

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

def check_output_file_completion(output_csv_path, input_file_original_stem_for_marker, state=None):
    """Check an output CSV file to determine if it has proper start and end markers.
    
    Parameters
    ----------
    output_csv_path : str
        Path to the output CSV file to check.
    input_file_original_stem_for_marker : str
        The original input filename used to construct expected markers.
    state : dict, optional
        Current state dictionary for tracking retries.
        
    Returns
    -------
    bool
        True if the file appears to be complete, False otherwise.
    """
    if not os.path.exists(output_csv_path):
        return False
    try:
        # Find the first line containing the start marker and the last line containing the end marker
        first_line_str = ""; last_line_str = ""
        
        try:
            with open(output_csv_path, 'rb') as fh:
                # Read first line
                first_line_bytes = fh.readline()
                if first_line_bytes:
                    first_line_str = first_line_bytes.decode('utf-8', 'replace').strip()
                
                # Read a buffer from the end for last line (avoiding reading entire file)
                lines_in_buffer = []
                chunk_size = 4096  # 4 KB
                fh.seek(0, os.SEEK_END)
                filesize = fh.tell()
                
                if filesize > chunk_size:
                    fh.seek(max(0, filesize - chunk_size))
                    # Read to discard partial line if not at start of file
                    if filesize > chunk_size: 
                        fh.readline()
                    # Now read complete lines
                    lines_in_buffer = fh.readlines()
                else:
                    # Small file - read it all
                    fh.seek(0)
                    lines_in_buffer = fh.readlines()
                
                if lines_in_buffer:
                    last_line_str = lines_in_buffer[-1].decode('utf-8', 'replace').strip()
                elif first_line_str:
                    logging.warning("Could not determine last line via buffer (binary mode) for {path}. First line was: '{fl}'".format(path=output_csv_path, fl=first_line_str))
        except Exception as e:
            logging.warning(f"Error reading file {output_csv_path} in binary mode: {e}")
            
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
        header_regex = r'^"?' + r'"?,"?'.join(re.escape(h) for h in CSV_HEADER) + r'"?$'
        if re.match(header_regex, last_line_str):
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
                
                # Handle retry logic if state is provided
                if state is not None:
                    # Get or initialize retry count
                    retry_key = f"retry_{input_file_original_stem_for_marker}"
                    retry_count = state.get(retry_key, 0) + 1
                    state[retry_key] = retry_count
                    
                    # If we've reached max retries, move to failed_files directory
                    if retry_count >= 3:
                        out_dir = os.path.dirname(output_csv_path)
                        failed_dir = os.path.join(out_dir, "failed_files")
                        os.makedirs(failed_dir, exist_ok=True)
                        
                        failed_path = os.path.join(failed_dir, os.path.basename(output_csv_path))
                        try:
                            shutil.copy2(output_csv_path, failed_path)
                            os.remove(output_csv_path)  # Remove the original after successful copy
                            logging.error(f"File {input_file_original_stem_for_marker} failed after 3 retries, moved to failed_files directory")
                            return False
                        except Exception as e:
                            logging.error(f"Error moving file to failed directory: {e}")
                
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

def ensure_logging_setup(output_dir, force=False):
    """Force-create a log file in the output directory and verify it's writable.
    
    Returns the path to the log file if successful, None otherwise.
    """
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir, exist_ok=True)
            print(f"Created output directory: {output_dir}")
        except Exception as e:
            print(f"ERROR: Failed to create output directory {output_dir}: {e}")
            return None
    
    log_filename = get_dynamic_log_filename(output_dir)
    log_path = os.path.join(output_dir, log_filename)
    
    try:
        # Create/touch log file to ensure it exists and is writable
        with open(log_path, 'a') as f:
            f.write(f"# Log initialized on {datetime.now().isoformat()}\n")
        
        # Setup or re-setup logging
        root_logger = logging.getLogger()
        if force:
            # Remove existing handlers
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
        
        # Add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)
        
        # Add file handler - use basic implementation to avoid _safe_add_file_handler issues
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        file_handler.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)
        
        root_logger.setLevel(logging.INFO)
        logging.info(f"Logging successfully initialized to {log_path}")
        print(f"Logging to: {log_path}")
        return log_path
    except Exception as e:
        print(f"ERROR: Failed to set up logging to {log_path}: {e}")
        return None

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
    
    # Prepare environment variables once for the entire worker
    try:
        # Ensure worker respects specific MetaMap options
        original_env = os.environ.copy()
        if current_metamap_options:
            # Deduplicate options like --lexicon db to avoid "overridden" warning
            os.environ["METAMAP_PROCESSING_OPTIONS"] = deduplicate_metamap_options(current_metamap_options)
            
        # Set Java heap size if configured
        java_heap_size = get_config_value("java_heap_size", os.getenv("JAVA_HEAP_SIZE", DEFAULT_JAVA_HEAP_SIZE))
        if java_heap_size:
            os.environ["JAVA_HEAP_SIZE"] = java_heap_size
            logging.info(f"[{worker_id}] Setting Java heap size to {java_heap_size}")
    except Exception as e:
        logging.error(f"[{worker_id}] Failed to set environment variables: {e}")
    
    mm = None
    
    for input_file_path_str in files_for_worker:
        input_file_basename = os.path.basename(input_file_path_str)
        output_csv_path = derive_output_csv_path(main_out_dir, input_file_basename)
        processing_error_occurred = False; duration_ms = 0
        start_time = time.time()
        
        try:
            # Close previous MetaMap instance if exists to ensure clean state
            if mm is not None:
                try:
                    mm.close()
                except Exception as e_close:
                    logging.warning(f"[{worker_id}] Error closing previous MetaMap instance: {e_close}")
                mm = None
            
            # Create a fresh MetaMap instance for each file
            try:
                mm = PyMetaMap(current_metamap_binary_path, debug=True)
                logging.info(f"[{worker_id}] Created new MetaMap instance for {input_file_basename}")
            except Exception as e_mm:
                logging.error(f"[{worker_id}] Failed to initialize PyMetaMap for '{input_file_basename}': {e_mm}")
                processing_error_occurred = True
                continue
            
            with open(input_file_path_str, 'r', encoding='utf-8') as f_in: 
                whole_note = f_in.read().strip()
            lines = [whole_note] if whole_note else []
            
            if not lines:
                logging.info(f"[{worker_id}] Input file {input_file_basename} is empty. Skipping.")
                results.append((input_file_basename, "0ms", False))
                with open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:
                    f_out.write(f"{START_MARKER_PREFIX}{input_file_basename}\n")
                    csv.writer(f_out, quoting=csv.QUOTE_ALL, doublequote=True).writerow(CSV_HEADER)
                # End marker written in finally
                continue
            
            pymm_timeout = int(get_config_value("pymm_timeout", os.getenv("PYMM_TIMEOUT", str(DEFAULT_PYMM_TIMEOUT))))
            logging.info(f"[{worker_id}] Processing {input_file_basename} with timeout {pymm_timeout}s")
            mmos_iter = mm.parse(lines, timeout=pymm_timeout)
            
            # Check if we received an empty result due to XML parsing error
            if not mmos_iter and hasattr(mmos_iter, '__len__') and len(mmos_iter) == 0:
                logging.warning(f"[{worker_id}] XML parsing error for {input_file_basename} - returning empty concept list")
                
                # Check if WSD server is running - this might be the cause of the issue
                wsd_running = is_wsd_server_running()
                if not wsd_running:
                    logging.error(f"[{worker_id}] WSD server is not running on port 5554 - this is likely causing XML parsing errors")
                    print(f"\n[{worker_id}] ERROR: WSD Server is not running! Attempting to restart it...")
                    
                    # Try to restart WSD server automatically
                    binary_path = get_config_value("metamap_binary_path")
                    if binary_path:
                        public_mm_dir = os.path.abspath(os.path.join(os.path.dirname(binary_path), os.pardir))
                        if os.path.isdir(public_mm_dir):
                            bin_path = os.path.join(public_mm_dir, "bin")
                            wsd_ctl = os.path.join(bin_path, "wsdserverctl")
                            if os.path.isfile(wsd_ctl):
                                print(f"[{worker_id}] Restarting WSD server...")
                                subprocess.run([wsd_ctl, "restart"], check=False)
                                # Give it time to start
                                max_attempts = 3
                                for attempt in range(max_attempts):
                                    time.sleep(3)
                                    if is_wsd_server_running():
                                        print(f"[{worker_id}] WSD server restarted successfully")
                                        logging.info(f"[{worker_id}] WSD server restarted successfully")
                                        break
                                    elif attempt < max_attempts - 1:
                                        print(f"[{worker_id}] Waiting for WSD server (attempt {attempt+1}/{max_attempts})...")
                
                # Write a valid CSV file anyway with just the headers
                with open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:
                    f_out.write(f"{START_MARKER_PREFIX}{input_file_basename}\n")
                    writer = csv.writer(f_out, quoting=csv.QUOTE_ALL, doublequote=True)
                    writer.writerow(CSV_HEADER)
                # Continue with the next file - don't treat as error
                continue
                
            concepts_list = [concept for mmo_item in mmos_iter for concept in mmo_item]
            logging.info(f"[{worker_id}] Found {len(concepts_list)} concepts in {input_file_basename}")

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
                    
                    logging.debug(f"[{worker_id}] Processed {len(concepts_to_write)} concepts in {len(concepts_by_utterance)} utterances for {input_file_basename}.")
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
                            # ------------------------------------------------------------------
                            # 1.  Concept-level (preferred) coordinates
                            # ------------------------------------------------------------------
                            pos_concept = ""
                            if concept.pos_start is not None:
                                try:
                                    uid_cur = getattr(concept, 'utterance_id', None)
                                    if uid_cur is not None and uid_cur in utterance_offset_map:
                                        rel_start = concept.pos_start - utterance_offset_map[uid_cur] + 1
                                        _len_val = concept.pos_length if concept.pos_length is not None else len(phrase_text or "")
                                        pos_concept = f"{rel_start}:{_len_val}"
                                    else:
                                        _len_val = concept.pos_length if concept.pos_length is not None else len(phrase_text or "")
                                        pos_concept = f"{concept.pos_start}:{_len_val}"
                                except Exception:
                                    pos_concept = ""

                            # ------------------------------------------------------------------
                            # 2.  Phrase-level coordinates (always available)
                            # ------------------------------------------------------------------
                            pos_phrase = ""
                            if concept.phrase_start is not None:
                                try:
                                    uid_cur = getattr(concept, 'utterance_id', None)
                                    if uid_cur is not None and uid_cur in utterance_offset_map:
                                        rel_start_p = concept.phrase_start - utterance_offset_map[uid_cur] + 1
                                        _len_val_p = concept.phrase_length if concept.phrase_length is not None else len(phrase_text or "")
                                        pos_phrase = f"{rel_start_p}:{_len_val_p}"
                                    else:
                                        _len_val_p = concept.phrase_length if concept.phrase_length is not None else len(phrase_text or "")
                                        pos_phrase = f"{concept.phrase_start}:{_len_val_p}"
                                except Exception:
                                    pos_phrase = ""

                            # Fallback – look-up by text if anything is missing
                            if (not pos_phrase or not pos_concept) and phrase_text:
                                utt_text_lookup = utterance_texts.get(getattr(concept, 'utterance_id', None), whole_note)
                                idx_f = utt_text_lookup.find(phrase_text)
                                if idx_f >= 0:
                                    cr_adjust = utt_text_lookup.count('\n', 0, idx_f)
                                    derived = f"{idx_f + cr_adjust + 1}:{len(phrase_text)}"
                                    if not pos_phrase:
                                        pos_phrase = derived
                                    if not pos_concept:
                                        pos_concept = derived

                            position_value = pos_concept or pos_phrase

                            # --- Final whole-note fallback if still empty ---
                            if not position_value and phrase_text:
                                try:
                                    flat_note = re.sub(r'\s+', ' ', whole_note.lower())
                                    flat_phrase = re.sub(r'\s+', ' ', phrase_text.lower())
                                    idx_glob = flat_note.find(flat_phrase)
                                    if idx_glob >= 0:
                                        position_value = f"{idx_glob + 1}:{len(flat_phrase)}"
                                except Exception:
                                    pass

                            # Collect debugging info when position is still missing
                            if not position_value:
                                try:
                                    dbg_path = os.path.join(main_out_dir, "_missing_position_debug.csv")
                                    with open(dbg_path, 'a', newline='', encoding='utf-8') as dbg_fh:
                                        csv.writer(dbg_fh).writerow([input_file_basename, concept.cui, phrase_text[:120]])
                                except Exception:
                                    pass

                            row_data = [
                                concept.cui,
                                concept.score,
                                concept_name,
                                pref_name,
                                phrase_text,
                                sem_types_formatted,
                                sources_formatted,
                                position_value,
                            ]
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
    
    # Clean up final MetaMap instance
    if mm is not None:
        try:
            mm.close()
        except Exception:
            pass
    
    logging.info(f"[{worker_id}] Finished processing batch of {len(files_for_worker)} files.")
    return results

def deduplicate_metamap_options(options_str):
    """Remove duplicate options like --lexicon db to avoid 'overridden' warnings"""
    options = shlex.split(options_str)
    result = []
    skip_next = False
    seen_options = set()
    
    for i, opt in enumerate(options):
        if skip_next:
            skip_next = False
            continue
            
        # Handle option-argument pairs
        if opt == "--lexicon" or opt == "-Z" or opt == "--year":
            # If we've seen this option before, skip it and its argument
            if opt in seen_options:
                if i+1 < len(options):  # Make sure there's an argument to skip
                    skip_next = True
                continue
            seen_options.add(opt)
            result.append(opt)
            # Add the argument too
            if i+1 < len(options):
                result.append(options[i+1])
                skip_next = True
        else:
            # For standalone options
            if opt not in seen_options:
                seen_options.add(opt)
                result.append(opt)
    
    return " ".join(result)

# --- Path Normalization Helper ---
def _normalize_path_for_os(path_str):
    if sys.platform == "win32" and isinstance(path_str, str):
        # /mnt/c/Users -> C:\\Users
        match_mnt = re.match(r"/mnt/([a-zA-Z])/(.*)", path_str)
        if match_mnt:
            drive = match_mnt.group(1).upper()
            rest_of_path = match_mnt.group(2)
            # Ensure correct Windows path separators using os.sep
            return f"{drive}:{os.sep}{rest_of_path.replace('/', os.sep)}"
        # /c/Users -> C:\\Users (common in MSYS/Git Bash contexts)
        match_drive_letter = re.match(r"/([a-zA-Z])/(.*)", path_str)
        if match_drive_letter:
            drive = match_drive_letter.group(1).upper()
            rest_of_path = match_drive_letter.group(2)
            # Ensure correct Windows path separators using os.sep
            return f"{drive}:{os.sep}{rest_of_path.replace('/', os.sep)}"
    return path_str # Return original if not Windows or no known pattern matches
# --- End Path Normalization Helper ---

# --- Logging Helper --------------------------------------------------------
def _safe_add_file_handler(log_path: str):
    """Attach a FileHandler to the root logger.

    Ensures *log_path*'s directory exists. If creation or handler
    instantiation fails, it falls back to a file named
    ``.pymm_fallback.log`` located in the *same* directory.  When that
    also fails, the error is logged to console and the function returns
    without raising – the application continues with console-only
    logging.
    """

    root_logger = logging.getLogger()

    try:
        Path(os.path.dirname(log_path)).mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
        root_logger.addHandler(fh)
        logging.info("Logging to %s", log_path)
        return
    except Exception as primary_err:
        # First remediation: if path starts with /mnt/<drive>/ or /<drive>/ (common in WSL paths)
        win_conv_path = None
        try:
            mnt_match = re.match(r"^/mnt/([a-zA-Z])/(.*)", log_path)
            if mnt_match:
                win_conv_path = f"{mnt_match.group(1).upper()}:{os.sep}{mnt_match.group(2).replace('/', os.sep)}"
            else:
                drive_match = re.match(r"^/([a-zA-Z])/(.*)", log_path)
                if drive_match:
                    win_conv_path = f"{drive_match.group(1).upper()}:{os.sep}{drive_match.group(2).replace('/', os.sep)}"
        except Exception:
            win_conv_path = None

        if win_conv_path:
            try:
                Path(os.path.dirname(win_conv_path)).mkdir(parents=True, exist_ok=True)
                fh_conv = logging.FileHandler(win_conv_path)
                fh_conv.setLevel(logging.INFO)
                fh_conv.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
                root_logger.addHandler(fh_conv)
                logging.warning("Converted WSL-style path '%s' to Windows path and logging to: %s", log_path, win_conv_path)
                return
            except Exception:
                # Fall through to generic fallback
                pass

        # Generic fallback: write to hidden log in same (possibly failing) directory; if that still fails, use CWD
        fallback_path = os.path.join(os.path.dirname(log_path), ".pymm_fallback.log")
        try:
            Path(os.path.dirname(fallback_path)).mkdir(parents=True, exist_ok=True)
            fh_fb = logging.FileHandler(fallback_path)
            fh_fb.setLevel(logging.INFO)
            fh_fb.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
            root_logger.addHandler(fh_fb)
            logging.warning("Primary log file '%s' could not be opened (%s). Using fallback '%s'.",
                            log_path, primary_err, fallback_path)
        except Exception as fallback_err:
            # Final attempt: use CWD
            try:
                cwd_fallback = os.path.join(os.getcwd(), ".pymm_fallback.log")
                fh_cwd = logging.FileHandler(cwd_fallback)
                fh_cwd.setLevel(logging.INFO)
                fh_cwd.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))
                root_logger.addHandler(fh_cwd)
                logging.error("Both primary ('%s') and output-dir fallback ('%s') failed (%s). Logging to CWD: %s", log_path, fallback_path, fallback_err, cwd_fallback)
            except Exception as final_err:
                logging.error("All attempts to create a file handler failed (%s). Continuing without file logging.", final_err)

def execute_batch_processing(inp_dir_str, out_dir_str, mode, global_metamap_binary_path, global_metamap_options):
    # Ensure logging is set up first thing
    ensure_logging_setup(out_dir_str)

    logging.info(f"Executing batch processing. Mode: {mode}, Input: {inp_dir_str}, Output: {out_dir_str}")
    logging.info(f"  MetaMap Binary: {global_metamap_binary_path}")
    logging.info(f"  MetaMap Options: {global_metamap_options}")

    inp_dir_orig = os.path.abspath(os.path.expanduser(inp_dir_str))
    # Normalize out_dir after abspath for OS compatibility in file operations
    out_dir_raw = os.path.abspath(os.path.expanduser(out_dir_str))
    out_dir = _normalize_path_for_os(out_dir_raw)
    # Make sure the potentially normalized directory exists
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    pid_path_raw = os.path.join(out_dir_raw, PID_FILE) # Use raw path for state file consistent with user input if needed
    pid_path = _normalize_path_for_os(pid_path_raw)
    try:
        with open(pid_path, 'w') as f_pid: f_pid.write(str(os.getpid()))
    except Exception as e_pid:
        logging.warning(f"Could not write PID to {pid_path} (raw: {pid_path_raw}): {e_pid}")

    all_input_files_orig_str = gather_inputs(inp_dir_orig)
    if not all_input_files_orig_str:
        logging.warning(f"No input .txt files found in {inp_dir_orig}. Nothing to do.")
        print(f"No input .txt files found in {inp_dir_orig}.")
        return

    state = load_state(out_dir) or {}
    files_to_process_paths = []
    completed_files_count_initial = 0
    
    # Create failed files directory
    failed_dir = os.path.join(out_dir, "failed_files")
    os.makedirs(failed_dir, exist_ok=True)
    
    logging.info(f"Scanning for already completed files in {out_dir}...")
    for input_file_str_path in all_input_files_orig_str:
        input_file_basename = os.path.basename(input_file_str_path)
        output_csv_path_str = derive_output_csv_path(out_dir, input_file_basename)
        if check_output_file_completion(output_csv_path_str, input_file_basename, state):
            completed_files_count_initial += 1
        else:
            if os.path.exists(output_csv_path_str):
                # Move to error folder if it's an existing but incomplete file
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
                # Pass the potentially normalized out_dir to workers
                normalized_args_tuple = (args_tuple[0], args_tuple[1], out_dir, args_tuple[3], args_tuple[4])
                future = executor.submit(process_files_with_pymm_worker, *normalized_args_tuple)
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
    log_file_name = get_dynamic_log_filename(out_dir_raw)
    log_primary_path = _normalize_path_for_os(os.path.join(out_dir_raw, log_file_name))

    # Avoid duplicate handlers across multiple calls
    root_logger = logging.getLogger()
    normalized_target = os.path.normcase(os.path.normpath(log_primary_path))
    already = any(
        isinstance(h, logging.FileHandler) and os.path.normcase(os.path.normpath(getattr(h, "baseFilename", ""))) == normalized_target
        for h in root_logger.handlers
    )
    if not already:
        _safe_add_file_handler(log_primary_path)

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
    
    # Force log setup right here to ensure logging works
    log_path = ensure_logging_setup(out_dir, force=True)
    if not log_path:
        print("WARNING: Failed to set up logging. Processing will continue but may not produce log files.")

    metamap_opts = get_config_value("metamap_processing_options", METAMAP_PROCESSING_OPTIONS_DEFAULT)
    current_max_workers = int(get_config_value("max_parallel_workers", str(MAX_PARALLEL_WORKERS_DEFAULT)))
    timeout_value = int(get_config_value("pymm_timeout", os.getenv("PYMM_TIMEOUT", str(DEFAULT_PYMM_TIMEOUT))))
    java_heap_size = get_config_value("java_heap_size", os.getenv("JAVA_HEAP_SIZE", DEFAULT_JAVA_HEAP_SIZE))
    # The global MAX_PARALLEL_WORKERS is used by execute_batch_processing, ensure it reflects current config
    global MAX_PARALLEL_WORKERS
    MAX_PARALLEL_WORKERS = current_max_workers

    print(f"MetaMap Options: '{metamap_opts}'")
    print(f"Max Parallel Workers: {MAX_PARALLEL_WORKERS}")
    print(f"Timeout per file: {timeout_value} seconds")
    print(f"Java heap size: {java_heap_size}")

    # Show nohup command for background processing
    script_name = os.path.basename(sys.argv[0])
    if script_name == "pymm-cli" or script_name == "pymm":
        nohup_cmd = f"nohup {script_name} start {inp_dir} {out_dir} > {out_dir}/pymm_run.log 2>&1 &"
        print("\nTo run in background with nohup, use this command:")
        print(f"  {nohup_cmd}")
        print("\nTo watch progress in real-time:")
        print(f"  tail -f {out_dir}/pymm_run.log")
        print(f"  tail -f {out_dir}/{get_dynamic_log_filename(out_dir)}")
        
        # Always run in background mode - no longer asking the user
        run_bg = True
        if run_bg:
            try:
                pid = os.fork()
                if pid > 0:
                    # Parent process
                    print(f"Started background process with PID {pid}")
                    # Write PID to file for easier management
                    pid_path = os.path.join(out_dir, PID_FILE)
                    try:
                        with open(pid_path, 'w') as f:
                            f.write(str(pid))
                        print(f"PID saved to {pid_path}")
                    except Exception as e:
                        print(f"Note: Could not save PID to file: {e}")
                    sys.exit(0)
                # Child process continues
                # Redirect stdout and stderr to log file
                sys.stdout.flush()
                sys.stderr.flush()
                log_file = open(os.path.join(out_dir, "pymm_run.log"), "a")
                os.dup2(log_file.fileno(), sys.stdout.fileno())
                os.dup2(log_file.fileno(), sys.stderr.fileno())
            except OSError:
                print("Background execution not supported on this platform")
                # Continue with normal execution

        # Always force-start MetaMap servers before batch processing
        print("Ensuring MetaMap servers are running (required for batch processing)...")
        metamap_binary_p = get_config_value("metamap_binary_path")
        if metamap_binary_p:
            public_mm_dir = os.path.abspath(os.path.join(os.path.dirname(metamap_binary_p), os.pardir))
            if os.path.isdir(public_mm_dir):
                start_metamap_servers(public_mm_dir)
            else:
                print("Warning: Could not determine public_mm directory from binary path")
                ensure_servers_running()
        else:
            ensure_servers_running()

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
    
    # Get basic stats
    total_overall = state.get("total_overall", 0)
    completed_overall_markers = state.get("completed_overall_markers", 0)
    
    # Calculate additional metrics
    retry_count = sum(1 for k in state.keys() if k.startswith("retry_"))
    
    # Count failed files by looking in the failed_files directory
    failed_dir = os.path.join(out_abs, "failed_files")
    failed_count = 0
    if os.path.isdir(failed_dir):
        failed_count = len([f for f in os.listdir(failed_dir) if f.endswith('.csv')])
    
    # Count error files that are in the output dir but have error markers
    error_files = []
    for filename in os.listdir(out_abs):
        if filename.endswith('.csv'):
            file_path = os.path.join(out_abs, filename)
            try:
                with open(file_path, 'r') as f:
                    # Try to read the last line to check for error marker
                    f.seek(0, os.SEEK_END)
                    pos = f.tell()
                    # Read last 200 chars to find markers
                    last_chunk_size = min(200, pos)
                    f.seek(pos - last_chunk_size, os.SEEK_SET)
                    chunk = f.read(last_chunk_size)
                    if ":ERROR" in chunk:
                        error_files.append(filename)
            except:
                pass
    
    error_count = len(error_files)
    
    # Calculate success percentage
    pct = (completed_overall_markers / total_overall * 100.0) if total_overall else 0.0
    
    # Determine batch status
    active_workers_info = state.get("active_workers_info", {})
    end_time = state.get("end_time")
    
    if end_time:
        if completed_overall_markers == total_overall:
            status = "COMPLETED (SUCCESS)"
        else:
            status = "COMPLETED (WITH ERRORS)"
    elif active_workers_info and any(active_workers_info.values()):
        status = "RUNNING"
    else:
        status = "IDLE OR CRASHED"
    
    # Get terminal width
    try:
        import shutil
        term_width, _ = shutil.get_terminal_size((80, 20))
    except:
        term_width = 80
    
    # Create an ASCII progress bar
    bar_width = min(50, term_width - 20)
    filled_width = int(bar_width * pct / 100)
    bar = "█" * filled_width + "░" * (bar_width - filled_width)
    
    # Print summary table
    print("\n" + "=" * term_width)
    print(f"BATCH STATUS: [{status}]")
    print("=" * term_width)
    
    print(f"\nProgress: [{bar}] {completed_overall_markers}/{total_overall} ({pct:.1f}%)")
    
    # Print statistics
    print("\nStatistics:")
    print(f"  Total Files:     {total_overall}")
    print(f"  Completed:       {completed_overall_markers}")
    print(f"  Failed Files:    {failed_count}")
    print(f"  Error Markers:   {error_count}")
    print(f"  Retry Attempts:  {retry_count}")
    
    # Display remaining files count
    remaining = total_overall - completed_overall_markers
    if remaining > 0:
        print(f"  Remaining:       {remaining}")
    
    # Active workers info
    if active_workers_info and any(active_workers_info.values()):
        active_files_display = [f"{wid}:{info.get('current_file', 'N/A')}" for wid, info in active_workers_info.items() if isinstance(info, dict)]
        if active_files_display: 
            print("\nActive Workers:")
            for worker_info in active_files_display:
                print(f"  {worker_info}")
    elif not state.get("end_time"): 
        print("\n  No active workers detected. The batch might be idle or crashed.")
    
    # Time calculation
    if "start_time" in state:
        start_dt = parse_iso_datetime_compat(state["start_time"])
        end_ts_str = state.get("end_time")
        
        if end_ts_str:
            end_dt = parse_iso_datetime_compat(end_ts_str)
            pre_calculated_elapsed = state.get("elapsed_seconds")
            
            if pre_calculated_elapsed is not None: 
                elapsed = int(pre_calculated_elapsed)
            elif start_dt and end_dt and end_dt >= start_dt: 
                elapsed = int((end_dt - start_dt).total_seconds())
            else: 
                elapsed = None
                
            if elapsed is not None: 
                hours = elapsed // 3600
                minutes = (elapsed % 3600) // 60
                seconds = elapsed % 60
                print(f"\nTotal processing time: {hours}h {minutes}m {seconds}s (Batch completed)")
            else: 
                print("\nTotal elapsed time: Could not determine (Batch completed, inconsistent times).")
        elif start_dt:
            now = datetime.now()
            if now >= start_dt: 
                elapsed = int((now - start_dt).total_seconds())
                hours = elapsed // 3600
                minutes = (elapsed % 3600) // 60
                seconds = elapsed % 60
                print(f"\nElapsed time so far: {hours}h {minutes}m {seconds}s")
                
                # Calculate estimated time remaining
                if completed_overall_markers > 0 and remaining > 0:
                    avg_time_per_file = elapsed / completed_overall_markers
                    est_remaining = avg_time_per_file * remaining
                    est_hours = int(est_remaining // 3600)
                    est_minutes = int((est_remaining % 3600) // 60)
                    est_seconds = int(est_remaining % 60)
                    print(f"Estimated time remaining: {est_hours}h {est_minutes}m {est_seconds}s")
            else: 
                print("\nElapsed time so far: Start time is in the future. Check system clock/state file.")
        else: 
            print("\nElapsed time: Could not determine (start time missing/invalid).")
    
    # Show performance metrics
    file_timings = state.get("file_timings", {})
    if file_timings:
        # Filter out non-numeric timing entries
        numeric_timings = [int(d.replace("ms","")) for d in file_timings.values() 
                        if isinstance(d, str) and d.replace("ms","").isdigit()]
        
        if numeric_timings:
            num_timed_files = len(numeric_timings)
            total_time_ms = sum(numeric_timings)
            avg_time_ms = total_time_ms / num_timed_files
            
            print("\nPerformance Metrics:")
            print(f"  Average time per file: {avg_time_ms:.0f} ms ({avg_time_ms/1000.0:.2f} s) over {num_timed_files} timed files")
            
            # Show min/max processing times
            min_time = min(numeric_timings)
            max_time = max(numeric_timings)
            print(f"  Fastest file: {min_time} ms ({min_time/1000.0:.2f} s)")
            print(f"  Slowest file: {max_time} ms ({max_time/1000.0:.2f} s)")
            
            # Estimated time remaining
            if remaining > 0 and not state.get("end_time"):
                etr_seconds = (avg_time_ms / 1000.0) * remaining
                etr_hours = int(etr_seconds // 3600)
                etr_minutes = int((etr_seconds % 3600) // 60)
                etr_seconds = int(etr_seconds % 60)
                print(f"  Estimated time remaining (ETR): {etr_hours}h {etr_minutes}m {etr_seconds}s for {remaining} files")
        else: 
            print("\n  No valid file timing data to calculate average or ETR.")
    else: 
        print("\n  No file timing data recorded yet.")
    
    # Option to view failed files
    if failed_count > 0 or error_count > 0:
        print(f"\n--- Failed/Error Files Management ---")
        print(f"  Failed files in directory: {failed_count}")
        print(f"  Files with error markers: {error_count}")
        
        try:
            choice = input("\nWould you like to view failed files? (y/n): ").strip().lower()
            if choice == 'y':
                view_failed_files(out_abs, failed_dir, error_files)
        except:
            pass

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
    from datetime import datetime
    
    # Import cursor control for better refreshing
    try:
        # Try to import cursor control libraries
        import cursor
        has_cursor_control = True
    except ImportError:
        has_cursor_control = False
    
    try:
        import shutil
        term_width, term_height = shutil.get_terminal_size((80, 20))
    except:
        term_width, term_height = 80, 20
    
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
    
    # For storing historical data to show trends
    cpu_history = {}
    mem_history = {}
    progress_history = []
    
    # Add mode tracking
    view_mode = "summary"  # Options: "summary", "workers", "files", "stats"
    auto_refresh = True
    refresh_interval = 2  # Update every 2 seconds
    
    # Check for msvcrt for Windows key capture
    has_msvcrt = False
    try:
        import msvcrt
        has_msvcrt = True
    except ImportError:
        pass
    
    # Check for Unix select for non-blocking input
    has_select = False
    try:
        import select
        has_select = True
    except ImportError:
        pass

    # ANSI escape codes for cursor control
    CLEAR_SCREEN = "\033[2J"
    CURSOR_HOME = "\033[H"
    ERASE_LINE = "\033[K"
    CURSOR_UP = "\033[F"
    SAVE_CURSOR = "\033[s"
    RESTORE_CURSOR = "\033[u"
    
    # Function to render a progress bar with specified attributes
    def render_progress_bar(value, total, width=50, prefix="", suffix=""):
        filled_width = int(width * value / total) if total else 0
        bar = "█" * filled_width + "░" * (width - filled_width)
        percent = f"{100 * value / total:.1f}%" if total else "0.0%"
        return f"{prefix}[{bar}] {percent} {suffix}"
    
    try:
        # Initial full screen render
        first_render = True
        last_update_time = time.time()
        
        while True:
            if auto_refresh:
                current_time = time.time()
                if current_time - last_update_time < refresh_interval:
                    time.sleep(0.1)  # Small sleep to prevent CPU spinning
                    
                    # Check for key presses while waiting
                    if has_msvcrt and msvcrt.kbhit():
                        try:
                            key = msvcrt.getch().decode('utf-8', errors='ignore').lower()
                            if key == 'q':
                                print("\nExiting dashboard...")
                                break
                            elif key == 'p':
                                auto_refresh = False
                                print("\nRefresh paused. Press 'p' to resume.")
                                continue
                            elif key in ['s', 'w', 'f', 't']:
                                view_mode = {"s": "summary", "w": "workers", "f": "files", "t": "stats"}[key]
                                first_render = True  # Force full redraw when changing modes
                            elif key == '+':
                                refresh_interval = max(0.5, refresh_interval - 0.5)
                            elif key == '-':
                                refresh_interval = min(10, refresh_interval + 0.5)
                        except:
                            pass  # Ignore any key decode errors
                    
                    continue
                
                last_update_time = current_time
                
                # Use clear screen for full redraw or cursor positioning for partial updates
                if first_render:
                    # Full clear and redraw
                    os.system('cls' if os.name == 'nt' else 'clear')
                    first_render = False
                elif has_cursor_control and sys.stdout.isatty():
                    # Move cursor to home position and redraw without clearing
                    print(CURSOR_HOME, end='')
                else:
                    # Fallback to regular clear if no cursor control
                    os.system('cls' if os.name == 'nt' else 'clear')
            
            # Load the current state
            try:
                with open(state_path) as f:
                    state = json.load(f)
            except Exception as e:
                print(f"Error reading state file: {e}")
                state = {}
            
            # Basic stats calculation (needed for any view)
            total = state.get('total_overall', 0)
            done = state.get('completed_overall_markers', 0)
            failed_count = len(state.get('file_timings', {}).get('failed_files', [])) if state.get('file_timings') else 0
            retry_count = state.get('retry_count', 0)
            pct = done/total*100 if total else 0
            
            # Determine batch status
            active_workers = state.get('active_workers_info', {})
            if not active_workers and state.get('end_time'):
                batch_status = "COMPLETED"
            elif active_workers:
                batch_status = "RUNNING"
            else:
                batch_status = "UNKNOWN"
            
            # Store progress for history
            progress_history.append(pct)
            progress_history = progress_history[-30:]  # Keep last 30 readings
            
            # Calculate progress rate and estimated time
            time_remaining_str = "calculating..."
            if len(progress_history) > 5:
                progress_delta = progress_history[-1] - progress_history[-5]
                if progress_delta > 0:  # Only if progress is being made
                    progress_rate = progress_delta / (5 * refresh_interval)
                    time_remaining = (100 - pct) / progress_rate if progress_rate > 0 else 0
                    time_remaining_str = f"{int(time_remaining // 60)}m {int(time_remaining % 60)}s"
            
            # Get process info
            children = []
            main_cpu = 0
            main_mem = 0
            cpu_trend = ""
            mem_trend = ""
            
            if pid and psutil and psutil.pid_exists(pid):
                try:
                    p = psutil.Process(pid)
                    main_cpu = p.cpu_percent(interval=0.1)
                    main_mem = p.memory_info().rss / (1024*1024)
                    
                    # Store historical data
                    if pid not in cpu_history:
                        cpu_history[pid] = []
                    if pid not in mem_history:
                        mem_history[pid] = []
                        
                    cpu_history[pid].append(main_cpu)
                    mem_history[pid].append(main_mem)
                    
                    # Keep only last 10 readings
                    cpu_history[pid] = cpu_history[pid][-10:]
                    mem_history[pid] = mem_history[pid][-10:]
                    
                    # Calculate trend
                    cpu_trend = "↑" if len(cpu_history[pid]) > 1 and cpu_history[pid][-1] > cpu_history[pid][-2] else "↓"
                    mem_trend = "↑" if len(mem_history[pid]) > 1 and mem_history[pid][-1] > mem_history[pid][-2] else "↓"
                    
                    # Get child processes
                    children = p.children(recursive=True)
                except Exception as proc_err:
                    print(f"Error getting process info: {proc_err}")
            
            # ----- DISPLAY HEADER (always shown) -----
            print("=" * term_width)
            print(f"MetaMap Batch Dashboard - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Output dir: {out_dir} | PID: {pid} | View: {view_mode.upper()} | Refresh: {'ON' if auto_refresh else 'OFF'} ({refresh_interval}s)")
            print("=" * term_width)
            
            # Always show basic progress info
            main_progress = render_progress_bar(done, total, width=term_width-20, 
                                               suffix=f"({done}/{total})")
            print(f"\nMain Batch: {main_progress}")
            print(f"Status: [{batch_status}] | Completed: {done} | Failed: {failed_count} | Retries: {retry_count}")
            
            # ----- DISPLAY CONTENT BASED ON VIEW MODE -----
            if view_mode == "summary":
                # CPU/RAM summary
                print(f"\nMain Process: CPU {main_cpu:.1f}% {cpu_trend} | RAM {main_mem:.1f} MB {mem_trend}")
                if batch_status == "RUNNING":
                    print(f"Est. Time Remaining: {time_remaining_str}")
                
                # Show worker count
                worker_count = len(children)
                if worker_count > 0:
                    # Get CPU and memory stats safely
                    try:
                        # Use list comprehensions with safe error handling
                        cpu_values = []
                        mem_values = []
                        for c in children:
                            try:
                                cpu_values.append(c.cpu_percent(interval=0.1))
                                mem_values.append(c.memory_info().rss / (1024*1024))
                            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                                # Skip terminated processes
                                pass
                        
                        if cpu_values and mem_values:
                            avg_cpu = sum(cpu_values) / len(cpu_values)
                            avg_mem = sum(mem_values) / len(mem_values)
                            print(f"Workers: {worker_count} active | Avg CPU: {avg_cpu:.1f}% | Avg RAM: {avg_mem:.1f} MB")
                        else:
                            print(f"Workers: {worker_count} active | Stats unavailable (processes changing)")
                    except Exception as e:
                        print(f"Workers: {worker_count} active | Stats error: {e}")
                
                # Show top 5 active workers from state
                if active_workers:
                    print("\nActive Workers (Top 5):")
                    active_worker_items = list(active_workers.items())
                    for wid, info in active_worker_items[:5]:
                        worker_file = info.get('current_file', 'Unknown')
                        worker_status = info.get('status', 'Unknown')
                        # Truncate filename if too long
                        if len(worker_file) > 40:
                            worker_file = worker_file[:37] + "..."
                        print(f"  {wid:<4} {worker_status:<10} {worker_file}")
                    if len(active_workers) > 5:
                        print(f"  ... and {len(active_workers) - 5} more workers (switch to Workers view with 'w')")
                
                # Show any active retry batches
                retry_batches = [(k, v) for k, v in state.items() if k.startswith("retry_") and isinstance(v, dict) and v.get("type") == "retry_batch"]
                active_retries = [(k, v) for k, v in retry_batches if v.get("status") in ["running", "pending"]]
                completed_retries = [(k, v) for k, v in retry_batches if v.get("status") == "completed" and v.get("end_time") and 
                                   (datetime.now() - parse_iso_datetime_compat(v.get("end_time"))).total_seconds() < 3600]  # Show completed within last hour
                
                if active_retries:
                    print("\nActive Retry Batches:")
                    for retry_id, info in active_retries:
                        current_file = info.get('current_file', 'N/A')
                        if len(current_file) > 30:
                            current_file = current_file[:27] + "..."
                        
                        # Show progress bar for each retry batch
                        total_retry_files = info.get('total_files', 0)
                        progress_parts = info.get('progress', '0/0').split('/')
                        done_retry_files = int(progress_parts[0]) if len(progress_parts) > 0 and progress_parts[0].isdigit() else 0
                        
                        # Compute batch number or ID for display
                        batch_num = info.get('batch_number', '?')
                        short_id = retry_id.split('_')[1][:6] if '_' in retry_id else retry_id[:6]
                        
                        # Render progress bar with batch identifier
                        retry_bar = render_progress_bar(done_retry_files, total_retry_files, width=40, 
                                                       prefix=f"Retry #{batch_num} [{short_id}]: ", 
                                                       suffix=f"({done_retry_files}/{total_retry_files}) {current_file}")
                        print(f"  {retry_bar}")
                
                if completed_retries:
                    print("\nRecently Completed Retry Batches:")
                    for retry_id, info in completed_retries[:3]:  # Show only the most recent 3
                        success = info.get('success_count', 0)
                        total = info.get('total_files', 0)
                        batch_num = info.get('batch_number', '?')
                        short_id = retry_id.split('_')[1][:6] if '_' in retry_id else retry_id[:6]
                        success_pct = (success / total * 100) if total > 0 else 0
                        print(f"  Retry #{batch_num} [{short_id}]: {success}/{total} successful ({success_pct:.1f}%)")
                    if len(completed_retries) > 3:
                        print(f"  ... and {len(completed_retries) - 3} more (switch to Stats view with 't')")
            
            elif view_mode == "workers":
                # Show detailed worker information in a table
                print("\n--- Worker Processes ---")
                if children:
                    print(f"{'PID':<8} {'CPU%':<8} {'RAM(MB)':<10} {'Status':<10}")
                    print("-" * 40)
                    # Sort workers by CPU usage (highest first)
                    sorted_children = []
                    for c in children:
                        try:
                            cpu = c.cpu_percent(interval=0.1)
                            mem = c.memory_info().rss / (1024*1024)
                            status = c.status()
                            sorted_children.append((c, cpu, mem, status))
                        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                            pass  # Skip terminated processes
                    
                    sorted_children.sort(key=lambda x: x[1], reverse=True)  # Sort by CPU usage
                    
                    for c, cpu, mem, status in sorted_children:
                        # Color coding based on CPU usage
                        cpu_str = f"{cpu:.1f}%"
                        if cpu > 80:
                            cpu_str += " !!"
                        elif cpu > 50:
                            cpu_str += " !"
                        
                        print(f"{c.pid:<8} {cpu_str:<8} {mem:.1f}MB     {status:<10}")
                else:
                    print("No worker processes detected.")
                
                # Show all active workers from state file
                if active_workers:
                    print("\n--- Active Workers from State ---")
                    print(f"{'Worker':<10} {'Status':<10} {'File':<50}")
                    print("-" * 70)
                    for wid, info in sorted(active_workers.items()):
                        worker_file = info.get('current_file', 'Unknown')
                        worker_status = info.get('status', 'Unknown')
                        # Truncate filename if too long
                        if len(worker_file) > 47:
                            worker_file = worker_file[:44] + "..."
                        print(f"{wid:<10} {worker_status:<10} {worker_file:<50}")
                else:
                    print("\nNo active workers reported in state file.")
                    
                # Show any active retry batches
                retry_batches = [(k, v) for k, v in state.items() if k.startswith("retry_") and isinstance(v, dict) and v.get("type") == "retry_batch"]
                active_retries = [(k, v) for k, v in retry_batches if v.get("status") in ["running", "pending"]]
                
                if active_retries:
                    print("\n--- Active Retry Workers ---")
                    for retry_id, info in active_retries:
                        batch_num = info.get('batch_number', '?')
                        short_id = retry_id.split('_')[1][:6] if '_' in retry_id else retry_id[:6]
                        print(f"Retry #{batch_num} [{short_id}]:")
                        
                        # Show progress bar
                        total_retry_files = info.get('total_files', 0)
                        progress_parts = info.get('progress', '0/0').split('/')
                        done_retry_files = int(progress_parts[0]) if len(progress_parts) > 0 and progress_parts[0].isdigit() else 0
                        
                        retry_bar = render_progress_bar(done_retry_files, total_retry_files, width=term_width-20, 
                                                     suffix=f"({done_retry_files}/{total_retry_files})")
                        print(f"  {retry_bar}")
                        
                        # Show current file being processed
                        current_file = info.get('current_file', 'N/A')
                        print(f"  Current: {current_file}")
            
            elif view_mode == "files":
                # Show file processing statistics
                print("\n--- File Processing Details ---")
                
                # Count files in different states
                failed_dir = os.path.join(out_dir, "failed_files")
                failed_count_actual = 0
                if os.path.isdir(failed_dir):
                    failed_count_actual = len([f for f in os.listdir(failed_dir) if f.endswith('.csv')])
                
                # Count error files in main directory
                error_files = []
                for filename in os.listdir(out_dir):
                    if filename.endswith('.csv'):
                        file_path = os.path.join(out_dir, filename)
                        try:
                            with open(file_path, 'r') as f:
                                f.seek(max(0, os.path.getsize(file_path) - 200))
                                tail = f.read()
                                if "ERROR" in tail:
                                    error_files.append(filename)
                        except:
                            pass
                
                print(f"Files in failed_dir: {failed_count_actual}")
                print(f"Files with error markers: {len(error_files)}")
                print(f"Completed files: {done}")
                print(f"Remaining files: {total - done}")
                
                # Recent file activity
                print("\n--- Recent File Activity ---")
                file_timings = state.get("file_timings", {})
                if file_timings:
                    # Get most recent entries (by timestamp if available, otherwise just last few)
                    recent_files = list(file_timings.items())[-10:]
                    for fname, timing in recent_files:
                        print(f"{fname:<50} {timing}")
                else:
                    print("No file timing information available.")
                
                # Show some error files if available
                if error_files:
                    print("\n--- Error Files (Sample) ---")
                    for i, fname in enumerate(error_files[:5]):
                        print(f"{i+1}. {fname}")
                    if len(error_files) > 5:
                        print(f"... and {len(error_files) - 5} more error files")
                        
                # Show retry file information if available
                retry_batches = [(k, v) for k, v in state.items() if k.startswith("retry_") and isinstance(v, dict) and v.get("type") == "retry_batch"]
                active_retries = [(k, v) for k, v in retry_batches if v.get("status") in ["running", "pending"]]
                
                if active_retries:
                    print("\n--- Retry Batch Files ---")
                    for retry_id, info in active_retries:
                        batch_num = info.get('batch_number', '?')
                        short_id = retry_id.split('_')[1][:6] if '_' in retry_id else retry_id[:6]
                        print(f"Retry #{batch_num} [{short_id}]:")
                        
                        # Show retry progress and current file
                        total_retry_files = info.get('total_files', 0)
                        progress_parts = info.get('progress', '0/0').split('/')
                        done_retry_files = int(progress_parts[0]) if len(progress_parts) > 0 and progress_parts[0].isdigit() else 0
                        current_file = info.get('current_file', 'N/A')
                        
                        print(f"  Progress: {done_retry_files}/{total_retry_files} files")
                        print(f"  Current: {current_file}")
                        
                        # Show recent retry files if available
                        recent_files = info.get('recent_files', [])
                        if recent_files:
                            print("  Recently processed:")
                            for i, (fname, status) in enumerate(recent_files[:3]):
                                status_display = "✓" if status else "✗"
                                print(f"    {status_display} {fname}")
            
            elif view_mode == "stats":
                # Show detailed performance statistics
                print("\n--- Performance Statistics ---")
                
                # Calculate advanced performance metrics
                perf_metrics = calculate_performance_metrics(state)
                display_performance_metrics(perf_metrics)
                
                # Calculate timing statistics
                file_timings = state.get("file_timings", {})
                if file_timings:
                    numeric_timings = [int(d.replace("ms","")) for d in file_timings.values() 
                                    if isinstance(d, str) and d.replace("ms","").isdigit()]
                    
                    if numeric_timings:
                        num_timed_files = len(numeric_timings)
                        total_time_ms = sum(numeric_timings)
                        avg_time_ms = total_time_ms / num_timed_files
                        min_time = min(numeric_timings)
                        max_time = max(numeric_timings)
                        
                        print(f"Files processed: {num_timed_files}")
                        print(f"Average time: {avg_time_ms:.0f} ms ({avg_time_ms/1000.0:.2f} s)")
                        print(f"Fastest file: {min_time} ms ({min_time/1000.0:.2f} s)")
                        print(f"Slowest file: {max_time} ms ({max_time/1000.0:.2f} s)")
                        print(f"Total processing time: {total_time_ms/1000.0:.2f} s")
                
                # Show retry batch statistics
                retry_batches = [(k, v) for k, v in state.items() if k.startswith("retry_") and isinstance(v, dict)]
                if retry_batches:
                    print("\n--- Retry Batch Statistics ---")
                    print(f"Total retry batches: {len(retry_batches)}")
                    
                    active_retries = [v for _, v in retry_batches if v.get("status") in ["running", "pending"]]
                    if active_retries:
                        print(f"Active retry batches: {len(active_retries)}")
                        
                    completed_retries = [v for _, v in retry_batches if v.get("status") == "completed"]
                    if completed_retries:
                        print(f"Completed retry batches: {len(completed_retries)}")
                        
                        # Calculate retry success rate
                        total_retried = sum(v.get("total_files", 0) for v in completed_retries)
                        total_success = sum(v.get("success_count", 0) for v in completed_retries)
                        if total_retried > 0:
                            success_rate = (total_success / total_retried) * 100
                            print(f"Retry success rate: {success_rate:.1f}% ({total_success}/{total_retried})")
                    
                    # Show details for last 5 retry batches
                    print("\nRecent Retry Batches:")
                    print(f"{'ID':<15} {'Status':<10} {'Files':<10} {'Success':<10} {'Duration':<10}")
                    print("-" * 60)
                    
                    # Sort by start time, newest first
                    sorted_batches = sorted(retry_batches, key=lambda x: x[1].get("start_time", ""), reverse=True)
                    
                    for batch_id, batch_info in sorted_batches[:5]:
                        status = batch_info.get("status", "unknown")
                        total = batch_info.get("total_files", 0)
                        success = batch_info.get("success_count", 0)
                        batch_num = batch_info.get("batch_number", "?")
                        short_id = f"#{batch_num}-{batch_id[:8]}"
                        
                        # Calculate duration if completed
                        duration = "N/A"
                        if status == "completed" and "start_time" in batch_info and "end_time" in batch_info:
                            start = parse_iso_datetime_compat(batch_info["start_time"])
                            end = parse_iso_datetime_compat(batch_info["end_time"])
                            if start and end:
                                duration_secs = (end - start).total_seconds()
                                if duration_secs < 60:
                                    duration = f"{duration_secs:.1f}s"
                                elif duration_secs < 3600:
                                    duration = f"{duration_secs/60:.1f}m"
                                else:
                                    duration = f"{duration_secs/3600:.1f}h"
                        
                        print(f"{short_id:<15} {status:<10} {total:<10} {success:<10} {duration:<10}")
                
                # Calculate histogram of processing times
                print("\n--- Processing Time Distribution ---")
                if len(numeric_timings) > 5:
                    # Create simplified time buckets
                    buckets = {
                        "< 1s": 0,
                        "1-5s": 0,
                        "5-10s": 0, 
                        "10-30s": 0,
                        "30-60s": 0,
                        "1-5m": 0,
                        "> 5m": 0
                    }
                    
                    for t in numeric_timings:
                        t_sec = t / 1000.0
                        if t_sec < 1:
                            buckets["< 1s"] += 1
                        elif t_sec < 5:
                            buckets["1-5s"] += 1
                        elif t_sec < 10:
                            buckets["5-10s"] += 1
                        elif t_sec < 30:
                            buckets["10-30s"] += 1
                        elif t_sec < 60:
                            buckets["30-60s"] += 1
                        elif t_sec < 300:
                            buckets["1-5m"] += 1
                        else:
                            buckets["> 5m"] += 1
                    
                    # Display bar chart
                    max_count = max(buckets.values())
                    bar_width = term_width - 30
                    for label, count in buckets.items():
                        if max_count > 0:
                            bar_len = int((count / max_count) * bar_width)
                            bar = "█" * bar_len
                            print(f"{label:<6}: {count:>4} {bar}")
                else:
                    print("Not enough timing data for distribution.")
                
                # System resource statistics
                if psutil:
                    print("\n--- System Resources ---")
                    cpu_percent = psutil.cpu_percent(interval=0.5)
                    mem = psutil.virtual_memory()
                    print(f"CPU Usage (System): {cpu_percent}%")
                    print(f"Memory (System): {mem.used/1024/1024/1024:.1f} GB / {mem.total/1024/1024/1024:.1f} GB ({mem.percent}%)")
                    
                    # I/O statistics if available
                    try:
                        io_counters = psutil.disk_io_counters()
                        print(f"Disk Read: {io_counters.read_bytes/1024/1024:.1f} MB")
                        print(f"Disk Write: {io_counters.write_bytes/1024/1024:.1f} MB")
                    except:
                        pass
            
            # ----- COMMANDS HELP (always shown) -----
            print("\n--- Commands ---")
            print("s: Summary view | w: Workers view | f: Files view | t: Stats view")
            print("p: Pause/resume refresh | +/-: Change refresh rate | q: Quit")
            
            # Handle input if not auto-refreshing
            if not auto_refresh:
                command = input("\nEnter command: ").strip().lower()
                if command == 'q':
                    break
                elif command == 's':
                    view_mode = "summary"
                    first_render = True  # Force full redraw when changing modes
                elif command == 'w':
                    view_mode = "workers"
                    first_render = True
                elif command == 'f':
                    view_mode = "files"
                    first_render = True
                elif command == 't':
                    view_mode = "stats"
                    first_render = True
                elif command == 'p':
                    auto_refresh = True
                elif command == '+':
                    refresh_interval = max(0.5, refresh_interval - 0.5)
                elif command == '-':
                    refresh_interval = min(10, refresh_interval + 0.5)
            elif not has_msvcrt and not has_select:
                # If we can't do non-blocking input, sleep for the refresh interval
                time.sleep(refresh_interval)
            
    except KeyboardInterrupt:
        print("\nExiting dashboard...")

def handle_kill_all_processes():
    """Handle killing all MetaMap and worker processes."""
    print("\n--- Kill All MetaMap Processes ---")
    if input("Are you sure you want to kill all MetaMap and worker processes? This will stop any running batches. (yes/no): ").strip().lower() == 'yes':
        terminated = kill_all_metamap_processes()
        if terminated > 0:
            print("Successfully terminated MetaMap processes.")
        else:
            print("No active processes were found or termination failed.")
    else:
        print("Kill operation cancelled.")

def handle_clear_output_directory():
    """Handle clearing an output directory for a fresh start."""
    print("\n--- Clear Output Directory ---")
    default_output = get_config_value("default_output_dir", "./output_csvs")
    out_dir = input(f"Enter output directory to clear (default: {default_output}): ").strip() or default_output
    
    if not os.path.exists(out_dir):
        print(f"Output directory does not exist: {out_dir}")
        if input("Create the directory? (yes/no): ").strip().lower() == 'yes':
            try:
                os.makedirs(out_dir, exist_ok=True)
                print(f"Created directory: {out_dir}")
            except Exception as e:
                print(f"Error creating directory: {e}")
        return
        
    if input(f"Are you sure you want to clear all files in {out_dir}? This cannot be undone. (yes/no): ").strip().lower() == 'yes':
        # First check and kill any running processes
        pid_path = os.path.join(out_dir, PID_FILE)
        if os.path.exists(pid_path):
            try:
                with open(pid_path, 'r') as f:
                    pid = int(f.read().strip())
                if input(f"Found PID file with process {pid}. Kill this process first? (yes/no): ").strip().lower() == 'yes':
                    try:
                        os.kill(pid, 15)  # SIGTERM
                        print(f"Sent termination signal to process {pid}")
                    except ProcessLookupError:
                        print(f"Process {pid} not found (may have already ended)")
                    except Exception as e:
                        print(f"Error killing process {pid}: {e}")
            except Exception as e:
                print(f"Error reading PID file: {e}")
        
        # Clear all files
        removed = clear_output_directory(out_dir)
        if removed > 0:
            print(f"Successfully removed {removed} files/directories from {out_dir}")
        else:
            print(f"No files were removed from {out_dir}")
    else:
        print("Clear operation cancelled.")

def handle_list_processed_files():
    """List pending and completed files in the batch processing."""
    out_dir = get_config_value("default_output_dir", "./output_csvs")
    try:
        out_dir = input(f"Enter output directory to check (default: {out_dir}): ").strip() or out_dir
    except:
        pass
        
    if not os.path.isdir(out_dir):
        print("No valid output directory configured. Please set one first.")
        return
    
    # Create failed files directory path
    failed_dir = os.path.join(out_dir, "failed_files")
    
    # Find all CSV files in output directory
    all_csv_files = []
    for root, _, files in os.walk(out_dir):
        if root == failed_dir:
            continue  # Skip failed files directory
        for filename in files:
            if filename.endswith('.csv'):
                all_csv_files.append(os.path.join(root, filename))
    
    # Find files in failed directory
    failed_files = []
    if os.path.exists(failed_dir):
        for filename in os.listdir(failed_dir):
            file_path = os.path.join(failed_dir, filename)
            if os.path.isfile(file_path):
                failed_files.append(file_path)
    
    # Check for error markers in output files
    error_files = []
    completed_files = []
    incomplete_files = []
    
    for csv_path in all_csv_files:
        try:
            with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
                # Check if file is complete
                first_line = None
                last_line = None
                first_line = f.readline().strip()
                
                # Get last line using seek
                f.seek(0, os.SEEK_END)
                pos = f.tell()
                
                # Read last 200 chars to extract end marker
                if pos > 200:
                    f.seek(pos - 200)
                    f.readline()  # Skip partial first line
                else:
                    f.seek(0)
                
                last_lines = f.readlines()
                if last_lines:
                    last_line = last_lines[-1].strip()
                
                # Categorize the file
                if first_line and first_line.startswith("META_BATCH_START_NOTE_ID:"):
                    if last_line and last_line.startswith("META_BATCH_END_NOTE_ID:"):
                        if ":ERROR" in last_line:
                            error_files.append(csv_path)
                        else:
                            completed_files.append(csv_path)
                    else:
                        incomplete_files.append(csv_path)
                else:
                    # Unknown format
                    incomplete_files.append(csv_path)
                    
        except:
            # If we can't read it for any reason, consider it incomplete
            incomplete_files.append(csv_path)
    
    # Count files with concepts
    files_with_concepts = 0
    total_concepts = 0
    
    for csv_path in completed_files:
        try:
            with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
                line_count = content.count('\n')
                
                # A complete file with only START marker, header, and END marker has 3 lines
                if line_count > 3:
                    files_with_concepts += 1
                    # Rough estimate: each concept is one line
                    concept_count = line_count - 3
                    total_concepts += concept_count
        except:
            pass
    
    # Get terminal width for display
    try:
        import shutil
        term_width, _ = shutil.get_terminal_size((80, 20))
    except:
        term_width = 80
    
    # Print summary
    print("\n" + "=" * term_width)
    print(f"FILE PROCESSING SUMMARY - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * term_width)
    
    print(f"\nTotal files:          {len(all_csv_files)}")
    print(f"Completed files:      {len(completed_files)} ({len(completed_files)/len(all_csv_files)*100:.1f}% if {len(all_csv_files)}>0 else 0)%)")
    print(f"Files with concepts:  {files_with_concepts} ({files_with_concepts/len(completed_files)*100:.1f}% if {len(completed_files)}>0 else 0)%)")
    print(f"Total concepts:       {total_concepts:,}")
    print(f"Failed files:         {len(failed_files)}")
    print(f"Files with errors:    {len(error_files)}")
    print(f"Incomplete files:     {len(incomplete_files)}")
    
    while True:
        print("\n--- File Management ---")
        print("1. View completed files")
        print("2. View files with error markers")
        print("3. View incomplete files")
        print("4. View failed files (from failed_files directory)")
        print("5. Search for file by name")
        print("6. Return to main menu")
        
        try:
            choice = input("\nEnter your choice (1-6): ").strip()
        except:
            return
        
        if choice == '1' and completed_files:
            view_file_list("Completed Files", completed_files)
        elif choice == '2' and error_files:
            view_file_list("Files with Error Markers", error_files)
        elif choice == '3' and incomplete_files:
            view_file_list("Incomplete Files", incomplete_files)
        elif choice == '4' and failed_files:
            view_file_list("Failed Files", failed_files)
        elif choice == '5':
            search_term = input("\nEnter part of filename to search: ").strip().lower()
            if search_term:
                # Search across all file categories
                search_results = []
                for file_path in all_csv_files + failed_files:
                    if search_term in os.path.basename(file_path).lower():
                        search_results.append(file_path)
                
                if search_results:
                    view_file_list(f"Search Results for '{search_term}'", search_results)
                else:
                    print(f"No files matching '{search_term}' found.")
        elif choice == '6':
            return
        else:
            print("Invalid choice or no files in that category.")

def view_file_list(category_name, file_list):
    """Display a list of files with pagination and allow user to select one to view."""
    if not file_list:
        print("No files to display.")
        return
    
    # Sort files by name for easier browsing
    sorted_files = sorted(file_list, key=lambda x: os.path.basename(x))
    
    page_size = 20
    total_pages = (len(sorted_files) + page_size - 1) // page_size
    current_page = 1
    
    while True:
        start_idx = (current_page - 1) * page_size
        end_idx = min(start_idx + page_size, len(sorted_files))
        
        print(f"\n=== {category_name} (Page {current_page}/{total_pages}) ===")
        print(f"{'#':<4} {'Filename':<50} {'Size':<10} {'Modified':<20}")
        print("-" * 85)
        
        for i, file_path in enumerate(sorted_files[start_idx:end_idx], start_idx + 1):
            # Get file info
            try:
                file_size = os.path.getsize(file_path)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime('%Y-%m-%d %H:%M')
                
                # Format filename (truncate if too long)
                filename = os.path.basename(file_path)
                if len(filename) > 46:
                    display_name = filename[:43] + "..."
                else:
                    display_name = filename
                
                # Format size
                if file_size < 1024:
                    size_str = f"{file_size} B"
                elif file_size < 1024 * 1024:
                    size_str = f"{file_size/1024:.1f} KB"
                else:
                    size_str = f"{file_size/(1024*1024):.1f} MB"
                
                print(f"{i:<4} {display_name:<50} {size_str:<10} {mod_time:<20}")
            except:
                print(f"{i:<4} {os.path.basename(file_path):<50} {'N/A':<10} {'N/A':<20}")
        
        # Pagination controls
        print("\nOptions:")
        if total_pages > 1:
            print(f"  n: Next page | p: Previous page | #: View file # | q: Back to menu")
        else:
            print(f"  #: View file # | q: Back to menu")
        
        command = input("\nEnter command: ").strip().lower()
        
        if command == 'q':
            return
        elif command == 'n' and current_page < total_pages:
            current_page += 1
        elif command == 'p' and current_page > 1:
            current_page -= 1
        elif command.isdigit():
            file_num = int(command)
            if 1 <= file_num <= len(sorted_files):
                view_file_details(sorted_files[file_num-1])
            else:
                print(f"Invalid file number. Enter 1-{len(sorted_files)}.")
        else:
            print("Invalid command.")

def display_file_snippet(file_path):
    """Display a snippet of a file."""
    try:
        print(f"\n=== File: {os.path.basename(file_path)} ===")
        
        if file_path.endswith('.csv'):
            # For CSV files, use csv module to properly handle quoting
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                lines = list(csv.reader(f))
                
                # Get first and last lines separately (metadata markers)
                first_line = ""
                last_line = ""
                
                with open(file_path, 'r', encoding='utf-8', errors='replace') as raw_f:
                    first_line = raw_f.readline().strip()
                    # Get last line by seeking near the end
                    try:
                        raw_f.seek(max(0, os.path.getsize(file_path) - 1024))
                        last_lines = raw_f.readlines()
                        if last_lines:
                            last_line = last_lines[-1].strip()
                    except Exception as e:
                        # If seeking fails, read the whole file
                        raw_f.seek(0)
                        last_line = raw_f.readlines()[-1].strip()
                
                print("\nStart marker:")
                print(first_line)
                
                print("\nEnd marker:")
                print(last_line)
                
                # Display header and first 10 rows
                if lines:
                    print("\nHeader:")
                    if len(lines) > 0:
                        print(', '.join(lines[0]))
                    
                    if len(lines) > 1:
                        print("\nData (first 10 rows):")
                        for i, row in enumerate(lines[1:11], 1):
                            print(f"{i}. {', '.join(row)}")
                        
                        if len(lines) > 11:
                            print(f"\n... and {len(lines) - 11} more rows")
                    else:
                        print("\nNo data rows found.")
                else:
                    print("Empty file.")
        else:
            # For text files, just show first 20 lines
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()
                
                print("\nContent (first 20 lines):")
                for i, line in enumerate(lines[:20], 1):
                    print(f"{i}: {line.rstrip()}")
                
                if len(lines) > 20:
                    print(f"\n... and {len(lines) - 20} more lines")
    except Exception as e:
        print(f"Error reading file: {e}")
    
    # Wait for user to press Enter before returning
    try:
        input("\nPress Enter to continue...")
    except:
        pass

def handle_retry_failed_files():
    """Batch retry all failed or error-marked files with increased timeout."""
    print("\n--- Retry Failed Files ---")
    
    # Get directories
    default_input_dir = get_config_value("default_input_dir", "./input_notes")
    default_output_dir = get_config_value("default_output_dir", "./output_csvs")
    
    try:
        output_dir = input(f"Enter output directory with failed files (default: {default_output_dir}): ").strip() or default_output_dir
    except EOFError:
        output_dir = default_output_dir
    
    out_abs = os.path.abspath(os.path.expanduser(output_dir))
    
    if not os.path.isdir(out_abs):
        print(f"Error: Output directory not found: {out_abs}")
        return
    
    # Find failed files
    failed_dir = os.path.join(out_abs, "failed_files")
    failed_files = []
    
    if os.path.isdir(failed_dir):
        for filename in os.listdir(failed_dir):
            if filename.endswith('.csv'):
                failed_file_path = os.path.join(failed_dir, filename)
                try:
                    with open(failed_file_path, 'r', encoding='utf-8', errors='replace') as f:
                        first_line = f.readline().strip()
                        if first_line.startswith("META_BATCH_START_NOTE_ID:"):
                            original_filename = first_line.replace("META_BATCH_START_NOTE_ID:", "")
                            failed_files.append((filename, original_filename, "failed_dir"))
                except:
                    # If we can't read it, still include it with an unknown original name
                    failed_files.append((filename, filename.replace(".csv", ".txt"), "failed_dir_unreadable"))
    
    # Find files with error markers in the main output directory
    error_files = []
    for filename in os.listdir(out_abs):
        if filename.endswith('.csv'):
            file_path = os.path.join(out_abs, filename)
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    first_line = f.readline().strip()
                    original_filename = first_line.replace("META_BATCH_START_NOTE_ID:", "") if first_line.startswith("META_BATCH_START_NOTE_ID:") else None
                    
                    # Check for error marker in the last part of the file
                    f.seek(0, os.SEEK_END)
                    pos = f.tell()
                    last_chars = min(200, pos)
                    f.seek(pos - last_chars)
                    last_content = f.read(last_chars)
                    
                    if ":ERROR" in last_content and original_filename:
                        error_files.append((filename, original_filename, "error_marker"))
            except:
                continue
    
    # Combine and deduplicate files
    all_failed_files = failed_files + error_files
    
    # Deduplicate based on original filename
    unique_files = {}
    for csv_name, orig_name, file_type in all_failed_files:
        # Prioritize files from failed_dir if duplicates exist
        if orig_name not in unique_files or file_type == "failed_dir":
            unique_files[orig_name] = (csv_name, file_type)
    
    if not unique_files:
        print("No failed files found to retry.")
        return
    
    print(f"\nFound {len(unique_files)} failed files to retry.")
    
    # Ask for increased timeout
    default_timeout = int(get_config_value("pymm_timeout", str(DEFAULT_PYMM_TIMEOUT)))
    try:
        new_timeout = input(f"Enter timeout in seconds for retries (default: {default_timeout * 2}, original: {default_timeout}): ")
        if new_timeout.strip() and new_timeout.isdigit():
            timeout = int(new_timeout)
        else:
            timeout = default_timeout * 2  # Double the default timeout for retries
    except:
        timeout = default_timeout * 2
    
    print(f"Using timeout: {timeout} seconds per file")
    
    # Ask for batch confirmation
    confirmation = input(f"Retry processing {len(unique_files)} files? (yes/no): ").strip().lower()
    if confirmation != "yes":
        print("Retry cancelled.")
        return
    
    # Auto-use nohup mode on Unix systems
    use_nohup = True
    if sys.platform == "win32":
        use_nohup = False  # Not supported on Windows
    
    # Execute retry in nohup mode (like regular batch processing)
    if use_nohup:
        # Create nohup command for retries
        script_name = os.path.basename(sys.argv[0])
        if script_name == "pymm-cli" or script_name == "pymm":
            # Build command with necessary parameters for retry
            retry_id = f"retry_{int(time.time())}"
            retry_log_path = os.path.join(out_abs, f"{retry_id}.log")
            
            # Store retry information in state file
            state = load_state(out_abs) or {}
            state[retry_id] = {
                "start_time": datetime.now().isoformat(),
                "total_files": len(unique_files),
                "status": "pending",
                "type": "retry_batch"
            }
            save_state(out_abs, state)
            
            # Store file list for retry
            retry_files_path = os.path.join(out_abs, f"{retry_id}.files")
            with open(retry_files_path, 'w') as f:
                for orig_filename in unique_files.keys():
                    f.write(f"{orig_filename}\n")
            
            # Create environment variables file for the retry process
            env_file_path = os.path.join(out_abs, f"{retry_id}.env")
            with open(env_file_path, 'w') as f:
                f.write(f"PYMM_TIMEOUT={timeout}\n")
                f.write(f"RETRY_BATCH_ID={retry_id}\n")
                f.write(f"RETRY_FILES_PATH={retry_files_path}\n")
            
            # Build nohup command - simplified version that just calls a retry-specific command
            nohup_cmd = f"nohup {script_name} retry-batch {out_abs} {default_input_dir} {retry_id} > {retry_log_path} 2>&1 &"
            
            print(f"\nRunning retry in nohup background mode with command:")
            print(f"  {nohup_cmd}")
            
            # Execute the nohup command
            try:
                os.system(nohup_cmd)
                print(f"Retry process started in background. Check progress with Monitor Dashboard.")
                print(f"Log file: {retry_log_path}")
                return
            except Exception as e:
                print(f"Error starting nohup process: {e}")
                print("Continuing in foreground mode.")
    
    # If nohup not available or failed, use fork or regular mode
    background_options = "1"  # Default to regular background mode if not nohup
    
    # Handle background processing
    if background_options == "1" or background_options.lower() == "yes":
        try:
            # Fork for Unix-like systems
            if hasattr(os, 'fork'):
                pid = os.fork()
                if pid > 0:
                    # Parent process
                    print(f"Started background retry process with PID {pid}")
                    retry_pid_path = os.path.join(out_abs, ".retry_pid")
                    with open(retry_pid_path, 'w') as f:
                        f.write(str(pid))
                    print(f"To check status: Look for files in {out_abs}")
                    print(f"PID stored in: {retry_pid_path}")
                    return
                # Child process continues below
            else:
                # Windows - try to detach as a background process
                print("Background mode not fully supported on this platform.")
                print("Process will continue running but may be tied to this terminal.")
                print("For better background support, use 'start pythonw' on Windows.")
        except Exception as e:
            print(f"Error starting background process: {e}")
            print("Continuing in foreground mode.")

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
        print("7. Kill All MetaMap Processes")
        print("8. Clear Output Directory")
        print("9. Retry Failed Files")
        print("10. Reset All Saved Configurations to Defaults")
        print("11. Exit")
        try:
            choice = input("Enter your choice (0-11): ").strip()
        except EOFError:
            print("\nExiting due to EOF.")
            break
        if choice == '0':
            handle_install_metamap()
        elif choice == '1':
            handle_configure_settings_menu()
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
            handle_list_processed_files()
        elif choice == '7':
            handle_kill_all_processes()
        elif choice == '8':
            handle_clear_output_directory()
        elif choice == '9':
            handle_retry_failed_files()
        elif choice == '10':
            handle_reset_settings()
        elif choice == '11':
            print("Exiting.")
            break
        else:
            print("Invalid choice. Please try again.")

def main():
    # Initial setup of basic console logging for any early messages or config prompts
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stdout)
    
    # Check and install dependencies
    check_and_install_dependencies()
    
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
            # Ensure basicConfig has run at least once for console output if this is the first logging attempt
            if not logging.getLogger().hasHandlers():
                logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", stream=sys.stdout)
            logging.error("Error: METAMAP_BINARY_PATH is not configured. Run 'pymm-cli' (interactive) or 'pymm-cli install' first.")
            sys.exit(1)
    else:
        configured_metamap_binary_path = None # Will be set by install logic
    
    # Other global settings also fetched using get_config_value
    # This allows them to be influenced by config file or env vars before subcommand logic runs.
    global MAX_PARALLEL_WORKERS
    MAX_PARALLEL_WORKERS = int(get_config_value("max_parallel_workers", os.getenv("MAX_PARALLEL_WORKERS", str(MAX_PARALLEL_WORKERS_DEFAULT))))
    default_metamap_options = get_config_value("metamap_processing_options", METAMAP_PROCESSING_OPTIONS_DEFAULT)

    # Determine output_dir_for_logging early for consistent log setup
    raw_output_dir_for_logging = None # Store the path as originally determined
    if subcmd in {"start", "resume"} and len(sys.argv) == 4: raw_output_dir_for_logging = os.path.abspath(os.path.expanduser(sys.argv[3]))
    elif subcmd in {"progress", "pid", "kill", "tail", "completed", "sample", "clearout", "badcsv"} and len(sys.argv) >= 3: raw_output_dir_for_logging = os.path.abspath(os.path.expanduser(sys.argv[2]))
    elif subcmd == "pending" and len(sys.argv) == 4: raw_output_dir_for_logging = os.path.abspath(os.path.expanduser(sys.argv[3]))
    elif subcmd in {"install", "validate"} and not raw_output_dir_for_logging:
        default_output_dir_val = get_config_value("default_output_dir")
        if default_output_dir_val:
            raw_output_dir_for_logging = os.path.abspath(os.path.expanduser(default_output_dir_val))

    final_output_dir_for_logging = _normalize_path_for_os(raw_output_dir_for_logging) if raw_output_dir_for_logging else None

    if final_output_dir_for_logging:
        # Use the new ensure_logging_setup function for more reliable logging
        log_path = ensure_logging_setup(final_output_dir_for_logging)
        if not log_path:
            # Fall back to old logging method if the new approach fails
            Path(final_output_dir_for_logging).mkdir(parents=True, exist_ok=True) # Ensure directory exists
            
            # Use the raw (potentially /mnt/c) path for get_dynamic_log_filename logic if it relies on specific path structure
            log_file_name = get_dynamic_log_filename(raw_output_dir_for_logging if raw_output_dir_for_logging else final_output_dir_for_logging)
            raw_log_path = os.path.join(raw_output_dir_for_logging if raw_output_dir_for_logging else final_output_dir_for_logging, log_file_name) # Build raw path
            final_log_path = _normalize_path_for_os(raw_log_path) # Normalize final log_path for FileHandler
            
            # Ensure the directory for the (potentially normalized) log_path exists
            Path(os.path.dirname(final_log_path)).mkdir(parents=True, exist_ok=True)
            
            root_logger = logging.getLogger()
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
            
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
            root_logger.addHandler(console_handler)
            root_logger.setLevel(logging.INFO)

            existing = any(
                isinstance(h, logging.FileHandler) and
                os.path.normcase(os.path.normpath(getattr(h, "baseFilename", ""))) == os.path.normcase(os.path.normpath(final_log_path))
                for h in root_logger.handlers
            )
            if not existing:
                _safe_add_file_handler(final_log_path)
    else:
        # No output directory specified, use basic console logging only
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
        root_logger.addHandler(console_handler)
        root_logger.setLevel(logging.INFO)

    if subcmd in {"start", "resume"}:
        if len(sys.argv) != 4: logging.error(f"Error: {subcmd} command requires INPUT_DIR and OUTPUT_DIR arguments."); print(f"Usage: pymm-cli {subcmd} INPUT_DIR OUTPUT_DIR"); sys.exit(1)
        inp_dir_str, out_dir_str = sys.argv[2:4]
                
        # Always force-start MetaMap servers before batch processing
        print("Ensuring MetaMap servers are running (required for batch processing)...")
        metamap_binary_p = get_config_value("metamap_binary_path")
        if metamap_binary_p:
            public_mm_dir = os.path.abspath(os.path.join(os.path.dirname(metamap_binary_p), os.pardir))
            if os.path.isdir(public_mm_dir):
                start_metamap_servers(public_mm_dir)
            else:
                print("Warning: Could not determine public_mm directory from binary path")
                ensure_servers_running()
        else:
            ensure_servers_running()
                    
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

    elif subcmd == "killall":
        print("Killing all MetaMap processes...")
        terminated = kill_all_metamap_processes()
        print(f"Terminated {terminated} process groups.")
        sys.exit(0)
    
    elif subcmd == "clearout" and len(sys.argv) >= 3:
        out_dir_to_clear = os.path.abspath(os.path.expanduser(sys.argv[2]))
        force_mode = len(sys.argv) >= 4 and sys.argv[3].lower() == "force"
        
        if not os.path.exists(out_dir_to_clear):
            print(f"Output directory does not exist: {out_dir_to_clear}")
            sys.exit(1)
        
        if not force_mode:
            print(f"WARNING: This will delete all output files in {out_dir_to_clear}")
            print("To skip this warning, use: pymm-cli clearout <directory> force")
            confirm = input("Are you sure you want to continue? (yes/no): ").strip().lower()
            if confirm != "yes":
                print("Operation cancelled.")
                sys.exit(0)
        
        # Check for running process and kill if needed
        pid_path = os.path.join(out_dir_to_clear, PID_FILE)
        if os.path.exists(pid_path):
            try:
                with open(pid_path, 'r') as f:
                    pid = int(f.read().strip())
                
                if force_mode or input(f"Found PID file with process {pid}. Kill this process? (yes/no): ").strip().lower() == 'yes':
                    try:
                        os.kill(pid, 15)  # SIGTERM
                        print(f"Sent termination signal to process {pid}")
                        time.sleep(2)  # Give process time to terminate
                    except ProcessLookupError:
                        print(f"Process {pid} not found (may have already ended)")
                    except Exception as e:
                        print(f"Error killing process {pid}: {e}")
            except Exception as e:
                print(f"Error reading PID file: {e}")
        
        # Clear all files
        removed = clear_output_directory(out_dir_to_clear)
        print(f"Successfully removed {removed} files/directories from {out_dir_to_clear}")
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

def view_failed_files(output_dir, failed_dir, error_files):
    """Display information about failed files and allow viewing snippets."""
    all_failed_files = []
    
    # Collect files from failed directory
    if os.path.isdir(failed_dir):
        for filename in os.listdir(failed_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(failed_dir, filename)
                all_failed_files.append(("failed_dir", file_path, filename))
    
    # Collect files with error markers
    for filename in error_files:
        file_path = os.path.join(output_dir, filename)
        all_failed_files.append(("error_marker", file_path, filename))
    
    if not all_failed_files:
        print("No failed files found.")
        return
        
    # Display files
    print("\nFailed Files:")
    print(f"{'#':<4} {'Type':<15} {'Filename':<50}")
    print("-" * 70)
    
    for idx, (fail_type, file_path, filename) in enumerate(all_failed_files, 1):
        type_display = "Failed Directory" if fail_type == "failed_dir" else "Error Marker"
        # Truncate filename if too long
        if len(filename) > 46:
            display_name = filename[:43] + "..."
        else:
            display_name = filename
        print(f"{idx:<4} {type_display:<15} {display_name:<50}")
    
    # Ask which file to view
    try:
        choice = input("\nEnter number to view file details (or 0 to return): ")
        if choice.isdigit() and 1 <= int(choice) <= len(all_failed_files):
            selected_idx = int(choice) - 1
            _, selected_file, _ = all_failed_files[selected_idx]
            view_file_details(selected_file)
    except:
        pass

def view_file_details(file_path):
    """Show detailed analysis of a file including start/end markers and content."""
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return
        
    print(f"\n=== File Analysis: {os.path.basename(file_path)} ===")
    
    try:
        # File stats
        file_size = os.path.getsize(file_path)
        mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        
        print(f"Path: {file_path}")
        print(f"Size: {file_size:,} bytes")
        print(f"Last Modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Analyze the content
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            # Get first line (should be start marker)
            first_line = f.readline().strip()
            
            # Check if it's a start marker
            if first_line.startswith("META_BATCH_START_NOTE_ID:"):
                print(f"\nStart Marker: {first_line}")
                original_filename = first_line.replace("META_BATCH_START_NOTE_ID:", "")
                print(f"Original Filename: {original_filename}")
            else:
                print(f"\nFirst Line (not a start marker): {first_line}")
            
            # Check for CSV header
            second_line = f.readline().strip()
            if second_line.startswith('"CUI"') or 'CUI' in second_line:
                print("CSV Header: Present")
            else:
                print(f"Second Line (not a CSV header): {second_line}")
            
            # Count the number of data rows
            content = f.read()
            data_rows = content.count('\n') + (0 if content.endswith('\n') else 1)
            
            # Check for end marker
            has_end_marker = False
            has_error_marker = False
            
            # Check the last 200 bytes for end marker
            f.seek(0, os.SEEK_END)
            pos = f.tell()
            last_chars = min(200, pos)
            f.seek(pos - last_chars)
            last_content = f.read(last_chars)
            
            for line in reversed(last_content.splitlines()):
                if line.startswith("META_BATCH_END_NOTE_ID:"):
                    has_end_marker = True
                    if ":ERROR" in line:
                        has_error_marker = True
                    print(f"End Marker: {line}")
                    break
            
            if not has_end_marker:
                print("End Marker: Not found (file may be incomplete)")
            
            print(f"\nData rows: {data_rows}")
            
            # Analyze potential errors
            if has_error_marker:
                print("\n=== ERROR ANALYSIS ===")
                print("File contains an error marker. This indicates:")
                print("- MetaMap completed but encountered an error")
                print("- This could be a timeout, parsing error, or other issue")
                print("- The file may or may not contain valid concepts")
                
                # See if there's a hint about the error in the log
                log_filename = get_dynamic_log_filename(os.path.dirname(file_path))
                log_path = os.path.join(os.path.dirname(file_path), log_filename)
                
                if os.path.exists(log_path):
                    original_name = os.path.basename(file_path).replace(".csv", "")
                    print(f"\nSearching log for errors related to {original_name}...")
                    
                    try:
                        with open(log_path, 'r', encoding='utf-8', errors='replace') as log_f:
                            log_content = log_f.read()
                            # Find error messages related to this file
                            error_lines = []
                            for line in log_content.splitlines():
                                if "ERROR" in line and original_name in line:
                                    error_lines.append(line)
                            
                            if error_lines:
                                print("Found relevant error messages:")
                                for err_line in error_lines[-3:]:  # Show last 3 errors
                                    print(f"  {err_line}")
                            else:
                                print("No specific error messages found for this file.")
                    except Exception as e:
                        print(f"Error reading log file: {e}")
            
            # Display content snippets
            print("\n=== CONTENT SNIPPETS ===")
            
            # Reset file pointer to beginning
            f.seek(0)
            
            # Show first 10 lines
            print("\nFirst 10 lines:")
            for i, line in enumerate(f, 1):
                print(f"{i}: {line.strip()}")
                if i >= 10:
                    break
            
            # Show content from the middle of the file (if large enough)
            if data_rows > 20:
                middle_pos = max(file_size // 2, 10)
                f.seek(middle_pos)
                # Skip to next line boundary
                f.readline()
                
                print("\nSnippet from middle of file:")
                for i in range(5):
                    line = f.readline()
                    if not line:
                        break
                    print(f"M{i+1}: {line.strip()}")
            
            # Show last 5 lines (excluding the end marker)
            print("\nLast 5 data lines (excluding end marker):")
            f.seek(0, os.SEEK_END)
            pos = f.tell()
            
            # Read last 1000 chars to extract last lines
            last_chars = min(1000, pos)
            f.seek(pos - last_chars)
            last_chunk = f.read(last_chars)
            
            last_lines = last_chunk.splitlines()
            # Filter out the end marker
            data_lines = [line for line in last_lines if not line.startswith("META_BATCH_END_NOTE_ID:")]
            
            for i, line in enumerate(data_lines[-5:]):
                print(f"L{i+1}: {line}")
        
        # Option to view corresponding input file
        if first_line.startswith("META_BATCH_START_NOTE_ID:"):
            input_filename = first_line.replace("META_BATCH_START_NOTE_ID:", "")
            default_input_dir = get_config_value("default_input_dir", "./input_notes")
            input_path = os.path.join(default_input_dir, input_filename)
            
            if os.path.exists(input_path):
                print(f"\nCorresponding input file found: {input_path}")
                if input("\nView input file? (y/n): ").strip().lower() == 'y':
                    view_input_file(input_path)
            else:
                print(f"\nCorresponding input file not found: {input_path}")
        
        # Offer to retry processing the file
        print("\n=== OPTIONS ===")
        choice = input("Would you like to retry processing this file? (y/n): ").strip().lower()
        if choice == 'y':
            original_filename = first_line.replace("META_BATCH_START_NOTE_ID:", "")
            retry_file_processing(original_filename)
            
    except Exception as e:
        print(f"Error analyzing file: {e}")
        import traceback
        traceback.print_exc()

def view_input_file(input_path):
    """Display the content of an input text file."""
    if not os.path.exists(input_path):
        print(f"File not found: {input_path}")
        return
        
    print(f"\n=== Input File: {os.path.basename(input_path)} ===")
    print(f"Size: {os.path.getsize(input_path):,} bytes")
    
    try:
        with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
            
            # Get statistics
            line_count = content.count('\n') + (0 if content.endswith('\n') else 1)
            word_count = len(content.split())
            
            print(f"Lines: {line_count} | Words: {word_count}")
            
            # Display beginning of file
            print("\n--- Beginning of file ---")
            lines = content.splitlines()
            for i, line in enumerate(lines[:15]):
                print(f"{i+1}: {line[:100]}")
                if len(line) > 100:
                    print("   ...")
                    
            if len(lines) > 30:
                print("\n... (content truncated) ...")
                
                # Display end of file
                print("\n--- End of file ---")
                for i, line in enumerate(lines[-5:]):
                    print(f"{line_count-5+i+1}: {line[:100]}")
                    if len(line) > 100:
                        print("   ...")
    except Exception as e:
        print(f"Error reading input file: {e}")

def retry_file_processing(original_filename):
    """Retry processing a specific file with MetaMap."""
    # Get paths
    default_input_dir = get_config_value("default_input_dir", "./input_notes")
    default_output_dir = get_config_value("default_output_dir", "./output_csvs")
    input_path = os.path.join(default_input_dir, original_filename)
    output_csv_path = derive_output_csv_path(default_output_dir, original_filename)
    
    # Check if the input file exists
    if not os.path.exists(input_path):
        print(f"Error: Input file not found: {input_path}")
        return
    
    # Ask for an increased timeout
    default_timeout = int(get_config_value("pymm_timeout", str(DEFAULT_PYMM_TIMEOUT)))
    try:
        new_timeout = input(f"Enter timeout in seconds (default: {default_timeout * 2}, original: {default_timeout}): ")
        if new_timeout.strip() and new_timeout.isdigit():
            timeout = int(new_timeout)
        else:
            timeout = default_timeout * 2  # Double the default timeout for retries
    except:
        timeout = default_timeout * 2
    
    # Check for any existing output file
    if os.path.exists(output_csv_path):
        backup_path = output_csv_path + f".bak.{int(time.time())}"
        try:
            shutil.copy2(output_csv_path, backup_path)
            print(f"Existing output file backed up to: {os.path.basename(backup_path)}")
            os.remove(output_csv_path)
        except Exception as e:
            print(f"Warning: Failed to backup/remove existing output file: {e}")
    
    print(f"\nProcessing file: {original_filename}")
    print(f"Input: {input_path}")
    print(f"Output: {output_csv_path}")
    print(f"Timeout: {timeout} seconds")
    
    # Initialize PyMetaMap
    try:
        metamap_binary_path = get_config_value("metamap_binary_path")
        if not metamap_binary_path:
            print("Error: MetaMap binary path not configured")
            return
            
        print("Initializing PyMetaMap...")
        from pymm import Metamap as PyMetaMap
        mm = PyMetaMap(metamap_binary_path, debug=True)
        
        # Read the input file
        with open(input_path, 'r', encoding='utf-8') as f_in:
            whole_note = f_in.read().strip()
        
        if not whole_note:
            print("Input file is empty. Skipping.")
            return
            
        lines = [whole_note]
        
        print(f"Processing with timeout {timeout}s...")
        start_time = time.time()
        
        # Process the file
        mmos_iter = mm.parse(lines, timeout=timeout)
        
        # Check for empty result
        if not mmos_iter or (hasattr(mmos_iter, '__len__') and len(mmos_iter) == 0):
            print("Warning: No concepts found or XML parsing error")
            # Write a valid CSV file with just the headers
            with open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:
                f_out.write(f"{START_MARKER_PREFIX}{original_filename}\n")
                writer = csv.writer(f_out, quoting=csv.QUOTE_ALL, doublequote=True)
                writer.writerow(CSV_HEADER)
                f_out.write(f"{END_MARKER_PREFIX}{original_filename}:ERROR\n")
            print("Created empty output CSV with error marker")
            return
                
        # Extract concepts and write to CSV
        concepts_list = [concept for mmo_item in mmos_iter for concept in mmo_item]
        print(f"Found {len(concepts_list)} concepts")
        
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:
            f_out.write(f"{START_MARKER_PREFIX}{original_filename}\n")
            writer = csv.writer(f_out, quoting=csv.QUOTE_ALL, doublequote=True)
            writer.writerow(CSV_HEADER)
            
            # Process concepts (simplified version)
            if concepts_list:
                for concept in concepts_list:
                    try:
                        if not concept.ismapping:
                            continue
                            
                        concept_name = getattr(concept, 'concept_name', getattr(concept, 'preferred_name', concept.matched))
                        pref_name = getattr(concept, 'preferred_name', concept_name)
                        phrase_text = getattr(concept, 'phrase_text', None) or getattr(concept, 'phrase', None) or getattr(concept, 'matched', '')
                        sem_types_formatted = ":".join(concept.semtypes) if getattr(concept, 'semtypes', None) else ""
                        sources_formatted = "|".join(concept.sources) if getattr(concept, 'sources', None) else ""
                        
                        # Get position value (simplified)
                        position_value = ""
                        if concept.pos_start is not None and concept.pos_length is not None:
                            position_value = f"{concept.pos_start}:{concept.pos_length}"
                        elif concept.phrase_start is not None and concept.phrase_length is not None:
                            position_value = f"{concept.phrase_start}:{concept.phrase_length}"
                        
                        row_data = [
                            concept.cui,
                            concept.score,
                            concept_name,
                            pref_name,
                            phrase_text,
                            sem_types_formatted,
                            sources_formatted,
                            position_value,
                        ]
                        writer.writerow([pymm_escape_csv_field(field) for field in row_data])
                    except Exception as e_concept:
                        print(f"Error processing concept: {e_concept}")
            
            # Write end marker with success
            f_out.write(f"{END_MARKER_PREFIX}{original_filename}\n")
        
        end_time = time.time()
        duration = end_time - start_time
        print(f"File processed successfully in {duration:.2f} seconds")
        print(f"Output written to: {output_csv_path}")
        
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        traceback.print_exc()
        
        # Write error marker if possible
        try:
            with open(output_csv_path, 'w', newline='', encoding='utf-8') as f_out:
                f_out.write(f"{START_MARKER_PREFIX}{original_filename}\n")
                writer = csv.writer(f_out, quoting=csv.QUOTE_ALL, doublequote=True)
                writer.writerow(CSV_HEADER)
                f_out.write(f"{END_MARKER_PREFIX}{original_filename}:ERROR\n")
            print("Created output CSV with error marker")
        except:
            pass
    finally:
        # Clean up
        if 'mm' in locals():
            try:
                mm.close()
            except:
                pass

def calculate_performance_metrics(state):
    """Calculate performance metrics for MetaMap processing based on state data.
    
    Args:
        state (dict): The current state dictionary containing processing information
        
    Returns:
        dict: A dictionary of performance metrics
    """
    metrics = {
        "throughput_per_hour": 0,
        "avg_processing_time": 0,
        "estimated_completion_time": None,
        "peak_memory_usage": 0,
        "recent_throughput": 0
    }
    
    # Get the list of completed files with timestamps
    completed_files = state.get("completed_files", [])
    if not completed_files:
        return metrics
    
    # Calculate average processing time
    start_time = parse_iso_datetime_compat(state.get("start_time", datetime.now().isoformat()))
    current_time = datetime.now()
    elapsed_hours = (current_time - start_time).total_seconds() / 3600
    
    # Calculate overall throughput
    if elapsed_hours > 0:
        metrics["throughput_per_hour"] = len(completed_files) / elapsed_hours
        
    # Get recent throughput (last 10 minutes)
    recent_files = [f for f in completed_files 
                    if "completion_time" in f and 
                    (current_time - parse_iso_datetime_compat(f["completion_time"])).total_seconds() < 600]
    if recent_files:
        metrics["recent_throughput"] = len(recent_files) * 6  # Files per hour (10 minutes * 6 = 1 hour)
    
    # Calculate average processing time per file (from the most recent 50 files)
    recent_completed = sorted(completed_files, 
                             key=lambda x: x.get("completion_time", ""),
                             reverse=True)[:50]
    
    if recent_completed:
        processing_times = []
        for file_info in recent_completed:
            if "start_time" in file_info and "completion_time" in file_info:
                start = parse_iso_datetime_compat(file_info["start_time"])
                end = parse_iso_datetime_compat(file_info["completion_time"])
                processing_times.append((end - start).total_seconds())
        
        if processing_times:
            metrics["avg_processing_time"] = sum(processing_times) / len(processing_times)
    
    # Estimate completion time
    remaining_files = state.get("remaining_file_count", 0)
    if remaining_files > 0 and metrics["recent_throughput"] > 0:
        hours_remaining = remaining_files / metrics["recent_throughput"]
        metrics["estimated_completion_time"] = current_time + datetime.timedelta(hours=hours_remaining)
    
    # Try to get peak memory usage if psutil is available
    try:
        import psutil
        metrics["peak_memory_usage"] = psutil.Process().memory_info().rss / (1024 * 1024)  # MB
    except (ImportError, AttributeError):
        pass
        
    return metrics

def display_performance_metrics(metrics):
    """Display performance metrics in a formatted way.
    
    Args:
        metrics (dict): Dictionary of performance metrics from calculate_performance_metrics
    """
    print("\n--- Performance Metrics ---")
    print(f"Throughput: {metrics['throughput_per_hour']:.2f} files/hour overall, {metrics['recent_throughput']:.2f} files/hour recent")
    
    if metrics['avg_processing_time'] > 0:
        print(f"Average processing time: {metrics['avg_processing_time']:.2f} seconds per file")
    
    if metrics['estimated_completion_time']:
        print(f"Estimated completion: {metrics['estimated_completion_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    
    if metrics['peak_memory_usage'] > 0:
        print(f"Peak memory usage: {metrics['peak_memory_usage']:.2f} MB")
    
    print("-------------------------")

def is_wsd_server_running():
    """Check if the WSD server is actually running by trying to connect to port 5554."""
    import socket
    try:
        # Try to connect to WSD server port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)  # 2 second timeout
        result = sock.connect_ex(('localhost', 5554))
        sock.close()
        
        # If result is 0, the port is open and server is likely running
        return result == 0
    except Exception as e:
        print(f"Error checking WSD server: {e}")
        return False

if __name__ == "__main__":
    import multiprocessing as _mp
    # Required on Windows when using ProcessPoolExecutor
    _mp.freeze_support()
    main() 