#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import tarfile
import urllib.request
import subprocess
import shutil # For rmtree
import sys # For sys.path manipulation
from os.path import abspath, exists, join, dirname, basename
from pathlib import Path
import shlex

try:
    from tqdm import tqdm
except ImportError:
    print("TQDM library not found. Progress bars will not be shown.")
    print("Please install it with: pip install tqdm")
    # Define a dummy tqdm class if not found, so the script doesn't break
    class tqdm:
        def __init__(self, iterable=None, *args, **kwargs):
            self.iterable = iterable
            self.total = kwargs.get('total', None)
            self.desc = kwargs.get('desc', '')
            self.unit = kwargs.get('unit', 'it')

        def __iter__(self):
            if self.iterable is not None:
                for obj in self.iterable:
                    yield obj
            else:
                # For manual updates
                pass # pragma: no cover
        
        def update(self, n=1):
            pass # pragma: no cover

        def close(self):
            pass # pragma: no cover

        def __enter__(self):
            return self # pragma: no cover

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass # pragma: no cover

# MetaMap 2020 download URL
# Allow override via environment variable so users can point to a mirrored/binary archive (e.g., UploadThing)
MAIN_URL = os.getenv(
    "METAMAP_MAIN_ARCHIVE_URL",
    "https://wqqatskmc4.ufs.sh/f/DBAghm8TQvXTPZnYG1sBRwezKdQbMgnYO325r8x6it9hNUGf",
)
# The following URLs are likely dead and will be commented out for now.
# DATA_URL = "https://github.com/LHNCBC/MetaMap-src/releases/download/public_mm_2020/public_mm_data_2020.tar.bz2"
# WSD_URL = "https://github.com/LHNCBC/MetaMap-src/releases/download/public_mm_2020/public_mm_wsd_2020.tar.bz2"

# Determine expected directory name from tarball based on URL content
if "public_mm_linux_main" in MAIN_URL:
    # Binary kit (e.g., public_mm_linux_main_2020.tar.bz2) extracts to "public_mm"
    SOURCE_DIR_NAME = "public_mm"
    IS_BINARY_KIT = True
    print(f"DEBUG: Detected BINARY kit from URL. Expecting top-level dir: {SOURCE_DIR_NAME}")
elif "MetaMap-src" in MAIN_URL: # e.g. MetaMap-src-public_mm_2020.tar.gz
    # Source kit from GitHub extracts to "MetaMap-src-public_mm_2020"
    SOURCE_DIR_NAME = "MetaMap-src-public_mm_2020"
    IS_BINARY_KIT = False
    print(f"DEBUG: Detected SOURCE kit from URL. Expecting top-level dir: {SOURCE_DIR_NAME}")
else:
    # Fallback, though unlikely if URL is one of the known ones
    SOURCE_DIR_NAME = "public_mm" # Default assumption
    IS_BINARY_KIT = True # Assume binary if unsure, as it's simpler
    print(f"DEBUG: UNKNOWN kit type from URL. Assuming BINARY. Expecting top-level dir: {SOURCE_DIR_NAME}")

# Define Project Root and META_INSTALL_DIR relative to this script's location
# This script is in .../PythonMetaMap/src/pymm/install_metamap.py
_INSTALL_SCRIPT_REAL_PATH = dirname(abspath(__file__))  # .../PythonMetaMap/src/pymm
_PYMM_DIR = _INSTALL_SCRIPT_REAL_PATH                 # Alias for clarity
_SRC_DIR = dirname(_PYMM_DIR)                         # .../PythonMetaMap/src
PROJECT_ROOT = dirname(_SRC_DIR)                      # .../PythonMetaMap
META_INSTALL_DIR = join(PROJECT_ROOT, "metamap_install") # .../PythonMetaMap/metamap_install

# This is the directory name INSIDE the tar.gz from GitHub for this tag
# SOURCE_DIR_NAME = "MetaMap-src-public_mm_2020" # Now set dynamically based on MAIN_URL
INST_SCRIPT_SUBPATH = os.path.join(SOURCE_DIR_NAME, "install.sh") # May not be correct for binary kit
INST_SCRIPT = os.path.join(META_INSTALL_DIR, INST_SCRIPT_SUBPATH)
# Expected location of public_mm after install.sh runs from within SOURCE_DIR_NAME
# For binary kit, public_mm is the SOURCE_DIR_NAME itself.
# For source kit, install.sh *creates* a public_mm directory, usually inside SOURCE_DIR_NAME or alongside.
# The most consistent final path for the actual MetaMap installation is META_INSTALL_DIR/public_mm
EXPECTED_PUBLIC_MM_PATH = os.path.join(META_INSTALL_DIR, "public_mm")
EXPECTED_METAMAP_BINARY = os.path.join(EXPECTED_PUBLIC_MM_PATH, "bin", "metamap")

# Utility: recursively search for a file name within a parent directory
def _find_file(root_dir, filename_candidates, max_depth=4):
    for dirpath, dirnames, filenames in os.walk(root_dir):
        relative_path = Path(os.path.relpath(dirpath, root_dir))
        depth = len(relative_path.parts)
        if relative_path == Path('.'): depth = 0

        if depth > max_depth:
            continue
        
        # Case-insensitive search for filename candidates
        filenames_lower = {fn.lower(): fn for fn in filenames} # Map lowercase to original case
        for fcand_template in filename_candidates:
            if fcand_template.lower() in filenames_lower:
                actual_filename = filenames_lower[fcand_template.lower()] # Get original casing
                return os.path.join(dirpath, actual_filename)
    return None

# Locate a usable MetaMap binary inside META_INSTALL_DIR (after extraction or installation)
def locate_metamap_binary():
    """Return absolute path to a MetaMap binary if found inside META_INSTALL_DIR, else None."""
    binary = _find_file(META_INSTALL_DIR, ["metamap", "metamap20"])
    if binary and os.access(binary, os.X_OK):
        return binary
    return None

# Update: adaptively detect install script path instead of hard-coding
def locate_install_script():
    # Search for install.sh specifically within the extracted source directory
    # SOURCE_DIR_NAME is globally defined (e.g., "MetaMap-src-public_mm_2020")
    # META_INSTALL_DIR is also globally defined (e.g., "metamap_install")
    search_base = os.path.join(META_INSTALL_DIR, SOURCE_DIR_NAME)
    if not os.path.isdir(search_base):
        # Fallback: if SOURCE_DIR_NAME doesn't exist (e.g. tarball extracted differently),
        # search the whole META_INSTALL_DIR. This case is less likely for the specified tarball.
        print(f"DEBUG: Expected source directory {search_base} not found. Searching from {META_INSTALL_DIR} instead.")
        search_base = META_INSTALL_DIR

    # Allow deep search for install.sh (e.g., in MetaMap-src-public_mm_2020/src/perl/metamap/)
    print(f"DEBUG: Searching for install.sh starting from: {search_base}")
    found_script = _find_file(search_base, ["install.sh"], max_depth=7)
    print(f"DEBUG: Result of _find_file for install.sh: {found_script}")
    return found_script

def download_and_extract(url, extract_to_dir, is_source=False):
    print(f"\n[Download & Extract] Target directory: {extract_to_dir}")
    
    # Handle existing directory that might be corrupted or empty
    if exists(extract_to_dir):
        try:
            # Check if directory is empty or only has hidden files
            contents = os.listdir(extract_to_dir)
            visible_contents = [f for f in contents if not f.startswith('.')]
            
            if not visible_contents:
                print(f"[Download & Extract] Directory exists but appears empty, removing it...")
                shutil.rmtree(extract_to_dir)
                print(f"[Download & Extract] Removed empty directory: {extract_to_dir}")
            else:
                print(f"[Download & Extract] Directory exists with {len(visible_contents)} items")
        except Exception as e:
            print(f"[Download & Extract] Error checking directory contents: {e}")
            print(f"[Download & Extract] Attempting to remove and recreate...")
            try:
                shutil.rmtree(extract_to_dir)
                print(f"[Download & Extract] Successfully removed problematic directory")
            except Exception as e2:
                print(f"[Download & Extract] Warning: Could not remove directory: {e2}")
                # Try to continue anyway
    
    if not exists(extract_to_dir):
        print(f"[Download & Extract] Creating directory: {extract_to_dir}")
        try:
            os.makedirs(extract_to_dir, exist_ok=True)
        except OSError as e:
            if e.errno == 17:  # File exists
                print(f"[Download & Extract] Directory creation failed with 'File exists' error")
                print(f"[Download & Extract] This might be a WSL/Windows sync issue")
                print(f"[Download & Extract] Attempting workaround...")
                # Try to create parent and then the directory
                parent_dir = os.path.dirname(extract_to_dir)
                if exists(parent_dir):
                    try:
                        # Use a different approach - create with Path
                        Path(extract_to_dir).mkdir(parents=True, exist_ok=True)
                        print(f"[Download & Extract] Directory created using alternative method")
                    except Exception as e3:
                        print(f"[Download & Extract] Alternative creation also failed: {e3}")
                        raise
            else:
                raise

    # Decide canonical filename to save as (ensures we have correct extension for binary kit)
    if IS_BINARY_KIT:
        canonical_filename = "public_mm_linux_main_2020.tar.bz2"
    else:
        canonical_filename = os.path.basename(url)

    tar_path = os.path.join(extract_to_dir, canonical_filename)

    # If an older download exists with the raw key name (no extension), rename it
    raw_key_filename = os.path.basename(url)
    raw_key_path = os.path.join(extract_to_dir, raw_key_filename)
    if raw_key_filename != canonical_filename and os.path.exists(raw_key_path) and not os.path.exists(tar_path):
        try:
            os.rename(raw_key_path, tar_path)
            print(f"Renamed existing tarball {raw_key_filename} -> {canonical_filename} for clarity.")
        except Exception as e_rn:
            print(f"Warning: could not rename {raw_key_filename} to {canonical_filename}: {e_rn}")

    tar_filename = canonical_filename
    
    # Determine the expected top-level directory name *inside* the tarball
    # This helps the "skip extraction if already extracted" logic.
    # Uses the globally determined SOURCE_DIR_NAME based on MAIN_URL
    potential_extracted_content_path = os.path.join(extract_to_dir, SOURCE_DIR_NAME)
    
    # Check 1: Skip download & extraction if the *specific content directory* (e.g., public_mm or MetaMap-src-public_mm_2020)
    # already exists and is not empty.
    if os.path.isdir(potential_extracted_content_path) and os.listdir(potential_extracted_content_path):
        print(f"Content directory {potential_extracted_content_path} already exists and is not empty. Skipping download and extraction.")
        return True # Indicate success for this step, as content is presumed to be there.

    # Check 2: Skip download if tarball itself already exists locally, but proceed to extraction.
    if os.path.exists(tar_path) and os.path.getsize(tar_path) > 0:
        print(f"Tarball {tar_path} already exists locally with size {os.path.getsize(tar_path)}. Skipping download, will proceed to extraction.")
    else:
        # If neither content path nor tarball exists, then download.
        print(f"\n[Download] Starting download from: {url}")
        print(f"[Download] Saving to: {tar_path}")
        try:
            # Add a common browser User-Agent to the request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            req = urllib.request.Request(url, headers=headers)
            # 30 s timeout – long enough for slow CDNs, short enough to fail fast
            response = urllib.request.urlopen(req, timeout=30)
            total_size = int(response.getheader('Content-Length', 0))
            block_size = 8192 # 8KB per chunk
            
            print(f"[Download] File size: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
            with (
                open(tar_path, 'wb') as out_file,
                tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading", ascii=True) as pbar
            ):
                while True:
                    chunk = response.read(block_size)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    pbar.update(len(chunk))
        except Exception as e:
            print(f"\n[Download] Error during download: {e}")
            # Try falling back to urlretrieve if the chunked download failed (e.g. no Content-Length)
            try:
                print("Falling back to simple download...")
                urllib.request.urlretrieve(url, tar_path)
            except Exception as e_fallback:
                print(f"Simple download fallback also failed: {e_fallback}")
                return False # Indicate failure

    print(f"Extracting {tar_path} to {extract_to_dir}...")

    # Determine mode dynamically from filename extension
    if tar_filename.endswith(".tar.gz") or tar_filename.endswith(".tgz"):
        mode = "r:gz"
    elif tar_filename.endswith(".tar.bz2") or tar_filename.endswith(".tbz2"):
        mode = "r:bz2"
    elif tar_filename.endswith(".tar.xz") or tar_filename.endswith(".txz"):
        mode = "r:xz"
    elif tar_filename.endswith(".tar"):
        mode = "r:" # Uncompressed tar
    else:
        # Attempt to detect format via magic bytes since filename lacks extension (common with CDN keys)
        try:
            with open(tar_path, "rb") as fh_magic:
                magic = fh_magic.read(6)
            if magic.startswith(b"BZh"):
                mode = "r:bz2"
                print("DEBUG: Detected bzip2 archive based on magic bytes.")
            elif magic.startswith(b"\x1f\x8b"):
                mode = "r:gz"
                print("DEBUG: Detected gzip archive based on magic bytes.")
            elif magic.startswith(b"\xfd7zXZ"):
                mode = "r:xz"
                print("DEBUG: Detected xz archive based on magic bytes.")
            else:
                print(f"Error: Unknown tar compression format for {tar_filename}. Cannot extract.")
                return False
        except Exception as e_magic:
            print(f"Error while attempting magic-byte detection: {e_magic}")
            return False
    
    print(f"DEBUG: Determined extraction mode: '{mode}' for filename: '{tar_filename}'")

    # Optionally rename file to include correct extension for clarity
    ext_map = {"r:bz2": ".tar.bz2", "r:gz": ".tar.gz", "r:xz": ".tar.xz", "r:": ".tar"}
    correct_ext = ext_map.get(mode, "")
    if correct_ext and not tar_filename.endswith(correct_ext):
        new_tar_path = tar_path + correct_ext
        try:
            os.rename(tar_path, new_tar_path)
            print(f"Renamed tarball to {os.path.basename(new_tar_path)} for clarity.")
            tar_path = new_tar_path
            tar_filename = os.path.basename(new_tar_path)
        except Exception as e_rename:
            print(f"Warning: Could not rename tarball: {e_rename}. Continuing with original filename.")

    # First attempt high-performance system tar extraction
    print(f"\n[Extract] Attempting extraction of {tar_path}...")
    print(f"[Extract] Compression mode: {mode}")
    if extract_with_system_tar(tar_path, extract_to_dir, mode):
        print("[Extract] ✓ System tar extraction completed successfully.")
        return True

    # Fallback to pure-Python extraction with path safety
    def _is_within_directory(directory, target):
        abs_directory = os.path.abspath(directory)
        abs_target = os.path.abspath(target)
        try:
            return os.path.commonpath([abs_directory, abs_target]) == abs_directory
        except ValueError:
            return False

    try:
        with tarfile.open(tar_path, mode) as tar:
            print("Streaming extraction … this may take several minutes (Python tarfile).")
            for member in tar.getmembers():
                member_path = os.path.join(extract_to_dir, member.name)
                if not _is_within_directory(extract_to_dir, member_path):
                    raise RuntimeError(f"Unsafe path in tar file: {member.name}")
            tar.extractall(path=extract_to_dir)
        print(f"[Extract] ✓ Successfully extracted {tar_filename} using Python tarfile.")
        return True # Indicate success
    except tarfile.ReadError as e:
        print(f"Error reading tar file {tar_path} with mode {mode}. It might not be a valid {mode.split(':')[1] if ':' in mode else 'tar'} compressed tar file or the download failed. Error: {e}")
        print("Please check the downloaded file. If it's a HTML error page, the URL is likely incorrect or the asset is missing.")
        return False # Indicate failure
    finally:
        # Optionally remove the tarball after extraction
        # if exists(tar_path): os.remove(tar_path)
        pass # pragma: no cover

# Attempt high-performance extraction using system tar + parallel decompressors
def extract_with_system_tar(tar_path: str, dest_dir: str, mode: str) -> bool:
    """Return True if system tar extracted successfully, False otherwise."""
    if not shutil.which("tar"):
        return False  # no system tar (very rare on Linux/WSL)

    comp_prog = ""  # default: tar auto-detects based on suffix
    if mode == "r:bz2" and shutil.which("pbzip2"):
        comp_prog = "--use-compress-program=pbzip2"
    elif mode == "r:gz" and shutil.which("pigz"):
        comp_prog = "--use-compress-program=pigz"
    elif mode == "r:xz" and shutil.which("pixz"):
        comp_prog = "--use-compress-program=pixz"

    os.makedirs(dest_dir, exist_ok=True)
    # Build cmd parts safely without shell=True
    cmd_parts = ["tar"] + (comp_prog.split() if comp_prog else []) + ["-xvf", tar_path, "-C", dest_dir]
    print(f"[Extract] Using system tar for extraction: {' '.join(cmd_parts)}")
    print("[Extract] Extraction progress:")
    process = subprocess.Popen(cmd_parts, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    if process.stdout:
        for line in iter(process.stdout.readline, ''):
            print(line, end='') # Print each line as it comes
        process.stdout.close()
    ret = process.wait()
    return ret == 0

def run_install_script():
    print(f"\n[Install Script] Starting MetaMap installation script...")
    print(f"[Install Script] Script location: {INST_SCRIPT}")
    install_script_dir = os.path.dirname(INST_SCRIPT)            # …/public_mm/bin
    public_mm_dir = os.path.dirname(install_script_dir)          # …/public_mm (one level up)

    # Decide where to execute the script from so that the default basedir is correct
    print(f"[Install Script] Script directory: {install_script_dir}")
    print(f"[Install Script] Public MM directory: {public_mm_dir}")
    # If the install script is inside a conventional …/public_mm/bin/ directory, run from
    # public_mm and invoke "bash bin/install.sh". Otherwise fall back to the old behaviour.
    if (
        os.path.basename(install_script_dir) == "bin"
        and os.path.basename(public_mm_dir) == "public_mm"
    ):
        run_cwd = public_mm_dir
        cmd = ["bash", "bin/install.sh"]
    else:
        run_cwd = install_script_dir
        cmd = ["./" + os.path.basename(INST_SCRIPT)]

    if exists(INST_SCRIPT):
        print(f"[Install Script] Executing from directory: {run_cwd}")
        print(f"[Install Script] Command: {' '.join(cmd)}")
        subprocess.call(["chmod", "+x", INST_SCRIPT])
        try:
            process = subprocess.Popen(cmd, cwd=run_cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            
            # Stream output with tqdm
            if process.stdout:
                with tqdm(desc="install.sh output", unit=" lines", ascii=True) as pbar:
                    for line in iter(process.stdout.readline, ''):
                        sys.stdout.write(line) # Use sys.stdout.write for direct printing
                        sys.stdout.flush()
                        pbar.update(1)
                process.stdout.close()
            
            process.wait()
            
            if process.returncode == 0:
                print(f"\n[Install Script] ✓ Installation script finished successfully (return code 0).")
                return True
            else:
                print(f"\n[Install Script] ✗ Installation script failed (return code {process.returncode}). Check output above for errors.")
                return False
        except Exception as e:
            print(f"\nError running install.sh: {e}")
            return False
    else:
        print(f"ERROR: Install script not found at {INST_SCRIPT}")
        print("The source code might have extracted into a differently named directory.")
        print(f"Please check the contents of '{META_INSTALL_DIR}' and update SOURCE_DIR_NAME in install_metamap.py if needed.")
        return False

def test_metamap_installation(metamap_binary_path):
    print("\n" + "="*70)
    print("Testing MetaMap Installation")
    print("="*70)
    if not exists(metamap_binary_path):
        print(f"[Test] ✗ MetaMap binary not found at: {metamap_binary_path}")
        print("[Test] Test cannot proceed. Please verify the installation.")
        return

    # Add src to sys.path to import pymm
    # Assumes install_metamap.py is in the project root, and pymm is in project_root/src/pymm
    # Reuse the already-determined _SRC_DIR pointing to …/src
    src_path = _SRC_DIR
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    try:
        from pymm import Metamap
        print(f"[Test] Initializing Metamap with binary: {metamap_binary_path}")
        # For testing, ensure METAMAP_PROCESSING_OPTIONS is unset or uses benign defaults
        # or cmdexecutor.py handles it. We assume default options in cmdexecutor are okay for a basic test.
        # os.environ.pop('METAMAP_PROCESSING_OPTIONS', None) # Optional: ensure no conflicting opts
        
        mm = Metamap(metamap_binary_path, debug=False) # Debug false for cleaner test output
        test_sentences = ["The patient reported heart attack and chest pain."]
        print(f"[Test] Test sentence: '{test_sentences[0]}'")
        print("[Test] Running MetaMap analysis...")
        mmos = mm.parse(test_sentences, timeout=60) # Generous timeout for first run
        
        concepts_found = []
        if mmos:
            for mmo in mmos:
                for concept in mmo:
                    concepts_found.append(concept)
        
        if concepts_found:
            print(f"\n[Test] ✓ SUCCESS: MetaMap found {len(concepts_found)} concepts")
            print(f"[Test] Example concept: CUI={concepts_found[0].cui}, Preferred Name={concepts_found[0].preferred_name}")
            print("\n[Test] MetaMap is working correctly!")
        else:
            print("\n[Test] ⚠ WARNING: MetaMap ran but found no concepts.")
            print("[Test] This might indicate an issue with data files or configuration.")
        mm.close() # Clean up temp files

    except ImportError:
        print("ERROR: Could not import Metamap from src.pymm. Ensure pymm is installed or src path is correct.")
    except Exception as e:
        print(f"ERROR: An error occurred during MetaMap test: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("\n" + "="*70)
    print("MetaMap 2020 Installation Process")
    print("="*70)
    print(f"\n[Config] Installation directory: {META_INSTALL_DIR}")
    print(f"[Config] Expected binary location: {EXPECTED_METAMAP_BINARY}")
    print(f"[Config] Source directory name: {SOURCE_DIR_NAME}")

    # Check for a fully completed installation first (e.g. metamap_install/public_mm/bin/metamap)
    print("\n[Check] Looking for existing MetaMap installation...")
    if exists(EXPECTED_METAMAP_BINARY) and os.access(EXPECTED_METAMAP_BINARY, os.X_OK):
        print(f"[Check] ✓ MetaMap found at: {EXPECTED_METAMAP_BINARY}")
        print(f"[Check] Binary is executable: Yes")
        
        # Provide clear options for existing installation
        print("\n[Options] MetaMap is already installed. What would you like to do?")
        print("  1. Keep existing installation and exit")
        print("  2. Run install.sh script to reconfigure")
        print("  3. Complete reinstallation (remove and reinstall)")
        
        if os.getenv("PYMM_FORCE_REINSTALL", "").lower() in {"yes", "true", "1"}:
            choice = "3"
        elif not sys.stdin.isatty():
            choice = "1"
        else:
            choice = input("\nSelect option [1/2/3] (default: 1): ").strip() or "1"
        
        if choice == "1":
            print("\n[Decision] Keeping existing installation.")
            test_metamap_installation(EXPECTED_METAMAP_BINARY)
            return EXPECTED_METAMAP_BINARY
            
        elif choice == "2":
            print("\n[Decision] Running install.sh script to reconfigure...")
            install_script_path = os.path.join(META_INSTALL_DIR, "public_mm", "bin", "install.sh")
            if os.path.exists(install_script_path):
                print(f"Running install script: {install_script_path}")
                # Setup the running environment
                install_script_dir = os.path.dirname(install_script_path)
                public_mm_dir = os.path.dirname(install_script_dir)
                
                # Run the script - similar to run_install_script() but with specific path
                try:
                    subprocess.call(["chmod", "+x", install_script_path])
                    print(f"Executing installer from: {public_mm_dir}")
                    process = subprocess.Popen(["bash", "bin/install.sh"], cwd=public_mm_dir, 
                                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
                                             text=True, bufsize=1)
                    
                    # Stream output with tqdm
                    if process.stdout:
                        with tqdm(desc="install.sh output", unit=" lines", ascii=True) as pbar:
                            for line in iter(process.stdout.readline, ''):
                                sys.stdout.write(line)
                                sys.stdout.flush()
                                pbar.update(1)
                        process.stdout.close()
                    
                    process.wait()
                    
                    if process.returncode == 0:
                        print(f"\n[Install Script] ✓ Installation script finished successfully (return code 0).")
                        test_metamap_installation(EXPECTED_METAMAP_BINARY)
                    else:
                        print(f"\n[Install Script] ✗ Installation script failed (return code {process.returncode}). Check output above for errors.")
                except Exception as e:
                    print(f"\nError running install.sh: {e}")
            else:
                print(f"[Error] Install script not found at: {install_script_path}")
            
            return EXPECTED_METAMAP_BINARY
            
        elif choice == "3":
            print("\n[Decision] Performing complete reinstallation...")
            public_mm_actual_path = os.path.join(META_INSTALL_DIR, "public_mm")
            print(f"\n[Cleanup] Removing existing installation directory: {public_mm_actual_path}...")
            try:
                if os.path.exists(public_mm_actual_path):
                    shutil.rmtree(public_mm_actual_path)
                # Also remove the tarball if we are forcing a reinstall from scratch
                tar_filename_guess = os.path.basename(MAIN_URL)
                local_tar_path = os.path.join(META_INSTALL_DIR, tar_filename_guess)
                if os.path.exists(local_tar_path):
                    os.remove(local_tar_path)
                # Also check for renamed tarball
                canonical_tar_path = os.path.join(META_INSTALL_DIR, "public_mm_linux_main_2020.tar.bz2")
                if os.path.exists(canonical_tar_path):
                    os.remove(canonical_tar_path)
                print("[Cleanup] ✓ Existing installation artifacts removed successfully.")
            except Exception as e:
                print(f"Error removing existing installation: {e}. Please remove it manually and try again.")
                return None # Exit if removal fails
        else:
            print(f"\n[Error] Invalid option: {choice}")
            return EXPECTED_METAMAP_BINARY
    
    # If not fully installed, print advisory messages
    print("\n[Installation] MetaMap is not installed. Starting installation process...")
    print(f"[Installation] Download URL: {MAIN_URL}")
    print(f"[Installation] Installation directory: {META_INSTALL_DIR}")
    
    # Check if installation directory exists but might be problematic
    if os.path.exists(META_INSTALL_DIR):
        try:
            contents = os.listdir(META_INSTALL_DIR)
            if not contents or all(f.startswith('.') for f in contents):
                print(f"[Installation] Found empty or problematic installation directory")
                if sys.stdin.isatty():
                    cleanup = input("Remove and recreate installation directory? (yes/no) [yes]: ").strip().lower()
                    if cleanup != 'no':
                        print(f"[Installation] Removing {META_INSTALL_DIR}...")
                        shutil.rmtree(META_INSTALL_DIR)
                        print("[Installation] Directory removed successfully")
                else:
                    # Non-interactive mode - try to clean up automatically
                    print(f"[Installation] Automatically cleaning up {META_INSTALL_DIR}...")
                    shutil.rmtree(META_INSTALL_DIR)
        except Exception as e:
            print(f"[Installation] Warning: Error checking installation directory: {e}")
    
    if IS_BINARY_KIT:
        print("This appears to be a BINARY kit (includes pre-compiled MetaMap and data files).")
        print("The script will extract it and run its internal setup (install.sh).")
    else:
        print("This appears to be a SOURCE kit.")
        print("It requires compilation and separate data file downloads (not handled by this script).")
        print("Success depends on the 'install.sh' script within the source to build correctly.")
    print("It is highly recommended to consult the official NLM MetaMap installation documentation.")
    print("This process works best in a Linux or WSL environment.\n")

    try:
        if not download_and_extract(MAIN_URL, META_INSTALL_DIR, is_source=IS_BINARY_KIT):
            print("\n[Error] Download or extraction failed. Aborting installation.")
            return None
    except Exception as e:
        print(f"\n[Error] Installation failed: {e}")
        
        # Offer to clean up and retry
        if "File exists" in str(e) and sys.stdin.isatty():
            print("\n[Error] This appears to be a file system synchronization issue.")
            retry = input("Would you like to clean up and retry? (yes/no) [yes]: ").strip().lower()
            if retry != 'no':
                try:
                    if os.path.exists(META_INSTALL_DIR):
                        shutil.rmtree(META_INSTALL_DIR)
                    print("[Cleanup] Directory removed. Retrying installation...")
                    if not download_and_extract(MAIN_URL, META_INSTALL_DIR, is_source=IS_BINARY_KIT):
                        print("\n[Error] Download or extraction failed on retry.")
                        return None
                except Exception as e2:
                    print(f"\n[Error] Cleanup and retry failed: {e2}")
                    return None
        else:
            return None

    # --- Enhanced Debugging ---
    print(f"DEBUG: Listing contents of META_INSTALL_DIR ({META_INSTALL_DIR}):")
    if os.path.exists(META_INSTALL_DIR) and os.path.isdir(META_INSTALL_DIR):
        for item_name in os.listdir(META_INSTALL_DIR):
            print(f"DEBUG:   - {item_name}")
            # If it's the expected source directory, list its contents too
            if item_name == SOURCE_DIR_NAME: # SOURCE_DIR_NAME is "MetaMap-src-public_mm_2020"
                sub_dir_path = os.path.join(META_INSTALL_DIR, item_name)
                print(f"DEBUG:   Listing contents of {sub_dir_path}:")
                if os.path.exists(sub_dir_path) and os.path.isdir(sub_dir_path):
                    for sub_item_name in os.listdir(sub_dir_path):
                        print(f"DEBUG:     - {sub_item_name}")
                        if sub_item_name.lower() == "install.sh":
                             print(f"DEBUG:       (Found install.sh directly: {os.path.join(sub_dir_path, sub_item_name)})")
                else:
                    print(f"DEBUG:     (Directory {sub_dir_path} does not exist or is not a directory)")
    else:
        print(f"DEBUG:   (Directory {META_INSTALL_DIR} does not exist or is not a directory)")
    print("DEBUG: --- End of directory listing ---")
    # --- End Enhanced Debugging ---

    print("\n[Search] Looking for MetaMap binary in extracted files...")
    auto_binary = locate_metamap_binary()
    if auto_binary:
        print(f"[Search] ✓ Found MetaMap binary: {auto_binary}")
        return auto_binary
    else:
        print("[Search] Binary not found. Need to run installation script.")

    # If not found, try to locate and run install.sh wherever it was extracted
    dynamic_install_script = locate_install_script()
    if dynamic_install_script and os.path.isfile(dynamic_install_script):
        global INST_SCRIPT
        INST_SCRIPT = dynamic_install_script  # Override for run_install_script
        print(f"Found install script at: {INST_SCRIPT}")
    else:
        print(f"DEBUG: Dynamic install script found by locate_install_script() was: {dynamic_install_script}")
        print(f"DEBUG: os.path.isfile(dynamic_install_script) result: {os.path.isfile(dynamic_install_script) if dynamic_install_script else 'N/A (path is None)'}")
        print("install.sh not found by locate_install_script or is not a file – will skip compilation step.")
        install_successful = False
    if dynamic_install_script:
        print("Proceeding to run the installation script from the downloaded source code…")
        install_successful = run_install_script()
    else:
        install_successful = False

    if install_successful:
        # Check if the binary exists as a primary indicator of success for programmatic calls
        if os.path.exists(EXPECTED_METAMAP_BINARY):
            print(f"\n[Summary] MetaMap installation completed successfully!")
            print(f"[Summary] Binary location: {EXPECTED_METAMAP_BINARY}")
            # Optionally run the test if called directly, but for import, binary existence is key.
            if __name__ == "__main__":
                test_metamap_installation(EXPECTED_METAMAP_BINARY)
            return EXPECTED_METAMAP_BINARY
        else:
            print(f"\n[Summary] ✗ Installation script finished, but MetaMap binary not found at: {EXPECTED_METAMAP_BINARY}")
            # Final attempt: maybe binary exists even though install script failed
            fallback_bin = locate_metamap_binary()
            if fallback_bin:
                print(f"MetaMap binary detected at: {fallback_bin}")
                return fallback_bin
            return None
    else:
        print("\n[Summary] Installation script failed or did not run.")
        # Final attempt: maybe binary exists even though install script failed
        print("[Summary] Searching for MetaMap binary...")
        fallback_bin = locate_metamap_binary()
        if fallback_bin:
            print(f"[Summary] ✓ MetaMap binary found at: {fallback_bin}")
            return fallback_bin
        else:
            print("[Summary] ✗ No MetaMap binary found")
        return None

    # The following is now largely unreachable due to returns above, but kept for context if __main__ block changes
    print("\n---------------------------------------------------------------------")
    print("Script finished.")
    print("Please check the output above for any errors during the process.")
    print(f"If install.sh ran, MetaMap might be installed in '{EXPECTED_PUBLIC_MM_PATH}'.")
    print("Refer to MetaMap's documentation for verification and to complete setup (e.g., data files, server configurations).")
    print("---------------------------------------------------------------------")

if __name__ == "__main__":
    main()
