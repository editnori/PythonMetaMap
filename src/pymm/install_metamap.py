#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import tarfile
import urllib.request
import subprocess
import shutil # For rmtree
import sys # For sys.path manipulation
from os.path import abspath, exists, join, dirname, basename

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

# MetaMap 2020 URLs
# Changed MAIN_URL to download the source code for the public_mm_2020 tag
MAIN_URL = "https://github.com/LHNCBC/MetaMap-src/archive/refs/tags/public_mm_2020.tar.gz"
# The following URLs are likely dead and will be commented out for now.
# DATA_URL = "https://github.com/LHNCBC/MetaMap-src/releases/download/public_mm_2020/public_mm_data_2020.tar.bz2"
# WSD_URL = "https://github.com/LHNCBC/MetaMap-src/releases/download/public_mm_2020/public_mm_wsd_2020.tar.bz2"

# Define Project Root and META_INSTALL_DIR relative to this script's location
# This script is in .../PythonMetaMap/src/pymm/install_metamap.py
_INSTALL_SCRIPT_REAL_PATH = dirname(abspath(__file__))  # .../PythonMetaMap/src/pymm
_PYMM_DIR = _INSTALL_SCRIPT_REAL_PATH                 # Alias for clarity
_SRC_DIR = dirname(_PYMM_DIR)                         # .../PythonMetaMap/src
PROJECT_ROOT = dirname(_SRC_DIR)                      # .../PythonMetaMap
META_INSTALL_DIR = join(PROJECT_ROOT, "metamap_install") # .../PythonMetaMap/metamap_install

# This is the directory name INSIDE the tar.gz from GitHub for this tag
SOURCE_DIR_NAME = "MetaMap-src-public_mm_2020" 
INST_SCRIPT_SUBPATH = os.path.join(SOURCE_DIR_NAME, "install.sh")
INST_SCRIPT = os.path.join(META_INSTALL_DIR, INST_SCRIPT_SUBPATH)
# Expected location of public_mm after install.sh runs from within SOURCE_DIR_NAME
EXPECTED_PUBLIC_MM_PATH = os.path.join(META_INSTALL_DIR, SOURCE_DIR_NAME, "public_mm")
EXPECTED_METAMAP_BINARY = os.path.join(EXPECTED_PUBLIC_MM_PATH, "bin", "metamap")

def download_and_extract(url, extract_to_dir, is_source=False):
    if not exists(extract_to_dir):
        os.makedirs(extract_to_dir)
    tar_filename = os.path.basename(url)
    tar_path = os.path.join(extract_to_dir, tar_filename)
    
    print(f"Downloading {url}...")
    try:
        response = urllib.request.urlopen(url)
        total_size = int(response.getheader('Content-Length', 0))
        block_size = 8192 # 8KB per chunk
        
        with (
            open(tar_path, 'wb') as out_file,
            tqdm(total=total_size, unit='B', unit_scale=True, desc=tar_filename, ascii=" #") as pbar
        ):
            while True:
                chunk = response.read(block_size)
                if not chunk:
                    break
                out_file.write(chunk)
                pbar.update(len(chunk))
    except Exception as e:
        print(f"Error during download: {e}")
        # Try falling back to urlretrieve if the chunked download failed (e.g. no Content-Length)
        try:
            print("Falling back to simple download...")
            urllib.request.urlretrieve(url, tar_path)
        except Exception as e_fallback:
            print(f"Simple download fallback also failed: {e_fallback}")
            return False # Indicate failure

    print(f"Extracting {tar_path} to {extract_to_dir}...")
    mode = "r:gz" if is_source else "r:bz2"
    try:
        with tarfile.open(tar_path, mode) as tar:
            members = tar.getmembers()
            for member in tqdm(members, desc=f"Extracting {tar_filename}", unit="file", ascii=" #"):
                tar.extract(member, path=extract_to_dir)
        print(f"Successfully extracted {tar_filename}.")
        return True # Indicate success
    except tarfile.ReadError as e:
        print(f"Error reading tar file {tar_path} with mode {mode}. It might not be a valid {mode.split(':')[1]} compressed tar file or the download failed. Error: {e}")
        print("Please check the downloaded file. If it's a HTML error page, the URL is likely incorrect or the asset is missing.")
        return False # Indicate failure
    finally:
        # Optionally remove the tarball after extraction
        # if exists(tar_path): os.remove(tar_path)
        pass # pragma: no cover

def run_install_script():
    print(f"Attempting to run install script: {INST_SCRIPT}")
    install_script_dir = os.path.dirname(INST_SCRIPT)
    if exists(INST_SCRIPT):
        print(f"Changing current directory to: {install_script_dir} for install.sh execution")
        # Grant execute permissions
        subprocess.call(["chmod", "+x", INST_SCRIPT])
        # Run the install script from its directory
        try:
            process = subprocess.Popen(["./" + os.path.basename(INST_SCRIPT)], cwd=install_script_dir, 
                                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            
            # Stream output with tqdm
            if process.stdout:
                with tqdm(desc="install.sh output", unit=" lines", ascii=" #") as pbar:
                    for line in iter(process.stdout.readline, ''):
                        sys.stdout.write(line) # Use sys.stdout.write for direct printing
                        sys.stdout.flush()
                        pbar.update(1)
                process.stdout.close()
            
            process.wait()
            
            if process.returncode == 0:
                print(f"\nInstallation script finished successfully (return code 0).")
                return True
            else:
                print(f"\nInstallation script failed (return code {process.returncode}). Check output above for errors.")
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
    print("\nAttempting to test MetaMap installation...")
    if not exists(metamap_binary_path):
        print(f"MetaMap binary not found at the expected path: {metamap_binary_path}")
        print("Test cannot proceed. Please verify the installation.")
        return

    # Add src to sys.path to import pymm
    # Assumes install_metamap.py is in the project root, and pymm is in project_root/src/pymm
    project_root = abspath(dirname(__file__))
    src_path = join(project_root, "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    try:
        from pymm import Metamap
        print(f"Initializing Metamap with binary: {metamap_binary_path}")
        # For testing, ensure METAMAP_PROCESSING_OPTIONS is unset or uses benign defaults
        # or cmdexecutor.py handles it. We assume default options in cmdexecutor are okay for a basic test.
        # os.environ.pop('METAMAP_PROCESSING_OPTIONS', None) # Optional: ensure no conflicting opts
        
        mm = Metamap(metamap_binary_path, debug=False) # Debug false for cleaner test output
        test_sentences = ["The patient reported heart attack and chest pain."]
        print(f"Parsing test sentence: '{test_sentences[0]}'")
        mmos = mm.parse(test_sentences, timeout=60) # Generous timeout for first run
        
        concepts_found = []
        if mmos:
            for mmo in mmos:
                for concept in mmo:
                    concepts_found.append(concept)
        
        if concepts_found:
            print(f"SUCCESS: MetaMap test found {len(concepts_found)} concepts.")
            print("Example concept CUI:", concepts_found[0].cui)
        else:
            print("WARNING: MetaMap test ran but found no concepts. This might indicate an issue with data files or configuration.")
        mm.close() # Clean up temp files

    except ImportError:
        print("ERROR: Could not import Metamap from src.pymm. Ensure pymm is installed or src path is correct.")
    except Exception as e:
        print(f"ERROR: An error occurred during MetaMap test: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("Starting MetaMap 2020 Source Code Download & Installation Process...")

    if exists(EXPECTED_PUBLIC_MM_PATH):
        print(f"\nAn existing MetaMap installation appears to be present at: {EXPECTED_PUBLIC_MM_PATH}")
        choice = input("Do you want to remove it and reinstall? (yes/no): ").strip().lower()
        if choice == 'yes':
            print(f"Removing existing installation directory: {META_INSTALL_DIR}...")
            try:
                shutil.rmtree(META_INSTALL_DIR)
                print("Existing installation removed.")
            except Exception as e:
                print(f"Error removing existing installation: {e}. Please remove it manually and try again.")
                return
        else:
            print("Skipping reinstallation. To run tests on the existing installation, you might need to do it manually.")
            print("If you want to proceed with testing this existing installation, ensure METAMAP_BINARY_PATH points to it.")
            # test_metamap_installation(EXPECTED_METAMAP_BINARY) # Or allow user to specify path
            return
    
    print("This script will attempt to download the MetaMap 2020 source code")
    print("and run its 'install.sh' script.")
    print("NOTE: A full MetaMap installation typically requires additional data files,")
    print("which this script no longer attempts to download due to unavailable links for the 2020 version.")
    print("Successful installation from source depends on the 'install.sh' script's ability")
    print("to fetch necessary components or on you providing them separately.")
    print("It is highly recommended to consult the official NLM MetaMap installation documentation.")
    print("This process works best in a Linux or WSL environment.\n")

    if not download_and_extract(MAIN_URL, META_INSTALL_DIR, is_source=True):
        print("Download or extraction failed. Aborting installation.")
        return
    
    print("\nProceeding to run the installation script from the downloaded source code...")
    install_successful = run_install_script()

    if install_successful:
        # Check if the binary exists as a primary indicator of success for programmatic calls
        if os.path.exists(EXPECTED_METAMAP_BINARY):
            print(f"MetaMap installation script finished. Binary found at: {EXPECTED_METAMAP_BINARY}")
            # Optionally run the test if called directly, but for import, binary existence is key.
            if __name__ == "__main__":
                test_metamap_installation(EXPECTED_METAMAP_BINARY)
            return EXPECTED_METAMAP_BINARY
        else:
            print(f"\nInstallation script finished, but MetaMap binary not found at expected location: {EXPECTED_METAMAP_BINARY}")
            return None
    else:
        print("\nInstallation script failed or did not run. Skipping post-installation test.")
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
