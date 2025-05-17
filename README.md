# PythonMetaMap

PythonMetaMap provides a lightweight Python interface for running [MetaMap](https://github.com/LHNCBC/MetaMap-src) and parsing its XML output. The project is maintained by **Dr. Layth Qassem** and takes inspiration from the original `pymm` library by Srikanth Mujjiga.

This repository bundles a lightweight Python wrapper around the MetaMap 2020 binaries.  It provides

* a parser that exposes source vocabularies and positional information;
* a simple command line interface `pymm-cli` for batch processing of text files; and
* a helper script `install_metamap.py` which can download and install MetaMap automatically.

## Installing

Clone the repository and install in editable mode:

```bash
pip install -e .
```

To download and install MetaMap automatically, run the helper script:

```bash
python install_metamap.py
```

This places the MetaMap binaries under `metamap_install/public_mm`. If `pymm-cli` cannot locate a MetaMap binary automatically, provide the path via `--metamap-path` or the `METAMAP_PATH` environment variable.

## Command Line Usage

Process a directory of `.txt` files and create `.csv` outputs:

```bash
pymm-cli <input_dir> <output_dir>
```

The tool writes progress to `<output_dir>/.pymm_state.json` so interrupted runs can be resumed. CSV output is written in a safe format using Python's `csv` module.

## Library Example

```python
from pymm import Metamap
mm = Metamap('/path/to/metamap/bin/metamap20')
mmos = mm.parse(['heart attack'])
for mmo in mmos:
    for concept in mmo:
        print(concept.cui, concept.matched, concept.pos_start)
```

## Acknowledgement

This project builds upon ideas from the original `pymm` project by Srikanth Mujjiga.

# Python MetaMap Orchestration

This project provides a Python-based orchestration platform for running MetaMap on a collection of text files. It uses `pymetamap` to interact with MetaMap, processes files in parallel, and includes features for progress tracking, checkpointing, and error handling.

This project is maintained by **Dr. Layth Qassem** and takes inspiration from the original `pymm` library by Srikanth Mujjiga and the `mimic_controller` logic.

## Features

*   Parallel processing of input text files.
*   Extraction of UMLS concepts, including CUIs, scores, concept names, preferred names, phrases, semantic types, sources, and positional information.
*   State management for resuming interrupted batch jobs.
*   Command-line interface for starting, resuming, and monitoring batch jobs.
*   Configurable MetaMap processing options.

## Prerequisites

1.  **Python:** Python 3.8+ is recommended.
2.  **MetaMap:** You need a functional MetaMap installation.
    *   Download and install MetaMap from the [NLM MetaMap Page](https://metamap.nlm.nih.gov/). Follow the official installation instructions for your operating system.
    *   For Windows users, installing MetaMap within WSL (Windows Subsystem for Linux) is often the most straightforward approach.
    *   Ensure that the MetaMap binaries (e.g., `metamap`, `skrmedpostctl`, `wsdserverctl`) are executable and in your system's PATH or you know their exact location.
3.  **Java:** MetaMap requires Java. Ensure a compatible JDK is installed and configured.

## Setup and Installation

1.  **Clone the Repository:**
    ```bash
    git clone <your-repository-url> # Replace <your-repository-url> with the actual URL
    cd PythonMetaMap 
    ```

2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install Dependencies:**
    Install the Python package in editable mode:
    ```bash
    python -m pip install -e .
    ```

4.  **Environment Variables:**
    Configure the following environment variables before running the controller:

    *   `METAMAP_BINARY_PATH`: (Required) The absolute path to your MetaMap executable (e.g., `/opt/public_mm/bin/metamap` or `/mnt/c/MetaMap/public_mm/bin/metamap` if using WSL from Windows).
    *   `METAMAP_PROCESSING_OPTIONS`: (Optional) A string of command-line options to pass to the MetaMap binary for each invocation.
        *   Example: `"-y -Z 2020AA --lexicon db --word_sense_disambiguation"`
        *   If not set, the system uses a default set of options defined in `src/pymm/cmdexecutor.py`.
    *   `MAX_PARALLEL_WORKERS`: (Optional) The number of parallel worker processes to use for processing files. Defaults to `100` in `mimic_controller.py`. Adjust based on your system's CPU cores and memory. A sensible starting point could be the number of CPU cores.
        *   Example: `export MAX_PARALLEL_WORKERS=8`
    *   `PYMM_TIMEOUT`: (Optional) Timeout in seconds for each individual MetaMap process execution on a file. Defaults to `120` seconds in `mimic_controller.py`.

    **Note on MetaMap Servers (WSD, SKR/MedPost):**
    The current `pymm` wrapper runs the `METAMAP_BINARY_PATH` executable directly for each file. If the MetaMap options you use (e.g., for word sense disambiguation) require the WSD Server and Tagging Server (SKR/MedPost) to be running, you must start these servers manually *before* running `mimic_controller.py`.
    Example commands to start these servers (paths may vary):
    ```bash
    # In your MetaMap installation directory (e.g., public_mm)
    ./bin/skrmedpostctl start
    ./bin/wsdserverctl start
    ```
    You would also need to stop them after processing:
    ```bash
    ./bin/skrmedpostctl stop
    ./bin/wsdserverctl stop
    ```
    Future versions of this project may include a Python-based utility to manage these servers.

## Usage

The main script for orchestration is `mimic_controller.py`. After installation (`python -m pip install -e .`), you can also use `pymm-cli` as a shortcut.

**Starting a New Batch Job:**
```bash
python mimic_controller.py start <input_directory> <output_directory>
# OR using the installed CLI script:
pymm-cli start <input_directory> <output_directory>
```
*   `<input_directory>`: Path to the directory containing your input `.txt` files.
*   `<output_directory>`: Path to the directory where output `.csv` files and state information will be stored. This directory will be created if it doesn't exist.

**Resuming an Interrupted Batch Job:**
```bash
python mimic_controller.py resume <input_directory> <output_directory>
# OR
pymm-cli resume <input_directory> <output_directory>
```

**Other Subcommands:**
Both `python mimic_controller.py <subcommand> ...` and `pymm-cli <subcommand> ...` can be used.
*   `validate INPUT_DIR`: Validates input files for encoding issues.
*   `progress OUTPUT_DIR`: Shows the progress of the current or last job in `OUTPUT_DIR`.
*   `pid OUTPUT_DIR`: Shows the PID of the running controller for `OUTPUT_DIR`.
*   `kill OUTPUT_DIR`: Sends a SIGTERM signal to the controller process for `OUTPUT_DIR`.
*   `tail OUTPUT_DIR [LINES]`: Tails the log file in `OUTPUT_DIR`.
*   `pending INPUT_DIR OUTPUT_DIR`: Lists input files that are not yet successfully processed.
*   `completed OUTPUT_DIR`: Lists output CSV files that are confirmed complete by markers.
*   `sample OUTPUT_DIR [N]`: Shows a sample of N completed output files.
*   `clearout OUTPUT_DIR`: Deletes all `*.csv` output files from `OUTPUT_DIR` (keeps logs and state).
*   `badcsv OUTPUT_DIR`: Lists CSVs that might be incomplete (e.g., header only).

**Example Workflow:**

1.  Ensure MetaMap is installed and accessible.
2.  Set environment variables (`METAMAP_BINARY_PATH`, etc.).
3.  If needed, start MetaMap support servers (WSD, SKR/MedPost).
4.  Prepare your input text files in a directory (e.g., `my_notes/`).
5.  Create an output directory (e.g., `my_results/`).
6.  Start processing:
    ```bash
    python mimic_controller.py start my_notes/ my_results/
    ```
7.  Monitor progress:
    ```bash
    python mimic_controller.py progress my_results/
    ```
8.  Once finished, if you started MetaMap support servers, stop them.

## Output Format

For each input `.txt` file, a corresponding `.csv` file will be generated in the output directory. The CSV files contain the following columns:
*   `CUI`: Concept Unique Identifier
*   `Score`: MetaMap score for the concept
*   `ConceptName`: The concept name found
*   `PrefName`: Preferred Name for the CUI
*   `Phrase`: The text phrase matched by MetaMap
*   `SemTypes`: Semantic Types (colon-separated)
*   `Sources`: UMLS Source Vocabularies (pipe-separated)
*   `Positions`: Positional information (e.g., `start:length`) within the utterance/document.

## Development Notes

*   The core MetaMap interaction logic is in `src/pymm/`.
*   `mmoparser.py` handles parsing the XML output from MetaMap.
*   `cmdexecutor.py` constructs and runs the MetaMap command.
*   `pymm.py` is the main wrapper class used by the controller.
*   `mimic_controller.py` orchestrates the batch processing.

This project aims to improve upon existing MetaMap wrappers by providing better orchestration, error handling, and richer data extraction for parallel processing workflows.
