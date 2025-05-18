# PythonMetaMap

PythonMetaMap provides a lightweight Python interface for running [MetaMap](https://github.com/LHNCBC/MetaMap-src) and parsing its XML output. The project is maintained by **Dr. Layth Qassem** and takes inspiration from the original `pymm` library by Srikanth Mujjiga.

This repository bundles a lightweight Python wrapper around the MetaMap 2020 binaries.  It provides

* a parser that exposes source vocabularies and positional information;
* a fully-featured CLI (`pymm-cli`) that can **download & install MetaMap automatically**, spin up its support servers, and run large-scale batch jobs with progress tracking; and
* a helper script `install_metamap.py` which can download and install MetaMap automatically.

## Installation (3-step quick start)

```bash
# 1.  Install Python dependencies (editable mode shown here)
pip install -e .

# 2.  One-click MetaMap install (downloads ~1 GB, compiles, config saved)
pymm-cli install

# 3.  Launch the interactive menu and follow the prompts
pymm-cli
```

Behind the scenes, step 2 downloads the MetaMap 2020 source code into `metamap_install/`, runs its `install.sh`, and records the resulting binary path inside `~/.pymm_controller_config.json`.  No environment variables are required.

## Command Line Usage (non-interactive)

Process a directory of `.txt` files and create `.csv` outputs:

```bash
pymm-cli <input_dir> <output_dir>
```

The tool writes progress to `<output_dir>/.mimic_state.json`; interrupted runs can be resumed automatically. CSV output is written safely using Python's `csv` module.

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

* **Python ≥ 3.8**
* **Java 8+** (required by MetaMap's `mmserver20`; most systems already have it)

That's it – MetaMap itself is downloaded and compiled for you via `pymm-cli install`.

## Advanced Configuration (optional)

`pymm-cli` reads/writes a JSON config in your home directory. You can tweak settings such as:

* `metamap_processing_options` – command-line flags passed to MetaMap (default covers most use-cases)
* `max_parallel_workers` – override auto-detected worker count
* `default_input_dir`, `default_output_dir` – convenience paths pre-filled in menus

Edit via the interactive "Configure Settings" option or manually with a text editor.

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
    *   `MAX_PARALLEL_WORKERS`: (Optional) Number of parallel worker processes. Auto-detected from CPU cores if omitted.
    *   `PYMM_TIMEOUT`: (Optional) Timeout (seconds) for each MetaMap call. Defaults to 120.

    **Note on MetaMap Servers (WSD, SKR/MedPost):**
    If your MetaMap options require support servers you can start/stop them from the **interactive menu → "Manage MetaMap Servers"** – no manual shell commands needed.

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

This project aims to improve upon existing MetaMap wrappers by providing better orchestration, error handling, and richer data extraction for parallel processing workflows – **all from a single Python CLI (`pymm-cli`)**.  No external shell scripts are required.

## Quick-Start (One-Liner)

If you are new to MetaMap and just want to get going, run the following from the project root after installing Python dependencies:

```bash
pymm-cli install && pymm-cli
```

The `install` sub-command will download and compile MetaMap 2020 under `metamap_install/` and automatically save the discovered binary path to the local configuration file in your home directory.  Once installation finishes, simply run `pymm-cli` to open the **built-in interactive menu** where you can configure defaults, start/stop servers, and kick off batch jobs with a few keystrokes.

## Using a custom download mirror

If you keep the MetaMap archive on your own S3/UploadThing bucket (or any HTTP-reachable location), just expose the URL via an environment variable before running the installer:

```bash
export METAMAP_MAIN_ARCHIVE_URL="https://wqqatskmc4.ufs.sh/f/<UploadThingKey>"
pymm-cli install   # will fetch from your mirror instead of GitHub
```

Leave the variable in your shell profile (e.g., `.bashrc`) to make subsequent `pymm-cli install` calls idempotent and offline-friendly.

## 2025 Update – server-less batch, background mode, live dashboard

* **Server-less by default:** the controller no longer tries to start MetaMap's Tagger/WSD/mmserver20.  Your jobs run fine without Java services; starting them manually is optional.
* **Background batch launch:** when you choose "Run MetaMap Batch Processing" in the interactive menu, the job is now started in a detached process (`nohup` / Windows-detached).  You can safely close the terminal; PID is written to `.mimic_pid` and logs stream to `<output_dir>/mimic_controller.log`.
* **Live Monitor Dashboard:** interactive menu option 3 opens a real-time dashboard that refreshes every two seconds, showing CPU/RAM, progress, and active workers.
* **Better logging:** each batch automatically creates/attaches a file handler so you can `tail -f` the log while the job is running.

These improvements mean you can kick off a long run, shut your laptop lid, and come back later—all from pure Python.

## Architecture – Python Wrapper vs. Java API

Below is a **side-by-side** view of the two batch pipelines that ship with this repository.  Both reach the same goal (map free-text to UMLS CUIs) but they differ in *where* the heavy-lifting happens and *what* is returned to the caller.

```text
            ┌────────────────────┐                         ┌────────────────────┐
            │  Input directory   │                         │  Input directory   │
            │   (*.txt files)    │                         │   (*.txt files)    │
            └────────┬───────────┘                         └────────┬───────────┘
                     │                                            │
     (Python)        │                                            │       (Java)
 MIMIC Controller    │                                            │  BatchRunner01
   + tqdm progress   │                                            │  (MetaMap Java API)
   + resume logic    ▼                                            ▼
            ┌────────────────────┐                         ┌────────────────────┐
            │  pymm.Metamap      │                         │  MetaMapApiImpl     │
            │  (Python wrapper)  │                         │  (gov.nih.nlm.*)    │
            └────────┬───────────┘                         └────────┬───────────┘
                     │  builds CLI                                     │  JVM      
                     │  + spawns PROC                                   │  calls C
                     ▼                                                  ▼
            ┌────────────────────┐                         ┌────────────────────┐
            │  MetaMap binary    │                         │  MetaMap binary    │
            │  (Prolog + C)      │                         │  (started by API)  │
            └────────┬───────────┘                         └────────┬───────────┘
                     │  writes XML                                    │  streams objects
                     ▼                                                  ▼
            ┌────────────────────┐                         ┌────────────────────┐
            │  mmoparser.py      │                         │  Result / Utterance │
            │  (DOM → Concept)   │                         │  PCM / Mapping      │
            └────────┬───────────┘                         └────────┬───────────┘
                     │                                            │
                     ▼                                            ▼
            ┌────────────────────┐                         ┌────────────────────┐
            │   CSV writer       │                         │   CSV writer       │
            │ (Unicode safe)     │                         │  (java.io.*)       │
            └────────────────────┘                         └────────────────────┘
```

Key differences:

1. **Process isolation** – The Python path launches the *metamap* binary as a
   *sub-process* per worker which keeps the JVM out of the equation.  The Java
   path embeds the native code inside the same JVM.  For long-running servers
   the latter has lower per-call overhead, but restarting a wedged MetaMap is
   simpler on the Python side (just kill the process).

2. **Error containment** – When MetaMap crashes it exits with a non-zero code
   which the Python wrapper captures and retries; the Java API surfaces native
   crashes as `IOException`/`UnsatisfiedLinkError` which bubble up the stack.

3. **Resource usage** – The Python model scales well on multi-core machines by
   forking multiple workers (avoids the GIL thanks to `multiprocessing`).  The
   Java route enjoys *in-process* JNI calls but must share a single JVM heap.

4. **Portability** – Python orchestration runs anywhere the MetaMap *binary*
   runs (Linux, WSL, macOS).  The Java code needs a compatible JDK and may need
   `jna` work-arounds on Alpine, containers, etc.

### Why not JSON?

MetaMap 2020's most structured output format is still **XML** (`--XMLf#`).  It
is verbose but:

* Maintains strict ordering and nesting that mirrors the Java object model.
* Is the *only* format that carries full positional information **and** negation
  attributes in one shot.

An experimental *JSON* fielded output exists in *MetaMapLite* but it is missing
several tags (e.g. `PositionalInfo`).  Until the upstream project standardises
on JSON we keep XML as the canonical interchange format and convert down-stream
(via `mmoparser.py`) to Python objects.  The overhead is negligible compared to
MetaMap's own runtime.

### Choosing between the two pipelines

| Criterion            | Prefer **Python**                             | Prefer **Java**                                |
|----------------------|-----------------------------------------------|------------------------------------------------|
| Quick prototyping    | ✅ one `pip install`                          |                                                |
| Tight JVM eco-system |                                               | ✅ integrate with Spring / Hadoop / Spark      |
| Resource isolation   | ✅ each worker is a separate OS process        |                                                |
| Ultra-high throughput|                                               | ✅ keep MetaMap resident inside one JVM        |
| Minimal dependencies | ✅ *No Java at runtime*                       |                                                |

For heterogeneous teams you can mix & match: run the Python controller for
batch jobs on a POSIX box while exposing a lightweight wrapper around the Java
API as a REST micro-service for ad-hoc queries.

---