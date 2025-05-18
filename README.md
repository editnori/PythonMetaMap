# PythonMetaMap

PythonMetaMap provides a lightweight Python interface for running [MetaMap](https://github.com/LHNCBC/MetaMap-src) and parsing its XML output. The project is maintained by **Dr. Layth Qassem** and takes inspiration from the original `pymm` library by Srikanth Mujjiga.

This repository bundles a lightweight Python wrapper around the MetaMap 2020 binaries.  It provides

* a parser that exposes source vocabularies and positional information;
* a fully-featured CLI (`pymm-cli`) that can **download & install MetaMap automatically**, spin up its support servers, and run large-scale batch jobs with progress tracking; and
* a helper script `install_metamap.py` which can download and install MetaMap automatically.

## Installation (3-step quick start)

### Method 1: Install from PyPI (Recommended)

```bash
# 1. Install the package directly from PyPI
pip install -i https://test.pypi.org/simple/ pythonmetamap==0.3.0

# 2. One-click MetaMap install (downloads ~1 GB, compiles, config saved)
pymm-cli install

# 3. Launch the interactive menu and follow the prompts
pymm-cli
```

### Method 2: Install from Source (Development)

```bash
# 1. Clone the repository
git clone https://github.com/editnori/PythonMetaMap.git
cd PythonMetaMap

# 2. Install Python dependencies in editable mode
pip install -e .

# 3. One-click MetaMap install (downloads ~1 GB, compiles, config saved)
pymm-cli install

# 4. Launch the interactive menu and follow the prompts
pymm-cli
```

Behind the scenes, the installation step downloads the MetaMap 2020 source code into `metamap_install/`, runs its `install.sh`, and records the resulting binary path inside `~/.pymm_controller_config.json`.  No environment variables are required.

## Command Line Usage (non-interactive)

Process a directory of `.txt` files and create `.csv` outputs:

```bash
pymm-cli start <input_dir> <output_dir>
```

The tool writes progress to `<output_dir>/.mimic_state.json`; interrupted runs can be resumed automatically. CSV output is written safely using Python's `csv` module.

## New Quality-of-Life Features (v0.4.0)

The latest version includes significant improvements to the user interface:

### Enhanced Dashboard
- Real-time CPU and RAM monitoring for each worker process
- Trend tracking with up/down arrows for resource usage
- Color-coded CPU usage warnings
- Detailed progress bar visualization
- Clear status indicators
- **NEW: Performance metrics view with throughput analysis**
- **NEW: Pause/resume functionality to stop auto-scrolling (press 'p')**
- **NEW: Multiple view modes with keyboard shortcuts (s: summary, w: workers, f: files, t: stats)**

### Improved File Management
- Detailed file analysis with statistics
- View both input and output files
- Pagination for browsing large file collections
- Error analysis for failed files with log inspection
- Comprehensive file search capabilities
- **NEW: Automatic dependency checking and installation**
- **NEW: Enhanced file snippets for better content preview**

### Better Batch Progress Display
- Clear status indicators: [RUNNING], [COMPLETED], [FAILED]
- Detailed statistics (total, completed, failed, retries)
- ASCII progress bar visualization
- Improved time estimation with hours/minutes/seconds format
- **NEW: Advanced performance metrics (throughput per hour, estimated completion time)**
- **NEW: Memory usage tracking for processing optimization**

### Retry Failed Files Option
- Dedicated menu option for retrying failed files
- Automatic detection of files that failed processing
- Option to set increased timeout for problematic files
- Batch processing with detailed feedback
- Automatic cleanup of failed files after successful retry
- **NEW: Automatic nohup background mode for retries on Unix systems**
- **NEW: Unique batch IDs for better tracking of retry operations**

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
*   Advanced file management and retry capabilities.

## Prerequisites

* **Python ≥ 3.8**
* **Java 8+** (required by MetaMap's `mmserver20`; most systems already have it)

That's it – MetaMap itself is downloaded and compiled for you via `pymm-cli install`.

## Advanced Configuration (optional)

`pymm-cli` reads/writes a JSON config in your home directory. You can tweak settings such as:

* `metamap_processing_options` – command-line flags passed to MetaMap (default covers most use-cases)
* `max_parallel_workers` – override auto-detected worker count
* `default_input_dir`, `default_output_dir` – convenience paths pre-filled in menus
* `pymm_timeout` - timeout (in seconds) for each MetaMap file processing
* `java_heap_size` - control memory allocation for MetaMap's Java process

Edit via the interactive "Configure Settings" option or manually with a text editor.

## Setup and Installation

1.  **Install from PyPI:**
    ```bash
    pip install -i https://test.pypi.org/simple/ pythonmetamap==0.3.0
    ```

2.  **Install MetaMap:**
    ```bash
    pymm-cli install
    ```

3.  **Environment Variables (Optional):**
    The following environment variables can be used to configure the controller, though the interactive configuration is preferred:

    *   `METAMAP_BINARY_PATH`: The absolute path to your MetaMap executable
    *   `METAMAP_PROCESSING_OPTIONS`: A string of command-line options to pass to the MetaMap binary
    *   `MAX_PARALLEL_WORKERS`: Number of parallel worker processes
    *   `PYMM_TIMEOUT`: Timeout (seconds) for each MetaMap call (default: 300)
    *   `JAVA_HEAP_SIZE`: Memory allocation for Java (e.g., "4g", "16g")

    **Note on MetaMap Servers (WSD, SKR/MedPost):**
    If your MetaMap options require support servers you can start/stop them from the **interactive menu → "Manage MetaMap Servers"** – no manual shell commands needed.

## Usage

**Using the Interactive CLI:**
```bash
pymm-cli
```
This brings up the menu with all available options.

**Starting a New Batch Job:**
```bash
pymm-cli start <input_directory> <output_directory>
```
*   `<input_directory>`: Path to the directory containing your input `.txt` files
*   `<output_directory>`: Path to the directory where output `.csv` files and state information will be stored

**Resuming an Interrupted Batch Job:**
```bash
pymm-cli resume <input_directory> <output_directory>
```

**Other Subcommands:**
*   `validate INPUT_DIR`: Validates input files for encoding issues
*   `progress OUTPUT_DIR`: Shows the progress of the current or last job
*   `pid OUTPUT_DIR`: Shows the PID of the running controller process
*   `kill OUTPUT_DIR`: Sends a SIGTERM signal to the controller process
*   `tail OUTPUT_DIR [LINES]`: Tails the log file in the output directory
*   `killall`: Terminates all running MetaMap processes
*   `clearout OUTPUT_DIR`: Deletes all output files to start fresh

## Output Format

For each input `.txt` file, a corresponding `.csv` file will be generated in the output directory. The CSV files contain the following columns:
*   `CUI`: Concept Unique Identifier
*   `Score`: MetaMap score for the concept
*   `ConceptName`: The concept name found
*   `PrefName`: Preferred Name for the CUI
*   `Phrase`: The text phrase matched by MetaMap
*   `SemTypes`: Semantic Types (colon-separated)
*   `Sources`: UMLS Source Vocabularies (pipe-separated)
*   `Position`: Positional information (e.g., `start:length`) within the utterance/document

## Quick-Start (One-Liner)

If you are new to MetaMap and just want to get going, run the following after installing the package:

```bash
pymm-cli install && pymm-cli
```

The `install` sub-command will download and compile MetaMap 2020 under `metamap_install/` and automatically save the discovered binary path to the local configuration file in your home directory. Once installation finishes, the interactive menu will open where you can configure defaults, start/stop servers, and kick off batch jobs with a few keystrokes.

## Using a custom download mirror

If you keep the MetaMap archive on your own S3/UploadThing bucket (or any HTTP-reachable location), just expose the URL via an environment variable before running the installer:

```bash
export METAMAP_MAIN_ARCHIVE_URL="https://wqqatskmc4.ufs.sh/f/<UploadThingKey>"
pymm-cli install   # will fetch from your mirror instead of GitHub
```

Leave the variable in your shell profile (e.g., `.bashrc`) to make subsequent `pymm-cli install` calls idempotent and offline-friendly.

## 2025 Update – Server-less batch, background mode, live dashboard

* **Server-less by default:** The controller no longer tries to start MetaMap's Tagger/WSD/mmserver20. Your jobs run fine without Java services; starting them manually is optional.
* **Background batch launch:** When you choose "Run MetaMap Batch Processing" in the interactive menu, the job is started in a detached process. You can safely close the terminal; PID is written to `.mimic_pid` and logs stream to `<output_dir>/mimic_controller.log`.
* **Live Monitor Dashboard:** Interactive menu option 3 opens a real-time dashboard that refreshes every two seconds, showing CPU/RAM, progress, and active workers.
* **Better logging:** Each batch automatically creates/attaches a file handler so you can `tail -f` the log while the job is running.
* **Failed files handling:** Problematic files are moved to a special directory for easy retry with increased timeouts.
* **File inspection:** Detailed file analysis and viewing capabilities for both input and output files.

These improvements mean you can kick off a long run, shut your laptop lid, and come back later—all from pure Python.

## Architecture – Python Wrapper vs. Java API

Below is a **side-by-side** view of the two batch pipelines that ship with this repository. Both reach the same goal (map free-text to UMLS CUIs) but they differ in *where* the heavy-lifting happens and *what* is returned to the caller.

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
   *sub-process* per worker which keeps the JVM out of the equation. The Java
   path embeds the native code inside the same JVM. For long-running servers
   the latter has lower per-call overhead, but restarting a wedged MetaMap is
   simpler on the Python side (just kill the process).

2. **Error containment** – When MetaMap crashes it exits with a non-zero code
   which the Python wrapper captures and retries; the Java API surfaces native
   crashes as `IOException`/`UnsatisfiedLinkError` which bubble up the stack.

3. **Resource usage** – The Python model scales well on multi-core machines by
   forking multiple workers (avoids the GIL thanks to `multiprocessing`). The
   Java route enjoys *in-process* JNI calls but must share a single JVM heap.

4. **Portability** – Python orchestration runs anywhere the MetaMap *binary*
   runs (Linux, WSL, macOS). The Java code needs a compatible JDK and may need
   `jna` work-arounds on Alpine, containers, etc.

### Why not JSON?

MetaMap 2020's most structured output format is still **XML** (`--XMLf#`). It
is verbose but:

* Maintains strict ordering and nesting that mirrors the Java object model.
* Is the *only* format that carries full positional information **and** negation
  attributes in one shot.

An experimental *JSON* fielded output exists in *MetaMapLite* but it is missing
several tags (e.g. `PositionalInfo`). Until the upstream project standardises
on JSON we keep XML as the canonical interchange format and convert down-stream
(via `mmoparser.py`) to Python objects. The overhead is negligible compared to
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