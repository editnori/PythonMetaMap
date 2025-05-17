# PythonMetaMap

PythonMetaMap provides a lightweight Python interface for running [MetaMap](https://github.com/LHNCBC/MetaMap-src) and parsing its XML output. The project is maintained by **Dr. Layth Qassem** and takes inspiration from the original `pymm` library by Srikanth Mujjiga.

## Installation

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
