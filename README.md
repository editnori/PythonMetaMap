# PythonMetaMap


=======
# Python MetaMap Wrapper
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
=======
Clone the repository and install it in editable mode:

```bash
pip install -e .
```

If MetaMap is not yet installed, run:

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
=======
This downloads the MetaMap archives from the official NLM release and runs the
`install.sh` script.  The binaries are placed under `metamap_install/public_mm`.

## Usage

To process a directory of `.txt` files and generate `.csv` outputs:

```bash
pymm-cli <input_dir> <output_dir>
```

`pymm-cli` automatically locates the MetaMap binary under
`metamap_install/public_mm/bin/metamap20`.  Use `--metamap-path` to specify a
custom location.

The command keeps a checkpoint file in the output directory so interrupted runs
can be resumed.

## Testing

Unit tests require `pytest` and a working MetaMap installation.  Set the
`TEST_METAMAP_PATH` environment variable to the MetaMap binary before running
`python -m pytest`.
