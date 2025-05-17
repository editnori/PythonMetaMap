# Python MetaMap Wrapper

This repository bundles a lightweight Python wrapper around the MetaMap 2020 binaries.  It provides

* a parser that exposes source vocabularies and positional information;
* a simple command line interface `pymm-cli` for batch processing of text files; and
* a helper script `install_metamap.py` which can download and install MetaMap automatically.

## Installing

Clone the repository and install it in editable mode:

```bash
pip install -e .
```

If MetaMap is not yet installed, run:

```bash
python install_metamap.py
```

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
