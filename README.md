# PythonMetaMap

PythonMetaMap provides a lightweight Python interface for running [MetaMap](https://github.com/LHNCBC/MetaMap-src) and parsing its XML output.  The project builds on the work of the original **pymm** library by Srikanth Mujjiga but is now maintained by **Dr. Layth Qassem**.

## Features

* Extract candidate and mapping concepts from MetaMap XML
* Capture positional information, negation flags and source vocabularies
* Simple API for running MetaMap or integrating into larger pipelines

## Installation

Clone the repository and install in editable mode:

```bash
git clone https://github.com/your-username/PythonMetaMap.git
cd PythonMetaMap
pip install -e .
```

Ensure that MetaMap itself is installed and available on your system.  The helper script `install_metamap.py` can assist with downloading the public MetaMap release.

## Usage

```python
from pymm import Metamap
mm = Metamap('/path/to/metamap/bin/metamap20')
mmos = mm.parse(['heart attack'])
for mmo in mmos:
    for concept in mmo:
        print(concept.cui, concept.matched, concept.pos_start)
```

## Acknowledgement

This library takes inspiration from the `pymm` project created by **Srikanth Mujjiga**.
