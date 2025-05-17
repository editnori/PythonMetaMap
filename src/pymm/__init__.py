# -*- coding: utf-8 -*-
# use the stdlib importlib.metadata on Py ≥3.8; fall back to back-port otherwise
try:
    from importlib.metadata import version as _pkg_version, PackageNotFoundError
except ImportError:  # Python <3.8 — use the back-port
    from importlib_metadata import version as _pkg_version, PackageNotFoundError

try:
    # Change here if project is renamed and does not equal the distribution name
    dist_name = 'pythonmetamap'
    __version__ = _pkg_version(dist_name)
except PackageNotFoundError:
    __version__ = 'unknown'

from .pymm import Metamap, MetamapStuck

__author__ = "Srikanth Mujjiga"
__copyright__ = "Srikanth Mujjiga"
__license__ = "mit"

# Backward-compatibility shim: expose cli.main as mimic_controller.main if external scripts import it
def _legacy_cli_main():
    from .mimic_controller import main as mc_main
    mc_main()

import types as _types
cli = _types.ModuleType('pymm.cli')
cli.main = _legacy_cli_main
import sys as _sys
_sys.modules['pymm.cli'] = cli
