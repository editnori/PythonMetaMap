# -*- coding: utf-8 -*-
from pkg_resources import get_distribution, DistributionNotFound

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = get_distribution(dist_name).version
except DistributionNotFound:
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
