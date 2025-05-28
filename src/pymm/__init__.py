#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
import sys

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    stream=sys.stdout
)

# Import main components
from .pymm import Metamap
from .mmoparser import parse, MMO, MMOS, Concept
from .cmdexecutor import MetamapCommand

# Import refactored components
from .core import (
    PyMMConfig,
    PyMMError,
    ServerConnectionError, 
    ParseError,
    MetamapStuck,
    ConfigurationError,
    StateManager
)

from .server import (
    ServerManager,
    HealthMonitor,
    ServerStatus,
    PortGuard
)

from .processing import (
    BatchRunner,
    RetryManager,
    MetaMapInstancePool,
    AdaptivePoolManager,
    FileProcessor
)

# Import CLI
from .cli import cli

__all__ = [
    # Original components
    'Metamap',
    'parse',
    'MMO',
    'MMOS',
    'Concept',
    'MetamapCommand',
    
    # Core components
    'PyMMConfig',
    'PyMMError',
    'ServerConnectionError',
    'ParseError', 
    'MetamapStuck',
    'ConfigurationError',
    'StateManager',
    
    # Server components
    'ServerManager',
    'HealthMonitor',
    'ServerStatus',
    'PortGuard',
    
    # Processing components
    'BatchRunner',
    'RetryManager',
    'MetaMapInstancePool',
    'AdaptivePoolManager',
    'FileProcessor',
    
    # CLI
    'cli'
]

# Version information
__version__ = '8.1.8'
__author__ = 'Dr.Layth Qassem'
__email__ = ''

# Allow configuring global settings via environment variables
PYMM_DEBUG = os.environ.get('PYMM_DEBUG', '').lower() in ('true', 'yes', '1')

# Easy-to-use factory function for creating optimized MetaMap instances
def create_metamap(metamap_path=None, debug=None):
    """
    Create a MetaMap instance with optimal settings.
    
    Args:
        metamap_path (str): Path to MetaMap binary (default: auto-detect)
        debug (bool): Enable debug mode (default: from env)
        
    Returns:
        Metamap: Configured MetaMap instance
        
    Raises:
        ValueError: If MetaMap path cannot be determined
    """
    # Resolve parameters
    if debug is None:
        debug = PYMM_DEBUG
    
    # Find metamap_path if not explicitly provided
    if metamap_path is None:
        # Try configuration
        config = PyMMConfig()
        metamap_path = config.get('metamap_binary_path')
        
        # Try environment variable
        if not metamap_path:
            metamap_path = os.environ.get('METAMAP_BINARY_PATH')
        
        # Try common locations if still not found
        if not metamap_path:
            potential_paths = [
                # Standard installation paths
                "/opt/public_mm/bin/metamap",
                "/usr/local/public_mm/bin/metamap",
                # Windows paths
                "C:\\public_mm\\bin\\metamap.bat",
                # Relative paths for development
                "./metamap_install/public_mm/bin/metamap",
                "../metamap_install/public_mm/bin/metamap",
            ]
            
            for path in potential_paths:
                if os.path.exists(path):
                    metamap_path = path
                    break
    
    if not metamap_path:
        raise ValueError("Cannot find MetaMap binary. Please provide metamap_path or set METAMAP_BINARY_PATH environment variable.")
    
    return Metamap(metamap_path=metamap_path, debug=debug)