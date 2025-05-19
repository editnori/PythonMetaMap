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

# Import new bridge components
try:
    from .java_bridge import MetaMapJavaBridge
except ImportError:
    MetaMapJavaBridge = None

try:
    from .metamap_bridge_controller import MetaMapBridgeController
except ImportError:
    MetaMapBridgeController = None

try:
    from .cache_manager import MetaMapCacheManager
except ImportError:
    MetaMapCacheManager = None

try:
    from .result_transformer import MetaMapResultTransformer
except ImportError:
    MetaMapResultTransformer = None

__all__ = [
    'Metamap',
    'parse',
    'MMO',
    'MMOS',
    'Concept',
    'MetamapCommand',
    'MetaMapJavaBridge',
    'MetaMapBridgeController',
    'MetaMapCacheManager',
    'MetaMapResultTransformer'
]

# Version information
__version__ = '0.5.0'
__author__ = 'Dr.Layth Qassem'
__email__ = ''

# Allow configuring global settings via environment variables
PYMM_DEBUG = os.environ.get('PYMM_DEBUG', '').lower() in ('true', 'yes', '1')
PYMM_USE_JAVA_BRIDGE = os.environ.get('PYMM_USE_JAVA_BRIDGE', 'true').lower() not in ('false', 'no', '0')
PYMM_CACHE_ENABLED = os.environ.get('PYMM_CACHE_ENABLED', '').lower() in ('true', 'yes', '1')
PYMM_CACHE_SIZE = int(os.environ.get('PYMM_CACHE_SIZE', '1000'))

# Easy-to-use factory function for creating optimized MetaMap instances
def create_metamap(metamap_path=None, use_java_bridge=None, use_cache=None, cache_size=None, debug=None):
    """
    Create a MetaMap instance with optimal settings.
    
    This is a factory function that configures all the bridge components
    based on the provided arguments and environment variables.
    
    Args:
        metamap_path (str): Path to MetaMap binary (default: auto-detect)
        use_java_bridge (bool): Whether to use Java bridge (default: from env)
        use_cache (bool): Whether to enable result caching (default: from env)
        cache_size (int): Size of cache (default: from env)
        debug (bool): Enable debug mode (default: from env)
        
    Returns:
        MetaMapBridgeController: Configured MetaMap bridge controller
        
    Raises:
        ValueError: If MetaMap path cannot be determined
    """
    # Resolve parameters, with provided values taking precedence over environment
    if debug is None:
        debug = PYMM_DEBUG
    
    if use_java_bridge is None:
        use_java_bridge = PYMM_USE_JAVA_BRIDGE
    
    if use_cache is None:
        use_cache = PYMM_CACHE_ENABLED
    
    if cache_size is None:
        cache_size = PYMM_CACHE_SIZE
    
    # Find metamap_path if not explicitly provided
    if metamap_path is None:
        # Try environment variable
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
    
    # Create controller
    if MetaMapBridgeController is not None:
        controller = MetaMapBridgeController(
            metamap_path=metamap_path,
            use_java_bridge=use_java_bridge,
            debug=debug
        )
        
        # Create cache if requested
        if use_cache and MetaMapCacheManager is not None:
            cache = MetaMapCacheManager(
                cache_size=cache_size,
                persistent=False,
                debug=debug
            )
            controller.cache_manager = cache
        
        # Create transformer
        if MetaMapResultTransformer is not None:
            controller.transformer = MetaMapResultTransformer(debug=debug)
        
        return controller
    else:
        # Fallback to standard Metamap if bridge controller not available
        return Metamap(metamap_path=metamap_path, debug=debug, use_java_bridge=use_java_bridge)
