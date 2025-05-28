#!/usr/bin/env python
"""
Basic setup test for PythonMetaMap v8.0.8

This script tests the basic installation and imports without requiring MetaMap.
"""

import sys
import importlib

def test_imports():
    """Test that all main modules can be imported"""
    modules_to_test = [
        'pymm',
        'pymm.core',
        'pymm.core.config',
        'pymm.core.exceptions',
        'pymm.core.state',
        'pymm.server',
        'pymm.server.manager',
        'pymm.processing',
        'pymm.processing.batch_runner',
        'pymm.processing.pool_manager',
        'pymm.processing.retry_manager',
        'pymm.cli',
        'pymm.cli.main',
    ]
    
    print("Testing imports...")
    failed = []
    
    for module_name in modules_to_test:
        try:
            module = importlib.import_module(module_name)
            print(f"✓ {module_name}")
        except ImportError as e:
            print(f"✗ {module_name}: {e}")
            failed.append((module_name, str(e)))
    
    return len(failed) == 0, failed

def test_basic_functionality():
    """Test basic functionality without MetaMap"""
    print("\nTesting basic functionality...")
    
    try:
        from pymm import PyMMConfig
        config = PyMMConfig()
        print("✓ Configuration system working")
        
        # Test setting and getting values
        config.set("test_key", "test_value")
        assert config.get("test_key") == "test_value"
        print("✓ Config set/get working")
        
        # Test version
        from pymm import __version__
        assert __version__ == "8.1.8"
        print(f"✓ Version correct: {__version__}")
        
        return True
    except Exception as e:
        print(f"✗ Basic functionality test failed: {e}")
        return False

def test_cli_entry_point():
    """Test CLI entry point"""
    print("\nTesting CLI entry point...")
    
    try:
        from pymm.cli.main import cli
        print("✓ CLI entry point available")
        return True
    except Exception as e:
        print(f"✗ CLI entry point failed: {e}")
        return False

def main():
    print("PythonMetaMap v8.0.8 Basic Setup Test")
    print("=" * 50)
    
    # Test imports
    imports_ok, failed_imports = test_imports()
    
    # Test basic functionality
    basic_ok = test_basic_functionality()
    
    # Test CLI
    cli_ok = test_cli_entry_point()
    
    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    
    all_ok = imports_ok and basic_ok and cli_ok
    
    if all_ok:
        print("✓ All basic tests passed!")
        print("\nNext steps:")
        print("1. Run: pymm --version")
        print("2. Run: pymm config show")
        print("3. Install MetaMap: pymm install")
        return 0
    else:
        print("✗ Some tests failed!")
        if failed_imports:
            print("\nFailed imports:")
            for module, error in failed_imports:
                print(f"  - {module}: {error}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 