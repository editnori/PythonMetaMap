#!/usr/bin/env python3
"""Wrapper script to handle scipy/numpy conflicts"""
import sys
import os

# Pre-emptively block scipy before any imports
sys.modules['scipy'] = None
sys.modules['scipy.stats'] = None
sys.modules['scipy.special'] = None
sys.modules['seaborn'] = None
sys.modules['matplotlib.pyplot'] = None

# Set environment variables
os.environ['PYMM_NO_SCIPY'] = '1'
os.environ['MPLBACKEND'] = 'Agg'

def main():
    """Run pymm CLI with scipy blocked"""
    try:
        # Import after blocking scipy - use main_simple to avoid scipy imports
        from pymm.cli.main_simple import main as cli_main
        return cli_main()
    except Exception as e:
        error_str = str(e)
        if "numpy.dtype size changed" in error_str or "numpy.ufunc size changed" in error_str:
            print("\nError: NumPy version conflict detected.")
            print("This typically occurs when system and pip packages conflict.")
            print("\nWorkaround options:")
            print("1. Use interactive mode: pymm interactive")
            print("2. Run directly: python -m pymm.cli.main_simple")
            print("3. Update numpy: pip install --upgrade numpy")
            return 1
        else:
            # For other errors, show the actual error
            import traceback
            traceback.print_exc()
            return 1

if __name__ == '__main__':
    sys.exit(main())