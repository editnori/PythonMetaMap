#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Srikanth Mujjiga"
__copyright__ = "Srikanth Mujjiga"
__license__ = "mit"

import sys
from setuptools import setup, find_packages

entry_points = {
    'console_scripts': [
        'pymm-cli = pymm.mimic_controller:main',
    ]
}

def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    sphinx = ['sphinx'] if needs_sphinx else []
    
    setup(
        name="pythonmetamap",
        version="0.5.1",
        packages=find_packages(where="src", exclude=["*.tests", "*.tests.*", "tests.*", "tests", "*.__pycache__", "__pycache__", "*.pyc"]),
        package_dir={"": "src"},
        install_requires=[
            "psutil>=5.9.0",
            "colorama>=0.4.4",
            "tqdm>=4.64.0",
            "rich>=12.0.0",
        ],
          entry_points=entry_points,
        include_package_data=True,
    )

if __name__ == "__main__":
    setup_package()
