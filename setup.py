#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Srikanth Mujjiga"
__copyright__ = "Srikanth Mujjiga"
__license__ = "mit"

import sys
from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_path = Path(__file__).parent / "README.md"
long_description = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""

entry_points = {
    'console_scripts': [
        'pymm = pymm.cli.main:main',
        'pymm-cli = pymm.cli.main:main',  # Backward compatibility
    ]
}

def setup_package():
    needs_sphinx = {'build_sphinx', 'upload_docs'}.intersection(sys.argv)
    sphinx = ['sphinx'] if needs_sphinx else []
    
    setup(
        name="pythonmetamap",
        version="8.2.8",
        author="Dr. Layth Qassem, PharmD, MS",
        author_email="layth.qassem@vanderbilt.edu",
        description="Advanced Python wrapper for NLM MetaMap with Java API integration",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/editnori/PythonMetaMap",
        packages=find_packages(where="src"),
        package_dir={"": "src"},
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Healthcare Industry",
            "Intended Audience :: Science/Research",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Topic :: Scientific/Engineering :: Medical Science Apps.",
            "Topic :: Text Processing :: Linguistic",
        ],
        python_requires=">=3.8",
        install_requires=[
            "click>=8.0.0",
            "rich>=12.0.0",
            "psutil>=5.9.0",
            "tqdm>=4.60.0",
            "colorama>=0.4.4",
            "jpype1>=1.4.0",  # For Java integration
            "pandas>=1.3.0",  # For data analysis
            "matplotlib>=3.4.0",  # For visualizations
            "seaborn>=0.11.0",  # For enhanced visualizations
            "openpyxl>=3.0.0",  # For Excel export
        ],
        entry_points=entry_points,
        include_package_data=True,
        zip_safe=False,
    )

if __name__ == "__main__":
    setup_package()
