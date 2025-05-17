#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Standard library imports
from os.path import abspath
import subprocess
import os
import shlex

__author__ = "Srikanth Mujjiga"
__copyright__ = "Srikanth Mujjiga"
__license__ = "mit"

class MetamapCommand:
    def __init__(self, metamap_path, input_file, output_file, debug):
        self.metamap_path = abspath(metamap_path)
        self.input_file = input_file
        self.output_file = output_file
        self.debug = debug
        self.command = self._get_command()
        if self.debug:
            print (self.command)

    def _get_command(self):
        # Default command options
        default_options = ["-c", "-Q", "4", "-K", "--sldi", "-I", "--XMLf1", "--negex", "--word_sense_disambiguation"]
        
        # Check for environment variable
        env_options_str = os.getenv("METAMAP_PROCESSING_OPTIONS")
        
        current_options = []
        if env_options_str:
            try:
                current_options = shlex.split(env_options_str)
                if self.debug:
                    print(f"Using METAMAP_PROCESSING_OPTIONS from environment: {current_options}")
            except ValueError as e:
                print(f"Warning: Invalid METAMAP_PROCESSING_OPTIONS format: {e}")
                print("Falling back to default options")
                current_options = default_options
        else:
            current_options = default_options
            if self.debug:
                print(f"Using default MetaMap options: {current_options}")

        cmd = [self.metamap_path] + current_options
        
        if not self.debug and "--silent" not in cmd: # Only add --silent if not in debug and not already present
            cmd += ["--silent"]
        
        cmd += [self.input_file, self.output_file]
        return cmd

    def execute(self, timeout=10):
        proc = subprocess.Popen(self.command , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            outs, errs = proc.communicate(timeout=timeout)
        finally:
            proc.kill()
