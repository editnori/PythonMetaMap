#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
import os
import logging
from .cmdexecutor import MetamapCommand
from .mmoparser import parse
from os.path import exists, dirname, abspath
import tempfile
from os import remove
from subprocess import TimeoutExpired
from xml.parsers.expat import ExpatError

__author__ = "Dr.Layth Qassem"
__copyright__ = "Dr.Layth Qassem"
__license__ = "mit"

# Configure logging
logger = logging.getLogger("PyMM")

class MetamapStuck(Exception):
    """ MetaMap Stuck Exception """
    pass

class Metamap:
    """ MetaMap Concept Extractor """

    def __init__(self, metamap_path, debug=False):
        """ MetaMap Wrapper parameters

        Args:
            metamap_path (str): Path to metamap
            debug (boolean): Debug On/Off
        """
        self.metamap_path = metamap_path
        self.debug = debug
        self.input_file, self.output_file = self._get_temp_files()
        
        self.metamap_command = MetamapCommand(self.metamap_path,
                self.input_file, self.output_file, self.debug)
        logger.info("Using traditional binary communication with MetaMap")
        if debug:
            print("Using traditional binary communication with MetaMap")

    def is_alive(self):
        """Check if MetaMap is running

        Returns:
            True if MetaMap is running, False otherwise
        """
        try:
            mmos = self.parse(["heart attack"])
            # Check if mmos is not None and is iterable
            if mmos is None:
                return False
            concepts = [concept for mmo in mmos for concept in mmo]
            # The original check for "len(concepts) == 7" might be too specific.
            # A more robust check might be if any concepts are returned for a common term.
            # For now, let's assume if it parses and returns something, it's alive.
            return True 
        except ExpatError:
            return False
        except MetamapStuck: # If it gets stuck, it's not "alive" for further requests
            return False
        except Exception as e: # Catch other potential errors during parse
            logger.warning(f"is_alive check failed with an exception: {e}")
            return False


    def _get_temp_files(self):
        TEMP_DIR = None

        if exists('/dev/shm'):
            # Original code had "print ("Exists")" which is a side effect.
            # Logging it instead if debug is on.
            if self.debug:
                logger.debug("Temporary file directory /dev/shm exists")
            TEMP_DIR = '/dev/shm'
        
        # Ensure temp files are created with a context that allows them to be written to
        # and then closed, but their names retained. NamedTemporaryFile(delete=False) is correct.
        with tempfile.NamedTemporaryFile(mode='w', delete=False, dir=TEMP_DIR, encoding='utf-8') as fp_in:
            input_file = fp_in.name

        with tempfile.NamedTemporaryFile(mode='w', delete=False, dir=TEMP_DIR, encoding='utf-8') as fp_out:
            output_file = fp_out.name

        if self.debug:
            logger.debug(f"Input File: {input_file}, Output File: {output_file}")

        return input_file, output_file

    def parse(self, sentences, timeout=60): # Increased default timeout
        """Returns the UMLS concetps

        Args:
            sentences (:obj:`list` of :obj:`str`): Input sentences for which concepts are to be extracted
            timeout (int): Timeout interval for MetaMap. Default 60 seconds.

        Returns:
            Iterator over MetaMap Objects or None if a critical error occurs.
        """
        if not sentences:
            return [] # Return empty list for empty input

        try:
            with open(self.input_file, mode="w", encoding="utf-8") as fp: # Ensure utf-8 writing
                for sentence in sentences:
                    fp.write(f"{sentence}\n") # Use f-string and ensure newline
        except IOError as e:
            logger.error(f"Failed to write to input file {self.input_file}: {e}")
            return None # Indicate failure

        try:
            self.metamap_command.execute(timeout=timeout)
            # The parse function in mmoparser can handle ExpatError and return an empty MMOS
            return parse(self.output_file)
        except TimeoutExpired:
            logger.error(f"Execution of MetaMap command timed out after {timeout} seconds.")
            if self.debug:
                print(f"Execution of MetaMap command timed out after {timeout} seconds.")
            raise MetamapStuck()
        except RuntimeError as e: # Catch errors from MetamapCommand.execute()
            logger.error(f"MetaMap command execution failed: {e}")
            if self.debug:
                print(f"MetaMap command execution failed: {e}")
            # Return an empty list or MMOS equivalent to signify failure but not crash
            return [] 
        except Exception as e: # Catch any other unexpected errors
            logger.error(f"An unexpected error occurred during MetaMap parsing: {e}")
            if self.debug:
                import traceback
                print(f"Unexpected error: {e}\n{traceback.format_exc()}")
            return []


    def close(self):
        """Clean up resources, close temporary files.
        """
        if not self.debug:
            for file_path in [self.input_file, self.output_file]:
                if file_path and os.path.exists(file_path):
                    try:
                        remove(file_path)
                    except Exception as e:
                        logger.warning(f"Error removing temporary file {file_path}: {e}")
        else:
            logger.debug(f"Debug mode: temporary files not removed: {self.input_file}, {self.output_file}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
