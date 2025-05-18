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
    """Thin wrapper around the MetaMap binary invocation.

    Parameters
    ----------
    metamap_path : str
        Absolute (or relative) path to the *metamap* executable.  It is
        resolved to an absolute path during construction so that workers
        launched from other CWDs do not lose sight of the binary.
    input_file : str
        Path to the temporary *input* text file containing the sentences to
        be mapped.
    output_file : str
        Path to the temporary *output* XML file that MetaMap writes.
    debug : bool
        When *True* the full command is printed and **--silent** is **not**
        appended.  When *False* MetaMap runs in silent-mode to cut down on
        noisy STDERR lines that slow down Pythonʼs subprocess pipe.
    """

    def __init__(self, metamap_path, input_file, output_file, debug=False):
        self.metamap_path = abspath(metamap_path)
        self.input_file = input_file
        self.output_file = output_file
        self.debug = bool(debug)
        # Build CLI once and reuse between calls – avoids repeated shlex work
        self.command = self._get_command()
        if self.debug:
            print("[pymm] MetaMap command:", " ".join(shlex.quote(p) for p in self.command))

    def _get_command(self):
        """Return a list of command-line tokens for *subprocess*.

        The logic honours an environment variable *METAMAP_PROCESSING_OPTIONS* –
        this makes the behaviour configurable from the outside without having
        to touch any Python code.  When the variable is missing, a curated set
        of options is used that closely mirrors the defaults of the Java API.
        """

        # Default command options – mirrors Java BatchRunner01 settings
        default_options = [
            "-c",                       # restrict concept candidates (no over-matching)
            "-Q", "4",                # term processing: conserve memory and run faster
            "-K",                      # ignore stop words
            "--sldi",                  # strict limit derivational variants
            "-I",                      # show candidate identifiers
            "--XMLf1",                 # compact XML format (faster to parse)
            "--negex",                 # attach negation features
            "--word_sense_disambiguation",  # WSD on – improves precision
        ]
        
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

        # Guarantee that an XML output format is requested – the downstream
        # parser requires valid XML. Users may supply their own option list
        # via *METAMAP_PROCESSING_OPTIONS*; append a safe default if none of
        # the --XML* flags are present.
        if not any(opt.startswith("--XML") for opt in current_options):
            current_options.append("--XMLf1")

        cmd = [self.metamap_path] + current_options
        
        # Silent-mode drastically reduces the volume of diagnostic output. We
        # only keep MetaMap chatty when *debug* is requested.
        if not self.debug and "--silent" not in cmd:
            cmd.append("--silent")
        
        cmd += [self.input_file, self.output_file]
        return cmd

    def execute(self, timeout: int = 60):
        """Run MetaMap synchronously.

        Parameters
        ----------
        timeout : int, optional
            Maximum run-time in **seconds**.  The timer includes MetaMapʼs own
            start-up overhead which can be a couple of seconds on the first
            ever call because of JVM warm-up and lexicon caching.

        Returns
        -------
        tuple[str, str]
            *(stdout, stderr)* decoded as UTF-8.  MetaMap normally writes its
            real results to *self.output_file* so these streams are only used
            for diagnostics.

        Raises
        ------
        subprocess.TimeoutExpired
            When the process exceeds *timeout* seconds.
        RuntimeError
            When MetaMap returns a non-zero exit status indicating an error.
        """

        proc = subprocess.Popen(
            self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )

        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        finally:
            # communicate() waits for process completion – kill only if still alive
            if proc.poll() is None:
                proc.kill()

        if proc.returncode != 0:
            raise RuntimeError(
                f"MetaMap exited with status {proc.returncode}. STDERR snippet:\n{stderr[:500]}"
            )

        if self.debug and stderr:
            print("[pymm][MetaMap stderr]", stderr.strip())

        return stdout, stderr
