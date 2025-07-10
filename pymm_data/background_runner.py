#!/usr/bin/env python3
import sys
import json
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pymm.processing.smart_batch_runner import SmartBatchRunner
from pymm.core.config import PyMMConfig

# Load state
state_file = Path("pymm_data/background_state.json")
state = json.loads(state_file.read_text())
files = [Path(f) for f in state["files"]]

# Create config
config = PyMMConfig()
for key, value in state["config"].items():
    config.set(key, value)

# Run processing
runner = SmartBatchRunner(config)
runner.process_files_with_tracker(files)
