#!/bin/bash
#
# PythonMetaMap Large Dataset Processing Script
# Handles 30,000+ files efficiently with chunking and resource management
#

set -e  # Exit on error

# Configuration
INPUT_DIR="${1:-input_notes}"
OUTPUT_DIR="${2:-output_csvs}"
WORKERS="${3:-6}"
CHUNK_SIZE="${4:-500}"
TIMEOUT="${5:-300}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}PythonMetaMap Large Dataset Processor${NC}"
echo "======================================"

# Check if running on Lambda server
if [[ ! -d "$INPUT_DIR" ]]; then
    echo -e "${RED}Error: Input directory '$INPUT_DIR' not found${NC}"
    exit 1
fi

# Count total files
TOTAL_FILES=$(find "$INPUT_DIR" -name "*.txt" | wc -l)
echo -e "Total files to process: ${YELLOW}$TOTAL_FILES${NC}"

# Set file descriptor limit
echo "Setting file descriptor limit..."
ulimit -n 32768
echo "File descriptor limit: $(ulimit -n)"

# Ensure Python path is set
export PATH="$PATH:/home/ubuntu/.local/bin"

# Check if PythonMetaMap is installed
if ! command -v pymm &> /dev/null; then
    echo -e "${RED}Error: PythonMetaMap (pymm) not found in PATH${NC}"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Function to check processing status
check_status() {
    if [[ -f "$OUTPUT_DIR/.processing_state.json" ]]; then
        PROCESSED=$(python3 -c "import json; print(len(json.load(open('$OUTPUT_DIR/.processing_state.json'))['processed']))" 2>/dev/null || echo "0")
        echo -e "Files already processed: ${GREEN}$PROCESSED${NC}"
    else
        echo "No previous processing state found"
    fi
}

# Function to monitor progress
monitor_progress() {
    while true; do
        sleep 30
        if [[ -f "$OUTPUT_DIR/.processing_state.json" ]]; then
            PROCESSED=$(python3 -c "import json; print(len(json.load(open('$OUTPUT_DIR/.processing_state.json'))['processed']))" 2>/dev/null || echo "0")
            PERCENTAGE=$(echo "scale=2; $PROCESSED * 100 / $TOTAL_FILES" | bc)
            echo -e "\r[$(date +'%H:%M:%S')] Progress: ${GREEN}$PROCESSED/$TOTAL_FILES${NC} (${YELLOW}$PERCENTAGE%${NC})"
        fi
    done
}

# Check current status
check_status

# Ask if we should clear state
if [[ "$PROCESSED" -gt 0 ]]; then
    read -p "Continue from previous state? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Clearing previous state..."
        rm -f "$OUTPUT_DIR/.processing_state.json"
        rm -f "$OUTPUT_DIR/.state.lock"
    fi
fi

# Start background monitoring
monitor_progress &
MONITOR_PID=$!

# Trap to kill monitor on exit
trap "kill $MONITOR_PID 2>/dev/null || true" EXIT

# Run chunked processing
echo -e "\nStarting chunked processing..."
echo "Configuration:"
echo "  Workers: $WORKERS"
echo "  Chunk size: $CHUNK_SIZE files"
echo "  Timeout: $TIMEOUT seconds per file"
echo

# Use nohup for background processing
nohup pymm chunked-process \
    "$INPUT_DIR" \
    "$OUTPUT_DIR" \
    --workers "$WORKERS" \
    --chunk-size "$CHUNK_SIZE" \
    --timeout "$TIMEOUT" \
    > "$OUTPUT_DIR/logs/processing_$(date +%Y%m%d_%H%M%S).log" 2>&1 &

PROCESS_PID=$!
echo -e "${GREEN}Processing started with PID: $PROCESS_PID${NC}"

# Save PID for later reference
echo $PROCESS_PID > "$OUTPUT_DIR/.processing.pid"

# Wait for process to complete
echo "Waiting for processing to complete..."
wait $PROCESS_PID
EXIT_CODE=$?

# Kill monitor
kill $MONITOR_PID 2>/dev/null || true

# Check final status
if [[ $EXIT_CODE -eq 0 ]]; then
    echo -e "\n${GREEN}Processing completed successfully!${NC}"
    
    # Final statistics
    FINAL_PROCESSED=$(find "$OUTPUT_DIR" -name "*.csv" | wc -l)
    echo -e "Total files processed: ${GREEN}$FINAL_PROCESSED${NC}"
    
    # Check for failed files
    if [[ -f "$OUTPUT_DIR/.processing_state.json" ]]; then
        FAILED=$(python3 -c "import json; print(len(json.load(open('$OUTPUT_DIR/.processing_state.json'))['failed']))" 2>/dev/null || echo "0")
        if [[ $FAILED -gt 0 ]]; then
            echo -e "${YELLOW}Warning: $FAILED files failed processing${NC}"
            echo "Check logs in $OUTPUT_DIR/logs/ for details"
        fi
    fi
else
    echo -e "\n${RED}Processing failed with exit code: $EXIT_CODE${NC}"
    echo "Check logs in $OUTPUT_DIR/logs/ for details"
fi

echo -e "\n${GREEN}Done!${NC}" 