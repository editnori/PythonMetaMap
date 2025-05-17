#!/usr/bin/env bash
# MetaMap CLI Utility
# Provides interactive and flag-based control for MetaMap 2020 servers
# Author: Auto-generated
#
# This script must live in the same directory as install_metamap.sh so it
# can locate the MetaMap installation at ./metamap_install/public_mm
#
#--------------------------------------------------------------------------
set -euo pipefail

# Establish base directory early to avoid unset-variable issues
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

##############################################################################
# Global paths / constants (read-only)
##############################################################################

readonly PIDS_FILE="/tmp/metamap_cli_term_pids"
readonly MIMIC_CTL="$BASE_DIR/mimic_controller.py"
readonly KIDNEY_IN="$BASE_DIR/metamap_demo/demoKidney2025/in/notes"
readonly KIDNEY_OUT="$BASE_DIR/metamap_demo/demoKidney2025/out"

# Detect sudo early (warn if password prompt will block)
if [[ "${MM_USE_RAMDB:-false}" == "true" && $EUID -ne 0 ]]; then
  if ! sudo -n true 2>/dev/null; then
    echo -e "${YELLOW}Warning:${RESET} RAM-disk mode will require sudo password to mount tmpfs." >&2
  fi
fi

##############################################################################
# Colors
##############################################################################
if [[ -t 1 ]]; then
  RED="$(printf '\\033[0;31m')"
  GREEN="$(printf '\\033[0;32m')"
  YELLOW="$(printf '\\033[0;33m')"
  RESET="$(printf '\\033[0m')"
else
  RED=""
  GREEN=""
  YELLOW=""
  RESET=""
fi

##############################################################################
# ASCII Banner
##############################################################################
print_banner() {
  cat <<'EOF'
  __  __  _____  _____   _    __  __   _    ____    ____  _      ___ 
 |  \\/  || ____||_   _| / \\  |  \\/  | / \\  |  _ \\  / ___|| |    |_ _|
 | |\\/| ||  _|    | |  / _ \\ | |\\/| |/ _ \\ | |_) || |    | |     | | 
 | |  | || |___   | | / ___ \\| |  | / ___ \\|  __/ | |___ | |___  | | 
 |_|  |_||_____| |___|/_/  \\_\\_|  |/_/   \\_\\_|     \\____||_____||___|

EOF
  echo -e "${YELLOW}MetaMap CLI${RESET}"
}

##############################################################################
# Path Resolution
##############################################################################
resolve_directories() {
  PUBLIC_MM_DIR="$BASE_DIR/metamap_install/public_mm"

  if [[ ! -x "$PUBLIC_MM_DIR/bin/skrmedpostctl" ]]; then
    echo -e "${RED}Error:${RESET} Cannot find MetaMap installation at: $PUBLIC_MM_DIR" >&2
    exit 1
  fi
  
  if [[ -z "${KIDNEY_IN:-}" ]] || [[ -z "${KIDNEY_OUT:-}" ]]; then
    echo -e "${RED}Error:${RESET} KIDNEY_IN or KIDNEY_OUT paths are not set. This should not happen." >&2
    echo -e "${RED}Please define KIDNEY_IN and KIDNEY_OUT near the top of the script.${RESET}" >&2
    exit 1
  else
    if [[ ! -d "$KIDNEY_IN" ]]; then
      echo -e "${YELLOW}Warning:${RESET} Kidney input directory $KIDNEY_IN does not exist. Creating it." >&2
      mkdir -p "$KIDNEY_IN"
    fi
    if [[ ! -d "$KIDNEY_OUT" ]]; then
      echo -e "${YELLOW}Warning:${RESET} Kidney output directory $KIDNEY_OUT does not exist. Creating it." >&2
      mkdir -p "$KIDNEY_OUT"
    fi
  fi
}

##############################################################################
# Helper: environment detection
##############################################################################
is_wsl() {
  grep -qiE "microsoft|wsl" /proc/sys/kernel/osrelease 2>/dev/null
}

##############################################################################
# Helper: wait until mmserver20 (port 8066) responds.
# Requires Bash /dev/tcp support (present in GNU bash).
##############################################################################

wait_for_mmserver() {
  local max_wait=60
  echo -n "Waiting for mmserver20 (port 8066) to become available..."
  for ((i=0;i<max_wait;i++)); do
    if (echo > /dev/tcp/127.0.0.1/8066) >/dev/null 2>&1; then
      echo -e " ${GREEN}up${RESET}"
      return 0
    fi
    sleep 1
    echo -n "."
  done
  echo -e " ${RED}timeout${RESET}"
  return 1
}

##############################################################################
# Optional: run MetaMap DB from a RAM-disk (tmpfs)
#   Enable with   MM_USE_RAMDB=true   (default false)
#   Size can be overridden with       MM_RAMDB_SIZE=10   (GB)
##############################################################################

USE_TMPFS_DB="${MM_USE_RAMDB:-false}"
TMPFS_SIZE_GB="${MM_RAMDB_SIZE:-10}"

setup_tmpfs_db() {
  if [[ "$USE_TMPFS_DB" != "true" ]]; then return; fi

  RAM_DIR="/dev/shm/metamap_db_${TMPFS_SIZE_GB}g"
  if ! mountpoint -q "$RAM_DIR"; then
    echo -e "${YELLOW}Creating tmpfs (${TMPFS_SIZE_GB} GB) at $RAM_DIR for MetaMap DB…${RESET}"
    sudo mkdir -p "$RAM_DIR"
    sudo mount -t tmpfs -o size=${TMPFS_SIZE_GB}G tmpfs "$RAM_DIR"
  fi

  echo -e "${YELLOW}Synchronising MetaMap DB into RAM (first run only)…${RESET}"
  rsync -a --delete "$PUBLIC_MM_DIR/DB/" "$RAM_DIR/" >/dev/null 2>&1

  export METAMAP_DB_HOME="$RAM_DIR"
  echo -e "${GREEN}DB is now served from RAM: $METAMAP_DB_HOME${RESET}"
}

##############################################################################
# Start Servers Automatically
##############################################################################
start_servers_auto() {
  echo -e "${YELLOW}Attempting automatic startup...${RESET}"

  # Reset PID tracking file
  : > "$PIDS_FILE"
  # Define a log file for mmserver20 output
  MMSERVER_LOG_FILE="$PUBLIC_MM_DIR/mmserver20.log"

  if is_wsl && command -v wt.exe >/dev/null 2>&1; then
    echo -e "${GREEN}WSL environment detected. Launching Windows Terminal panes...${RESET}"
    # This WSL command already handles starting mmserver20 in its own pane.
    # We might still want to run wait_for_mmserver after this for confirmation.
    wt.exe -w 0 new-tab --title "MetaMap Skr/WSD" wsl -d "${WSL_DISTRO_NAME:-}" --cd "$PUBLIC_MM_DIR" -- bash -lc "./bin/skrmedpostctl start && ./bin/wsdserverctl start && exec bash" \; split-pane --title "MetaMap mmserver20" wsl -d "${WSL_DISTRO_NAME:-}" --cd "$PUBLIC_MM_DIR" -- bash -lc "./bin/mmserver20 && exec bash"
    echo -e "${YELLOW}Please wait for services to initialize in the new terminal windows...${RESET}"
    wait_for_mmserver || echo -e "${RED}mmserver20 did not become available after WSL launch. Please check the dedicated terminal pane.${RESET}"
    return
  fi

  # ---------------------------------------------------------------------
  # Launch Tagger (skrmedpostctl) and WSD server pairs, one per mmserver
  # ---------------------------------------------------------------------

  NUM_MM_SERVERS="${NUM_MM_SERVERS:-1}"   # how many mmserver20 processes to spawn (may be overridden below but we need it here)
  TAGGER_FIRST_PORT="${TAGGER_FIRST_PORT:-9050}"
  WSD_FIRST_PORT="${WSD_FIRST_PORT:-5557}"

  echo -e "Starting ${NUM_MM_SERVERS} Tagger+WSD instances..."

  for ((i=0;i<NUM_MM_SERVERS;i++)); do
     TAG_PORT=$((TAGGER_FIRST_PORT+i))
     WSD_PORT=$((WSD_FIRST_PORT+i))

     TAG_LOG="$PUBLIC_MM_DIR/skrmedpostctl_${TAG_PORT}.log"
     WSD_LOG="$PUBLIC_MM_DIR/wsdserverctl_${WSD_PORT}.log"

     echo -n "  • Tagger on $TAG_PORT... "
     (cd "$PUBLIC_MM_DIR" && nohup ./bin/skrmedpostctl start "$TAG_PORT" > "$TAG_LOG" 2>&1 &)
     sleep 0.5 # Give it a moment to spawn
     echo -e "${YELLOW}issued (check $TAG_LOG)${RESET}"

     echo -n "    WSD on $WSD_PORT... "
     (cd "$PUBLIC_MM_DIR" && nohup ./bin/wsdserverctl start "$WSD_PORT" > "$WSD_LOG" 2>&1 &)
     sleep 0.5 # Give it a moment to spawn
     echo -e "${YELLOW}issued (check $WSD_LOG)${RESET}"
  done

  # -----------------------------------------------------------------------
  # If requested, move the Berkeley DB to RAM before we launch mmserver20
  # -----------------------------------------------------------------------
  setup_tmpfs_db

  # Determine desired number of parallel mmserver instances
  NUM_MM_SERVERS="${NUM_MM_SERVERS:-1}"   # may have been set earlier but re-evaluate after env overrides
  FIRST_PORT="${MM_FIRST_PORT:-8066}"
  TAGGER_FIRST_PORT="${TAGGER_FIRST_PORT:-9050}"
  WSD_FIRST_PORT="${WSD_FIRST_PORT:-5557}"

  # Inform downstream processes (e.g., Python controller) which ports are active
  PORT_LIST=""

  for ((i=0;i<NUM_MM_SERVERS;i++)); do
     PORT=$((FIRST_PORT+i))
     PORT_LIST+="${PORT},"
     LOG_PATH="$PUBLIC_MM_DIR/mmserver20_${PORT}.log"
     TAG_PORT=$((TAGGER_FIRST_PORT+i))
     WSD_PORT=$((WSD_FIRST_PORT+i))
     echo -n "Starting mmserver20 on port $PORT (to use Tagger $TAG_PORT, WSD $WSD_PORT)... "
     touch "$LOG_PATH"
     # Launch with port override in env
     (MMSERVER_PORT="$PORT" TAGGER_SERVER_PORT="$TAG_PORT" WSD_SERVER_PORT="$WSD_PORT" \
       nohup "$PUBLIC_MM_DIR/bin/mmserver20" > "$LOG_PATH" 2>&1 &)
     echo -e "${GREEN}done${RESET}"
  done

  # Trim trailing comma and export for child processes
  PORT_LIST=${PORT_LIST%,}
  export MM_SERVER_PORTS="$PORT_LIST"
  # Also keep MAX_PARALLEL_WORKERS in sync with CPU cores unless user overrides
  THREADS="${MM_THREADS:-$(nproc --all 2>/dev/null || echo 4)}"
  export MAX_PARALLEL_WORKERS="$THREADS"
  # Store its PID if we can reliably get it (nohup makes this tricky directly)
  # We'll rely on pgrep and wait_for_mmserver

  # Wait for mmserver20 to become available
  if wait_for_mmserver; then
    echo -e "${GREEN}mmserver20 is up and responsive.${RESET}"
  else
    echo -e "${RED}mmserver20 failed to start or become responsive. Check $MMSERVER_LOG_FILE.${RESET}"
    echo -e "${YELLOW}You may need to start it manually in a separate terminal: cd '$PUBLIC_MM_DIR' && ./bin/mmserver20${RESET}"
    # Consider exiting here or providing a clearer failure state if mmserver is critical for next steps
  fi

  echo -e "${YELLOW}Automatic startup sequence initiated. Use 'check_status' to verify all components.${RESET}"
}

##############################################################################
# Start Servers Manually (print commands)
##############################################################################
start_servers_manual() {
  local pmmdir="$PUBLIC_MM_DIR" # Capture once for literal replacement below
  cat <<'EOF'
Copy & paste the following into TWO separate terminals (or tabs):

# ---------------------------------------------------------------------------
# Terminal 1  –  Launch TAGGER + WSD pairs (one per mmserver)
# ---------------------------------------------------------------------------
NUM=20   # adjust
TAG_BASE=9050   # first Tagger port
WSD_BASE=5557   # first WSD port
cd "$pmmdir" && for i in $(seq 0 $((NUM-1))); do \
   TAG=$((TAG_BASE+i)); WSD=$((WSD_BASE+i)); \
   (TAGGER_SERVER_PORT=$TAG nohup ./bin/skrmedpostctl start $TAG > skrmedpostctl_${TAG}.log 2>&1 &) ; \
   (WSD_SERVER_PORT=$WSD nohup ./bin/wsdserverctl start $WSD > wsdserverctl_${WSD}.log 2>&1 &) ; \
 done

# ---------------------------------------------------------------------------
# Terminal 2  –  MetaMap server(s)
# ---------------------------------------------------------------------------
# Option A: single server on default port 8066
cd "$pmmdir" && ./bin/mmserver20

# Option B: 20 parallel servers on ports 8066-8085
cd "$pmmdir" && for i in $(seq 0 19); do \
   PORT=$((8066+i)); TAG=$((9050+i)); WSD=$((5557+i)); \
   (MMSERVER_PORT=$PORT TAGGER_SERVER_PORT=$TAG WSD_SERVER_PORT=$WSD nohup ./bin/mmserver20 > mmserver20_${PORT}.log 2>&1 &) ; \
 done

# After they are up, execute these in the shell where you will run the batch job:
export MM_SERVER_PORTS=$(seq -s, 8066 1 8085)
export MAX_PARALLEL_WORKERS=20
# (Adjust the numbers if you change the loop above.)
EOF
}

##############################################################################
# Check Status
##############################################################################
check_status() {
  cd "$PUBLIC_MM_DIR"
  echo -e "${YELLOW}skrmedpostctl status:${RESET}"
  ./bin/skrmedpostctl status || true
  echo
  echo -e "${YELLOW}wsdserverctl status:${RESET}"
  ./bin/wsdserverctl status || true
  echo
  if pgrep -f mmserver20 >/dev/null 2>&1; then
    echo -e "${GREEN}mmserver20 is running (PID(s): $(pgrep -fl mmserver20 | awk '{print $1}' | paste -sd,))${RESET}"
  else
    echo -e "${RED}mmserver20 is NOT running${RESET}"
  fi
}

##############################################################################
# Stop Servers
##############################################################################
stop_servers() {
  cd "$PUBLIC_MM_DIR"
  echo -e "${YELLOW}Stopping skrmedpostctl...${RESET}"
  ./bin/skrmedpostctl stop || true
  echo -e "${YELLOW}Stopping wsdserverctl...${RESET}"
  ./bin/wsdserverctl stop || true
  echo -e "${YELLOW}Stopping mmserver20 (pkill)...${RESET}"
  pkill -f mmserver20 || true

  # Close any terminal windows we opened previously - This logic is now moot as we don't open them.
  # if [[ -f "$PIDS_FILE" ]]; then
  #   while read -r pid; do
  #     kill "$pid" 2>/dev/null || true
  #   done < "$PIDS_FILE"
  #   rm -f "$PIDS_FILE"
  #   echo -e "${GREEN}Closed spawned terminal windows.${RESET}"
  # fi
  echo -e "${GREEN}Stop commands issued.${RESET}"
}

##############################################################################
# Detect Java Path
##############################################################################
detect_java() {
  JAVA_BIN=$(command -v java || true)
  if [[ -n "$JAVA_BIN" ]]; then
    JAVA_PATH="$(readlink -f "$JAVA_BIN")"
    echo -e "${GREEN}Java detected at: $JAVA_PATH${RESET}"
    java -version 2>&1 | head -n 1
  else
    echo -e "${RED}Java executable not found in PATH${RESET}"
  fi
}

##############################################################################
# Environment Summary
##############################################################################
env_summary() {
  print_banner
  echo "Base directory     : $BASE_DIR"
  echo "MetaMap public_mm  : $PUBLIC_MM_DIR"
  echo "Java executable    : $(command -v java || echo 'N/A')"
  echo "Python executable  : $(command -v python || echo 'N/A')"
  if command -v python >/dev/null 2>&1; then
    python --version 2>&1 | head -n 1
  fi
  echo "skrmedpostctl PID  : $(pgrep -f skrmedpost || echo 'N/A')"
  echo "wsdserverctl  PID  : $(pgrep -f wsdserverctl || echo 'N/A')"
  echo "mmserver20    PID  : $(pgrep -f mmserver20 || echo 'N/A')"
  if [[ -d "$PUBLIC_MM_DIR/DB" ]]; then
    echo "DB disk usage      : $(du -sh "$PUBLIC_MM_DIR/DB" | cut -f1)"
  fi
}

##############################################################################
# Run Java SimpleTest Demo
##############################################################################
run_simple_test() {
  DEMO_DIR="$BASE_DIR/metamap_demo"
  SIMPLE_TEST_SRC="demoMetaMap2020/src/demo/metamaprunner2020/SimpleTest.java"
  BIN_DIR="$DEMO_DIR/bin"

  # Pre-flight check for MetaMap servers
  if ! wait_for_mmserver; then
    echo -e "${RED}mmserver20 not reachable on port 8066. Cannot run SimpleTest demo.${RESET}"
    echo -e "${YELLOW}Attempt to start servers using './metamap_cli.sh start' or check their status.${RESET}"
    return 1
  fi

  if [[ ! -f "$DEMO_DIR/$SIMPLE_TEST_SRC" ]]; then
    echo -e "${RED}SimpleTest source not found at $DEMO_DIR/$SIMPLE_TEST_SRC${RESET}"
    echo "Ensure you extracted the demo package during installation."
    return 1
  fi

  mkdir -p "$BIN_DIR"

  CLASSPATH="$PUBLIC_MM_DIR/src/javaapi/target/metamap-api-2.0.jar:$PUBLIC_MM_DIR/src/javaapi/dist/prologbeans.jar:$BIN_DIR:."

  # Compile if class file missing or outdated
  if [[ ! -f "$BIN_DIR/demo/metamaprunner2020/SimpleTest.class" || "$DEMO_DIR/$SIMPLE_TEST_SRC" -nt "$BIN_DIR/demo/metamaprunner2020/SimpleTest.class" ]]; then
    echo -e "${YELLOW}Compiling SimpleTest.java...${RESET}"
    javac -cp "$CLASSPATH" -d "$BIN_DIR" "$DEMO_DIR/$SIMPLE_TEST_SRC" || {
      echo -e "${RED}Compilation failed.${RESET}"; return 1; }
  fi

  echo -e "${GREEN}Running SimpleTest...${RESET}"
  java -cp "$CLASSPATH" demo.metamaprunner2020.SimpleTest
}

##############################################################################
# Run Java BatchRunner01 demo (process MIMIC notes)
##############################################################################
run_batch_test() {
  DEMO_DIR="$BASE_DIR/metamap_demo"
  BATCH_RUNNER_SRC="demoMetaMap2020/src/demo/metamaprunner2020/BatchRunner01.java"
  BIN_DIR="$DEMO_DIR/bin"
  INPUT_DIR="$DEMO_DIR/demoMetaMap2020/in/mimic"
  OUTPUT_DIR="$DEMO_DIR/demoMetaMap2020/out"

  # Pre-flight check for MetaMap servers
  if ! wait_for_mmserver; then
    echo -e "${RED}mmserver20 not reachable on port 8066. Cannot run BatchRunner01 demo.${RESET}"
    echo -e "${YELLOW}Attempt to start servers using './metamap_cli.sh start' or check their status.${RESET}"
    return 1
  fi

  if [[ ! -f "$DEMO_DIR/$BATCH_RUNNER_SRC" ]]; then
    echo -e "${RED}BatchRunner01 source not found at $DEMO_DIR/$BATCH_RUNNER_SRC${RESET}"
    return 1
  fi

  if [[ ! -d "$INPUT_DIR" ]]; then
    echo -e "${RED}Input directory $INPUT_DIR not found. Provide MIMIC notes before running this test.${RESET}"
    return 1
  fi

  mkdir -p "$BIN_DIR" "$OUTPUT_DIR"

  CLASSPATH="$PUBLIC_MM_DIR/src/javaapi/target/metamap-api-2.0.jar:$PUBLIC_MM_DIR/src/javaapi/dist/prologbeans.jar:$BIN_DIR:."

  # Compile if necessary
  if [[ ! -f "$BIN_DIR/demo/metamaprunner2020/BatchRunner01.class" || "$DEMO_DIR/$BATCH_RUNNER_SRC" -nt "$BIN_DIR/demo/metamaprunner2020/BatchRunner01.class" ]]; then
    echo -e "${YELLOW}Compiling BatchRunner01.java...${RESET}"
    javac -cp "$CLASSPATH" -d "$BIN_DIR" "$DEMO_DIR/$BATCH_RUNNER_SRC" || {
      echo -e "${RED}Compilation failed.${RESET}"; return 1; }
  fi

  echo -e "${GREEN}Running BatchRunner01...${RESET}"
  java -cp "$CLASSPATH" demo.metamaprunner2020.BatchRunner01 "$INPUT_DIR" "$OUTPUT_DIR"

  echo -e "${GREEN}Batch run complete. Check output CSVs in $OUTPUT_DIR${RESET}"
}

##############################################################################
# Run MIMIC Batch with Validation (Python controller)
##############################################################################
run_mimic_validated() {
  mode="${1:-start}"
  DEMO_DIR="$BASE_DIR/metamap_demo"
  INPUT_DIR="$DEMO_DIR/demoMetaMap2020/in/mimic"
  OUTPUT_DIR="$DEMO_DIR/demoMetaMap2020/out"
  MIMIC_LOG_FILE="$OUTPUT_DIR/mimic_controller.log"

  # Pre-flight check for MetaMap servers
  if ! wait_for_mmserver; then
    echo -e "${RED}mmserver20 not reachable on port 8066. Cannot run MIMIC batch.${RESET}"
    echo -e "${YELLOW}Attempt to start servers using './metamap_cli.sh start' or check their status.${RESET}"
    return 1
  fi
  # Ideally, also check skrmedpostctl and wsdserverctl status here if possible
  # For now, wait_for_mmserver is the primary gatekeeper.

  if [[ ! -f "$MIMIC_CTL" ]]; then
    echo -e "${RED}mimic_controller.py not found at $MIMIC_CTL${RESET}"
    return 1
  fi

  if [[ ! -d "$INPUT_DIR" ]]; then
    echo -e "${RED}Input directory $INPUT_DIR not found. Cannot proceed.${RESET}"
    return 1
  fi

  mkdir -p "$OUTPUT_DIR"

  CLASSPATH="$PUBLIC_MM_DIR/src/javaapi/target/metamap-api-2.0.jar:$PUBLIC_MM_DIR/src/javaapi/dist/prologbeans.jar:$DEMO_DIR/bin:."

  # ----------------------------------------------------------
  # If user launched mmserver20 instances manually and defined
  # MM_SERVER_PORTS but forgot MAX_PARALLEL_WORKERS, infer it
  # from the number of ports so every server gets at least one
  # Java worker.
  # ----------------------------------------------------------
  if [[ -z "${MAX_PARALLEL_WORKERS:-}" && -n "${MM_SERVER_PORTS:-}" ]]; then
    export MAX_PARALLEL_WORKERS="$(echo "$MM_SERVER_PORTS" | tr ',' '\n' | wc -l | tr -d ' ')"
  fi

  export METAMAP_BINARY_PATH="$PUBLIC_MM_DIR/bin/metamap20" # Auto-set for the Python controller
  cmd="python '$MIMIC_CTL' '$mode' '$INPUT_DIR' '$OUTPUT_DIR'"

  # Ensure mmserver is up before launching controller
  if ! wait_for_mmserver; then
    echo -e "${RED}mmserver20 not reachable on port 8066. Start servers first.${RESET}"
    return 1
  fi
  
  echo -e "${YELLOW}Running MIMIC controller with nohup in background...${RESET}"
  nohup bash -c "$cmd" > "$MIMIC_LOG_FILE" 2>&1 &
  echo "$!" >> "$PIDS_FILE" # Store controller PID
  echo "MIMIC Controller PID $! (nohup). Output -> $MIMIC_LOG_FILE"
  echo "Use './metamap_cli.sh mimic-progress' or './metamap_cli.sh mimic-tail' to monitor."
}

##############################################################################
# Show MIMIC processing status - DEPRECATED by direct mimic-progress/mimic-tail
##############################################################################
# show_mimic_status() { ... } # Removed as direct commands are better

##############################################################################
# Interactive MIMIC Utilities Menu
##############################################################################
show_mimic_menu() {
  DEMO_DIR="$BASE_DIR/metamap_demo"
  INPUT_DIR="$DEMO_DIR/demoMetaMap2020/in/mimic"
  OUTPUT_DIR="$DEMO_DIR/demoMetaMap2020/out"

  while true; do
    clear
    echo "===== MIMIC Utilities ====="
    echo " 1) Start/Resume MIMIC Batch Processing"
    echo " 2) Progress summary       (./metamap_cli.sh mimic-progress)"
    echo " 3) Tail live log          (./metamap_cli.sh mimic-tail [lines])"
    echo " 4) Validate input notes   (./metamap_cli.sh mimic-validate)"
    echo " 5) List pending files     (./metamap_cli.sh mimic-pending)"
    echo " 6) List completed CSVs    (./metamap_cli.sh mimic-completed)"
    echo " 7) Sample one output CSV  (./metamap_cli.sh mimic-sample)"
    echo " 8) Show controller PID    (./metamap_cli.sh mimic-pid)"
    echo " 9) Kill current batch job (./metamap_cli.sh mimic-kill)"
    echo "10) Clear output CSVs      (./metamap_cli.sh mimic-clearout)"
    echo "11) Show in/out directory"
    echo " Q) Back to Main Menu"
    echo -ne "\nSelect: "
    read -r sub
    case "${sub,,}" in
      1) run_mimic_validated start ; read -p "MIMIC controller started/resumed. Press Enter" ;; # Default to start which should handle resume
      2) python "$MIMIC_CTL" progress "$OUTPUT_DIR" ; read -p "Press Enter" ;;
      3) read -p "Lines to tail [20]: " n; n=${n:-20}; python "$MIMIC_CTL" tail "$OUTPUT_DIR" "$n" ; read -p "Press Enter" ;;
      4) python "$MIMIC_CTL" validate "$INPUT_DIR" ; read -p "Press Enter" ;;
      5) python "$MIMIC_CTL" pending "$INPUT_DIR" "$OUTPUT_DIR" ; read -p "Press Enter" ;;
      6) python "$MIMIC_CTL" completed "$OUTPUT_DIR" ; read -p "Press Enter" ;;
      7) python "$MIMIC_CTL" sample "$OUTPUT_DIR" ; read -p "Press Enter" ;;
      8) python "$MIMIC_CTL" pid "$OUTPUT_DIR" ; read -p "Press Enter" ;;
      9) python "$MIMIC_CTL" kill "$OUTPUT_DIR" ; read -p "Press Enter" ;;
      10) python "$MIMIC_CTL" clearout "$OUTPUT_DIR" ; read -p "Output cleared. Press Enter" ;;
      11) echo "Input: $INPUT_DIR"; echo "Output: $OUTPUT_DIR"; read -p "Press Enter" ;;
      q) break ;;
      *) echo "Invalid" ; sleep 1 ;;
    esac
  done
}

##############################################################################
# Kidney utilities menu
##############################################################################
show_kidney_menu() {
  # KIDNEY_IN and KIDNEY_OUT are global
  
  while true; do
    clear
    echo "===== Kidney Batch Processing & Utilities ====="
    echo " 1) Start/Resume Kidney Batch Processing"
    echo " 2) Progress summary       (./metamap_cli.sh kidney-progress)"
    echo " 3) Tail live log          (./metamap_cli.sh kidney-tail [lines])"
    echo " 4) Validate input notes   (./metamap_cli.sh kidney-validate)"
    echo " 5) List input note files"
    echo " 6) List pending files     (./metamap_cli.sh kidney-pending)"
    echo " 7) List completed CSVs    (./metamap_cli.sh kidney-completed)"
    echo " 8) Sample one output CSV  (./metamap_cli.sh kidney-sample)"
    echo " 9) Show controller PID    (./metamap_cli.sh kidney-pid)"
    echo "10) Kill current batch job (./metamap_cli.sh kidney-kill)"
    echo "11) Clear output CSVs      (./metamap_cli.sh kidney-clearout)"
    echo "12) Show in/out directory"
    echo " Q) Back to Main Menu"
    echo -ne "\nSelect: "
    read -r sub
    case "${sub,,}" in
      1) run_kidney_validated start ; read -p "Kidney controller started/resumed. Press Enter" ;;
      2) python "$MIMIC_CTL" progress "$KIDNEY_OUT" ; read -p "Press Enter" ;;
      3) read -p "Lines to tail [20]: " n; n=${n:-20}; python "$MIMIC_CTL" tail "$KIDNEY_OUT" "$n" ; read -p "Press Enter" ;;
      4) python "$MIMIC_CTL" validate "$KIDNEY_IN" ; read -p "Press Enter" ;;
      5) echo "Listing files in $KIDNEY_IN:" ; ls -1 "$KIDNEY_IN" | cat ; read -p "Press Enter" ;;
      6) python "$MIMIC_CTL" pending "$KIDNEY_IN" "$KIDNEY_OUT" ; read -p "Press Enter" ;;
      7) python "$MIMIC_CTL" completed "$KIDNEY_OUT" ; read -p "Press Enter" ;;
      8) python "$MIMIC_CTL" sample "$KIDNEY_OUT" ; read -p "Press Enter" ;;
      9) python "$MIMIC_CTL" pid "$KIDNEY_OUT" ; read -p "Press Enter" ;;
      10) python "$MIMIC_CTL" kill "$KIDNEY_OUT" ; read -p "Press Enter" ;;
      11) python "$MIMIC_CTL" clearout "$KIDNEY_OUT" ; read -p "Output cleared. Press Enter" ;;
      12) echo "Input: $KIDNEY_IN"; echo "Output: $KIDNEY_OUT"; read -p "Press Enter" ;;
      q) break ;;
      *) echo "Invalid" ; sleep 1 ;;
    esac
  done
}

##############################################################################
# Interactive Menu (Main)
##############################################################################
show_menu() {
  while true; do
    clear
    print_banner
    echo -e "\nSelect an option:\n"
    echo " 1) Start servers automatically (background + manual mmserver20 step)"
    echo " 2) Start servers manually (show commands)"
    echo " 3) Check status of servers"
    echo " 4) Stop all servers"
    echo " 5) Detect Java path"
    echo " 6) Environment summary (includes Python version)"
    echo " 7) Run Java SimpleTest demo"
    echo " 8) Run Java BatchRunner01 (MIMIC note) demo - (Direct Java, no controller)"
    echo " 9) MIMIC Batch Processing & Utilities" # Renamed & consolidated
    echo "10) Kidney Batch Processing & Utilities" # Renamed & consolidated
    echo "11) Recompile Java BatchRunner01"
    echo " Q) Quit"
    echo -ne "\nEnter choice: "
    read -r choice
    case "${choice,,}" in
      1) start_servers_auto ; read -p "Press Enter to continue" ;;
      2) start_servers_manual ; read -p "Press Enter to continue" ;;
      3) check_status ; read -p "Press Enter to continue" ;;
      4) stop_servers ; read -p "Press Enter to continue" ;;
      5) detect_java ; read -p "Press Enter to continue" ;;
      6) env_summary ; read -p "Press Enter to continue" ;;
      7) run_simple_test ; read -p "Press Enter to continue" ;;
      8) run_batch_test ; read -p "Press Enter to continue" ;;
      9) show_mimic_menu ;;
      10) show_kidney_menu ;;
      11) recompile_java_runner ; read -p "Press Enter to continue" ;;
      q) break ;;
      *) echo "Invalid choice" ; sleep 1 ;;
    esac
  done
}

##############################################################################
# Kidney batch variant
##############################################################################
run_kidney_validated() {
  mode="${1:-start}" # 'start' will now handle resume logic in controller
  # KIDNEY_IN and KIDNEY_OUT are global
  KIDNEY_LOG_FILE="$KIDNEY_OUT/kidney_controller.log"

  # Pre-flight check for MetaMap servers
  if ! wait_for_mmserver; then
    echo -e "${RED}mmserver20 not reachable on port 8066. Cannot run Kidney batch.${RESET}"
    echo -e "${YELLOW}Attempt to start servers using './metamap_cli.sh start' or check their status.${RESET}"
    return 1
  fi

  if [[ ! -f "$MIMIC_CTL" ]]; then
    echo -e "${RED}mimic_controller.py not found at $MIMIC_CTL${RESET}"
    return 1
  fi

  # Input directory for kidney notes is $KIDNEY_IN ($BASE_DIR/metamap_demo/demoKidney2025/in/notes)
  if [[ ! -d "$KIDNEY_IN" ]]; then
    echo -e "${RED}Kidney input directory $KIDNEY_IN not found. Cannot proceed.${RESET}"
    return 1
  fi

  mkdir -p "$KIDNEY_OUT"

  CLASSPATH="$PUBLIC_MM_DIR/src/javaapi/target/metamap-api-2.0.jar:$PUBLIC_MM_DIR/src/javaapi/dist/prologbeans.jar:$BASE_DIR/metamap_demo/bin:."

  # ----------------------------------------------------------
  # If user launched mmserver20 instances manually and defined
  # MM_SERVER_PORTS but forgot MAX_PARALLEL_WORKERS, infer it
  # from the number of ports so every server gets at least one
  # Java worker.
  # ----------------------------------------------------------
  if [[ -z "${MAX_PARALLEL_WORKERS:-}" && -n "${MM_SERVER_PORTS:-}" ]]; then
    export MAX_PARALLEL_WORKERS="$(echo "$MM_SERVER_PORTS" | tr ',' '\n' | wc -l | tr -d ' ')"
  fi

  export METAMAP_BINARY_PATH="$PUBLIC_MM_DIR/bin/metamap20" # Auto-set for the Python controller
  cmd="python '$MIMIC_CTL' '$mode' '$KIDNEY_IN' '$KIDNEY_OUT'"

  # Ensure server ready
  if ! wait_for_mmserver; then
    echo -e "${RED}mmserver20 not reachable on port 8066. Start servers first.${RESET}"
    return 1
  fi
  
  echo -e "${YELLOW}Running Kidney controller with nohup in background...${RESET}"
  nohup bash -c "$cmd" > "$KIDNEY_LOG_FILE" 2>&1 &
  echo "$!" >> "$PIDS_FILE" # Store controller PID
  echo "Kidney Controller PID $! (nohup). Output -> $KIDNEY_LOG_FILE"
  echo "Use './metamap_cli.sh kidney-progress' or './metamap_cli.sh kidney-tail' to monitor."
}


##############################################################################
# Test N Kidney Notes
##############################################################################
run_kidney_test() {
  num_to_test="${1:-10}" # Default to 10 if no argument provided
  echo -e "${YELLOW}Preparing to test the first $num_to_test kidney notes...${RESET}"

  # Pre-flight check for MetaMap servers
  if ! wait_for_mmserver; then
    echo -e "${RED}mmserver20 not reachable on port 8066. Cannot run Kidney test.${RESET}"
    echo -e "${YELLOW}Attempt to start servers using './metamap_cli.sh start' or check their status.${RESET}"
    return 1
  fi

  if [[ ! -d "$KIDNEY_IN" ]]; then
    echo -e "${RED}Kidney input directory not found: $KIDNEY_IN${RESET}"
    return 1
  fi

  TEMP_KIDNEY_IN_DIR_NAME="kidney_test_$(date +%s)_$RANDOM"
  TEMP_KIDNEY_IN_PATH="$BASE_DIR/$TEMP_KIDNEY_IN_DIR_NAME" # Create temp in base_dir to avoid /tmp issues
  mkdir -p "$TEMP_KIDNEY_IN_PATH"
  echo "Temporary input directory for test: $TEMP_KIDNEY_IN_PATH"

  # Find and copy the first N files
  find "$KIDNEY_IN" -maxdepth 1 -type f | head -n "$num_to_test" | while IFS= read -r file_path; do
    cp "$file_path" "$TEMP_KIDNEY_IN_PATH/"
  done

  # Check if any files were copied
  if [[ -z "$(ls -A "$TEMP_KIDNEY_IN_PATH")" ]]; then
    echo -e "${RED}No files found in $KIDNEY_IN to test.${RESET}"
    rm -rf "$TEMP_KIDNEY_IN_PATH"
    return 1
  fi

  echo -e "${GREEN}Copied files to temporary directory for testing.${RESET}"

  CLASSPATH="$PUBLIC_MM_DIR/src/javaapi/target/metamap-api-2.0.jar:$PUBLIC_MM_DIR/src/javaapi/dist/prologbeans.jar:$BASE_DIR/metamap_demo/bin:."
  
  # Call controller with 'start'. Controller handles resume from $KIDNEY_OUT.
  # For a true isolated test, $KIDNEY_OUT would need to be a temp dir too, or cleaned.
  # Current plan: test output goes to the regular $KIDNEY_OUT. User should be aware.
  # The controller will determine pending files based on $KIDNEY_OUT.
  export METAMAP_BINARY_PATH="$PUBLIC_MM_DIR/bin/metamap20" # Auto-set for the Python controller
  local cmd_test="python '$MIMIC_CTL' start '$TEMP_KIDNEY_IN_PATH' '$KIDNEY_OUT'"


  if ! wait_for_mmserver; then
    echo -e "${RED}mmserver20 not reachable on port 8066. Start servers first.${RESET}"
    rm -rf "$TEMP_KIDNEY_IN_PATH"
    return 1
  fi

  echo -e "${YELLOW}Launching controller for test run (foreground)...${RESET}"
  # Execute in foreground for a test run to see output directly
  eval "$cmd_test" # Using eval to handle quotes in cmd_test correctly
  
  echo -e "${GREEN}Test run complete. Output (if any) in $KIDNEY_OUT.${RESET}"
  echo -e "${YELLOW}Cleaning up temporary input directory: $TEMP_KIDNEY_IN_PATH${RESET}"
  rm -rf "$TEMP_KIDNEY_IN_PATH"
  echo -e "${GREEN}Temporary input directory removed.${RESET}"
}

##############################################################################
# Recompile BatchRunner01.java
##############################################################################
recompile_java_runner() {
  echo -e "${YELLOW}Attempting to recompile BatchRunner01.java...${RESET}"

  local JAVA_SOURCE_FILE="$BASE_DIR/metamap_demo/demoMetaMap2020/src/demo/metamaprunner2020/BatchRunner01.java"
  local METAMAP_API_JAR="$PUBLIC_MM_DIR/src/javaapi/target/metamap-api-2.0.jar"
  local PROLOGBEANS_JAR="$PUBLIC_MM_DIR/src/javaapi/dist/prologbeans.jar"
  local CLASS_OUTPUT_DIR="$BASE_DIR/metamap_demo/bin"
  local EXPECTED_CLASS_FILE="$CLASS_OUTPUT_DIR/demo/metamaprunner2020/BatchRunner01.class"

  if [[ ! -f "$JAVA_SOURCE_FILE" ]]; then
    echo -e "${RED}Error:${RESET} Java source file not found at $JAVA_SOURCE_FILE" >&2
    return 1
  fi
  if [[ ! -f "$METAMAP_API_JAR" ]]; then
    echo -e "${RED}Error:${RESET} MetaMap API JAR not found at $METAMAP_API_JAR" >&2
    return 1
  fi
  if [[ ! -f "$PROLOGBEANS_JAR" ]]; then
    echo -e "${RED}Error:${RESET} Prologbeans JAR not found at $PROLOGBEANS_JAR" >&2
    return 1
  fi

  if ! command -v javac >/dev/null 2>&1; then
    echo -e "${RED}Error:${RESET} 'javac' command not found. Please ensure JDK is installed and in PATH." >&2
    return 1
  fi

  mkdir -p "$CLASS_OUTPUT_DIR/demo/metamaprunner2020"

  echo "Current directory: $(pwd)"
  echo "Compiling with command:"
  local compile_cmd="javac -cp \"$METAMAP_API_JAR:$PROLOGBEANS_JAR:$CLASS_OUTPUT_DIR:.\" -d \"$CLASS_OUTPUT_DIR\" \"$JAVA_SOURCE_FILE\""
  echo "  $compile_cmd"
  
  # Store modification time of class file before compilation
  local old_mod_time=""
  if [[ -f "$EXPECTED_CLASS_FILE" ]]; then
    old_mod_time=$(stat -c %Y "$EXPECTED_CLASS_FILE")
  fi

  # Execute compilation
  # Need to be careful with quoting if paths have spaces (though current ones don't)
  # Using eval to handle complex command string with quotes, though direct execution is safer if possible.
  # Direct execution:
  # javac -cp "$METAMAP_API_JAR:$PROLOGBEANS_JAR:$CLASS_OUTPUT_DIR:." -d "$CLASS_OUTPUT_DIR" "$JAVA_SOURCE_FILE"
  # For now, using eval as the command string is built up.

  if eval "$compile_cmd"; then # Using eval because of the quotes in classpath and paths
    echo -e "${GREEN}Compilation successful (javac exit code 0).${RESET}"
    if [[ -f "$EXPECTED_CLASS_FILE" ]]; then
        local new_mod_time=$(stat -c %Y "$EXPECTED_CLASS_FILE")
        if [[ "$new_mod_time" != "$old_mod_time" ]]; then
            echo -e "${GREEN}Class file $EXPECTED_CLASS_FILE has been updated.${RESET}"
        else
            echo -e "${YELLOW}Warning:${RESET} javac reported success, but class file modification time did not change. Check for compilation warnings or if source was already up-to-date."
        fi
    else
        echo -e "${RED}Error:${RESET} javac reported success, but expected class file $EXPECTED_CLASS_FILE was not found!"
    fi
  else
    echo -e "${RED}Error:${RESET} Compilation failed (javac returned non-zero exit code)."
    echo "Please check for errors above. You may need to run the javac command manually to diagnose further."
    return 1
  fi
  return 0
}

##############################################################################
# Check if Python Controller is Active
##############################################################################
check_controller_active() {
  local out_dir="$1"
  local state_file="$out_dir/.mimic_state.json"
  local pid_file="$out_dir/.mimic_pid"

  if [[ ! -f "$pid_file" ]]; then
    echo -e "${YELLOW}Controller PID file not found in $out_dir. Likely not running or never started via this CLI.${RESET}"
    return 1
  fi

  local pid
  pid=$(cat "$pid_file")

  if ps -p "$pid" > /dev/null 2>&1; then
    echo -e "${GREEN}Controller process (PID: $pid) is running.${RESET}"
    if [[ -f "$state_file" ]]; then
      if grep -q '"end_time":' "$state_file"; then # Crude check if end_time key exists
        echo -e "${YELLOW}State file indicates batch may have an end_time. Check progress for details.${RESET}"
      else
        echo -e "${GREEN}State file does not indicate an end_time; batch likely ongoing.${RESET}"
      fi
    else
      echo -e "${YELLOW}State file not found. Cannot confirm batch completion status from state.${RESET}"
    fi
  else
    echo -e "${RED}Controller process (PID: $pid) is NOT running.${RESET}"
    if [[ -f "$state_file" ]]; then
      if grep -q '"end_time":' "$state_file"; then
        echo -e "${GREEN}State file indicates batch has an end_time (likely completed or stopped).${RESET}"
      else
        echo -e "${YELLOW}State file does not indicate an end_time, but process is gone. May have crashed.${RESET}"
      fi
    else
      echo -e "${YELLOW}State file not found. Process is gone.${RESET}"
    fi
  fi
}

##############################################################################
# Main
##############################################################################
main() {
  resolve_directories

  case "${1:-interactive}" in
    start)   start_servers_auto ;;
    manual)  start_servers_manual ;;
    status)  check_status ;;
    stop)    stop_servers ;;
    java)    detect_java ;;
    env)     env_summary ;;
    simple)  run_simple_test ;;
    batch)   run_batch_test ;;
    recompile-java) recompile_java_runner ;;
    
    mimic)           run_mimic_validated "${2:-start}" ;;
    mimic-progress)  python "$MIMIC_CTL" progress "$BASE_DIR/metamap_demo/demoMetaMap2020/out" ;;
    mimic-tail)      python "$MIMIC_CTL" tail "$BASE_DIR/metamap_demo/demoMetaMap2020/out" "${2:-20}" ;;
    mimic-validate)  python "$MIMIC_CTL" validate "$BASE_DIR/metamap_demo/demoMetaMap2020/in/mimic" ;;
    mimic-pending)   python "$MIMIC_CTL" pending "$BASE_DIR/metamap_demo/demoMetaMap2020/in/mimic" "$BASE_DIR/metamap_demo/demoMetaMap2020/out" ;;
    mimic-completed) python "$MIMIC_CTL" completed "$BASE_DIR/metamap_demo/demoMetaMap2020/out" ;;
    mimic-sample)    python "$MIMIC_CTL" sample "$BASE_DIR/metamap_demo/demoMetaMap2020/out" ;;
    mimic-pid)       python "$MIMIC_CTL" pid "$BASE_DIR/metamap_demo/demoMetaMap2020/out" ;;
    mimic-kill)      python "$MIMIC_CTL" kill "$BASE_DIR/metamap_demo/demoMetaMap2020/out" ;;
    mimic-clearout)  python "$MIMIC_CTL" clearout "$BASE_DIR/metamap_demo/demoMetaMap2020/out" ;;
    mimic-outdir)    echo "$BASE_DIR/metamap_demo/demoMetaMap2020/out" ;;
    mimic-indir)     echo "$BASE_DIR/metamap_demo/demoMetaMap2020/in/mimic" ;;
    mimic-active)    check_controller_active "$BASE_DIR/metamap_demo/demoMetaMap2020/out" ;;
    
    kidney)            run_kidney_validated "${2:-start}" ;;
    kidney-progress)   python "$MIMIC_CTL" progress "$KIDNEY_OUT" ;;
    kidney-tail)       python "$MIMIC_CTL" tail "$KIDNEY_OUT" "${2:-20}" ;;
    kidney-validate)   python "$MIMIC_CTL" validate "$KIDNEY_IN" ;;
    kidney-pending)    python "$MIMIC_CTL" pending "$KIDNEY_IN" "$KIDNEY_OUT" ;;
    kidney-completed)  python "$MIMIC_CTL" completed "$KIDNEY_OUT" ;;
    kidney-sample)     python "$MIMIC_CTL" sample "$KIDNEY_OUT" ;;
    kidney-pid)        python "$MIMIC_CTL" pid "$KIDNEY_OUT" ;;
    kidney-kill)       python "$MIMIC_CTL" kill "$KIDNEY_OUT" ;;
    kidney-clearout)   python "$MIMIC_CTL" clearout "$KIDNEY_OUT" ;;
    kidney-outdir)     echo "$KIDNEY_OUT" ;;
    kidney-indir)      echo "$KIDNEY_IN" ;;
    kidney-test)       run_kidney_test "${2:-}" ;; # Pass optional N
    kidney-active)     check_controller_active "$KIDNEY_OUT" ;;
    
    help|-h|--help) echo "Usage: $0 [start|manual|status|stop|java|env|simple|batch|mimic|mimic-progress|...|kidney|kidney-progress|...|kidney-test <N>]" ;;
    *)       show_menu ;;
  esac
}

main "$@" 