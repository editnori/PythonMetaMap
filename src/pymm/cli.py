import argparse
import os
import sys
import json
import csv
import multiprocessing as mp
from .pymm import Metamap, MetamapStuck


def _find_default_metamap():
    """Return the default MetaMap binary path if installed relative to repo."""
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
    candidate = os.path.join(repo_root, "metamap_install", "public_mm", "bin", "metamap20")
    return candidate if os.path.exists(candidate) else None


def _process_file(args):
    """Worker helper for processing a single file."""
    filepath, out_dir, metamap_path, timeout = args
    basename = os.path.basename(filepath)
    out_csv = os.path.join(out_dir, os.path.splitext(basename)[0] + '.csv')
    mm = Metamap(metamap_path)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            text = f.read().strip()
        if not text:
            return basename, False, 'empty input'
        mmos = mm.parse([text], timeout=timeout)
        concepts = [c for mmo in mmos for c in mmo]
        with open(out_csv, 'w', encoding='utf-8', newline='') as out_f:
            writer = csv.writer(out_f)
            writer.writerow(['cui', 'score', 'matched'])
            for c in concepts:
                writer.writerow([c.cui, c.score, c.matched])
        return basename, True, None
    except Exception as exc:  # broad catch to report failures
        return basename, False, str(exc)
    finally:
        try:
            mm.close()
        except Exception:
            pass

def run_batch(input_dir, output_dir, metamap_path, workers, timeout):
    files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.txt')]
    if not files:
        print('No input files found in', input_dir)
        return 1
    os.makedirs(output_dir, exist_ok=True)
    state_file = os.path.join(output_dir, '.pymm_state.json')
    state = {}
    if os.path.exists(state_file):
        with open(state_file, 'r') as f:
            state = json.load(f)
    pending = [f for f in files if os.path.basename(f) not in state]
    args_iter = [(f, output_dir, metamap_path, timeout) for f in pending]
    with mp.Pool(processes=workers) as pool:
        for basename, ok, err in pool.imap_unordered(_process_file, args_iter):
            state[basename] = {'ok': ok, 'error': err}
            with open(state_file, 'w') as sf:
                json.dump(state, sf, indent=2)
            status = 'OK' if ok else 'FAIL'
            print(basename, status)
    return 0

def main(argv=None):
    parser = argparse.ArgumentParser(description='Run MetaMap over a directory of text files.')
    parser.add_argument('input_dir', help='Directory containing .txt files')
    parser.add_argument('output_dir', help='Directory to store output .csv files')
    parser.add_argument('--metamap-path', default=None, help='Path to metamap binary')
    parser.add_argument('--workers', type=int, default=max(1, mp.cpu_count() - 1), help='Number of parallel workers')
    parser.add_argument('--timeout', type=int, default=60, help='Timeout for metamap processing per file')
    args = parser.parse_args(argv)

    if not args.metamap_path:
        args.metamap_path = os.getenv('METAMAP_PATH') or _find_default_metamap()

    if not args.metamap_path or not os.path.exists(args.metamap_path):
        parser.error('Valid MetaMap installation not found. Use --metamap-path or set METAMAP_PATH')
    return run_batch(args.input_dir, args.output_dir, args.metamap_path, args.workers, args.timeout)

if __name__ == '__main__':
    sys.exit(main())
