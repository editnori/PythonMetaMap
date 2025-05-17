import os
import argparse
import urllib.request
import tarfile
import tempfile
import subprocess

MAIN_URL = "https://github.com/LHNCBC/MetaMap-src/releases/download/public_mm_2020/public_mm_linux_main_2020.tar.bz2"
JAVA_URL = "https://github.com/LHNCBC/MetaMap-src/releases/download/public_mm_2020/public_mm_linux_javaapi_2020v2.tar.bz2"

def download_and_extract(url, dest):
    tar_path = os.path.join(tempfile.gettempdir(), os.path.basename(url))
    print(f'Downloading {url}...')
    try:
        urllib.request.urlretrieve(url, tar_path)
        print('Extracting...')
        with tarfile.open(tar_path, 'r:*') as tar:
            tar.extractall(dest)
        os.remove(tar_path)
        return True
    except urllib.error.URLError as e:
        print(f'Download error: {e}')
        return False
    except tarfile.ReadError as e:
        print(f'Error extracting archive: {e}')
        if os.path.exists(tar_path):
            os.remove(tar_path)
        return False
    except Exception as e:
        print(f'Unexpected error: {e}')
        if os.path.exists(tar_path):
            os.remove(tar_path)
        return False

def parse_args():
    parser = argparse.ArgumentParser(description='Download and install MetaMap')
    default_dir = os.path.join(os.path.dirname(__file__), 'metamap_install')
    parser.add_argument('--install-dir', default=default_dir,
                        help='Directory to install MetaMap (default: %(default)s)')
    return parser.parse_args()

def main():
    args = parse_args()
    install_dir = os.path.expanduser(args.install_dir)
    public_mm = os.path.join(install_dir, 'public_mm')
    if os.path.exists(public_mm):
        print('MetaMap already installed at', public_mm)
        return
    try:
        os.makedirs(install_dir, exist_ok=True)
        print('Step 1/3: Downloading and extracting main package...')
        if not download_and_extract(MAIN_URL, install_dir):
            print('Failed to download or extract main package. Installation aborted.')
            return
        print('Step 2/3: Downloading and extracting Java API package...')
        download_and_extract(JAVA_URL, install_dir)
        print('Step 3/3: Running MetaMap installation script...')
        install_script = os.path.join(public_mm, 'bin', 'install.sh')
        if os.path.exists(install_script):
            try:
                subprocess.check_call(['bash', install_script], cwd=public_mm)
                print('MetaMap installation completed successfully!')
                print(f'MetaMap binaries are located at: {public_mm}/bin')
            except subprocess.CalledProcessError as e:
                print(f'Error running installation script: {e}')
                print('Please run the installation script manually:')
                print(f'cd {public_mm} && ./bin/install.sh')
        else:
            print('Installation script not found. Please complete installation manually:')
            print(f'Check the README in {public_mm} for instructions.')
    except Exception as e:
        print(f'Unexpected error during installation: {e}')

if __name__ == '__main__':
    main()
