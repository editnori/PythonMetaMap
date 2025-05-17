import os
import tarfile
import urllib.request
import subprocess
import tempfile

META_INSTALL_DIR = os.path.join(os.path.dirname(__file__), 'metamap_install')
PUBLIC_MM_DIR = os.path.join(META_INSTALL_DIR, 'public_mm')

MAIN_URL = 'https://github.com/LHNCBC/MetaMap-src/releases/download/public_mm_2020/public_mm_linux_main_2020.tar.bz2'
JAVA_URL = 'https://github.com/LHNCBC/MetaMap-src/releases/download/public_mm_2020/public_mm_linux_javaapi_2020v2.tar.bz2'


def download_and_extract(url, dest):
    tar_path = os.path.join(tempfile.gettempdir(), os.path.basename(url))
    print(f'Downloading {url}...')
    urllib.request.urlretrieve(url, tar_path)
    print('Extracting...')
    with tarfile.open(tar_path, 'r:*') as tar:
        tar.extractall(dest)
    os.remove(tar_path)


def main():
    if os.path.exists(PUBLIC_MM_DIR):
        print('MetaMap already installed at', PUBLIC_MM_DIR)
        return
    os.makedirs(META_INSTALL_DIR, exist_ok=True)
    download_and_extract(MAIN_URL, META_INSTALL_DIR)
    download_and_extract(JAVA_URL, META_INSTALL_DIR)
    install_script = os.path.join(PUBLIC_MM_DIR, 'bin', 'install.sh')
    if os.path.exists(install_script):
        subprocess.check_call(['bash', install_script], cwd=PUBLIC_MM_DIR)
    else:
        print('install.sh not found; please complete installation manually.')


if __name__ == '__main__':
    main()
