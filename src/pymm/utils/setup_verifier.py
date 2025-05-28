"""
Setup Verifier for PythonMetaMap
Ensures all components are properly installed and configured
"""
import os
import sys
import subprocess
import shutil
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class SetupVerifier:
    """Verify and fix PythonMetaMap setup"""
    
    def __init__(self):
        self.issues = []
        self.warnings = []
        self.fixes_applied = []
        
    def verify_all(self) -> Dict[str, any]:
        """Run all verification checks"""
        results = {
            'python_version': self._check_python_version(),
            'dependencies': self._check_dependencies(),
            'metamap_installation': self._check_metamap_installation(),
            'java_setup': self._check_java_setup(),
            'server_scripts': self._check_server_scripts(),
            'directories': self._check_directories(),
            'permissions': self._check_permissions(),
            'configuration': self._check_configuration()
        }
        
        return {
            'results': results,
            'issues': self.issues,
            'warnings': self.warnings,
            'fixes_applied': self.fixes_applied,
            'is_valid': len(self.issues) == 0
        }
    
    def _check_python_version(self) -> Dict[str, any]:
        """Check Python version compatibility"""
        version = sys.version_info
        is_valid = version.major == 3 and version.minor >= 7
        
        result = {
            'version': f"{version.major}.{version.minor}.{version.micro}",
            'is_valid': is_valid
        }
        
        if not is_valid:
            self.issues.append(f"Python 3.7+ required, found {result['version']}")
            
        return result
    
    def _check_dependencies(self) -> Dict[str, any]:
        """Check required Python dependencies"""
        required_packages = [
            'psutil',
            'colorama', 
            'tqdm',
            'rich',
            'jpype1'
        ]
        
        missing = []
        installed = []
        
        for package in required_packages:
            try:
                __import__(package.replace('-', '_').lower())
                installed.append(package)
            except ImportError:
                missing.append(package)
        
        result = {
            'installed': installed,
            'missing': missing,
            'is_valid': len(missing) == 0
        }
        
        if missing:
            self.warnings.append(f"Missing Python packages: {', '.join(missing)}")
            self.warnings.append("Install with: pip install " + ' '.join(missing))
            
        return result
    
    def _check_metamap_installation(self) -> Dict[str, any]:
        """Check MetaMap installation"""
        from ..install_metamap import META_INSTALL_DIR, EXPECTED_METAMAP_BINARY
        
        result = {
            'install_dir': META_INSTALL_DIR,
            'binary_path': EXPECTED_METAMAP_BINARY,
            'is_installed': False,
            'is_executable': False,
            'version': None
        }
        
        if os.path.exists(EXPECTED_METAMAP_BINARY):
            result['is_installed'] = True
            result['is_executable'] = os.access(EXPECTED_METAMAP_BINARY, os.X_OK)
            
            # Try to get version
            try:
                output = subprocess.check_output(
                    [EXPECTED_METAMAP_BINARY, '--version'],
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=5
                )
                if 'MetaMap' in output:
                    result['version'] = output.strip()
            except:
                pass
        
        if not result['is_installed']:
            self.issues.append("MetaMap is not installed")
            self.issues.append("Run: pymm install-metamap")
        elif not result['is_executable']:
            self.issues.append(f"MetaMap binary is not executable: {EXPECTED_METAMAP_BINARY}")
            # Try to fix
            try:
                os.chmod(EXPECTED_METAMAP_BINARY, 0o755)
                self.fixes_applied.append(f"Made {EXPECTED_METAMAP_BINARY} executable")
                result['is_executable'] = True
            except Exception as e:
                self.issues.append(f"Failed to make binary executable: {e}")
                
        return result
    
    def _check_java_setup(self) -> Dict[str, any]:
        """Check Java installation and configuration"""
        result = {
            'java_home': os.environ.get('JAVA_HOME', ''),
            'java_version': None,
            'is_valid': False
        }
        
        # Check if Java is available
        java_cmd = shutil.which('java')
        if java_cmd:
            try:
                output = subprocess.check_output(
                    ['java', '-version'],
                    stderr=subprocess.STDOUT,
                    text=True
                )
                result['java_version'] = output.split('\n')[0]
                result['is_valid'] = True
            except:
                pass
        
        if not result['is_valid']:
            self.issues.append("Java is not installed or not in PATH")
            self.issues.append("Install Java 8 or higher")
        elif not result['java_home']:
            self.warnings.append("JAVA_HOME environment variable is not set")
            
        return result
    
    def _check_server_scripts(self) -> Dict[str, any]:
        """Check MetaMap server scripts"""
        from ..install_metamap import META_INSTALL_DIR
        
        scripts_dir = Path(META_INSTALL_DIR) / "public_mm" / "bin"
        required_scripts = ['skrmedpostctl', 'wsdserverctl']
        
        result = {
            'scripts_dir': str(scripts_dir),
            'found': [],
            'missing': [],
            'is_valid': False
        }
        
        if scripts_dir.exists():
            for script in required_scripts:
                script_path = scripts_dir / script
                if script_path.exists():
                    result['found'].append(script)
                    # Make executable if not already
                    if not os.access(script_path, os.X_OK):
                        try:
                            os.chmod(script_path, 0o755)
                            self.fixes_applied.append(f"Made {script} executable")
                        except:
                            pass
                else:
                    result['missing'].append(script)
        else:
            result['missing'] = required_scripts
            
        result['is_valid'] = len(result['missing']) == 0
        
        if result['missing']:
            self.warnings.append(f"Missing server scripts: {', '.join(result['missing'])}")
            
        return result
    
    def _check_directories(self) -> Dict[str, any]:
        """Check required directories"""
        from ..core.config import PyMMConfig
        
        config = PyMMConfig()
        
        dirs_to_check = {
            'input_notes': Path('input_notes'),
            'output_csvs': Path('output_csvs'),
            'metamap_install': Path('metamap_install')
        }
        
        result = {
            'checked': {},
            'created': [],
            'is_valid': True
        }
        
        for name, path in dirs_to_check.items():
            exists = path.exists()
            result['checked'][name] = {
                'path': str(path),
                'exists': exists
            }
            
            if not exists and name in ['input_notes', 'output_csvs']:
                try:
                    path.mkdir(parents=True, exist_ok=True)
                    result['created'].append(name)
                    self.fixes_applied.append(f"Created directory: {path}")
                except Exception as e:
                    self.warnings.append(f"Could not create {name} directory: {e}")
                    
        return result
    
    def _check_permissions(self) -> Dict[str, any]:
        """Check file and directory permissions"""
        from ..install_metamap import META_INSTALL_DIR
        
        result = {
            'issues': [],
            'fixed': [],
            'is_valid': True
        }
        
        # Check MetaMap installation directory
        mm_dir = Path(META_INSTALL_DIR)
        if mm_dir.exists():
            # Check if we can write to the directory
            test_file = mm_dir / '.pymm_test'
            try:
                test_file.touch()
                test_file.unlink()
            except:
                result['issues'].append(f"Cannot write to {mm_dir}")
                result['is_valid'] = False
                
        return result
    
    def _check_configuration(self) -> Dict[str, any]:
        """Check PyMM configuration"""
        from ..core.config import PyMMConfig
        
        config = PyMMConfig()
        
        result = {
            'config_file': str(config.CONFIG_FILE),
            'exists': Path(config.CONFIG_FILE).exists(),
            'settings': {},
            'is_valid': True
        }
        
        # Check key settings
        key_settings = [
            'metamap_binary_path',
            'server_scripts_dir',
            'default_input_dir',
            'default_output_dir'
        ]
        
        for key in key_settings:
            value = config.get(key)
            result['settings'][key] = {
                'value': value,
                'is_set': value is not None and value != ''
            }
            
            if not result['settings'][key]['is_set'] and key == 'metamap_binary_path':
                self.warnings.append(f"Configuration '{key}' is not set")
                
        return result
    
    def fix_common_issues(self) -> List[str]:
        """Attempt to fix common setup issues"""
        fixes = []
        
        # Fix directory permissions on Unix-like systems
        if sys.platform != 'win32':
            from ..install_metamap import META_INSTALL_DIR
            mm_dir = Path(META_INSTALL_DIR)
            
            if mm_dir.exists():
                try:
                    # Make all scripts executable
                    for script in mm_dir.rglob('*.sh'):
                        os.chmod(script, 0o755)
                        fixes.append(f"Made {script.name} executable")
                        
                    for script in mm_dir.rglob('*ctl'):
                        os.chmod(script, 0o755)
                        fixes.append(f"Made {script.name} executable")
                except Exception as e:
                    logger.warning(f"Could not fix permissions: {e}")
                    
        return fixes
    
    def print_report(self):
        """Print a formatted verification report"""
        print("\n" + "="*70)
        print("PythonMetaMap Setup Verification Report")
        print("="*70)
        
        results = self.verify_all()
        
        # Summary
        print(f"\nStatus: {'✓ VALID' if results['is_valid'] else '✗ INVALID'}")
        print(f"Issues: {len(results['issues'])}")
        print(f"Warnings: {len(results['warnings'])}")
        print(f"Fixes Applied: {len(results['fixes_applied'])}")
        
        # Details
        if results['issues']:
            print("\n[ISSUES - Must Fix]")
            for issue in results['issues']:
                print(f"  ✗ {issue}")
                
        if results['warnings']:
            print("\n[WARNINGS - Should Fix]")
            for warning in results['warnings']:
                print(f"  ⚠ {warning}")
                
        if results['fixes_applied']:
            print("\n[FIXES APPLIED]")
            for fix in results['fixes_applied']:
                print(f"  ✓ {fix}")
                
        # Component Status
        print("\n[COMPONENT STATUS]")
        for component, status in results['results'].items():
            if isinstance(status, dict) and 'is_valid' in status:
                symbol = '✓' if status.get('is_valid', False) else '✗'
                print(f"  {symbol} {component.replace('_', ' ').title()}")
                
        print("\n" + "="*70)
        
        return results['is_valid']


def verify_setup():
    """Run setup verification"""
    verifier = SetupVerifier()
    is_valid = verifier.print_report()
    
    if not is_valid:
        print("\nRun 'pymm setup --fix' to attempt automatic fixes")
        
    return is_valid


def fix_setup():
    """Run setup verification and apply fixes"""
    verifier = SetupVerifier()
    
    # First verify
    print("Checking setup...")
    results = verifier.verify_all()
    
    # Apply additional fixes
    print("\nApplying fixes...")
    additional_fixes = verifier.fix_common_issues()
    
    # Verify again
    print("\nVerifying after fixes...")
    verifier = SetupVerifier()  # Fresh instance
    is_valid = verifier.print_report()
    
    return is_valid


if __name__ == "__main__":
    verify_setup() 