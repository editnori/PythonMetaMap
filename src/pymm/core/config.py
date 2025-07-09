"""Configuration management for PythonMetaMap"""
import json
import os
import sys
from pathlib import Path
from typing import Optional, Any, Dict, List
from dataclasses import dataclass, field


@dataclass
class Config:
    """Configuration for MetaMap processing with Java API support"""
    # MetaMap paths
    metamap_path: str = ""
    metamap_binary: str = ""
    server_scripts_dir: str = ""
    
    # Java configuration
    java_home: str = ""
    java_heap_size: str = "4G"
    java_api_path: str = ""
    
    # Processing options
    data_version: str = "2020AA"
    lexicon: str = "db"
    word_sense_disambiguation: bool = True
    prefer_multiple_concepts: bool = False
    ignore_word_order: bool = False
    allow_overmatches: bool = False
    compute_all_mappings: bool = False
    
    # Semantic types
    restrict_to_sources: List[str] = field(default_factory=list)
    exclude_sources: List[str] = field(default_factory=list)
    restrict_to_sts: List[str] = field(default_factory=list)
    exclude_sts: List[str] = field(default_factory=list)
    
    # Performance options
    max_instances: int = 4
    timeout: int = 300
    max_retries: int = 3
    batch_size: int = 100
    
    # Server configuration
    tagger_port_base: int = 1795
    wsd_port_base: int = 5554
    mmserver_port_base: int = 8066
    server_startup_timeout: int = 60
    server_persistence_hours: int = 24
    
    # Output options
    output_dir: str = ""
    output_format: str = "csv"
    include_positions: bool = True
    
    # Custom options
    custom_options: str = ""
    
    @classmethod
    def from_pymm_config(cls, pymm_config) -> 'Config':
        """Create Config from PyMMConfig instance"""
        config = cls()
        
        # Map PyMMConfig fields to Config fields
        if pymm_config.get("metamap_binary_path"):
            binary_path = Path(pymm_config.get("metamap_binary_path"))
            config.metamap_path = str(binary_path.parent.parent)
            config.metamap_binary = str(binary_path)
        
        if pymm_config.get("server_scripts_dir"):
            config.server_scripts_dir = pymm_config.get("server_scripts_dir")
        
        # Java configuration
        config.java_home = pymm_config.get("java_home", os.environ.get("JAVA_HOME", ""))
        config.java_heap_size = pymm_config.get("java_heap_size", "4G")
        
        # Processing options from string
        if pymm_config.get("metamap_processing_options"):
            options = pymm_config.get("metamap_processing_options")
            if "-y" in options or "--word_sense_disambiguation" in options:
                config.word_sense_disambiguation = True
            if "-Y" in options:
                config.prefer_multiple_concepts = True
            if "-i" in options:
                config.ignore_word_order = True
            if "-o" in options:
                config.allow_overmatches = True
            if "-b" in options:
                config.compute_all_mappings = True
        
        # Performance options
        config.max_instances = pymm_config.get("max_parallel_workers", 4)
        config.timeout = pymm_config.get("pymm_timeout", 300)
        config.max_retries = pymm_config.get("retry_max_attempts", 3)
        
        return config
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "metamap_path": self.metamap_path,
            "metamap_binary": self.metamap_binary,
            "server_scripts_dir": self.server_scripts_dir,
            "data_version": self.data_version,
            "lexicon": self.lexicon,
            "word_sense_disambiguation": self.word_sense_disambiguation,
            "prefer_multiple_concepts": self.prefer_multiple_concepts,
            "ignore_word_order": self.ignore_word_order,
            "allow_overmatches": self.allow_overmatches,
            "compute_all_mappings": self.compute_all_mappings,
            "restrict_to_sources": self.restrict_to_sources,
            "exclude_sources": self.exclude_sources,
            "restrict_to_sts": self.restrict_to_sts,
            "exclude_sts": self.exclude_sts,
            "max_instances": self.max_instances,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "batch_size": self.batch_size,
            "output_dir": self.output_dir,
            "output_format": self.output_format,
            "java_heap_size": self.java_heap_size,
            "use_java_api": self.use_java_api,
            "custom_options": self.custom_options
        }


class PyMMConfig:
    """Centralized configuration management for PythonMetaMap"""
    
    CONFIG_FILE = Path.home() / ".pymm_controller_config.json"
    
    DEFAULTS = {
        "metamap_processing_options": "-c -Q 4 -K --sldi -I --XMLf1 --negex -y -Z 2020AA --lexicon db",
        "max_parallel_workers": 4,
        "pymm_timeout": 300,
        "java_heap_size": "4g",
        "retry_max_attempts": 3,
        "retry_backoff_base": 2,
        "health_check_interval": 30,
        "port_wait_timeout": 60,
        "use_instance_pool": None,  # Auto-detect based on CPU
        "metamap_instance_count": None,  # Auto-detect
        "default_input_dir": "./input_notes",
        "default_output_dir": "./output_csvs"
    }
    
    def __init__(self):
        self._config = self._load_config()
        self._apply_auto_defaults()
        self._apply_auto_detection()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from file, merge with defaults"""
        if self.CONFIG_FILE.exists():
            try:
                with open(self.CONFIG_FILE, 'r') as f:
                    user_config = json.load(f)
                return {**self.DEFAULTS, **user_config}
            except Exception as e:
                print(f"Warning: Config load error: {e}, using defaults", file=sys.stderr)
        return self.DEFAULTS.copy()
    
    def _apply_auto_defaults(self):
        """Apply automatic defaults based on system capabilities"""
        cpu_count = os.cpu_count() or 4
        
        # Auto-configure instance pool usage
        if self._config.get('use_instance_pool') is None:
            self._config['use_instance_pool'] = cpu_count > 8
            
        # Auto-configure instance count
        if self._config.get('metamap_instance_count') is None:
            self._config['metamap_instance_count'] = max(1, cpu_count // 4)
    
    def _apply_auto_detection(self):
        """Apply auto-detected paths and settings"""
        try:
            from ..utils.auto_detector import AutoDetector
            detector = AutoDetector()
            
            # Only auto-detect if not already configured
            if not self._config.get('java_home'):
                java_home = detector.detect_java()
                if java_home:
                    self._config['java_home'] = java_home
                    
            if not self._config.get('metamap_home'):
                metamap_home = detector.detect_metamap()
                if metamap_home:
                    self._config['metamap_home'] = metamap_home
                    
            if not self._config.get('metamap_binary_path'):
                metamap_binary = detector.detect_metamap_binary(
                    self._config.get('metamap_home')
                )
                if metamap_binary:
                    self._config['metamap_binary_path'] = metamap_binary
            
            # Auto-detect directories
            dirs = detector.detect_data_directories()
            if not os.path.exists(self._config.get('default_input_dir', '')):
                self._config['default_input_dir'] = dirs['input']
            if not os.path.exists(self._config.get('default_output_dir', '')):
                self._config['default_output_dir'] = dirs['output']
            
            # Apply optimal settings if not configured
            settings = detector.get_optimal_settings()
            if self._config.get('max_parallel_workers') == self.DEFAULTS['max_parallel_workers']:
                self._config['max_parallel_workers'] = settings['workers']
            if not self._config.get('chunk_size'):
                self._config['chunk_size'] = settings['chunk_size']
                
        except Exception as e:
            # Silent fail - auto-detection is optional
            pass
    
    def save(self):
        """Save current configuration to file"""
        self.CONFIG_FILE.parent.mkdir(exist_ok=True, parents=True)
        with open(self.CONFIG_FILE, 'w') as f:
            json.dump(self._config, f, indent=2)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with fallback to environment variable"""
        value = self._config.get(key)
        if value is None:
            env_value = os.getenv(key.upper())
            if env_value is not None:
                value = env_value
        
        # Use default if no value found
        if value is None:
            return default
            
        # Type conversion for known numeric fields
        numeric_fields = {
            'max_parallel_workers', 'pymm_timeout', 'retry_max_attempts',
            'retry_backoff_base', 'health_check_interval', 'port_wait_timeout',
            'metamap_instance_count', 'max_instances', 'server_startup_timeout',
            'server_persistence_hours', 'tagger_port_base', 'wsd_port_base',
            'mmserver_port_base'
        }
        
        if key in numeric_fields and isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return default
                
        return value
    
    def set(self, key: str, value: Any):
        """Set configuration value and save"""
        self._config[key] = value
        self.save()
        print(f"Configuration updated: '{key}' = {value}", file=sys.stderr)
    
    def remove(self, key: str):
        """Remove configuration key"""
        if key in self._config:
            del self._config[key]
            self.save()
            print(f"Config value '{key}' removed", file=sys.stderr)
    
    def reset(self, keys: Optional[list] = None):
        """Reset configuration to defaults"""
        if keys:
            for key in keys:
                if key in self._config:
                    if key in self.DEFAULTS:
                        self._config[key] = self.DEFAULTS[key]
                    else:
                        del self._config[key]
        else:
            self._config = self.DEFAULTS.copy()
            self._apply_auto_defaults()
        self.save()
    
    def prompt_for_value(self, key: str, prompt_text: str, 
                        explanation: str = "", is_essential: bool = False,
                        validator=None) -> Optional[str]:
        """Interactive prompt for configuration value"""
        current_value = self.get(key)
        
        if explanation:
            print(f"  ({explanation})")
            
        hint = ""
        if current_value:
            hint = f" (current: '{current_value}', Enter to keep)"
        elif key in self.DEFAULTS:
            hint = f" (default: '{self.DEFAULTS[key]}', Enter to use)"
            
        try:
            user_input = input(f"{prompt_text}{hint}: ").strip()
            
            if user_input:
                if validator and not validator(user_input):
                    print(f"Invalid value: {user_input}")
                    return None
                self.set(key, user_input)
                return user_input
            elif current_value:
                return current_value
            elif key in self.DEFAULTS:
                self.set(key, self.DEFAULTS[key])
                return self.DEFAULTS[key]
            elif is_essential:
                print(f"Error: Essential setting '{key}' requires a value")
                return None
                
        except (EOFError, KeyboardInterrupt):
            print(f"\nWarning: Cannot prompt for {key}", file=sys.stderr)
            return current_value
    
    def discover_metamap_binary(self, search_root: str = "metamap_install") -> Optional[str]:
        """Search for MetaMap binary in installation directory"""
        search_path = Path(search_root)
        if not search_path.is_dir():
            return None
            
        candidate_names = {
            "metamap", "metamap20",
            "metamap.exe", "metamap20.exe",
            "metamap.bat", "metamap20.bat",
        }
        
        for root, dirs, files in os.walk(search_path):
            for fname in files:
                if fname.lower() in candidate_names:
                    return str(Path(root) / fname)
        return None
    
    def configure_interactive(self, reset: bool = False):
        """Interactive configuration wizard"""
        print("\n--- PythonMetaMap Configuration ---")
        
        if reset:
            keys_to_reset = [
                "metamap_binary_path", "server_scripts_dir",
                "default_input_dir", "default_output_dir",
                "metamap_processing_options", "max_parallel_workers"
            ]
            self.reset(keys_to_reset)
            print("Configuration reset. Re-prompting for values.")
        
        # Essential: MetaMap binary path
        binary_path = self.get("metamap_binary_path")
        if not binary_path:
            discovered = self.discover_metamap_binary()
            if discovered:
                print(f"Found MetaMap binary: {discovered}")
                use_discovered = input("Use this binary? (yes/no) [yes]: ").strip().lower()
                if use_discovered != 'no':
                    self.set("metamap_binary_path", discovered)
                    binary_path = discovered
        
        if not binary_path:
            binary_path = self.prompt_for_value(
                "metamap_binary_path",
                "Full path to MetaMap binary",
                "e.g., /opt/public_mm/bin/metamap",
                is_essential=True
            )
            
            if not binary_path:
                # Offer to install
                try:
                    choice = input("Install MetaMap now? (yes/no): ").strip().lower()
                    if choice == 'yes':
                        from ..install_metamap import main as install_main
                        installed_path = install_main()
                        if installed_path and os.path.isfile(installed_path):
                            self.set("metamap_binary_path", installed_path)
                            binary_path = installed_path
                except Exception as e:
                    print(f"Installation failed: {e}")
        
        # Server scripts directory
        if binary_path:
            default_scripts_dir = str(Path(binary_path).parent)
            self.prompt_for_value(
                "server_scripts_dir",
                "Directory containing server control scripts",
                "Usually same as MetaMap binary directory",
                validator=lambda x: os.path.isdir(x)
            )
        
        # Other settings
        self.prompt_for_value(
            "default_input_dir",
            "Default input directory for notes",
            "Directory containing .txt files to process"
        )
        
        self.prompt_for_value(
            "default_output_dir", 
            "Default output directory for CSVs",
            "Directory to save results"
        )
        
        self.prompt_for_value(
            "metamap_processing_options",
            "MetaMap processing options",
            "Command-line flags for MetaMap (e.g., -y -Z ...)"
        )
        
        self.prompt_for_value(
            "max_parallel_workers",
            "Maximum parallel workers",
            "Number of files to process simultaneously",
            validator=lambda x: x.isdigit() and int(x) > 0
        )
        
        self.prompt_for_value(
            "java_heap_size",
            "Java heap size",
            "e.g., 4g, 16g, 100g - increase for large files"
        )
        
        print("--- Configuration Complete ---")
        
    def __getattr__(self, name: str) -> Any:
        """Allow attribute-style access to config values"""
        if name.startswith('_'):
            raise AttributeError(name)
        return self.get(name)