"""Custom exceptions for PythonMetaMap"""

class PyMMError(Exception):
    """Base exception for PythonMetaMap"""
    pass

class MetamapStuck(PyMMError):
    """MetaMap process timeout or stuck"""
    pass

class ServerConnectionError(PyMMError):
    """Server connection failure"""
    def __init__(self, service: str, details: str = ""):
        self.service = service
        self.details = details
        super().__init__(f"{service} connection failed: {details}")

class ParseError(PyMMError):
    """XML/Output parsing failure"""
    def __init__(self, file_path: str, details: str = ""):
        self.file_path = file_path
        self.details = details
        super().__init__(f"Parse error in {file_path}: {details}")

class ConfigurationError(PyMMError):
    """Configuration related errors"""
    pass

class PortBindingError(PyMMError):
    """Port already in use or binding failure"""
    def __init__(self, port: int, details: str = ""):
        self.port = port
        self.details = details
        super().__init__(f"Port {port} binding error: {details}")