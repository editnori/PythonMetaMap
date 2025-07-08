"""Enable running pymm as a module: python -m pymm"""
from .cli.main import cli

if __name__ == "__main__":
    cli() 