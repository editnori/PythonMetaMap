#!/usr/bin/env python3
"""Simplified main entry point without scipy/analysis dependencies"""
import os
import sys

# Block scipy modules before any imports
sys.modules['scipy'] = None
sys.modules['scipy.stats'] = None
sys.modules['scipy.special'] = None
sys.modules['scipy.spatial'] = None
sys.modules['seaborn'] = None

# Disable analysis features
os.environ['PYMM_NO_SCIPY'] = '1'
os.environ['PYMM_NO_ANALYSIS'] = '1'
os.environ['MPLBACKEND'] = 'Agg'

def main():
    """Main entry point for simplified CLI"""
    import click
    from pymm.cli.run import run_cmd
    from pymm.cli.check import check_cmd
    from pymm.cli.install import install_cmd
    from pymm.cli.config import config_cmd
    from pymm.cli.clean import clean_cmd
    from pymm.cli.server import server_cmd
    from pymm.cli.batch import batch_cmd
    from pymm.cli.interactive import interactive_cmd
    from pymm.cli.version import version_cmd
    
    @click.group()
    @click.pass_context
    def cli(ctx):
        """Python MetaMap CLI - Advanced NLP Processing Tool"""
        ctx.ensure_object(dict)
    
    # Register commands
    cli.add_command(run_cmd, name='run')
    cli.add_command(check_cmd, name='check')
    cli.add_command(install_cmd, name='install')
    cli.add_command(config_cmd, name='config')
    cli.add_command(clean_cmd, name='clean')
    cli.add_command(server_cmd, name='server')
    cli.add_command(batch_cmd, name='batch')
    cli.add_command(interactive_cmd, name='interactive')
    cli.add_command(version_cmd, name='version')
    
    # Note: analysis and enhanced-analysis commands are not available in simple mode
    
    return cli()

if __name__ == '__main__':
    sys.exit(main())