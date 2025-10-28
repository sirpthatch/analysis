#!/usr/bin/env python3
"""
path_finder.py - A command-line tool for path finding analysis

This script provides command-line interface for path finding operations
with diagnostic output capabilities.
"""

import click
from pathlib import Path
import sys
from path_finder import PathFinderHarness


@click.command()
@click.option(
    '--locations',
    type=click.Path(exists=False, file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    help='Path of the public_restrooms location file'
)
@click.option(
    '--diags',
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help='Directory for diagnostic output files'
)
def main(diags, locations):
    """
    Path Finder - A tool for path finding analysis

    DIAGS: Directory where diagnostic files will be written
    """
    # Create diagnostics directory if it doesn't exist
    diags.mkdir(parents=True, exist_ok=True)

    click.echo(f"✓ Path finder initialized")
    click.echo(f"✓ Public Restrooms File: {locations}")
    click.echo(f"✓ Diagnostics directory: {diags}")

    harness = PathFinderHarness(locations)
    harness.run()

    click.echo("✓ Path finder completed successfully")


if __name__ == '__main__':
    main()
