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
from datetime import datetime


@click.command()
@click.option(
    '--locations',
    type=click.Path(exists=False, file_okay=True, dir_okay=False, path_type=Path),
    required=True,
    help='Path of the public_restrooms location file'
)
@click.option(
    '--output',
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
    required=True,
    help='Path to store the outputs to (route description, map, and diags)'
)
@click.option(
    '--optimize/--no-optimize',
    default=False,
    help='Enable or disable starting point optimization (default: disabled)'
)
def main(output, locations, optimize):
    """
    Path Finder - A tool for path finding analysis

    output: Directory where  files will be written
    locations: the locations file for the public restrooms
    optimize: whether to run starting point optimization
    """
    # Create output directory if it doesn't exist
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = output / timestamp
    output.mkdir(parents=True, exist_ok=True)

    click.echo(f"✓ Path finder initialized")
    click.echo(f"✓ Public Restrooms File: {locations}")
    click.echo(f"✓ Output directory: {output}")
    click.echo(f"✓ Starting point optimization: {'enabled' if optimize else 'disabled'}")

    harness = PathFinderHarness(locations, output)
    # Pass optimization flag through to the harness run method
    if optimize:
        harness.optimize_starting_point()
    else:
        harness.run()
    
    click.echo("✓ Path finder completed successfully")


if __name__ == '__main__':
    main()
