"""Console script for wflogger."""

__author__ = """Ag Stephens"""
__contact__ = 'ag.stephens@stfc.ac.uk'
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"

import sys
import os
import json
import click
from click import ClickException


@click.group()
def main():
    """Console script."""
    #click.echo("You are running the 'wflogger' command-line.")
    return 0


@main.command()


if __name__ == "__main__":

    sys.exit(main())  # pragma: no cover
