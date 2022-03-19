"""Console script for wflogger."""

__author__ = """Ag Stephens"""
__contact__ = 'ag.stephens@stfc.ac.uk'
__copyright__ = "Copyright 2020 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"

import os
import sys
import datetime as dt
from dateutil import parser

import click
import psycopg2

from .wflogger import insert_record, DEFAULT_ITERATION, DEFAULT_FLAG
from .credentials import creds, user_id, hostname


@click.group()
def main():
    """Console script."""
    #click.echo("You are running the 'wflogger' command-line.")
    return 0


@main.command()
@click.argument("workflow")
@click.argument("tag")
@click.argument("stage_number")
@click.argument("stage")
@click.argument("iteration")
@click.option("-d", "--date-time", default=None)
@click.option("-c", "--comment", default="")
@click.option("-f", "--flag", default=DEFAULT_FLAG)
def log(workflow, tag, stage_number, stage, iteration=0, date_time=None, comment="", flag=DEFAULT_FLAG):
    insert_record(workflow, tag, stage_number, stage, iteration, date_time, comment, flag)


if __name__ == "__main__":

    sys.exit(main())  # pragma: no cover

