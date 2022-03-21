# wflogger - Workflow logging utilities

Workflow logging utilities (for JASMIN and LOTUS)

* Free software: BSD - see LICENCE file in top-level package directory

## Installation

Simple installation using a virtual environment (`venv`):

```
mkdir work
cd work/

git clone https://github.com/cedadev/wflogger
cd wflogger

python -m venv venv --system-site-packages
source venv/bin/activate
pip install -r requirements.txt

python setup.py develop
``` 

## How it works


### Command-line

From the command-line:

```
```

### Running on JASMIN

Assuming you have your credentials file set up correctly, you can log 
each event with the following command-line script:

```
/apps/jasmin/community/wflogger/wflog
```

The `help` information shows which options are required:

```
$ /apps/jasmin/community/wflogger/wflog --help
Usage: wflogger log [OPTIONS] WORKFLOW TAG STAGE_NUMBER STAGE ITERATION

Options:
  -d, --date-time TEXT
  -c, --comment TEXT
  -f, --flag INTEGER
  --help                Show this message and exit.

```

A typical example would be to run with the following inputs:
- workflow: `my-sat-processor`
- tag: `v1.0`
- stage_number: `1`
- stage: `start`
- iteration: `1`

The command-line incantation to run this would be:

```
/apps/jasmin/community/wflogger/wflog my-sat-processor v1.0 1 start 1
```

(No output is shown if the script runs successfully.)


