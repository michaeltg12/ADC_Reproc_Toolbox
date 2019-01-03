#!/Users/ofg/projects/Reproc_Toolbox/venv/bin/python
### #!/apps/base/python3/bin/python3

"""
Author: Michael Giansiracusa
Email: giansiracumt@ornl.gov

Purpose:
    This module is a wrapper and manager for executing the steps in a
    reprocessing task and provides access to various utility tools.

Arguments
    required arguments
        job : str
            A DQR for this job. The reprocessing team has agreed on using the
            DQR # as the primary job name and several of the tools require it
            to operate but if the situation arises where the job does not have
            a DQR # then another job name can be used.
    commands
        auto
        stage
        rename
        process
        review
        remove
        archive
        cleanup

    optional arguments

Output:
    This script is the main workflow and tool manager for ADC Reprocessing.
    This tool will manage different tools and workflows for the reprocessing
    team and will provide help and guidance for each tool or workflow.
"""

import click
import os
import re
import sys
import time
import unittest
import yaml
from datetime import date
from logging import config, getLogger
# from support.interactive import Interactive

HEADER = '''
 ____                             _____           _ _
|  _ \ ___ _ __  _ __ ___   ___  |_   _|__   ___ | | |__   _____  __
| |_) / _ \ '_ \| '__/ _ \ / __|   | |/ _ \ / _ \| | '_ \ / _ \ \/ /
|  _ <  __/ |_) | | | (_) | (__    | | (_) | (_) | | |_) | (_) >  <
|_| \_\___| .__/|_|  \___/ \___|   |_|\___/ \___/|_|_.__/ \___/_/\_\\
'''

global DATASTREAM_REGEX
global DQR_REGEX
global REPROC_HOME
global MAX_TRIES
global reproc_logger
global TODAY

HELP = yaml.load(open('documentation/help.yaml'))
DQR_REGEX = re.compile(r"D\d{6}(\.)*(\d)*")
DATASTREAM_REGEX = re.compile(r"(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|sbs|shb|"
                              r"tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|nim|oli|osi|pvc|"
                              r"rld|sgp|smt|twp|yeu)\w+\.(\w){2}")
REPROC_HOME = os.environ.get('REPROC_HOME')
PLUGIN_FOLDER = os.path.join(os.path.dirname(__file__), 'tools')
MAX_TRIES = 3
TODAY = int(date.fromtimestamp(time.time()).strftime("%Y%m%d"))


# setup logging with a config file and get main reproc_logger
global_config = yaml.load(open(".config/logging_config.yaml"))
config.dictConfig(global_config['logging'])
reproc_logger = getLogger("reproc_logger")

@click.group()
def entry_point():
    pass

if __name__ == "__main__":
    entry_point()
