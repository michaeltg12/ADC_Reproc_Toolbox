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

from commands import stage
from commands import rename, printStuff
# from commands.release0 import updateDB

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
global TODAY

# setup logging with a config file and get main reproc_logger
global_config = yaml.load(open(".config/logging_config.yaml"))
config.dictConfig(global_config['logging'])
reproc_logger = getLogger("reproc_logger")

plugin_folder = os.path.join(os.path.dirname(__file__), 'tools')

class Config(object):
    def __init__(self, debug, *args, **kwargs):
        self.debug = debug
        self.help = yaml.load(open('documentation/help.yaml'))
        self.dqr_regex = re.compile(r"D\d{6}(\.)*(\d)*")
        self.datastream_regex = re.compile(r"(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|sbs|shb|"
                                      r"tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|nim|oli|osi|pvc|"
                                      r"rld|sgp|smt|twp|yeu)\w+\.(\w){2}")
        self.reproc_home = os.environ.get('REPROC_HOME')
        self.today = int(date.fromtimestamp(time.time()).strftime("%Y%m%d"))

@click.group()
@click.option('--debug', '-D', help='Enable debug messages.')
@click.pass_context
def main(ctx, debug):
    ctx.obj = Config(debug)
    pass

@main.group(help='main group for all rename commands.')
def rename_group():
    click.echo('rename group')

@click.command(help='IDK if this will work. Whoa! It works!')
def tools():
    reproc_logger.info('Add some helpful description here! Boom!')
    cli()

main.add_command(stage)
rename_group.add_command(rename)
rename_group.add_command(printStuff)
# main.add_command(release)
main.add_command(tools)

class MyCLI(click.MultiCommand):
    '''Main CLI for the Reprocessing toolbox'''

    def list_commands(self, ctx):
        """Return list of commands in section."""
        rv = []
        for filename in os.listdir(plugin_folder):
            if filename.endswith('.py'):
                rv.append(filename[:-3])
        rv.sort()
        rv.insert(0, 'help')
        return rv

    def get_command(self, ctx, name):
        reproc_logger.info('name = {}'.format(name))
        ns = {}
        fn = os.path.join(plugin_folder, name + '.py')
        try:
            with open(fn) as f:
                code = compile(f.read(), fn, 'exec')
                eval(code, ns, ns)
        except FileNotFoundError:
            reproc_logger.warning('Available commands = {}'.format(self.list_commands(ctx)))
            reproc_logger.warning(self.help)
            raise click.UsageError('Invalid command: {}'.format(name))
        try:
            return ns['cli']
        except KeyError:
            reproc_logger.warning('Available commands = {}'.format(self.list_commands(ctx)))
            reproc_logger.warning(self.help)
            raise click.UsageError('Invalid command: {}'.format(name))

cli = MyCLI(help='This tool\'s subcommands are loaded from a plugin folder dynamically.')

if __name__ == '__main__':
    print(sys.argv[:])
    if len(sys.argv[:]) == 1:
        sys.argv.append('--help')
    if sys.argv[1] == 'tools':
        sys.argv.remove('tools')
        cli()
    else:
        main()



