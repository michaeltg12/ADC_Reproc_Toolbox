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
import logging
import os
import re
import sys
import time
import unittest
import yaml
from datetime import date
from logging import config, getLogger

# TODO may want to change this to only import specific modules
from config.config import *
from commands.stage import *
from commands.rename import *
# from commands.release0 import updateDB

HEADER = '''
 ____                             _____           _ _
|  _ \ ___ _ __  _ __ ___   ___  |_   _|__   ___ | | |__   _____  __
| |_) / _ \ '_ \| '__/ _ \ / __|   | |/ _ \ / _ \| | '_ \ / _ \ \/ /
|  _ <  __/ |_) | | | (_) | (__    | | (_) | (_) | | |_) | (_) >  <
|_| \_\___| .__/|_|  \___/ \___|   |_|\___/ \___/|_|_.__/ \___/_/\_\\
'''

plugin_folder = os.path.join(os.path.dirname(__file__), 'tools')

class Config(object):
    def __init__(self, debug, *args, **kwargs):
        self.debug = debug
        self.help = yaml.load(open('documentation/help.yaml'))
        self.dqr_regex = dqr_regex
        self.datastream_regex = datastream_regex
        self.reproc_home = reproc_home
        self.today = today
        # setup logging with a config file and get main reproc_logger
        global_config = yaml.load(open("config/logging_config.yaml"))
        config.dictConfig(global_config['logging'])
        self.reproc_logger = getLogger("reproc_logger")
        if self.debug:
            self.reproc_logger.setLevel(logging.DEBUG)

@click.group()
@click.version_option(version=version)
@click.option('--debug', '-D', help='Print debug messages to console.')
@click.pass_context
def main(ctx, debug):
    
    """ Main cli entry point """
    reproc_logger.debug('debug logging enabled.')
    ctx.obj = Config(debug)
    pass

@main.group(help='Staging module and staging support programs')
def staging():
    """ Staging module entry point """

@main.group(help='main group for all rename commands.')
def rename_group():
    """ Rename module entry point """

@click.command(help='IDK if this will work. Whoa! It works!')
def tools():
    """ Plugin Manager entry point """
    cli()

staging.add_command(stage)
rename_group.add_command(rename)
rename_group.add_command(printStuff)
# main.add_command(release)
main.add_command(tools)

class PluginManager(click.MultiCommand):
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

cli = PluginManager(help='This tool\'s subcommands are loaded from a plugin folder dynamically.')

if __name__ == '__main__':
    """ This is done so that the debug flag can be anywhere in the
    command line and won't mess up the argument list. It doesn't 
    work unless this specific logger gets passed around which might
    be the way it has to be. """
    if '-D' in sys.argv:
        reproc_logger.setLevel(logging.DEBUG)
        try: sys.argv.remove('-D')
        except: pass
        try: sys.argv.remove('--debug')
        except: pass

    """ This is done so that when the user provides no arguments, 
    the help message is displayed. """
    if len(sys.argv[:]) == 1:
        sys.argv.append('--help')

    """ This is so the tools custom plugin manager doesn't get 
    confused with the extra argument when creating the plugin 
    manager object. """
    if sys.argv[1] == 'tools':
        sys.argv.remove('tools')
        cli()
    else:
        main()



