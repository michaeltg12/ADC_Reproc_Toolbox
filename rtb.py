#!/data/home/giansiracusa/dev_venv/bin/python
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
from pathlib import Path
import random
import re
import sys
import time
import yaml
from datetime import date
from logging import config, getLogger

# TODO may want to change this to only import specific modules
from config.config import *
from commands.mgmt import *
from commands.stage import *
from commands.rename import *
# from commands.release0 import updateDB

HEADER = """
 ____                          _____           _ _               
|  _ \ ___ _ __  _ __ ___   __|_   _|__   ___ | | |__   _____  __
| |_) / _ \ '_ \| '__/ _ \ / __|| |/ _ \ / _ \| | '_ \ / _ \ \/ /
|  _ <  __/ |_) | | | (_) | (__ | | (_) | (_) | | |_) | (_) >  < 
|_| \_\___| .__/|_|  \___/ \___||_|\___/ \___/|_|_.__/ \___/_/\_\\
          |_| 
"""
main_help = f"""
This is the main cli for the reprocessing toolbox.
Below is a list of main group debug and test options followed by,
a list of possible commands to run. Commands can be chained but 
the options and arguments for each command must be listed in full
before invoking the next command."""

plugin_folder = os.path.join(os.path.dirname(__file__), 'tools')

class Config(object):
    def __init__(self, *args, **kwargs):
        self.debug = kwargs['debug']
        self.help = yaml.load(open('documentation/help.yaml'))
        self.today = today
        # setup logging with a config file and get main reproc_logger
        global_config = yaml.load(open("config/logging_config.yaml"))
        config.dictConfig(global_config['logging'])
        # if debug or test then enable reproc_test_logger from config/logging_config.yaml
        if kwargs['debug'] or kwargs['test']:
            self.reproc_logger = getLogger("reproc_test_logger")
        else:
            self.reproc_logger = getLogger("reproc_logger")
        # if test then set custom staging location from config/config.py
        self.reproc_home = reproc_home if not kwargs['test'] else test_reproc_home
        self.post_proc = post_proc if not kwargs['test'] else test_post_proc
        self.reproc_logger.debug(f'-- Testing --\n\tREPROC_HOME={test_reproc_home}\n\tPOST_PROC={test_post_proc}')

"""
Example of using command nesting to group commands.
This doesn't support chaining and is maybe too verbose to be useful
"""
# @click.group()
# @click.version_option(version=version)
# @click.option('--debug', is_flag=True, help='Print debug messages to console. This argument must be before commands. '
#                                           'ex: rtb -D setup init.')
# @click.option('--test', is_flag=True, count=True, help='Change project environment to directory set in config.py file.')
# @click.pass_context
# def main(ctx, *args, **kwargs):
#     """ Main cli entry point """
#     print('main args: {}\n\tkwargs: {}'.format(args, kwargs)) # TODO remove
#     ctx.obj = Config(debug=kwargs['debug'], test=kwargs['test'])
#     ctx.obj.reproc_logger.debug(f'-- Debug Mode --{c}')
#     pass
#
# @main.group(help='Setup module and updating support programs.')
# def setup():
#     """ Setup module entry point """
#
# @main.group(help='Staging module and staging support programs.')
# def staging():
#     """ Staging module entry point """
#
# @main.group(help='main group for all rename commands.')
# def rename_group():
#     """ Rename module entry point """
#
# @click.command(help='IDK if this will work. Whoa! It works!')
# def tools():
#     """ Plugin Manager entry point """
#     cli()
#
# setup.add_command(init)
# staging.add_command(stage)
# staging.add_command(get_filelist)
# rename_group.add_command(rename)
# rename_group.add_command(printStuff)
# # main.add_command(release)
# main.add_command(tools)

@click.group(chain=True, invoke_without_command=True, help=main_help)
@click.pass_context
@click.option('--debug', '-D', is_flag=True, help='Print debug messages to console. Invoke before all commands.')
@click.option('--test', is_flag=True, help='Change project environment to directory set in '
                                           'config.py file and enable debug mode.')
@click.version_option(version=version)
def main(ctx, *args, **kwargs):
    ctx.obj = Config(debug=kwargs['debug'], test=kwargs['test'])
    pass

@click.command(help='Whoa! It works!')
def tools():
    """ Plugin Manager entry point """

main.add_command(init)
main.add_command(stage)
main.add_command(get_filelist)
main.add_command(rename)
main.add_command(printStuff)
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
    print('{}\n{}'.format(c,HEADER))
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



