#!/Users/ofg/PycharmProjects/ADC_Reproc_Toolbox/venv/bin/python
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

import argparse
import logging
import os
import re
import sys
import time
import unittest
import yaml
from datetime import date
from support.interactive import Interactive

header = '''
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

DQR_REGEX = re.compile(r"D\d{6}(\.)*(\d)*")
DATASTREAM_REGEX = re.compile(r"(acx|awr|dmf|fkb|gec|hfe|mag|mar|mlo|nic|nsa|osc|pgh|pye|sbs|shb|"
                              r"tmp|wbu|zrh|asi|cjc|ena|gan|grw|isp|mao|mcq|nac|nim|oli|osi|pvc|"
                              r"rld|sgp|smt|twp|yeu)\w+\.(\w){2}")
REPROC_HOME = os.environ.get('REPROC_HOME')
MAX_TRIES = 3
TODAY = int(date.fromtimestamp(time.time()).strftime("%Y%m%d"))

# setup logging with a config file and get main reproc_logger
global_config = yaml.load(open(".config/logging_config.yaml"))
logging.config.dictConfig(global_config['logging'])
reproc_logger = logging.getLogger("reproc_logger")

class ReprocTB(object):

    def __init__(self):
        reproc_logger.info(header)

        self.args = self.parseArgs()
        self.args.func()

    def parseArgs(self):
        # print help message if no arguments passed
        if len(sys.argv) == 1:
            sys.argv.append('-h')

        # define some general help descriptions
        description = '''
        Reprocessing Toolbox Manager:
        This toolbox helps manage the different parts of the reprocessing workflow. 
        It also contains functionality to run the various utility tools that have 
        been developed to facilitate reprocessing.
        Supported commands are:
            auto       Run full workflow in auto mode
            stage      Stage data
            email      Send reprocess completed email to arm users
        '''
        usage = '''rtb <command> [<args>]'''
        example = """ EXAMPLE: rtb stage -j D123456.1 """

        parser = argparse.ArgumentParser(prog='Reproc Toolbox', description=description, usage=usage, epilog=example)
        parser.add_argument('-v', '--version', action='version', version=f'%(prog)s {apm.__version__}')
        subparsers = parser.add_subparsers(help='Sub-parser help')

        parser_test = subparsers.add_parser('test')
        parser_test.set_defaults(func=self.test)

        parser_vapinfo = subparsers.add_parser('vapinfo')
        parser_vapinfo.set_defaults(func=self.vapinfo)

        parser_auto = subparsers.add_parser('auto')
        parser_auto.add_argument('-j', required=True, dest='job_name', help='Job name, preferable a dqr number.')
        parser_auto.set_defaults(func=self.auto)

        parser_stage = subparsers.add_parser('stage')
        # parser_stage.add_argument('-j', dest='job',
        #                           help='DQR number. Can be another name for the job but alternate job names '
        #                                'will break some functionality.')
        # parser_stage.add_argument('-I', '--interactive', dest='interactive', action='store_true',
        #                           help='stage in interactive mode.')
        parser_stage.set_defaults(func=self.stage)

        parser_interactive = subparsers.add_parser('interactive')
        parser_interactive.set_defaults(func=self.interactive_mode)

        return parser.parse_args()

    def test(self):
        # TODO not fully implemented
        reproc_logger.info('Testing')
        test_config = test.config()
        reproc_logger.info(self.args)
        reproc_logger.info(jprint(test_config, sort_keys=True, indent=4))
        unittest.main(buffer=True)
        exit(0)

    def vapinfo(self):
        vap = VapMgr({})
        vaps = vap.vap_info()
        reproc_logger.info("\nAvailable vaps --\n-- {}".format("\n-- ".join(vaps)))
        exit(0)

    def auto(self):
        reproc_logger.info('Auto Workflow')

    def stage(self):
        reproc_logger.info('Staging')
        is_dqr = self.interactive_mode([
            {
                'type': 'confirm',
                'name': 'reprocessing',
                'message': 'Is this a DQR reprocesssing job?',
                'default': False
            }
        ])
        if is_dqr:
            self.job_name = self.interactive_mode([
                {
                    'type':'input',
                    'name':'job_name',
                    'message':'Enter a DQR# or job name.',
                    'validate':DQRNumberValidator
                }
            ])
        else:
            self.job_name = self.interactive_mode([
                {
                    'type': 'input',
                    'name': 'job_name',
                    'message': 'Enter a DQR# or job name.'
                }
            ])
        self.datastream = self.interactive_mode(
            {})

    def interactive_mode(self, questions):
        IM = InteractiveMode()
        answer = IM.interact(questions)
        reproc_logger.info(answer)


if __name__ == '__main__':
    ReprocTB()