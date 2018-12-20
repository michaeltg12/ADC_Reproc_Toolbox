#!/apps/base/python3/bin/python3

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

# Ctrl+C Handler
import signal

# System
import sys
import os
from os.path import abspath

# Input Parsing
import argparse
import re

# Time Handling
import time
from datetime import date
from datetime import timedelta

# File handlers
import json
import yaml

# Logging
import logging

# Fix backspace on input
import readline

# Unit Testing
import unittest
from apm.classes import mock

# APM Imports
import apm
from apm import Files
from apm import UI
from apm import VapMgr

from apm import Stage
from apm import Rename
from apm import Process
from apm import Review
from apm import Remove
from apm import Archive
from apm import Cleanup
from apm import Demo

from apm.classes.system import jprint

from apm import test

DQR_REGEX = re.compile(r"D\d{6}(\.)*(\d)*")

global max_tries
global today

max_tries = 3
today = int(date.fromtimestamp(time.time()).strftime("%Y%m%d"))

# TODO FOR DEBUGGING DURRING DEVELOPMENT
from inspect import currentframe, getframeinfo, getouterframes
# print("\n\t*** frameinfo ***\n{}\n".format(getframeinfo(currentframe(), context=2)))
# print("\n\t*** outerframes ***\n{}\n".format(getouterframes(currentframe(), context=2)))

def main():
    # setup logging with a config file and get main reproc_logger
    global_config = yaml.load(open(".config/logging_config.yaml"))
    logging.config.dictConfig(global_config['logging'])
    reproc_logger = logging.getLogger("reproc_logger")

    # if no cmd line args, add -h before parse_args to display help
    if not len(sys.argv) > 1:
        sys.argv.append("-h")

    # printing current version
    if '-v' in sys.argv:
        reproc_logger.info(apm.__version__)
        return

    # Retrieve arguments from user and get command
    config = parse_args()
    command = config['command'].lower()

    # Check to see if this is a test
    if command == 'test':
        test_config = test.config()

        sys.argv = [sys.argv[0]]
        reproc_logger.info(jprint(config, sort_keys=True, indent=4))
        unittest.main(buffer=True)
        # unittest.main()
        return

    # Prints a list of the available vaps
    if command == 'info' or command == 'vapinfo':
        vap = VapMgr({})
        vaps = vap.vap_info()
        reproc_logger.info(" Available vaps --\n\t\t--{}".format("\n-- ".join(vaps)))
        return

    # Validate user arguments
    temp = validate_config(config, command)
    config = temp if temp else config

    if command == 'check':
        jprint(config, sort_keys=True, indent=4)
        return

    # Save the config to file
    s = time.time()
    f = Files(config)
    f.save_config()
    f.load_filenames()
    files = f.files

    # Check to see if any files are not currently being tracked
    # Or if any tracked files have been deleted
    print("Checking status of tracked files...", end=" ")
    sys.stdout.flush()

    json_file = '{0}/{1}/{1}.json'.format(config['stage'], config['job'])
    if os.path.exists(json_file) and config['ingest']:
        fp = open(json_file, 'r')
        files = json.loads(fp.read())
        fp.close()

        cwd = os.getcwd()
        os.chdir('{}/{}/collection'.format(config['stage'], config['job']))

        keys = files.keys()

        sites = set(os.listdir('.'))
        for site in keys:
            if site not in sites:
                files.pop(site)
                continue

            os.chdir(site)

            instruments = set(os.listdir('.'))
            ins_keys = files[site].keys()

            for ins in ins_keys:
                if ins not in instruments:
                    files[site].pop(ins)
                    continue

                os.chdir(ins)

                filelist = set(os.listdir('.'))
                for i in filelist:
                    if i not in files[site][ins] and not (os.path.isdir(i) and i == "other_files"):
                        exit("\nThe file {0}/{1}/{2} is currently untracked.\nPlease edit {3}.json to start tracking this file.\n".format(site, ins, i, config['job']))

                for i in files[site][ins]:
                    if i not in filelist:
                        files[site][ins][i]["deleted"] = True

                os.chdir('..')

            os.chdir('..')

        os.chdir(cwd)
    print("Done") # Done checking status of tracked files
    sys.stdout.flush()

    # Run the appropriate command
    if command == 'auto':
        print('Attempting to stage files for datastreams: {}'.format(config['datastream']))

        skip = False

        if not config['duplicates']:
            s = Stage(config, files)
            config, files = s.run()

            if config['exit']:
                exit()

            if config['duplicates']:
                skip = True

        if not skip and not config['vap']:
            r = Rename(config, files)
            config, files = r.run()
            if config['exit']:
                exit()
        exit()

    elif command == 'stage':
        print("*"*50,"\n", json.dumps(config, indent=2), "*"*50, "\n")
        skip = False

        if not config['duplicates']:
            s = Stage(config, files)
            config, files = s.run()

            if config['exit']:
                exit()

            if config['duplicates']:
                skip = True

        if not skip and not config['vap']:
            r = Rename(config, files)
            config, files = r.run()
            if config['exit']:
                exit()

    elif command == 'rename':
        # If rename is called explicitly, force rename even if config is set to false
        switch = True if config['rename'] == False else False

        if switch:
            config['rename'] = True

        if not config['vap']:
            r = Rename(config, files)
            config, files = r.run()
            if config['exit']:
                exit()

        if switch:
            config['rename'] = False

    elif command == 'process':
        r = Rename(config, files)
        has_coll = r.check_for_collisions()
        files = r.files

        if has_coll:
            config = r.config
            files = r.files
        else:
            p = Process(config, files)
            config, files = p.run()
            if config['exit']:
                exit()

    elif command == 'review':
        r = Rename(config, files)
        has_coll = r.check_for_collisions()
        files = r.files

        if has_coll:
            config = r.config
            files = r.files
        else:
            r = Review(config, files)
            config, files = r.run()
            if config['exit']:
                exit()

    elif command == 'remove':
        r = Rename(config, files)
        has_coll = r.check_for_collisions()
        files = r.files

        if has_coll:
            config = r.config
            files = r.files
        else:
            r = Remove(config, files)
            config, files = r.run()
            if config['exit']:
                exit()

    elif command == 'archive':
        r = Rename(config, files)
        has_coll = r.check_for_collisions()
        files = r.files

        if has_coll:
            config = r.config
            files = r.files
        else:
            a = Archive(config, files)
            config, files = a.run()
            if config['exit']:
                exit()

    elif command == 'cleanup':
        r = Rename(config, files)
        has_coll = r.check_for_collisions()
        files = r.files

        if has_coll:
            config = r.config
            files = r.files
        else:
            c = Cleanup(config, files)
            config, files = c.run()
            if config['exit']:
                exit()

    elif command == 'prep':
        r = Rename(config, files)
        has_coll = r.check_for_collisions()
        files = r.files

        if has_coll:
            config = r.config
            files = r.files
        else:
            d = Demo(config, files)
            config, files = d.run()
            if config['exit']:
                exit()

    elif command == "notification":
        # Alka's module goes here
        print('Yay notify the user shit has changed.')

    else:
        sys.argv.append("-h")
        config = parse_args()

    f.config = config
    f.files = files
    f.save_config()
    f.save_filenames()


def parse_args():
    """ Setup argument parsing and parse the arguments """
    username = os.environ.get('USER')

    # Setup parser and groups
    parser = argparse.ArgumentParser(description='ARM Processing Manager')
    ui_flags = parser.add_mutually_exclusive_group()
    stage_type = parser.add_mutually_exclusive_group()

    # Setup positional arguments
    parser.add_argument('command', help='Which of the APM stages to run: '
                                        'auto, stage, rename, process, review, remove, archive, cleanup')

    # Demo options
    parser.add_argument('--demo', help='Prep for different stages of a demo, available options include: remove, archive, cleanup')

    # Date
    parser.add_argument('-b', '--begin', type=int, default=0, help='Format: YYYYMMDD - date to start processing data')
    parser.add_argument('-e', '--end', type=int, default=0, help='Format:YYYYMMDD - date to stop processing data')

    # SIF/Datastreams
    parser.add_argument('-s', '--site', help='The site the data is from')
    parser.add_argument('-i', '--instrument', help='The instrument used to collect the data')
    parser.add_argument('-f', '--facility', help='The facility where the instrument is located')
    parser.add_argument('-d', '--datastream', nargs='+', help='One or more datastream patterns. "%%" and "*" can be used as wildcards.')

    # Job
    parser.add_argument('-j', '--job', required=True, help='DQR # for job')

    # Alias
    parser.add_argument('-a', '--alias', help='An alias for the Ingest to use to connect to the database. Def: apm')

    # Flow control flags
    parser.add_argument('--stage', help='Specify a staging directory')
    parser.add_argument('--source', help='Specify a source directory')
    parser.add_argument('--no-rename', action='store_false', help='Do not strip the ARM prefix from the files')
    parser.add_argument('--no-db-up', action='store_false', help='Do not update the config database')
    parser.add_argument('--no-compare', action='store_false', help='Do not compare the ingest output for re-archiving')

    # Other
    parser.add_argument('--ingest-flags', nargs='+', help='Flags you want APM to pass to the INGEST. Ex. --ingest-flags F (Do not use "-F" APM will add the "-") (Will apply to all ingests if running for multiple datastreams)')

    # Ingest Vs Vap
    stage_type.add_argument('--ingest', action='store_true', help='Ingest vs. VAP (default)')
    stage_type.add_argument('--vap', action='store_true', help='VAP vs. Ingest')

    # UI Flags
    ui_flags.add_argument('-I', '--interactive', action='store_true', help='Prompt for various inputs')
    ui_flags.add_argument('-q', '--quiet', action='store_true', help='Suppresses prompts and exits gracefully if unable to run')
    ui_flags.add_argument('-D', '--devel', action='store_true', help='Run APM in development mode')


    # Parse the args
    arguments = parser.parse_args()

    if (arguments.ingest == False) and (arguments.vap == False):
        arguments.ingest = True

    args = {
        'command': arguments.command,
        'demo': arguments.demo,
        'begin': arguments.begin,
        'end': arguments.end,
        'site': arguments.site,
        'instrument': arguments.instrument,
        'facility': arguments.facility,
        'datastream': arguments.datastream,
        'duplicates': False,
        'job': arguments.job,
        'alias': arguments.alias,
        'stage': arguments.stage,
        'source': arguments.source,
        'rename': arguments.no_rename,
        'db_up': arguments.no_db_up,
        'compare': arguments.no_compare,
        'iflags': arguments.ingest_flags,
        'ingest': arguments.ingest,
        'vap': arguments.vap,
        'interactive': arguments.interactive,
        'quiet': arguments.quiet,
        'devel': arguments.devel,
        'username': username,
        'exit': False,
        "cleanup_status": {
                "review": {
                    "status": True,
                },
                "remove": {
                    "status": False,
                    "deletion_list": False,
                    "archive_list": False,
                    "files_bundled": False,
                },
                "archive": {
                    "status": False,
                    "files_deleted": False,
                    "move_files": False,
                    "files_released": False,
                },
                "cleanup": {
                    "status": False,
                    "files_archived": False,
                    "files_cleaned_up": False,
                },
            }
        }

    f = Files(args)

    if args['stage'] != None:
        temp = f.clean_path(args['stage'])
        args['stage'] = abspath(temp)
    if args['source'] != None:
        temp = f.clean_path(args['source'])
        args['source'] = abspath(temp)

    return args

def validate_config(config, command):
    # files obj has a attribute default which is a generic, empty config
    f = Files(config)

    if command == "auto":
        # check if job is a dqr else we can't use auto staging
        try:
            dqr = DQR_REGEX.search(config['job']).group()
        except AttributeError:
            print("Job")
        temp = f.db_load_config()
    else:
        temp = f.load_config()
    config = temp if temp else config

    config['begin'], config['end'] = check_dates(config)
    config['site'], config['instrument'], config['facility'], config['datastream'] = check_sif_datastream(config)
    config['source'] = check_source(config)
    config['stage'] = check_stage(config)
    config['job'] = check_job(config)

    return config

def check_dates(config):
    begin = int(config['begin'])
    end = int(config['end'])
    interactive = config['interactive']
    quiet = config['quiet']

    if interactive:
        # Prompt for dates
        begin = get_date(begin, 'begin')
        end = get_date(end, 'end')

    # Validate dates
    if (begin == 0) or (end == 0):
        if quiet:
            exit('Valid dates are required')
        else:
            if (begin == 0) and (end != 0):
                print('A begin date is required.')
                begin = get_date(begin, 'begin')
            elif (begin != 0) and (end == 0):
                print('An end date is required.')
                end = get_date(end, 'end')
            else:
                begin = get_date(begin, 'begin')
                end = get_date(end, 'end')
                # if not vap:
                # exit('No dates specified. Please run again and provide begin and/or end dates.')

    if (begin > end) and (end != 0):
        exit('Start date must be the same day or before the end date')
    if begin > today:
        exit('No data for the selected date range.')
    if end > today:
        end = today

    return begin, end


def get_date(date, text, level=1):
    date = date if date > 0 else None
    error = False
    error_msg = "A valid date is required. Please try again{}"
    try:
        print('\nPlease enter a {} date (format: YYYYMMDD)'.format(text))
        temp = input('{}: '.format('00000000' if date == None else str(date)))

        if temp == '':
            if not date:
                error = True
        else:
            try:
                date = int(temp)
            except TypeError:
                return None

        if error:
            if level < max_tries:
                print(error_msg.format(": "))
                date = get_date(date, text, level=level+1)
            else:
                exit(error_msg.format("."))

    except ValueError:
        error_msg = 'A numberic value is required for dates. Please try again{}'
        if level < max_tries:
            print(error_msg.format(": "))
            date = get_date(date, text, level=level+1)
        else:
            exit(error_msg.format("."))

    return date


def check_sif_datastream(config):
    site = config['site']
    instrument = config['instrument']
    facility = config['facility']
    datastream = config['datastream']
    interactive = config['interactive']
    quiet = config['quiet']
    vap = config['vap']

    if datastream == None or not (type(datastream) == list and len(datastream) > 0) or vap == True:
        datastream = None
        if quiet and (instrument == None or (vap == False and (site == None or facility == None or len(site) != 3))):
            exit("There was an error retrieving your site, instrument, facility or datastream information. Please try again.")
        if interactive:
            site, instrument, facility = ask_sif(s=site, i=instrument, f=facility, vap=vap)
        if site == None:
            site = ask_sif(s=site, vap=vap)[0]
        elif (len(site) != 3) and not (vap and site == ''):
            print("ERROR: Site abbreviations are 3 characters in length. Please try again.")
            site = ask_sif(s=site, vap=vap)[0]
        if instrument == None:
            instrument = ask_sif(i=instrument)[1]
        if facility == None:
            facility = ask_sif(f=facility, vap=vap)[2]
    else:
        site = None
        instrument = None
        facility = None

    return site, instrument, facility, datastream

def ask_sif(s=False, i=False, f=False, level=1, vap=False):
    """ Ask for site instrument and facility data """
    if s != False:
        temp = input("\nPlease enter the three character site abbreviation you would like to use: ")
        if temp == '' and not vap:
            if s == None or s == True:
                if level < max_tries:
                    s = ask_sif(s=s, level=level+1)[0]
                else:
                    exit("ERROR: Site abbreviations are 3 characters in length. Please try again.")
            else: # Use default value
                temp = s

        if len(temp) == 3 or (temp == '' and vap):
            s = temp
        else:
            print("ERROR: Site abbreviations are 3 characters in length. Please try again.")
            s = ask_sif(s=s, level=level+1)[0]

    if i != False:
        temp = input("\nPlease enter the name of the instrument/process you would like to use: ")
        if temp == '':
            if i == None or i == True:
                if level < max_tries:
                    i = ask_sif(i=i, level=level+1)[1]
                else:
                    exit("ERROR: An instrument/process must be specified. Please try again.")
        else:
            i = temp

    if f != False:
        temp = input("\nPlease enter the facility you would like to use: ")
        if temp == '' and not vap:
            if f == None or f == True:
                if level < max_tries:
                    f = ask_sif(f=f, level=level+1)[2]
                else:
                    exit("ERROR: A facility must be specified. Please try again.")
        elif temp == '' and vap:
            f = temp
        else:
            f = temp

    return s, i, f

def check_source(config, level=1):
    source = config['source']
    interactive = config['interactive']
    quiet = config['quiet']

    if source == None:
        source = '/data/archive'

    if interactive:
        message = "Specify a source directory: ({})".format(source)
        error = 'A source directory must be specified. Please try again.'
        default = source
        source = ask_for_dir(message, error=error, default=default, required=True)

    if not os.path.exists(source):
        if level < max_tries and not quiet:
            message = '{} does not exists.\nPlease specify and existing source directory: '.format(source)
            error = 'A source directory must be specified. Please try again.'
            config['source'] = ask_for_dir(message, error=error, required=True)
            source = check_source(config, level+1)
        else:
            exit('{} does not exists. Please try again and specify and existing source directory.'.format(source))

    if len(os.listdir(source)) == 0:
        if level < max_tries and not quiet:
            message = '{} is not readable or contains no data.\nPlease specify a valid source directory: '.format(source)
            error = 'A source directory must be specified. Please try again.'
            config['source'] = ask_for_dir(message, error=error, required=True)
            source = check_source(config, level+1)
        else:
            exit('{} is not readable or contains no data. Please try again and specify a valid source directory.'.format(source))

    return source

def check_stage(config, level=1):
    f = Files(config)
    stage = config['stage']
    interactive = config['interactive']
    quiet = config['quiet']

    if stage == None:
        reproc_home = os.environ.get('REPROC_HOME')
        home = os.environ.get('HOME')
        if reproc_home != None:
            stage = reproc_home
        elif home.split('/')[1] == 'data':
            stage = '{}/reprocessing/data'.format(home)
        else:
            stage = '/data/home/{}/reprocessing/data'.format(config['username'])
    if interactive:
        message = "Specify a stage directory: ({})".format(stage)
        error = 'A stage directory must be specified. Please try again.'
        default = stage
        stage = ask_for_dir(message, error=error, default=default, required=True)

    if not os.path.exists(stage):
        try:
            os.makedirs(stage)
        except:
            if level < max_tries and not quiet:
                print('Unable to create {}'.format(stage))
                message = 'Please specify a new location: '
                error = 'A stage directory must be specified. Please try again.'
                config['stage'] = ask_for_dir(message, error=error, required=True)
                stage = check_stage(config, level+1)
            else:
                exit('Unable to create {}. Please try again.'.format(stage))

    if not f.is_dir_writable(stage):
        if level < max_tries and not quiet:
            print('{} is not writable.'.format(stage))
            message = 'Please specify a new location: '
            error = 'A stage directory must be specified. Please try again.'
            config['stage'] = ask_for_dir(message, error=error, required=True)
            stage = check_stage(config, level+1)
        else:
            exit('{} is not writable. Please try again.'.format(stage))

    return stage

def ask_for_dir(message, default=None, error=None, required=False, level=1):
    """ Ask user for a directory location """
    f = Files({})
    folder = input(message)
    if folder == '':
        if default != None:
            folder = default
        elif required:
            if level < max_tries:
                folder = ask_for_dir(message, default=default, error=error, required=required, level=level+1)
            else:
                exit(error)
        else:
            return

    folder = abspath(f.clean_path(folder))
    return folder

def check_job(config):
    job = config['job']
    interactive = config['interactive']

    if interactive:
        message = 'Please specify a job name: '
        if job:
            message = '{0}({1})'.format(message, job)
        temp = input(message)
        if temp != '':
            job = temp

    if not job:
        import uuid
        uid = str(uuid.uuid1())
        uid = uid.split('-')
        job = '{0}{1}'.format(uid[0], uid[3])

    config['job'] = job
    f = Files(config)
    f.setup_job_dir()

    return job

################################################################################
# Unittest Test Cases
################################################################################
class TestCheckDates(unittest.TestCase):
    """ Check the check_dates and get_date functions """
    def setUp(self):
        today = date.fromtimestamp(time.time())
        self.config = test.config()
        self.begin = self.config['begin']
        self.end = self.config['end']
        self.today = int(today.strftime("%Y%m%d"))
        self.earlier = int((today - timedelta(days=3)).strftime("%Y%m%d"))
        self.later = int((today + timedelta(days=3)).strftime("%Y%m%d"))
        self.evenLater = int((today + timedelta(days=6)).strftime("%Y%m%d"))
        self.message = "Please enter a date (YYYYMMDD): "

    ################################################
    # Test Get Date
    ################################################
    def test_1(self):
        """ No input -> FAIL """
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            with self.assertRaises(SystemExit):
                get_date(0, self.message)

    def test_2(self):
        """ Non-numeric input -> FAIL """
        with mock.patch('__builtin__.input', side_effect=['asdf', 'asdf', 'asdf']):
            with self.assertRaises(SystemExit):
                get_date(0, self.message)

    def test_3(self):
        """ 'None' -> 'non-numberic' -> 'valid date' -> PASS """
        with mock.patch('__builtin__.input', side_effect=['', 'asdf', self.today]):
            result = get_date(0, self.message)
            expected = self.today

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_4(self):
        """ Valid date -> PASS """
        with mock.patch('__builtin__.input', side_effect=[self.today]):
            result = get_date(0, self.message)
            expected = self.today

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_5(self):
        """ 'None' -> PASS """
        with mock.patch('__builtin__.input', side_effect=['']):
            result = get_date(self.today, self.message)
            expected = self.today

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    ################################################
    # Test Check Dates
    ################################################
    # Interactive
    def test_6(self):
        """ Ask for dates even with dates given """
        self.config['interactive'] = True
        with mock.patch('__builtin__.input', side_effect=['', '']):
            result = check_dates(self.config)
            expected = (self.begin, self.end)

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_7(self):
        """ Supply new dates even though dates already provided """
        self.config['interactive'] = True
        with mock.patch('__builtin__.input', side_effect=[self.earlier, self.today]):
            result = check_dates(self.config)
            expected = (self.earlier, self.today)

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_8(self):
        """ No supplied dates, No passed dates -> FAIL """
        self.config['interactive'] = True
        self.config['begin'] = 0
        self.config['end'] = 0
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            with self.assertRaises(SystemExit):
                check_dates(self.config)

    def test_9(self):
        """ Supply begin date, test end date -> FAIL """
        self.config['interactive'] = True
        self.config['begin'] = 0
        self.config['end'] = 0
        with mock.patch('__builtin__.input', side_effect=[self.today, '', '', '']):
            with self.assertRaises(SystemExit):
                check_dates(self.config)

    # Quiet
    def test_10(self):
        """ No begin date -> FAIL """
        self.config['quiet'] = True
        self.config['begin'] = 0
        with self.assertRaises(SystemExit):
            check_dates(self.config)

    def test_11(self):
        """ No end date -> FAIL """
        self.config['quiet'] = True
        self.config['end'] = 0
        with self.assertRaises(SystemExit):
            check_dates(self.config)

    def test_12(self):
        """ No dates -> FAIL """
        self.config['quiet'] = True
        self.config['begin'] = 0
        self.config['end'] = 0
        with self.assertRaises(SystemExit):
            check_dates(self.config)

    # Check for dates
    def test_13(self):
        """ No begin date -> PASS """
        self.config['begin']
        with mock.patch('__builtin__.input', side_effect=[self.begin]):
            result = check_dates(self.config)
            expected = (self.begin, self.end)

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_14(self):
        """ No end date -> PASS """
        self.config['end'] = 0
        with mock.patch('__builtin__.input', side_effect=[self.end]):
            result = check_dates(self.config)
            expected = (self.begin, self.end)

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_15(self):
        """ No dates -> PASS """
        self.config['begin'] = 0
        self.config['end'] = 0
        with mock.patch('__builtin__.input', side_effect=[self.begin, self.end]):
            result = check_dates(self.config)
            expected = (self.begin, self.end)

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    # Validate dates
    def test_16(self):
        """ Begin > End -> Fail """
        self.config['begin'] = self.end
        self.config['end'] = self.begin
        with self.assertRaises(SystemExit):
            check_dates(self.config)

    def test_17(self):
        """ Begin > Today -> FAIL """
        self.config['begin'] = self.later
        self.config['end'] = self.evenLater
        with self.assertRaises(SystemExit):
            check_dates(self.config)

    def test_18(self):
        """ End > Today -> PASS """
        self.config['begin'] = self.today
        self.config['end'] = self.later
        result = check_dates(self.config)
        expected = (self.today, self.today)

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

############################################################
# Test SIF Datastream
############################################################
class TestSIFD(unittest.TestCase):
    """ Test SIF and Datastream validation """
    def setUp(self):
        self.config = test.config()

    ################################################
    # Test Ask SIF
    ################################################
    def test_1(self):
        """ Site -> No Input -> FAIL """
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            with self.assertRaises(SystemExit):
                ask_sif(s=True)

    def test_2(self):
        """ Site -> Too Short Input -> Too Long Input -> No Input -> FAIL """
        with mock.patch('__builtin__.input', side_effect=['as', 'asdf', '']):
            with self.assertRaises(SystemExit):
                ask_sif(s=True)

    def test_3(self):
        """ Site -> Good Input -> PASS """
        with mock.patch('__builtin__.input', side_effect=[self.config['site']]):
            result = ask_sif(s=None)
            expected = (self.config['site'], False, False)

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_4(self):
        """ Instrument -> No Input -> FAIL """
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            with self.assertRaises(SystemExit):
                ask_sif(i=True)

    def test_5(self):
        """ Instrument -> Good Input -> PASS """
        with mock.patch('__builtin__.input', side_effect=[self.config['instrument']]):
            result = ask_sif(i=None)
            expected = (False, self.config['instrument'], False)

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_6(self):
        """ Facility -> No Input -> FAIL """
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            with self.assertRaises(SystemExit):
                ask_sif(f=True)

    def test_7(self):
        """ Facility -> Good Input -> PASS """
        with mock.patch('__builtin__.input', side_effect=[self.config['facility']]):
            result = ask_sif(f=None)
            expected = (False, False, self.config['facility'])

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_8(self):
        """ All -> Good Input -> PASS """
        with mock.patch('__builtin__.input', side_effect=[self.config['site'], self.config['instrument'], self.config['facility']]):
            result = ask_sif(s=None, i=None, f=None)
            expected = (self.config['site'], self.config['instrument'], self.config['facility'])

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_9(self):
        """ VAP -> Site -> No Input -> PASS """
        with mock.patch('__builtin__.input', side_effect=['']):
            result = ask_sif(s=None, vap=True)[0]
            expected = ''

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_10(self):
        """ VAP -> Facility -> No Input -> PASS """
        with mock.patch('__builtin__.input', side_effect=['']):
            result = ask_sif(f=None, vap=True)[2]
            expected = ''

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    ################################################
    # Test Check SIF Datastream
    ################################################
    def test_11(self):
        """ Datastream and SIF -> Return Datastream only -> PASS """
        self.config['datastream'] = ['sgpmfrsrC1.00', 'nsairtC1.00']
        result = check_sif_datastream(self.config)
        expected = (None, None, None, self.config['datastream'])

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_12(self):
        """ Datastream -> Return Datastream -> PASS """
        self.config['datastream'] = ['sgpmfrsrC1.00', 'nsairtC1.00']
        self.config['site'] = None
        self.config['instrument'] = None
        self.config['facility'] = None

        result = check_sif_datastream(self.config)
        expected = (None, None, None, self.config['datastream'])

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_13(self):
        """ SIF -> PASS """
        result = check_sif_datastream(self.config)
        expected = (self.config['site'], self.config['instrument'], self.config['facility'], None)

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_14(self):
        """ SIF -> Interactive -> Ask for all 3 -> PASS """
        self.config['interactive'] = True
        with mock.patch('__builtin__.input', side_effect=[self.config['site'], self.config['instrument'], self.config['facility']]):
            result = check_sif_datastream(self.config)
            expected = (self.config['site'], self.config['instrument'], self.config['facility'], None)

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_15(self):
        """ SIF -> Quiet -> PASS """
        self.config['quiet'] = False
        result = check_sif_datastream(self.config)
        expected = (self.config['site'], self.config['instrument'], self.config['facility'], None)

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_16(self):
        """ SIF -> Bad Site -> Quiet -> FAIL """
        self.config['quiet'] = True
        self.config['site'] = 'asdf'
        with self.assertRaises(SystemExit):
            check_sif_datastream(self.config)

    def test_17(self):
        """ SIF -> No Site -> Quiet -> FAIL """
        self.config['quiet'] = True
        self.config['site'] = None
        with self.assertRaises(SystemExit):
            check_sif_datastream(self.config)

    def test_18(self):
        """ SIF -> No Instrument -> Quiet -> FAIL """
        self.config['quiet'] = True
        self.config['instrument'] = None
        with self.assertRaises(SystemExit):
            check_sif_datastream(self.config)

    def test_19(self):
        """ SIF -> No Facility -> Quiet -> FAIL """
        self.config['quiet'] = True
        self.config['facility'] = None
        with self.assertRaises(SystemExit):
            check_sif_datastream(self.config)

    def test_20(self):
        """ No SIF -> Ask for Data -> PASS """
        result = check_sif_datastream(self.config)
        expected = (self.config['site'], self.config['instrument'], self.config['facility'], None)

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected



############################################################
# Test Source/Stage Dir
############################################################
class TestSourceStage(unittest.TestCase):
    """ Test source and stage directory validation """
    def setUp(self):
        self.stage = None
        self.source = None
        self.config = test.config()
        self.message = "Please enter a directory: "
        self.error = "A directory is required. Please try again."
        self.default = {
            'stage': '/data/home/{0}/.test_apm'.format(self.config['username']),
            'source': '/data/archive'
        }

        self.alt = {
            'stage': {
                'not_exists': '/data/stage',
                'not_writable': '/data/archive',
                'new': '/data/home/{0}/.test_apm/stage'.format(self.config['username'])
            },
            'source': {
                'not_exists': '/data/home/{0}/.test/source'.format(self.config['username']),
                'empty': '/data/home/{0}/.test_apm/empty_source'.format(self.config['username'])
            }
        }

        dirs = [self.default['stage'], self.alt['source']['empty']]
        for i in dirs:
            if not os.path.exists(i):
                os.makedirs(i)


    def tearDown(self):
        f = Files({})
        dirs = [self.stage, self.source, self.alt['source']['empty'], self.default['stage']]
        for i in dirs:
            if not i:
                continue

            if not os.path.exists(i):
                continue

            if not f.is_dir_empty(i):
                f.empty_dir(i)
            os.rmdir(i)

    ################################################
    # Test Ask for Dir
    ################################################
    def test_1(self):
        """ Default -> No input -> PASS """
        self.stage = self.default['stage']
        with mock.patch('__builtin__.input', side_effect=['']):
            result = ask_for_dir(self.message, default=self.default['stage'])
            expected = self.default['stage']

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_2(self):
        """ Required -> No input -> FAIL """
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            with self.assertRaises(SystemExit):
                ask_for_dir(self.message, required=True, error=self.error)

    def test_3(self):
        """ No input -> PASS """
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            result = ask_for_dir(self.message)
            expected = None

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    ################################################
    # Test Check Source
    ################################################
    def test_4(self):
        """ None -> Exists -> Not empty -> PASS """
        result = check_source(self.config)
        expected = self.default['source']

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_5(self):
        """ Dir -> Not exists -> Not empty -> PASS """
        self.config['source'] = self.alt['source']['not_exists']
        with mock.patch('__builtin__.input', side_effect=[self.default['source']]):
            result = check_source(self.config)
            expected = self.default['source']

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_6(self):
        """ Dir -> Not exists -> No input -> FAIL """
        self.config['source'] = self.alt['source']['not_exists']
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            with self.assertRaises(SystemExit):
                check_source(self.config)

    def test_7(self):
        """ Dir -> Exists -> Empty -> FAIL """
        self.config['source'] = self.alt['source']['empty']
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            with self.assertRaises(SystemExit):
                check_source(self.config)

    def test_8(self):
        """ None -> Interactive -> No input -> PASS """
        self.config['interactive'] = True
        with mock.patch('__builtin__.input', side_effect=['']):
            result = check_source(self.config)
            expected = self.default['source']

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_9(self):
        """ Dir -> Interactive -> Dir -> PASS """
        self.config['interactive'] = True
        self.config['source'] = self.alt['source']['not_exists']
        with mock.patch('__builtin__.input', side_effect=[self.default['source']]):
            result = check_source(self.config)
            expected = self.default['source']

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_10(self):
        """ Dir -> Quiet -> Not Exists -> FAIL """
        self.config['quiet'] = True
        self.config['source'] = self.alt['source']['not_exists']
        with self.assertRaises(SystemExit):
            check_source(self.config)

    def test_11(self):
        """ Dir -> Quiet -> Empty -> FAIL """
        self.config['quiet'] = True
        self.config['source'] = self.alt['source']['empty']
        with self.assertRaises(SystemExit):
            check_source(self.config)

    ################################################
    # Test Check Stage
    ################################################
    def test_12(self):
        """ None -> Exists -> Writable -> PASS """
        result = check_stage(self.config)
        expected = '/data/home/{0}/apm'.format(self.config['username'])

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_13(self):
        """ Dir -> Not exists -> Writable -> PASS """
        self.stage = self.alt['stage']['new']
        self.config['stage'] = self.stage
        result = check_stage(self.config)
        expected = self.stage

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_14(self):
        """ Dir -> Not exists -> No input -> FAIL """
        self.config['stage'] = self.alt['stage']['not_exists']
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            with self.assertRaises(SystemExit):
                check_stage(self.config)

    def test_15(self):
        """ Dir -> Exists -> Not Writable -> FAIL """
        self.config['stage'] = self.alt['stage']['not_writable']
        with mock.patch('__builtin__.input', side_effect=['', '', '']):
            with self.assertRaises(SystemExit):
                check_stage(self.config)

    def test_16(self):
        """ None -> Interactive -> No input -> PASS """
        self.config['interactive'] = True
        with mock.patch('__builtin__.input', side_effect=['']):
            result = check_stage(self.config)
            expected = '/data/home/{0}/apm'.format(self.config['username'])

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_17(self):
        """ None -> Interactive -> Dir -> PASS """
        self.stage = self.alt['stage']['new']
        self.config['interactive'] = True
        with mock.patch('__builtin__.input', side_effect=[self.stage]):
            result = check_stage(self.config)
            expected = self.stage

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_18(self):
        """ Dir -> Quiet -> Not exists -> FAIL """
        self.config['quiet'] = True
        self.config['stage'] = self.alt['stage']['not_exists']
        with self.assertRaises(SystemExit):
            check_stage(self.config)

    def test_19(self):
        """ Dir -> Quiet -> Not writable -> FAIL """
        self.config['quiet'] = True
        self.config['stage'] = self.alt['stage']['not_writable']
        with self.assertRaises(SystemExit):
            check_stage(self.config)

############################################################
# Test Job
############################################################
class TestJob(unittest.TestCase):
    """ Test Job input and directory validation """
    def setUp(self):
        self.config = test.config()
        self.config['stage'] = '/data/home/{0}/.test_apm'.format(self.config['username'])
        self.stage = self.config['stage']

        if not os.path.exists(self.config['stage']):
            os.makedirs(self.config['stage'])

    def tearDown(self):
        if not self.stage:
            return

        if not os.path.exists(self.stage):
            return

        f = Files({})
        if not f.is_dir_empty(self.stage):
            f.empty_dir(self.stage)

        os.rmdir(self.stage)

    def get_job(self, files):
        jobs = set(os.listdir(self.stage))
        if len(jobs) == len(files) + 1:
            for i in jobs:
                if i not in files:
                    return i
        else:
            return


    def test_1(self):
        """ No Job -> Return Random Job -> PASS """
        self.config['job'] = None
        files = set(os.listdir(self.stage))
        result = check_job(self.config)
        expected = self.get_job(files) if self.get_job(files) != None else os.listdir(self.stage)[0]

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected


    def test_2(self):
        """ No Job -> Interactive -> Job Name -> PASS """
        self.config['job'] = None
        self.config['interactive'] = True
        with mock.patch('__builtin__.input', side_effect=['test']):
            result = check_job(self.config)
            expected = 'test'

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_3(self):
        """ No Job -> Interactive -> No Job -> PASS """
        files = set(os.listdir(self.stage))
        self.config['job'] = None
        self.config['interactive'] = True
        with mock.patch('__builtin__.input', side_effect=['']):
            result = check_job(self.config)
            expected = self.get_job(files) if self.get_job(files) != None else os.listdir(self.stage)[0]

            print("Result:   {}\nExpected: {}".format(result, expected))
            assert result == expected

    def test_4(self):
        """ Job -> PASS """
        result = check_job(self.config)
        expected = 'test'

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected


################################################################################

def signal_handler(signum, frame):
    """ Catch keyboard interrupts gracefully """
    signal.signal(signal.SIGINT, original_sigint)

    ui = UI()
    try:
        terminate = ui.yn_choice("\nAre you sure you want to terminate?", 'y')
        if terminate:
            sys.exit(0)
    except KeyboardInterrupt:
        # If user Presses Ctrl-C while in this section
        print("\nTerminating")
        exit()
    except RuntimeError:
        # If user presses Ctrl-C while already asking for input
        print('\nTerminating')
        exit()
    except Exception as e:
        # Other errors
        raise e

    signal.signal(signal.SIGINT, signal_handler)

if __name__ == '__main__':
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, signal_handler)
    main()
