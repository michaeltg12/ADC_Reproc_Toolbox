#!/apps/base/python3/bin/python3

import os
import sys
import shutil
import json
from glob import glob
import subprocess
import hashlib
import datetime

import unittest
import mock
import time

import test

from apm.classes.system import jprint
from apm.classes.system import get_shell
from apm.classes.system import dir_pattern
from apm.classes.reproc_db import ReprocDB

# TODO FOR DEBUGGING DURRING DEVELOPMENT
from inspect import currentframe, getframeinfo, getouterframes
# print("\n\t*** frameinfo ***\n{}\n".format(getframeinfo(currentframe(), context=2)))
# print("\n\t*** outerframes ***\n{}\n".format(getouterframes(currentframe(), context=2)))

REPROC_HOME = os.environ.get('REPROC_HOME')

class Files:
    """ Work with Files """

    def __init__(self, config, files=None):
        """ Initialize with args """
        self.files = files
        self.config = config
        self.default = {
            "alias": None,
            "begin": 0,
            "command": None,
            "compare": True,
            "datastream": None,
            "db_up": True,
            "demo": None,
            "devel": False,
            "duplicates": False,
            "end": 0,
            "exit": False,
            "facility": None,
            "iflags": None,
            "ingest": True,
            "instrument": None,
            "interactive": False,
            "job": None,
            "quiet": False,
            "site": None,
            "source": None,
            "stage": None,
            "vap": False,
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

    def load_db_config(self):
        with open(os.path.join(REPROC_HOME, 'ADC_Reproc_Toolbox', '.config', '.arm_db_connect')) as arm_db_config:
            config_json = json.load(arm_db_config)
            development_config = config_json["development_config"]
            production_config = config_json["production_config"]
        return production_config, development_config

    def db_load_config(self):
        production_config, development_config = self.load_db_config()

        reproc_db = ReprocDB(production_config, self.config['job'])
        start_date, end_date = reproc_db.get_time_period()
        try:
            self.config['start_date'] = start_date.strftime("%Y-%m-%d %H:%M:%S")
            self.config['end_date'] = end_date.strftime("%Y-%m-%d %H:%M:%S")
            self.config['begin'] = start_date.strftime("%Y%m%d")
            self.config['end'] = end_date.strftime("%Y%m%d")
        except AttributeError as ae:
            exit('{} - db_load_config - get_time_period returned string. May be connected to wrong db.'.format(ae))
        self.config['datastream'] = reproc_db.get_affected_datastreams()

        #TODO this architecture needs to be verified
        if len(self.config['datastream']) == 1:
            site, instrument, facility, level = self.parse_datastream(self.config['datastream'][0])
            self.config['site'] = site
            self.config['instrument'] = instrument
            self.config['facility'] = facility
            self.config['level'] = level

            if reproc_db.is_equation_present(self.config['job']):
                self.config['equation'] = reproc_db.get_equation(self.config['job'])
            else:
                self.config['equation'] = None

        else:
            site, instrument, facility, level = self.parse_datastream(self.config['datastream'][0])
            self.config['site'] = site
            self.config['instrument'] = instrument
            self.config['facility'] = facility
            self.config['level'] = "00"

            if reproc_db.is_equation_present(self.config['job']):
                self.config['equation'] = reproc_db.get_equation(self.config['job'])
            else:
                print("Equation not present, can not fully auto-reprocess.")

            job_name = self.config['job']
            job_file = "{}.conf".format(job_name)
            job_path = self.get_job_path(job_file)

            if job_path != None:
                try:
                    fp = open(dir_pattern().format(job_path, job_file), 'r')
                    old = json.loads(fp.read())
                    fp.close()
                except Exception as e:
                    # raise e
                    print('Unable to load job config from file.')
                    return False

                config = self.merge_settings(self.config, self.default, self.config)

                for i in self.config:
                    if i not in config:
                        config[i] = self.config[i]

                config['interactive'] = False

                if config['ingest'] == True and config['vap'] == True:
                    config['vap'] = not config['ingest']

                return self.config

            else:
                return False

    @staticmethod
    def parse_datastream(datastream):
        site = datastream[:3]
        instrument = datastream[3:-5]
        facility = datastream[-5:-3]
        level = datastream[-2:]
        return site, instrument, facility, level

    def load_config(self):
        job_name = self.config['job']
        job_file = "{}.conf".format(job_name)
        job_path = self.get_job_path(job_file)

        if job_path != None:
            try:
                fp = open(dir_pattern().format(job_path, job_file), 'r')
                old = json.loads(fp.read())
                fp.close()
            except Exception as e:
                # raise e
                print('Unable to load job config from file.')
                return False

            config = self.merge_settings(self.config, self.default, old)

            for i in self.config:
                if i not in config:
                    config[i] = self.config[i]

            if config['interactive'] == True and config['quiet'] == True:
                config['quiet'] = not config['interactive']

            if config['ingest'] == True and config['vap'] == True:
                config['vap'] = not config['ingest']

            if self.config['datastream'] != self.default['datastream']:
                config['site'] = self.default['site']
                config['instrument'] = self.default['instrument']
                config['facility'] = self.default['facility']
            elif ((self.config['site'] != self.default['site']) or
                (self.config['instrument'] != self.default['instrument']) or
                (self.config['facility'] != self.default['facility'])):
                config['datastream'] = self.default['datastream']

            return config

        else:
            return False

    def merge_settings(self, config, default, old):
        '''Merge the settings from default and old into config'''
        settings = {}

        no_persist = [
            'interactive',
            'quiet',
            'demo',
        ]

        for i in default:
            if type(default[i]) != dict:
                if i in config and config[i] == default[i] and i in old and i not in no_persist:
                    settings[i] = old[i]
                elif type(config[i]) == str and (config[i].lower() == 'clear' or config[i].lower == 'none'):
                    settings[i] = default[i]
                else:
                    settings[i] = config[i]

            else:
                settings[i] = self.merge_settings(config[i], default[i], old[i])

        return settings

    def clean_path(self, path):
        """ Strip the trailing / from a path """
        if type(path) != str:
            return None

        if len(path) == 0:
            return None

        if path[-1] == '/':
            return path[:-1]
        else:
            return path

    def get_job_path(self, name):
        """ Find a job file from the specified name """
        job_path = None

        username = os.environ.get('USER')
        home = os.path.expanduser('~')

        config_path = '{}/post_processing/{}'.format(REPROC_HOME, self.config['job'])
        old_config_path = '{}/.processing'.format(home)

        # Make sure config path exists
        if not os.path.exists(config_path):
            os.makedirs(config_path)

        # Check to see if old config path exists
        # Move all of the config files
        # Remove the directory
        if os.path.exists(old_config_path):
            old_config_files = os.listdir(old_config_path)
            for i in old_config_files:
                if not os.path.exists(dir_pattern().format(config_path, i)):
                    src = dir_pattern().format(old_config_path, i)
                    dst = dir_pattern().format(config_path, i)
                    try:
                        os.rename(src, dst)
                    except OSError:
                        shutil.move(src, dst)
                    # print("Remove dir"

            self.empty_dir(old_config_path)
            os.rmdir(old_config_path)

        # Check to see if config file is in current dir
        file_list = glob(name)

        if len(file_list) == 1:
            job_path = os.getcwd()

            # if config file is a link, make sure it points to a .apm directory
            if os.path.islink(name):
                path = os.path.realpath(name).split('/')
                if '.processing' in path:
                    os.remove(name)
                    os.symlink(dir_pattern().format(config_path, name), dir_pattern().format(os.getcwd(),name))
            return job_path
        else:
            if len(glob(dir_pattern().format(config_path, name))) == 1:
                return config_path
            else:
                return

    def save_config(self):
        config = self.config
        username = None
        if config['username']:
            username = config['username']
        else:
            username = os.environ.get('USER')

        post_path = '{}/post_processing/{}'.format(REPROC_HOME, self.config['job'])
        if not os.path.isdir(post_path):
            os.makedirs(post_path)
        job = '{}.conf'.format(config['job'])
        jfile = os.path.join(post_path, job)

        fp = open(jfile, 'w')
        fp.write(json.dumps(config, separators=(',', ': '), indent=4, sort_keys=True))
        fp.close()

        link = dir_pattern(3).format(config['stage'], config['job'], job)
        if not os.path.exists(link):
            os.symlink(jfile, link)
        else:
            if os.path.islink(link):
                # if config file is a link, make sure it points to the right file
                if os.path.realpath(link) != jfile:
                    os.remove(link)
                    os.symlink(jfile, link)
            else:
                # If it's not a link, make it one
                os.remove(link)
                os.symlink(jfile, link)

    def save_env(self):
        command = None
        divider = None
        ext = None
        shell = get_shell()

        def create_env(ext, divider, command):
            pattern = '{0} {1}{2}"{3}"\n'
            archive = self.config['source']
            path = dir_pattern().format(self.config['stage'], self.config['job'])
            env = pattern.format(command, 'DATA_HOME', divider, path)
            env_vars = {'ARCHIVE_DATA': archive, 'COLLECTION_DATA': '$DATA_HOME/collection',
                'CONF_DATA': '$DATA_HOME/conf', 'DATASTREAM_DATA': '$DATA_HOME/datastream', 'DB_DATA': '$DATA_HOME/db',
                'HEALTH_DATA': '$DATA_HOME/health', 'LOGS_DATA': '$DATA_HOME/logs', 'OUT_DATA': '$DATA_HOME/out',
                'QUICKLOOK_DATA': '$DATA_HOME/quicklooks', 'TMP_DATA': '$DATA_HOME/tmp', 'WWW_DATA': '$DATA_HOME/www'}

            if self.config['vap']:
                env_vars['DSUTIL_HOME'] = '/apps/ds'
                env_vars['VAP_HOME'] = '/apps/process'

            for k in env_vars:
                env += pattern.format(command, k, divider, env_vars[k])

            envfile = 'env.{}'.format(ext)
            envfile = dir_pattern(3).format(self.config['stage'], self.config['job'], envfile)
            fp = open(envfile, 'w')
            fp.write(env)
            fp.close()

            print("\nTo update your environment variables for this job, run the following commands")
            # print("change to the job directory and run")
            print("cd {}".format(path))
            print("source env.{}".format(ext))

        # Write a shell script to modify the environment variables for both bash and csh/tcsh
        ext = 'bash'
        divider = '='
        command = ''
        create_env(ext, divider, command)
        ext = 'csh'
        command = 'setenv'
        divider = ' '
        create_env(ext, divider, command)

    def save_filenames(self):
        """ Save list of filenames to json file """
        filename = '{}.json'.format(self.config['job'])
        path = dir_pattern().format(self.config['stage'], self.config['job'])
        fp = open(dir_pattern().format(path, filename), 'w')
        fp.write(json.dumps(self.files, indent=4, sort_keys=True, separators=(',', ': ')))
        fp.close()
        return

    def load_filenames(self):
        """ Load list of filenames from json file """
        filename = '{}.json'.format(self.config['job'])
        path = dir_pattern().format(self.config['stage'], self.config['job'])
        try:
            fp = open(dir_pattern().format(path, filename), 'r')
            self.files = json.loads(fp.read())
            fp.close()
            return self.files
        except:
            return None

    def get_hash(self, file_name):
        if not os.path.exists(file_name):
            return None

        blocksize = 65536
        hasher = hashlib.sha256()
        afile = open(file_name, 'rb')
        buf = afile.read(blocksize)
        while (len(buf) > 0):
            hasher.update(buf)
            buf = afile.read(blocksize)
        afile.close()

        return hasher.digest()

    def is_same_file(self, file_1, file_2):
        return self.get_hash(file_1) == self.get_hash(file_2)

    def empty_dir(self, folder):
        """ Delete all files and folders in the specified directory """
        folder = self.clean_path(folder)
        filelist = os.listdir(folder)
        if len(filelist) > 0:
            for i in filelist:
                f = dir_pattern().format(folder, i)

                if os.path.isfile(f) or os.path.islink(f):
                    os.remove(f)
                elif os.path.isdir(f):
                    self.empty_dir(f)
                    os.rmdir(f)
                else:
                    exit("Unable to delete {}. Please manually remove this file and try again.".format(f))


    def setup_job_dir(self):
        """ Make sure the specified job directory:
                    - Exists
                    - Is writable
                    - Has all of the appropriate directories
        """
        folders = ['archive', 'collection', 'conf', 'datastream', 'db', 'health', 'logs', 'out', 'quicklooks', 'tmp', 'www']
        job_dir = dir_pattern().format(self.config['stage'], self.config['job'])

        if not os.path.exists(job_dir):
            try:
                os.mkdir(job_dir)
            except:
                exit('Unable to create job directory. Please try again.')

        if self.is_dir_writable(job_dir):
            for i in folders:
                path = dir_pattern().format(job_dir, i)
                if not os.path.exists(path):
                    try:
                        os.mkdir(path)
                    except:
                        exit('Unable to create directory structure in job folder.')

        else:
            exit('{} is not writable. Please try again and specify a job location that is writable.'.format(job_dir))

        return

    def is_dir_empty(self, folder):
        return len(os.listdir(folder)) == 0

    def strip_name(self, name):
        """ Return the name provided with the ARM prefix stripped """
        new_name = None
        move = False

        if type(name) != str:
            return None, move

        temp = name.split('.')

        if len(temp) > 5:
            if temp[-1][0] == 'v':
                try:
                    int(temp[-1][1:])
                    temp = temp[:-1]
                except:
                    pass

            if temp[4] == 'raw':
                temp = temp[5:]

            elif temp[4] == 'orig' or temp[4] == 'bad' or temp[4].startswith('edit'):
                move = True

            new_name = '.'.join(temp)

        return new_name, move


    def rename_file(self, name):
        """ Strip the ARM prefix from the specified file """
        new_name, move_file = self.strip_name(name)

        if move_file == True:
            new_name = 'other_files/{}'.format(new_name)
            if not os.path.exists('other_files'):
                os.mkdir('other_files')

        if new_name != None and os.path.exists(name):
            if os.path.exists(new_name):
                exit("File named {} already exists.\n Please fix this conflict and try again.".format(new_name))

            try:
                os.rename(name, new_name)
                return new_name
            except OSError:
                shutil.move(name, new_name)
                return new_name
        elif new_name == None:
            return name

        return

    def rename_files(self, location, files):
        """ Strip the ARM prefix from a list of files """
        cwd = os.getcwd()
        os.chdir(location)
        for i in files:
            self.rename_file(i)

        os.chdir(cwd)
        return

    def is_dir_writable(self, folder, verbose=False):
        """ Check to see if the provided folder is writable """
        if type(folder) != str:
            return False
        writable = None
        quiet = self.config['quiet']

        if os.path.exists(folder):
            if verbose and not quiet:
                print("{} is writable:\t".format(folder), end="")

            writable = os.access(folder, os.W_OK | os.X_OK)

            if verbose and not quiet:
                print("Pass" if writable else "Fail")

        else:
            print('{} does not exists'.format(folder))
            writable = False

        return writable


    def get_file_by_uuid(self, uuid):
        """ Find a file by the given uuid """
        if self.files == None or uuid == None:
            return

        for i in self.files:
            for j in self.files[i]:
                for k,v in self.files[i][j].items():
                    if v['uuid'] == uuid:
                        return k

        return

    def get_original_name(self, name):
        if self.files == None or name == None:
            return

        return self.get_value_for_key(name, 'original_name')

    def get_stripped_name(self, name):
        if self.files == None or name == None:
            return

        return self.get_value_for_key(name, 'stripped_name')

    def get_processed_name(self, name):
        if self.files == None or name == None:
            return

        return self.get_value_for_key(name, 'processed_name')

    def get_duplicate_files(self, name):
        if self.files == None or name == None:
            return

        return self.get_value_for_key(name, 'duplicate_files')

    def get_value_for_key(self, name, key):
        if self.files == None or name == None or key == None:
            return

        for i in self.files:
            for j in self.files[i]:
                if name in self.files[i][j]:
                    return self.files[i][j][name][key]

        return



################################################################################
# Unit tests
################################################################################
class TestFiles(unittest.TestCase):
    """ Test the Files Class """
    def setUp(self):
        self.config = test.config()
        self.username = self.config['username']

    ##################################################
    # Clean Path
    ##################################################
    def test_1(self):
        """ Dir with / """
        f = Files(self.config)
        folder = "/data/home/{}/apm/".format(self.username)
        result = f.clean_path(folder)
        expected = folder[:-1]

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ Dir without / """
        f = Files(self.config)
        folder = "/data/home/{}/apm".format(self.username)
        result = f.clean_path(folder)
        expected = folder

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    ##################################################
    # Is Dir Writable
    ##################################################
    def test_3(self):
        """ Pass writable folder -> Return True """
        f = Files(self.config)
        folder = "/data/home/{}/apm".format(self.username)
        result = f.is_dir_writable(folder)
        expected = True

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_4(self):
        """ Pass non writable folder -> Return False """
        f = Files(self.config)
        folder = "/data/archive".format(self.username)
        result = f.is_dir_writable(folder)
        expected = False

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_5(self):
        """ Pass non existing folder -> Return False """
        f = Files(self.config)
        folder = "/data/test_apm"
        result = f.is_dir_writable(folder)
        expected = False

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

################################################################################
# Load Config
################################################################################
class TestLoadConfig(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.stage = '/data/home/{}/.test_apm'.format(os.environ.get('USER'))
        self.conf = dir_pattern().format(self.stage, 'test.conf')
        self.config = test.config()
        self.config['job'] = 'test1'
        self.config['stage'] = self.stage
        self.files = ['test1', 'test2']

        if not os.path.exists(self.stage):
            os.mkdir(self.stage)

        for i in self.files:
            name = '{}.conf'.format(i)
            src = dir_pattern().format('../../testfiles', name)
            dst = dir_pattern().format(self.stage, name)

            if not os.path.exists(dst):
                shutil.copy(src, dst)

        os.chdir(self.stage)

    def tearDown(self):
        os.chdir(self.cwd)
        if os.path.exists(self.stage):
            shutil.rmtree(self.stage)

    def test_1(self):
        """ No input -> compare to default -> PASS """
        f = Files(self.config)
        result = f.load_config()
        expected = self.config

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ With datastream from file, pass in SIF data -> PASS """
        self.config['site'] = "sgp"
        self.config['instrument'] = "mfrsr"
        self.config['facility'] = "C1"
        self.config['job'] = 'test2'

        f = Files(self.config)
        result = f.load_config()
        expected = self.config
        # expected['stage'] = self.stage
        # expected['job'] = 'test2'

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_3(self):
        """ With SIF from file, pass in Datastream -> PASS """
        self.config['datastream'] = ["sgpmfrsrC1.00", "nsairtC1.00"]
        f = Files(self.config)
        result = f.load_config()
        expected = self.config
        expected['site'] = None
        expected['instrument'] = None
        expected['facility'] = None

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_4(self):
        """ Pass in both quiet and interactive, get only interactive back """
        self.config['interactive'] = True
        self.config['quiet'] = True
        f = Files(self.config)
        result = f.load_config()
        expected = self.config
        expected['quiet'] = False

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected


    def test_5(self):
        """ Pass in both vap and ingest and get only ingest back """
        self.config['ingest'] = True
        self.config['vap'] = True
        f = Files(self.config)
        result = f.load_config()
        expected = self.config
        expected['vap'] = False

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_6(self):
        """ Change begin and end dates """
        self.config['begin'] = 20120501
        self.config['end'] = 20120507
        f = Files(self.config)
        result = f.load_config()
        expected = self.config

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

################################################################################
# Clean Path
################################################################################
class TestCleanPath(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.file = Files(self.config)

    def test_1(self):
        """ pass in trailing / return without """
        path = '/data/home/'
        result = self.file.clean_path(path)
        expected = path[:-1]

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ pass without trailing / return same """
        path = '/data/home'
        result = self.file.clean_path(path)
        expected = path

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_3(self):
        """ pass empty string """
        path = ''
        result = self.file.clean_path(path)
        expected = None

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_4(self):
        """ pass non string type """
        path = 123456789
        result = self.file.clean_path(path)
        expected = None

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_5(self):
        """ pass None type """
        path = None
        result = self.file.clean_path(path)
        expected = path

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected


################################################################################
# Get Job Path
################################################################################
class TestGetJobPath(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.file = 'test.json'
        self.file_source = '../../testfiles/{}'.format(self.file)
        self.conf_dir = '/data/home/{}/.apm'.format(os.environ.get('USER'))
        self.old_conf = '{}/.processing'.format(os.path.expanduser('~'))
        if not os.path.exists(self.conf_dir):
            os.mkdir(self.conf_dir)

    def tearDown(self):
        if os.path.exists(self.file):
            os.remove(self.file)

        if os.path.exists(dir_pattern().format(self.conf_dir, self.file)):
            os.remove(dir_pattern().format(self.conf_dir, self.file))

        if os.path.exists(dir_pattern().format(self.old_conf, self.file)):
            os.remove(dir_pattern().format(self.old_conf, self.file))

        if os.path.exists(self.old_conf):
            shutil.rmtree(self.old_conf)

    def test_1(self):
        """ Find job file in current path """
        shutil.copy(self.file_source, '.')
        f = Files(self.config)
        result = f.get_job_path(self.file)
        expected = os.getcwd()

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ Find job file in default path """
        shutil.copy(self.file_source, self.conf_dir)
        f = Files(self.config)
        result = f.get_job_path(self.file)
        expected = self.conf_dir

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_3(self):
        """ Don't find job file """
        f = Files(self.config)
        result = f.get_job_path(self.file)
        expected = None

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_4(self):
        """ Test move """
        if not os.path.exists(self.old_conf):
            os.mkdir(self.old_conf)
        shutil.copy(self.file_source, self.old_conf)
        try:
            os.symlink(dir_pattern().format(self.old_conf, self.file), self.file)
        except:
            os.remove(self.file)
            os.symlink(dir_pattern().format(self.old_conf, self.file), self.file)

        f = Files(self.config)
        result = f.get_job_path(self.file)
        expected = os.getcwd()

        dir_list = os.listdir(os.path.expanduser('~'))
        conf_list = os.listdir(self.conf_dir)

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

        assert '.processing' not in dir_list
        assert self.file in conf_list
        assert self.old_conf not in os.path.realpath(self.file)
        assert self.conf_dir in os.path.realpath(self.file)

################################################################################
# Save Config
################################################################################
class TestSaveConfig(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.username = self.config['username']
        self.config['job'] = 'apm_test'
        self.stage = '/data/home/{}/.apm_test'.format(self.username)
        self.config['stage'] = self.stage
        self.file = Files(self.config)
        self.path = '/data/home/{}/.apm'.format(self.username)
        self.jobfile = '{}/{}.conf'.format(self.path, self.config['job'])
        self.cwd = os.getcwd()
        if not os.path.exists(self.stage):
            os.mkdir(self.stage)

        if not os.path.exists('{}/apm_test'.format(self.stage)):
            os.mkdir('{}/apm_test'.format(self.stage))

        os.chdir(self.stage)

    def tearDown(self):
        os.chdir(self.cwd)
        if os.path.exists(self.jobfile):
            os.remove(self.jobfile)

        if os.path.exists(self.stage):
            self.file.empty_dir(self.stage)
            os.rmdir(self.stage)

    def test_1(self):
        """ save over existing file """
        if os.path.exists(self.jobfile):
            os.remove(self.jobfile)

        # Create an empty file
        fp = open(self.jobfile, 'w').close()

        self.file.save_config()
        fp = open(self.jobfile, 'r')
        result = json.loads(fp.read())
        fp.close()
        expected = self.config

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected
        assert self.jobfile.split('/')[-1] in os.listdir('{}/{}'.format(self.stage, self.config['job']))


    def test_2(self):
        """ save brand new file """
        if os.path.exists(self.jobfile):
            os.remove(self.jobfile)

        self.file.save_config()
        fp = open(self.jobfile, 'r')
        result = json.loads(fp.read())
        fp.close()
        expected = self.config

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected
        assert self.jobfile.split('/')[-1] in os.listdir('{}/{}'.format(self.stage, self.config['job']))



################################################################################
# Save Env
################################################################################
class TestSaveEnv(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.config['stage'] = '/data/home/{}/.apm_test'.format(self.config['username'])
        self.stage = self.config['stage']
        self.jobdir = dir_pattern().format(self.stage, self.config['job'])
        self.file = Files(self.config)

        if not os.path.exists(self.stage):
            os.mkdir(self.stage)

        if not os.path.exists(self.jobdir):
            os.mkdir(self.jobdir)

    def tearDown(self):
        if os.path.exists(self.stage):
            self.file.empty_dir(self.stage)
            os.rmdir(self.stage)

    def test_1(self):
        """ Brand new env file -> Bash shell"""
        path = dir_pattern().format(self.stage, self.config['job'])
        with mock.patch.dict('os.environ', {'SHELL': '/bin/bash'}):
            self.file.save_env()
            files = os.listdir(path)

            assert 'env.sh' in files

    def test_2(self):
        """ File already exists -> Bash shell """
        hello = "hello, World!"
        envfile = dir_pattern(3).format(self.stage, self.config['job'], 'env.sh')
        fp = open(envfile, 'w')
        fp.write(hello)
        fp.close()

        with mock.patch.dict('os.environ', {'SHELL': '/bin/bash'}):
            self.file.save_env()
            fp = open(envfile, 'r')
            result = fp.read()
            fp.close()

            print("Result:   {}\nhello: {}".format(result, hello))
            assert result != hello

    def test_3(self):
        """ csh shell """
        path = dir_pattern().format(self.stage, self.config['job'])
        with mock.patch.dict('os.environ', {'SHELL': '/bin/csh'}):
            self.file.save_env()
            files = os.listdir(path)

            assert 'env.csh' in files

    def test_4(self):
        """ Unknown shell """
        with mock.patch.dict('os.environ', {'SHELL': '/apps/base/python2.7/bin/python'}):
            with self.assertRaises(SystemExit):
                self.file.save_env()


################################################################################
# Get Hash
################################################################################
class TestGetHash(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.file = Files(self.config)
        username = self.config['username']
        self.home = '/data/home/{}/.apm_test'.format(username)
        self.testfile = '{}/hello.txt'.format(self.home)
        self.expected = "fcff297b5772aa6d04967352c5f4eb96"
        if not os.path.exists(self.home):
            os.mkdir(self.home)

        fp = open(self.testfile, 'w')
        fp.write("hello, World!")
        fp.close()

    def tearDown(self):
        if os.path.exists(self.testfile):
            os.remove(self.testfile)

        if os.path.exists(self.home):
            os.rmdir(self.home)

    def test_1(self):
        """ pass non existant file """
        result = self.file.get_hash('/data/home/hello.txt')
        expected = None

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected


    def test_2(self):
        """ pass existing file """
        result = self.file.get_hash(self.testfile)
        expected = self.expected

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_3(self):
        """ pass relative file path """
        cwd = os.getcwd()
        os.chdir(self.home)
        result = self.file.get_hash(self.testfile.split('/')[-1])
        expected = self.expected
        os.chdir(cwd)

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected



################################################################################
# Is Same File
################################################################################
class TestIsSameFile(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.file = Files(self.config)
        username = self.config['username']
        self.home = '/data/home/{}/.apm_test'.format(username)
        self.testhello = '{}/hello.txt'.format(self.home)
        self.testworld = '{}/world.txt'.format(self.home)

        if not os.path.exists(self.home):
            os.mkdir(self.home)

        fp = open(self.testhello, 'w')
        fp.write("hello, World!")
        fp.close()

        fp = open(self.testworld, 'w')
        fp.write("hello, World!")
        fp.close()

    def tearDown(self):
        if os.path.exists(self.testhello):
            os.remove(self.testhello)

        if os.path.exists(self.testworld):
            os.remove(self.testworld)

        if os.path.exists(self.home):
            os.rmdir(self.home)

    def test_1(self):
        """ pass identical files """
        result = self.file.is_same_file(self.testhello, self.testworld)
        expected = True

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ pass same file twice """
        result = self.file.is_same_file(self.testhello, self.testhello)
        expected = True

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_3(self):
        """ pass different files """
        fp = open(self.testworld, 'w')
        fp.write("testing")
        fp.close()

        result = self.file.is_same_file(self.testhello, self.testworld)
        expected = False

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_4(self):
        """ pass one file non existant for other file """
        result = self.file.is_same_file(self.testhello, "/data/home/hello.txt")
        expected = False

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected


################################################################################
# Test Empty Dir Methods
################################################################################
class TestEmpty(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.username = self.config['username']
        self.path = '/data/home/{}/.test_apm'.format(self.username)
        self.structure = {
            'empty': 'folder',
            'not_empty1': {
                'hello.txt': 'file',
                'world.txt': 'file',
            },
            'not_empty2': {
                'hello': {
                    'hello.txt': 'file',
                    'world.txt': 'file',
                },
                'world': 'folder',
                'hello.txt': 'file',
                'world.txt': 'file',
            },
        }
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        cwd = os.getcwd()
        os.chdir(self.path)
        self.setup_structure(self.structure)
        os.chdir(cwd)

    def tearDown(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def setup_structure(self, structure):
        for i in structure:
            if structure[i] == 'folder' or type(structure[i]) == dict:
                if not os.path.exists(i):
                    os.mkdir(i)
                if type(structure[i]) == dict:
                    os.chdir(i)
                    self.setup_structure(structure[i])
                    os.chdir('..')
            elif structure[i] == 'file':
                if not os.path.exists(i):
                    open(i, 'w').close()


    ##################################################
    # Empty Dir
    ##################################################
    def test_1(self):
        """ Pass empty dir -> Make sure dir stays empty """
        f = Files(self.config)
        folder = '{}/empty'.format(self.path)
        f.empty_dir(folder)
        result = os.listdir(folder)
        expected = []

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ Pass dir with files -> make sure empty """
        f = Files(self.config)
        folder = '{}/not_empty1'.format(self.path)
        f.empty_dir(folder)
        result = os.listdir(folder)
        expected = []

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_3(self):
        """ Pass dir with files and folders -> make sure empty """
        f = Files(self.config)
        folder = '{}/not_empty2'.format(self.path)
        f.empty_dir(folder)
        result = os.listdir(folder)
        expected = []

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected


    ##################################################
    # Is Dir Empty
    ##################################################
    def test_4(self):
        """ Pass empty dir -> return True """
        f = Files(self.config)
        folder = '{}/empty'.format(self.path)
        result = f.is_dir_empty(folder)
        expected = True

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_5(self):
        """ Pass non empty dir -> return False """
        f = Files(self.config)
        folder = '{}/not_empty2'.format(self.path)
        result = f.is_dir_empty(folder)
        expected = False

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

################################################################################
# Setup Job Dir
################################################################################
class TestSetupJobDir(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.stage = '/data/home/{}/.test_apm'.format(self.config['username'])
        self.config['stage'] = self.stage
        self.folders = ['archive', 'collection', 'conf', 'datastream', 'db', 'health', 'logs', 'out', 'quicklooks', 'tmp', 'www']

        if not os.path.exists(self.stage):
            os.mkdir(self.stage)

    def tearDown(self):
        if os.path.exists(self.stage):
            shutil.rmtree(self.stage)

    def test_1(self):
        """ Verify all folders created """
        f = Files(self.config)
        f.setup_job_dir()
        result = os.listdir(dir_pattern().format(self.config['stage'], self.config['job']))
        expected = self.folders
        result.sort()

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ Pass job folder with missing folders """
        job = dir_pattern().format(self.config['stage'], self.config['job'])
        if not os.path.exists(job):
            os.mkdir(job)
        for i in range(len(self.folders) / 2):
            temp = dir_pattern().format(job, self.folders[i])
            if not os.path.exists(temp):
                os.mkdir(temp)

        f = Files(self.config)
        f.setup_job_dir()
        result = os.listdir(job)
        expected = self.folders
        result.sort()

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected


    def test_3(self):
        """ Set stage to not writable -> FAIL """
        self.config['stage'] = '/data'
        f = Files(self.config)
        with self.assertRaises(SystemExit):
            f.setup_job_dir()

    def test_4(self):
        """ Set stage to dir where files exist but cannot create structure -> FAIL """
        self.config['stage'] = '/'
        self.config['job'] = 'data'
        f = Files(self.config)
        with self.assertRaises(SystemExit):
            f.setup_job_dir()

################################################################################
# Get Stripped Name
################################################################################
class TestGetStrippedName(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.file = Files(self.config)

    def test_1(self):
        """ pass name without stripping """
        name = '1234567890.fac'
        result = self.file.strip_name(name)
        expected = None

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ pass name with bad """
        name = 'site.instrument.datestring.timestamp.bad.1234567890.fac'
        result = self.file.strip_name(name)
        expected = 'bad'

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_3(self):
        """ pass name with orig """
        name = 'site.instrument.datestring.timestamp.orig.1234567890.fac'
        result = self.file.strip_name(name)
        expected = 'orig'

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_4(self):
        """ pass name needs stripped """
        name = 'site.instrument.datestring.timestamp.raw.1234567890.fac'
        result = self.file.strip_name(name)
        expected = '1234567890.fac'

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_5(self):
        """ pass non string name """
        name = 1234567890
        result = self.file.strip_name(name)
        expected = False

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_6(self):
        """ pass None """
        result = self.file.strip_name(None)
        expected = False

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected



################################################################################
# Rename File
################################################################################
class TestRenameFile(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.config = test.config()
        self.username = self.config['username']
        self.stage = '/data/home/{}/.apm_test'.format(self.username)
        self.path = '{}/{}/sgp/sgpmfrsrC1.00'.format(self.stage, self.config['job'])
        self.file = Files(self.config)
        self.rawfile = "sgpmfrsrC1.00.date.timestamp.raw.1234567890.C1"
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        os.chdir(self.path)

        if not os.path.exists(self.rawfile):
            open(self.rawfile, 'w').close()

    def tearDown(self):
        os.chdir(self.cwd)

        if os.path.exists(self.stage):
            self.file.empty_dir(self.stage)
            os.rmdir(self.stage)

    def test_1(self):
        """ pass non existant file """
        if os.path.exists(self.rawfile):
            os.remove(self.rawfile)
        result = self.file.rename_file(self.rawfile)
        expected = None

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected
        assert len(os.listdir(self.path)) == 0

    def test_2(self):
        """ pass file already renamed """
        temp = self.rawfile.split('.')
        temp = [temp[-2], temp[-1]]
        filename = '.'.join(temp)
        os.rename(self.rawfile, filename)

        result = self.file.rename_file(filename)
        expected = filename

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected
        assert filename in os.listdir(self.path)

    def test_3(self):
        """ pass file needs renamed """
        temp = self.rawfile.split('.')
        temp = [temp[-2], temp[-1]]

        result = self.file.rename_file(self.rawfile)
        expected = '.'.join(temp)

        print("Result:   {}\nExpected: {}".format(result, expected))
        print(os.listdir(self.path))
        assert result == expected
        assert expected in os.listdir(self.path)

    def test_4(self):
        """ pass bad file """
        temp = self.rawfile.split('.')
        for k,v in enumerate(temp):
            if v == 'raw':
                temp[k] = 'bad'
        filename = '.'.join(temp)
        os.rename(self.rawfile, filename)

        result = self.file.rename_file(filename)
        expected = 'other_files/{}'.format(filename)

        print("Result:   {}\nExpected: {}".format(result, expected))
        print(os.listdir(self.path))
        print(os.listdir('{}/other_files'.format(self.path)))
        assert result == expected
        assert 'other_files' in os.listdir(self.path)
        assert filename in os.listdir('{}/other_files'.format(self.path))

    def test_5(self):
        """ pass orig file """
        temp = self.rawfile.split('.')
        for k,v in enumerate(temp):
            if v == 'raw':
                temp[k] = 'orig'
        filename = '.'.join(temp)
        os.rename(self.rawfile, filename)

        result = self.file.rename_file(filename)
        expected = 'other_files/{}'.format(filename)

        print("Result:   {}\nExpected: {}".format(result, expected))
        print(os.listdir(self.path))
        print(os.listdir('{}/other_files'.format(self.path)))
        assert result == expected
        assert 'other_files' in os.listdir(self.path)
        assert filename in os.listdir('{}/other_files'.format(self.path))

    def test_6(self):
        """ pass where renamed file already exists (would overwrite) """
        temp = self.rawfile.split('.')
        temp = [temp[-2], temp[-1]]
        filename = '.'.join(temp)

        fp = open(filename, 'w').close()

        with self.assertRaises(SystemExit):
            self.file.rename_file(self.rawfile)


################################################################################
# Rename Files
################################################################################
class TestRenameFiles(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.config = test.config()
        self.file = Files(self.config)
        self.files = [
            "sgpmfrsrC1.00.date.timestamp.raw.1234567890.C1",
            "sgpmfrsrC1.00.date.timestamp.raw.2345678901.C1",
            "sgpmfrsrC1.00.date.timestamp.raw.3456789012.C1",
            "sgpmfrsrC1.00.date.timestamp.raw.4567890123.C1",
        ]
        self.stage = '/data/home/{}/.apm_test/'.format(self.config['username'])
        self.path = '{}/sgp/sgpmfrsrC1.00'.format(self.stage)

        if not os.path.exists(self.path):
            os.makedirs(self.path)

        os.chdir(self.path)
        for f in self.files:
            open(f, 'w').close()

    def tearDown(self):
        os.chdir(self.cwd)
        if os.path.exists(self.stage):
            self.file.empty_dir(self.stage)
            os.rmdir(self.stage)

    def test_1(self):
        """Pass list of files -> verify all files have proper names """
        self.file.rename_files(self.path, self.files)
        result = os.listdir(self.path)
        result.sort()
        expected = self.files
        for k,v in enumerate(expected):
            expected[k] = self.file.strip_name(v)
        expected.sort()

        print("Result:   {}\nExpected: {}".format(result, expected))
        for i in expected:
            assert i in result

################################################################################
# Is Dir Writable
################################################################################
class TestIsDirWritable(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.file = Files(self.config)

    def test_1(self):
        """ pass writable dir """
        username = self.config['username']
        folder = '/data/home/{}'.format(username)
        result = self.file.is_dir_writable(folder)
        expected = True
        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ pass non-writable dir """
        folder = '/data'
        result = self.file.is_dir_writable(folder)
        expected = False
        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_3(self):
        """ pass non existant dir """
        folder = '/data/asdf'
        result = self.file.is_dir_writable(folder)
        expected = False
        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_4(self):
        """ pass non string """
        folder = 1234
        result = self.file.is_dir_writable(folder)
        expected = False
        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_5(self):
        """ pass None """
        folder = None
        result = self.file.is_dir_writable(folder)
        expected = False
        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

################################################################################
# Get Shell
################################################################################
# class TestGetShell(unittest.TestCase):
# 	def setUp(self):
# 		self.config = test.config()
# 		self.file = Files(self.config)

# 	def test_1(self):
# 		"""Shell is Bash"""
# 		with mock.patch.dict('os.environ', {'SHELL': '/bin/bash'}):
# 			result = get_shell()
# 			expected = 'bash'

# 			print("Result:   {}\nExpected: {}".format(result, expected)
# 			assert result == expected

# 	def test_2(self):
# 		"""Shell is CSH"""
# 		with mock.patch.dict('os.environ', {'SHELL': '/bin/csh'}):
# 			result = get_shell()
# 			expected = 'csh'

# 			print("Result:   {}\nExpected: {}".format(result, expected)
# 			assert result == expected

# 	def test_3(self):
# 		"""Shell is Python"""
# 		with mock.patch.dict('os.environ', {'SHELL': '/apps/base/python2.7/bin/python'}):
# 			result = get_shell()
# 			expected = None

# 			print("Result:   {}\nExpected: {}".format(result, expected)
# 			assert result == expected

################################################################################

if __name__ == '__main__':
    pass
    unittest.main(buffer=True)
    # unittest.main()

