#!/apps/base/python3/bin/python3

import os
import sys
import subprocess
import time
import calendar
import json

from subprocess import Popen
from subprocess import PIPE
from subprocess import CalledProcessError

################################################################################

def convert_date_to_timestamp(date):
    """ Convert date in YYYYMMDD format to a Unix Timestamp """
    if type(date) != str:
        date = str(date)

    return calendar.timegm(time.strptime(date, '%Y%m%d'))

################################################################################

def dir_pattern(depth=2):
    """ Return a directory pattern string with the amount of depth specified """
    if type(depth) != int:
        raise ValueError

    pattern = "{}"
    if depth >= 1:
        for i in range(1, depth):
            pattern += "/{}"

    return pattern


################################################################################

def get_shell():
    """Get the user's default shell"""
    shell = None

    if os.environ['SHELL'] == '/bin/bash':
        shell = 'bash'
    elif os.environ['SHELL'] == '/bin/csh':
        shell = 'csh'
    elif os.environ['SHELL'] == '/bin/tcsh':
        shell = 'tcsh'

    return shell

################################################################################

def is_number(n):
    try:
        int(n)
        return True
    except ValueError:
        return False

################################################################################

def jprint(text, indent=2, sort_keys=False, separators=(',', ': ')):
    print(json.dumps(text, indent=indent, sort_keys=sort_keys, separators=separators))

################################################################################

def update_archive(datastreams):
    try:
        print('Retrieving updated file list from archive database...',end="")
        sys.stdout.flush()

        command = ['inv_sync_archive', '-sync']

        for v in datastreams:
            command.append(v)

        sys.stdout.flush()
        ps = Popen(command, stdout=PIPE, stderr=PIPE)
        ps.communicate()
        returncode = ps.returncode

        if returncode == 0:
            print('Done')
            return True
        else:
            print('Failed')
            return False

    except CalledProcessError as e:
        print('Failed')

    except Exception as e:
        print('Failed')
        raise e

################################################################################

def update_env(envpath):
    """ Update the environment variables for this run """

    cwd = os.getcwd()
    os.chdir(envpath)

    env_vars = {}

    if not (os.path.exists('env.sh') or os.path.exists('env.csh')):
        return False

    if os.path.exists('env.sh'):
        envfile = 'env.sh'
        command = 'export'
        divider = '='

    elif os.path.exists('env.csh'):
        envfile = 'env.csh'
        command = 'setenv'
        divider = ' '

    fp = open(envfile, 'r')
    contents = fp.read()
    fp.close()

    lines = contents.split('\n')
    for line in lines:
        words = line.split(' ')
        if words[0] != command:
            continue

        var = ' '.join(words[1:len(words)])
        parts = var.split(divider)
        if len(parts) == 2:
            var = parts[0]
            val = parts[1]
        elif len(parts) > 2:
            var = parts[0]
            val = divider.join(parts[1:len(parts)])
        else:
            # length of parts < 2
            continue

        # strip quotes from val
        val = val.strip('"')

        # Set environment variable
        env_vars[var] = val

    for k,v in env_vars.items():
        if k != "DATA_HOME":
            env_vars[k] = v.replace("$DATA_HOME", env_vars["DATA_HOME"])

    for k,v in env_vars.items():
        os.environ[k] = v

    os.chdir(cwd)
    return True

################################################################################
