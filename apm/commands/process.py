#!/apps/base/python3/bin/python3

import os
import getpass
import glob
import threading
import time
import sys
import re
import platform

from dsdb import DSDB
import psycopg2

from datetime import datetime

from subprocess import CalledProcessError
from subprocess import Popen
from subprocess import PIPE
from subprocess import check_output

from apm.classes.db import DB

from apm.classes.system import dir_pattern
from apm.classes.system import jprint
from apm.classes.system import get_shell
from apm.classes.system import update_env

from apm.pmanager.manager import PluginManager

global DEVEL
# DEVEL = True
DEVEL = False

global binpath


global vappath
vappath = "/apps/ds/bin"

class Process:
    """ Run Ingest or VAP processing task """

    def __init__(self, config, files=None):
        """ Initialize with args """
        global DEVEL
        global binpath

        self.config = config
        self.files = files
        self.db = DB(self.config)
        self.manager = PluginManager()


        # Only set devel to false if devel is already false
        if 'devel' in self.config.keys() and DEVEL != True:
            DEVEL = self.config['devel']

        if DEVEL or platform.node() == 'copper':
            binpath = "/apps/ldmf6/process/bin"
        else:
            binpath = "/apps/process/bin"


    def run(self):
        """ Run the process phase """
        if DEVEL:
            pass
        home = os.path.expanduser('~')

        # Update env variables
        print("\nUpdating environment vars...", end="")
        sys.stdout.flush()

        if not update_env(dir_pattern().format(self.config['stage'], self.config['job'])):
            shell = get_shell()
            if shell == "bash":
                ext = 'sh'
            else:
                ext = 'csh'

            print("Fail")
            exit("Error: Unable to locate env.%s." % ext)
        print("Done") # Updating Env Vars

        # Check for .db_connect file
        print("\nLocating .db_connect...", end="")
        sys.stdout.flush()

        db_file = dir_pattern().format(home, ".db_connect")

        if not os.path.exists(db_file):
            fp = open(db_file, 'w')
            fp.close()

        # Check for apm or user specified alias
        if not self.setup_alias(db_file):
            exit()
        print("Done")

        if self.config['ingest']:
            ##################################################
            # START INGEST PROCESSING
            ##################################################

            # Find the ingest executable for each process
            print("\nLocating ingest executable...", end="")
            sys.stdout.flush()

            processes = self.db.get_data_paths()

            for k, v in enumerate(processes):
                ingest, multiple = self.find_ingest_exec(v['proc'])

                if not ingest:
                    print("Fail")
                    exit("Unable to find Ingest executable for {}".format(v['proc']))
                else:
                    processes[k]['ingest'] = ingest
                    processes[k]['multiple'] = multiple

            # Add a plugin spot to update the ingest as necessary
            # Then create the IRT ingest plugin
            # Check to see if a plugin needs to modify the datastream
            temp = self.manager.callPluginCommand('hook_ingest_alter', {'processes': processes})
            processes = temp if temp != None else processes

            print("Done")
            print("")
            db_commands = []
            # Update the database

            if self.config['db_up'] != False:
                print("\nUpdating the database...", end="")
                sys.stdout.flush()

                for process in processes:
                    if not self.update_db(process):
                        print("Fail")
                        exit("ERROR: Unable to update database for {}".format(process['proc']))

                print("Done")

            print("\nExecuting ingest processes...", end="")
            sys.stdout.flush()

            # Execute an Ingest process
            threads = {}
            status = {}

            done = False


            while not done:
                done = True
                for k,v in enumerate(processes):
                    # Make sure all needed keys exist
                    if 'complete' not in v:
                        processes[k]['complete'] = False
                        v['complete'] = False

                    key = v['ingest'].split('/')[-1].split('_')[0]
                    if (key not in threads or status[key] == True) and v['complete'] == False:
                        done = False
                        status[key] = False
                        threads[key] = Ingest(v, self.config, k)
                        if not threads[key]:
                            exit("ERROR: Ingest object not created")
                        threads[key].start()
                    elif threads[key].is_alive():
                        done = False
                    elif (not threads[key].is_alive()) and (v['complete'] == False or status[key] == False):
                        status[key] = True
                        processes[threads[key].key]['complete'] = True
                        processes[threads[key].key]['result'] = threads[key].result
                        result = processes[threads[key].key]['result']
                        # Notify the user if there was an error, that way they can correct
                        # 	the error and run again or run manually if needed
                        if threads[key].error != False:
                            print("There was an error: ", end="")
                            print(threads[key].error)

                        if self.files and result:
                            for i,site in result.items():
                                for j,sif in site.items():
                                    for k,name in sif.items():
                                        if k in self.files[i][j]:
                                            self.files[i][j][name] = self.files[i][j][k]
                                            self.files[i][j].pop(k)
                                            self.files[i][j][name]['processed_name'] = name
                                            self.files[i][j][name]['current_name'] = name

                    elif v['complete'] == True:
                        pass
                    else:
                        print("OOPS forgot a status")


            print("Done")

            ##################################################
            # END INGEST PROCESSING
            ##################################################
        elif self.config['vap']:
            ##################################################
            # START VAP PROCESSING
            ##################################################
            print('Running vapmgr...', end="")
            sys.stdout.flush()

            starttime = None
            endtime = None
            is_success = None

            # Make sure vappath is in the path env variable
            syspath = os.environ.get('PATH')
            syspath = syspath.split(':')
            if binpath not in syspath:
                syspath.append(binpath)
                syspath = ':'.join(syspath)
                os.environ['PATH'] = syspath

            ###############################################
            # Remove this code for production
            ###############################################

            vaphome = binpath.split('/')[:-1]
            vaphome = '/'.join(vaphome)
            os.environ['VAP_HOME'] = vaphome

            ###############################################

            # Run vapmgr setup to create any needed aliases
            setup = [
                '%s/vapmgr' % vappath,
                '-setup',
                '-r',
                '%s.%s' % (self.config['site'], self.config['facility']),
                self.config['instrument']
            ]

            ps = Popen(setup, stdout=PIPE, stderr=PIPE)
            (output, error) = ps.communicate()
            returncode = ps.returncode


            if returncode != 0:
                print("ERROR: Unable to setup vapmgr")
                print("")
                print(error)
                exit()

            # Run vapmgr to process the vaps
            starttime = datetime.now().replace(microsecond=0)

            command = [
                '%s/vapmgr' % vappath,
                '-r',
                '%s.%s' % (self.config['site'], self.config['facility']),
                '-start',
                str(self.config['begin']),
                '-end',
                str(self.config['end']),
                '-force',
                self.config['instrument']
            ]

            ps = Popen(command, stdout=PIPE, stderr=PIPE)
            (output, error) = ps.communicate()
            returncode = ps.returncode
            endtime = datetime.now().replace(microsecond=0)

            if returncode != 0:
                print("ERROR: Error running vapmgr")
                print("")
                print(error)
                exit()


            # vapmgr ran successfully
            # Find out what log files need parsed
            path = dir_pattern(5).format(self.config['stage'], self.config['job'], 'logs', self.config['site'], '%s_logs')
            proc_path = path % 'proc'
            instr_path = path % 'instr'

            vaplogs = []
            vapmgrlogs = []
            vapmgrqclogs = []

            year = str(starttime.year).zfill(4)
            month = str(starttime.month).zfill(2)

            regex_log_file_pattern = '%s.*%s.*%s\.%s%s00.000000.%s'

            proc = regex_log_file_pattern % (self.config['site'], self.config['instrument'], self.config['facility'], year, month, 'VAP')
            instr = regex_log_file_pattern % (self.config['site'], self.config['instrument'], self.config['facility'], year, month, 'vapmgrlog')
            instrqc = regex_log_file_pattern % (self.config['site'], self.config['instrument'], self.config['facility'], year, month, 'vapmgrqclog')

            # vap logs don't always exist. Need to check to make sure they do before trying to access them
            if os.path.exists(proc_path):
                vaplog_dirs = os.listdir(proc_path)
                for d in vaplog_dirs:
                    tmp = os.listdir(dir_pattern().format(proc_path, d))
                    for i in tmp:
                        if re.search(proc, i):
                            vaplogs.append(dir_pattern(3).format(proc_path, d, i))



            if not os.path.exists(instr_path):
                exit("Unable to find vapmgr log files")

            vapmgrlog_dirs = os.listdir(instr_path)
            for d in vapmgrlog_dirs:
                tmp = os.listdir(dir_pattern().format(instr_path, d))
                for i in tmp:
                    if re.search(instr, i):
                        vapmgrlogs.append(dir_pattern(3).format(instr_path, d, i))
                    elif re.search(instrqc, i):
                        vapmgrqclogs.append(dir_pattern(3).format(instr_path, d, i))


            logs = {}

            # Parse VAP log file
            if len(vaplogs) > 0:
                for k,log in enumerate(vaplogs):
                    temp = self.parse_vap_log(log, starttime, endtime)
                    logs['vap'] = []
                    for i in temp:
                        i['log_file'] = vaplogs[k]
                        logs['vap'].append(i)


            # Parse vapmgr log file
            if len(vapmgrlogs) > 0:
                for k,log in enumerate(vapmgrlogs):
                    temp = self.parse_vapmgr_log(log, starttime, endtime)
                    logs['vapmgr'] = []
                    for i in temp:
                        i['log_file'] = vapmgrlogs[k]
                        logs['vapmgr'].append(i)

            # Parse vapmgrqclog
            if len(vapmgrqclogs) > 0:
                for k,log in enumerate(vapmgrqclogs):
                    temp = self.parse_vapmgr_log(log, starttime, endtime, qc=True)
                    logs['vapmgrqc'] = []
                    for i in temp:
                        i['log_file'] = vapmgrqclogs[k]
                        logs['vapmgrqc'].append(i)

            print('Done')


            if 'vap' in logs and len(logs['vap']) > 0:
                print('')
                print("VAP Results")

                for k,log in enumerate(logs['vap']):
                    print("Running: %s for %s..." % (log['process'], log['dates']), end="")
                    if log['status']:
                        print(log['message'])
                    else:
                        print("ERROR")
                        print("\tFor more information see the log entry starting on line %d of the following log file:\n\t %s" % (log['line_number'], log['log_file']))

            elif 'vapmgr' in logs and len(logs['vapmgr']) > 0:
                print('')
                print("VapMGR Results")

                for log in logs['vapmgr']:
                    print(self.vapmgr_log_results(log, 'output'))

            if 'vapmgr' in logs and len(logs['vapmgr']) > 0:
                print('')
                print("VapMGR Quicklooks Results")

                for log in logs['vapmgr']:
                    print(self.vapmgr_log_results(log, 'quicklooks'))


            if 'vapmgrqc' in logs and len(logs['vapmgrqc']) > 0:
                print('')
                print("VapMGRQC Results")

                for log in logs['vapmgrqc']:
                    print(self.vapmgr_log_results(log, 'output'))

            print('')

            ##################################################
            # END VAP PROCESSING
            ##################################################

        return self.config, self.files

    def find_ingest_exec(self, process):
        """ Find the appropriate ingest executable """
        # Does the ingest run for multiple processes and require the -n option
        multiple = False

        cwd = os.getcwd()
        base_path = binpath
        os.chdir(base_path)

        executable = {}

        # Get a list of ingest executables
        ingest = glob.glob('*_ingest')

        skip = [
        # 	'xsapr_ingest',
        # 	'wacrspec_ingest',
        # 	'wacr_ingest',
        # 	'mwacrspec_ingest',
        ]

        # Loop over the executables and get the process names for each
        for i in ingest:
            multiple = False
            if (i not in skip ): # Remove this line for productions, This suppresses an error on Copper
                # Get the help text
                help_text = ""
                try:
                    ps = Popen([i, '-h'], stdout=PIPE, stderr=PIPE)
                    (output, error) = ps.communicate()
                    returncode = ps.returncode
                    help_text = output

                except CalledProcessError as e:
                    error.close()
                    output.close()
                    help_text = ""
                    status = e.returncode
                    if DEVEL:
                        print("\nCALLED PROCESS ERROR: GET HELP TEXT\n")

                # Check for process names
                help_text = help_text.split("VALID PROCESS NAMES")

                # If process names exist
                if len(help_text) == 2:
                    names = help_text[1].strip()
                    names = names.split('\n')
                    # Add each of the valid process names to the dict
                    for n in names:
                        n = n.strip()
                        executable[n] = {
                            'executable': i,
                            'multiple': True
                        }


                elif len(help_text) < 2:
                    name = i.split('_')
                    executable[name[0]] = {
                        'executable': i,
                        'multiple': False
                    }

        if process in executable:
            return dir_pattern().format(base_path, executable[process]['executable']), executable[process]['multiple']
        else:
            return None, None


    def update_db(self, process):
        """ Return command to update db config for specfied process """
        proc = process['proc']
        command = 'grep "/{0}.process" /apps/process/package/scripts/*_conf.post'.format(proc)

        try:
            db_command = check_output(command, shell=True)
        except CalledProcessError as e:
            # print(e
            db_command = ''


        # error = open("/home/twilliams/err.log", 'w')
        # out = open("/home/twilliams/out.log", 'w')

        db_command = db_command.split(':')[0]
        if db_command == '':
            return None
        else:
            try:
                ps = Popen(db_command, stdout=PIPE, stderr=PIPE)
                (output, error) = ps.communicate()
                returncode = ps.returncode
                return True
            except CalledProcessError as e:
                error.close()
                output.close()
                status = e.returncode
                print(e)
                return None



    def setup_alias(self, db_file, alias=None, level=1):
        """ Make sure proper alias exists. Create apm alias if it doesn't exist. """
        if not alias:
            als = 'apm' if not self.config['alias'] else self.config['alias']
            del alias
        else:
            als = alias
            del alias

        ######################################################################
        # Remove this code when vapmgr accepts -a argument
        ######################################################################
        if self.config['vap']:
            als = 'vapmgr'
        ######################################################################

        fp = open(db_file, 'r')
        contents = fp.read()
        fp.close()

        lines = contents.split('\n')

        for line in lines:
            words = line.split()
            if len(words) == 5 and words[0] != '#' and not words[0].startswith('#'):
                (alias, host, database, user, password) = words
                if alias == als:
                    break
        else:
            # Doesn't have the specified alias
            if als == 'apm':
                # Doesn't have alias 'apm'
                alias = 'apm'
                host = 'pgdb.dmf.arm.gov'
                database = 'dsdb_reproc'
                user = 'dsdb_data'

                # Ask user for password
                if level == 1:
                    print("Alias '{}' does not exist. Please enter a password for user '{}'".format(alias, user))

                password = getpass.getpass()

                if not password:
                    if level < 3:
                        print("Error: unable to create alias '{}' without passowrd.\nPlease enter a password".format(alias))
                        self.setup_alias(db_file, alias=alias, level=level+1)

                    else:
                        print("Error: unable to create alias '{}' without password. Please try again.".format(alias))
                        return False

                else:
                    # validate provided password
                    # Write temp file
                    tmp_db_file = dir_pattern(3).format(self.config['stage'], self.config['job'], '.db_connect.tmp')
                    fp = open(tmp_db_file, 'w')
                    fp.write('{}  {}  {}  {}  {}'.format(alias, host, database, user, password))
                    fp.close()

                    if self.validate_alias(alias, tmp_db_file):
                        os.remove(tmp_db_file)
                    else:
                        if level < 3:
                            os.remove(tmp_db_file)
                            print("Error: invalid password provided.\nPlease enter a password")
                            return self.setup_alias(db_file, alias=alias, level=level+1)

                        else:
                            os.remove(tmp_db_file)
                            print("Error: invalid password provided, please try again")
                            return False

                # Write alias to file
                db_creds = '{}  {}  {}  {}  {}\n'.format(alias, host, database, user, password)

                fp = open(db_file, 'a')
                fp.write(db_creds)
                fp.close()

            else:
                # Alias is specified but is not 'apm'
                print("Unable to find alias '{}'. Please update .db_connect and try again.".format(als))
                return False

        return True

    def validate_alias(self, alias, db_file):
        ''' Attempt to login to the database with the given alias to validate the password '''
        try:
            db = DSDB(conn_file=db_file, alias=alias)
            db.connect()
        except psycopg2.OperationalError as e:
            return False
        except Exception as e:
            raise e

        return True

    def parse_vap_log(self, log_file, start_time, end_time):
        status = 'Status:  '
        process = 'Process: '
        dates = 'Processing data: '

        entries = []

        if not os.path.exists(log_file):
            return False

        log = open(log_file, 'r')
        text = log.readlines()
        log.close()

        parse = False

        entry = None
        for k,line in enumerate(text):

            # Looking for open
            if line.startswith('**** OPENED:'):
                timeformat = "**** OPENED: %Y-%m-%d %X\n"
                linedate = datetime.strptime(line, timeformat)

                if linedate >= start_time and linedate <= end_time:
                    entry = {}
                    entry['line_number'] = k+1
                    parse = True

            # Looking for close
            elif parse and line.startswith('**** CLOSED:'):
                timeformat = "**** CLOSED: %Y-%m-%d %X\n"
                linedate = datetime.strptime(line, timeformat)

                if linedate >= start_time and linedate <= end_time:
                    entries.append(entry)
                    parse = False


            # Looking for status
            elif parse and line.startswith(status):
                result = line.replace(status, '')[:-1]
                entry['status'] = True if result == 'Successful' else False
                # entry['status'] = False if result == 'Successful' else True
                entry['message'] = result

            elif parse and line.startswith(process):
                result = line.replace(process, '')[:-1]
                entry['process'] = result

            elif parse and line.startswith(dates):
                result = line.replace(dates, '')[:-1]
                entry['dates'] = result


        return entries




    def parse_vapmgr_log(self, log_file, start_time, end_time, qc=False):
        section_div = "-----------------------------------\n";
        quicklooks = "QUICKLOOKS:\n"
        output = "OUTPUT DATA:\n"
        warning = "*** WARNING:"
        section = None

        entries = []

        if not os.path.exists(log_file):
            return False

        log = open(log_file, 'r')
        text = log.readlines()
        log.close()

        parse = False

        entry = None

        for k,line in enumerate(text):

            # Looking for open
            if line.startswith('****OPEN'):
                timeformat = "Time: %a %b %d %X %Y\n"
                linedate = datetime.strptime(text[k+1], timeformat)

                if linedate >= start_time and linedate <= end_time:
                    entry = {}
                    entry['line_number'] = k+1
                    parse = True

            # Looking for close
            elif parse and line.startswith('****CLOSED'):
                section = None
                timeformat = "Close time: %a %b %d %X %Y\n"
                linedate = datetime.strptime(text[k-1], timeformat)

                if linedate >= start_time and linedate <= end_time:
                    entries.append(entry)
                    parse = False

            # Looking for "vapmgr output"
            elif parse and line.startswith('RUNNING'):
                entry['running'] = line[:-1]

            elif parse and line == section_div:
                section = text[k+1]

            elif parse and section != None:
                if line == section or line == '':
                    pass

                # Looking for "Output Section"
                elif parse and section == output:
                    if line.startswith(warning):
                        entry['output'] = line.replace('*** ', '').replace(" ***\n", '')[:-1]

                # Looking for "Quicklook Section"
                elif parse and not qc and section == quicklooks:
                    if line.startswith(warning):
                        entry['quicklooks'] = line.replace('*** ', '').replace(" ***\n", '')

        return entries


    def vapmgr_log_results(self, entry, key):
        output = None
        if key not in entry:
            output = '%s... Successful' % entry['running']
        else:
            # print(error message
            output = """ERROR
    %s
    For more information see the log entry starting on line %d of the following log file:
    %s""" % (entry[key], entry['line_number'], entry['log_file'])
        return output

class Ingest(threading.Thread):
    def __init__(self, options, config, key):
        parts = options['input'].split('/')[1].split('.')[0]
        self.site = parts[:3]
        self.instrument = options['proc']
        self.facility = self.get_facility(parts)

        self.flags = config['iflags']

        date = datetime.now()
        year = str(date.year).zfill(4)
        month = str(date.month).zfill(2)

        log_pattern = "{0}/{1}/logs/{2}/%s_logs/{2}{3}{4}/{2}{3}{4}.{5}{6}00.000000.%s"
        log_pattern = log_pattern.format(config['stage'], config['job'], self.site, self.instrument, self.facility, year, month)

        self.format = "%Y-%m-%d %H:%M:%S"
        self.key = key
        self.error = False
        self.result = None
        self.logfile = log_pattern % ('proc', 'Ingest')
        self.renamelog = log_pattern % ('instr', 'renamelog')
        self.config = config
        self.options = options
        self.ingest = options['ingest']
        self.times = {
            'ingest': {
                'start': None,
                'end': None
            },
            'rename': {
                'start': None,
                'end': None,
            }
        }

        threading.Thread.__init__(self)

    def run(self):
        self.times['ingest']['start'] = datetime.now().replace(microsecond=0)

        self.command = []
        self.command.append(self.options['ingest'])
        self.command.append("-a")
        if not self.config['alias']:
            self.command.append('apm')
        else:
            self.command.append(self.config['alias'])

        # Add the site to the command
        self.command.append('-s')
        self.command.append(self.site)

        # Add the facility to the command
        self.command.append('-f')
        self.command.append(self.facility)

        # If multiple add -n and the instrument to the command
        if self.options['multiple']:
            self.command.append('-n')
            self.command.append(self.instrument)

        # Add the final option
        self.command.append('-R')

        # Add additional user specified flags
        if self.flags != None and type(self.flags == list):
            for i in self.flags:
                self.command.append('-%s' % i)

        ps = Popen(self.command, stdout=PIPE, stderr=PIPE)
        (output, error) = ps.communicate()
        returncode = ps.returncode

        self.times['ingest']['end'] = datetime.now().replace(microsecond=0)

        if returncode != 0:

            self.error = "Error running ingest (%s)" % ' '.join(self.command)
            self.result = None
            return
        else:
            self.stdout = output
            self.stderr = error

        ##################################################
        # Parse the log file.
        ##################################################
        if not os.path.exists(self.logfile):
            self.error = "ERROR: Unable to find log file"
            return

        log = open(self.logfile, 'r')
        text = log.readlines()
        log.close()

        parse = False
        names = {}

        for k,line in enumerate(text):
            if line.startswith('**** OPENED: '):
                timeformat = "**** OPENED: %Y-%m-%d %X\n"
                linedate = datetime.strptime(line, timeformat)

                if linedate >= self.times['ingest']['start'] and linedate <= self.times['ingest']['end']:
                    parse = True

            elif parse and line.startswith('**** CLOSED: '):
                timeformat = "**** CLOSED: %Y-%m-%d %X\n"
                linedate = datetime.strptime(line, timeformat)

                if linedate >= self.times['ingest']['start'] and linedate <= self.times['ingest']['end']:
                    parse = False


            elif parse and line.startswith("Renaming:   "):
                old_path = line.replace('Renaming:   ', '').replace("\n", '')
                new_path = text[k + 1].replace(' -> to:     ', '').replace("\n", '')

                parts = old_path.split('/')
                site = parts[-3]
                sif = parts[-2]

                old_name = parts[-1]
                new_name = new_path.split('/')[-1]

                if site not in names:
                    names[site] = {}

                if sif not in names[site]:
                    names[site][sif] = {}

                names[site][sif][old_name] = new_name

        folder = dir_pattern(5).format(self.config['stage'], self.config['job'], 'collection', self.config['site'], '{}{}{}.00'.format(self.config['site'], self.config['instrument'], self.config['facility']))
        listdir = os.listdir(folder)


        if len(listdir) > 0:
            ##################################################
            # Run Rename Raw
            ##################################################
            if self.config['instrument'] in self.get_rename_raw_process_list():
                self.times['rename']['start'] = datetime.now().replace(microsecond=0)
                command = ['%s/rename_raw' % binpath, '-s', self.config['site'], '-f', self.config['facility'], self.config['instrument']]
                ps = Popen(command, stdout=PIPE, stderr=PIPE)
                (output, error) = ps.communicate()
                returncode = ps.returncode
                self.times['rename']['end'] = datetime.now().replace(microsecond=0)

                if returncode != 0:
                    self.result = names
                    self.error = error
                    return

                ##################################################
                # Parse rename_raw log file
                ##################################################
                if not os.path.exists(self.renamelog):
                    self.result = names
                    self.error = "renamelog does not exist"
                    return

                lf = open(self.renamelog, 'r');
                logs = lf.readlines()
                lf.close()

                parse = False

                for k,line in enumerate(logs):
                    if line.startswith("****OPEN"):
                        i = k + 1
                        timeline = logs[i]
                        timeformat = "Time: %a %b %d %X %Y\n"
                        opentime = datetime.strptime(timeline, timeformat)

                        if opentime >= self.times['rename']['start'] and opentime <= self.times['rename']['end']:
                            parse = True

                    elif parse and line.startswith("Close time: "):
                        i = k
                        timeformat = "Close time: %a %b %d %X %Y\n"
                        closetime = datetime.strptime(line, timeformat)

                        if closetime >= self.times['rename']['start'] and closetime <= self.times['rename']['end']:
                            parse = False

                    elif parse and line.startswith("Renamed: "):
                        old_path = line.replace("Renamed: ", "").replace("\n", '').split(' (')[0]
                        new_path = logs[k + 1].replace(" ->      ", '').replace("\n", '')

                        parts = old_path.split('/')
                        site = parts[-3]
                        sif = parts[-2]

                        old_name = parts[-1]
                        new_name = new_path.split('/')[-1]

                        if site not in names:
                            names[site] = {}

                        if sif not in names[site]:
                            names[site][sif] = {}

                        names[site][sif][old_name] = new_name


                ##################################################
                # Check for additional files
                ##################################################
                listdir = os.listdir(folder)
                if len(listdir) > 0:
                    self.result = names
                    self.error = "rename_raw did not move all the files in {}".format(folder)
                    return

        self.result = names
        return

    def get_rename_raw_process_list(self):
        path = binpath
        binary = "rename_raw"
        command = dir_pattern().format(path, binary)

        if not os.path.exists(command):
            print("Cannot find Rename_raw")
            return False

        helptext = None
        try:
            ps = Popen([command, '-h'], stdout=PIPE, stderr=PIPE)
            (output, error) = ps.communicate()
            returncode = ps.returncode
            helptext = output
        except CalledProcessError as e:
            error.close()
            output.close()
            status = e.returncode
            helptext = e.output

        if helptext == None:
            print("Unable to get rename_raw help text")
            return False

        helptext = helptext.split("\n\n")

        processes = None

        for k,v in enumerate(helptext):
            if v == "SUPPORTED PROCESSES:" and (k+1) < len(helptext):
                processes = helptext[k+1]

        if processes == None:
            print("Unable to get list of processes from rename_raw")
            return False

        processes = processes.split("\n")

        for k,v in enumerate(processes):
            v = re.sub(r'\s+', ' ', v).strip().split(' ')
            if v[0] == "*":
                processes[k] = v[1].lower()
            else:
                processes[k] = v[0].lower()

        processes = list(set(processes))
        processes.sort()

        return processes


    def get_facility(self, process):
        ''' Parse the facility from the process name '''
        facility = []
        for i in range(len(process) -1, -1, -1):
            try:
                int(process[i])
                facility.append(process[i])
                continue
            except ValueError as e:
                facility.append(process[i])
                break
            except Exception as e:
                raise e
        facility.reverse()
        return ''.join(facility)
