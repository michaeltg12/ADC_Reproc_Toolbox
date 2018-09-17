#!/apps/base/python3/bin/python3

import os
import shutil
import time
import json
import tarfile
import smtplib
import sys

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from glob import glob

from subprocess import Popen
from subprocess import PIPE
from subprocess import CalledProcessError

from apm.pmanager.manager import PluginManager

# Import classes
from apm.classes.db import DB
from apm.classes.ui import UI
from apm.classes.files import Files

# Import commands
from apm.commands.process import Process

# Import functions
from apm.classes.system import jprint
from apm.classes.system import dir_pattern
from apm.classes.system import is_number
from apm.classes.system import update_archive
from apm.classes.system import update_env
from apm.classes.system import convert_date_to_timestamp

from datetime import datetime

##################################################
# For production code,
# remove all instances of this variable
##################################################
global DEVEL
DEVEL = False
##################################################



##################################################
# Future Development
##################################################
# Upload the deletion list to the appropriate EWO
##################################################

class Remove:
    """ Delete the old processed (and maybe raw) files from the archive """

    def __init__(self, config, files):
        """ Initialize with args """
        global DEVEL
        DEVEL = config['devel']

        self.config = config
        self.files = files
        self.maps = {
            "orig": { "tar": {}, "raw": {}, "history": {}, "names": {} },
            "new": { "tar": {}, "raw": {} }
        }
        self.archive = {
            "add": { "tar": [], "raw": {} },
            "remove": { "tar": [], "raw": {} }
        }


    def run(self):
        """ Run the remove portion of the cleanup phase """
        self.start_time = datetime.now()
        if not self.config['cleanup_status']['review']['status']:
            print("\nData must be reviewed before it can be removed from the archive.")
            self.config['exit'] = True
            return self.config, self.files

        stage = self.config['stage']
        job = self.config['job']

        del_file = '%s.deletion-list.txt' % job
        job_folder = dir_pattern().format(stage, job)

        exists = False
        replace = False

        # Check to see if deletion file exists
        if os.path.exists(dir_pattern().format(job_folder, del_file)):
            exists = True
            ui = UI()
            replace = ui.yn_choice('%s already exists.\n Would you like to overwrite this file?' % del_file, 'n')

        if exists and not replace:
            return self.config, self.files

        # Either file doesn't exist or user has chosen to overwrite it
        # Create <job>.deletion-list.txt file

        # Reset statuses for this run
        for k in self.config['cleanup_status']['remove']:
            self.config['cleanup_status']['remove'][k] = False

        contents = []

        ##################################################
        # Get list of files from datastream folder
        ##################################################
        datastreams = []
        datastream_path = dir_pattern(3).format(stage, job, 'datastream')
        for site in os.listdir(datastream_path):
            path = dir_pattern().format(datastream_path, site)
            for folder in os.listdir(path):
                abs_folder = dir_pattern().format(path,folder)
                if os.path.isdir(abs_folder) and not os.path.islink(abs_folder):
                    datastreams.append(folder)

        # Processed files
        p_files = {}
        for k,v in enumerate(datastreams):
            if v not in p_files:
                p_files[v] = []

            p_files[v] = os.listdir(dir_pattern(3).format(datastream_path, site, v))

        ##################################################
        # Update the local copy of the archive db
        ##################################################
        # print("\nUpdating list of files stored at the archive..."
        # if not DEVEL:
        # 	update_archive(datastreams)
        # print("Done"
        ##################################################
        # Get list of files from archive db
        ##################################################
        print("\nRetrieving list of relevant files stored at the archive...", end="")
        # Connect to the database
        archive_files = {}
        db_file = '/apps/ds/conf/datainv/.db_connect'
        alias = 'inv_read'

        if not os.path.exists(db_file):
            print("\nUnable to connect to the archive database. Please try again later.")
            self.config['exit'] = True
            return self.config, self.files

        db = DB(self.config, db_file=db_file, alias=alias)

        # Store the query
        query = "SELECT * FROM get_remote_files_by_tag('%s') WHERE file_stamp >= %d AND file_stamp <= %d AND file_active = true ORDER BY file_stamp, file_version;"

        # List the column names so the values can be mapped in a dictionary
        cols = ['file_tag', 'file_name', 'file_version', 'file_size', 'file_stored', 'file_md5', 'file_stamp', 'file_checked', 'file_active']

        # convert the start and end dates to a unix timestamp
        start = convert_date_to_timestamp(self.config['begin'])
        end = convert_date_to_timestamp(self.config['end'])

        # Query the database for each of the datastreams
        for k,v in enumerate(datastreams):
            args = (v, start, end)
            result = db.query(query % args, columns=cols)

            if len(result) > 0:
                archive_files[v] = result
            else:
                print("\nNo results for %s" % v)

        # Unset loop variables
        if len(datastreams) > 0:
            del k, v, args, result

        print("Done")

        print("Map original tar bundle structure...",end="")
        self.maps['orig']['tar'] = self.get_tar_structure(dir_pattern(3).format(stage, job, "file_comparison/tar"))
        print("Done")

        if self.config['ingest']:
            # Add files to the list that should be removed from the archive
            print("\nGenerating list of files to remove from the archive...")
            sys.stdout.flush()
            ##################################################
            # Compare raw files to see if they changed
            ##################################################

            # Setup Variables for the following code to use
            file_history = self.files								# List of files as they have traveled from tar file through the ingest. Mapped by their current name

            raw_streams = []												# The datastreams that contain the raw files (ex. sgpmfrsrC1.00)

            # Setup the paths for the ingested and untarred raw files
            new_folder = dir_pattern(3).format(stage, job, 'datastream')
            old_folder = dir_pattern(3).format(stage, job, 'file_comparison/raw')

            raw_files = {}													# container to hold a mapping of raw files in the <job>/datastream folder
            archive_tars = {}												# Container to hold a list of tar files at the archive

            bundle_data = False											# Does the raw data in "datastream" need to be bundled


            # Get a list of the sites in "datastream"
            for site in os.listdir(new_folder):
                raw_files[site] = {}


            # Establish a structure for the raw files in "datastream"
            #  This structure follows the same pattern as 'file_history'
            for site in raw_files:
                for instrument in glob(dir_pattern(3).format(new_folder, site, '*.00')):
                    instrument = instrument.split('/')[-1]
                    raw_files[site][instrument] = {}
                    raw_streams.append(instrument)
                    for f in os.listdir(dir_pattern(3).format(new_folder, site, instrument)):
                        raw_files[site][instrument][f] = {}

            # Compare all of the existing files
            #  By comparing existing files instead of files that were unpacked
            #  we make sure to include all files and can check for files that are not being tracked
            # 		(This should never happen)
            c = Files(self.config)
            for i,s in raw_files.items(): 		# i = key, s = site
                for j,p in s.items():   				# j = key, p = process/instrument
                    pbar = UI()
                    percent = 0
                    pbar.progress(percent)
                    count = len(p)
                    l = 1
                    for k,f in p.items(): 				# k = key, f = file
                        # Compare the file in 'datastream' with its counterpart in 'file_comparison/raw'
                        if k not in file_history[i][j]: # This if statement should never evaluate "True"
                            # File is not being tracked
                            # Raw files in datastream need to be rebundled
                            bundle_data = True

                            # Tar file with this raw file needs to be added to the archive
                            # Make sure the site is in the dict
                            if i not in self.archive['add']['raw']:
                                self.archive['add']['raw'][i] = {j: {}}

                            # Make sure the process is in the dict
                            if j not in self.archive['add']['raw'][i]:
                                self.archive['add']['raw'][i][j] = {}

                            # Add the file to the dict
                            self.archive['add']['raw'][i][j][k] = {}

                            continue # Go to the next iteration of the loop (file cannot be compared because there is no counterpart)

                        # Compare the ingested raw file with the unpacked raw file
                        file_path = dir_pattern(5).format(stage, job, '%s', i, j)
                        file_1 = dir_pattern().format(file_path % 'datastream', k)
                        file_2 = dir_pattern().format(file_path % 'file_comparison/raw', file_history[i][j][k]['original_name'])
                        if not c.is_same_file(file_1, file_2):
                            # The files are not the same. Raw files in datastream need to be rebundled
                            bundle_data = True

                            # Ensure self.archive['remove']['raw'] has the proper structure
                            if i not in self.archive['remove']['raw']:
                                self.archive['remove']['raw'][i] = {j: []}

                            if j not in self.archive['remove']['raw'][i]:
                                self.archive['remove']['raw'][i][j] = []

                            self.archive['remove']['raw'][i][j].append(k)

                            # Make self.archive['remove']['raw'][i][j] a unique list
                            self.archive['remove']['raw'][i][j] = list(set(self.archive['remove']['raw'][i][j]))

                        percent = int((float(l) / float(count)) * 100)
                        pbar.progress(percent)
                        l = l + 1

                    percent = int((float(l) / float(count)) * 100)
                    pbar.progress(percent)
                    print("")
                    sys.stdout.flush()

            # Unset loop variables
            if len(raw_files) > 0:
                del i, j, k, s, p, f, c

            if bundle_data:
                # Fill self.maps['orig']['history'] and bundle the data
                for site in file_history:
                    if site not in self.maps['orig']['history']:
                        self.maps['orig']['history'][site] = {}

                    for process in file_history[site]:
                        if process not in self.maps['orig']['history'][site]:
                            self.maps['orig']['history'][site][process] = {}

                        for f,d in file_history[site][process].items():
                            if d['original_name'] not in self.maps['orig']['history'][site][process]:
                                self.maps['orig']['history'][site][process][d['original_name']] = d

                # Find any orig/bad files and copy them over (correcting names as necessary)
                other_files_path = dir_pattern(3).format(stage, job, 'file_comparison/raw/%s/%s/%s')
                for i,s in self.maps['orig']['history'].items():
                    for j,p in s.items():
                        bad_files = glob(other_files_path % (i, j, '*.bad.*'))
                        orig_files = glob(other_files_path % (i, j, '*.orig.*'))
                        edit_files = glob(other_files_path % (i, j, '*.edit*.*'))

                        # if len(orig_files) > 0:
                        # 	pbar = UI()
                        # 	count = len(orig_files)
                        # 	pbar.progress(0)

                        for k, of in enumerate(orig_files):
                            oFile = of.split('/')[-1]
                            if oFile in p:
                                key = oFile.replace('orig', 'raw')
                                if key in p:
                                    filename = p[key]['current_name'].replace('.raw.', '.orig.')
                                    filename = dir_pattern(6).format(stage, job, 'datastream', i, j, filename)
                                    shutil.copy(of, filename)

                            del k, of, oFile, key

                        # print(""
                        # sys.stdout.flush()

                        # if len(bad_files) > 0:
                        # 	pbar = UI()
                        # 	count = len(bad_files)
                        # 	pbar.progress(0)
                        for k, bf in enumerate(bad_files):
                            bFile = bf.split('/')[-1]
                            if bFile in p:
                                key = bFile.replace('bad', 'raw')

                                if key in p:
                                    filename = p[key]['current_name'].replace('.raw.', '.bad.')
                                else:
                                    filename = bFile

                                filename = dir_pattern(6).format(stage, job, 'datastream', i, j, filename)
                                shutil.copy(bf, filename)

                            # # Update progress bar
                            # pbar.progress(int((float(k + 1) / float(count)) * 100))

                            del k, bf, bFile, key


                        # print(""
                        # sys.stdout.flush()

                        # if len(edit_files) > 0:
                        # 	pbar = UI()
                        # 	count = len(edit_files)
                        # 	pbar.progress(0)
                        for k, ef in enumerate(edit_files):
                            eFile = ef.split('/')[-1]
                            temp = eFile.split('.')
                            edit = None
                            for t in temp:
                                if temp[t].startswith('edit'):
                                    edit = temp[t]
                                    break

                            if eFile in p:
                                key = eFile.replace(edit, 'raw')
                                if key in p:
                                    filename = p[key]['current_name'].replace('.raw.', ".%s." % edit)
                                    filename = dir_pattern(6).format(stage, job, 'datastream', i, j, filename)
                                    shutil.copy(ef, filename)

                            # # Update progress bar
                            # pbar.progress(int((float(k + 1) / float(count)) * 100))

                            del k, ef, eFile, edit, t, key

                        # print(""
                        # sys.stdout.flush()

                        del j, p
                    del i, s

                # Create any needed orig files
                print("Create needed orig files...")
                sys.stdout.flush()

                for i,s in self.archive['remove']['raw'].items():
                    for j,p in s.items():
                        path = dir_pattern(5).format(stage, job, "datastream", i, j)
                        k = 0
                        count = len(p)
                        for f in p:
                            orig = f.replace('.raw.', '.orig.')
                            if not os.path.exists(dir_pattern().format(path, orig)):
                                src = dir_pattern(6).format(stage, job, "file_comparison/raw", i, j, file_history[i][j][f]['unpacked_name'])
                                dst = dir_pattern().format(path, orig)
                                shutil.copy(src, dst)
                                # del src, dst
                            percent = int((float(k) / float(count)) * 100)
                            pbar.progress(percent)
                            k = k + 1

                        if percent < 100:
                            percent = int((float(k) / float(count)) * 100)
                            pbar.progress(percent)
                        print("")

                    # Unset loop variables
                    # del i, s, j, p, path, f, orig, src, dst

                print("Done")

                # Bundle the data
                self.bundle_raw_data(raw_streams)
                self.config['cleanup_status']['remove']['files_bundled'] = True

                print("Map new tar bundle structure...",end="")
                self.maps['new']['tar'] = self.get_tar_structure(dir_pattern(3).format(stage, job, "datastream"))
                print("Done")

                print("")
                print("Mapping raw structure from original tar files...",end="")
                self.maps['orig']['raw'] = self.map_raw_structure(self.maps['orig']['tar'])
                print("Done")

                print("Mapping raw structure from new tar files...",end="")
                self.maps['new']['raw'] = self.map_raw_structure(self.maps['new']['tar'])
                print("Done")

                ##################################################
                # Find all of the tar files that need
                #   to be removed from the archive
                ##################################################
                print("")
                print("Generating list of tar files to be removed from the archive...")
                sys.stdout.flush()

                # Find all of the tar files that need to be removed from the archive
                for i,s in self.archive['remove']['raw'].items():
                    percent = 0
                    for j,p in s.items():
                        pbar = UI()
                        count = len(p)
                        pbar.progress(percent)
                        k = 1
                        for raw_file in p:
                            tar_files = self.find_original_tar_bundle(file_history[i][j][raw_file]['original_name'], i, j)
                            for f in tar_files:
                                if f not in self.archive['remove']['tar']:
                                    tar = {
                                        'site': i,
                                        'instrument': j,
                                        'file_name': f
                                    }
                                    self.archive['remove']['tar'].append(tar)
                            percent = int((float(k) / float(count)) * 100)
                            pbar.progress(percent)
                            k = k + 1

                        if percent == 99:
                            pbar.progress(100)
                        print("")
                        sys.stdout.flush()

                # Unset loop variables
                if len(self.archive['remove']['raw']) > 0:
                    del i, s, j, p, raw_file, tar_files, f, tar

                print("Done")

                ##################################################
                # Find all of the tar files that need
                #   to be added to the archive
                ##################################################
                print("")
                print("Generating list of tar files to be added to the archive...")
                pbar = UI()
                pbar.progress(0)
                count = len(self.archive['remove']['tar'])
                percent = 0
                i = 1

                # Find all of the tar files that need to be added to the archive
                for tar_file in self.archive['remove']['tar']:
                    files = self.find_all_files_from_original_tar(tar_file['file_name'], tar_file['site'], tar_file['instrument'])
                    for f in files:
                        temp = f
                        if not any(d['file_name'] == temp for d in self.archive['add']['tar']):
                            tar = {
                                'site': tar_file['site'],
                                'instrument': tar_file['instrument'],
                                'file_name': f
                            }

                            self.archive['add']['tar'].append(tar)
                    percent = int((float(i) / float(count)) * 100)
                    pbar.progress(percent)
                    i = i+1

                if percent == 99:
                    pbar.progress(100)
                print("")
                sys.stdout.flush()

                # Unset loop variables
                if len(self.archive['remove']['tar']) > 0:
                    del tar_file, files, f

                for i,s in self.archive['add']['raw'].items():
                    for j,p in s.items():
                        pbar = UI()
                        pbar.progress(0)
                        percent = 0
                        count = len(p)
                        i = 1
                        for raw_file, info in p.items():
                            tar_files = self.find_original_tar_bundle(raw_file, i, j)
                            for f in tar_files:
                                temp = f
                                if not any(d['file_name'] == temp for d in self.archive['add']['tar']):
                                    tar = {
                                        'site': i,
                                        'instrument': j,
                                        'file_name': f
                                    }
                                    self.archive['add']['tar'].append(tar)
                            percent = int((float(i) / float(count)) * 100)
                            pbar.progress(percent)
                            i = i + 1

                        if percent == 99:
                            pbar.progress(100)
                        print("")
                        sys.stdout.flush()


                # Unset loop variables
                if len(self.archive['add']['raw']) > 0:
                    del i, s, j, p, raw_file, info, tar_files

                    if 'f' in locals():
                        del f
                    if 'tar' in locals():
                        del tar

                ##################################################
                # Update archive db for raw datastream
                ##################################################
                if not DEVEL:
                    update_archive(raw_streams)

                # Get list of tar files from the archive
                for k,v in enumerate(raw_streams):
                    stream = dir_pattern(5).format(stage, job, 'file_comparison/tar', site, v)
                    files = os.listdir(stream)
                    files = "','".join(files)
                    args = (v, files)
                    query = "SELECT * FROM get_remote_files_by_tag('%s') WHERE file_active = true and file_name in ('%s')"
                    result = db.query(query % args, columns=cols)

                    if len(result) > 0:
                        archive_tars[v] = result
                    else:
                        print("\nNo results for %s" % v)

                # Unset loop variables
                del k, v, args, result

                print("Done generating tar file list")

                # Find data on tar files in list and add it to 'contents'
                print("")
                print("Adding tar files to deletion list...",end="")

                for f in self.archive['remove']['tar']:
                    files = archive_tars[f['instrument']]
                    for k,v in enumerate(files):
                        if v['file_name'] == f['file_name']:
                            index = k
                            break
                    else:
                        print("\nUnable to find %s in archive db" % f['file_name'])
                        self.config['exit'] = True
                        return self.config, self.files

                    temp = f['file_name']
                    if not any(d['filename'] == temp for d in contents):
                        contents.append({
                            'datastream': f['instrument'],
                            'filename': f['file_name'],
                            'hash': files[index]['file_md5'],
                            'version': files[index]['file_version']
                        })

                if len(self.archive['remove']['tar']) > 0:
                    del f, files, k, v, index
                    pass

                print("Done")

        # Set proper file names in deletion list
        print("Setting proper file names in deletion list...",end="")
        for k,v in archive_files.items():
            if k.split('.')[-1] != '00':
                for key,f in enumerate(v):
                    if f['file_name'] not in p_files[k]:
                        temp = f['file_name']
                        pass
                        if not any(d['filename'] == temp for d in contents):
                            contents.append({
                                'datastream': k,
                                'filename': f['file_name'],
                                'hash': f['file_md5'],
                                'version': f['file_version']
                            })

        print("Done")

        # Store the list of files that need to be archived to file
        archive_json_file = dir_pattern(3).format(stage, job, 'archive.json')
        fp = open(archive_json_file, 'w')
        fp.write(json.dumps(self.archive['add']['tar'], indent=2, sort_keys=False, separators=(',', ': ')))
        fp.close()
        del fp

        # Update the saved status
        self.config['cleanup_status']['remove']['archive_list'] = True

        ##################################################
        # Write the results to file
        # (Use '\r\n' for Windows line endings)
        ##################################################
        print("\nEmailing deletion list...",end="")
        sys.stdout.flush()
        file_contents = []

        contents = sorted(contents, key=self.get_sort_key)

        for line in contents:
            l = "%s.v%s %s" % (line['filename'], line['version'], line['hash'])
            file_contents.append(l)

        fp = open(dir_pattern().format(job_folder, del_file), 'w')
        fp.write("\r\n".join(file_contents))
        fp.close()
        del fp

        # Update the saved status
        self.config['cleanup_status']['remove']['deletion_list'] = True

        # Send the deletion list to the appropriate place (currently email, may be upload at a later time)
        self.email_del_list("%s.deletion-list.txt" % self.config['job'])
        # self.upload_del_list()

        print("Done")

        # Update the saved status
        self.config['cleanup_status']['remove']['status'] = True

        duration = datetime.now() - self.start_time
        print(duration)

        return self.config, self.files

    def bundle_raw_data(self, datastreams):
        """ Bundle the raw data in <job>/datastream/<site>/<site><instrument><facility>.00 """
        if type(datastreams) == str:
            datastreams = [datastreams]

        if type(datastreams) != list:
            return False

        # Update env variables so bundle_data will tar the right files and put the tar files in the right place
        p = Process(self.config, self.files)
        print("\nUpdating environment variables...",end="")
        if update_env(dir_pattern().format(self.config['stage'], self.config['job'])):
            print("Done")
        else:
            print("Failed")
            return False

        # Validate the bundle alias exists
        home = os.path.expanduser('~')
        db_file = dir_pattern().format(home, ".db_connect")
        p.setup_alias(db_file, 'bundle')

        # Run this process for each of the passed streams
        print("Bundling raw data...",end="")
        sys.stdout.flush()
        for stream in datastreams:
            # split the stream string to get the needed information
            stream = stream.split('.')[0]

            for i,e in reversed(list(enumerate(stream))):
                if not is_number(e):
                    fac = i
                    break
            else:
                print("Failed: Could not separate facility from %s" % stream)
                return False

            s = stream[0:3]
            i = stream[3:fac]
            f = stream[fac:]

            # Build the command
            command = ['bundle_data', '-e', '-s', s, '-f', f, i]

            # Run the command
            try:
                ps = Popen(command, stdout=PIPE, stderr=PIPE)
                ps.communicate()
                returncode = ps.returncode
                if returncode != 0:
                    print("Bad Return...",end="")
                    print("Failed")
                    return False
            except CalledProcessError as e:
                print("Called Process Error...",end="")
                print("Failed")
                return False
            except Exception as e:
                raise e

        print("Done")
        return True

    def get_sort_key(self, obj):
        ''' return the key to be sorted '''
        return obj['filename']

    def split_filename(self, name):
        """ Split a filename from it's path """
        parts = name.split('/')
        path = '/'.join(parts[:-1])
        filename = parts[-1]
        return (path, filename)


    def find_original_tar_bundle(self, raw_file, site, instrument):
        """ Find the original tar bundle this file was inside """
        return self.maps['orig']['raw'][site][instrument][raw_file]

    def find_all_files_from_original_tar(self, orig_tar, site, instrument):
        """ Find all of the files from the original tar bundle in the tar bundles in datastream """
        # Returns an array of tar file names that contain all of the old tar files
        orig_raw_files = {}
        new_tar_files = []

        # Get list of files in orig_tar
        names = self.maps['orig']['tar'][site][instrument][orig_tar]

        for name in names:
            path, raw_name = self.split_filename(self.maps['orig']['history'][site][instrument][name]['current_name'])
            orig_raw_files[name] = self.maps['new']['raw'][site][instrument][raw_name]

        for raw_file, tar_files in orig_raw_files.items():
            for v in tar_files:
                # if v not in new_tar_files:
                new_tar_files.append(v)

        # Make the list unique and return the list
        return list(set(new_tar_files))

    def get_all_files_from_tar(self, tar_file, path):
        """ Get a list of all the file names in the specified tar file """
        tar = tarfile.open("%s/%s" % (path, tar_file), 'r')
        names = tar.getnames()
        tar.close()

        return names


    def get_tar_structure(self, path):
        """
            Return the structure of the files contained within the tar files at the given location
            The given path should be the where a site directory is located
            Example: datastream should be provided
                job/datastream/site/process/list_of_tar_files
        """
        global DEVEL
        file_list = {}
        for site in os.listdir(path):
            file_list[site] = {}
            for process_path in glob(dir_pattern(3).format(path, site, '*.00')):
                process = process_path.split('/')[-1]
                file_list[site][process] = {}
                tar_file_list = os.listdir(process_path)

                if DEVEL:
                    print("Retrieving file list for %s" % process)
                    pbar = UI()
                    pbar.progress(0)
                    count = len(tar_file_list)
                    i = 1

                for tar_file in tar_file_list:
                    if not os.path.isdir(dir_pattern().format(process_path, tar_file)):
                        file_list[site][process][tar_file] = self.get_all_files_from_tar(tar_file, process_path)
                    if DEVEL:
                        pbar.progress(int((float(i)/float(count)) * 100))
                        i = i + 1

                if DEVEL:
                    print("")
        return file_list

    def map_raw_structure(self, tar_structure):
        ''' Map the raw structure based on the tar structure '''
        global DEVEL
        raw_structure = {}

        if DEVEL:
            ui = UI()
            percent = 0

        for i,s in tar_structure.items():
            if i not in raw_structure:
                raw_structure[i] = {}

            process_count = len(s)
            process_index = 0

            for j,p in s.items():
                if j not in raw_structure[i]:
                    raw_structure[i][j] = {}

                if DEVEL:
                    print("Mapping process: %s..." % j)
                    process_index = 1 + process_index
                    file_count = len(p)
                    file_index = 0
                    process_factor = (float(process_index) / float(process_index))
                for k,tar in p.items():
                    if DEVEL:
                        percent = int(((float(file_index) / float(file_count)) * 100) * process_factor)
                        ui.progress(percent)
                    for raw in tar:
                        if raw not in raw_structure[i][j]:
                            raw_structure[i][j][raw] = []
                        raw_structure[i][j][raw].append(k)

                    if DEVEL:
                        file_index = 1 + file_index

                if DEVEL and percent == 99:
                    ui.progress(100)
                    print("")

        return raw_structure

    def upload_del_list(self):
        ##################################################
        # Place code here to upload deletion list to
        #  Extraview/ServiceNow
        #
        # Ask user if they want to upload the file
        # If yes, upload the file to the appropriate service
        # Need to know if the upload location will be
        #  hard coded or if it will be provided
        #  by the user
        # We may be able to upload the file to ServiceNow
        #  for record purposes, while also reaching
        #  directly to the archive to trigger the files
        #  to be deleted directly (or at least some
        #  notification method).
        ##################################################
        pass

    def email_del_list(self, del_file):
        global DEVEL
        # Get the directory where the job and deletion file are stored
        job_dir = dir_pattern().format(self.config['stage'], self.config['job'])

        # Get the contents of the deletion list
        cwd = os.getcwd()
        os.chdir(job_dir)
        f = open(del_file, 'r')
        attach_file = f.read();
        f.close()
        os.chdir(cwd)

        # Setup the email variables
        email_from = 'apm@arm.gov'
        email_to = ['dmfoper@arm.gov']
        # email_to = ['thom.williams@pnnl.gov']

        if DEVEL:
            email_to.append('thom.williams@pnnl.gov')

        text = [
            'The deletion list for %s is attached.' % self.config['job'],
            'Once the files have been deleted, run the following commands:',
            '',
            'cd %s' % job_dir,
            'apm archive -j %s' % self.config['job'],
            'Attachment:',
            del_file,
        ]
        text = '\n'.join(text)

        ############################################################
        # Create a message
        msg = MIMEMultipart()
        msg['From'] = email_from
        msg['To'] = ', '.join(email_to)
        msg['Subject'] = "APM: %s deletion list" % self.config['job']

        # Add the body of the message
        msgText = MIMEText(text)
        msg.attach(msgText)

        # Add the attachment
        attachment = MIMEText(attach_file)
        attachment.add_header('Content-Disposition', 'attachment', filename=del_file)
        msg.attach(attachment)

        # Send the message
        s = smtplib.SMTP('localhost')
        s.sendmail(email_from, email_to, msg.as_string())
        s.quit()

