#!/apps/base/python3/bin/python3

import os
import json

from apm.pmanager.manager import PluginManager

from apm.classes.db import DB
from apm.classes.files import Files

from apm.classes.system import dir_pattern
from apm.classes.system import jprint
from apm.classes.system import convert_date_to_timestamp
from apm.classes.system import update_archive

############################################################
# Set Development flag
############################################################
global DEVEL
DEVEL = False
############################################################
# {
# 	"status": False,
# 	"files_archived": False,
# 	"files_cleaned_up": False,
# }

class Cleanup:
    """ Cleanup data files before archiving job information """

    def __init__(self, config, files=None):
        """ Initialize with args """
        global DEVEL
        DEVEL = config['devel']

        self.config = config
        self.files = files
        self.db = DB(self.config)


    def run(self):
        """ Run the cleanup portion of the cleanup phase """
        if self.config['cleanup_status']['archive']['status'] != True:
            print("Data files must be archived before they can be cleaned up.")
            self.config['exit'] = True
            return self.config, self.files

        stage = self.config['stage']
        job = self.config['job']

        ################################################################################
        # Update local archive database
        ################################################################################
        if not self.config['cleanup_status']['cleanup']['files_archived']:
            print("Updating local copy of the archive...",end="")
            # Setup the datastreams to update
            datastreams = []
            datastream_path = dir_pattern(3).format(stage, job, 'datastream')
            for site in os.listdir(datastream_path):
                path = dir_pattern().format(datastream_path, site)
                for folder in os.listdir(path):
                    abs_folder = dir_pattern().format(path,folder)
                    if os.path.isdir(abs_folder) and not os.path.islink(abs_folder):
                        datastreams.append(folder)


            # Update the local copy of the archive db
            if not DEVEL:
                update_archive(datastreams)

            print("Done")
            ################################################################################
            # Verify that all files to be added to the archive, were added
            ################################################################################
            print("Verifying processed and bundled files have been archived...",end="")
            cwd = os.getcwd()

            archive_files = {}
            db_file = '/apps/ds/conf/datainv/.db_connect'
            alias = 'inv_read'

            if not os.path.exists(db_file):
                print("Failed")
                print("Unable to connect to the archive database. Please try again later.")
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

            archive_file = dir_pattern(3).format(stage, job, 'current_archive.json')
            fp = open(archive_file, 'r')
            oArch = json.loads(fp.read())
            fp.close()
            del fp

            os.chdir(datastream_path)
            for site in os.listdir('.'):
                path = dir_pattern().format(datastream_path, site)
                os.chdir(site)

                for folder in os.listdir('.'):
                    os.chdir(folder)

                    args = (folder, start, end)
                    result = db.query(query % args, columns=cols)

                    for f in os.listdir('.'):
                        if not os.path.isdir(dir_pattern().format(os.getcwd(), f)):
                            try:
                                new_version = next(d['file_version'] for d in result if d['file_name'] == f)
                                old_version = next(o['file_version'] for o in oArch[folder] if o['file_name'] == f)
                                if not new_version > old_version:
                                    print("Failed")
                                    print("Not all files have been successfully archived. Please try again later.")
                                    self.config['exit'] = True
                                    return self.config, self.files
                            except StopIteration:
                                pass

                    os.chdir('..')
                os.chdir('..')

            os.chdir(cwd)
            self.config['cleanup_status']['cleanup']['files_archived'] = True
            print("Done")

        ################################################################################
        # Remove all files from `<job>/datastream`
        ################################################################################
        if not self.config['cleanup_status']['cleanup']['files_cleaned_up']:
            print("Cleaning up project files...",end="")
            # Remove archive.json
            # Remove current_archive.json
            # Remove <job>.deletion-list.txt

            f = Files(self.config)
            path = dir_pattern().format(stage, job)
            delete = [
                "datastream",
                "collection",
                "file_comparison/raw",
                "file_comparison/tar",
                'archive.json',
                'current_archive.json',
                '%s.deletion-list.txt' % job,
            ]

            try:
                for i in delete:
                    item = dir_pattern().format(path, i)
                    if os.path.exists(item):
                        if os.path.isdir(item):
                            f.empty_dir(item)
                        elif os.path.isfile(item):
                            os.remove(item)

            except:
                print("Failed")
                print("Unable to cleanup all files. Please try again, or cleanup project manually.")
                self.config['exit'] = True
                return self.config, self.files

            print("Done")
            self.config['cleanup_status']['cleanup']['files_cleaned_up'] = True

        self.config['cleanup_status']['cleanup']['status'] = True
        return self.config, self.files
