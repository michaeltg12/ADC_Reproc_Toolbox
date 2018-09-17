#!/apps/base/python3/bin/python3

import os
import shutil
# import json

# from apm.pmanager.manager import PluginManager

# from apm.classes.db import DB
# from apm.classes.files import Files

from apm.classes.system import dir_pattern
from apm.classes.system import jprint
# from apm.classes.system import convert_date_to_timestamp

############################################################
# Set Development flag
############################################################
global DEVEL
DEVEL = False
############################################################

class Demo:
    """ Cleanup data files before archiving job information """

    def __init__(self, config, files=None):
        """ Initialize with args """
        global DEVEL
        DEVEL = config['devel']

        self.config = config
        self.files = files

        self.path = '/data/home/twilliams/apm_demo/%s'


    def run(self):
        ''' Prep APM command to run in devel mode '''
        demo = self.config['demo']

        stage = self.config['stage']
        job = self.config['job']


        if self.config['devel']:
            if demo.lower() == 'remove':
            ################################################################################
            # Prep for APM Remove
            ##################################################
            # Modify the raw comparison files so they are different and there
            #  will be a file to delete
            ################################################################################
                cwd = os.getcwd()
                path = self.path % 'remove'
                job_path = dir_pattern().format(stage, job)
                dst = dir_pattern().format(job_path, 'file_comparison/raw/sgp/sgpmfrsrE9.00')

                os.chdir(path)

                if not os.path.exists(dst):
                    print("Unable to update the job directory for this demo. Proper directory structure does not exist.")
                    self.config['exit'] = True
                    return self.config, self.files

                for f in os.listdir('.'):
                    src = dir_pattern().format(path, f)
                    shutil.copy(src, dst)


                os.chdir(cwd)
                return self.config, self.files

            elif demo.lower() == 'archive':
            ################################################################################
            # Prep for APM Archive
            ##################################################
            # Create a file to be used as `<job>.archive.json`
            #  that has the appropriate files removed
            ################################################################################
                cwd = os.getcwd()
                path = self.path % 'archive'

                os.chdir(path)
                src = dir_pattern().format(path, 'job.archive.json')
                dst = dir_pattern(3).format(stage, job, '%s.archive.json' % job)
                shutil.copy(src, dst)

                os.chdir(cwd)
                return self.config, self.files

            elif demo.lower() == 'cleanup':
            ################################################################################
            # Prep for APM Cleanup
            ##################################################
            # Replace `current_archive.json` with a file that has
            #  updated version numbers for processed files
            ################################################################################
                cwd = os.getcwd()
                path = self.path % 'cleanup'

                os.chdir(path)

                src = dir_pattern().format(path, 'current_archive.json')
                dst = dir_pattern().format(stage, job)
                shutil.copy(src, dst)

                os.chdir(cwd)
                return self.config, self.files



