#!/apps/base/python3/bin/python3

import os
import sys
from glob import glob

from apm.classes.files import Files
from apm.classes.system import dir_pattern
from apm.classes.system import jprint

from apm.pmanager.manager import PluginManager

class Rename:
    """ Strip ARM Prefix from files """

    def __init__(self, config, files=None):
        """ Initialize with args """
        self.config = config
        self.files = files

    def run(self):
        manager = PluginManager()
        config = self.config
        f = Files(config)
        cwd = os.getcwd()
        stage = config['stage']
        collection = dir_pattern(3).format(stage, config['job'], 'collection')

        # Make sure files are supposed to be renamed
        if config['rename'] == False:
            return config, self.files

        # Verify there are no file collisions
        if self.check_for_collisions():
            return config, self.files

        # Strip the ARM prefix from all of the files
        print("\nStripping ARM prefix from files... ",end="")
        sys.stdout.flush()

        manager.callPluginCommand('hook_rename_preprocess', {'config': config})

        os.chdir(collection)
        sites = set(os.listdir('.'))
        for site in sites:
            os.chdir(site)
            instruments = set(os.listdir('.'))
            for ins in instruments:
                os.chdir(ins)
                files = set(os.listdir('.'))
                for i in files:
                    new_name = f.rename_file(i)
                    if new_name != None:
                        if i != new_name:
                            self.files[site][ins][new_name] = self.files[site][ins][i]
                            self.files[site][ins].pop(i)

                        self.files[site][ins][new_name]['current_name'] = new_name
                        self.files[site][ins][new_name]['stripped_name'] = new_name

                os.chdir('..')

            os.chdir('..')

        manager.callPluginCommand('hook_renamed_files_alter', {'config': config})

        print("Done\n")
        sys.stdout.flush()

        return config, self.files


    def check_for_collisions(self):
        """ Check all unpacked files for file naming collisions """
        print("Checking for file naming collisions...",end="")
        sys.stdout.flush()

        config = self.config
        f = Files(config, self.files)
        cwd = os.getcwd()
        collection = dir_pattern(3).format(config['stage'], config['job'], 'collection')
        os.chdir(collection)

        sites = os.listdir('.')
        for site in sites:
            os.chdir(site)
            instruments = set(os.listdir('.'))
            for ins in instruments:
                os.chdir(ins)
                files = set(os.listdir('.'))
                names = self.files[site][ins]

                # Mark files as deleted
                for k,v in names.items():
                    if k not in files:
                        names[k]['deleted'] = True

                # Check for duplicates
                for k,v in names.items():
                    if len(v['duplicate_files']) > 0 and v['deleted'] == False:
                        for i in v['duplicate_files']:
                            name = f.get_file_by_uuid(i)
                            if names[name]['uuid'] == i and names[name]['deleted'] == False:
                                config['duplicates'] = True
                                print("Fail")
                                print("Files with naming collisions still exist.\nPlease resolve these issues before continuing.\n")
                                return True

                os.chdir('..')

            os.chdir('..')

        os.chdir(cwd)
        config['duplicates'] = False
        print("Done")
        sys.stdout.flush()
        return False
