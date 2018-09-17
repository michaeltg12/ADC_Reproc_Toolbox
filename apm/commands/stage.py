#!/apps/base/python3/bin/python3

import os
import sys
import shutil
import uuid
from apm.pmanager.manager import PluginManager

from apm.classes.db import DB
from apm.classes.files import Files
from apm.classes.unpack import UnPack
from apm.classes.vapmgr import VapMgr

from apm.classes.system import dir_pattern
from apm.classes.system import jprint

import errno

global duplicates
duplicates = {}

class Stage:
    """ Stage files for Ingest or VAP reprocessing task """

    def __init__(self, config, files=None):
        """ Initialize with args """
        self.files = files
        self.config = config
        self.cwd = os.getcwd()
        self.manager = PluginManager()

    def run(self):
        config = self.config
        manager = self.manager

        if config['ingest']:
            # If staging for Ingest

            # Make sure collection does not have any files that might get overwritten
            empty = self.check_collection_empty()
            if not empty:
                print("\nFiles currently exist in your collection directory.\nPlease empty {}/{}/collection and try again.\n".format(config['stage'], config['job']))
                config['exit'] = True
                return config, self.files

            # cd to the stage directory
            os.chdir(config['stage'])

            # Check to see if a plugin needs to modify the datastream
            temp = manager.callPluginCommand('hook_datastream_alter', {'config': config})
            config = temp if temp != None else config

            # Check to see if a plugin needs to modify the SIF data
            temp = manager.callPluginCommand('hook_sif_alter', {'config': config})
            config = temp if temp != None else config

            # Establish a database connection
            db = DB(config)

            # Get the data_paths
            data_paths = db.get_data_paths()

            # Check to see if a plugin needs to modify the data_paths
            temp = manager.callPluginCommand('hook_data_paths_alter', {'config': config, 'data_paths': data_paths})
            data_paths = temp if temp != None else data_paths

            # for each instrument
            for k,v in enumerate(data_paths):
                archive_path = v['output']
                stage_path = v['input']

                # Set tar_path and check for plugin modifications
                tar_path = '{}/{}'.format(config['source'], archive_path)
                temp = manager.callPluginCommand('hook_tar_path_alter', {'config': config, 'tar_path': tar_path})
                tar_path = temp if temp != None else tar_path

                if os.path.exists(tar_path):
                    # Get a list of tar files that match specified dates
                    tar = UnPack(config, archive_path, stage_path)
                    tar_files = tar.get_tar_files()

                    temp = manager.callPluginCommand('hook_tar_files_alter', {'config': config})
                    tar_files = temp if temp != None else tar_files

                    if tar_files and len(tar_files) > 0:
                        # compare_path = '{}/{}/.compare/{}'.format(config['stage'], config['job'], stage_path)
                        compare_path = dir_pattern(5).format(config['stage'], config['job'], 'file_comparison', 'raw', stage_path)
                        tar_backup = dir_pattern(5).format(config['stage'], config['job'], 'file_comparison', 'tar', stage_path)
                        collection_path = '{}/{}/collection/{}'.format(config['stage'], config['job'], stage_path)


                        # Make the above paths if they don't already exist
                        if not os.path.exists(compare_path):
                            os.makedirs(compare_path)

                        if not os.path.exists(tar_backup):
                            os.makedirs(tar_backup)

                        if not os.path.exists(collection_path):
                            os.makedirs(collection_path)


                        # Copy the tar files to the backup location
                        if not tar.copy_files(tar_files, tar_backup):
                            print("Unable to copy tar files")

                        # Unpack the tar files
                        tar.extract_tar_files(tar_files)
                        has_dups = tar.handle_duplicate_files()
                        if has_dups:
                            config['duplicates'] = True

                            for i in has_dups:
                                duplicates[i] = has_dups[i]

                    else:
                        temp = tar_path.split('/')
                        if not config['quiet']:
                            print('\nData not available for {} using the dates specified'.format(temp[-1]))

                else:
                    temp = tar_path.split('/')
                    if not config['quiet']:
                        print('\nData for {} does not exist.'.format(temp[-1]))

                site, process = stage_path.split('/')

                if self.files == None:
                    self.files = {}

                if site not in self.files:
                    self.files[site] = {}

                site = self.files[site]
                if process not in site:
                    site[process] = {}

                process = site[process]

                if os.path.exists(dir_pattern(4).format(self.config['stage'], self.config['job'], 'collection', stage_path)):
                    files = os.listdir(dir_pattern(4).format(self.config['stage'], self.config['job'], 'collection', stage_path))
                    dup_uuid = {}
                    for i in files:
                        original_name = i
                        temp = i.split('.')
                        if temp[-1][0] == 'v':
                            try:
                                int(temp[-1][1:])
                                original_name = '.'.join(temp[:-1])
                            except:
                                pass

                        process[i] = {
                            "uuid": str(uuid.uuid4()),
                            "current_name": i,
                            "original_name": original_name,
                            "stripped_name": None,
                            "processed_name": None,
                            "unpacked_name": i,
                            "duplicate_files": [],
                            "deleted": False,
                        }
                        if original_name != i:
                            dup_uuid[i] = process[i]['uuid']

                    for i in duplicates:
                        if i.startswith(data_paths[k]['input']):
                            for j in duplicates[i]:
                                site, process, name = j.split('/')
                                for l in duplicates[i]:
                                    temp = l.split('/')
                                    if j != l:
                                        self.files[site][process][name]['duplicate_files'].append(dup_uuid[temp[2]])


                    # Copy the config files from /data/conf to /<stage>/<job>/conf
                    conf_path = "/data/conf/{0}/{0}{1}{2}".format(self.config['site'], self.config['instrument'], self.config['facility'])
                    conf_dest = "{0}/{1}/conf/{2}".format(self.config['stage'], self.config['job'], self.config['site'])
                    dest_folder = "{}{}{}".format(self.config['site'], self.config['instrument'], self.config['facility'])
                    if not os.path.exists(conf_path):
                        conf_path = "/data/conf/{0}/{1}{2}".format(self.config['site'], self.config['instrument'], self.config['facility'])
                        conf_dest = "{0}/{1}/conf/{2}".format(self.config['stage'], self.config['job'], self.config['site'])
                        dest_folder = "{}{}".format(self.config['instrument'], self.config['facility'])


                    if os.path.exists(conf_path):
                        if not os.path.exists(conf_dest):
                            os.makedirs(conf_dest)

                        if os.path.exists(dir_pattern().format(conf_dest, dest_folder)):
                            try:
                                os.rmdir(dir_pattern().format(conf_dest, dest_folder))
                            except OSError as e:
                                if e.errno == errno.ENOTEMPTY:
                                    exit("Unable to copy config files to {}. Destination is not empty.".format(dir_pattern().format(conf_dest, dest_folder)))
                                else:
                                    raise e

                        shutil.copytree(conf_path, dir_pattern().format(conf_dest, dest_folder))



            f = Files(self.config)
            src = dir_pattern(3).format(config['stage'], config['job'], 'collection')
            # dst = dir_pattern(3).format(config['stage'], config['job'], '.compare')
            dst = dir_pattern(4).format(config['stage'], config['job'], 'file_comparison', 'raw')
            if os.path.exists(dst):
                f.empty_dir(dst)
                os.rmdir(dst)

            shutil.copytree(src, dst)

            if len(duplicates) > 0:
                print('')
                print('The following files had naming collisions when unpacked.\nPlease verify the contents and keep only the appropriate file(s).')
                print('Please do not rename files, simply delete any unwanted files.')
                for i in duplicates:
                    print('')
                    for j in duplicates[i]:
                        print(j)
                print('')

            f.save_env()

        elif config['vap']:
            f = Files(self.config)
            f.save_env()

            vap = VapMgr(self.config)
            vap.add_to_env()

        return config, self.files

    def check_collection_empty(self, folder=None):
        """ Make sure no data will be overwritten if files are unpacked """
        cwd = os.getcwd()
        empty = True

        if folder == None:
            folder = '{}/{}/collection'.format(self.config['stage'], self.config['job'])

        os.chdir(folder)

        files = os.listdir('.')
        for i in files:
            if os.path.isdir(i):
                empty = self.check_collection_empty(folder=i)
                if not empty:
                    break;
            elif os.path.isfile(i):
                empty = False
                break;

        os.chdir(cwd)
        return empty
