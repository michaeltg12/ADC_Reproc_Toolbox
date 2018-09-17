#!/apps/base/python3/bin/python3

import os
import shutil
import tarfile
import time
import datetime
import threading

from glob import glob
from uuid import UUID

from apm.classes.ui import UI
from apm.classes.files import Files
from apm.classes.system import jprint
from apm.classes.system import dir_pattern


class UnPack:
    """ Interface to unpack tar files """

    def __init__(self, config, archive_path, stage_path, files={}):
        """ Initialize with args """
        self.filenames = files
        self.config = config
        self.begin = self.config['begin']
        self.end = self.config['end']
        self.quiet = self.config['quiet']

        self.source = self.config['source']
        self.stage = self.config['stage']
        self.job = self.config['job']
        self.archive_path = dir_pattern().format(self.source, archive_path)
        self.stage_path = dir_pattern(4).format(self.stage, self.job, 'collection', stage_path)

        self.cwd = os.getcwd()

        temp = self.stage_path.split('/')
        self.local = dir_pattern().format(temp[-2], temp[-1])

        # List of tar files
        self.files = None

        # duplicate file manipulation
        self.file_names = []
        self.st_files = []
        self.members = []
        self.duplicates = {}
        self.dups = None

    def get_tar_files(self):
        """ Retrieve a list of tar files that match the specified dates """
        os.chdir(self.archive_path)
        files = glob('*.tar')

        temp = str(self.begin)
        by = int(temp[:4])
        bm = int(temp[4:6])
        bd = int(temp[6:8])
        temp = str(self.end)
        ey = int(temp[:4])
        em = int(temp[4:6])
        ed = int(temp[6:8])

        begin = datetime.date(by, bm, bd)
        end = datetime.date(ey, em, ed)

        tar_files = []
        dates = {}

        # Check to see if data exists for the selected instrument
        if len(files) == 0:
            if not self.quiet:
                print("No data for the selected instrument.")
            return

        # Make sure I have only the names of the files, not the full path
        for k,v in enumerate(files):
            files[k] = v.split('/')[-1]

        # Sort the files by date
        files.sort()

        # Collect all the dates for the various files
        for i in files:
            temp = i.split('.')
            date = temp[2]
            if len(date) == 6:
                date = "19%s" % date

            year = date[:4]
            month = date[4:6]
            day = date[6:8]
            key = "{}-{}-{}".format(year, month, day)
            if key not in dates:
                dates[key] = [i]
            else:
                dates[key].append(i)

        keys = dates.keys()
        keys = sorted(keys)

        first = keys[0].split('-')
        first = datetime.date(int(first[0]), int(first[1]), int(first[2]))
        last = keys[-1].split('-')
        last = datetime.date(int(last[0]), int(last[1]), int(last[2]))

        begin_index = None if (begin < first or begin > last or begin == None) else 0
        end_index = None if (end < first or end > last or end == None) else 0

        if (begin != None) or (end != None):
            for k,v in enumerate(keys):
                # Get the file's date
                temp = v.split('-')
                temp = datetime.date(int(temp[0]), int(temp[1]), int(temp[2]))
                if temp <= begin:
                    if not temp == begin:
                        begin_index = k
                    elif not begin_index:
                        begin_index = k if k == 0 else (k - 1)

                if temp >= end:
                    if temp == end:
                        end_index = (k + 1) if ((k + 1) < len(dates)) else k
                    elif not end_index:
                        end_index = k

                if (k == 0) and (temp > begin):
                    begin_index = k

                if ((k + 1) == len(dates)) and (temp < end) and (end_index == None):
                    end_index = k

        if begin_index != None and end_index == None:
            end_index = begin_index
        elif begin_index == None and end_index != None:
            begin_index = end_index
        elif begin_index == None and end_index == None:
            print("No data for the selected date range")
            return

        for i in range(begin_index, end_index + 1):
            for j in dates[keys[i]]:
                tar_files.append(j)

        return tar_files

    def extract_tar_files(self, files=None):
        if files == None:
            files = self.files
        if not self.quiet:
            print('\n{} files will be extracted from: \n {}\n and will be staged in: \n {}'.format(len(files), self.archive_path, self.stage_path))

        # cd to the stage dir
        if not os.path.exists(self.stage_path):
            os.makedirs(self.stage_path)
        os.chdir(self.stage_path)

        # Setup the progress bar
        pbar = UI()
        length = len(files)
        for i,f in enumerate(files):
            # Set initial percentage
            percent = int((i / float(length)) * 100)
            untar = UnTarFiles(self, f)
            untar.start()
            while untar.is_alive():
                # Update percentage
                pbar.progress(percent)

            # Make sure percentage reaches 100
            percent = int((float(i + 1) / float(length)) * 100)
            pbar.progress(percent)


        # Go to the next line in the console
        print("")

        os.chdir(self.cwd)

    def copy_files(self, files, dest):
        """ Copy the specified array of files to the destination directory """
        # If files is a string make it an array with the string in it
        if not type(files) == list and type(files) == str:
            files = [files]

        # files must be an array
        if type(files) != list:
            return False

        # Destination must be a directory
        if not os.path.isdir(dest):
            return False

        print("Copying %d tar files to %s" % (len(files), dest))
        pbar = UI()
        length = len(files)
        percent = 0
        pbar.progress(percent)
        # Copy each file in files to dest show progress on screen
        for k,v in enumerate(files):
            shutil.copy(v, dest)
            percent = int((float(k + 1) / float(length)) * 100)
            pbar.progress(percent)

        return True



    def handle_duplicate_files(self):
        # Handle duplicates
        f = Files(self.config)
        dup_list = {}
        duplicates = {}

        files = self.file_names
        dups = self.duplicates

        if len(dups) > 0:
            for i,n in dups.items():
                for j,v in enumerate(n):
                    folder = 'dup_{}'.format(j + 1)
                    delete = False
                    move = False
                    if f.is_same_file(dir_pattern().format(self.stage_path, i), dir_pattern(3).format(self.stage_path, folder, v)):
                        delete = True
                        move = False
                    else:
                        delete = False
                        move = True

                    if delete:
                        os.remove(dir_pattern(3).format(self.stage_path, folder, v))
                    elif move:
                        if i not in dup_list:
                            name = '{}.v1'.format(i)
                            dup_list[i] = [name]
                            src = dir_pattern().format(self.stage_path, i)
                            dst = dir_pattern().format(self.stage_path, name)
                            try:
                                os.rename(src, dst)
                            except OSError:
                                shutil.move(src, dst)

                        num = len(dup_list[i]) + 1
                        name = '{}.v{}'.format(v, num)
                        dup_list[i].append(name)
                        src = dir_pattern(3).format(self.stage_path, folder, v)
                        dst = dir_pattern().format(self.stage_path, name)
                        try:
                            os.rename(src, dst)
                        except OSError:
                            shutil.move(src, dst)

            for i in dup_list:
                if len(dup_list[i]) > 1:
                    key = dir_pattern().format(self.local, i)
                    duplicates[key] = []
                    for j in dup_list[i]:
                        duplicates[key].append(dir_pattern().format(self.local, j))

            self.dups = duplicates

            # Delete directory if now empty
            dupdirs = glob('{}/dup_*'.format(self.stage_path))
            for i in dupdirs:
                f.empty_dir(i)
                os.rmdir(i)

        return False if duplicates == {} else duplicates



################################################################################
# Multi threading
################################################################################
class UnTarFiles(threading.Thread):
    def __init__(self, tar, tar_file):
        self.error = False
        self.result = None
        self.tar = tar
        self.file = tar_file
        self.config = self.tar.config
        temp = self.tar.stage_path
        self.local = dir_pattern().format(temp[-2], temp[-1])
        threading.Thread.__init__(self)

    def run(self):
        """ Unpack the tar file """
        # Setup Vars
        st_files = self.tar.st_files
        file_names = self.tar.file_names
        # files = self.tar.members
        # temp = self.config['']

        files = []
        for i in range(len(st_files)):
            files.append([])

        # Open the tar file
        tar = tarfile.open(dir_pattern().format(self.tar.archive_path, self.file), 'r')

        # Get the content of the tar file and check for duplicate file names
        members = tar.getmembers()

        f = Files(self.config)

        # Iterate over each tar file
        for i,m in enumerate(members):

            # Make sure arrays are not 0 length
            if len(file_names) == 0:
                file_names.append([])
            if len(files) == 0:
                files.append([])
            if len(st_files) == 0:
                st_files.append([])

            # Iterate over each entry in file_names
            # Add the file name to the correct array
            for k,v in enumerate(file_names):
                sf_names = st_files[k]
                sn = f.strip_name(m.name)
                if sn == None or sn == 'orig' or sn == 'bad':
                    sn = m.name

                if not (m.name in v or sn in sf_names):
                    file_names[k].append(m.name)
                    files[k].append(m)
                    st_files[k].append(sn)
                    break

            else:
                file_names.append([m.name])
                files.append([m])
                st_files.append([sn])

        duplicates = {}
        stripped = st_files[0]
        full_names = file_names[0]

        for i in range(1, len(file_names)):
            for k,v in enumerate(file_names[i]):
                try:
                    myIndex = stripped.index(st_files[i][k])
                except IndexError:
                    pass
                    print("\nOOPS\n")
                    print("\nI: {}\nK: {}".format(i, k))
                try:
                    key = full_names[myIndex]
                except IndexError:
                    pass
                    print("\nOOPS 2\n")

                if key not in duplicates:
                    duplicates[key] = []
                duplicates[key].append(v)

        # Extract all files
        for i in range(len(files)):
            path = None
            if i > 0:
                path = 'dup_{}'.format(i)
            else:
                path = ''

            tar.extractall(path=path, members=files[i])

        tar.close()

        self.tar.duplicates = duplicates

        return

################################################################################
# Unit tests
################################################################################
import unittest
################################################################################
# Get Tar Files
################################################################################
class TestGetTarFiles(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_1(self):
        """ Pass with SIF data """
        pass

    def test_2(self):
        """ Pass with Datastream data """
        pass

    def test_3(self):
        """ Pass with no data """
        pass

################################################################################
# Extract Tar Files
################################################################################
class TestExtractTarFiles(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_1(self):
        """ Pass list of files """
        pass

    def test_2(self):
        """ Pass list of files that do not exist """
        pass

    def test_3(self):
        """ Pass empty list """
        pass

################################################################################
# Handle Duplicate Files
################################################################################
class TestHandleDuplicateFiles(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_1(self):
        """ Pass with no config.duplicates """
        pass

    def test_2(self):
        """ Pass with duplicates """
        pass



################################################################################
def main():
    pass

if __name__ == '__main__':
    main()
