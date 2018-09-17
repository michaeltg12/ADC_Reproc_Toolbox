from .Locker import NFSLocker
import logging
import shutil
import os
import datetime
class NoFiles(Exception):
    pass

class FilenameDateParser(object):
    def __init__(self, filename):
        self.filename = filename
        self.date_tuple = self.parse_filename(filename)

    def linux_time(self):
        year, month, day, hour, minute, second = self.date_tuple
        dt = datetime.datetime(year=year, month=month, day=day,
                               hour=hour, minute=minute, second=second)
        return (dt - datetime.datetime(1970,1,1)).total_seconds()

    def parse_filename(self, filename):
        split_name = filename.split(".")
        ymd = split_name[2]
        hms = split_name[3]
        if len(ymd) == 6:
            year = 1900 + int(ymd[:2])
            ymd = ymd[2:]
        else:
            year = int(ymd[:4])
            ymd = ymd[4:]
        month = int(ymd[:2])
        day = int(ymd[2:])
        hour = int(hms[:2])
        minute = int(hms[2:4])
        second = int(hms[4:])
        return year, month, day, hour, minute, second

    def __lt__(self, other):
        return self.date_tuple < other.date_tuple


class NFSFileGrabber(object):
    def __init__(self, locker, incoming_directory, work_directory):
        self.logger = logging.getLogger('root.nfs_filegrabber')
        self.logger.debug('Checking Config Parameters')
        self.verify_config(work_directory)
        self.verify_config(incoming_directory)
        self.locker = locker
        self.incoming_directory = incoming_directory
        self.work_directory = work_directory

    def verify_config(self, folder):
        if not os.path.exists(folder):
            error_msg = 'Unable to start processing files' \
                        ' because %s doesnt exist'
            self.logger.error(error_msg, folder)
            exit(1)

    def _move_files(self, files, dst_dir):
        moved_files = []
        for fullPathSrc in files:
            filename = os.path.basename(fullPathSrc)
            fullPathDest = os.path.join(dst_dir, filename)
            try:
                shutil.move(fullPathSrc, fullPathDest)
                moved_files.append(fullPathDest)
            except (FileNotFoundError, PermissionError) as my_err:
                self.logger.warning('%s: mv %s %s',str(my_err), fullPathSrc, fullPathDest)
        return moved_files

    def claim_files(self, number_of_files):
        self.locker.acquire_lock()
        try:
            files_in_incoming_folder = self._locate_files(number_of_files)
            moved_files = self._move_files(files_in_incoming_folder, self.work_directory)
            self.locker.release_lock()
            return moved_files
        except NoFiles:
            self.locker.release_lock()
            return []

    def claim_files_by_size(self, max_size):
        self.locker.acquire_lock()
        try:
            files_in_incoming_folder = self._locate_files_by_size(max_size)
            moved_files = self._move_files(files_in_incoming_folder, self.work_directory)
            self.locker.release_lock()
            return moved_files
        except NoFiles:
            self.locker.release_lock()
            return []

    def _locate_files(self, number_of_files):
        files = []
        incoming_files = os.listdir(self.incoming_directory)
        if len(incoming_files) == 0:
            raise NoFiles('no files in directory')
        for eachFile in incoming_files:
            fullPath = os.path.join(self.incoming_directory, eachFile)
            files.append(fullPath)
            if len(files) >= number_of_files:
                break
        return files

    def _locate_files_by_size(self, max_size_in_bytes):
        files = []
        total_size = 0
        incoming_files = os.listdir(self.incoming_directory)
        if len(incoming_files) == 0:
            raise NoFiles('no files in directory')
        for eachFile in incoming_files:
            fullPath = os.path.join(self.incoming_directory, eachFile)
            file_size = os.path.getsize(fullPath)
            files.append(fullPath)
            total_size += file_size
            if total_size >= max_size_in_bytes:
                break
        return files

    def return_file_to_incoming_directory(self, filename):
        self._move_files([filename], self.incoming_directory)
