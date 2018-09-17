import os
import logging
import time
import sys

class NFSLocker(object):
    def __init__(self, lock_directory_location, sleep_time):
        self.lock_directory_location= lock_directory_location
        self.logger = logging.getLogger('root.nfslocker')
        self.acquired_lock = False
        self.sleep_time = sleep_time
        try:
            self.acquire_lock()
            self.release_lock()
        except AssertionError:
            err_msg = 'Unable to provide locking service for module.'
            self.logger.critical(err_msg)
            sys.stderr.write(err_msg)
            os._exit(1)

    def make_lock_dir(self):
        try:
            os.mkdir(self.lock_directory_location)
            self.acquired_lock = True
            return 0
        except OSError as err:
            return err.errno

    def acquire_lock(self):
        while 1:
            err = self.make_lock_dir()
            if err == 0:
                return
            if err == 17:  # File exists
                time.sleep(self.sleep_time)
            else:
                self.logger.critical('Encountered Unexpected OSError Acquiring Lock')
                self.logger.critical(str(err))
                assert False

    def release_lock(self):
        if self.acquired_lock:
            try:
                os.rmdir(self.lock_directory_location)
                self.acquired_lock = False
            except OSError as err:
                self.logger.critical('Encountered Unexpected OSError Releasing Lock')
                self.logger.critical(str(err))
                assert False
