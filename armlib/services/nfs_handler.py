import os
import logging
import socket
import signal
from .common_retry_func import retry_assert_path
from .daemon3x import ThreadedSignalHandler
from armlib.config import load_config
from armlib.NFSLock import NFSFileGrabber, NFSLocker


class ReturnFileError(Exception):
    pass


class NFSThreadedSignalHandler(ThreadedSignalHandler):
    def __init__(self, incoming_directory, lock_directory, work_directory, WorkerClass, wargs):
        super(NFSThreadedSignalHandler, self).__init__()
        self.terminate = False
        # setup logger
        self.logger = logging.getLogger('root.nfs_daemon')
        # setup locker
        self.incoming_directory = incoming_directory
        retry_assert_path(self.incoming_directory)
        lock_directory = lock_directory
        self.locker = NFSLocker(lock_directory, 1)
        # setup worker
        self.work_directory_base = work_directory
        retry_assert_path(self.work_directory_base)
        self.worker = WorkerClass(*wargs)

    def acquire_work_directory(self):
        host_pid = '%s.%d' % (socket.gethostname(), os.getpid())
        work_directory = os.path.join(self.work_directory_base, host_pid)
        os.mkdir(work_directory)
        return work_directory

    def die(self):
        if self.locker and self.locker.acquired_lock:
            self.locker.release_lock()
        if os.path.exists(self.work_directory):
            unprocessed_files = os.listdir(self.work_directory)
            # return files to incoming directory
            for each_file in unprocessed_files:
                fp = os.path.join(self.work_directory, each_file)
                self.grabber.return_file_to_incoming_directory(fp)
            os.rmdir(self.work_directory)
        super(NFSThreadedSignalHandler, self).die()

    def end_gracefully(self, signum, _):
        self.logger.debug('Daemon is terminating.')
        self.terminate = True
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def run(self):
        self.work_directory = self.acquire_work_directory()
        self.logger.debug('Initializing NFSFileGrabber()')
        self.grabber = NFSFileGrabber(self.locker, self.incoming_directory, self.work_directory)
        self.logger.debug('Starting to process files')
        while 1:
            if self.terminate:
                self.die()
            self.process_files(self.grabber)
