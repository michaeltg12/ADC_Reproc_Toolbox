import os
import logging.config
import socket
import signal
from armlib.services import Daemon
from armlib.config import load_config
from armlib.NFSLock import NFSFileGrabber, NFSLocker


class ReturnFileError(Exception):
    pass


def setup_logger(logging_config):
    logging.config.dictConfig(logging_config)


class NFSDaemon(Daemon):
    def __init__(self, config_filename, pidfile, WorkerClass):
        self.terminate = False
        self.config = load_config(config_filename)
        self.submodule_config = self.config['nfs_locations']
        # setup logger
        logging_config = self.config['logging']
        setup_logger(logging_config)
        self.logger = logging.getLogger('root.nfs_daemon')
        # setup locker
        self.incoming_directory = self.submodule_config['incoming_directory']
        self.assert_path(self.incoming_directory)
        lock_directory = self.submodule_config['lock_directory']
        self.locker = NFSLocker(lock_directory, 1)
        # setup worker
        self.work_directory_base = self.submodule_config['work_directory']
        self.assert_path(self.work_directory_base)
        self.worker = WorkerClass(self.config)
        super(NFSDaemon, self).__init__(pidfile)

    def assert_path(self, path):
        if not os.path.exists(path):
            self.logger.error('%s does not exist', path)
            exit(1)

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
            if len(unprocessed_files) == 0:
                os.rmdir(self.work_directory)
        super(NFSDaemon, self).end_gracefully('', '')

    def end_gracefully(self, signum, _):
        self.logger.debug('Daemon is terminating.')
        self.terminate = True
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def run(self):
        self.work_directory = self.acquire_work_directory()
        self.logger.debug('Initializing NFSFileGrabber()')
        grabber = NFSFileGrabber(self.locker, self.incoming_directory, self.work_directory)
        self.logger.debug('Starting to process files')
        while 1:
            if self.terminate:
                self.die()
            self.process_files(grabber)
