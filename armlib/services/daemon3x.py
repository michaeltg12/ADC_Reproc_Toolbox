"""Generic linux daemon base class for python 3.x."""

import sys
import os
import time
import signal
import logging
import traceback
import threading
import multiprocessing


def daemonize():
    """Deamonize a process. UNIX double fork mechanism."""
    logger = logging.getLogger('root.daemonize')
    try:
        pid = os.fork()
        if pid > 0:
            # exit first parent
            os._exit(0)
    except OSError as err:
        logger.error('fork #1 failed: %s', str(err))
        os._exit(1)

    # decouple from parent environment
    os.chdir('/')
    os.setsid()
    os.umask(0)

    # do second fork
    try:
        pid = os.fork()
        if pid > 0:
            # exit from second parent
            os._exit(0)
    except OSError as err:
        logger.error('fork #2 failed: %s', str(err))
        os._exit(1)

    # redirect standard file descriptors
    sys.stdout.flush()
    sys.stderr.flush()
    si = open(os.devnull, 'r')
    so = open(os.devnull, 'a+')
    se = open(os.devnull, 'a+')

    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())


class ThreadedSignalHandler(object):
    """
    This class will manage SIGHUP, SIGTERM, and SIGINT in the main thread.
    Then the other thread will not be interrupted by the signals.
    So if you want a class that gracefully handles signals you subclass this.
    Usage: subclass the ThreadedSignalHandler class,
    override the run() method, and override the end_gracefully() method.
    """

    def __init__(self):
        self.logger = logging.getLogger('root.daemon')

    def die(self):
        os._exit(0)

    def end_gracefully(self, signum, _):
        """
        Override this method to cleanup your daemon
        """
        err_msg = 'Daemon Class did not implement end_gracefully'
        raise NotImplementedError(err_msg)

    def start(self):
        """Start the daemon."""

        # Handle Signals
        signal.signal(signal.SIGHUP, self.end_gracefully)
        signal.signal(signal.SIGTERM, self.end_gracefully)
        signal.signal(signal.SIGINT, self.end_gracefully)

        t = threading.Thread(target=self.log_run, args=tuple())
        t.start()
        while t.is_alive():
            time.sleep(1)

    def log_run(self):
        try:
            self.run()
        except:
            err = traceback.format_exc()
            self.logger.error(err)

    def run(self):
        """You should override this method when you subclass Daemon.
        It will be called after the process has been daemonized by
        start() or restart()."""
        err_msg = 'ThreadedSignalHandler subclass did not implement run'
        raise NotImplementedError(err_msg)


class ProcessDaemonizer(object):
    '''
    This is a class that daemonizes a process.
    '''
    def __init__(self, daemon_class, dargs):
        self.daemon_class = daemon_class
        self.dargs = dargs
        daemonize()
        self.logger = logging.getLogger('root.multiprocessing_daemonizer')

    def start(self):
        try:
            daemon = self.daemon_class(*self.dargs)
        except:
            err = traceback.format_exc()
            self.logger.error(err)
            exit(1)

        daemon.start()


class MultiProcessingDaemonizer(ThreadedSignalHandler):
    '''
    This is a process manager that allows the creation of many daemon workers
    that all use the same config.
    The interface for this class is init and start.
    Note: DO NOT CALL RUN DIRECTLY.
    '''
    def __init__(self, ndaemons, daemon_class, dargs):
        self.ndaemons = ndaemons
        self.daemon_class = daemon_class
        self.dargs = dargs
        self.children = []
        self.logger = logging.getLogger('root.multiprocessing_daemonizer')
        daemonize()

    def end_gracefully(self, signum, _):
        self.logger.info('Shutting Down')
        for each_child in self.children:
            os.kill(each_child.pid, signal.SIGTERM)

        proc_is_still_alive = True
        while proc_is_still_alive:
            proc_is_still_alive = False
            for child in self.children:
                if child.exitcode is None:
                    proc_is_still_alive = True
            if proc_is_still_alive:
                time.sleep(1)
        super(MultiProcessingDaemonizer, self).die()

    def run(self):
        for i in range(0, self.ndaemons):
            p = multiprocessing.Process(target=self.launcher, args=tuple())
            p.start()
            self.children.append(p)

        while 1:
            time.sleep(30)
            for child in self.children:
                if not (child.exitcode is None):
                    # shutdown the master if a child dies
                    os.kill(os.getpid(), signal.SIGTERM)

    def launcher(self):
        # make sure that the daemon is initialized in the process that it runs in.
        # this allows each child to initialize and get its own connections
        try:
            daemon = self.daemon_class(*self.dargs)
        except:
            err = traceback.format_exc()
            self.logger.error(err)
            exit(1)
        daemon.start()
