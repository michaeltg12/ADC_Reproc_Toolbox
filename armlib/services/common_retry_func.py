import os
from .retry import retry_call

def assert_path(path):
    if not os.path.exists(path):
        raise FileNotFoundError('Cannot find %s'%(path))


def assert_mount(path):
    if not os.path.ismount(path):
        raise AssertionError('Filesystem isnt mounted %s'%(path))

def retry_assert_path(path):
    f = assert_path
    fargs= (path,)
    retry_call(f=f, fargs=fargs, exceptions=(OSError), tries=10, delay=5)

def retry_assert_mount(path):
    f = assert_mount
    fargs= (path,)
    retry_call(f=f, fargs=fargs, exceptions=(AssertionError), tries=10, delay=5)