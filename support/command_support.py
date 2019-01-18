import click
from datetime import datetime as dt
from config.config import dqr_regex, datastream_regex
from support.decorators import *
from logging import getLogger
reproc_logger = getLogger('reproc_logger')

def dqr_from_cwd(*args, **kwargs):
    '''the ctx &  param positional argument is not used but
        must be there for click option and argument validation'''
    cwd = kwargs.pop('cwd')
    reproc_logger.info('parsing dqr - {}'.format(cwd))
    result =  dqr_regex.search(cwd)
    if result:
        valid_dqr = result.group()
    else:
        reproc_logger.debug('Unable to parse dqr from cwd. {}'.format(cwd))
        return None
    return valid_dqr

# def datastream_from_cwd()