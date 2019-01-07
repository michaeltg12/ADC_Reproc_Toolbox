import click
from datetime import datetime as dt
from config.config import dqr_regex, datastream_regex
from logging import getLogger
reproc_logger = getLogger('reproc_logger')


def dqr_from_cwd(cwd: str):
    '''the ctx &  param positional argument is not used but
        must be there for click option and argument validation'''
    reproc_logger.debug('parsing dqr - {}'.format(cwd))
    try:
        valid_dqr = dqr_regex.search(cwd).group()
    except AttributeError:
        reproc_logger.debug('Unable to parse dqr from cwd. {}'.format(cwd))
        raise ValueError
    return valid_dqr