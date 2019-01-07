import click
from config.config import dqr_regex, datastream_regex
from datetime import datetime as dt
from logging import getLogger
from os import getcwd
reproc_logger = getLogger('reproc_logger')

def validate_dqr(ctx, param, dqr: str):
    '''the ctx &  param positional argument is not used but
        must be there for click option and argument validation'''
    reproc_logger.info('validating dqr - {}'.format(dqr))
    try:
        valid_dqr = dqr_regex.search(dqr).group()
    except AttributeError:
        reproc_logger.info('Error validating dqr provided. {}'.format(dqr))
        exit(1)
    except TypeError:
        try:
            valid_dqr = dqr_regex.search(getcwd()).group()
        except AttributeError:
            reproc_logger.info('Could not parse dqr from cwd.')
            exit(1)
    return valid_dqr

def validate_ds(ctx, param, datastream: str):
    '''the ctx &  param positional argument is not used but
    must be there for click option and argument validation'''
    try:
        valid_datastream = datastream_regex.search(datastream).group()
    except AttributeError:
        reproc_logger.info('Error validating dqr provided. {}'.format(datastream))
        exit(1)
    except TypeError:
        try:
            valid_datastream = datastream_regex.search(getcwd()).group()
        except AttributeError:
            reproc_logger.info('Could not parse datastream from cwd.')
            exit(1)
    return datastream

def validate_date(ctx, param, date: str):
    '''the ctx & param positional argument is not used but
        must be there for click option and argument validation'''
    reproc_logger.info('validating param - {}'.format(date))
    try:
        dt.strptime(date, '%Y-%m-%d')
    except ValueError:
        try:
            dt.strptime(date, '%Y%m%d')
        except ValueError:
            reproc_logger.info('Error validating dqr provided. {}'.format(date))
            raise ValueError
    else:
        return date
