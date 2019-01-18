import click
from config.config import dqr_regex, datastream_regex
from datetime import datetime as dt
from logging import getLogger
from pathlib import Path, PurePath
reproc_logger = getLogger('reproc_logger')

def validate_dqr(ctx, param, job: str):
    '''the ctx &  param positional argument is not used but
        must be there for click option and argument validation'''
    ctx.obj.reproc_logger.debug('validating dqr - {}'.format(job))
    try:
        valid_dqr = dqr_regex.search(job).group()
    except AttributeError:
        ctx.obj.reproc_logger.warning('Error validating dqr provided. {}'.format(job))
    except TypeError:
        try:
            valid_dqr = dqr_regex.search(getcwd()).group()
        except AttributeError:
            ctx.obj.reproc_logger.warning('Could not parse dqr from cwd.')
            return job
    return valid_dqr

def validate_ds(ctx, param, datastream: str):
    '''the ctx &  param positional argument is not used but
    must be there for click option and argument validation'''
    ctx.obj.reproc_logger.debug('validating datastream - {}'.format(datastream))
    try:
        valid_datastream = datastream_regex.search(datastream).group()
    except AttributeError:
        reproc_logger.warning('Error validating dqr provided. {}'.format(datastream))
        exit(1)
    except TypeError:
        try:
            valid_datastream = datastream_regex.search(Path.cwd()).group()
        except AttributeError:
            reproc_logger.warning('Could not parse datastream from cwd.')
            exit(1)
    return datastream

def validate_date(ctx, param, date: str):
    '''the ctx & param positional argument is not used but
        must be there for click option and argument validation'''
    ctx.obj.reproc_logger.debug('validating date - {}'.format(date))
    try:
        dt.strptime(date, '%Y-%m-%d')
    except TypeError:
        reproc_logger.warning('A Valid date was not provided. YYYY-MM-DD')
        exit(1)
    except ValueError:
        try:
            dt.strptime(date, '%Y%m%d')
        except ValueError:
            reproc_logger.warning('Error validating dqr provided. {}'.format(date))
            raise AttributeError
    else:
        return date

def validate_file(ctx, param, filepath: str):
    '''the ctx & param positional argument is not used but
       must be there for click option and argument validation'''
    if not Path.is_file(Path(filepath)):
        if not Path.is_file(Path.joinpath(Path.cwd(), PurePath.name(filepath))):
            reproc_logger.warning('Error, file does not exist - {}'.format(filepath))
            raise AttributeError
