import click
from config.config import dqr_regex, datastream_regex
from datetime import datetime as dt
from logging import getLogger
from os import getcwd
from pathlib import Path, PurePath
from support.interactive import interact
from support.setup_support import load_job_config
reproc_logger = getLogger('reproc_logger')

def validate_dqr(ctx, param, job: str):
    '''the ctx &  param positional argument is not used but
        must be there for click option and argument validation'''
    ctx.obj.reproc_logger.debug('validating dqr - {}'.format(job))
    try:
        valid_dqr = dqr_regex.search(job).group()
    except AttributeError:
        ctx.obj.reproc_logger.error('Error validating dqr provided. {}'.format(job))
    except TypeError:
        try:
            valid_dqr = dqr_regex.search(getcwd()).group()
        except AttributeError:
            ctx.obj.reproc_logger.error('Could not parse dqr from cwd.')
            return job
    return valid_dqr

def validate_ds(ctx, param, datastream: str):
    '''the ctx &  param positional argument is not used but
    must be there for click option and argument validation'''
    if not datastream:
        try:
            datastream = load_job_config(context=ctx)['datastreams'].keys()
        except FileNotFoundError:
            ctx.obj.reproc_logger.error('Could not load datastream from job config file.')
            exit(1)
    if len(datastream) > 1:
        questions = [{'type': 'list', 'name': 'datastream', 'message': 'Select one datastream', 'choices': list(datastream),
                'validate': lambda answer: 'You must choose at least one datastream.' if len(answer) == 0 else True}]
        datastream = interact(questions)['datastream']
    ctx.obj.reproc_logger.debug('validating datastream - {}'.format(datastream))
    try:
        valid_datastream = datastream_regex.search(datastream).group()
    except AttributeError:
        reproc_logger.error('Error validating dqr provided. {}'.format(datastream))
        exit(1)
    except TypeError:
        try:
            valid_datastream = datastream_regex.search(Path.cwd()).group()
        except AttributeError:
            reproc_logger.error('Could not parse datastream from cwd.')
            exit(1)
    return datastream

def validate_start(ctx, param, start: str):
    '''the ctx & param positional argument is not used but
        must be there for click option and argument validation'''
    if not start:
        try:
            start = load_job_config(context=ctx)['datastreams'][ctx.params['datastream']]['start']
        except FileNotFoundError:
            ctx.obj.reproc_logger.error('Could not load start date from job config file.')
            raise FileNotFoundError
    ctx.obj.reproc_logger.debug('validating start date - {}'.format(start))
    try:
        dt.strptime(start, '%Y-%m-%d')
    except TypeError:
        reproc_logger.error('A Valid start date was not provided. YYYY-MM-DD. {}'.format(start))
        raise AttributeError
    except ValueError:
        try:
            dt.strptime(start, '%Y%m%d')
        except ValueError:
            reproc_logger.error('A Valid start date was not provided. YYYY-MM-DD. {}'.format(start))
            raise AttributeError
    return start

def validate_end(ctx, param, end: str):
    '''the ctx & param positional argument is not used but
        must be there for click option and argument validation'''
    if not end:
        try:
            end = load_job_config(context=ctx)['datastreams'][ctx.params['datastream']]['end']
        except FileNotFoundError:
            ctx.obj.reproc_logger.error('Could not load end date from job config file.')
            raise FileNotFoundError
    ctx.obj.reproc_logger.debug('validating end date - {}'.format(end))
    try:
        dt.strptime(end, '%Y-%m-%d')
    except TypeError:
        reproc_logger.error('A Valid end date was not provided. YYYY-MM-DD. {}'.format(end))
        raise AttributeError
    except ValueError:
        try:
            dt.strptime(end, '%Y%m%d')
        except ValueError:
            reproc_logger.error('A Valid end date was not provided. YYYY-MM-DD. {}'.format(end))
            raise AttributeError
    return end

def validate_file(ctx, param, filepath: str):
    '''the ctx & param positional argument is not used but
       must be there for click option and argument validation'''
    if not Path.is_file(Path(filepath)):
        if not Path.is_file(Path.joinpath(Path.cwd(), PurePath.name(filepath))):
            reproc_logger.error('Error, file does not exist - {}'.format(filepath))
            raise AttributeError
