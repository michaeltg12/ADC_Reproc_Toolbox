import click
from datetime import datetime as dt
from config.config import dqr_regex, datastream_regex
from logging import getLogger
reproc_logger = getLogger('reproc_logger')

def validate_dqr(ctx: click.Context, param, dqr: str):
    '''the param positional argument is not used but
        must be there for click option and argument validation'''
    reproc_logger.debug('validating dqr - {}'.format(dqr))
    try:
        dqr_regex.search(dqr).group()
    except AttributeError:
        reproc_logger.debug('Error validating dqr provided. {}'.format(dqr))
        raise click.BadParameter()
    else:
        return dqr

def validate_ds(param, datastream: str):
    '''the param positional argument is not used but
    must be there for click option and argument validation'''
    reproc_logger.debug('validating - {}'.format(datastream))
    try:
        datastream_regex.search(datastream).group()
    except AttributeError:
        reproc_logger.debug('Error validating dqr provided. {}'.format(datastream))
        raise click.BadParameter('Error validating datastream provided. {}'.format(datastream))
    else:
        return datastream

def validate_date(param, date: str):
    '''the param positional argument is not used but
        must be there for click option and argument validation'''
    reproc_logger.debug('validating param - {}'.format(date))
    try:
        dt.strptime(date, '%Y-%m-%d')
    except ValueError:
        try:
            dt.strptime(date, '%Y%m%d')
        except ValueError:
            reproc_logger.debug('Error validating dqr provided. {}'.format(date))
            raise click.BadParameter('Error validating date provided. {}'.format(date))
    else:
        return date
