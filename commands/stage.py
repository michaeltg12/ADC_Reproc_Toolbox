from os import getcwd
from support.command_support import *
from support.validators import *
from logging import getLogger
reproc_logger = getLogger("reproc_logger")

@click.command(help='Manual data staging.')
@click.option('--job', '-j', help='DQR# as job name.') #, callback=validate_dqr)
@click.option('--datastream', '-ds', callback=validate_ds, help='The datastream to stage.')
@click.option('--start', '-s', callback=validate_date, help='Start date')
@click.option('--end', '-e', callback=validate_date, help='End date')
def stage(datastream, start, end, job):
    reproc_logger.info('job = {} ds={}: {} - {}'.format(job, datastream, start, end))
    if job == None:
        job = dqr_from_cwd(job)
    reproc_logger.info('job = {}'.format(job))
    reproc_logger.info("running staging module")