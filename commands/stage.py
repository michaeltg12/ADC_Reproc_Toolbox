import click
from logging import getLogger
reproc_logger = getLogger("reproc_logger")

@click.command(help='This will stage some files!')
@click.option('--datastream', '-ds', help='The datastream to stage.')
@click.option('--start', '-s', help='Start date')
@click.option('--end', '-e', help='End date')
def stage(datastream, start, end):
    reproc_logger.info('stage some files with Josephs awesome module.')
    reproc_logger.info('ds={}: {} - {}'.format(datastream, start, end))
    reproc_logger.info("running staging module")