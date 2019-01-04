import click
from logging import getLogger
reproc_logger = getLogger('reproc_logger')

@click.command('rename', help='This will rename some files! Yea!')
@click.option('--indexes', '-i', default='', help='The indexes to cut!') # this has to be the last

@click.pass_obj
def rename(config, indexes):
    reproc_logger.debug('rename debug mode active.')
    reproc_logger.info('indexes = {}'.format(indexes))
    reproc_logger.info('rename some files.')
    printStuff()

@click.command('printStuff', help='print stuff help')
def printStuff():
    click.echo('testing import method .... idk stuff')
