import click

from logging import getLogger
reproc_logger = getLogger('reproc_logger')

@click.command(help='This will rename some fucking files! Yea!')
@click.option('--indexes', '-i', default='', help='The fucking indexes to cut! Fuck yea!') # this has to be the last
def rename(indexes):
    reproc_logger.info('indexes = {}'.format(indexes))
    reproc_logger.info('rename some shitty files.')
    printStuff()

@click.command(help='print stuff help')
def printStuff():
    click.echo('testing import method .... idk stuff')
