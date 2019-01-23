import click
from commands.mgmt import *
from support.interactive_support import *
from support.validators import *

@click.command(help='Reprocessing Wizard!')
@click.pass_context
def guided(ctx):
    questions = [
        {'type': 'confirm', 'name': 'confirm', 'message': 'Does this job have a DQR#?', 'default': True}]
    confirm = interact(questions)['confirm']

    if confirm:
        questions = [
            {'type': 'input', 'name': 'DQR', 'message': 'What is the DQR# for this job?', 'validate': DQRNumberValidator}]
        job = interact(questions)['DQR']
    else:
        questions = [{'type': 'input', 'name': 'job', 'message': 'What is the name for this job?'}]
        job = interact(questions)['job']
    questions = [{'type': 'confirm', 'name': 'confirm', 'message': 'Initialize this job?', 'default': True}]
    if interact(questions)['confirm']:
        ctx.invoke(setup, job=job)

    questions = [{'type': 'confirm', 'name': 'confirm', 'message': 'Would you like to stage a datastream?',
                  'default': False}]
    confirm = interact(questions)['confirm']

    while confirm:
        ds_info = get_ds_info()
        ctx.obj.reproc_logger.info('Datastream Staging Info:\n{}'.format(ds_info))
        questions = [{'type': 'confirm', 'name': 'confirm', 'message': 'Would you like to stage another datastream?',
                      'default': False}]
        confirm = interact(questions)['confirm']


    ctx.obj.reproc_logger.warning('Guided mode still under construction. Thanks for being patient with the mess. :)')

@click.pass_context
def get_ds_info(ctx):
    questions = [
        {'type': 'input', 'name': 'datastream', 'message': 'What is the datastream you would like to stage?',
         'validate': DatastreamValidator},
        {'type': 'input', 'name': 'start', 'message': 'What is the start date (YYYYMMDD) of data staging?',
         'validate': DateValidator},
        {'type': 'input', 'name': 'end', 'message': 'What is the end date (YYYYMMDD) of data staging?',
         'validate': DateValidator},
        {'type': 'list', 'name': 'source',
         'message': 'Would you like to stage from Data Archive, Data Datastream, or HPSS?',
         'choices': ['Data Archive', 'Data Datastream', 'HPSS'], 'filter': lambda val: val.lower().replace(' ','_')}
    ]
    ds_info = interact(questions)
    return ds_info