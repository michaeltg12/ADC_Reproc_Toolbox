import json
import os
import subprocess
from config.config import *
from support.command_support import *
from support.validators import *

stage_help='''Manual data staging. Required argument: DQR# as job name.
A non DQR job name can be used but it will break the funcionality of some
tools in this toolbox.'''
@click.command(help=stage_help)
@click.argument('job')
@click.option('--datastream', '-ds', callback=validate_ds, help='The datastream to stage.')
@click.option('--start', '-s', callback=validate_start, help='Start date')
@click.option('--end', '-e', callback=validate_end, help='End date')
@click.pass_context
def stage(ctx, *args, **kwargs):
    job = kwargs.pop('job')
    datastream = kwargs.pop('datastream')
    start = kwargs.pop('start')
    end = kwargs.pop('end')
    if job == None:
        job = dqr_from_cwd(cwd=os.getcwd())
    ctx.obj.reproc_logger.info('job = {} ds={}: {} - {}'.format(job, datastream, start, end))
    ctx.obj.reproc_logger.info("running staging module")
    ctx.obj.reproc_logger.debug('debug mode activated')


@click.command(help='Get a list highest version files given a datastream, start, and end.')
@click.option('--userid', '-u', default='giansiracusam1', help='User ID, can be obtained using get_userid method.')
@click.option('--datastream', '-ds', callback=validate_ds, help='REQUIRED: The datastream to stage.')
@click.option('--start', '-s', callback=validate_start, help='REQUIRED: Start date')
@click.option('--end', '-e', callback=validate_end, help='REQUIRED: End date')
@click.option('--to-file/--no-file', default=False, help='Write output to file')
@click.pass_context
def get_filelist(ctx, *args, **kwargs):
    # more info about adrsws @ https://adc.arm.gov/docs/adrsws.html
    cmd = [adrsws_loc, '-u', kwargs['userid'], '-d', kwargs['userid'],
           '-t', 'flist', '-d', kwargs['datastream'], '-s', kwargs['start'], '-e', kwargs['end'],  '-v']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate() # a tuple with two bytes type objects
    parsed_out = json.loads(out.decode('utf-8'))
    if parsed_out['status'] == 'success':
        if kwargs['to_file']:
            with open('filelist.txt','w') as flist: # writes to cwd
                for f in parsed_out['files']:
                    flist.write(f'{f}\n')
        else:
            return parsed_out['files'] # this is a list
    else:
        ctx.obj.reproc_logger.warning('{} - {}'.format(parsed_out['status'],parsed_out['msg']))
        return RuntimeError

get_userid_help='''Retrieve the userid to use ADRSWS. 
This command requires an email registered with ARM. User registration at https://adc.arm.gov'''
@click.command(help=get_userid_help)
@click.argument('email')
@click.pass_context
def get_userid(ctx, *args, **kwargs):
    # more info about adrsws @ https://adc.arm.gov/docs/adrsws.html
    cmd = [adrsws_loc, '-t', 'userid', '-a', kwargs['email']]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate()  # a tuple with two bytes type objects
    parsed_out = json.loads(out.decode('utf-8'))
    try:
        ctx.obj.reproc_logger.info('userid = {}'.format(parsed_out['userid']))
        return parsed_out['userid']
    except KeyError:
        ctx.obj.reproc_logger.warning('{} - {}'.format(parsed_out['status'],parsed_out['msg']))
        return RuntimeError

