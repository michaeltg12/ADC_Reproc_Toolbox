import json
import os
import subprocess
from support.command_support import *
from support.validators import *

@click.command(help='Manual data staging.')
@click.option('--job', '-j', help='DQR# as job name.') #, callback=validate_dqr)
@click.option('--datastream', '-ds', callback=validate_ds, help='The datastream to stage.')
@click.option('--start', '-s', callback=validate_date, help='Start date')
@click.option('--end', '-e', callback=validate_date, help='End date')
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
@click.option('--start', '-s', callback=validate_date, help='REQUIRED: Start date')
@click.option('--end', '-e', callback=validate_date, help='REQUIRED: End date')
@click.option('--to-file/--no-file', default=False, help='Write output to file')
def get_filelist(*args, **kwargs):
    # more info about adrsws @ https://adc.arm.gov/docs/adrsws.html
    cmd = ['/data/project/0021718_1509993009/bin/./adrsws.sh', '-u', kwargs['userid'], '-d', kwargs['userid'],
           '-t', 'flist', '-d', kwargs['datastream'], '-s', kwargs['start'], '-e', kwargs['end'],  '-v']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    out, err = process.communicate() # a tuple with two bytes type objects
    decoded_out = out.decode('utf-8')
    parsed_out = json.loads(decoded_out)
    if parsed_out['status'] == 'success':
        if kwargs['to_file']:
            with open('filelist.txt','w') as flist: # writes to cwd
                for f in parsed_out['files']:
                    flist.write(f'{f}\n')
        else:
            return parsed_out['files'] # this is a list
    else:
        print('{} - {}'.format(parsed_out['status'],parsed_out['msg']))
        return RuntimeError

@click.command(help='Stage using adrsws by passing a file that contains one file to stage per line.')
@click.option('--filename', '-f', callback=validate_file, help='Filename or full path to file.')
def stage1(*args, **kwargs):
    pass