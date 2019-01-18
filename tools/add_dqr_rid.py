#!/apps/base/python3/bin/python3

import click
import glob
import re

from netCDF4 import Dataset

try:
    from progress.bar import PixelBar
except ImportError:
    from .progress.bar import PixelBar

class CustomProgress(PixelBar):
    message = 'Adding reprocessing hidory: '
    suffix = '%(percent).1f%% eta:%(eta)ds  - elapsed:%(elapsed)ds'

help_description='''
This adds a global variable with the dqr and rid to an existing NetCDF file. 
'''

example ='''
EXAMPLE: python3 add_dqr_rid.py -re "sgpmetE37*" --info "(D170828.16"

ERRORS: IDK
'''

@click.command(help = help_description + example)
@click.argument('--regex', '-re', type=str,
              help='regular expression for getting list of files to tar')
@click.argument('--dqr', '-d', help='DQR ID')
@click.argument('--rid', '-r', help='RID')
@click.argument('--debug', '-D', is_flag=True, help='print debug statements.')
def main(regex, dqr, rid, debug):
    files = glob.glob(regex)
    DEBUG = debug
    prog = CustomProgress(max=len(files))
    new_entry = "reprocessed for DQR ID {} and RID {}".format(dqr, rid)
    for f in files:
        rootgrp = Dataset(f, "r+")
        try:
            if re.search('\n', rootgrp.history):
                history = rootgrp.history.split('\n')[0]
            else:
                history = rootgrp.history
            rootgrp.history = "{}\n{}".format(history, new_entry)
        except Exception as e:
            if DEBUG:
                print("{} --> {}".format(e, f))
        try:
            del rootgrp.reprocessing
        except Exception as e:
            if DEBUG:
                print("{} --> {}".format(e, f))
        rootgrp.close()
        prog.next()
    prog.finish()

if __name__ == '__main__':
    main()