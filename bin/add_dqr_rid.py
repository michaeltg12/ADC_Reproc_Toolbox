#!/apps/base/python3/bin/python3

import argparse
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

def main(files, args):
    DEBUG = args.debug
    prog = CustomProgress(max=len(files))
    new_entry = "reprocessed for DQR ID {} and RID {}".format(args.dqr, args.rid)
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

help_description='''
This adds a global variable with the dqr and rid to an existing NetCDF file. 
'''

example ='''
EXAMPLE: python3 add_dqr_rid.py -re "sgpmetE37*" --info "(D170828.16"

ERRORS: IDK
'''
def get_args():
    parser = argparse.ArgumentParser(description=help_description, epilog=example,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    requiredArguments = parser.add_argument_group('required arguments.')
    requiredArguments.add_argument('-re', type=str, dest='regex',
                                   help='regular expression for getting list of files to tar', required=True)
    requiredArguments.add_argument('-d','--dqr', type=str, dest='dqr', help='DQR ID')
    requiredArguments.add_argument('-r', '--rid', type=str, dest='rid', help='RID')

    parser.add_argument('-D', '--debug', action='store_true', dest='debug', help='print debug statements.')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = get_args()
    files = glob.glob(args.regex)
    main(files, args)