#!/apps/base/python3/bin/python3

import os
import re
import sys
import glob
import tarfile
import argparse
import itertools

def tar_em():
    args = get_args()
    files = glob.glob(args.regex)
    ordered_files = sorted(files)


    try:
        cutfile_index = re.findall('\d+', args.cutfile)
        print('cut file index = {}'.format(cutfile_index))
    except Exception:
        cutfile_index = False
    try:
        addfile_index = re.search('\d+', args.addfile).group(0)
        addfile_pattern = args.addfile.split(' ')[1]
        print('add file index = {}\nadd file pattern = {}'.format(addfile_index, addfile_pattern))
    except Exception:
        addfile_index = False
        addfile_pattern = False


    user_continue()

    if addfile_index and cutfile_index:
        for i, n in enumerate(cutfile_index):
            cutfile_index[i] = str(eval(n) + 1)


    for f in ordered_files:
        split_file = f.split(args.delimiter)
        file_name = ''

        if addfile_index:
            # this loop will add the pattern to the tar file
            for i, section in enumerate(split_file):
                # if we are at the index specified to add pattern 
                if str(i) in addfile_index:
                    if i == 0:
                        file_name = '{}.{}'.format(addfile_pattern, split_file[i])
                    elif i == len(split_file):
                        file_name = '{}.{}'.format(file_name, addfile_pattern)
                    else:
                        # add pattern before the specified index so it will work at index 0
                        file_name = '{}.{}.{}'.format(file_name, addfile_pattern, split_file[i])
                # if not at specified index then reconstruct the original file name
                else:
                    # if first element then simple assignment
                    if i == 0:
                        file_name = split_file[i]
                    # else construct name with . delimiter
                    else:
                        file_name = '{}.{}'.format(file_name, split_file[i])

            split_file = file_name.split('.')

        if cutfile_index:
            file_name = ''
            for i, section in enumerate(split_file):
                if str(i) not in cutfile_index:
                    if len(file_name) == 0:
                        file_name = split_file[i]
                    else:
                        file_name = '{}.{}'.format(file_name, split_file[i])
        if not addfile_index and not cutfile_index:
            file_name = f

        os.system('mv {} {}'.format(f, file_name))

        print('{} ---> {}'.format(f, file_name))


def user_continue():
    flag = True
    while flag:
        decision = input('Continue? (y or n) :')
        if decision == 'y' or decision == 'yes':
            flag = False
        elif decision == 'n' or decision == 'no':
            print('aborting strip file name script.')
            exit(0)

help_description='''
This script will cut or add to file names in a directory.\n
Build a list using the regular expression given,\n
Adding a pattern to a file will be in front of the \n
  index specified. "0" would be before index "0" or\n
  at the front of the file. Adding at the end of the\n
  file gets a little buggy if you are also removing \n
  indexes. Indexes start at 0, cutting the 0 element \n
  will cut the first element of the filename. File elements \n
  are defined by splitting the file name on '.'.
'''

example ='''
EXAMPLE: \n
  strip_file_name.py -re 'sgpmetE33*' -cf '4' -af '1 Table2.'\n
  strip_file_name.py -re '*201706*' -af '2 table1'\n
  strip_file_name.py -re '*20170[56]*' -cf '1 3 4 5 6'\n

ERRORS:\n
  strip_file_name.py -c '1' --> must have -re argument\n
  strip_file_name.py -re 'oliaos*' -c '1 3 4 5' -a '5 test' --> adding and removing is buggy, \n
        lower add index to achive intended results\n
  strip_file_name.py -re 'sgp*' -af 'Table2 1' --> will add "1" at index "2"\n
'''

def get_args():
    parser = argparse.ArgumentParser(description=help_description, epilog=example, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    requiredArguments = parser.add_argument_group('required arguments.')
    requiredArguments.add_argument('-re', type=str, dest='regex', help='regular expression for getting list of files to rename', required=True)

    parser.add_argument('-cf', type=str, dest='cutfile', help='cut index from files')
    parser.add_argument('-af', type=str, dest='addfile', help='add argument to file names at index')
    parser.add_argument('-d','--delimiter', type=str, dest='delimiter', default='.', help='')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    tar_em()

