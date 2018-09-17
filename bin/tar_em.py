#!/apps/base/python3/bin/python3

import os
import re
import sys
import glob
import tarfile
import argparse
import itertools

try:
    from progress.bar import PixelBar
except ImportError:
    from .progress.bar import PixelBar
    
class CustomProgress(PixelBar):
    message = 'Creating tar files.'
    suffix = '%(percent).1f%% eta:%(eta)ds  - elapsed:%(elapsed)ds'

def tar_em():
    args = get_args()
    files = glob.glob(args.regex)
    ordered_files = sorted(files)
    tar_name_dict = {}
    prog = CustomProgress(max=len(files))

    try:
        cutar_index = re.findall('\d+', args.cutar)
        print('cut tar index = {}'.format(cutar_index))
    except Exception:
        cutar_index = False
    try:
        addtar_index = re.search('\d+', args.addtar).group(0)
        addtar_pattern = args.addtar.split(' ')[1]
        print('add tar index = {}\nadd tar pattern = {}'.format(addtar_index, addtar_pattern))
    except Exception:
        addtar_index = False
        addtar_pattern = False
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

    # if we added an element to the filename, increment cut tar indexes
    if addtar_index and cutar_index:
        for i, n in enumerate(cutar_index):
            cutar_index[i] = str(eval(n) + 1)
    for f in ordered_files:
        split_tar = f.split('.')
        day = split_tar[2]
        if day not in tar_name_dict.keys():
            tar_file_name = ''
            if addtar_index:
                # this loop will add the pattern to the tar file
                for i, section in enumerate(split_tar):
                    # if we are at the index specified to add pattern 
                    if str(i) in addtar_index:
                        if i == 0:
                            tar_file_name = '{}.{}'.format(addtar_pattern, split_tar[i])
                        elif i == len(split_tar):
                            tar_file_name = '{}.{}'.format(tar_file_name, addtar_pattern)
                        else:
                            # add pattern before the specified index so it will work at index 0
                            tar_file_name = '{}.{}.{}'.format(tar_file_name, addtar_pattern, split_tar[i])
                    # if not at specified index then reconstruct the original file name
                    else:
                        # if first element then simple assignment
                        if i == 0:
                            tar_file_name = split_tar[i]
                        # else construct name with . delimiter
                        else:
                            tar_file_name = '{}.{}'.format(tar_file_name, split_tar[i])

                split_tar = tar_file_name.split('.')

            if cutar_index:
                tar_file_name = ''
                for i, section in enumerate(split_tar):
                    if str(i) not in cutar_index:
                        if i == 0:
                            tar_file_name = split_tar[i]
                        else:
                            tar_file_name = '{}.{}'.format(tar_file_name, split_tar[i])

            if not addtar_index and not cutar_index:
                tar_file_name = f
            if tar_file_name[-3:] != 'tar':
                    tar_file_name = '{}.{}'.format(tar_file_name, 'tar')

            tar_name_dict[day] = tar_file_name

    user_continue()

    if addfile_index and cutfile_index:
        for i, n in enumerate(cutfile_index):
            cutfile_index[i] = str(eval(n) + 1)

    if not os.path.exists('files_in_tars'):
            os.makedirs('files_in_tars')

    tarlog = open('files_in_tars/tar.log', 'w+')
    for day, name in tar_name_dict.items():
        tarlog.write('file day = {}\n'.format(day))
        tar = tarfile.open(name, "w")
        move_files = []
        for f in ordered_files:
            split_file = f.split('.')
            if eval(split_file[2]) == eval(day):
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

                if file_name == f:
                    tar.add(file_name)
                else:
                    os.system('cp {} {}'.format(f, file_name))
                    tar.add(file_name)
                    os.system('rm {}'.format(file_name))
                tarlog.write('{} ---> {}\n'.format(file_name, name))
                

                move_files.append(f)
                prog.next()
        for f in move_files:
                os.system('mv {} files_in_tars/'.format(f))
        tar.close()
    prog.finish()
    tarlog.close()
    print('Log and original files in --> files_in_tars/')

def user_continue():
    flag = True
    while flag:
        decision = input('Continue? (y or n) :')
        if decision == 'y' or decision == 'yes':
            flag = False
        elif decision == 'n' or decision == 'no':
            print('aborting tar files script.')
            exit(0)

help_description='''
This script will tar files in a directory.\n
Build a list using the regular expression given,\n
  add pattern to tar or files at the given index,\n
  remove element from tar or files at the given index.\n
Tar file will have the name of the first file of each day.\n
Files will be added to tars and originals moved to,\n
  files_in_tars/ \n
  under the current working directory.\n 
Adding a pattern to a file will be in front of the \n
  index specified. "0" would be before index "0" or\n
  at the front of the file. Adding at the end of the\n
  file gets a little buggy if you are also removing \n
  indexes. All tar files will have .tar at then end.\n
'''

example ='''
EXAMPLE: \n
    tar_em.py -re 'sgpmetE33*' -ct '3' -at '0 Talbe1.' -cf '4' -af '1 Table2.'\n
    tar_em.py -re '*201706*' -ct '5' -af '2 table1'\n
    tar_em.py -re '*20170[56]*' -ct '1 3 4 5 6'\n

ERRORS:\n
    tar_em.py -ct '1' --> must have -re argument\n
    tar_em.py -re 'oliaos*' -ct '1 3 4 5' -at '5 test' --> adding and removing is buggy, \n
        lower add index to achive intended results\n
    tar_em.py -re 'sgp*' -af 'Table2 1' --> will add "1" at index "2"\n
'''

def get_args():
    parser = argparse.ArgumentParser(description=help_description, epilog=example, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    requiredArguments = parser.add_argument_group('required arguments.')
    requiredArguments.add_argument('-re', type=str, dest='regex', help='regular expression for getting list of files to tar', required=True)

    parser.add_argument('-ct', type=str, dest='cutar', help='cut index from tar')
    parser.add_argument('-at', type=str, dest='addtar', help='add argument to tar file name at index')

    parser.add_argument('-cf', type=str, dest='cutfile', help='cut index from files')
    parser.add_argument('-af', type=str, dest='addfile', help='add argument to file names at index')

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    tar_em()

