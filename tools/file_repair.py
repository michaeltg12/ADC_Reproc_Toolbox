#!/apps/base/python3/bin/python3

import argparse
import csv
import json
import os
import shutil
from tempfile import NamedTemporaryFile

def main(debug):
    if os.path.exists('duplicate.json'):
        dup_json = json.load(open('duplicate.json'))
        print('\t*** Duplicate records. ***')
        for key, value in dup_json.items():
            print('{}\n{}\n'.format(key, value))

        while True:
            user_input = input('delete duplicate records? y or n: ')
            if user_input == 'y':
                for file_name_key, duplicate_values in dup_json.items():
                    for value in duplicate_values:
                        with open(file_name_key, 'r') as csvfile:
                            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                            for line in reader:
                                temp = ','.join(line[1:4])
                                if temp == value:
                                    print(line)
                            while True:
                                print(file_name_key)
                                delete_line = input('Which line to delete? ex: 1, 2, 3 or 0 for none: ')
                                int_check = delete_line.replace(' ', '').replace(',', '')
                                try:
                                    if type(eval(int_check)) == int:
                                        break
                                except Exception:
                                    pass

                        if '0' not in delete_line:
                            print('Deleting record(s) - {}'.format(delete_line))
                            tempfileobj = NamedTemporaryFile(mode='wt', delete=False)
                            with open(file_name_key, 'r') as csvfile, open(tempfileobj.name, 'w') as tempfile:
                                reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                                writer = csv.writer(tempfile, delimiter=',', quotechar='"')
                                counter = 1

                                for line in reader:
                                    temp = ','.join(line[1:4])
                                    if temp == value and str(counter) not in delete_line:
                                        print('not deleting line {}.'.format(counter))
                                        writer.writerow(line)
                                        counter += 1
                                    elif temp == value:
                                        print('deleting line - {}'.format(counter))
                                        counter += 1
                                    else:
                                        writer.writerow(line)
                            
                            shutil.move(tempfile.name, file_name_key)
                break
            elif user_input == 'n':
                break
    else:
        print('no duplicate records json file.')

    if os.path.exists('outside.json'):
        out_json = json.load(open('outside.json'))
        print('\t*** Records outside normal file range. ***')
        for key, value in out_json.items():
            print('{}\n{}\n'.format(key, value))
        while True:
            user_input = input('delete outside records? y or n: ')
            if user_input == 'y':
                for file_name_key, outside_values in out_json.items():
                    with open(file_name_key, 'r') as csvfile:
                        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                        for line in reader:
                            temp = ','.join(line[1:4])
                            if temp in outside_values:
                                print(line)
                        print('file --> {}'.format(file_name_key))
                        delete_check = input('delete outside records for this file? y or n: ')
                        if delete_check == 'y':
                            tempfileobj = NamedTemporaryFile(mode='wt', delete=False)
                            with open(file_name_key, 'r') as csvfile, open(tempfileobj.name, 'w') as tempfile:
                                reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                                writer = csv.writer(tempfile, delimiter=',', quotechar='"')
                                exclude = []
                               
                                for line in reader:
                                    temp = ','.join(line[1:4])
                                    if temp not in outside_values:
                                        writer.writerow(line)
                                      
                            shutil.move(tempfile.name, file_name_key)
                            if os.path.getsize(file_name_key) < 1:
                                print('removing empty file - {}\n'.format(file_name_key))
                                os.remove(file_name_key)
                break
            elif user_input == 'n':
                break
    else:
        print('no outside records json file.')

help_description='''
This repairs overlapping records and records that are more
than one day different than the file name date.
'''

example ='''
EXAMPLE: \n
    None

ERRORS:\n
    IDK
'''

def get_args():
    parser = argparse.ArgumentParser(description=help_description, epilog=example,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-D', '--debug', action='store_true', dest='debug', help='print debug statements.')
    
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = get_args()
    main(args.debug)

