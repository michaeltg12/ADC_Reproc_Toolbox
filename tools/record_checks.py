#!/apps/base/python3/bin/python3

import argparse
from collections import defaultdict
import csv
import datetime
from glob import glob
import json
import re

from dateutil.parser import parse

try:
    from progress.bar import PixelBar
except ImportError:
    from .progress.bar import PixelBar


class CustomProgress(PixelBar):
    message = 'Checking file records'
    suffix = '%(percent).1f%% eta:%(eta)ds  - elapsed:%(elapsed)ds'


def check_records(args):
    full = []
    outside = defaultdict(list)
    duplicate = defaultdict(list)
    prog = CustomProgress(max=len(args.full_path_list))
    time_delta_limit = datetime.timedelta(args.time_delta)

    # try:
    for file_path in args.full_path_list:
        file_date = re.findall('\d{8}', file_path)[0]
        if args.date_format:
            file_datetime = datetime.datetime.strptime(file_date, "%Y%m%d")
        else:
            file_datetime = make_datetime([file_date])  # def needs an array

        reader = csv.reader(open(file_path), delimiter=args.delimiter)
        for i, line in enumerate(reader):
            if i >= args.header:
                date_array = []
                # print('date columns --> {}'.format(args.date_column))
                for i in args.date_column.split(','):
                    date_array.append(line[int(i)])
                # print(date_array)
                try:
                    if args.date_format:
                        temp = " ".join(date_array)
                        record_datetime = datetime.datetime.strptime(temp, args.date_format)
                    else:
                        record_datetime = make_datetime(date_array)
                        record_datetime_str = str(record_datetime)
                    if time_delta_limit < abs(file_datetime - record_datetime):
                        outside[file_path].append(record_datetime_str)
                except ValueError as ve:
                    print('\nvalue error: {}'.format(ve))
                    if file_path not in outside:
                        outside[file_path].append(record_datetime_str)

                if record_datetime_str in full:
                    if record_datetime_str not in duplicate[file_path]:
                        duplicate[file_path].append(record_datetime_str)
                else:
                    full.append(record_datetime_str)

        if len(full) > args.window:
            full = full[:args.window]
        prog.next()
    # except Exception as e:
    #     print("\n\t{} --> {}".format(file_path, e))
    prog.finish()

    if duplicate:
        with open('duplicate.json', 'w') as dup_json:
            json.dump(duplicate, dup_json)
        if args.debug:
            print('\t*** Duplicate records exist in the following files. ***\n')
            for key, value in duplicate.items():
                print('{}\n{}\n'.format(key, value))

    if outside:
        with open('outside.json', 'w') as out_json:
            json.dump(outside, out_json)
        if args.debug:
            print('\t*** Record dates outside normal file range for the following files. ***\n')
            for key, value in outside.items():
                print('{}\n{}\n'.format(key, value))

    if not duplicate and not outside:
        print('No duplicate or outside records.')


def make_datetime(date_array):
    if len(date_array) == 1:
        # print("len 1")
        date_time = parse(date_array[0], fuzzy=True)
        # print(date_time)
    elif len(date_array) > 1:
        # print("len {}".format(len(date_array)))
        date_string = ""
        for element in date_array:
            date_string = "{} {}".format(date_string, element)
            # print(date_string)
        date_string = date_string.strip()
        # print(date_string)
        date_time = parse(date_string, fuzzy=True)
        # print(date_time)
    return date_time


def interactive(args):
    affirmative = ['yes', 'y', 'cowabunga', 'ole']
    break_values = ['no', 'nope', 'out', 'bye', 'exit', 'default', 'd', 'def']
    print("break values = {}".format(break_values))
    while True:
        files = None
        args.regex = input("\nRegex to get files (* for all) --> ")
        try:
            print(type(args.regex), args.regex, files)
            files = glob(args.regex)
            files.sort()
        except ValueError:
            print("Regex must be a string.")
        if files:
            print('First 5 -->')
            for f in files[:5]:
                print(f)
            print('Last 5 -->')
            for f in files[-5:]:
                print(f)
            if input("Is this correct (y, n)? ") == 'y':
                break
        else:
            print("Empty file list")
        if args.regex in break_values:
            break

    while True:
        args.header = input("\nHow many lines long is the header --> ")
        if args.header in break_values:
            print('Defaulted to no header')
            args.header = 0
            break
        try:
            args.header = abs(int(args.header))
            if input("Is this correct --> {} <-- : ".format(args.header)) in affirmative:
                break
        except ValueError:
            print("Must enter an integer.")

    while True:
        print('\nThe column numbers start at 0. There can be multiple columns separated by a comma.\n'
              'Example: <0, 1>')
        args.date_column = input("What column is the date in --> ")
        if args.date_column in break_values:
            print('Default to first column (index 0)')
            args.date_column = 0
            break
        elif input("Is this correct --> {} <-- : ".format(args.date_column)) in affirmative:
                break
        else: pass

    while True:
        print("\nWindow is a slididng number of records to store in memory and check for overlapping dates.\n"
              "Default is 2000, the program takes significantly longer to run the larger the number.")
        args.window = input("What would you like the window to be --> ")
        if args.window in break_values:
            print('Default window set to 2000 records.')
            args.window = 2000
            break
        try:
            args.window = abs(int(args.window))
            if input("Is this correct --> {} <-- : ".format(args.window)) in affirmative:
                break
        except ValueError:
            print("Must be an integer.")

    while True:
        print("\nTime delta is the number of days from the date on the file name that would trigger that file\n"
              "to be marked as having timestamps outside the expected range. Default is 7 days away from date \n"
              "on file name.")
        args.time_delta = input("What would you like the time delta to be --> ")
        if args.time_delta in break_values:
            print('Default window set to 7 days.')
            args.time_delta = 7
            break
        try:
            args.time_delta = abs(int(args.time_delta))
            if input("Is this correct --> {} <-- : ".format(args.time_delta)) in affirmative:
                break
        except ValueError:
            print("Must be an integer.")

    while True:
        print("\nWould you like to specify the date format?\n"
              "Example: <%Y%m%d>, <%Y,%j>, <%Y%m%d %H%M%S>, or <%Y-%m-%dT%H:%M:%S> just to name a few.\n"
              "Year = %Y, month = %m, day = %d, julian day = %j, hour = %H, minute = %M, second = %S.\n"
              "Default will try and parse it on it's own (enter <default>). ** will not support 2 digit year **\n"
              "If the date is in more than one column, each element will be separated by a space.")
        args.date_format = input("Enter date format --> ")
        if args.date_format in break_values:
            print("Default parser selected.")
            args.date_format = None
            break
        elif input("Is this correct --> {} <-- : ".format(args.date_format)) in affirmative:
            break

    while True:
        print("\nFile delimiter can be (, \\t comma tab . space) ")
        args.delimiter = input("What is the file delimiter --> ")
        if args.delimiter in break_values:
            print("Default delimiter comma selected")
            args.delimiter = ','
            break
        elif input("Is this correct --> {} <-- : ".format(args.delimiter)) in affirmative:
            if args.delimiter == 'tab' or args.delimiter == '\\t':
                args.delimiter = '\t'
            elif args.delimiter == 'comma' or args.delimiter == ',':
                args.delimiter == ','
            elif args.delimiter == 'space':
                args.delimiter = ' '
            break

    args.debug = True
    return args


help_description = '''
This finds overlapping time records and records that are more
than one day away from the file date. Compares records within 
a sliding window. Default window is ~2 weeks but can be altered.
Increasing the windows significantly slows execution. 
'''

example = '''
EXAMPLE: \n
    record-checks -re 'sgpmetE38*'

ERRORS:\n
    IDK
'''


def get_args():
    parser = argparse.ArgumentParser(description=help_description, epilog=example,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # requiredArguments = parser.add_argument_group('required arguments.')
    # requiredArguments.add_argument('-re', type=str, dest='regex',
    #                                help='regular expression for getting list of files to tar', required=True)

    parser.add_argument('-re', type=str, dest='regex', help='regular expression for getting list of files to tar')
    parser.add_argument('-n', '--num', type=int, default=0, dest='recordnumber', help='record number')
    parser.add_argument('-D', '--debug', action='store_true', dest='debug', help='print debug statements.')
    parser.add_argument('-w', '--window', type=int, default=2000, dest='window',
                        help='declare sliding window for overlap comparison.')
    parser.add_argument('-t', '--time_delta', type=int, default=7, dest='time_delta',
                        help='number of days away from file name date to be considered outside.')
    parser.add_argument("-I", "--interactive", action="store_true", dest="interactive")
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    if args.interactive:
        args = interactive(args)
    args.full_path_list = glob(args.regex)
    args.recordnumber = args.recordnumber
    check_records(args)

