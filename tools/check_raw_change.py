#!/apps/base/python3/bin/python3

import os
import re
import sys
import glob
import tarfile

match_data = re.compile("\A\d+")
match_header = re.compile("\A\D+")
working_dir = "/data/home/giansiracusa/testing_dir/raw_change"

def main(dir, ds):
    # get tar file names
    tar_files = glob.glob(os.path.join(dir,"*"))
    tar_files.sort()

    # check type
    print('starting binary search')
    binary_search(tar_files, ds)

def binary_search(tar_file_search, ds):
    if len(tar_file_search) == 0:
        print('finished search')
        return False
    else:
        print("\n\t***************************************************\n"
              "\t******************** Searching ********************\n"
              "\t***************************************************\n")
        middle = len(tar_file_search) // 2
        print("middle = {}".format(middle))

        #print("\nold file --> {}".format(tar_file_search[0]))
        for i in range(len(tar_file_search)):
            older_type = check_type(tar_file_search[i])
            if older_type != False:
                break

        #print("\nnew file --> {}".format(tar_file_search[-1]))
        for i in range(len(tar_file_search)):
            index = -1-i
            newest_type = check_type(tar_file_search[index])
            if newest_type != False:
                break

        if older_type == newest_type:
            print("{} --> No detectable change in file format between files\n{}\n{}"
                  .format(ds, tar_file_search[0], tar_file_search[-1]))
            return False

        #print("\nmiddle file --> {}".format(tar_file_search[middle]))
        middle_type = check_type(tar_file_search[middle])
        if older_type != middle_type:
            if middle <= 4:
                print("\n\t******************************************************\n"
                      "\t******************** Found Change ********************\n"
                      "\t******************************************************\n"
                      "{} --> change occurs between the following files\n{}\n{}".
                      format(ds, tar_file_search[0], tar_file_search[middle]))
            else: print("Searching first half.")
            binary_search(tar_file_search[:middle], ds)
        elif middle_type != newest_type:
            if middle <= 4:
                print("\n\t******************************************************\n"
                      "\t******************** Found Change ********************\n"
                      "\t******************************************************\n"
                      "{}--> change occurs between the following files\n{}\n{}"
                      .format(ds, tar_file_search[middle], tar_file_search[-1]))
            else:
                print("Searching second half.")
            binary_search(tar_file_search[middle:], ds)
        else:
            print("error on middle = {}".format(middle))

def check_type(tar_file):
    raw_file = extract_tar(tar_file, working_dir)
    if raw_file == False:
        return False
    raw_path = os.path.join(working_dir, raw_file)
    first_line = open(raw_path).readline()
    if match_data.search(first_line):
        os.remove(raw_file)
        print("data --> {}".format(raw_file))
        print("first line --> {}".format(first_line, end=''))
        return "data"
    elif match_header.search(first_line):
        print("header --> {}".format(raw_file))
        print("first line --> {}".format(first_line, end=''))
        os.remove(raw_file)
        return "header"
    else:
        print("unknown --> {}".format(raw_file))
        print("first line --> {}".format(first_line, end=''))
        os.remove(raw_file)
        print("{}--> unknown format -->{}".format(raw_file, first_line))
        return "unknown"

def extract_tar(tar_file, working_dir):
    tar_errors = []
    ret_file = ""
    try:
        tar = tarfile.open(tar_file)
        for i, member in enumerate(tar.getmembers()):
            if i == 0 and member.isreg():
                member.name = os.path.basename(member.name)
                tar.extract(member, working_dir)
                ret_file = member.name
    except FileNotFoundError:
        tar_errors.append(tar_file)
        return False
    except tarfile.ReadError:
        tar_errors.append(tar_file)
        return False
    if len(tar_errors) > 0: print("error on --> {}".format(tar_errors))
    return ret_file

if __name__ == "__main__":
    try:
        ds = sys.argv[1]
        dir = os.path.join("/data/archive", ds[:3], ds)
        os.path.isdir(dir)
    except IndexError:
        print("using cwd")
        main(os.getcwd())
    else:
        if os.path.isdir(dir):
            main(dir, ds)
        else:
            print("directory dne --> {}".format(dir))