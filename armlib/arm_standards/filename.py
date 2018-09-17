import os
from datetime import datetime
import re


class InvalidARMFileName(Exception):
    pass


def split_versioned_filename(fullPath):
    versioned_filename = os.path.basename(fullPath)
    regex = '^(\S+).v(\d+)$'
    m = re.search(regex, versioned_filename)
    if not m:
        raise InvalidARMFileName('Invalid Versioned Filename')
    unversioned_filename, version = m.groups()
    try:
        version = int(version)
    except ValueError:
        raise  InvalidARMFileName('Invalid Version on %s'%(versioned_filename))
    return unversioned_filename, version


def get_datastream(armfile):
    basename = os.path.basename(armfile)
    filename_pieces = basename.split('.')
    datastream = ".".join(filename_pieces[:2])
    return datastream


def get_data_archive_path(filename):
    verify_arm_filename(filename)
    pieces = filename.split('.')
    if pieces[-1][0] == 'v':
        unversioned_filename = '.'.join(pieces[:-1])
    else:
        unversioned_filename = filename
    DATA_ARCHIVE = '/data/archive'
    site = unversioned_filename[:3]
    datastream = get_datastream(unversioned_filename)
    full_path = os.path.join(DATA_ARCHIVE, site, datastream, unversioned_filename)
    return full_path


def parse_datastream(filename):
    base = os.path.basename(filename)
    datastream = get_datastream(base)
    pattern = r'(?P<dev>D?)(?P<site>\S\S\S)(?P<time_integration>\d*)(?P<instrument>\S*)(?P<facility>[A-Z]\d+).(?P<data_level>\S+)'
    groups = re.search(pattern, datastream)
    if not groups: return None
    return groups.groupdict()


def verify_arm_filename(fullPath):
    base_name = os.path.basename(fullPath)
    pieces = base_name.split('.')
    number_of_periods = len(pieces)
    if number_of_periods < 5:
        raise InvalidARMFileName('Too Few Fields in name: %s'%(base_name))
    ymd = pieces[2]
    # yyyymmdd
    if len(ymd) != 8 and len(ymd) != 6:
        raise InvalidARMFileName('Invalid Date in name:%s'%(base_name))

    period_date = ".".join(pieces[2:4])
    if len(ymd) == 6:
        period_date = '19' + period_date

    #hhmmss
    hms = pieces[3]
    if len(hms) != 6:
        raise InvalidARMFileName('Invalid Timestamp in name: %s'%(base_name))

    try:
        datetime.strptime(period_date, '%Y%m%d.%H%M%S')
    except ValueError as ve:
        raise InvalidARMFileName('Invalid Date in name: %s'%(str(ve)))

    data_format = pieces[4]
    if len(data_format) >= 4:
        raise InvalidARMFileName('Invalid data format: %s' % (base_name))