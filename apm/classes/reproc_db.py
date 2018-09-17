#!/apps/base/python3/bin/python3

"""
********** info_from_dqr **********

Author: Alka Singh
Email: singhar@ornl.gov

Purpose:
    DQR level reprocessing.

    A process can call this to process information out of a DQR.

Note:
    Not done yet

Classes:
    ProcessingDQR

Methods:
    get_time_period
    get_affected_datastreams
    get_affected_files
    is_equation_present
    get_equation
    get_to_users_list
    get_bcc_users_list
    is_equation_present


Example:

Attributes:


Todo:
    Add get users by filenames
    Add get users by DQR

"""

# Import required libraries
import argparse
from datetime import timedelta
import logging
import os
import sys

from inspect import currentframe, getframeinfo
cf = currentframe()

# try importing armlib
try:
    import armlib
except ImportError:
    armlib = os.path.join(os.environ.get("REPROC_HOME"), "ADC_Reproc_Manager")
    if not os.path.isdir(armlib):
        assert False, 'cannot find armlib'
    sys.path.insert(0, armlib)

from armlib.config import load_config
from armlib.database import ARMNamedTupleAutocommitConnection


class ReprocDB(object):
    def __init__(self, postgres_config, dqr):
        # for logging and db connection
        self.logger = logging.getLogger('root.ProcessingDQR')
        self.conn = ARMNamedTupleAutocommitConnection(**postgres_config)
        self.dqr_id = dqr

    def get_time_period(self):
        """
        This method finds the start and end date of a DQR based on dqrid
        """
        start_date = ""
        end_date = ""

        args = [self.dqr_id]
        sql_time = "SELECT  distinct start_date, end_date " \
                   "FROM pifcardqr2.varname_metric " \
                   "WHERE id = %s"

        results_tp = self.conn.fexecute(sql_time, args)

        for entry in results_tp:
            start_date = entry.start_date
            end_date = entry.end_date

        return start_date, end_date

    def get_affected_datastreams(self):
        """
        This method creates a list of affected datastreams by a dqr based on dqrid
        :returns a list of affected datastreams
        """
        affected_ds = []
        args = [self.dqr_id]
        sql_ds = "SELECT distinct datastream " \
                 "FROM pifcardqr2.varname_metric " \
                 "WHERE id = %s"

        results_ds = self.conn.fexecute(sql_ds, args)
        # print(results_ds)

        for entry in results_ds:
            affected_ds.append(entry.datastream)
        # print(affected_ds)
        return affected_ds

    def get_affected_files(self, affected_ds, start, end):
        """
        This method creates a list of affected filenames by a dqr based on dqrid
        :returns a list of affected filenames
        """
        all_levels = True  # get all data levels of the datastream
        affected_files = []
        # start_date, end_date, earlier, later = self.get_time_period(dqr_id)
        # affected_ds = self.get_affected_datastreams(dqr_id)

        for entry in affected_ds:
            if all_levels:
                file_name = str(entry[0]).split(".")[0] + '%'
            else:
                file_name = str(entry[0]) + '%'

            # TODO speed up this query!!
            args = [file_name, start, end, start, end, start]
            sql_fn = "SELECT versioned_filename " \
                     "FROM data_reception.file_contents " \
                     "WHERE versioned_filename LIKE %s and " \
                     "((start_time BETWEEN %s and %s ) " \
                     "or (end_time BETWEEN %s and %s ) " \
                     "or (%s BETWEEN start_time and end_time ) )"

            results_fn = self.conn.fexecute(sql_fn, args)
            for row in results_fn:
                affected_files.append(row)
        # self.logger.debug(affected_files)
        return affected_files

    def is_equation_present(self, dqr_id):
        """
        This method finds if there is a equation present for a dqrid to help in reprocessing
        :returns true if there is an equation or false otherwise
        """
        args = [dqr_id]

        sql_sugg = "SELECT equation " \
                   "FROM pifcardqr2.reprocessing " \
                   "WHERE dqr = %s"

        results = self.conn.fexecute(sql_sugg, args)

        if len(results) == 0 or not results[0].equation:
            return False
        else:
            return True

    def get_equation(self, dqr_id):
        """
        This method extracts the equation from a dqr based on dqrid
        :returns a string of equation(s) based on which reprocessing would be done
        """
        equation = ""
        args = [dqr_id]

        sql_equation = "SELECT equation " \
                       "FROM pifcardqr2.reprocessing " \
                       "WHERE dqr = %s"

        results_equation = self.conn.fexecute(sql_equation, args)

        for entry in results_equation:
            equation = entry.equation  # print("The suggested equation is: " + str(equation))

        return equation

    def get_to_users_list(self):
        """
        This method extracts a list of emails who are either developers or instrument mentors of an affected instrument
        :returns a string of emails of instrument mentors or developers
        """
        to_users = []
        affected_ds = self.get_affected_datastreams()
        for ds in affected_ds:
            args = [ds]
            sql_to = "select distinct email " \
                     "from people.people p " \
                     "inner join people.group_role gr on gr.person_id = p.person_id " \
                     "inner join arm_int2.datastream_info di on upper(di.instrument_code) = gr.role_name " \
                     "where datastream = %s"

            results_to = self.conn.fexecute(sql_to, args)

            for entry in results_to:
                to_users.append(entry.email)
        # print("To Users:  " + str(to_users))
        return to_users

    def get_bcc_users_list(self, affected_files):
        """
        This method extracts a list of emails of users who has ordered the data in last 5 years
        :returns a string of emails of users of data in last 5 years
        """
        bcc_users = set()
        start_date, end_date, early, late = self.get_time_period()

        for file in affected_files:
            args = [file, start_date]

            # this is pointed to people.people
            sql_sessid = "SELECT distinct email " \
                         "FROM arm.current_retrievals cr " \
                         "inner join arm.retrieval_archive ra on ra.session_id=cr.session_id " \
                         "inner join people.people p on ra.arch_user_id = p.arch_user_id " \
                         "WHERE new_filename = %s and session_date >= %s"

            results_sess_id = self.conn.fexecute(sql_sessid, args)

            # Adding users in bcc list
            for entry in results_sess_id:
                # print(entry.user_email)
                bcc_users.add(entry.email)

        print("BCC Users:  " + str(bcc_users))
        return bcc_users

    @staticmethod
    def sort_files(affected_files, start_date, end_date):
        # cut filename without version from results and sort
        raw_tar_list = []
        netcdf_file_list = []
        raw_tar_buffer = []
        netcdf_file_buffer = []

        for f in affected_files:
            no_version = ((str(f).split("'"))[1])[:-3]
            split_file = no_version.split('.')
            if no_version[-3:] == 'tar':
                raw_tar_buffer.append(no_version)
                if split_file[2] >= start_date.strftime('%Y%m%d') and split_file[2] <= end_date.strftime('%Y%m%d'):
                    raw_tar_list.append(no_version)

            elif no_version[-3:] == 'cdf' or no_version[-2:] == 'nc':
                netcdf_file_buffer.append(no_version)
                if split_file[2] >= start_date.strftime('%Y%m%d') and split_file[2] <= end_date.strftime('%Y%m%d'):
                    netcdf_file_list.append(no_version)
        return raw_tar_list, netcdf_file_list, raw_tar_buffer, netcdf_file_buffer


################################################################################

def do_setup():
    """
    This method does the initial setup for authentication etc
    """
    file_path = os.path.dirname('/data/project/0021718_1509993009/ARM_Reprocessing_Toolbox/')
    config_file_name = '.config.ini'
    config_path = os.path.join(file_path, config_file_name)

    config = load_config(config_path)

    return config


help_description = '''
This is a module for quering the database to find information about a
reprocessing task given only a DQR #
'''

example = '''
EXAMPLE: 
'''


def setup_args():
    parser = argparse.ArgumentParser(description=help_description, epilog=example,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('dqr', type=str, help='dqr number for looking up info about reprocessing task')
    parser.add_argument('-v', default=False, action='store_true', dest='verbose',
                        help='shows all datastreams that match your search criteria')
    args = parser.parse_args()
    return args

# if __name__ == '__main__':
#     import argparse
#     args = setup_args()
#     if not len(sys.argv) > 1:
#         sys.argv.append("-h")
#     else:
#         print(str(args))

#     config = do_setup()
