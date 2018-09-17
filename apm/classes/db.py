#!/apps/base/python3/bin/python3

import os
from dsdb import DSDB

global home

home = os.path.expanduser('~')

class DB():
    """ DSDB Database interface """

    def __init__(self, config, db_file=None, alias=None):
        """ Initialize database connection """
        # if passing in db_file or alias both must be passed in order to specify one
        self.config = config
        self.type = 'Ingest' if self.config['ingest'] else 'VAP'

        self.db_file = None
        self.alias = None

        pattern = 'get_process_{}_ds_classes'
        self.output = pattern.format('output')
        self.input = pattern.format('input')
        self.inquire = 'inquire_remote_tags'


        if db_file and alias:
            self.db_file = db_file
            self.alias = alias
        else:
            db_file = '{}/.db_connect'.format(home)
            if os.path.exists(db_file):
                if self.alias_exists('apm', db_file):
                    self.db_file = db_file
                    self.alias = 'apm'
                elif self.alias_exists('dsdb', db_file):
                    self.db_file = db_file
                    self.alias = 'dsdb'

            if self.db_file == None and os.path.exists('/apps/ds/conf/dsdb/.db_connect'):
                self.db_file = '/apps/ds/conf/dsdb/.db_connect'
                self.alias = 'dsdb_dmf'

            if self.db_file == None or self.alias == None:
                exit("Error: Unable to connect to the database. No .db_connect file found.")


    def query(self, query, columns=None):
        db = DSDB(conn_file=self.db_file, alias=self.alias)
        result = []

        if columns:
            result = db.query(query, cols=columns)
        else:
            result = db.query(query)

        return result



    def get_data_paths(self):
        """ Get the input and output data paths from the database """
        if self.config['datastream'] == None:
            return self.get_instrument_path()
        else:
            return self.get_datastream_paths()

    def get_datastream_path(self, stream):
        """ Get a single datastream path from the database """
        # Check this path and decide on the correct file and alias
        self.db_file = '/apps/ds/conf/datainv/.db_connect'
        self.alias = 'inv_read'

        paths = []
        if type(stream) != str:
            stream = str(stream)

        db = DSDB(conn_file=self.db_file, alias=self.alias)
        stream = stream.replace('*', '%')
        info = ['datastream', 'checked', 'synced', 'cache']
        data = ['%{}%'.format(stream)]
        archive = db.sp(self.inquire, info, data)

        if len(archive) == 0:
            print("No datastreams found in dsdb database matching {}.".format(stream))

        # Get the first three characters for the site prefix
        for i in archive:
            temp = ''
            for j in range(3):
                temp += i['datastream'][j]

            if i['datastream'].split('.')[-1] == '00':
                value = '{}/{}'.format(temp, i['datastream'])
                proc = i['datastream'].split('.')[0][3:]
                for n in range(len(proc)-1, -1, -1):
                    try:
                        int(proc[n])
                    except:
                        proc = proc[:n]
                        break
                paths.append({'output': value, 'input': value, 'proc': proc})

        return paths

    def get_datastream_paths(self):
        """ Get a list of datastream paths from the database """
        streams = self.config['datastream']
        if type(streams) != list:
            exit("Invalid type of datastreams, expected list")
        paths = []
        for i in streams:
            t = self.get_datastream_path(i)
            for j in t:
                paths.append(j)

        return paths

    def get_instrument_path(self):
        """ Get a list of SIF paths from the database """
        site = self.config['site']
        ins = self.config['instrument']
        fac = self.config['facility']

        if site == None or ins == None or fac == None:
            exit("Input data is not valid")

        db = DSDB(conn_file=self.db_file, alias=self.alias)
        info = ['type', 'proc', 'class', 'level']
        data = [self.type, ins]
        archive_results = db.sp(self.output, info, data)
        stage_results = db.sp(self.input, info, data)

        archive = []
        stage = []

        for i in archive_results:
            if i['level'] == '00':
                archive.append(i)

        for i in stage_results:
            if i['level'] == '00':
                stage.append(i)

        if len(archive) == 0 or len(stage) == 0:
            exit("No data from the database")
        elif len(stage) > 1:
            exit("Multiple inputs detected: Please contact the developer to fix this problem")

        proc = stage[0]['proc']
        stage = '{0}/{0}{1}{2}.{3}'.format(site, stage[0]['class'], fac, stage[0]['level'])
        paths = []
        for i in archive:
            temp = '{0}/{0}{1}{2}.{3}'.format(site, i['class'], fac, i['level'])
            paths.append({'output': temp, 'input': stage, 'proc': proc})

        return paths

    def get_process(self, instrument=None):
        """ Get the process name for a specified instrument """
        ins = None
        if instrument != None:
            ins = instrument
        elif self.config['instrument'] != None:
            ins = self.config['instrument']
        else:
            return False

        db = DSDB(conn_file=self.db_file, alias=self.alias)
        info = ['type', 'proc', 'class', 'level']
        data = [self.type, ins]
        print(self.input)
        print(info)
        print(data)
        tempin = db.sp(self.input, info, data)

        process = None
        for p in tempin:
            if p['level'] == '00':
                process = p['proc']
                break;

        return process

    def alias_exists(self, alias, db_file):
        """ Check to see if the specified alias exists in the specied file """
        fp = open(db_file, 'r')
        contents = fp.read()
        fp.close()

        lines = contents.split('\n')

        for line in lines:
            words = line.split()
            if len(words) == 5 and words[0] != '#':
                (f_alias, host, database, user, password) = words
                if f_alias == alias:
                    break
        else:
            return False

        return True



################################################################################
# Unit tests
################################################################################
import unittest
import test

################################################################################
# Get Data Paths
################################################################################
class TestGetDataPaths(unittest.TestCase):
    def setUp(self):
        self.config = test.config()

    def test_1(self):
        """ Pass datastream """
        self.config['site'] = None
        self.config['instrument'] = None
        self.config['facility'] = None
        self.config['datastream'] = ["sgpmfrsrC1.00"]
        db = DB(self.config)

        result = db.get_data_paths()
        expected = [{'output': 'sgp/sgpmfrsrC1.00', 'input': 'sgp/sgpmfrsrC1.00'}]

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ Pass SIF """
        db = DB(self.config)

        result = db.get_data_paths()
        expected = [{'output': 'sgp/sgpmfrsrC1.00', 'input': 'sgp/sgpmfrsrC1.00'}]

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_3(self):
        """ Pass None """
        self.config['site'] = None
        self.config['instrument'] = None
        self.config['facility'] = None
        db = DB(self.config)

        with self.assertRaises(SystemExit):
            db.get_data_paths()

################################################################################
# Get Datastream Paths
################################################################################
class TestGetDatastreamPaths(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.config['datastream'] = ["sgpmfrsrC1.00", "nsairtC1.00"]

    def test_1(self):
        """ Pass single datastream """
        self.config['datastream'] = self.config['datastream'][:-1]
        db = DB(self.config)
        result = db.get_datastream_paths()
        expected = [{'output': 'sgp/sgpmfrsrC1.00', 'input': 'sgp/sgpmfrsrC1.00'}]

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ Pass multiple streams """
        db = DB(self.config)
        result = db.get_datastream_paths()
        expected = [
            {'output': 'sgp/sgpmfrsrC1.00', 'input': 'sgp/sgpmfrsrC1.00'},
            {'output': 'nsa/nsairtC1.00', 'input': 'nsa/nsairtC1.00'}
        ]

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_3(self):
        """ Pass no streams """
        self.config['datastream'] = []
        db = DB(self.config)
        result = db.get_datastream_paths()
        expected = []

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_4(self):
        """ Pass non list """
        self.config['datastream'] = 'sgpmfrsrC1.00'
        db = DB(self.config)
        with self.assertRaises(SystemExit):
            db.get_datastream_paths()



################################################################################
# Get Datastream Path
################################################################################
class TestGetDatastreamPath(unittest.TestCase):
    def setUp(self):
        self.config = test.config()
        self.db = DB(self.config)

    def test_1(self):
        """ Pass valid stream """
        result = self.db.get_datastream_path('sgpmfrsrC1.00')
        expected = [{'output': 'sgp/sgpmfrsrC1.00', 'input': 'sgp/sgpmfrsrC1.00'}]

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ Pass invalid stream """
        with self.assertRaises(SystemExit):
            self.db.get_datastream_path('asdf.00')

    def test_3(self):
        """ Pass None """
        with self.assertRaises(SystemExit):
            self.db.get_datastream_path(None)


################################################################################
# Get Instrument Path
################################################################################
class TestGetInstrumentPath(unittest.TestCase):
    def setUp(self):
        self.config = test.config()

    def test_1(self):
        """ Pass SIF """
        db = DB(self.config)
        result = db.get_instrument_path()
        expected = [{'output': 'sgp/sgpmfrsrC1.00', 'input': 'sgp/sgpmfrsrC1.00'}]

        print("Result:   {}\nExpected: {}".format(result, expected))
        assert result == expected

    def test_2(self):
        """ Pass IF """
        self.config['site'] = None
        db = DB(self.config)
        with self.assertRaises(SystemExit):
            db.get_instrument_path()

    def test_3(self):
        """ Pass SF """
        self.config['instrument'] = None
        db = DB(self.config)
        with self.assertRaises(SystemExit):
            db.get_instrument_path()

    def test_4(self):
        """ Pass SI """
        self.config['facility'] = None
        db = DB(self.config)
        with self.assertRaises(SystemExit):
            db.get_instrument_path()

    def test_5(self):
        """ Pass None """
        self.config['site'] = None
        self.config['instrument'] = None
        self.config['facility'] = None
        db = DB(self.config)
        with self.assertRaises(SystemExit):
            db.get_instrument_path()



################################################################################
# Pass for now
################################################################################
################################################################################
# Get Process
################################################################################


################################################################################
def main():
    unittest.main(buffer=True)


if __name__ == '__main__':
    main()


