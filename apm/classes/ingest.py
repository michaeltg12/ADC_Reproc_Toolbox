import os
import re
import sys
import glob
import json

from subprocess import CalledProcessError
from subprocess import Popen
from subprocess import PIPE
from subprocess import check_output

#from apm.classes.files import Files
binpath = '/apps/process/bin'

class Ingest:
    def __init__(self, config=None):
        self.ingest = None
        e = os.getenv('REPROC_HOME')
        #Checks if the environment variables have been set and sets if not
        if 'test_var' not in os.environ:
            ps = Popen('source env.sh', shell = True)
            out, err = ps.communicate()
            returncode = ps.returncode

        if config != None:
            config_path = '{}/post_processing/{}'.format(e, config['job'])
            #f = os.path.join(e, 'DQR#')
            #info = json.load(open(f))
            self.instrument = config['instrument']
            self.site = config['site']
            self.facility = config['facility']
            self.command = None
            self.job = config['job']
            self.path = os.path.join(config['stage'], 'post_processing', config['job'])
        else:
            self.instrument = None
            self.site = None
            self.facility = None
            self.command = None
            self.job = None
            self.path = e

    """
    Will eventually change this to parse the Graph ML tree for the dependencies
    """
    def find_ingest(self):
        ingest_path = binpath
        os.chdir(ingest_path)

        ingest = glob.glob('*_ingest')

        for i in ingest:
            try:
                ps = Popen([i, '-h'], stdout=PIPE, stderr=PIPE)
                out, err = ps.communicate()
                returncode = ps.returncode
                help_text = str(out)

            except CalledProcessError as e:
                err.close()
                out.close()
                help_text = None
                status = e.returncode
            
            help_text = re.split(r'VALID PROCESS NAMES', help_text)

            if len(help_text) > 1:
                names = (help_text[1].strip()).split('\n')
                if self.instrument in names:
                    self.ingest = os.path.join(binpath, i)
                    return True
            else:
                name = i.split('_')
                if self.instrument in name:
                    self.ingest = os.path.join(binpath, i)
                    return False
        return None

    def run(self):
        multiple = self.find_ingest()
        if multiple == None:
            print("No ingest found for {} instrument".format(self.instrument))
            return 

        self.command = '{} '.format(self.ingest)
        if multiple:
            self.command += '-n {} '.format(self.instrument)
        self.command += '-s {} -f {} -R'.format(self.site, self.facility)
        
        #Add stuff for optional flags like begin and end dates if needed
        if os.path.exists(self.path):
            os.chdir(self.path)
            #TODO Remove -h so the ingest runs correctly
            ps = Popen('{} -h'.format(self.command), shell=True)
            out, err = ps.communicate()
        
            if ps.returncode != 0:
                print("The ingest quit before completing. Please check files and output.")
        else:
            print("{} path does not exist".format(self.path))
""" 
Main for testing in an interactive mode. Doesn't run ingest just calls the correct ingest help to make sure
all the options were added correctly
"""
def main():
    if len(sys.argv) < 1: return 
    ing = Ingest()
    if sys.argv[1] == '-I':
        ing.instrument = input("Instrument: ")
        ing.ingest = input("Please enter the ingest name: ")
        ing.facility = input("Enter facility: ")
        ing.site = input("Enter site: ")
        ing.job = input("Enter DQR: ") 
        ing.path = os.path.join(ing.path, 'post_processing', ing.job)
        ing.run()

if __name__ == '__main__':
    main()
