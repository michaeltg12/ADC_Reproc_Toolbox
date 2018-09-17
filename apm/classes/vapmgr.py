#!/apps/base/python3/bin/python3

import os
import stat
import subprocess

from apm.classes.db import DB

from apm.classes.system import get_shell
from apm.classes.system import dir_pattern

class VapMgr:
    """ Interface with vapmgr command """

    def __init__(self, config):
        """ Initialize with config """
        if config == {}:
            return
        self.config = config
        self.path = dir_pattern().format(self.config['stage'], self.config['job'])
        self.job = dir_pattern().format(self.path, 'collection')
        self.ops = dir_pattern(3).format(self.path, 'conf', 'ops')
        self.file = dir_pattern().format(self.ops, 'vapmgr.conf')
        if not os.path.exists(self.ops):
            os.mkdir(self.ops)

        if not os.path.exists(self.file):
            os.symlink('/data/conf/ops/vapmgr.conf', self.file)

    def vap_info(self):
        command = '/apps/ds/bin/vapmgr -i'
        try:
            vaps = subprocess.check_output(command)
        except subprocess.CalledProcessError as e:
            vaps = ""

        vaps = vaps.split('\n')
        vaplist = []
        for k,v in enumerate(vaps):
            if len(v) > 2 and v[2] == '>':
                vaplist.append(v)

        print('')
        print('Available VAPs')
        print('')

        for v in vaplist:
            print(v)

        print('')
        return


    def add_to_env(self):
        ext = get_shell()
        if ext == 'bash':
            ext = 'sh'
        elif ext == 'csh':
            ext = 'csh'
        else:
            exit("Unable to determine shell. Please run again from Bash or CSH shell.")

        db = DB(self.config)
        site = self.config['site']
        ins = self.config['instrument']
        fac = self.config['facility']
        output = '\n'

        # Construct the vapmgr command
        output += "/apps/ds/bin/vapmgr -setup "
        if site:
            output += "-r {}".format(site)
            if fac:
                output += ".{} ".format(fac)
        output += ins

        env = 'env.{}'.format(ext)
        env = dir_pattern().format(self.path, env)
        fp = open(env, 'a')
        fp.write(output)
        fp.close()

        return

################################################################################
# Unit tests
################################################################################


################################################################################

def main():
    pass

if __name__ == '__main__':
    main()


