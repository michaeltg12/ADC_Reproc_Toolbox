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
        return ['  > _adi_acecor', '  > _aerinf', '  > _aeriprof', '  > _aerosolbe', '  > _aip', '  > _aod', '  > _aosaopclap', '  > _aosaoppsap', '  > _aosccnavg', '  > _baebbr', '  > _beflux', '  > _ccnprof', '  > _cldtype', '  > _dlprof_wind', '  > _dlprof_wstats', '  > _griddedsonde', '  > _gvrpwv', '  > _interpolatedsonde', '  > _kazrarscl', '  > _kazrcor', '  > _kazrcorc0', '  > _langley', '  > _lssonde', '  > _masc_flake_anal', '  > _mergesonde', '  > _mergesonde2', '  > _mfrsrcip', '  > _mfrsrcldod', '  > _microbase', '  > _mplcmask', '  > _mwrret', '  > _ndrop', '  > _pblhtsonde', '  > _pblhtsondeyr', '  > _qcrad_beflux', '  > _qcrad_brs', '  > _qcrad_sirs', '  > _radflux', '  > _radflux2', '  > _rlprof2_merge', '  > _rlprof2_mr', '  > _rlprof2_temp', '  > _rlprof_asr0', '  > _rlprof_asr1', '  > _rlprof_asr10', '  > _rlprof_asr10_diff', '  > _rlprof_be10', '  > _rlprof_calib', '  > _rlprof_dep10', '  > _rlprof_dep2', '  > _rlprof_ext1', '  > _rlprof_ext10', '  > _rlprof_ext10_diff', '  > _rlprof_fex', '  > _rlprof_merge', '  > _rlprof_mr0', '  > _rlprof_mr10', '  > _rlprof_temp10', '  > _rlprof_temp60', '  > _sacradvvad', '  > _sashe_aod', '  > _sashe_langley', '  > _sfccldgrid', '  > _shallowcumulus', '  > _sondeadjust', '  > _summaryqc', '  > _surfspecalb', '  > _swfluxanal', '  > _twrmr']
        command = ['/apps/ds/bin/vapmgr', '-i']
        try:
            vaps = subprocess.check_output(command).decode('utf-8')
        except subprocess.CalledProcessError as e:
            vaps = ""

        vaps = vaps.split('\n')
        vaplist = []
        for k,v in enumerate(vaps):
            if len(v) > 2 and v[2] == '>':
                vaplist.append(v)

        return vaplist


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


