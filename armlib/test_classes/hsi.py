from armlib.shell import AbnormalExecutionError
import os
import argparse


class FakeHpss(object):
    FAKE_FILES_ON_HPSS = []


def parse_cmd(args):
    parser = argparse.ArgumentParser(prog='hsi')
    parser.add_argument('-k', nargs=1, help='keytab')
    parser.add_argument('-l', nargs=1, help='user')
    parser.add_argument('-A', nargs=1, help='auth method')
    parser.add_argument('cmd', nargs='+')
    args = parser.parse_args(args)
    assert args.k[0] is not None
    assert args.A[0] == 'keytab'
    assert args.l[0] == 'arm'
    return args


class HSI(object):
    CMD = ''

    @staticmethod
    def fake_execute(args):
        if isinstance(args, list):
            args = parse_cmd(args[1:])
        cmd = args.cmd
        op = cmd[0]
        if op == 'ls':
            HSI.fake_execute_hsi_ls(args)
        elif op == 'cput':
            HSI.fake_execute_cput(args)
        elif op == 'get':
            HSI.fake_execute_get(args)
        elif op == 'in':
            HSI.fake_execute_in(args)
        elif op =='chmod':
            HSI.fake_execute_hsi_chmod(args)
        else:
            raise Exception('unknown command %s' % (op))

    @staticmethod
    def fake_execute_in(args):
        cmd = args.cmd
        filename = cmd[-1]
        assert os.path.exists(filename)
        with open(filename,'r') as f:
            for i in f.readlines():
                i = i.strip()
                hsi_call = i.split(' ')
                if hsi_call[0] == 'cput':
                    op, flag, local, colon, hpss = hsi_call
                    hsi_cmd = [op, flag, local, colon, hpss]
                elif hsi_call[0] == 'get':
                    op, local, colon, hpss = hsi_call
                    hsi_cmd = [op, local, colon, hpss]
                assert op == HSI.CMD
                assert colon == ':'
                args.cmd = hsi_cmd
                try:
                    HSI.fake_execute(args)
                except:
                    pass
        returncode = 0
        raise AbnormalExecutionError(returncode, hpss, hpss)


    @staticmethod
    def fake_execute_get(args):
        cmd = args.cmd
        hpss_path = cmd[-1]
        local_path = cmd[-3]
        if hpss_path in FakeHpss.FAKE_FILES_ON_HPSS:
            returncode = 0
        else:
            returncode = 1
        raise AbnormalExecutionError(returncode, hpss_path, hpss_path)

    @staticmethod
    def fake_execute_cput(args):
        cmd = args.cmd
        hpss_path = cmd[-1]
        local_path = cmd[-3]
        FakeHpss.FAKE_FILES_ON_HPSS.append(hpss_path)
        returncode = 0
        raise AbnormalExecutionError(returncode, hpss_path, hpss_path)

    @staticmethod
    def fake_execute_hsi_ls(args):
        cmd = args.cmd
        hpss_path = cmd[-1]
        if hpss_path in FakeHpss.FAKE_FILES_ON_HPSS:
            filename = os.path.basename(hpss_path)
            err_msg = '%s\n'%(filename)
            returncode = 0
        else:
            err_msg = '%s\n'%(hpss_path)
            returncode = 64
        raise AbnormalExecutionError(returncode, err_msg, err_msg)

    @staticmethod
    def fake_execute_hsi_chmod(args):
        cmd = args.cmd
        hpss_path = cmd[-1]
        if hpss_path in FakeHpss.FAKE_FILES_ON_HPSS:
            filename = os.path.basename(hpss_path)
            err_msg = '%s\n'%(filename)
            returncode = 0
        else:
            err_msg = '%s\n'%(hpss_path)
            returncode = 64
        raise AbnormalExecutionError(returncode, err_msg, err_msg)

