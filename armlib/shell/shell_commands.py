from subprocess import PIPE, Popen
import os
import string
import datetime
import signal


def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class HpssError(Exception):
    pass


class AbnormalExecutionError(Exception):
    def __init__(self, returncode, stderr, *args, **kwargs):
        self.returncode = returncode
        self.stderr = stderr
        super(AbnormalExecutionError, self).__init__(*args, **kwargs)


def execute_shell(args, stdin_content=None):
    """Execute a shell command and raise a Python error on exit codes >0."""
    kwargs = {'stdout':PIPE,
              'stderr':PIPE,
              'universal_newlines':True,
              'preexec_fn':preexec_function}
    if isinstance(stdin_content, str):
        kwargs['stdin'] = PIPE
    process = Popen(args, **kwargs)
    if isinstance(stdin_content, str):
        output, errors = process.communicate(input=stdin_content)
        returncode = process.poll()
    else:
        output, errors = process.communicate()
        returncode = process.poll()
    if errors:
        err_msg = 'Error executing %s: %s' % (args, errors)
        raise AbnormalExecutionError(returncode, errors, err_msg)
    return output


def get_md5sum(fullpath):
    if os.path.isfile(fullpath):
        cmd = ['md5sum', fullpath]
        output = execute_shell(cmd)
        output = output.split(" ")[0]
        output = output.strip()
        return output
    raise FileNotFoundError('Invalid path %s'%(fullpath))


def call_hsi_ls(hpss_destination, keytab):
    cmd = '/usr/local/bin/hsi -A keytab -k %s -l arm ls %s'
    cmd = cmd %(keytab, hpss_destination)
    cmd = cmd.split(" ")
    try:
        execute_shell(cmd)
    except AbnormalExecutionError as aee:
        if aee.returncode == 0:
            return True
        elif aee.returncode == 64:
            return False
        else:
            raise aee

def call_hsi_chmod(hpss_destination, keytab):
    cmd = '/usr/local/bin/hsi -A keytab -k %s -l arm chmod ugo+r %s'
    cmd = cmd %(keytab, hpss_destination)
    cmd = cmd.split(" ")
    try:
        execute_shell(cmd)
    except AbnormalExecutionError as aee:
        if aee.returncode == 0:
            return True
        else:
            raise aee


def get_temp_file():
    linux_time = datetime.datetime.now().strftime('%s')
    temp_filename = '/tmp/%s.%s.hpss.in'%(os.getpid(), linux_time)
    return temp_filename


def is_valid_filename(filename):
    for eachChr in filename:
        if eachChr in string.whitespace:
            return False
    return True


def call_hsi_cput(files, keytab):
    _call_hsi_in(files, 'cput -P', keytab)

def call_hsi_get(files, keytab):
    _call_hsi_in(files, 'get', keytab)

def _call_hsi_in(files, cmd, keytab):
    if cmd != 'get' and cmd !='cput -P':
        raise HpssError('Invalid Command %s'%(cmd))
    if not is_valid_filename(keytab):
        raise HpssError('Keytab file is invalid: %s'%(keytab))
    hpss_command_filename = get_temp_file()
    while os.path.exists(hpss_command_filename):
        hpss_command_filename = get_temp_file()

    with open(hpss_command_filename, 'w') as f:
        os.chmod(hpss_command_filename, mode=0o600)
        for local_file in files:
            if not is_valid_filename(local_file):
                raise HpssError('Invalid Filename %s' % (local_file))
            hpss_destination = get_hpss_path(local_file)
            hpss_cmd = '%s %s : %s\n'%(cmd, local_file, hpss_destination)
            f.write(hpss_cmd)

    cmd = '/usr/local/bin/hsi -A keytab -k %s -l arm in %s'
    cmd = cmd %(keytab, hpss_command_filename)
    cmd = cmd.split(" ")
    try: execute_shell(cmd)
    except AbnormalExecutionError as aee:
        if aee.returncode != 0:
            raise HpssError('[ERROR %s] %s', str(aee.returncode), aee.stderr)
    os.remove(hpss_command_filename)


def get_hpss_path(versioned_filename):
    # all files will have well formed names.
    # This is ensured by verification process
    file_base = os.path.basename(versioned_filename)
    split_filename = file_base.split('.')
    datastream = '%s.%s'%(split_filename[0], split_filename[1])
    version = split_filename[-1]
    if version[0] != 'v':
        raise AssertionError('Invalid Versioned Filename %s'%(versioned_filename))
    #file extenstion must be at index 4 by ARM naming standards
    file_extension = split_filename[4]
    # /u2/arm/<file extension>/<datastream>/<filename>
    hpss_path = os.path.join('/f1/arm', file_extension, datastream, file_base)
    return hpss_path