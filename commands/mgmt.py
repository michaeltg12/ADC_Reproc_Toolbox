import click
import json
import subprocess
from pathlib import Path, PurePath
from support.setup_support import *
from support.validators import *
from config.config import *

@click.command(help='Initialize a job and post processing directories.')
@click.option('--job', '-j', help='DQR# as job name.')
@click.pass_context
def setup(ctx, *args, **kwargs):
    # validator was made to work with click.options param=None b/c not used
    path_args = {
        'reproc_home' : ctx.obj.reproc_home,
        'job': validate_dqr(ctx, None, kwargs['job']),
        'post_proc' : ctx.obj.post_proc
    }
    ctx.obj.reproc_logger.debug('Path Arguments: {}'.format(path_args))
    # the above settings will get substituted into each element of the array imported from config.config
    for path in init_paths:
        dir_path = Path(path.format(**path_args))
        if not Path.is_dir(dir_path):
            ctx.obj.reproc_logger.debug('\ncreating directory: {}'.format(dir_path))
            Path.mkdir(dir_path)
    for filename in data_files:
        filepath = PurePath(path_args['reproc_home']).joinpath(path_args['job'], filename)
        with open(filepath, 'w') as env_file:
            contents = data_files[filename].format(**path_args)
            ctx.obj.reproc_logger.debug('\n\twriting config file: {}\n{}\n'.format(filename, contents))
            env_file.writelines(contents)
    updated_job_conf = update_job_conf(default_job_conf, command='init', **kwargs)
    conf_filepath = PurePath(path_args['reproc_home']).joinpath(path_args['job'], f'{path_args["job"]}.conf')
    with open(conf_filepath,'w') as open_config_file:
        ctx.obj.reproc_logger.debug('\n\twriting default conf file: {}\n{}\n'.
                                    format(conf_filepath.name, updated_job_conf))
        json.dump(updated_job_conf, open_config_file)

@click.command(help='Update tracked files, configs, and logs for given job')
@click.option('--job', '-j', help='DQR# as job name.')
@click.pass_context
def Update(ctx, *args, **kwargs):
    ctx.obj.reproc_logger.debug('Update module not yet implemented.')