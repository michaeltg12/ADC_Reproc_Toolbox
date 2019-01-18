import click
import json
from pathlib import Path
from support.validators import *
from config.config import *

@click.command(help='Initialize a job directory hierarchy and post processing directory.')
@click.option('--job', '-j', help='DQR# as job name.')
@click.pass_context
def init(ctx, *args, **kwargs):
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
    conf_filepath = PurePath(path_args['reproc_home']).joinpath(path_args['job'], f'{path_args["job"]}.conf')
    with open(conf_filepath,'w') as conf_file:
        ctx.obj.reproc_logger.debug('\n\twriting default conf file: {}\n{}\n'.
                                    format(conf_filepath.name, default_job_conf))
        json.dump(default_job_conf, conf_file)



