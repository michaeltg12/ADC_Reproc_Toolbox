import click
import json
from config.config import *
from datetime import datetime as dt
from pathlib import Path, PurePath

def load_job_config(*args, **kwargs):
    ctx = kwargs['context']
    job_config = PurePath(ctx.obj.reproc_home).joinpath(ctx.params['job'], f'{ctx.params["job"]}.conf')
    return json.load(open(job_config))

def update_job_conf(*args, **kwargs):
    for key in kwargs:
        default_job_conf[key]=kwargs[key]
    return default_job_conf