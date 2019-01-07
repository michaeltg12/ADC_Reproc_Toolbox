import click
from datetime import datetime as dt
from config.config import dqr_regex, datastream_regex
from logging import getLogger
reproc_logger = getLogger('reproc_logger')