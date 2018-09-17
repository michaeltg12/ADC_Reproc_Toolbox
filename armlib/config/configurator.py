import socket
import logging.config
try:
    import configparser
except ImportError:
    import ConfigParser as configparser
from .parse import parse_db_config, parse_rabbit_config
class ConfigFileNotFound(Exception):
    pass

def setup_logger(logging_config):
    logging.config.dictConfig(logging_config)

def nest_on_names_attr(config):
    nested_config ={}
    stitched_sections = []
    for eachKey in config:
        if 'names' in config[eachKey]:
            names = config[eachKey]['names']
            names = names.replace(' ','')
            names = names.split(',')
            nested_config[eachKey] = config[eachKey]
            for eachName in names:
                nested_config[eachKey][eachName] = config[eachName]
                stitched_sections.append(eachName)
            stitched_sections.append(eachKey)
    for eachKey in config:
        if eachKey in stitched_sections:
            pass
        else:
            nested_config[eachKey] = config[eachKey]
    return nested_config

def merge_logging(config):
    for eachKey in config['logging']:
        if eachKey == 'logfile':
            logging_config['handlers']['logfile']['filename'] = \
                config['logging'][eachKey]
        elif eachKey == 'level':
            logging_config['handlers']['console']['level'] = config['logging'][eachKey]
            logging_config['handlers']['logfile']['level'] = config['logging'][eachKey]
        elif eachKey == 'recipients':
            send_to = config['logging'][eachKey].split(',')
            send_to = [i.strip() for i in send_to]
            if len(send_to) > 0:
                logging_config['handlers']['email']['toaddrs'] = send_to
                logging_config['loggers']['root']['handlers'].append('email')
    config['logging'] = logging_config


def configurator(config_filename, nested=False):
    config = configparser.ConfigParser()
    config.read(config_filename)
    retval = dict(config)
    for i in config.sections():
        retval[i] = dict(config[i])

    if 'logging' in retval:
        merge_logging(retval)
    else:
        retval['logging'] = logging_config

    if nested:
        return nest_on_names_attr(retval)
    return retval


def load_config(config_filename, nested=True):
    logger = logging.getLogger('root.load_config')
    try:
        config = configurator(config_filename, nested)
        parse_rabbit_config(config)
        parse_db_config(config)
        setup_logger(config['logging'])
        return config
    except (KeyError, FileNotFoundError, ValueError) as err:
        logger.critical('Unable to load config file.')
        logger.critical(str(err))
    exit(1)


logging_config = {'version': 1,
                  'loggers':{'root':
                                 {'level':'INFO',
                                  'handlers': ['logfile', 'console']
                                  }
                             },
                  'formatters': {'standard':
                                     {'format':'%(name)s %(asctime)s %(levelname)s %(message)s',
                                      'datefmt':'%m/%d/%Y %H:%M:%S'}
                                 },
                  'handlers': {'logfile':
                                   {'class': 'logging.handlers.RotatingFileHandler',
                                    'filename': '/data/project/0021718_1509993009/post_processing/reproc_management.log',
                                    'formatter': 'standard',
                                    'maxBytes': 10485760,
                                    'backupCount':10,
                                    'level': 'INFO'
                                    },
                               'console':
                                   {'class': 'logging.StreamHandler',
                                    'stream': 'ext://sys.stdout',
                                    'formatter': 'standard',
                                    'level': 'DEBUG'
                                    },
                               'email':
                                   {'class':'logging.handlers.SMTPHandler',
                                    'mailhost':('localhost', 25),
                                    'fromaddr':socket.gethostname(),
                                    'toaddrs':['giansiracumt@ornl.gov'],
                                    'subject':'Houston, we have a problem.',
                                    'level':'CRITICAL'
                                    }
                               }
                  }



