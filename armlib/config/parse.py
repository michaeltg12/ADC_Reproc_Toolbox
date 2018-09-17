
def __parse(config, key, defaultv, defaultt):
    if key in config:
        config[key] = \
            defaultt(config[key])
    else:
        config[key] = defaultv

def parse_int(config, key, defaultv):
    __parse(config, key, defaultv, int)

def parse_float(config, key, defaultv):
    __parse(config, key, defaultv, float)

def parse_bool(config, key, defaultv):
    if key in config:
        if config[key] == 'False':
            config[key] = False
        elif config[key] == 'True':
            config[key] = True
        else:
            raise ValueError('Invalid value for confirm_devliery.')
    else:
        config[key] = defaultv



'''
=================================
SAMPLE
=================================
[postgres]
connection_string: dbname=x user=x password=x application_name=x host=x
retry_delay: 1
retry_backoff: 2
max_retry_delay: 60
'''

def parse_db_config(config):
    if 'postgres' not in config:
        return
    postgres_config = config['postgres']
    parse_int(postgres_config, 'retry_delay', 1)
    parse_int(postgres_config, 'retry_backoff', 2)
    parse_int(postgres_config, 'max_retry_delay', 120)

'''
=================================
SAMPLE
=================================
[rabbitmq]
username: x
password: x
port: 5672
vhost: /
hosts: s2, s1
prefetch_count: 1
confirm_delivery: True
'''

def parse_rabbit_config(config):
    if 'rabbitmq' not in config:
        return
    rabbit_config = config['rabbitmq']
    rabbit_config['hosts'] = \
        rabbit_config['hosts'] \
            .replace(' ', '') \
            .split(',')

    parse_int(rabbit_config, 'port', 5672)
    parse_int(rabbit_config, 'prefetch_count', 1)
    parse_bool(rabbit_config, 'confirm_delivery', True)
    if 'exchanges' in config:
        del config['exchanges']['names']
        parse_durable(config['exchanges'])
        rabbit_config['exchanges'] = config['exchanges']
        del config['exchanges']
    if 'queues' in config:
        del config['queues']['names']
        parse_durable(config['queues'])
        rabbit_config['queues'] = config['queues']
        del config['queues']
    if 'bindings' in config:
        del config['bindings']['names']
        rabbit_config['bindings'] = config['bindings']
        del config['bindings']

def parse_durable(config):
    for i in config:
        parse_bool(config[i],'durable',False)
