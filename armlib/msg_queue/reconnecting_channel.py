import logging
import time
from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.exceptions import AuthenticationError, ConnectionClosed, ChannelClosed

# pylint: disable=R0903
class ReconnectingChannel(object):

    """Context manager that allows reconnecting after failure."""

    def __init__(self, username, password, hosts, **kwargs):
        """Setup instance variables.

        """
        self.username = username
        self.password = password
        self.hosts = hosts
        self.connection = None
        self.channel = None
        self.additional_args = kwargs
        self.establish_connection()

    def setup(self, **kwargs):
        if 'confirm_delivery' in kwargs:
            self.confirm_delivery()
        if 'prefetch_count' in kwargs:
            prefetch_count = kwargs['prefetch_count']
            self.basic_qos(prefetch_count=prefetch_count)
        if 'exchanges' in kwargs:
            self.setup_exchanges(kwargs['exchanges'])
        if 'queues' in kwargs:
            self.setup_queues(kwargs['queues'])
        if 'bindings' in kwargs:
            self.setup_bindings(kwargs['bindings'])

    def iterate_config(self, conf_dict, func, keyword=''):
        for i in conf_dict:
            config = conf_dict[i]
            if isinstance(keyword, str) and keyword != '':
                value = conf_dict[i][keyword]
                setattr(self, i, value)
            func(**config)

    def setup_exchanges(self, exchange_config):
        self.iterate_config(exchange_config, self.exchange_declare, 'exchange')

    def setup_queues(self, queue_config):
        self.iterate_config(queue_config, self.queue_declare, 'queue')

    def setup_bindings(self, binding_config):
        self.iterate_config(binding_config, self.queue_bind)

    def close_connection(self):
        if self.channel:
            try:
                self.channel.close()
            except ChannelClosed:
                pass
        if self.connection:
            try:
                self.connection.close()
            except ConnectionClosed:
                pass

    def exchange_declare(self, **kwargs):
        self.channel.exchange_declare(**kwargs)

    def queue_declare(self, **kwargs):
        self.channel.queue_declare(**kwargs)

    def queue_bind(self, **kwargs):
        self.channel.queue_bind(**kwargs)

    def basic_qos(self, **kwargs):
        self.channel.basic_qos(**kwargs)

    def consume(self, *args, **kwargs):
        msg = self.channel.consume(*args, **kwargs)
        return msg

    def confirm_delivery(self):
        return self.channel.confirm_delivery()

    def basic_ack(self, **kwargs):
        return self.channel.basic_ack(**kwargs)

    def basic_publish(self, **kwargs):
        return self.channel.basic_publish(**kwargs)

    def basic_nack(self, **kwargs):
        return self.channel.basic_nack(**kwargs)

    def basic_reject(self, **kwargs):
        return self.channel.basic_reject(**kwargs)

    def cancel(self):
        self.channel.cancel()

    def connect(self, host):
        pika_credentials = PlainCredentials(self.username,
                                            self.password)
        pika_dsn = ConnectionParameters(host, credentials=pika_credentials)
        self.connection = BlockingConnection(pika_dsn)
        self.channel = self.connection.channel()

    def establish_connection(self):
        """Loop through available hosts until a connection succeeds."""
        logger = logging.getLogger('root.rabbit_connector')

        while self.connection is None or \
                self.connection.is_closed or \
                self.connection.is_closing:
            logger.info('Attempting to reconnect to Rabbitmq')
            for host in self.hosts:
                try:
                    self.connect(host)
                    logger.info('Connected to %s', host)
                    self.setup(**self.additional_args)
                    return
                # pylint: disable=W0703
                except (AuthenticationError, ConnectionClosed):
                    err_msg = 'Failed to connect to %s with: username=%s password=%s'
                    logger.debug(err_msg, host, self.username, self.password)
                    continue
            time.sleep(10)