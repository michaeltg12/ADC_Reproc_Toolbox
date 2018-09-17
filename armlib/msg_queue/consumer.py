import logging
import signal
from pika.exceptions import ConnectionClosed
from .reconnecting_channel import ReconnectingChannel
from armlib.services import ThreadedSignalHandler


class Consumer(ThreadedSignalHandler):
    """The each instance of archiver pulls from a rabbitMQ queue and handles the message.
    Attributes:

        writer (:obj: Writer): an object that implements the writer interface
        tape_type (str): this specifies whether the writer is onsite or offsite
        rabbit_config (dict): this specifies username, password, and host of the rabbitMQ queue
    """

    def __init__(self, rabbit_config):
        """Initialize the service.

        Attributes:
            writer (:obj: Writer): an object that implements the writer interface
        """
        self.rabbit_config = rabbit_config
        self.reconnecting_channel = ReconnectingChannel(**self.rabbit_config)
        self.terminate = False
        super(Consumer, self).__init__()

    def establish_rabbit_connection(self):
        self.reconnecting_channel.establish_connection()

    def end_gracefully(self, signum, _):
        self.logger.debug('Daemon is terminating.')
        self.terminate = True
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def start_listening(self, queue_name, inactivity_timeout):
        while 1:
            try:
                generator = self.reconnecting_channel.consume(queue_name, inactivity_timeout=inactivity_timeout)
                for message in generator:
                    # if there has been no activity. do housekeeping.
                    if self.terminate:
                        self.reconnecting_channel.cancel()
                        return
                    elif message is None:
                        self.do_housekeeping()
                    else:
                        self.handle_message(*message)
            except ConnectionClosed:
                logger = logging.getLogger('root.consumer')
                logger.error('Lost connection to Rabbitmq.')
                self.establish_rabbit_connection()

    def do_housekeeping(self):
        raise NotImplementedError()

    def handle_message(self, method, _, body):
        raise NotImplementedError()

    def run(self):
        raise NotImplementedError()
