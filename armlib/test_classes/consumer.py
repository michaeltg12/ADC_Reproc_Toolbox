

from .reconnecting_channel import MockReconnectingChannel

class MockConsumer(object):
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
        self.reconnecting_channel = MockReconnectingChannel(**self.rabbit_config)


    def end_gracefully(self):
        NotImplementedError()

    def start_listening(self, queue_name):
        for message in self.reconnecting_channel.consume(queue_name, inactivity_timeout=60.):
            # if there has been no activity for a minute. close the tar.
            if message is None:
                self.do_housekeeping()
            else:
                self.handle_message(*message)

    def do_housekeeping(self):
        raise NotImplementedError()

    def handle_message(self, method, _, body):
        raise NotImplementedError()

