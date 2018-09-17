
from armlib.msg_queue import ReconnectingChannel

class MockMethod(object):
    def __init__(self, ack_callback, unack_callback, message):
        self.delivery_tag = (ack_callback, unack_callback, message)


class MockMessageQueue(object):
    def __init__(self, name):
        self.name = name
        self.messages = []
        self.acked = []
        self.unacked = []
    def __str__(self):
        my_string= 'Messages:\n\t' + "\n\t".join([str(i) for i in self.messages])
        return my_string
    def add(self, message):
        message = (MockMethod(self.ack_message, self.unack_message, message), "", message)
        self.messages.append(message)

    def ack_message(self, message):
        self.acked.append(message)
        self.unacked.remove(message)

    def unack_message(self, message):
        self.unacked.remove(message)
        message = (MockMethod(self.ack_message, self.unack_message, message), "", message)
        self.messages.append(message)


    def consume(self):
        if len(self.messages) == 0:
            return None
        msg = self.messages.pop()
        self.unacked.append(msg[-1])
        return msg

class MockExchange(object):
    def __init__(self, name, type):
        self.name = name
        self.type = type
        self.listeners = {}
    def __str__(self):
        return "Name:%s Type:%s"%(self.name, self.type)
    def bind(self, queue, routing_key):
        try:
            self.listeners[routing_key].append(queue)
        except:
            self.listeners[routing_key] = [queue]
    def publish(self, message, routing_key):
        for l in self.listeners[routing_key]:
            l.add(message)


class MockReconnectingChannel(ReconnectingChannel):

    """Context manager that allows reconnecting after failure."""

    def __init__(self, *args, **kwargs):
        """Setup instance variables.

        """
        self.confirm = False
        self.exchanges = {}
        self.queues = {}
        self.setup(**kwargs)

    def close_connection(self):
        pass

    def exchange_declare(self, **kwargs):
        exchange = kwargs['exchange']
        etype = kwargs['type']
        self.exchanges[exchange] = MockExchange(exchange, etype)
        print("Declaring Exchange %s" % (str(self.exchanges[exchange])))

    def queue_declare(self, **kwargs):
        queue = kwargs['queue']
        self.queues[queue] = MockMessageQueue(queue)

    def queue_bind(self, **kwargs):
        exchange = kwargs['exchange']
        queue = kwargs['queue']
        try:
            routing_key = kwargs['routing_key']
        except:
            routing_key = ''
        self.exchanges[exchange].bind(self.queues[queue],routing_key)

    def basic_qos(self, **kwargs):
        pass

    def consume(self, *args, **kwargs):
        msg = self.queues[args[0]].consume()
        return msg

    def confirm_delivery(self):
        self.confirm = True

    def basic_ack(self, **kwargs):
        ack, trash, message = kwargs['delivery_tag']
        ack(message)

    def basic_publish(self, **kwargs):
        exchange = kwargs['exchange']
        routing_key = kwargs['routing_key']
        message = kwargs['body']
        if not isinstance(message, bytes):
            message = bytes(message,'UTF-8')
        self.exchanges[exchange].publish(message, routing_key)
        if self.confirm:
            return True

    def basic_nack(self, **kwargs):
        trash, nack, message = kwargs['delivery_tag']
        nack(message)

    def basic_reject(self, **kwargs):
        trash, nack, message = kwargs['delivery_tag']
        nack(message)

    def cancel(self):
        pass
