# tributary dependencies
from tributary.core import Message, Engine
from tributary.streams import StreamElement, StreamProducer

# amqp dependencies
from haigha.connection import Connection
from haigha.message import Message as AMQPMessage

# other imports
import json

# exports
__all__ = ['AMQPMessenger', 'AMQPListener', 'JSONListener', 'StringListener']


class AMQPMessenger(AMQPMessenger):
    """docstring for AMQPMessenger"""
    def __init__(self, name, user, password, vhost, host, exchange, queue,
                 routing_key, exchange_type='topic', auto_delete=False,
                 heartbeat=None, debug=False):
        super(AMQPMessenger, self).__init__(name)

        # amqp connection details
        self.user = user
        self.password = password
        self.vhost = vhost
        self.host = host
        self.exchange = exchange
        self.queue = queue
        self.routing_key = routing_key
        self.heartbeat = heartbeat
        self.debug = debug

        # options
        self.exchange_type = exchange_type
        self.auto_delete = auto_delete

    def preProcess(self, msg):
        """Called when the actor starts"""

        # open connection
        self.conn = Connection(
            user=self.user, password=self.password,
            vhost=self.vhost, host=self.host,
            heartbeat=self.heartbeat, debug=self.debug)

        # create AMQP channel
        self.channel = self.conn.channel()
        self.channel.exchange.declare(self.exchange, self.exchange_type)
        self.channel.queue.declare(self.queue, self.auto_delete)
        self.channel.queue.bind(self.queue, self.exchange, self.routing_key)

    def postProcess(self, message):
        """Called when actor is stopped or killed"""
        self.conn.close()

    def process(self, message):
        """Publish message to queue"""
        if self.debug:
            self.log("Publishing: " + str(message.data))
        self.channel.basic.publish(
            AMQPMessage(str(message.data)),
            self.exchange, self.routing_key)


class AMQPListener(StreamProducer):
    """docstring for AMQPListener"""
    def __init__(self, name, user, password, vhost, host, exchange, queue,
                 routing_key, exchange_type='topic', auto_delete=False,
                 heartbeat=None, debug=False):
        super(AMQPListener, self).__init__(name)

        # amqp connection details
        self.user = user
        self.password = password
        self.vhost = vhost
        self.host = host
        self.exchange = exchange
        self.queue = queue
        self.routing_key = routing_key
        self.heartbeat = heartbeat
        self.debug = debug

        # options
        self.exchange_type = exchange_type
        self.auto_delete = auto_delete

    def preProcess(self, msg):
        """Called when the actor starts"""

        # open connection
        self.conn = Connection(
            user=self.user, password=self.password,
            vhost=self.vhost, host=self.host,
            heartbeat=self.heartbeat, debug=self.debug)

        # create AMQP channel
        self.channel = self.conn.channel()
        self.channel.exchange.declare(self.exchange, self.exchange_type)
        self.channel.queue.declare(self.queue, self.auto_delete)
        self.channel.queue.bind(self.queue, self.exchange, self.routing_key)

    def postProcess(self, message):
        """Called when actor is stopped or killed"""
        self.conn.close()

    def parse(self, data):
        """Parses data received"""
        raise NotImplementedError("parse")

    def process(self, message=None):
        """Recieves message from queue"""

        while self.running:
            message = self.channel.basic.get(self.queue)
            if message:
                content = message.body

                # log message
                if self.debug:
                    self.log("Recieved: " + str(content))

                # send to child nodes
                self.scatter(Message(**self.parse(content)))
            else:
                # yield to other greenlet
                self.tick()


class JSONListener(AMQPListener):
    """docstring for JSONListener"""
    def parse(self, data):
        msg = json.loads(str(data))
        if type(msg) == dict:
            return msg
        else:
            return {'body': msg}


class StringListener(AMQPListener):
    """docstring for StringListener"""
    def parse(self, data):
        return {'body': data}
