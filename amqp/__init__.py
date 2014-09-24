# tributary dependencies
from tributary.core import Message, Engine
from tributary.streams import StreamElement, StreamProducer
from tributary import log_exception

# amqp dependencies
from haigha.connection import Connection
from haigha.message import Message as AMQPMessage

# other imports
from gevent.queue import Empty
import json
import datetime

# exports
__all__ = ['AMQPMessenger', 'AMQPListener', 'JSONListener',
           'StringListener', 'BatchAMQPMessenger']


class AMQPMessenger(StreamElement):
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
                # self.tick()
                self.sleep(1)


class BatchAMQPMessenger(StreamElement):
    """docstring for AMQPMessenger"""
    def __init__(self, name, user, password, vhost, host, exchange, queue,
                 routing_key, exchange_type='topic', auto_delete=False,
                 heartbeat=None, debug=False, sendfreq=1, sendlimit=1000):
        super(BatchAMQPMessenger, self).__init__(name)
        self.sendfreq = sendfreq
        self.sendlimit = sendlimit
        if self.sendfreq < 0:
            self.sendfreq = 1
        if self.sendlimit < 0:
            self.sendlimit = 0

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

    def handle_exception(self, exc):
        log_exception(self.name, "Error: ")

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

    def flush(self, message=None):
        """Empties the queue"""
        self.log("Flushing Queue... (%s)" % (self.inbox.qsize()))
        if not self.inbox.empty():
            self.log("Flushing Queue...")
            for message in self.inbox:
                self.handle(message)

        if self.debug:
            self.log("Inbox Size: " + str(self.inbox.qsize()))
        self.running = False

    def execute(self):
        """Handles the data flow for streams"""
        self.running = True
        last_timestamp = datetime.datetime.now()

        self.log("Starting...")
        while self.running:

            try:

                # if (datetime.datetime.now() - last_timestamp).total_seconds() < self.sendfreq:
                #     self.tick()
                #     continue

                # if self.debug:

                sent = 0
                while self.inbox.qsize() > 0:

                    # Boolean flag to determine message validity
                    valid = True

                    # get message
                    message = self.inbox.get_nowait()

                    # Iterates over all the filters and overrides to modify the
                    # stream's default capability.
                    for modifier in self.modifiers:
                        if isinstance(modifier, BaseOverride):
                            message = modifier.apply(message)
                        elif isinstance(modifier, BasePredicate):
                            if not modifier.apply(message):
                                valid = False

                                # Must be a break and not return because setting
                                # the initialization flag would be skipped if it
                                # needed to be set.
                                break

                    # the incoming message was not filtered
                    if valid:

                        # process the incoming message
                        self.handle(message)

                        sent += 1

                    if self.sendlimit > 0:
                        if sent >= self.sendlimit:
                            break

                # logging sent messages
                self.log("Sent %s messages..." % (sent - 1 if sent > 0 else 0))

            except Empty:
                # Empty signifies that the queue is empty, so yield to another node
                pass
            except Exception:
                self.log_exception("Error in '%s': %s" % (self.__class__.__name__, self.name))
                # self.tick()
            finally:
                # delay processing
                self.sleep(self.sendfreq)

        # self.tick()
        # self.stop()
        self.log("Exiting...")


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
