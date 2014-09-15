

if __name__ == '__main__':
    from haigha.connection import Connection
    from haigha.message import Message

    connection = Connection(
      user='guest', password='guest',
      vhost='/', host='localhost',
      heartbeat=None, debug=True)

    ch = connection.channel()
    ch.exchange.declare('test_exchange', 'direct')
    ch.queue.declare('test_queue', auto_delete=True)
    ch.queue.bind('test_queue', 'test_exchange', 'test_key')
    ch.basic.publish( Message('body', application_headers={'hello':'world'}),
      'test_exchange', 'test_key' )
    print ch.basic.get('test_queue')
    connection.close()