import unittest

from rthrift.broadcast import get_sender
import thriftpy
import threading
from time import sleep
import os

class Responder(object):

    def __init__(self, thrift_mod):
        self.thrift_mod = thrift_mod

        self.set_ping_func(lambda x: x)

    def set_ping_func(self, func):
        self._ping_func = func

    def ping(self, val):
        return self._ping_func(val)


class TestCommications(unittest.TestCase):
    def serverThread(self):
        self.server.serve()

    def setUp(self):
        thrift_mod = thriftpy.load("tests/test_resources/service.thrift")
        uri = os.environ.get('AMQP_URI', 'amqp://guest:guest@localhost:5672/%2f')

        queue = 'test.communication'
        exchange = 'amq.match'

        #responder = Responder(thrift_mod)
        #self.responder = responder
        #self.server = get_server(thrift_mod.TestService, responder, uri, exchange, queue)
        #threading.Thread(target=self.serverThread).start()

        self.sender = get_sender(thrift_mod.TestService, uri, exchange)




    def test_successful(self):
        #self.responder.set_ping_func(lambda x: x)
        for i in range(20):
            self.sender.ping_broadcast(i)
            #self.assertEqual(response, i)


    def tearDown(self):
        #self.server.close()

        self.sender._iprot.trans.close()
        self.sender._iprot.trans.shutdown()

if __name__ == '__main__':
    unittest.main()
