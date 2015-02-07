import unittest

from rthrift.server import get_server
from rthrift.client import get_client
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

        responder = Responder(thrift_mod)
        self.responder = responder
        self.server = get_server(thrift_mod.TestService, responder, uri, routing_keys=True)
        threading.Thread(target=self.serverThread).start()

        self.client = get_client(thrift_mod.TestService, uri, read_timeout=1)




    def test_successful(self):
        self.responder.set_ping_func(lambda x: x)
        for i in range(20):
            response = self.client.ping(i)
            self.assertEqual(response, i)

    def test_wrong_response(self):
        self.responder.set_ping_func(lambda x: None)
        with self.assertRaises(thriftpy.thrift.TApplicationException):
            response = self.client.ping(1)


    def test_timeout(self):
        def wait_long(val):
            sleep(3)
            return val
        self.responder.set_ping_func(wait_long)
        with self.assertRaises(thriftpy.transport.transport.TTransportException):
            response = self.client.ping(101)


        sleep(3) # give server time to clear
        self.responder.set_ping_func(lambda x: x)

        request = 102
        response = self.client.ping(request)
        self.assertEqual(response, request)

    def tearDown(self):
        self.server.close()

        self.client._iprot.trans.close()
        self.client._iprot.trans.shutdown()

if __name__ == '__main__':
    unittest.main()
