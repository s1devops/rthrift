import unittest

from rthrift.multi import Multi
import thriftpy2 as thriftpy
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
    def setUp(self):
        thrift_mod = thriftpy.load("tests/test_resources/service.thrift")
        uri = os.environ.get('AMQP_URI', 'amqp://guest:guest@localhost:5672/%2f')

        self.multi = Multi(uri)

        responder = Responder(thrift_mod)
        self.responder = responder
        self.client = self.multi.new_client(thrift_mod.TestService, read_timeout=1)
        self.server = self.multi.new_server(thrift_mod.TestService, responder, routing_keys=True)

        self.multi.start()


    def test_successful(self):
        self.responder.set_ping_func(lambda x: x)

        send_count = 20
        recv_count = 0
        for i in range(send_count):
            response = self.client.ping(i)
            recv_count += 1
            self.assertEqual(response, i)
        self.assertEqual(recv_count, send_count)

    def test_wrong_response(self):
        self.responder.set_ping_func(lambda x: None)
        with self.assertRaises(thriftpy.thrift.TApplicationException):
            response = self.client.ping(1)


    def test_timeout(self):
        def wait_long(val):
            sleep(3)
            return val
        self.responder.set_ping_func(wait_long)
        with self.assertRaises(thriftpy.transport.TTransportException):
            response = self.client.ping(101)

        sleep(3) # give server time to clear
        self.responder.set_ping_func(lambda x: x)

        request = 102
        response = self.client.ping(request)
        self.assertEqual(response, request)

    def tearDown(self):
        self.multi.shutdown(wait=True)

if __name__ == '__main__':
    unittest.main()
