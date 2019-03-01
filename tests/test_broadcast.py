import unittest

from rthrift.multi import Multi
import thriftpy2 as thriftpy
from time import sleep
import os

from queue import Queue

class Responder(object):

    def __init__(self, thrift_mod, listener_queue):
        self.thrift_mod = thrift_mod
        self.listener_queue = listener_queue


    def ping_broadcast(self, val):
        self.listener_queue.put(val)


class TestCommications(unittest.TestCase):
    def setUp(self):
        thrift_mod = thriftpy.load("tests/test_resources/service.thrift")
        uri = os.environ.get('AMQP_URI', 'amqp://guest:guest@localhost:5672/%2f')

        self.multi = Multi(uri)

        self.listener_queue = Queue()
        responder = Responder(thrift_mod, self.listener_queue)
        self.responder = responder
        self.sender = self.multi.new_sender(thrift_mod.TestBroadcastService)
        self.listener = self.multi.new_listener(thrift_mod.TestBroadcastService, responder, routing_keys=True)
        self.multi.start()

        # Give the listener enough time to start up
        sleep(3)


    def test_successful(self):
        range_max = 20
        for i in range(range_max):
            self.sender.ping_broadcast(i)

        recv_count = 0
        for i in range(range_max):
            val = self.listener_queue.get()
            self.assertEqual(val, i)
            recv_count += 1
        self.assertEqual(range_max, recv_count)

    def tearDown(self):
        self.multi.shutdown(wait=True)

if __name__ == '__main__':
    unittest.main()
