import unittest

from rthrift.broadcast import get_sender, get_listener
import thriftpy
import threading
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
    def listenerThread(self):
        self.listener.serve()

    def setUp(self):
        thrift_mod = thriftpy.load("tests/test_resources/service.thrift")
        uri = os.environ.get('AMQP_URI', 'amqp://guest:guest@localhost:5672/%2f')

        self.listener_queue = Queue()
        responder = Responder(thrift_mod, self.listener_queue)
        self.responder = responder
        self.listener = get_listener(thrift_mod.TestBroadcastService, responder, uri, routing_keys=True)
        threading.Thread(target=self.listenerThread).start()
        sleep(3) # give listener time to start and register

        self.sender = get_sender(thrift_mod.TestBroadcastService, uri)




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
        self.listener.close()

        self.sender._iprot.trans.close()
        self.sender._iprot.trans.shutdown()

if __name__ == '__main__':
    unittest.main()
