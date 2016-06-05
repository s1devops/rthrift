from uuid import uuid4
import threading

from thriftpy.thrift import TProcessor
from thriftpy.transport import TBufferedTransportFactory

from .rabbit.client import RClient
from .thrift.server import TThreadedServer_R
from .thrift.transport import (TTransport_R, TBinaryProtocol_R,
                               TBinaryProtocolFactory_R)
from .thrift.client import TClient_R


class Multi(object):

    def __init__(self, uri):
        self.rmq_client = RClient(uri)
        self.children = []
        self.servers = []
        self._threads = []


    def new_client(self, service, exchange='amq.topic', read_timeout=None,
                   transport_mode=TTransport_R.CLIENT):
        c_transport = TTransport_R(self.rmq_client, transport_mode, amqp_exchange=exchange)
        c_proto = TBinaryProtocol_R(c_transport, transport_mode=transport_mode, service=service)
        client = TClient_R(service, c_proto)
        client.add_close_action(c_transport.close)
        client.add_close_action(c_transport.shutdown)

        if read_timeout is not None:
            c_transport.set_read_timeout(read_timeout)
        c_transport.open()

        self.children.append(client)
        return client


    def new_server(self, service, responder, exchange='amq.topic',
                   routing_keys=None, queue=None, transport_mode=TTransport_R.SERVER):

        if queue is None:
            queue = '{}.{}'.format(service.__module__, service.__name__)

        if routing_keys is True:
            routing_keys = ['{}.{}.{}'.format(service.__module__, service.__name__, s)
                            for s in service.thrift_services]

        s_transport = TTransport_R(self.rmq_client,
                                   transport_mode,
                                   amqp_exchange=exchange,
                                   amqp_queue=queue,
                                   routing_keys=routing_keys)

        processor = TProcessor(service, responder)

        server = TThreadedServer_R(processor,
                                   s_transport,
                                   TBufferedTransportFactory(),
                                   TBinaryProtocolFactory_R())

        server.add_close_action(s_transport.shutdown)
        self.children.append(server)
        self.servers.append(server)
        return server


    def new_sender(self, service, exchange='amq.topic', read_timeout=None):
        return self.new_client(service, exchange, read_timeout,
                               transport_mode=TTransport_R.BROADCAST_SENDER)


    def new_listener(self, service, responder, exchange='amq.topic', routing_keys=None, queue=None):
        if queue is None:
            queue = str(uuid4())

        if routing_keys is True:
            routing_keys = ['{}.{}.{}'.format(service.__module__, service.__name__, s)
                            for s in service.thrift_services]

        server = self.new_server(service,
                                 responder,
                                 exchange=exchange,
                                 routing_keys=routing_keys,
                                 queue=queue,
                                 transport_mode=TTransport_R.BROADCAST_LISTENER)

        self.children.append(server)
        return server


    def start(self):
        for server in self.servers:
            thread = threading.Thread(target=server.serve)
            thread.start()
            self._threads.append(thread)


    def shutdown(self, wait=False):
        for child in self.children:
            child.close()

        self.rmq_client.shutdown()

        if wait is True:
            self.join()


    def join(self):
        for thread in self._threads:
            thread.join()
