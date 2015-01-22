from .transport import TTransport_R, TBinaryProtocolFactory_R
from .rclient import RClient


from thriftpy.thrift import TClient, TProcessor
from thriftpy.server import TThreadedServer, TSimpleServer
from thriftpy.transport.transport import TBufferedTransportFactory

class TThreadedServer_R(TThreadedServer):
    def __init__(self, *args, **kwargs):
        TThreadedServer.__init__(self, *args, **kwargs)
        self._close_actions = []

    def add_close_action(self, action):
        self._close_actions.append(action)

    def close(self):
        self.closed = True
        for action in self._close_actions:
            action()


def get_server(service, responder, uri, exchange, queue):
    rclient = RClient(uri)
    s_transport = TTransport_R(rclient, TTransport_R.SERVER, amqp_exchange = exchange, amqp_queue = queue)
    processor = TProcessor(service, responder)
    server = TThreadedServer_R(processor, s_transport, TBufferedTransportFactory(), TBinaryProtocolFactory_R())

    server.add_close_action(s_transport.shutdown)
    return server
