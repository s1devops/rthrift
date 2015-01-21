from .transport import TTransport_R, TBinaryProtocolFactory_R
from .rclient import RClient

from thriftpy.thrift import TClient, TProcessor
from thriftpy.server import TThreadedServer, TSimpleServer
from thriftpy.transport.transport import TBufferedTransportFactory


def get_server(service, responder, uri, exchange, queue):
        rclient = RClient(uri)
        s_transport = TTransport_R(rclient, TTransport_R.SERVER, amqp_exchange = exchange, amqp_queue = queue)
        processor = TProcessor(service, responder)
        server = TThreadedServer(processor, s_transport, TBufferedTransportFactory(), TBinaryProtocolFactory_R())

        return server
