from .rclient import RClient
from .transport import TTransport_R, TBinaryProtocol_R, TBinaryProtocolFactory_R

from .server import TThreadedServer_R
from thriftpy.transport.transport import TBufferedTransportFactory
from thriftpy.thrift import TClient, TProcessor
from uuid import uuid4


def get_sender(service, uri, exchange):
    client = RClient(uri)
    c_transport = TTransport_R(client, TTransport_R.BROADCAST_SENDER, amqp_exchange = exchange)
    c_proto = TBinaryProtocol_R(c_transport, transport_mode=TTransport_R.BROADCAST_SENDER,service_name=service.__name__)
    client = TClient(service, c_proto)
    c_transport.open()

    return client


def get_listener(service, responder, uri, exchange, routing_keys, queue=None):
    if queue is None:
        queue = str(uuid4())
    rclient = RClient(uri)
    s_transport = TTransport_R(rclient, TTransport_R.BROADCAST_LISTENER, amqp_exchange = exchange, amqp_queue = queue, routing_keys=routing_keys)
    processor = TProcessor(service, responder)
    server = TThreadedServer_R(processor, s_transport, TBufferedTransportFactory(), TBinaryProtocolFactory_R())

    server.add_close_action(s_transport.shutdown)
    return server
