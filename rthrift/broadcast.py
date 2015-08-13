from .rclient import RClient
from .transport import TTransport_R, TBinaryProtocol_R, TBinaryProtocolFactory_R

from .server import TThreadedServer_R
from .client import TClient_R
from thriftpy.transport import TBufferedTransportFactory
from thriftpy.thrift import TProcessor
from uuid import uuid4


def get_sender(service, uri, exchange='amq.topic'):
    rmq_client = RClient(uri)
    c_transport = TTransport_R(rmq_client, TTransport_R.BROADCAST_SENDER, amqp_exchange=exchange)
    c_proto = TBinaryProtocol_R(c_transport, transport_mode=TTransport_R.BROADCAST_SENDER, service=service)
    client = TClient_R(service, c_proto)
    client.add_close_action(c_transport.close)
    client.add_close_action(c_transport.shutdown)

    c_transport.open()

    return client


def get_listener(service, responder, uri, exchange='amq.topic', routing_keys=None, queue=None):
    if queue is None:
        queue = str(uuid4())

    if routing_keys is True:
        routing_keys = ['{}.{}.{}'.format(service.__module__, service.__name__, s) for s in service.thrift_services]
    rclient = RClient(uri)
    s_transport = TTransport_R(rclient, TTransport_R.BROADCAST_LISTENER, amqp_exchange = exchange, amqp_queue = queue, routing_keys=routing_keys)
    processor = TProcessor(service, responder)
    server = TThreadedServer_R(processor, s_transport, TBufferedTransportFactory(), TBinaryProtocolFactory_R())
    server.add_close_action(s_transport.shutdown)

    return server
