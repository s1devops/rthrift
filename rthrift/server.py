from thriftpy2.thrift import TProcessor
from thriftpy2.transport import TBufferedTransportFactory

from .thrift.server import TThreadedServer_R
from .thrift.transport import TTransport_R, TBinaryProtocolFactory_R
from .rabbit.client import RClient


def get_server(service, responder, uri, exchange='amq.topic',
               routing_keys=None, queue=None, transport_mode=TTransport_R.SERVER):

    if queue is None:
        queue = '{}.{}'.format(service.__module__, service.__name__)

    if routing_keys is True:
        routing_keys = ['{}.{}.{}'.format(service.__module__, service.__name__, s)
                        for s in service.thrift_services]

    rclient = RClient(uri)

    s_transport = TTransport_R(rclient,
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

    return server
