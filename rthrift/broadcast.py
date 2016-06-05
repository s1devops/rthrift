from uuid import uuid4

from .server import get_server
from .client import get_client
from .thrift.transport import TTransport_R


def get_sender(service, uri=None, exchange='amq.topic', rmq_client=None):
    return get_client(service, uri=uri, exchange=exchange,
                      transport_mode=TTransport_R.BROADCAST_SENDER,
                      rmq_client=rmq_client)


def get_listener(service, responder, uri, exchange='amq.topic', routing_keys=None, queue=None):
    if queue is None:
        queue = str(uuid4())

    if routing_keys is True:
        routing_keys = ['{}.{}.{}'.format(service.__module__, service.__name__, s)
                        for s in service.thrift_services]

    return get_server(service,
                      responder,
                      uri,
                      exchange=exchange,
                      routing_keys=routing_keys,
                      queue=queue,
                      transport_mode=TTransport_R.BROADCAST_LISTENER)
