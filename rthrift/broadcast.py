from .transport import TTransport_R

from .server import get_server
from .client import get_client
from uuid import uuid4


def get_sender(service, uri, exchange='amq.topic'):
    return get_client, service, uri, exchange=exchange, role=TTransport_R.BROADCAST_SENDER)


def get_listener(service, responder, uri, exchange='amq.topic', routing_keys=None, queue=None):
    if queue is None:
        queue = str(uuid4())

    if routing_keys is True:
        routing_keys = ['{}.{}.{}'.format(service.__module__, service.__name__, s) for s in service.thrift_services]

    return get_server(service, responder, uri, exchange=exchange, routing_keys=routing_keys, queue=queue, role=TTransport_R.BROADCAST_LISTENER)
