from thriftpy.thrift import TClient

from .rclient import RClient
from .transport import TTransport_R, TBinaryProtocol_R

def get_client(service, uri, exchange, queue, read_timeout=None):
    client = RClient(uri)
    c_transport = TTransport_R(client, TTransport_R.CLIENT, amqp_exchange = exchange, amqp_queue = queue)
    c_proto = TBinaryProtocol_R(c_transport)
    client = TClient(service, c_proto)
    if read_timeout is not None:
        c_transport.set_read_timeout(read_timeout)
    c_transport.open()

    return client
