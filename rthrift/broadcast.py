from thriftpy.thrift import TClient

from .rclient import RClient
from .transport import TTransport_R, TBinaryProtocol_R

def get_sender(service, uri, exchange):
    client = RClient(uri)
    c_transport = TTransport_R(client, TTransport_R.BROADCAST_SENDER, amqp_exchange = exchange)
    c_proto = TBinaryProtocol_R(c_transport)
    client = TClient(service, c_proto)
    c_transport.open()

    return client
