from io import BytesIO
from thriftpy.protocol.binary import TBinaryProtocol
from thriftpy.thrift import TMessageType, TApplicationException

from thriftpy.transport.transport import TTransportBase, TTransportException

from .rclient import Message
import rabbitpy

class TBinaryProtocolFactory_R(object):
    def __init__(self, strict_read=True, strict_write=True):
        self.strict_read = strict_read
        self.strict_write = strict_write

    def get_protocol(self, trans):
        return TBinaryProtocol_R(trans, self.strict_read, self.strict_write)

class TBinaryProtocol_R(TBinaryProtocol):
    pass
    #def write_message_begin(self, name, ttype, seqid):
    #    #self.trans.set_rpc_func(name)
    #    #self.trans.set_rpc_msg_id(seqid)
    #    super().write_message_begin(name, ttype, seqid)

    #def read_message_begin(self):
    #    print("reading message begin")
    #    api, type, seqid = super().read_message_begin()
    #    print(api, type, seqid)
    #    return api, type, seqid


class TTransport_Dummy(object):
    def excp(self):
        raise TTransportException('rmq is closed')

    def listen(self):
        self.excp()

    def read(self,sz):
        raise TTransportException('rmq is closed')

    def close(self):
        pass

class TTransport_R(TTransportBase):
    CLIENT = 'CLIENT'
    SERVER = 'SERVER'

    CLOSED = 'CLOSED'
    OPEN = 'OPEN'

    def __init__(self, amqp_client, role = None, amqp_exchange = None, amqp_queue = None):
        if role is None:
            role = self.CLIENT
        self._amqp_client = amqp_client
        self.amqp_exchange = amqp_exchange
        self.amqp_queue = amqp_queue

        self._status = self.CLOSED

        self._accept_count = 0

        # TODO: bind server/client
        self._wbuf = None
        self._rbuf = None

        self._rpc_func = None
        self._rpc_msg_id = None
        self._role = role

        self._rpc_queue = None

    def listen(self):
        queue = self._amqp_client.consumer(self.msg_recv, name=self.amqp_queue)
        queue.bind(self.amqp_exchange, self.amqp_queue)
        self._amqp_client.start()
        self._status = self.OPEN

    def accept(self):
        # TODO: do we want to return something besides self?
        #self._read_msg()
        if self._accept_count < 1:
            self._accept_count += 1
            return self
        else:
            [t.join() for t in self._amqp_client._threads]
            return TTransport_Dummy()

    def open(self):
        if self._role == self.CLIENT:
            q = self._amqp_client.consumer(self.msg_recv, no_ack = True, name='amq.rabbitmq.reply-to')
            self._amqp_client.start()
            self._status = self.OPEN

    def close(self):
        #print("close called")
        pass

    def shutdown(self):
        self._status = self.CLOSED
        self._amqp_client.shutdown()
        self.rpc_queue.put(None)

    @property
    def wbuf(self):
        if self._wbuf is None:
            self._wbuf = BytesIO()
        return self._wbuf

    @property
    def rbuf(self):
        if self._rbuf is None:
            self._rbuf = BytesIO()
        return self._rbuf

    @property
    def rpc_func(self):
        return self._rpc_func

    @property
    def rpc_queue(self):
        if self._rpc_queue is None:
            import queue
            self._rpc_queue = queue.Queue()
        return self._rpc_queue

    def set_rpc_func(self, name):
        self._rpc_func = name

    @property
    def rpc_msg_id(self):
        return self._rpc_msg_id

    def set_rpc_msg_id(self, id):
        self._rpc_msg_id = id

    def wbuf_reset(self):
        self._wbuf = BytesIO()

    def write(self, buf):
        if self._status != self.OPEN:
            raise TTransportException('RMQ Transport Not Open (write)')
        self.wbuf.write(buf)

    def msg_recv(self, msg):
        self.rpc_queue.put(msg)

        if self._role == self.SERVER and msg is not None:
            msg.ack()

    def _read_msg(self):
        msg = self.rpc_queue.get()
        if msg is None:
            return False
        if self._role == self.SERVER:
            self._reply_to = msg.properties['reply_to']

        #TODO: merge with existing buffer?
        self._rbuf = BytesIO(msg.body)
        return True

    def read(self, sz):
        if self._status != self.OPEN:
            raise TTransportException('RMQ Transport Not Open (read)')

        ret = self.rbuf.read(sz)
        if len(ret) != 0:
            return ret

        self._read_msg()
        if self._status != self.OPEN:
            raise TTransportException('RMQ Transport Not Open (read)')

        return self.rbuf.read(sz)

    def flush(self):
        msg = self.wbuf.getvalue()
        self.wbuf_reset()

        properties = {
            'content_type': 'application/x-thrift',
            'correlation_id': self.rpc_msg_id,
        }

        message = Message(msg,properties)

        if self._role == self.CLIENT:
            properties['reply_to'] = 'amq.rabbitmq.reply-to'
            exchange = self.amqp_exchange
            self._amqp_client.publish(message, exchange, self.amqp_queue)
        else:
            #TODO: are we thread safe?
            self._amqp_client.publish(message, "", self._reply_to)
