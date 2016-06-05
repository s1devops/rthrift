from queue import Queue
from enum import Enum

class PacketTypes(Enum):
    UNKNOWN = 0x1
    PUBLISH = 0x2
    RECEIVE = 0x3
    CONSUME = 0x5
    SHUTDOWN = 0x6


class CommandPacket(object):
    TYPE = PacketTypes.UNKNOWN

    def __repr__(self):
        return 'CommandPacket ({0})'.format(self.TYPE.name)


class CommandPacketPublish(CommandPacket):
    TYPE = PacketTypes.PUBLISH

    def __init__(self, msg, exchange, routing_key='', mandatory=False, channel_id=None):
        self.msg = msg
        self.exchange = exchange
        self.routing_key = routing_key
        self.mandatory = mandatory
        self.channel_id = channel_id


class CommandPacketConsume(CommandPacket):
    TYPE = PacketTypes.CONSUME

    def __init__(self, queue_id, reply_queue=None, channel=None):
        self.queue_id = queue_id
        self.reply_queue = reply_queue or Queue()
        self.channel = channel


class CommandPacketReceive(CommandPacket):
    TYPE = PacketTypes.RECEIVE

    def __init__(self, msg, queue_id):
        self.msg = msg
        self.queue_id = queue_id


class CommandPacketShutdown(CommandPacket):
    TYPE = PacketTypes.SHUTDOWN
