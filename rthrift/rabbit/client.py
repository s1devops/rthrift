from queue import Queue
from uuid import uuid4
import threading
import logging

import rabbitpy

from .packets import (PacketTypes, CommandPacketReceive,
                      CommandPacketConsume, CommandPacketShutdown,
                      CommandPacketPublish)


class RClient(object):

    def __init__(self, server_uri):
        self.logger = logging.getLogger(__name__)
        self.server_uri = server_uri

        self._conn = rabbitpy.Connection(self.server_uri)
        #self._channel = self.conn.channel()

        self.thread_actions = []
        self._threads = []

        self.cmd_queue = Queue()
        self.add_thread_action(self._cmd_consume)

    def _cmd_consume(self):
        reply_queues = {}
        channels = {}
        default_channel = self.conn.channel()
        while True:
            cmd = self.cmd_queue.get()
            self.cmd_queue.task_done()
            self.logger.debug('Command packet: %s', cmd)
            if cmd.TYPE == PacketTypes.PUBLISH:
                channel = channels.get(cmd.channel_id, default_channel)
                msg = cmd.msg.to_rmq_msg(channel)
                msg.publish(cmd.exchange, cmd.routing_key, cmd.mandatory)
            elif cmd.TYPE == PacketTypes.SHUTDOWN:
                for _, target_queue in reply_queues.items():
                    target_queue.put(cmd)
                reply_queues = {}
                return
            elif cmd.TYPE == PacketTypes.RECEIVE:
                if cmd.queue_id in reply_queues:
                    reply_queues[cmd.queue_id].put(cmd)
                else:
                    raise Exception('todo, unrouted packets')
            elif cmd.TYPE == PacketTypes.CONSUME:
                reply_queues[cmd.queue_id] = cmd.reply_queue
                if cmd.channel is not None:
                    channels[cmd.queue_id] = cmd.channel
            else:
                raise Exception('unknown packet type')

    @property
    def conn(self):
        return self._conn

    def add_thread_action(self, func, args=None):
        if args is None:
            args = ()
        thread = threading.Thread(target=func, args=args)
        thread.start()
        self._threads.append(thread)

    def start(self):
        pass

    def consumer(self, message_action, name='', setup_func=None, no_ack=False, **kwargs):
        channel = self.conn.channel()
        rmq_queue = rabbitpy.Queue(channel, name=name, **kwargs)
        rmq_queue.declare()
        queue_id = str(uuid4())

        def action_func(local_queue, reply_queue):
            while True:
                cmd = reply_queue.get()
                reply_queue.task_done()
                if cmd.TYPE == PacketTypes.SHUTDOWN:
                    local_queue.stop_consuming()
                    return
                elif cmd.TYPE == PacketTypes.RECEIVE:
                    result = message_action(cmd.msg)
                    if result is False:
                        return
                else:
                    raise Exception('Unknown packet type in action_func')

        def consume_func(local_queue):
            for msg in rmq_queue.consume(no_ack=no_ack):
                if msg is None:
                    return
                self.cmd_queue.put(CommandPacketReceive(msg, queue_id))

        if setup_func:
            setup_func(rmq_queue)

        reply_queue = Queue()
        cmd = CommandPacketConsume(queue_id, reply_queue, channel)
        self.cmd_queue.put(cmd)

        self.add_thread_action(consume_func, args=(rmq_queue,))
        self.add_thread_action(action_func, args=(rmq_queue, reply_queue))

        return queue_id


    def shutdown(self):
        self.cmd_queue.put(CommandPacketShutdown())
        #Ugly, but the only reliable way to close the consumer from any thread
        #self.channel._read_queue.put(pamqp.specification.Basic.Cancel())
        #self.channel.close()

        for thread in self._threads:
            thread.join()
        self.cmd_queue.join()
        #self.conn.close()

    def publish(self, message, exchange_name, routing_key, channel_id=None):
        cmd = CommandPacketPublish(message,
                                   exchange=exchange_name,
                                   routing_key=routing_key,
                                   channel_id=channel_id)
        self.cmd_queue.put(cmd)
