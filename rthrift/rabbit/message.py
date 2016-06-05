import rabbitpy

class Message(object):
    def __init__(self, body_value, properties=None, auto_id=False, opinionated=False):
        """Create a new instance of the Message object."""
        # Always have a dict of properties set
        self.properties = properties or {}

        self.body_value = body_value
        self.auto_id = auto_id
        self.opinionated = opinionated

    def to_rmq_msg(self, channel):
        return rabbitpy.Message(channel, self.body_value, self.properties, self.auto_id, self.opinionated)
