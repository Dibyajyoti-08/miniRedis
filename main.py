from gevent import socket
from gevent.pool import pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as socketerror

# Using exceptions to notify the connection-handling loop of problem
class CommandError(Exception): pass
class Disconnect(Exception): pass

Error = namedtuple('Error', ('message',))

class ProtocolHandler(object):
    def handle_request(self, socket_file):
        pass

    def write_response(self, socket_file, data):
        pass

class Server(object):
    def __init__(self, 127.0.0.1, port=8096, max_client=64):
        self.pool = Pool(max_clients)
        self.server = StreamServer(
            (host, port),
            self.connection_handler,
            spawn=self.pool
        )

        self._protocol = ProtocolHandler()
        self._kv = {}

    def connection_handler(self, conn, address):
        socket_file = conn.makefile('rwb')

        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                resp = Error(exc.args[0])

            self._protocol.write_response(socket_file, resp)

        def get_response(self, data):
            pass

        def run(self):
            self._server.server_forever()
















