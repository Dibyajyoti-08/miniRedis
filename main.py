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
    def __init__(self):
        self.handlers = {
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_integer,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dict
        }
    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            # Delegate to the appropriate handler based on the first byte
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('bad request')

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip('\r\n')

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip('\r\n'))

    def handle_integer(self, socket_file):
        return int(socket_file.readline().rstrip('\r\n'))

    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip('\r\n'))
        if length == -1:
            return None  # Special-case for NULLs.
        length += 2 # Include the trailing \r\n in count.
        return socket_file.read(length)[:-2]

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip('\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip('\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def write_response(self, socket_file, data):
        def write_response(self, socket_file, data):
            buf = BytesIO()
            self._write(buf, data)
            buf.seek(0)
            socket_file.write(buf.getvalue())
            socket_file.flush()

        def _write(self, buf, data):
            if isinstance(data, str):
                data = data.encode('utf-8')

            if isinstance(data, bytes):
                buf.write('$%s\r\n%s\r\n' % (len(data), data))
            elif isinstance(data, int):
                buf.write(':%s\r\n' % data)
            elif isinstance(data, Error):
                buf.write('-%s\r\n' % Error.message)
            elif isinstance(data, (list, tuple)):
                buf.write('*%s\r\n' % len(data))
                for item in data:
                    self._write(buf, item)
            elif isinstance(data, dict):
                buf.write('%%%s\r\n' % len(data))
                for key in data:
                    self._write(buf, key)
                    self._write(buf, data[key])
            elif data is None:
                buf.write('$-1\r\n')
            else:
                raise CommandError('unrecognized type: %s' % type(data))

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
