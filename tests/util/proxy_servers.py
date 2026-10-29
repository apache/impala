# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Minimal in-process HTTP, HTTPS and SOCKS5 proxies for tests.
#
# Every proxy forwards all connections to a single fixed backend address, regardless of
# the destination requested by the client, and records the requested destinations. This
# lets tests use unresolvable host names in URLs to prove that requests went through the
# proxy.

import select
import socket
import socketserver
import ssl
import struct
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class StaticFileServer(object):
  """HTTP(S) server that answers every GET request with the contents of a file."""

  def __init__(self, port, file_path, certfile=None, keyfile=None):
    with open(file_path, 'rb') as f:
      body = f.read()

    class Handler(BaseHTTPRequestHandler):
      def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

      def log_message(self, _format, *_args):
        pass

    HTTPServer.allow_reuse_address = True
    self._server = HTTPServer(('127.0.0.1', port), Handler)
    if certfile is not None:
      context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
      context.load_cert_chain(certfile, keyfile)
      self._server.socket = context.wrap_socket(self._server.socket, server_side=True)
    self._thread = threading.Thread(target=self._server.serve_forever)
    self._thread.daemon = True

  def start(self):
    self._thread.start()

  def stop(self):
    self._server.shutdown()
    self._server.server_close()


def _pipe(sock_a, sock_b):
  """Copies data in both directions until either side closes its connection."""
  sockets = [sock_a, sock_b]
  while True:
    readable, _, _ = select.select(sockets, [], [], 30)
    if not readable:
      return
    for sock in readable:
      data = sock.recv(65536)
      if not data:
        return
      # A TLS socket may hold already decrypted data that select() does not report.
      while isinstance(sock, ssl.SSLSocket) and sock.pending():
        data += sock.recv(sock.pending())
      (sock_b if sock is sock_a else sock_a).sendall(data)


def _recv_exactly(sock, num_bytes):
  data = b''
  while len(data) < num_bytes:
    chunk = sock.recv(num_bytes - len(data))
    if not chunk:
      raise IOError('Connection closed by client')
    data += chunk
  return data


class _ProxyServer(socketserver.ThreadingTCPServer):
  allow_reuse_address = True
  daemon_threads = True


class _BaseProxy(object):
  """Common start/stop logic. 'requests' lists the destinations requested by clients."""

  def __init__(self, port, backend_address, certfile=None, keyfile=None):
    self.backend_address = backend_address
    self.requests = []
    self._lock = threading.Lock()
    self._server = _ProxyServer(('127.0.0.1', port), self._make_handler())
    if certfile is not None:
      context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
      context.load_cert_chain(certfile, keyfile)
      self._server.socket = context.wrap_socket(self._server.socket, server_side=True)
    self._thread = threading.Thread(target=self._server.serve_forever)
    self._thread.daemon = True

  def start(self):
    self._thread.start()

  def stop(self):
    self._server.shutdown()
    self._server.server_close()

  def clear_requests(self):
    with self._lock:
      del self.requests[:]

  def get_requests(self):
    with self._lock:
      return list(self.requests)

  def _record(self, request):
    with self._lock:
      self.requests.append(request)

  def _make_handler(self):
    raise NotImplementedError()


class HttpProxy(_BaseProxy):
  """HTTP proxy that supports both absolute-form requests (e.g. 'GET http://host/path')
  and CONNECT tunnels. Pass 'certfile' and 'keyfile' to make it an HTTPS proxy, i.e. a
  proxy that clients connect to over TLS. Requests are recorded as
  (method, request target) tuples."""

  def _make_handler(self):
    proxy = self

    class Handler(socketserver.StreamRequestHandler):
      def handle(self):
        request_line = self.rfile.readline().decode('latin-1').strip()
        # A client that aborts the TLS handshake, e.g. on a public key pin mismatch,
        # closes the connection without sending a request.
        if not request_line:
          return
        headers = []
        while True:
          line = self.rfile.readline().decode('latin-1')
          if line in ('\r\n', '\n', ''):
            break
          headers.append(line.strip())
        method, target, version = request_line.split(' ', 2)
        proxy._record((method, target))
        backend = socket.create_connection(proxy.backend_address)
        try:
          if method == 'CONNECT':
            self.wfile.write(b'HTTP/1.1 200 Connection established\r\n\r\n')
            self.wfile.flush()
            _pipe(self.connection, backend)
          else:
            path = '/' + target.split('://', 1)[-1].split('/', 1)[-1]
            forwarded = ['{0} {1} {2}'.format(method, path, version)]
            forwarded += [h for h in headers
                          if not h.lower().startswith(('connection:', 'proxy-'))]
            forwarded.append('Connection: close')
            backend.sendall(('\r\n'.join(forwarded) + '\r\n\r\n').encode('latin-1'))
            _pipe(self.connection, backend)
        finally:
          backend.close()

    return Handler


class Socks5Proxy(_BaseProxy):
  """SOCKS5 proxy without authentication that supports the CONNECT command. Requests are
  recorded as (destination host, destination port) tuples."""

  def _make_handler(self):
    proxy = self

    class Handler(socketserver.BaseRequestHandler):
      def handle(self):
        conn = self.request
        version, num_methods = struct.unpack('!BB', _recv_exactly(conn, 2))
        methods = _recv_exactly(conn, num_methods)
        if version != 5 or b'\x00' not in methods:
          conn.sendall(b'\x05\xff')
          return
        conn.sendall(b'\x05\x00')
        version, command, _, address_type = struct.unpack('!BBBB',
            _recv_exactly(conn, 4))
        if address_type == 1:
          host = socket.inet_ntoa(_recv_exactly(conn, 4))
        elif address_type == 3:
          host_len = struct.unpack('!B', _recv_exactly(conn, 1))[0]
          host = _recv_exactly(conn, host_len).decode('idna')
        elif address_type == 4:
          host = socket.inet_ntop(socket.AF_INET6, _recv_exactly(conn, 16))
        else:
          conn.sendall(b'\x05\x08\x00\x01' + b'\x00' * 6)
          return
        port = struct.unpack('!H', _recv_exactly(conn, 2))[0]
        if command != 1:
          conn.sendall(b'\x05\x07\x00\x01' + b'\x00' * 6)
          return
        proxy._record((host, port))
        backend = socket.create_connection(proxy.backend_address)
        try:
          conn.sendall(b'\x05\x00\x00\x01' + b'\x00' * 6)
          _pipe(conn, backend)
        finally:
          backend.close()

    return Handler
