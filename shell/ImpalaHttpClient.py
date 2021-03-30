#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

from io import BytesIO
import os
import os.path
import ssl
import sys
import warnings
import base64
import datetime

from six.moves import urllib, http_client

from thrift.transport.TTransport import TTransportBase
from shell_exceptions import HttpError
from cookie_util import get_first_matching_cookie, get_cookie_expiry

import six


# This was taken from THttpClient.py in Thrift to allow making changes Impala needs.
# The current changes that have been applied:
# - Added logic for the 'Expect: 100-continue' header on large requests
# - If an error code is received back in flush(), an exception is thrown.
# Note there is a copy of this code in Impyla.
class ImpalaHttpClient(TTransportBase):
  """Http implementation of TTransport base."""

  # When sending requests larger than this size, include the 'Expect: 100-continue' header
  # to indicate to the server to validate the request before reading the contents. This
  # value was chosen to match curl's behavior. See Section 8.2.3 of RFC2616.
  MIN_REQUEST_SIZE_FOR_EXPECT = 1024

  def __init__(self, uri_or_host, port=None, path=None, cafile=None, cert_file=None,
      key_file=None, ssl_context=None, auth_cookie_names=None):
    """ImpalaHttpClient supports two different types of construction:

    ImpalaHttpClient(host, port, path) - deprecated
    ImpalaHttpClient(uri, [port=<n>, path=<s>, cafile=<filename>, cert_file=<filename>,
        key_file=<filename>, ssl_context=<context>], auth_cookie_names=<cookienamelist>])

    Only the second supports https.  To properly authenticate against the server,
    provide the client's identity by specifying cert_file and key_file.  To properly
    authenticate the server, specify either cafile or ssl_context with a CA defined.
    NOTE: if both cafile and ssl_context are defined, ssl_context will override cafile.
    auth_cookie_names is used to specify the list of possible cookie names used for
    cookie-based authentication. If there's only one name in the cookie name list, a str
    value can be specified instead of the list.
    """
    if port is not None:
      warnings.warn(
          "Please use the ImpalaHttpClient('http{s}://host:port/path') constructor",
          DeprecationWarning,
          stacklevel=2)
      self.host = uri_or_host
      self.port = port
      assert path
      self.path = path
      self.scheme = 'http'
    else:
      parsed = urllib.parse.urlparse(uri_or_host)
      self.scheme = parsed.scheme
      assert self.scheme in ('http', 'https')
      if self.scheme == 'http':
        self.port = parsed.port or http_client.HTTP_PORT
      elif self.scheme == 'https':
        self.port = parsed.port or http_client.HTTPS_PORT
        self.certfile = cert_file
        self.keyfile = key_file
        self.context = ssl.create_default_context(cafile=cafile) \
            if (cafile and not ssl_context) else ssl_context
      self.host = parsed.hostname
      self.path = parsed.path
      if parsed.query:
        self.path += '?%s' % parsed.query
    try:
      proxy = urllib.request.getproxies()[self.scheme]
    except KeyError:
      proxy = None
    else:
      if urllib.request.proxy_bypass(self.host):
        proxy = None
    if proxy:
      parsed = urllib.parse.urlparse(proxy)
      self.realhost = self.host
      self.realport = self.port
      self.host = parsed.hostname
      self.port = parsed.port
      self.proxy_auth = self.basic_proxy_auth_header(parsed)
    else:
      self.realhost = self.realport = self.proxy_auth = None
    self.__auth_cookie_names = auth_cookie_names
    self.__auth_cookie = None
    self.__auth_cookie_expiry = None
    self.__wbuf = BytesIO()
    self.__http = None
    self.__http_response = None
    self.__timeout = None
    self.__custom_headers = None

  @staticmethod
  def basic_proxy_auth_header(proxy):
    if proxy is None or not proxy.username:
      return None
    ap = "%s:%s" % (urllib.parse.unquote(proxy.username),
                    urllib.parse.unquote(proxy.password))
    cr = base64.b64encode(ap).strip()
    return "Basic " + cr

  def using_proxy(self):
    return self.realhost is not None

  def open(self):
    if self.scheme == 'http':
      self.__http = http_client.HTTPConnection(self.host, self.port,
                                               timeout=self.__timeout)
    elif self.scheme == 'https':
      self.__http = http_client.HTTPSConnection(self.host, self.port,
                                                key_file=self.keyfile,
                                                cert_file=self.certfile,
                                                timeout=self.__timeout,
                                                context=self.context)
    if self.using_proxy():
      self.__http.set_tunnel(self.realhost, self.realport,
                             {"Proxy-Authorization": self.proxy_auth})

  def close(self):
    self.__http.close()
    self.__http = None
    self.__http_response = None

  def isOpen(self):
    return self.__http is not None

  def setTimeout(self, ms):
    if ms is None:
      self.__timeout = None
    else:
      self.__timeout = ms / 1000.0

  def setCustomHeaders(self, headers):
    self.__custom_headers = headers

  def updateAuthCookie(self, headers):
    if self.__auth_cookie_names:
      c = get_first_matching_cookie(self.__auth_cookie_names, self.path, headers)
      if c:
        self.__auth_cookie = c
        self.__auth_cookie_expiry = get_cookie_expiry(c)

  def getAuthCookie(self):
    if self.__auth_cookie and self.__auth_cookie_expiry and \
        self.__auth_cookie_expiry <= datetime.datetime.now():
      self.__auth_cookie = None
      self.__auth_cookie_expiry = None
    return self.__auth_cookie

  def addAuthCookieToRequestHeaders(self):
    auth_cookie = self.getAuthCookie()
    if auth_cookie:
      cookie_header = auth_cookie.output(attrs=['value'], header='').strip()
      if cookie_header:
        self.__http.putheader('Cookie', cookie_header)

  def read(self, sz):
    return self.__http_response.read(sz)

  def readBody(self):
    return self.__http_response.read()

  def write(self, buf):
    self.__wbuf.write(buf)

  def flush(self):
    if self.isOpen():
      self.close()
    self.open()

    # Pull data out of buffer
    data = self.__wbuf.getvalue()
    self.__wbuf = BytesIO()

    # HTTP request
    if self.using_proxy() and self.scheme == "http":
      # need full URL of real host for HTTP proxy here (HTTPS uses CONNECT tunnel)
      self.__http.putrequest('POST', "http://%s:%s%s" %
                             (self.realhost, self.realport, self.path))
    else:
      self.__http.putrequest('POST', self.path)

    # Write headers
    self.__http.putheader('Content-Type', 'application/x-thrift')
    data_len = len(data)
    self.__http.putheader('Content-Length', str(data_len))
    if data_len > ImpalaHttpClient.MIN_REQUEST_SIZE_FOR_EXPECT:
      # Add the 'Expect' header to large requests. Note that we do not explicitly wait
      # for the '100 continue' response before sending the data - HTTPConnection simply
      # ignores these types of responses, but we'll get the right behavior anyways.
      self.__http.putheader("Expect", "100-continue")
    if self.using_proxy() and self.scheme == "http" and self.proxy_auth is not None:
      self.__http.putheader("Proxy-Authorization", self.proxy_auth)

    if not self.__custom_headers or 'User-Agent' not in self.__custom_headers:
      user_agent = 'Python/ImpalaHttpClient'
      script = os.path.basename(sys.argv[0])
      if script:
        user_agent = '%s (%s)' % (user_agent, urllib.parse.quote(script))
      self.__http.putheader('User-Agent', user_agent)

    if self.__custom_headers:
      for key, val in six.iteritems(self.__custom_headers):
        self.__http.putheader(key, val)

    self.addAuthCookieToRequestHeaders()
    self.__http.endheaders()

    # Write payload
    self.__http.send(data)

    # Get reply to flush the request
    self.__http_response = self.__http.getresponse()
    self.code = self.__http_response.status
    self.message = self.__http_response.reason
    self.headers = self.__http_response.msg
    self.updateAuthCookie(self.headers)

    if self.code >= 300:
      # Report any http response code that is not 1XX (informational response) or
      # 2XX (successful).
      body = self.readBody()
      raise HttpError(self.code, self.message, body, self.headers)
