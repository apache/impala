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
from __future__ import print_function, unicode_literals

from io import BytesIO
import os
import os.path
import ssl
import sys
import warnings
import base64
import datetime
from collections import namedtuple

from six.moves import urllib, http_client

from thrift.transport.TTransport import TTransportBase
from shell_exceptions import HttpError, AuthenticationException
from cookie_util import get_all_matching_cookies, get_all_cookies, get_cookie_expiry

import six

# Declare namedtuple for Cookie with named fields - cookie and expiry_time
Cookie = namedtuple('Cookie', ['cookie', 'expiry_time'])

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
      key_file=None, ssl_context=None, http_cookie_names=None, socket_timeout_s=None,
      verbose=False):
    """ImpalaHttpClient supports two different types of construction:

    ImpalaHttpClient(host, port, path) - deprecated
    ImpalaHttpClient(uri, [port=<n>, path=<s>, cafile=<filename>, cert_file=<filename>,
        key_file=<filename>, ssl_context=<context>], http_cookie_names=<cookienamelist>])

    Only the second supports https.  To properly authenticate against the server,
    provide the client's identity by specifying cert_file and key_file.  To properly
    authenticate the server, specify either cafile or ssl_context with a CA defined.
    NOTE: if both cafile and ssl_context are defined, ssl_context will override cafile.
    http_cookie_names is used to specify a comma-separated list of possible cookie names
    used for cookie-based authentication or session management. If a cookie with one of
    these names is returned in an http response by the server or an intermediate proxy
    then it will be included in each subsequent request for the same connection. If it
    is set as wildcards, all cookies in an http response will be preserved.
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
    if (not http_cookie_names) or (str(http_cookie_names).strip() == ""):
      self.__preserve_all_cookies = False
      self.__http_cookie_dict = None
      self.__auth_cookie_names = None
    elif str(http_cookie_names).strip() == "*":
      self.__preserve_all_cookies = True
      self.__http_cookie_dict = dict()
      self.__auth_cookie_names = set()
    else:
      self.__preserve_all_cookies = False
      # Build a dictionary that maps cookie name to namedtuple.
      cookie_names = http_cookie_names.split(',')
      self.__http_cookie_dict = \
          {cn: Cookie(cookie=None, expiry_time=None) for cn in cookie_names}
      # Store the auth cookie names in __auth_cookie_names.
      # Assume auth cookie names end with ".auth".
      self.__auth_cookie_names = {cn for cn in cookie_names if cn.endswith(".auth")}
    # Set __are_matching_cookies_found as True if matching cookies are found in response.
    self.__are_matching_cookies_found = False
    self.__wbuf = BytesIO()
    self.__http = None
    self.__http_response = None
    self.__timeout = socket_timeout_s
    # __custom_headers is used to store HTTP headers which are generated in runtime for
    # new request.
    self.__custom_headers = None
    self.__get_custom_headers_func = None
    self.__basic_auth = None
    self.__kerb_service = None
    self.__add_custom_headers_funcs = []
    self.__verbose = verbose

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

  def getCustomHeadersWithBasicAuth(self, cookie_header, has_auth_cookie):
    custom_headers = {}
    if cookie_header:
      # Add cookies to HTTP header.
      custom_headers['Cookie'] = cookie_header
    # Add the 'Authorization' header to request even if the auth cookie is
    # present to avoid a round trip in case the cookie is expired when server
    # receive the request. Since the 'auth' value is calculated once, so it
    # won't cause a performance issue.
    custom_headers['Authorization'] = "Basic " + self.__basic_auth
    return custom_headers

  def getCustomHeadersWithBearerAuth(self, cookie_header, has_auth_cookie):
    custom_headers = {}
    if cookie_header:
      # Add cookies to HTTP header.
      custom_headers['Cookie'] = cookie_header
    # Add the 'Authorization' header to request even if the auth cookie is
    # present to avoid a round trip in case the cookie is expired when server
    # receive the request. Since the 'auth' value is calculated once, so it
    # won't cause a performance issue.
    custom_headers['Authorization'] = "Bearer {0}".format(self.__bearer_token)
    return custom_headers

  def getCustomHeadersWithNegotiateAuth(self, cookie_header, has_auth_cookie):
    import kerberos
    custom_headers = {}
    if cookie_header:
      # Add cookies to HTTP header.
      custom_headers['Cookie'] = cookie_header
    # For GSSAPI over http we need to dynamically generate custom request headers.
    if not has_auth_cookie:
      try:
        _, krb_context = kerberos.authGSSClientInit(self.__kerb_service)
        kerberos.authGSSClientStep(krb_context, "")
        negotiate_details = kerberos.authGSSClientResponse(krb_context)
        custom_headers['Authorization'] = "Negotiate " + negotiate_details
      except kerberos.GSSError:
        raise AuthenticationException("Kerberos authentication failure.")
    return custom_headers

  def getCustomHeadersWithoutAuth(self, cookie_header, has_auth_cookie):
    custom_headers = {}
    if cookie_header:
      # Add cookies to HTTP header.
      custom_headers['Cookie'] = cookie_header
    return custom_headers

  # Set function to generate customized HTTP headers for Ldap authorization.
  def setLdapAuth(self, basic_auth):
    # auth mechanism: LDAP
    self.__basic_auth = basic_auth
    self.__get_custom_headers_func = self.getCustomHeadersWithBasicAuth

  # Set function to generate customized HTTP headers for JWT authorization.
  def setJwtAuth(self, jwt):
    # auth mechanism: JWT
    self.__bearer_token = jwt
    self.__get_custom_headers_func = self.getCustomHeadersWithBearerAuth

  # Set function to generate customized HTTP headers for Kerberos authorization.
  def setKerberosAuth(self, kerb_service):
    # auth mechanism: GSSAPI
    self.__kerb_service = kerb_service
    self.__get_custom_headers_func = self.getCustomHeadersWithNegotiateAuth

  # Set function to generate customized HTTP headers without authorization.
  def setNoneAuth(self):
    # auth mechanism: None
    self.__get_custom_headers_func = self.getCustomHeadersWithoutAuth

  # Whenever http(s) calls are made to the backend impala, each function
  # added through this method will be called.  Thus, arbitrary custom
  # headers can be set on each request.
  # parameters:
  #  funcs - tuple of functions where each takes no arguments and returns
  #      a dict of http headers
  # Note:  if the custom function returns a http header with a name that
  # does not start with "X-" or "x-", it will cause an error to be thrown
  def addCustomHeaderFunc(self, *funcs):
    if funcs is None:
      return

    for f in funcs:
      self.__add_custom_headers_funcs.append(f)

  # Update HTTP headers based on the saved cookies and auth mechanism.
  def refreshCustomHeaders(self):
    self.__custom_headers = {}

    if self.__get_custom_headers_func:
      cookie_header, has_auth_cookie = self.getHttpCookieHeaderForRequest()
      self.__custom_headers = \
          self.__get_custom_headers_func(cookie_header, has_auth_cookie)

    for f in self.__add_custom_headers_funcs:
      headers = f()
      if headers is not None:
        for key in headers:
          assert key[0:2].lower() == "x-", \
            "header '{0}' is not valid, all custom headers must start with "\
            "'X-' or 'x-'".format(key)
          assert key not in self.__custom_headers, \
            "header '{0}' already exists in custom headers dictionary".format(key)
          self.__custom_headers[key] = headers[key]

  # Return first value as a cookie list for Cookie header. It's a list of name-value
  # pairs in the form of <cookie-name>=<cookie-value>. Pairs in the list are separated by
  # a semicolon and a space ('; ').
  # Return second value as True if the cookie list contains auth cookie.
  def getHttpCookieHeaderForRequest(self):
    if (self.__http_cookie_dict is None) or not self.__are_matching_cookies_found:
      return None, False
    cookie_headers = []
    has_auth_cookie = False
    for cn, c_tuple in self.__http_cookie_dict.items():
      if c_tuple.cookie:
        if c_tuple.expiry_time and c_tuple.expiry_time <= datetime.datetime.now():
          self.__http_cookie_dict[cn] = Cookie(cookie=None, expiry_time=None)
        else:
          cookie_header = c_tuple.cookie.output(attrs=['value'], header='').strip()
          cookie_headers.append(cookie_header)
          if not has_auth_cookie and self.__auth_cookie_names \
              and cn in self.__auth_cookie_names:
            has_auth_cookie = True
    if not cookie_headers:
      self.__are_matching_cookies_found = False
      return None, False
    else:
      return '; '.join(cookie_headers), has_auth_cookie

  # Extract cookies from response and save those cookies for which the cookie names
  # are in the cookie name list specified in the __init__().
  def extractHttpCookiesFromResponse(self):
    if self.__preserve_all_cookies:
      matching_cookies = get_all_cookies(self.path, self.headers)
    elif self.__http_cookie_dict:
      matching_cookies = get_all_matching_cookies(
          self.__http_cookie_dict.keys(), self.path, self.headers)
    else:
      matching_cookies = None

    if matching_cookies:
      if self.__verbose:
        names = sorted([c.key for c in matching_cookies])
        print("Preserving cookies: " + ', '.join(names), file=sys.stderr)
        sys.stderr.flush()

      self.__are_matching_cookies_found = True
      for c in matching_cookies:
        self.__http_cookie_dict[c.key] = Cookie(c, get_cookie_expiry(c))
        if c.key.endswith(".auth"):
          self.__auth_cookie_names.add(c.key)

  # Return True if there are any saved cookies which are sent in previous request.
  def areHttpCookiesSaved(self):
    return self.__are_matching_cookies_found

  # Clean all saved cookies.
  def cleanHttpCookies(self):
    if (self.__http_cookie_dict is not None) and self.__are_matching_cookies_found:
      self.__are_matching_cookies_found = False
      self.__http_cookie_dict = \
          {cn: Cookie(cookie=None, expiry_time=None) for cn in self.__http_cookie_dict}

  def read(self, sz):
    return self.__http_response.read(sz)

  def readBody(self):
    return self.__http_response.read()

  def write(self, buf):
    self.__wbuf.write(buf)

  def flush(self):
    # Send HTTP request and receive response.
    # Return True if the client should retry this method.
    def sendRequestRecvResp(data):
      if self.isOpen():
        self.close()
      self.open()

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

      self.refreshCustomHeaders()
      if not self.__custom_headers or 'User-Agent' not in self.__custom_headers:
        user_agent = 'Python/ImpalaHttpClient'
        script = os.path.basename(sys.argv[0])
        if script:
          user_agent = '%s (%s)' % (user_agent, urllib.parse.quote(script))
        self.__http.putheader('User-Agent', user_agent)

      if self.__custom_headers:
        for key, val in six.iteritems(self.__custom_headers):
          self.__http.putheader(key, val)

      self.__http.endheaders()

      # Write payload
      self.__http.send(data)

      # Get reply to flush the request
      self.__http_response = self.__http.getresponse()
      self.code = self.__http_response.status
      self.message = self.__http_response.reason
      self.headers = self.__http_response.msg
      # A '401 Unauthorized' response might mean that we tried cookie-based
      # authentication with one or more expired cookies.
      # Delete the cookies and try again.
      if self.code == 401 and self.areHttpCookiesSaved():
        self.cleanHttpCookies()
        return True
      else:
        self.extractHttpCookiesFromResponse()
        return False

    # Pull data out of buffer
    data = self.__wbuf.getvalue()
    self.__wbuf = BytesIO()

    retry = sendRequestRecvResp(data)
    if retry:
      # Received "401 Unauthorized" response. Delete HTTP cookies and then retry.
      sendRequestRecvResp(data)

    if self.code >= 300:
      # Report any http response code that is not 1XX (informational response) or
      # 2XX (successful).
      body = self.readBody().decode('utf-8')
      raise HttpError(self.code, self.message, body, self.headers)
