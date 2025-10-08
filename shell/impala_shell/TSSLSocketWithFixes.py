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
from __future__ import absolute_import, print_function, unicode_literals
import ssl

from thrift.transport.TSSLSocket import TSSLSocket


def openssl_fallback(*_args):
  return True


class TSSLSocketWithFixes(TSSLSocket):
  """
  This is a subclass of Thrift 0.16.0's TSSLSocket
  https://github.com/apache/thrift/blob/0.16.0/lib/py/src/transport/TSSLSocket.py
  that fixes isOpen (THRIFT-5595) and adds support for Python 3.12+.

  Requires Python 2.7.9+ or Python 3.2+. For Python 2.7 and 3.2-3.9, uses match_hostname
  and PROTOCOL_TLS. For Python 3.10 and 3.11, uses match_hostname and PROTOCOL_TLS_CLIENT.
  For Python 3.12+, relies solely on OpenSSL's built-in hostname validation, enabled by
  PROTOCOL_TLS_CLIENT.
  """
  def __init__(self, host, port, cert_reqs, ca_certs=None):
    # Implement Python 3.12 override from Thrift 0.22.0
    # https://github.com/apache/thrift/commit/23e0e5ce75300451f49727ee438edbc76fcbb372
    # Earlier versions continue to use ssl.match_hostname, which is available in
    # Python 2.7.9+ and Python 3.2+.
    ssl_version = ssl.PROTOCOL_SSLv23
    try:
      from ssl import match_hostname
      validate_callback = match_hostname
    except ImportError:
      validate_callback = openssl_fallback
      # ssl.PROTOCOL_TLS_CLIENT is available in Python 3.6+ and enables secure defaults
      # (CERT_REQUIRED, check_hostname). Only use it when match_hostname is unavailable
      # (i.e. Python 3.12+) to avoid regressing the clarity of error messages in earlier
      # versions. See https://issues.apache.org/jira/browse/THRIFT-792 for future work.
      assert hasattr(ssl, "PROTOCOL_TLS_CLIENT")
      if cert_reqs == ssl.CERT_NONE:
        # If no cert verification is requested, use the most compatible option.
        ssl_version = ssl.PROTOCOL_TLS
      else:
        # This also enables CERT_REQUIRED and check_hostname by default.
        ssl_version = ssl.PROTOCOL_TLS_CLIENT

    TSSLSocket.__init__(self, host=host, port=port, cert_reqs=cert_reqs,
                        ca_certs=ca_certs, ssl_version=ssl_version,
                        validate_callback=validate_callback)

  # THRIFT-5595: override TSocket.isOpen because it's broken for TSSLSocket
  def isOpen(self):
    return self.handle is not None
