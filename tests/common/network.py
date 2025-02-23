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

# Tools for identifying network characteristics.

from __future__ import absolute_import, division, print_function
import socket
import ssl

from tests.common.environ import IS_REDHAT_DERIVATIVE

# Retrieves the host external IP rather than localhost/127.0.0.1 so we have an IP that
# Impala will consider distinct from storage backends to force remote scheduling.
def get_external_ip():
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s.settimeout(0)
  # This address is used to get the networking stack to identify a return IP address.
  # Timeout=0 means it doesn't need to resolve.
  s.connect(('10.254.254.254', 1))
  return s.getsockname()[0]


def split_host_port(host_port):
  """Checks if the host name also contains a port and separates the two.
  Returns either (host, None) or (host, port). Detects if host is an ipv6 address
  like "[::]" and removes the brackets from it.
  """
  is_ipv6_address = host_port[0] == "["
  if is_ipv6_address:
    parts = host_port[1:].split("]")
    if len(parts) == 1 or not parts[1]:
      return (parts[0], None)
    return (parts[0], int(parts[1][1:]))
  else:
    parts = host_port.split(":")
    if len(parts) == 1:
      return (parts[0], None)
    return (parts[0], int(parts[1]))


def to_host_port(host, port):
  is_ipv6_address = ":" in host
  fmt = "[{0}]:{1}" if is_ipv6_address else "{0}:{1}"
  return fmt.format(host, port)


CERT_TO_CA_MAP = {
  "wildcard-cert.pem": "wildcardCA.pem",
  "wildcard-san-cert.pem": "wildcardCA.pem"
}

REQUIRED_MIN_OPENSSL_VERSION = 0x10001000
# Python supports TLSv1.2 from 2.7.9 officially but on Red Hat/CentOS Python2.7.5
# with newer python-libs (eg python-libs-2.7.5-77) supports TLSv1.2 already
if IS_REDHAT_DERIVATIVE:
  REQUIRED_MIN_PYTHON_VERSION_FOR_TLSV12 = (2, 7, 5)
else:
  REQUIRED_MIN_PYTHON_VERSION_FOR_TLSV12 = (2, 7, 9)
_openssl_version_number = getattr(ssl, "OPENSSL_VERSION_NUMBER", None)
if _openssl_version_number is None:
  SKIP_SSL_MSG = "Legacy OpenSSL module detected"
elif _openssl_version_number < REQUIRED_MIN_OPENSSL_VERSION:
  SKIP_SSL_MSG = "Only have OpenSSL version %X, but test requires %X" % (
    ssl.OPENSSL_VERSION_NUMBER, REQUIRED_MIN_OPENSSL_VERSION)
else:
  SKIP_SSL_MSG = None
