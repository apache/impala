# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Thrift utility functions

from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
import getpass
import sasl

def create_transport(host, port, service, transport_type="buffered", user=None,
                     password=None, use_ssl=False, ssl_cert=None):
  """
  Create a new Thrift Transport based on the requested type.
  Supported transport types:
  - buffered, returns simple buffered transport
  - plain_sasl, return a SASL transport with the PLAIN mechanism
  - kerberos, return a SASL transport with the GSSAPI mechanism

  If use_ssl is True, the connection will use SSL, optionally using the file at ssl_cert
  as the CA cert.
  """
  port = int(port)
  if use_ssl:
    from thrift.transport import TSSLSocket
    if ssl_cert is None:
      sock = TSSLSocket.TSSLSocket(host, port, validate=False)
    else:
      sock = TSSLSocket.TSSLSocket(host, port, validate=True, ca_certs=ssl_cert)
  else:
    sock = TSocket(host, port)
  if transport_type.lower() == "buffered":
    return TBufferedTransport(sock)

  # Set defaults for LDAP connections
  if transport_type.lower() == "plain_sasl":
    if user is None: user = getpass.getuser()
    if password is None: password = ""

  # Initializes a sasl client
  from shell.thrift_sasl import TSaslClientTransport
  def sasl_factory():
    sasl_client = sasl.Client()
    sasl_client.setAttr("host", host)
    sasl_client.setAttr("service", service)
    if transport_type.lower() == "plain_sasl":
      sasl_client.setAttr("username", user)
      sasl_client.setAttr("password", password)
    sasl_client.init()
    return sasl_client
  if transport_type.lower() == "plain_sasl":
    return TSaslClientTransport(sasl_factory, "PLAIN", sock)
  else:
    # GSSASPI is the underlying mechanism used by kerberos to authenticate.
    return TSaslClientTransport(sasl_factory, "GSSAPI", sock)
