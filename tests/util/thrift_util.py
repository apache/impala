#!/usr/bin/env python
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

def create_transport(use_kerberos, host, port, service):
  """
  Create a new Transport based on the connection type.

  If not using kerberos, just return a simple buffered transport. For
  the kerberos, a sasl transport is created.
  """
  sock = TSocket(host, int(port))
  if not use_kerberos:
    return TBufferedTransport(sock)

  # Initializes a sasl client
  from shell.thrift_sasl import TSaslClientTransport
  def sasl_factory():
    try:
      import saslwrapper as sasl
    except ImportError:
      print 'saslwrapper not found, trying to import sasl'
      import sasl
    sasl_client = sasl.Client()
    sasl_client.setAttr("host", host)
    sasl_client.setAttr("service", service)
    sasl_client.init()
    return sasl_client
  # GSSASPI is the underlying mechanism used by kerberos to authenticate.
  return TSaslClientTransport(sasl_factory, "GSSAPI", sock)
