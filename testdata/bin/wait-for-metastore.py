#!/usr/bin/env impala-python
#
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
#
# This script waits for the Hive Metastore service to become available by attempting
# to execute the get_database("default") Thrift RPC until the call succeeds,
# or a timeout is reached.

from __future__ import absolute_import, division, print_function
import os
import time
from optparse import OptionParser
from tests.util.thrift_util import create_transport

# Imports required for Hive Metastore Client
from hive_metastore import ThriftHiveMetastore
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

# Disable buffering to get "print" output on the console immediately.
os.environ["PYTHONUNBUFFERED"] = "1"

parser = OptionParser()
parser.add_option("--metastore_hostport", dest="metastore_hostport",
                  default="localhost:9083", help="Metastore hostport to wait for.")
parser.add_option("--transport", dest="transport", default="buffered",
                  help="Transport to use for connecting to HiveServer2. Valid values: "
                  "'buffered', 'kerberos', 'plain_sasl'.")
options, args = parser.parse_args()

metastore_host, metastore_port = options.metastore_hostport.split(':')
hive_transport = create_transport(metastore_host, metastore_port, "hive",
                                  options.transport)
protocol = TBinaryProtocol.TBinaryProtocol(hive_transport)
hive_client = ThriftHiveMetastore.Client(protocol)

# Try to connect to the Hive metastore
now = time.time()
TIMEOUT_SECONDS = 300.0
while time.time() - now < TIMEOUT_SECONDS:
  try:
    hive_transport.open()
    resp = hive_client.get_database("default")
    if resp is not None:
      print("Metastore service is up at %s." % options.metastore_hostport)
      exit(0)
  except Exception as e:
    if "SASL" in str(e):  # Bail out on SASL failures
      print("SASL failure when attempting connection:")
      raise
    if "GSS" in str(e):   # Other GSSAPI failures
      print("GSS failure when attempting connection:")
      raise
    print("Waiting for the Metastore at %s..." % options.metastore_hostport)
  finally:
    hive_transport.close()
    time.sleep(0.5)

print("Metastore service failed to start within %s seconds." % TIMEOUT_SECONDS)
exit(1)
