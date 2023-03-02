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


# Retrieves the host external IP rather than localhost/127.0.0.1 so we have an IP that
# Impala will consider distinct from storage backends to force remote scheduling.
def get_external_ip():
  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s.settimeout(0)
  # This address is used to get the networking stack to identify a return IP address.
  # Timeout=0 means it doesn't need to resolve.
  s.connect(('10.254.254.254', 1))
  return s.getsockname()[0]
