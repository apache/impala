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

import sys


def get_kerb_host_from_kerberos_host_fqdn(kerberos_host_fqdn):
  kerb_host = kerberos_host_fqdn.split(':')[0]
  if sys.version_info.major < 3:
    # sasl does not accept unicode strings, explicitly encode the string into ascii.
    kerb_host = kerb_host.encode('ascii', 'ignore')
  return kerb_host
