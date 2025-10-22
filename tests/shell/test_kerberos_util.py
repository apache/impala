# -*- coding: utf-8 -*-
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

from __future__ import absolute_import, division, print_function

from impala_shell.kerberos_util import get_kerb_host_from_kerberos_host_fqdn
from tests.common.base_test_suite import BaseTestSuite


class TestKerberosUtil(BaseTestSuite):
  def test_get_kerb_host_from_kerberos_host_fqdn(self):
    assert isinstance(get_kerb_host_from_kerberos_host_fqdn("any.host:1234"), str)
    assert get_kerb_host_from_kerberos_host_fqdn("any.host:1234") == "any.host"
    assert get_kerb_host_from_kerberos_host_fqdn("any.host") == "any.host"
