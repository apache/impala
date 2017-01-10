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

from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
import requests

class TestWebPage(ImpalaTestSuite):
  def test_memz(self):
    """test /memz at impalad / statestored / catalogd"""

    page = requests.get("http://localhost:25000/memz")
    assert page.status_code == requests.codes.ok
    page = requests.get("http://localhost:25010/memz")
    assert page.status_code == requests.codes.ok
    page = requests.get("http://localhost:25020/memz")
    assert page.status_code == requests.codes.ok

  def test_query_profile_encoded_unknown_query_id(self):
    """Test that /query_profile_encoded error message starts with the expected line in
    case of missing query and does not contain any leading whitespace.
    """
    cluster = ImpalaCluster()
    impalad = cluster.get_any_impalad()
    result = impalad.service.read_debug_webpage("query_profile_encoded?query_id=123")
    assert result.startswith("Could not obtain runtime profile: Query id")


