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

import json
import random
import re
import requests
import psutil
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestWebPage(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestWebPage, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--enable_extended_memory_metrics=true"
  )
  def test_varz_hidden_variables(self):
    """Tests that modified hidden variables show up in /varz"""
    response = requests.get("http://localhost:25000/varz?json")
    assert response.status_code == requests.codes.ok
    varz_json = json.loads(response.text)
    flag = [e for e in varz_json["flags"]
            if e["name"] == "enable_extended_memory_metrics"]
    assert len(flag) == 1
    assert flag[0]["default"] == "false"
    assert flag[0]["current"] == "true"
    assert flag[0]["experimental"]

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--webserver_max_post_length_bytes=100"
  )
  def test_max_post_length(self):
    """Tests that the maximum length of a POST request that will be accepted"""
    too_big_post_content = "c" * 10000
    # POST that exceeds the limit
    response = requests.post("http://localhost:25000/", too_big_post_content)
    assert response.status_code == requests.codes.request_entity_too_large

    # POST within the limit
    # This is on a URI that does not understand POST and treats it like a GET.
    ok_post_content = "c" * 100
    response = requests.post("http://localhost:25000/", ok_post_content)
    assert response.status_code == requests.codes.ok

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args()
  def test_webserver_interface(self):
    addrs = psutil.net_if_addrs()
    print("net_if_addrs returned: %s" % addrs)
    ip_matcher = re.compile("\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
    ip_addrs = []
    for addr in addrs:
      for snic in addrs[addr]:
        if ip_matcher.match(snic.address):
          ip_addrs.append(snic.address)

    # There must be at least one available interface on the machine.
    assert len(ip_addrs) > 0, addrs

    ports = ["25000", "25010", "25020"]
    # With default args, the webserver should be accessible over all interfaces for all
    # daemons.
    for ip in ip_addrs:
      for port in ports:
        response = requests.get("http://%s:%s/" % (ip, port))
        assert response.status_code == requests.codes.ok, ip

    # Pick a random interface and restart with the webserver on that interface.
    interface = random.choice(ip_addrs)
    self._start_impala_cluster(["--impalad_args=--webserver_interface=%s" % interface])

    # Now the webserver should only be accessible over the choosen interface.
    for ip in ip_addrs:
      try:
        response = requests.get("http://%s:25000/" % ip)
        assert ip == interface
        assert response.status_code == requests.codes.ok, ip
      except requests.exceptions.ConnectionError:
        assert ip != interface
