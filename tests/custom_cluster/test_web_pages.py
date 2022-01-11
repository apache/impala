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
from tests.shell.util import run_impala_shell_cmd


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

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--query_stmt_size=0"
  )
  def test_query_stmt_without_truncate(self):
    """Check if the full query string is displayed in the query list on the WebUI."""
    # The input query is a select + 450 'x ' long.
    query_select = "x " * 450
    query = 'select "{0}"'.format(query_select)
    # In the site there is an extra \ before the " so we need that in the expected
    # response too.
    expected = 'select \\"{0}\\"'.format(query_select)
    self.execute_query(query)
    response = requests.get("http://localhost:25000/queries?json")
    response_json = response.text
    assert expected in response_json, "No matching statement found in the queries site."

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--query_stmt_size=10"
  )
  def test_query_stmt_with_custom_length(self):
    """Check if the partial query with the correct length is displayed in the query list
    on the WebUI."""
    # The input query is a select + 450 'x ' long.
    query = 'select "{0}"'.format("x " * 450)
    # Searching for the custom, 10 chars long response. In the site there is an extra \
    # before the " so we need that in the expected response too.
    expected = 'select \\"x ...'
    self.execute_query(query)
    response = requests.get("http://localhost:25000/queries?json")
    response_json = response.text
    assert expected in response_json, "No matching statement found in the queries site."

  # Checks if 'messages' exists/does not exist in 'result_stderr' based on the value of
  # 'should_exist'
  def _validate_shell_messages(self, result_stderr, messages, should_exist=True):
    for msg in messages:
      if should_exist:
        assert msg in result_stderr, result_stderr
      else:
        assert msg not in result_stderr, result_stderr

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--ping_expose_webserver_url=false"
  )
  def test_webserver_url_not_exposed(self, vector):
    if vector.get_value('table_format').file_format != 'text':
      pytest.skip('runs only for text table_format')
    # If webserver url is not exposed, debug web urls shouldn't be printed out.
    shell_messages = ["Query submitted at: ", "(Coordinator: ",
        "Query progress can be monitored at: "]
    query_shell_arg = '--query=select * from functional.alltypes'
    # hs2
    results = run_impala_shell_cmd(vector, [query_shell_arg])
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=False)
    # beeswax
    results = run_impala_shell_cmd(vector, ['--protocol=beeswax', query_shell_arg])
    self._validate_shell_messages(results.stderr, shell_messages, should_exist=False)
    # Even though webserver url is not exposed, it is still accessible.
    page = requests.get('http://localhost:25000')
    assert page.status_code == requests.codes.ok

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--logtostderr=true --redirect_stdout_stderr=false",
    statestored_args="--logtostderr=true --redirect_stdout_stderr=false",
    catalogd_args="--logtostderr=true --redirect_stdout_stderr=false"
  )
  def test_webserver_hide_logs_link(self, vector):
    """Validate that there is no /logs link when we use --logtostderr=true """
    ports = ["25000", "25010", "25020"]
    for port in ports:
      # Get the webui home page as json.
      response = requests.get("http://localhost:%s?json" % port)
      assert response.status_code == requests.codes.ok
      home = json.loads(response.text)
      # Get the items in the navbar.
      navbar = home["__common__"]['navbar']
      found_links = [link_item['link'] for link_item in navbar]
      # The links that we expect to see in the navbar.
      expected_coordinator_links = [
        "/",
        "/admission",
        "/backends",
        "/catalog",
        "/hadoop-varz",
        "/jmx",
        "/log_level",
        "/memz",
        "/metrics",
        "/profile_docs",
        "/queries",
        "/rpcz",
        "/sessions",
        "/threadz",
        "/varz",
      ]
      expected_statestore_links = [
        "/",
        "/log_level",
        "/memz",
        "/metrics",
        "/profile_docs",
        "/rpcz",
        "/subscribers",
        "/threadz",
        "/topics",
        "/varz",
      ]
      expected_catalog_links = [
        "/",
        "/catalog",
        "/jmx",
        "/log_level",
        "/memz",
        "/metrics",
        "/operations",
        "/profile_docs",
        "/rpcz",
        "/threadz",
        "/varz",
      ]
      msg = "bad links from webui port %s" % port
      if port == "25000":
        assert found_links == expected_coordinator_links, msg
      elif port == "25010":
        assert found_links == expected_statestore_links, msg
      elif port == "25020":
        assert found_links == expected_catalog_links, msg

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--disable_content_security_policy_header=true",
    statestored_args="--disable_content_security_policy_header=true",
    catalogd_args="--disable_content_security_policy_header=true"
  )
  def test_cdp_header_disabled(self):
    """Test that if servers are started with the flag
    --disable_content_security_policy_header=true then the emission of the CDP header is
    disabled."""
    ports = ["25000", "25010", "25020"]  # Respectively the impalad, statestore, catalog.
    for port in ports:
      response = requests.get("http://localhost:%s" % port)
      assert 'Content-Security-Policy' not in response.headers, \
        "CSP header present despite being disabled (port %s)" % port
