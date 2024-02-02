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
import json
import random
import re
import requests
import psutil
import pytest
import time

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import (
  DEFAULT_CLUSTER_SIZE,
  CustomClusterTestSuite)
from tests.common.skip import SkipIfFS
from tests.shell.util import run_impala_shell_cmd

SMALL_QUERY_LOG_SIZE_IN_BYTES = 40 * 1024


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
    ip_matcher = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
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
    assert '"resource_pool": "default-pool"' in response_json

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
    assert '"resource_pool": "default-pool"' in response_json

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      cluster_size=1,
      impalad_args="--query_log_size_in_bytes=" + str(SMALL_QUERY_LOG_SIZE_IN_BYTES)
  )
  def test_query_log_size_in_bytes(self):
    """Check if query list is limited by query_log_size_in_bytes flag."""
    # This simple test query will produce ~8520 bytes of QueryStateRecord.
    query = "select version()"
    num_queries = 10
    for i in range(0, num_queries):
      self.execute_query_expect_success(self.client, query)

    # Retrieve and verify the total size metrics.
    metric_key = "impala-server.query-log-est-total-bytes"
    metric_value = self.cluster.impalads[0].service.get_metric_value(metric_key)
    assert metric_value > 0
    assert metric_value <= SMALL_QUERY_LOG_SIZE_IN_BYTES

    # Verify that the query page only contains a subset of the test queries.
    queries_response = requests.get("http://localhost:25000/queries?json")
    queries_json = json.loads(queries_response.text)
    assert len(queries_json["completed_queries"]) > 0
    assert len(queries_json["completed_queries"]) < num_queries

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
        "Query state can be monitored at: "]
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
        "/events",
        "/hadoop-varz",
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

  @staticmethod
  def _get_inflight_catalog_operations():
    response = requests.get("http://localhost:25020/operations?json")
    assert response.status_code == requests.codes.ok
    operations = json.loads(response.text)
    assert "inflight_catalog_operations" in operations
    return operations["inflight_catalog_operations"]

  @staticmethod
  def _get_finished_catalog_operations():
    response = requests.get("http://localhost:25020/operations?json")
    assert response.status_code == requests.codes.ok
    operations = json.loads(response.text)
    assert "finished_catalog_operations" in operations
    return operations["finished_catalog_operations"]

  @CustomClusterTestSuite.with_args(catalogd_args="--catalog_operation_log_size=2")
  def test_catalog_operations_limit(self, unique_database):
    tbl = unique_database + ".tbl"
    self.execute_query("create table {0}_1 (id int)".format(tbl))
    self.execute_query("create table {0}_2 (id int)".format(tbl))
    self.execute_query("create table {0}_3 (id int)".format(tbl))
    self.execute_query("drop table {0}_1".format(tbl))
    finished_operations = self._get_finished_catalog_operations()
    # Verify only 2 operations are shown
    assert len(finished_operations) == 2
    op = finished_operations[0]
    assert op["status"] == "FINISHED"
    assert op["catalog_op_name"] == "DROP_TABLE"
    assert op["target_name"] == tbl + "_1"
    op = finished_operations[1]
    assert op["status"] == "FINISHED"
    assert op["catalog_op_name"] == "CREATE_TABLE"
    assert op["target_name"] == tbl + "_3"

  @CustomClusterTestSuite.with_args(catalogd_args="--catalog_operation_log_size=-1")
  def test_catalog_operations_negative_limit(self, unique_database):
    # Test negative limit on catalog_operation_log_size. The limit is converted to be
    # Integer.MAX_VALUE. Run hundreds of commands and see whether they are all in the
    # operation log.
    tbl = unique_database + ".tbl"
    self.execute_query("create table {0} (id int)".format(tbl))
    num = 500
    for i in range(num):
      self.execute_query("invalidate metadata " + tbl)
    finished_operations = self._get_finished_catalog_operations()
    # Verify all operations are in the history. There are one DROP_DATABASE, one
    # CREATE_DATABASE, one CREATE_TABLE and 'num' INVALIDATEs in the list.
    assert len(finished_operations) == 3 + num
    for i in range(num):
      op = finished_operations[i]
      assert op["status"] == "FINISHED"
      assert op["catalog_op_name"] == "INVALIDATE_METADATA"
      assert op["target_name"] == tbl
    op = finished_operations[-3]
    assert op["status"] == "FINISHED"
    assert op["catalog_op_name"] == "CREATE_TABLE"
    assert op["target_name"] == tbl
    op = finished_operations[-2]
    assert op["status"] == "FINISHED"
    assert op["catalog_op_name"] == "CREATE_DATABASE"
    assert op["target_name"] == unique_database
    op = finished_operations[-1]
    assert op["status"] == "FINISHED"
    assert op["catalog_op_name"] == "DROP_DATABASE"
    assert op["target_name"] == unique_database

  @CustomClusterTestSuite.with_args(
    impalad_args="--catalog_client_rpc_timeout_ms=100 "
                 "--catalog_client_rpc_retry_interval_ms=10 "
                 "--catalog_client_connection_num_retries=2")
  def test_catalog_operations_with_rpc_retry(self):
    """Test that catalog RPC retries are all shown in the /operations page"""
    # Run a DESCRIBE to ensure the table is loaded. So the first RPC attempt will
    # time out in its real work. This triggers a PrioritizeLoad RPC which usually
    # finishes in 40ms. So 100ms for catalog RPC timeout is enough.
    self.execute_query("describe functional.alltypes")
    try:
      # This runs around 600ms with the debug action so the catalog RPC will timeout.
      self.execute_query("refresh functional.alltypes", {
        "debug_action": "catalogd_refresh_hdfs_listing_delay:SLEEP@100"
      })
    except ImpalaBeeswaxException as e:
      assert "RPC recv timed out" in str(e)
    # In impalad side, the query fails by the above error. However, in catalogd side,
    # the RPCs are still running. Check the in-flight operations.
    inflight_operations = self._get_inflight_catalog_operations()
    assert len(inflight_operations) == 2
    for op in inflight_operations:
      assert op["status"] == "STARTED"
      assert op["catalog_op_name"] == "REFRESH"
      assert op["target_name"] == "functional.alltypes"
    assert inflight_operations[0]["query_id"] == inflight_operations[1]["query_id"]
    assert inflight_operations[0]["thread_id"] != inflight_operations[1]["thread_id"]

    # Wait until the catalog operations finish
    while len(self._get_inflight_catalog_operations()) != 0:
      time.sleep(1)

    # Verify both RPC attempts are shown as finished operations.
    finished_operations = self._get_finished_catalog_operations()
    assert len(finished_operations) == 2
    for op in finished_operations:
      assert op["status"] == "FINISHED"
      assert op["catalog_op_name"] == "REFRESH"
      assert op["target_name"] == "functional.alltypes"
    assert finished_operations[0]["query_id"] == finished_operations[1]["query_id"]
    assert finished_operations[0]["thread_id"] != finished_operations[1]["thread_id"]

  def _verify_topic_size_metrics(self):
    # Calculate the total topic metrics from the /topics page
    response = requests.get("http://localhost:25010/topics?json")
    assert response.status_code == requests.codes.ok
    topics_json = json.loads(response.text)
    total_key_size = 0
    total_value_size = 0
    total_topic_size = 0
    for topic in topics_json["topics"]:
      total_key_size += topic["key_size_bytes"]
      total_value_size += topic["value_size_bytes"]
      total_topic_size += topic["total_size_bytes"]

    # Retrieve and verify the total topic metrics from the /metrics page
    response = requests.get("http://localhost:25010/metrics?json")
    assert response.status_code == requests.codes.ok
    metrics_json = json.loads(response.text)["metric_group"]["metrics"]
    for metric in metrics_json:
      if metric["name"] == "statestore.total-key-size-bytes":
        assert metric["value"] == total_key_size
      elif metric["name"] == "statestore.total-value-size-bytes":
        assert metric["value"] == total_value_size
      elif metric["name"] == "statestore.total-topic-size-bytes":
        assert metric["value"] == total_topic_size

  @CustomClusterTestSuite.with_args()
  def test_transient_topic_size(self):
    self._verify_topic_size_metrics()
    # Kill an impalad and wait until it's removed
    killed_impalad = self.cluster.impalads[2]
    killed_impalad.kill()
    # Before we kill an impalad, there are DEFAULT_CLUSTER_SIZE + 1 subscribers
    # (DEFAULT_CLUSTER_SIZE impalads and 1 for catalogd). After we kill one impalad,
    # there should be DEFAULT_CLUSTER_SIZE subscribers.
    self.cluster.statestored.service.wait_for_live_subscribers(DEFAULT_CLUSTER_SIZE)
    # Verify the topic metrics again
    self._verify_topic_size_metrics()

  @SkipIfFS.hive
  @CustomClusterTestSuite.with_args(
    catalogd_args="--hms_event_polling_interval_s=1 "
                  "--debug_actions=catalogd_event_processing_delay:SLEEP@2000")
  def test_event_processor_status(self, unique_database):
    """Verify the /events page by using a long delay in event processing."""
    self.execute_query("create table {}.part (i int) partitioned by (p int)".format(
        unique_database))
    insert_stmt = "insert into {}.part partition(p) select id, month from "\
        "functional.alltypes".format(unique_database)
    self.execute_query(insert_stmt)
    # Run the same INSERT statement in Hive to get non-self events.
    self.run_stmt_in_hive("set hive.exec.dynamic.partition.mode=nonstrict;" + insert_stmt)
    page = requests.get("http://localhost:25020/events").text
    # Wait until the batched events are being processed
    while "a batch of" not in page:
      time.sleep(1)
      page = requests.get("http://localhost:25020/events").text
    expected_lines = [
      "Lag Info", "Lag time:", "Current Event Batch", "Metastore Event Batch:",
      "Event ID starts from", "Event time starts from",
      "Started processing the current batch at",
      "Started processing the current event at",
      "Current Metastore event being processed",
      "(a batch of ", " events on the same table)",
    ]
    for expected in expected_lines:
      assert expected in page, "Missing '%s' in events page:\n%s" % (expected, page)

  @SkipIfFS.hive
  @CustomClusterTestSuite.with_args(
    catalogd_args="--hms_event_polling_interval_s=1 "
                  "--invalidate_metadata_on_event_processing_failure=false "
                  "--inject_process_event_failure_event_types=CREATE_TABLE")
  def test_event_processor_error_message(self, unique_database):
    """Verify the /events page show the error of event processing"""
    self.run_stmt_in_hive("create table {}.tbl(i int)".format(unique_database))
    # Wait enough time for the event to be processed
    time.sleep(2)
    page = requests.get("http://localhost:25020/events").text
    expected_lines = [
      "Unexpected exception received while processing event",
      "Event id:", "Event Type: CREATE_TABLE", "Event message:",
    ]
    for expected in expected_lines:
      assert expected in page, "Missing '%s' in events page:\n%s" % (expected, page)

    # Verify the latest event id still get updated
    json_res = json.loads(requests.get("http://localhost:25020/events?json").text)
    old_latest_event_id = json_res["progress-info"]["latest_event_id"]
    # Generate new events
    self.run_stmt_in_hive("create table {}.tbl2(i int)".format(unique_database))
    # Wait enough time for the event to be polled
    time.sleep(2)
    json_res = json.loads(requests.get("http://localhost:25020/events?json").text)
    new_latest_event_id = json_res["progress-info"]["latest_event_id"]
    assert new_latest_event_id > old_latest_event_id
    # Current event (the failed one) should not be cleared
    assert "current_event" in json_res["progress-info"]

    # Verify the error message disappears after a global INVALIDATE METADATA
    self.execute_query("invalidate metadata")
    page = requests.get("http://localhost:25020/events").text
    assert "Unexpected exception" not in page, "Still see error message:\n" + page
