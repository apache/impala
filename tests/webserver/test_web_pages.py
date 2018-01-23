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

from tests.common.skip import SkipIf
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
import json
import requests

class TestWebPage(ImpalaTestSuite):

  GET_JAVA_LOGLEVEL_URL = "http://localhost:{0}/get_java_loglevel"
  SET_JAVA_LOGLEVEL_URL = "http://localhost:{0}/set_java_loglevel"
  RESET_JAVA_LOGLEVEL_URL = "http://localhost:{0}/reset_java_loglevel"
  SET_GLOG_LOGLEVEL_URL = "http://localhost:{0}/set_glog_level"
  RESET_GLOG_LOGLEVEL_URL = "http://localhost:{0}/reset_glog_level"
  CATALOG_URL = "http://localhost:{0}/catalog"
  CATALOG_OBJECT_URL = "http://localhost:{0}/catalog_object"
  TABLE_METRICS_URL = "http://localhost:{0}/table_metrics"
  QUERY_BACKENDS_URL = "http://localhost:{0}/query_backends"
  QUERY_FINSTANCES_URL = "http://localhost:{0}/query_finstances"
  RPCZ_URL = "http://localhost:{0}/rpcz"
  THREAD_GROUP_URL = "http://localhost:{0}/thread-group"
  # log4j changes do not apply to the statestore since it doesn't
  # have an embedded JVM. So we make two sets of ports to test the
  # log level endpoints, one without the statestore port and the
  # one with it.
  TEST_PORTS_WITHOUT_SS = ["25000", "25020"]
  TEST_PORTS_WITH_SS = ["25000", "25010", "25020"]
  CATALOG_TEST_PORT = ["25020"]

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

  def get_and_check_status(self, url, string_to_search = "", ports_to_test = None):
    """Helper method that polls a given url and asserts the return code is ok and
    the response contains the input string."""
    if ports_to_test is None:
      ports_to_test = self.TEST_PORTS_WITH_SS
    for port in ports_to_test:
      input_url = url.format(port)
      response = requests.get(input_url)
      assert response.status_code == requests.codes.ok\
          and string_to_search in response.text, "Offending url: " + input_url
    return response.text

  def get_debug_page(self, page_url):
    """Returns the content of the debug page 'page_url' as json."""
    response = self.get_and_check_status(page_url + "?json", ports_to_test=[25000])
    return json.loads(response)

  def get_and_check_status_jvm(self, url, string_to_search = ""):
    """Calls get_and_check_status() for impalad and catalogd only"""
    return self.get_and_check_status(url, string_to_search,
                                     ports_to_test=self.TEST_PORTS_WITHOUT_SS)

  def test_log_level(self):
    """Test that the /log_level page outputs are as expected and work well on basic and
    malformed inputs. This however does not test that the log level changes are actually
    in effect."""
    # Check that the log_level end points are accessible.
    self.get_and_check_status_jvm(self.GET_JAVA_LOGLEVEL_URL)
    self.get_and_check_status_jvm(self.SET_JAVA_LOGLEVEL_URL)
    self.get_and_check_status_jvm(self.RESET_JAVA_LOGLEVEL_URL)
    self.get_and_check_status(self.SET_GLOG_LOGLEVEL_URL)
    self.get_and_check_status(self.RESET_GLOG_LOGLEVEL_URL)
    # Try getting log level of a class.
    get_loglevel_url = (self.GET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsTable")
    self.get_and_check_status_jvm(get_loglevel_url, "DEBUG")

    # Set the log level of a class to TRACE and confirm the setting is in place
    set_loglevel_url = (self.SET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsTable&level=trace")
    self.get_and_check_status_jvm(set_loglevel_url, "Effective log level: TRACE")

    get_loglevel_url = (self.GET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsTable")
    self.get_and_check_status_jvm(get_loglevel_url, "TRACE")
    # Check the log level of a different class and confirm it is still DEBUG
    get_loglevel_url = (self.GET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsPartition")
    self.get_and_check_status_jvm(get_loglevel_url, "DEBUG")

    # Reset Java logging levels and check the logging level of the class again
    self.get_and_check_status_jvm(self.RESET_JAVA_LOGLEVEL_URL, "Java log levels reset.")
    get_loglevel_url = (self.GET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsTable")
    self.get_and_check_status_jvm(get_loglevel_url, "DEBUG")

    # Set a new glog level and make sure the setting has been applied.
    set_glog_url = (self.SET_GLOG_LOGLEVEL_URL + "?glog=3")
    self.get_and_check_status(set_glog_url, "v set to 3")

    # Try resetting the glog logging defaults again.
    self.get_and_check_status( self.RESET_GLOG_LOGLEVEL_URL, "v set to ")

    # Try to get the log level of an empty class input
    get_loglevel_url = (self.GET_JAVA_LOGLEVEL_URL + "?class=")
    self.get_and_check_status_jvm(get_loglevel_url)

    # Same as above, for set log level request
    set_loglevel_url = (self.SET_JAVA_LOGLEVEL_URL + "?class=")
    self.get_and_check_status_jvm(get_loglevel_url)

    # Empty input for setting a glog level request
    set_glog_url = (self.SET_GLOG_LOGLEVEL_URL + "?glog=")
    self.get_and_check_status(set_glog_url)

    # Try setting a non-existent log level on a valid class. In such cases,
    # log4j automatically sets it as DEBUG. This is the behavior of
    # Level.toLevel() method.
    set_loglevel_url = (self.SET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsTable&level=foo&")
    self.get_and_check_status_jvm(set_loglevel_url, "Effective log level: DEBUG")

    # Try setting an invalid glog level.
    set_glog_url = self.SET_GLOG_LOGLEVEL_URL + "?glog=foo"
    self.get_and_check_status(set_glog_url, "Bad glog level input")

    # Try a non-existent endpoint on log_level URL.
    bad_loglevel_url = self.SET_GLOG_LOGLEVEL_URL + "?badurl=foo"
    self.get_and_check_status(bad_loglevel_url)

  def test_catalog(self):
    """Tests the /catalog and /catalog_object endpoints."""
    self.get_and_check_status_jvm(self.CATALOG_URL, "functional")
    self.get_and_check_status_jvm(self.CATALOG_URL, "alltypes")
    # IMPALA-5028: Test toThrift() of a partitioned table via the WebUI code path.
    self.__test_catalog_object("functional", "alltypes")
    self.__test_catalog_object("functional_parquet", "alltypes")
    self.__test_catalog_object("functional", "alltypesnopart")
    self.__test_catalog_object("functional_kudu", "alltypes")
    self.__test_table_metrics("functional", "alltypes", "total-file-size-bytes")
    self.__test_table_metrics("functional_kudu", "alltypes", "alter-duration")

  def __test_catalog_object(self, db_name, tbl_name):
    """Tests the /catalog_object endpoint for the given db/table. Runs
    against an unloaded as well as a loaded table."""
    self.client.execute("invalidate metadata %s.%s" % (db_name, tbl_name))
    self.get_and_check_status(self.CATALOG_OBJECT_URL +
      "?object_type=TABLE&object_name=%s.%s" % (db_name, tbl_name), tbl_name,
      ports_to_test=self.TEST_PORTS_WITHOUT_SS)
    self.client.execute("select count(*) from %s.%s" % (db_name, tbl_name))
    self.get_and_check_status(self.CATALOG_OBJECT_URL +
      "?object_type=TABLE&object_name=%s.%s" % (db_name, tbl_name), tbl_name,
      ports_to_test=self.TEST_PORTS_WITHOUT_SS)

  def __test_table_metrics(self, db_name, tbl_name, metric):
    self.client.execute("refresh %s.%s" % (db_name, tbl_name))
    self.get_and_check_status(self.TABLE_METRICS_URL +
      "?name=%s.%s" % (db_name, tbl_name), metric, ports_to_test=self.CATALOG_TEST_PORT)

  def __run_query_and_get_debug_page(self, query, page_url):
    """Runs a query to obtain the content of the debug page pointed to by page_url, then
    cancels the query."""
    query_handle =  self.client.execute_async(query)
    response_json = ""
    try:
      response = self.get_and_check_status(
        page_url + "?query_id=%s&json" % query_handle.get_handle().id,
        ports_to_test=[25000])
      response_json = json.loads(response)
    finally:
      self.client.cancel(query_handle)
    return response_json

  def test_backend_states(self, unique_database):
    """Test that /query_backends returns the list of backend states for DML or queries;
    nothing for DDL statements"""
    CROSS_JOIN = ("select count(*) from functional.alltypes a "
                  "CROSS JOIN functional.alltypes b CROSS JOIN functional.alltypes c")
    for q in [CROSS_JOIN,
              "CREATE TABLE {0}.foo AS {1}".format(unique_database, CROSS_JOIN),
              "DESCRIBE functional.alltypes"]:
      response_json = self.__run_query_and_get_debug_page(q, self.QUERY_BACKENDS_URL)

      if "DESCRIBE" not in q:
        assert len(response_json['backend_states']) > 0
      else:
        assert 'backend_states' not in response_json

  def test_backend_instances(self, unique_database):
    """Test that /query_finstances returns the list of fragment instances for DML or
    queries; nothing for DDL statements"""
    CROSS_JOIN = ("select count(*) from functional.alltypes a "
                  "CROSS JOIN functional.alltypes b CROSS JOIN functional.alltypes c")
    for q in [CROSS_JOIN,
              "CREATE TABLE {0}.foo AS {1}".format(unique_database, CROSS_JOIN),
              "DESCRIBE functional.alltypes"]:
      response_json = self.__run_query_and_get_debug_page(q, self.QUERY_FINSTANCES_URL)

      if "DESCRIBE" not in q:
        assert len(response_json['backend_instances']) > 0
      else:
        assert 'backend_instances' not in response_json

  def test_io_mgr_threads(self):
    """Test that IoMgr threads have readable names. This test assumed that all systems we
    support have a disk called 'sda'."""
    response = self.get_and_check_status(
        self.THREAD_GROUP_URL + "?group=disk-io-mgr&json", ports_to_test=[25000])
    response_json = json.loads(response)
    thread_names = [t["name"] for t in response_json['threads']]
    expected_name_patterns = ["ADLS remote", "S3 remote", "HDFS remote", "sda"]
    for pattern in expected_name_patterns:
      assert any(pattern in t for t in thread_names), \
           "Could not find thread matching '%s'" % pattern

  @SkipIf.not_krpc
  def test_krpc_rpcz(self):
    """Test that KRPC metrics are exposed in /rpcz and that they are updated when
    executing a query."""
    TEST_QUERY = "select count(c2.string_col) from \
        functional.alltypestiny join functional.alltypessmall c2"
    SVC_NAME = 'impala.DataStreamService'

    def get_svc_metrics(svc_name):
      rpcz = self.get_debug_page(self.RPCZ_URL)
      assert len(rpcz['services']) > 0
      for s in rpcz['services']:
        if s['service_name'] == svc_name:
          assert len(s['rpc_method_metrics']) > 0, '%s metrics are empty' % svc_name
          return sorted(s['rpc_method_metrics'], key=lambda m: m['method_name'])
      assert False, 'Could not find metrics for %s' % svc_name

    before = get_svc_metrics(SVC_NAME)
    self.client.execute(TEST_QUERY)
    after = get_svc_metrics(SVC_NAME)

    assert before != after
