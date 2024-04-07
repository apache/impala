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
from tests.common.environ import ImpalaTestClusterFlagsDetector
from tests.common.file_utils import grep_dir
from tests.common.skip import SkipIfBuildType, SkipIfDockerizedCluster
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.parse_util import parse_duration_string_ms
from datetime import datetime
from multiprocessing import Process, Queue
from time import sleep, time
import itertools
import json
import logging
import os
import pytest
import re
import requests

LOG = logging.getLogger(__name__)


class TestWebPage(ImpalaTestSuite):
  ROOT_URL = "http://localhost:{0}/"
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
  MEMZ_URL = "http://localhost:{0}/memz"
  METRICS_URL = "http://localhost:{0}/metrics"
  JMX_URL = "http://localhost:{0}/jmx"
  ADMISSION_URL = "http://localhost:{0}/admission"
  RESET_RESOURCE_POOL_STATS_URL = "http://localhost:{0}/resource_pool_reset"
  BACKENDS_URL = "http://localhost:{0}/backends"
  PROMETHEUS_METRICS_URL = "http://localhost:{0}/metrics_prometheus"
  QUERIES_URL = "http://localhost:{0}/queries"
  HEALTHZ_URL = "http://localhost:{0}/healthz"
  EVENT_PROCESSOR_URL = "http://localhost:{0}/events"
  HADOOP_VARZ_URL = "http://localhost:{0}/hadoop-varz"

  # log4j changes do not apply to the statestore since it doesn't
  # have an embedded JVM. So we make two sets of ports to test the
  # log level endpoints, one without the statestore port and the
  # one with it.
  TEST_PORTS_WITHOUT_SS = ["25000", "25020"]
  TEST_PORTS_WITH_SS = ["25000", "25010", "25020"]
  IMPALAD_TEST_PORT = ["25000"]
  CATALOG_TEST_PORT = ["25020"]

  def test_get_root_url(self):
    """Tests that the root URL is accessible and loads properly"""
    self.get_and_check_status(self.ROOT_URL)

  def test_get_build_flags(self):
    """Tests that the build flags on the root page contain valid values"""
    for port in self.TEST_PORTS_WITH_SS:
      build_flags = ImpalaTestClusterFlagsDetector.\
          get_build_flags_from_web_ui(self.ROOT_URL.format(port))

      assert len(build_flags) == 3
      assert "is_ndebug" in build_flags
      assert build_flags["is_ndebug"] in ["true", "false"]
      assert "cmake_build_type" in build_flags
      assert build_flags["cmake_build_type"] in ["debug", "release", "address_sanitizer",
          "tidy", "ubsan", "ubsan_full", "tsan", "tsan_full", "code_coverage_release",
          "code_coverage_debug", "debug_noopt"]
      assert "library_link_type" in build_flags
      assert build_flags["library_link_type"] in ["dynamic", "static"]

  @SkipIfBuildType.remote
  def test_root_correct_build_flags(self, cluster_properties):
    """Tests that the build flags on the root page contain correct values"""
    assert not cluster_properties.is_remote_cluster()
    for port in self.TEST_PORTS_WITH_SS:
      build_flags = ImpalaTestClusterFlagsDetector.\
          get_build_flags_from_web_ui(self.ROOT_URL.format(port))

      assert build_flags["cmake_build_type"] == cluster_properties.build_flavor
      assert build_flags["library_link_type"] == cluster_properties.library_link_type

  def test_root_consistent_build_flags(self):
    """Tests that the build flags on the root page contain consistent values"""
    for port in self.TEST_PORTS_WITH_SS:
      build_flags = ImpalaTestClusterFlagsDetector.\
          get_build_flags_from_web_ui(self.ROOT_URL.format(port))

      is_ndebug = build_flags["is_ndebug"] == "true"

      if not is_ndebug:
        assert not build_flags["cmake_build_type"] in ["release"]

      if build_flags["cmake_build_type"] in ["debug"]:
        assert not is_ndebug

  def test_root_other_info(self):
    """Tests to check glibc version and locale is available"""
    for port in self.TEST_PORTS_WITH_SS:
      other_info_page = requests.get(self.ROOT_URL.format(port) + "/?json").text
      other_info = json.loads(other_info_page)
      assert "effective_locale" in other_info
      assert "glibc_version" in other_info

  def test_memz(self):
    """Tests /memz at impalad / statestored / catalogd"""

    page = requests.get("http://localhost:25000/memz")
    assert page.status_code == requests.codes.ok
    page = requests.get("http://localhost:25010/memz")
    assert page.status_code == requests.codes.ok
    page = requests.get("http://localhost:25020/memz")
    assert page.status_code == requests.codes.ok
    page = requests.head("http://localhost:25000/memz")
    assert page.status_code == requests.codes.ok
    page = requests.head("http://localhost:25010/memz")
    assert page.status_code == requests.codes.ok
    page = requests.head("http://localhost:25020/memz")
    assert page.status_code == requests.codes.ok

  def test_memz_shows_fragment_instance_id(self):
    """Tests that the memory breakdown on memz shows fragment instance IDs."""
    query = "select count(*) from functional_parquet.alltypes where bool_col = sleep(100)"
    query_handle = self.client.execute_async(query)
    try:
      self.wait_for_state(query_handle, self.client.QUERY_STATES['RUNNING'], 1000)
      memz_breakdown = self.get_debug_page(self.MEMZ_URL)['detailed']
      finstance_re = re.compile("Fragment [0-9a-f]{16}:[0-9a-f]{16}")
      assert finstance_re.search(memz_breakdown), memz_breakdown
    finally:
      self.client.close_query(query_handle)

  def test_query_profile_encoded_unknown_query_id(self):
    """Test that /query_profile_encoded error message starts with the expected line in
    case of missing query and does not contain any leading whitespace.
    """
    cluster = ImpalaCluster.get_e2e_test_cluster()
    impalad = cluster.get_any_impalad()
    result = impalad.service.read_debug_webpage("query_profile_encoded?query_id=123")
    assert result.startswith("Could not obtain runtime profile: Query id")

  def test_jmx_endpoint(self):
    """Tests that the /jmx endpoint on the Catalog and Impalads returns a valid json."""
    for port in self.TEST_PORTS_WITHOUT_SS:
      input_url = self.JMX_URL.format(port)
      response = requests.get(input_url)
      assert response.status_code == requests.codes.ok
      assert "application/json" == response.headers['Content-Type']
      jmx_json = ""
      try:
       jmx_json = json.loads(response.text)
       assert "beans" in jmx_json.keys(), "Ill formatted JSON returned: %s" % jmx_json
      except ValueError:
        assert False, "Invalid JSON returned from /jmx endpoint: %s" % jmx_json

  def get_and_check_status(
      self, url, string_to_search="", ports_to_test=None, regex=False, headers=None):
    """Helper method that polls a given url and asserts the return code is ok and
    the response contains the input string."""
    if ports_to_test is None:
      ports_to_test = self.TEST_PORTS_WITH_SS

    responses = []
    for port in ports_to_test:
      input_url = url.format(port)
      response = requests.head(input_url, headers=headers)
      assert response.status_code == requests.codes.ok, "URL: {0} Str:'{1}'\nResp:{2}"\
        .format(input_url, string_to_search, response.text)
      response = requests.get(input_url, headers=headers)
      assert response.status_code == requests.codes.ok, "URL: {0} Str:'{1}'\nResp:{2}"\
        .format(input_url, string_to_search, response.text)
      if regex:
        assert re.search(string_to_search, response.text), "URL: {0} Str:'{1}'\nResp:{2}"\
          .format(input_url, string_to_search, response.text)
      else:
        assert string_to_search in response.text, "URL: {0} Str:'{1}'\nResp:{2}".format(
          input_url, string_to_search, response.text)
      responses.append(response)
      assert 'Content-Security-Policy' in response.headers, "CSP header missing"
    return responses

  def post_and_check_status(self, url, data={}, string_to_search="", ports_to_test=None):
    """Helper method that posts to a given url, then asserts the return code is ok and
    the response contains the expected string."""
    if ports_to_test is None:
      ports_to_test = self.TEST_PORTS_WITH_SS

    responses = []
    for port in ports_to_test:
      input_url = url.format(port)
      response = requests.head(input_url)
      assert response.status_code == requests.codes.ok, "URL: {0} Str:'{1}'\nResp:{2}"\
        .format(input_url, string_to_search, response.text)
      response = requests.post(input_url, data=data)
      assert response.status_code == requests.codes.ok, "URL: {0} Str:'{1}'\nResp:{2}"\
        .format(input_url, string_to_search, response.text)
      assert string_to_search in response.text, "URL: {0} Str:'{1}'\nResp:{2}".format(
        input_url, string_to_search, response.text)
      responses.append(response)
      assert 'Content-Security-Policy' in response.headers, "CSP header missing"
    return responses

  def get_debug_page(self, page_url, port=25000):
    """Returns the content of the debug page 'page_url' as json."""
    responses = self.get_and_check_status(page_url + "?json", ports_to_test=[port])
    assert len(responses) == 1
    assert "application/json" in responses[0].headers['Content-Type']
    return json.loads(responses[0].text)

  def get_and_check_status_jvm(self, url, string_to_search=""):
    """Calls get_and_check_status() for impalad and catalogd only"""
    return self.get_and_check_status(url, string_to_search,
                              ports_to_test=self.TEST_PORTS_WITHOUT_SS)

  def post_and_check_status_jvm(self, url, data={}, string_to_search=""):
    """Calls post_and_check_status() for impalad and catalogd only"""
    return self.post_and_check_status(url, data, string_to_search,
                              ports_to_test=self.TEST_PORTS_WITHOUT_SS)

  def test_content_type(self):
    """Checks that an appropriate content-type is set for various types of pages."""
    # Mapping from each page to its MIME type.
    page_to_mime =\
        {"?json": "application/json", "?raw": "text/plain; charset=UTF-8",
        "": "text/html; charset=UTF-8"}
    for port in self.TEST_PORTS_WITH_SS:
      for page, content_type in page_to_mime.items():
        url = self.METRICS_URL.format(port) + page
        assert content_type == requests.get(url).headers['Content-Type']

  def test_log_level(self):
    """Test that the /log_level page outputs are as expected and work well on basic and
    malformed inputs. This however does not test that the log level changes are actually
    in effect."""
    # Check that the log_level end points are accessible.
    self.post_and_check_status_jvm(self.SET_JAVA_LOGLEVEL_URL)
    self.post_and_check_status_jvm(self.RESET_JAVA_LOGLEVEL_URL)
    self.post_and_check_status(self.SET_GLOG_LOGLEVEL_URL)
    self.post_and_check_status(self.RESET_GLOG_LOGLEVEL_URL)

    # Set the log level of a class to TRACE and confirm the setting is in place
    self.post_and_check_status_jvm(self.SET_JAVA_LOGLEVEL_URL,
        {"class": "org.apache.impala.catalog.HdfsTable", "level": "trace"},
        "org.apache.impala.catalog.HdfsTable : TRACE")

    # Reset Java logging levels
    self.post_and_check_status_jvm(self.RESET_JAVA_LOGLEVEL_URL, {},
        "Java log levels reset.")

    # Set a new glog level and make sure the setting has been applied.
    self.post_and_check_status(self.SET_GLOG_LOGLEVEL_URL, {"glog": 3}, "val(3)")

    # Try resetting the glog logging defaults again.
    self.post_and_check_status(self.RESET_GLOG_LOGLEVEL_URL, {},
        "Current backend log level: ")

    # Try to set the log level with an empty class input
    self.post_and_check_status_jvm(self.SET_JAVA_LOGLEVEL_URL, {"class": ""})

    # Empty input for setting a glog level request
    self.post_and_check_status(self.SET_GLOG_LOGLEVEL_URL, {"glog": ""})

    # Try setting a non-existent log level on a valid class. In such cases,
    # log4j automatically sets it as DEBUG. This is the behavior of
    # Level.toLevel() method.
    self.post_and_check_status_jvm(self.SET_JAVA_LOGLEVEL_URL,
        {"class": "org.apache.impala.catalog.HdfsTable", "level": "foo"},
        "org.apache.impala.catalog.HdfsTable : DEBUG")

    # Try setting an invalid glog level.
    self.post_and_check_status(self.SET_GLOG_LOGLEVEL_URL, {"glog": "foo"},
        "Bad glog level input")

    # Try a non-existent endpoint on log_level URL.
    self.post_and_check_status(self.SET_GLOG_LOGLEVEL_URL, {"badurl": "foo"})

  @pytest.mark.execute_serially
  def test_uda_with_log_level(self):
    """IMPALA-7903: Impala crashes when executing an aggregate query with log level set
    to 3. Running this test serially not to interfere with other tests setting the log
    level."""
    # Check that the log_level end points are accessible.
    self.post_and_check_status(self.SET_GLOG_LOGLEVEL_URL)
    self.post_and_check_status(self.RESET_GLOG_LOGLEVEL_URL)
    # Set log level to 3.
    self.post_and_check_status(self.SET_GLOG_LOGLEVEL_URL, {"glog": 3}, "val(3)")
    # Check that Impala doesn't crash when running a query that aggregates.
    self.client.execute("select avg(int_col) from functional.alltypessmall")
    # Reset log level.
    self.post_and_check_status(self.RESET_GLOG_LOGLEVEL_URL, {},
        "Current backend log level: ")

  def test_catalog(self, cluster_properties, unique_database):
    """Tests the /catalog and /catalog_object endpoints."""
    # Non-partitioned table
    query = "create table {0}.foo (id int, val int)".format(unique_database)
    self.execute_query(query)
    insert_query = "insert into {0}.foo values (1, 200)".format(unique_database)
    self.execute_query(insert_query)
    # Partitioned table
    partitioned_query = "create table {0}.foo_part (id int, val int) partitioned by (" \
      "year int)".format(unique_database)
    self.execute_query(partitioned_query)
    partition_insert_query = "insert into {0}.foo_part partition (year=2010) values "\
      "(1, 200)".format(unique_database)
    self.execute_query(partition_insert_query)
    # Kudu table
    kudu_query = "create table {0}.foo_kudu (id int, val int, primary key (id))  " \
      "stored as kudu".format(unique_database)
    self.execute_query(kudu_query)
    kudu_insert_query = "insert into {0}.foo_kudu values (1, 200)".format(unique_database)
    self.execute_query(kudu_insert_query)
    # Partitioned parquet table
    parquet_query = "create table {0}.foo_part_parquet (id int, val int) partitioned " \
      "by (year int) stored as parquet".format(unique_database)
    self.execute_query(parquet_query)
    parquet_insert_query = "insert into {0}.foo_part_parquet partition (year=2010) " \
      "values (1, 200)".format(unique_database)
    self.execute_query(parquet_insert_query)

    self.get_and_check_status_jvm(self.CATALOG_URL, unique_database)
    self.get_and_check_status_jvm(self.CATALOG_URL, "foo_part")
    # IMPALA-5028: Test toThrift() of a partitioned table via the WebUI code path.
    self.__test_catalog_object(unique_database, "foo_part", cluster_properties)
    self.__test_catalog_object(unique_database, "foo_kudu", cluster_properties)
    self.__test_catalog_object(unique_database, "foo_part_parquet", cluster_properties)
    self.__test_catalog_object(unique_database, "foo", cluster_properties)
    self.__test_json_db_object(unique_database)
    self.__test_json_table_object(unique_database, "foo")
    self.__test_json_table_object(unique_database, "foo_part")
    self.__test_json_table_object(unique_database, "foo_part_parquet")
    self.__test_table_metrics(unique_database, "foo_part", "total-file-size-bytes")
    self.__test_table_metrics(unique_database, "foo_part", "num-files")
    self.__test_table_metrics(unique_database, "foo_part", "alter-duration")
    self.__test_table_metrics(unique_database, "foo_part", "events-process-duration")
    self.__test_catalog_tablesfilesusage(unique_database, "foo_part", "1")
    self.__test_catalog_tables_loading_time(unique_database, "foo_part")
    self.get_and_check_status(self.EVENT_PROCESSOR_URL, "events-consuming-delay",
        ports_to_test=self.CATALOG_TEST_PORT)

  def __test_catalog_object(self, db_name, tbl_name, cluster_properties):
    """Tests the /catalog_object endpoint for the given db/table. Runs
    against an unloaded as well as a loaded table."""
    obj_url = self.CATALOG_OBJECT_URL + \
              "?object_type=TABLE&object_name={0}.{1}".format(db_name, tbl_name)

    if cluster_properties.is_catalog_v2_cluster():
      impalad_expected_str = \
          "No URI handler for &apos;/catalog_object&apos;"
      self.client.execute("invalidate metadata %s.%s" % (db_name, tbl_name))
      self.get_and_check_status(obj_url, tbl_name, ports_to_test=self.CATALOG_TEST_PORT)
      # Catalog object endpoint is disabled in local catalog mode.
      self.check_endpoint_is_disabled(obj_url, impalad_expected_str,
                                      ports_to_test=self.IMPALAD_TEST_PORT)
      self.client.execute("select count(*) from %s.%s" % (db_name, tbl_name))
      self.get_and_check_status(obj_url, tbl_name, ports_to_test=self.CATALOG_TEST_PORT)
      self.check_endpoint_is_disabled(obj_url, impalad_expected_str,
                                      ports_to_test=self.IMPALAD_TEST_PORT)
    else:
      impalad_expected_str = tbl_name
      self.client.execute("invalidate metadata %s.%s" % (db_name, tbl_name))
      self.get_and_check_status(obj_url, tbl_name, ports_to_test=self.CATALOG_TEST_PORT)
      self.get_and_check_status(obj_url, impalad_expected_str,
          ports_to_test=self.IMPALAD_TEST_PORT)
      self.client.execute("select count(*) from %s.%s" % (db_name, tbl_name))

      self.get_and_check_status(obj_url, tbl_name, ports_to_test=self.CATALOG_TEST_PORT)
      self.get_and_check_status(obj_url, impalad_expected_str,
          ports_to_test=self.IMPALAD_TEST_PORT)

  def __test_json_db_object(self, db_name):
    """Tests the /catalog_object?json endpoint of catalogd for the given db."""
    obj_url = self.CATALOG_OBJECT_URL + \
              "?json&object_type=DATABASE&object_name={0}".format(db_name)
    responses = self.get_and_check_status(obj_url, ports_to_test=self.CATALOG_TEST_PORT)
    obj = json.loads(json.loads(responses[0].text)["json_string"])
    assert obj["type"] == 2, "type should be DATABASE"
    assert "catalog_version" in obj, "TCatalogObject should have catalog_version"
    db_obj = obj["db"]
    assert db_obj["db_name"] == db_name
    assert "metastore_db" in db_obj, "Loaded database should have metastore_db"

  def __test_json_table_object(self, db_name, tbl_name):
    """Tests the /catalog_object?json endpoint of catalogd for the given db/table. Runs
    against an unloaded as well as a loaded table."""
    obj_url = self.CATALOG_OBJECT_URL + \
              "?json&object_type=TABLE&object_name={0}.{1}".format(db_name, tbl_name)
    self.client.execute("invalidate metadata %s.%s" % (db_name, tbl_name))
    responses = self.get_and_check_status(obj_url, ports_to_test=self.CATALOG_TEST_PORT)
    obj = json.loads(json.loads(responses[0].text)["json_string"])
    assert obj["type"] == 3, "type should be TABLE"
    assert "catalog_version" in obj, "TCatalogObject should have catalog_version"
    tbl_obj = obj["table"]
    assert tbl_obj["db_name"] == db_name
    assert tbl_obj["tbl_name"] == tbl_name
    assert "hdfs_table" not in tbl_obj, "Unloaded table should not have hdfs_table"

    self.client.execute("refresh %s.%s" % (db_name, tbl_name))
    responses = self.get_and_check_status(obj_url, ports_to_test=self.CATALOG_TEST_PORT)
    obj = json.loads(json.loads(responses[0].text)["json_string"])
    assert obj["type"] == 3, "type should be TABLE"
    assert "catalog_version" in obj, "TCatalogObject should have catalog_version"
    tbl_obj = obj["table"]
    assert tbl_obj["db_name"] == db_name
    assert tbl_obj["tbl_name"] == tbl_name
    assert "columns" in tbl_obj, "Loaded TTable should have columns"
    assert tbl_obj["table_type"] == 0, "table_type should be HDFS_TABLE"
    assert "metastore_table" in tbl_obj
    hdfs_tbl_obj = tbl_obj["hdfs_table"]
    assert "hdfsBaseDir" in hdfs_tbl_obj
    assert "colNames" in hdfs_tbl_obj
    assert "nullPartitionKeyValue" in hdfs_tbl_obj
    assert "nullColumnValue" in hdfs_tbl_obj
    assert "partitions" in hdfs_tbl_obj
    assert "prototype_partition" in hdfs_tbl_obj

  def check_endpoint_is_disabled(self, url, string_to_search="", ports_to_test=None):
    """Helper method that verifies the given url does not exist."""
    if ports_to_test is None:
      ports_to_test = self.TEST_PORTS_WITH_SS
    for port in ports_to_test:
      input_url = url.format(port)
      response = requests.head(input_url)
      assert response.status_code == requests.codes.not_found, "URL: {0} Str:'{" \
        "1}'\nResp:{2}".format(input_url, string_to_search, response.text)
      response = requests.get(input_url)
      assert response.status_code == requests.codes.not_found, "URL: {0} Str:'{" \
        "1}'\nResp:{2}".format(input_url, string_to_search, response.text)
      assert string_to_search in response.text, "URL: {0} Str:'{1}'\nResp:{2}".format(
        input_url, string_to_search, response.text)

  def __test_table_metrics(self, db_name, tbl_name, metric):
    self.client.execute("refresh %s.%s" % (db_name, tbl_name))
    self.get_and_check_status(self.TABLE_METRICS_URL +
      "?name=%s.%s" % (db_name, tbl_name), metric, ports_to_test=self.CATALOG_TEST_PORT)

  def __test_catalog_tables_loading_time(self, db_name, tbl_name):
    """Test the list of tables with the longest loading time in the catalog page.
    Make sure the table exists. And the table is not empty"""
    self.client.execute("refresh %s.%s" % (db_name, tbl_name))
    self.get_and_check_status(self.CATALOG_URL,
      "Tables with Longest Metadata Loading Time", ports_to_test=self.CATALOG_TEST_PORT)
    response = self.get_and_check_status(self.CATALOG_URL + "?json",
      "longest_loading_tables", ports_to_test=self.CATALOG_TEST_PORT)
    response_json = json.loads(response[0].text)
    assert "longest_loading_tables" in response_json, \
      "Response {0}".format(response_json)
    loading_tables = response_json["longest_loading_tables"]
    assert len(loading_tables) > 0
    members = ["median_metadata_loading_time_ns", "max_metadata_loading_time_ns",
               "p75_loading_time_ns", "p95_loading_time_ns", "p99_loading_time_ns"]
    for member in members:
      if member not in loading_tables[0]:
        assert False, "{0} not in loading tables {1}".format(member, loading_tables)

  def __test_catalog_tablesfilesusage(self, db_name, tbl_name, numfiles):
    """Test the list of tables with  most number of files in the catalog page.
    Make sure the loaded table has correct file count."""
    self.client.execute("refresh %s.%s" % (db_name, tbl_name))
    response = self.get_and_check_status(self.CATALOG_URL,
      "Tables with Most Number of Files", ports_to_test=self.CATALOG_TEST_PORT)
    list_file_str = re.search('<table id="high-file-count-tables"( .*?)</table>',
      response[0].text, re.MULTILINE | re.DOTALL)
    target_metric = "%s.%s-metric" % (db_name, tbl_name)
    list_files = re.findall('<tr>(.*?)</tr>', list_file_str.group(0),
      re.MULTILINE | re.DOTALL)
    for trow in list_files:
      # Find the entry for the db table and verify its file count.
      if re.search(target_metric, trow) is not None:
        # Get the number following <td> in the entry
        nfiles = re.search('(?<=\<td\>)\d+', trow)
        assert nfiles.group(0) == numfiles
    response = self.get_and_check_status(self.CATALOG_URL + "?json",
      "high_file_count_tables", ports_to_test=self.CATALOG_TEST_PORT)
    response_json = json.loads(response[0].text)
    high_filecount_tbls = response_json["high_file_count_tables"]
    tbl_fname = "%s.%s" % (db_name, tbl_name)
    assert len(high_filecount_tbls) > 0
    # The expected table might not be in the Top-N list, we may not find it
    # in the list. Just make sure the file count is right if it is in the
    # list.
    for tblinfo in high_filecount_tbls:
      if tblinfo["name"] == tbl_fname:
        assert tblinfo["num_files"] == int(numfiles)

  def test_query_stmt(self):
    """Create a long select query then check if it is truncated in the response json."""
    # The imput query is a select + 450 "x " long, which is long enough to get truncated.
    query = "select \"{0}\"".format("x " * 450)
    # The expected result query should be 253 long and contains the first 250
    # chars + "..."
    expected_result = "select \"{0}...".format("x " * 121)
    check_if_contains = False
    response_json = self.__run_query_and_get_debug_page(
      query, self.QUERIES_URL, expected_state=self.client.QUERY_STATES["FINISHED"])
    # Search the json for the expected value.
    # The query can be in in_filght_queries even though it is in FINISHED state.
    for json_part in itertools.chain(
      response_json['completed_queries'], response_json['in_flight_queries']):
      if expected_result in json_part['stmt']:
        check_if_contains = True
        break

    assert check_if_contains, "No matching statement found in the jsons at {}: {}".format(
        datetime.now(), json.dumps(response_json, sort_keys=True, indent=4))

  def __run_query_and_get_debug_page(self, query, page_url, query_options=None,
                                     expected_state=None):
    """Runs a query to obtain the content of the debug page pointed to by page_url, then
    cancels the query. Optionally takes in an expected_state parameter, if specified the
    method waits for the query to reach the expected state before getting its debug
    information."""
    if query_options:
      self.client.set_configuration(query_options)
    query_handle = self.client.execute_async(query)
    response_json = ""
    try:
      if expected_state:
        self.wait_for_state(query_handle, expected_state, 100)
      responses = self.get_and_check_status(
        page_url + "?query_id=%s&json" % query_handle.get_handle().id,
        ports_to_test=[25000])
      assert len(responses) == 1
      response_json = json.loads(responses[0].text)
    finally:
      self.client.cancel(query_handle)
      self.client.close_query(query_handle)
    return response_json

  @pytest.mark.xfail(run=False, reason="IMPALA-8059")
  def test_backend_states(self, unique_database):
    """Test that /query_backends returns the list of backend states for DML or
    queries; nothing for DDL statements"""
    sleep_query = "select sleep(10000) from functional.alltypes limit 1"
    ctas_sleep_query = "create table {0}.foo as {1}".format(unique_database, sleep_query)
    running_state = self.client.QUERY_STATES['RUNNING']
    backend_state_properties = ['cpu_user_s', 'rpc_latency', 'num_remaining_instances',
                                'num_instances', 'peak_per_host_mem_consumption',
                                'time_since_last_heard_from', 'status', 'host',
                                'cpu_sys_s', 'done', 'bytes_read']

    for query in [sleep_query, ctas_sleep_query]:
      response_json = self.__run_query_and_get_debug_page(query,
                                                          self.QUERY_BACKENDS_URL,
                                                          expected_state=running_state)

      assert 'backend_states' in response_json
      backend_states = response_json['backend_states']
      assert len(backend_states) > 0

      for backend_state in backend_states:
        for backend_state_property in backend_state_properties:
          assert backend_state_property in backend_state
        assert backend_state['status'] == 'OK'
        assert not backend_state['done']

    response_json = self.__run_query_and_get_debug_page("describe functional.alltypes",
                                                        self.QUERY_BACKENDS_URL)
    assert 'backend_states' not in response_json

  @pytest.mark.xfail(run=False, reason="IMPALA-8059")
  def test_backend_instances(self, unique_database, query_options=None):
    """Test that /query_finstances returns the list of fragment instances for DML or queries;
    nothing for DDL statements"""
    sleep_query = "select sleep(10000) from functional.alltypes limit 1"
    ctas_sleep_query = "create table {0}.foo as {1}".format(unique_database, sleep_query)
    running_state = self.client.QUERY_STATES['RUNNING']
    instance_stats_properties = ['fragment_name', 'time_since_last_heard_from',
                                 'current_state', 'first_status_update_received',
                                 'instance_id', 'done']

    for query in [sleep_query, ctas_sleep_query]:
      response_json = self.__run_query_and_get_debug_page(query,
                                                          self.QUERY_FINSTANCES_URL,
                                                          expected_state=running_state)

      assert 'backend_instances' in response_json
      backend_instances = response_json['backend_instances']
      assert len(backend_instances) > 0

      for backend_instance in backend_instances:
        assert 'host' in backend_instance
        assert 'instance_stats' in backend_instance

        instances_stats = backend_instance['instance_stats']
        assert len(instances_stats) > 0
        for instance_stats in instances_stats:
          for instance_stats_property in instance_stats_properties:
            assert instance_stats_property in instance_stats
        assert not instance_stats['done']

    response_json = self.__run_query_and_get_debug_page("describe functional.alltypes",
                                                         self.QUERY_BACKENDS_URL)
    assert 'backend_instances' not in response_json

  @pytest.mark.xfail(run=False, reason="IMPALA-8059")
  def test_backend_instances_mt_dop(self, unique_database):
    """Test that accessing /query_finstances does not crash the backend when running with
    mt_dop."""
    self.test_backend_instances(unique_database, query_options=dict(mt_dop=4))

  def test_io_mgr_threads(self):
    """Test that IoMgr threads have readable names. This test assumed that all systems we
    support have a disk called 'sda'."""
    responses = self.get_and_check_status(
        self.THREAD_GROUP_URL + "?group=disk-io-mgr&json", ports_to_test=[25000])
    assert len(responses) == 1
    response_json = json.loads(responses[0].text)
    # Verify metric keys for each thread
    for t in response_json['threads']:
      assert "name" in t
      assert "id" in t
      assert "user_s" in t
      assert "kernel_s" in t
      assert "iowait_s" in t
    thread_names = [t["name"] for t in response_json['threads']]
    expected_name_patterns = ["ADLS remote", "S3 remote", "HDFS remote"]
    for pattern in expected_name_patterns:
      assert any(pattern in t for t in thread_names), \
          "Could not find thread matching '%s'" % pattern

  def test_rpc_read_write_metrics(self):
    """Test that read/write metrics are exposed in /rpcz"""
    rpcz = self.get_debug_page(self.RPCZ_URL)
    hist_time_regex = "[0-9][0-9numsh.]*"
    rpc_histogram_regex = (
      "Count: [0-9]+, sum: " + hist_time_regex
      + ", min / max: " + hist_time_regex + " / " + hist_time_regex
      + ", 25th %-ile: " + hist_time_regex
      + ", 50th %-ile: " + hist_time_regex
      + ", 75th %-ile: " + hist_time_regex
      + ", 90th %-ile: " + hist_time_regex
      + ", 95th %-ile: " + hist_time_regex
      + ", 99.9th %-ile: " + hist_time_regex)
    assert len(rpcz['servers']) > 0
    for s in rpcz['servers']:
      for m in s['methods']:
        assert re.search(rpc_histogram_regex, m["summary"])
        assert re.search(rpc_histogram_regex, m["read"])
        assert re.search(rpc_histogram_regex, m["write"])

  def test_krpc_rpcz(self):
    """Test that KRPC metrics are exposed in /rpcz and that they are updated when
    executing a query."""
    TEST_QUERY = "select count(c2.string_col) from \
        functional.alltypestiny join functional.alltypessmall c2"
    SVC_NAME = 'impala.DataStreamService'

    def is_krpc_use_unix_domain_socket():
      rpcz = self.get_debug_page(self.RPCZ_URL)
      return rpcz['rpc_use_unix_domain_socket']

    def get_per_conn_metrics(inbound):
      """Get inbound or outbound per-connection metrics"""
      rpcz = self.get_debug_page(self.RPCZ_URL)
      if inbound:
        key = "inbound_per_conn_metrics"
      else:
        key = "per_conn_metrics"
      conns = rpcz[key]
      return conns

    def get_svc_metrics(svc_name):
      rpcz = self.get_debug_page(self.RPCZ_URL)
      assert len(rpcz['services']) > 0
      for s in rpcz['services']:
        if s['service_name'] == svc_name:
          assert len(s['rpc_method_metrics']) > 0, '%s metrics are empty' % svc_name
          return sorted(s['rpc_method_metrics'], key=lambda m: m['method_name'])
      assert False, 'Could not find metrics for %s' % svc_name

    krpc_use_uds = is_krpc_use_unix_domain_socket()

    svc_before = get_svc_metrics(SVC_NAME)
    inbound_before = get_per_conn_metrics(True)
    outbound_before = get_per_conn_metrics(False)
    self.client.execute(TEST_QUERY)
    svc_after = get_svc_metrics(SVC_NAME)
    inbound_after = get_per_conn_metrics(True)
    outbound_after = get_per_conn_metrics(False)

    assert svc_before != svc_after
    if not krpc_use_uds:
      assert inbound_before != inbound_after
      assert outbound_before != outbound_after

    # Some connections should have metrics after executing query
    assert len(inbound_after) > 0
    assert len(outbound_after) > 0
    # Spot-check some fields, including socket stats.
    for conn in itertools.chain(inbound_after, outbound_after):
      assert conn["remote_addr"] != ""
      assert conn["num_calls_in_flight"] >= 0
      assert conn["num_calls_in_flight"] == len(conn["calls_in_flight"])
      # Check rtt, which should be present in 'struct tcp_info' even in old kernels
      # like 2.6.32.
      # Skip these checking if using UDS.
      if not krpc_use_uds:
        assert conn["socket_stats"]["rtt"] > 0, conn
        # send_queue_bytes uses TIOCOUTQ, which is also present in 2.6.32 and even older
        # kernels.
        assert conn["socket_stats"]["send_queue_bytes"] >= 0, conn

  @pytest.mark.execute_serially
  def test_admission_page(self):
    """Sanity check for the admission debug page's http end points (both admission and
    reset stats end points)."""
    # Make sure at least one query is submitted to the default pool since impala startup,
    # so that it shows up in the admission control debug page. Checks for both with and
    # without the pool_name search string.
    self.client.execute("select 1")
    response_json = self.__fetch_resource_pools_json()

    # Find the default pool. It is either "root.default" if a fair-scheduler.xml file
    # is provided or "default-pool" otherwise.
    default_pool = None
    for pool_json in response_json:
      pool_name = pool_json['pool_name']
      if pool_name in ['default-pool', 'root.default']:
        default_pool = pool_name
        break
    assert default_pool is not None, \
        "Expected a default pool to be present in {0}".format(response_json)

    response_json = self.__fetch_resource_pools_json(default_pool)
    assert response_json[0]['pool_name'] == default_pool

    # Make sure the reset informational stats endpoint works, both with and without the
    # pool_name search string.
    assert response_json[0]['total_admitted'] > 0
    self.get_and_check_status(
      self.RESET_RESOURCE_POOL_STATS_URL + "?pool_name={0}".format(default_pool),
      ports_to_test=[25000])
    response_json = self.__fetch_resource_pools_json(default_pool)
    assert response_json[0]['total_admitted'] == 0

    self.client.execute("select 1")
    response_json = self.__fetch_resource_pools_json(default_pool)
    assert response_json[0]['total_admitted'] > 0
    self.get_and_check_status(self.RESET_RESOURCE_POOL_STATS_URL, ports_to_test=[25000])
    response_json = self.__fetch_resource_pools_json(default_pool)
    pool_config = response_json[0]
    assert pool_config['total_admitted'] == 0

    # check that metrics exist
    assert 'max_query_mem_limit' in pool_config
    assert 'min_query_mem_limit' in pool_config
    assert 'clamp_mem_limit_query_option' in pool_config

  def __fetch_resource_pools_json(self, pool_name=None):
    """Helper method used to fetch the resource pool json from the admission debug page.
    If a 'pool_name' is passed to this method, it adds the pool_name search string to the
    http request."""
    search_string = "?json"
    if pool_name is not None:
      search_string += "&pool_name=" + pool_name
    responses = self.get_and_check_status(self.ADMISSION_URL + search_string,
                                          ports_to_test=[25000])
    assert len(responses) == 1
    response_json = json.loads(responses[0].text)
    assert 'resource_pools' in response_json
    return response_json['resource_pools']

  @SkipIfBuildType.remote
  def test_backends_page(self):
    """Sanity check for the backends debug page's http end point"""
    responses = self.get_and_check_status(self.BACKENDS_URL + '?json',
                                          ports_to_test=[25000])
    assert len(responses) == 1
    response_json = json.loads(responses[0].text)
    assert 'backends' in response_json
    # When this test runs, all impalads would have already started.
    assert len(response_json['backends']) == 3
    assert response_json['num_active_backends'] == 3
    assert 'num_quiescing_backends' not in response_json
    assert 'num_blacklisted_backends' not in response_json

    # Look at results for a single backend - they are not sorted.
    backend_row = response_json['backends'][0]

    assert len(backend_row['webserver_url']) > 0
    webserver_ports = ('25000', '25001', '25002')
    assert backend_row['webserver_url'].endswith(webserver_ports)

    # The 'address' column is the backend port of the impalad.
    assert len(backend_row['address']) > 0
    krpc_ports = ('27000', '27001', '27002')
    assert backend_row['address'].endswith(krpc_ports)

    # The 'krpc_address' is the krpc address of the impalad.
    assert len(backend_row['krpc_address']) > 0
    krpc_ports = ('27000', '27001', '27002')
    assert backend_row['krpc_address'].endswith(krpc_ports)

    assert backend_row['is_coordinator']
    assert backend_row['is_executor']
    assert not backend_row['is_quiescing']
    assert not backend_row['is_blacklisted']
    assert len(backend_row['admit_mem_limit']) > 0
    assert len(backend_row['process_start_time']) > 0
    assert len(backend_row['version']) > 0

  def test_download_profile(self):
    """Test download text profile for a query"""
    query = "select count(*) from functional.alltypes"
    query_id = self.client.execute(query).query_id
    profile_page_url = "{0}query_profile?query_id={1}".format(
        self.ROOT_URL, query_id)
    # Check the download tag is there.
    for profile_format in ["Text", "Json"]:
      responses = self.get_and_check_status(
        profile_page_url, profile_format,
        ports_to_test=self.IMPALAD_TEST_PORT)

      assert len(responses) == 1

      if profile_format == 'Text':
        download_link = "query_profile_plain_text?query_id={0}".format(query_id)
        assert download_link in responses[0].text
        # Get the response from download link and validate it by checking
        # the query is in the file.
        responses = self.get_and_check_status(
            self.ROOT_URL + download_link, query, self.IMPALAD_TEST_PORT)
        # Check the query id is in the content of the reponse.
        assert len(responses) == 1
        assert query_id in responses[0].text
      elif profile_format == 'Json':
        download_link = "query_profile_json?query_id={0}".format(query_id)
        assert download_link in responses[0].text
        # Get the response from download link and validate it by checking
        # the query is in the file.
        responses = self.get_and_check_status(
            self.ROOT_URL + download_link, query, self.IMPALAD_TEST_PORT)

        assert len(responses) == 1
        # Check the return content is valid json
        try:
          json_res = json.loads(responses[0].text)
        except ValueError:
          assert False, "Downloaded JSON format query profile cannot be parsed. " \
              "Json profile:{0}".format(responses[0].text)
        # Find the query id in json
        assert query_id in json_res["contents"]["profile_name"], json_res

  def test_prometheus_metrics(self):
    """Test to check prometheus metrics"""
    resp = self.get_and_check_status(self.PROMETHEUS_METRICS_URL)
    assert len(resp) == 3
    # check if metric shows up
    assert 'impala_statestore_subscriber_heartbeat_interval_time_min' in resp[0].text

  def test_healthz_endpoint(self):
    """Test to check that the /healthz endpoint returns 200 OK."""
    for port in self.TEST_PORTS_WITH_SS:
      page = requests.get(self.HEALTHZ_URL.format(port))
      assert page.status_code == requests.codes.ok
      page = requests.head(self.HEALTHZ_URL.format(port))
      assert page.status_code == requests.codes.ok

  def test_knox_compatibility(self):
    """Checks that the template files conform to the requirements for compatibility with
    the Apache Knox service definition."""
    # Matches all 'a' links with an 'href' that doesn't start with either '#' (which stays
    # on the same page an so doesn't need to the hostname) or '{{ __common__.host-url }}'
    # Note that if we ever need to add a link that doesn't conform to this, we will
    # probably also have to change the Knox service definition.
    href_regex = "<(a|link) .*? href=['\"](?!({{ __common__.host-url }})|#)"
    # Matches all 'script' tags that aren't absoluve urls.
    script_regex = "<script .*?src=['\"](?!({{ __common__.host-url }})|http)"
    # Matches all 'form' tags that are not followed by including the hidden inputs.
    form_regex = "<form [^{]*?>(?!{{>www/form-hidden-inputs.tmpl}})"
    # Matches XMLHttpRequest.open() in javascript that are not followed with make_url().
    javascript_regex = "open\(['\"]GET['\"], (?!make_url)"
    # Matches urls in json parameters passed to DataTables.
    datatables_regex = "url: ['\"](?!make_url)"
    regex = "(%s)|(%s)|(%s)|(%s)|(%s)" % \
        (href_regex, script_regex, form_regex, javascript_regex, datatables_regex)
    results = grep_dir(os.path.join(os.environ['IMPALA_HOME'], "www"), regex, ".*\.tmpl")
    assert len(results) == 0, \
        "All links on the webui must include the webserver host: %s" % results

    # Check that when Knox integration is not being used, the links are relative, by
    # checking for the root link from the header.
    self.get_and_check_status(self.ROOT_URL, "href='/'", self.IMPALAD_TEST_PORT)
    # Check that if the 'x-forwarded-context' header is present in the request, the links
    # are written as absolute.
    self.get_and_check_status(self.ROOT_URL,
        "href='http://.*:%s/'" % self.IMPALAD_TEST_PORT[0], self.IMPALAD_TEST_PORT,
        regex=True, headers={'X-Forwarded-Context': '/gateway'})

  def test_catalog_operations_endpoint(self):
    """Test to check that the /operations endpoint returns 200 OK."""
    page = requests.get("http://localhost:25020/operations")
    assert page.status_code == requests.codes.ok
    page = requests.head("http://localhost:25020/operations")
    assert page.status_code == requests.codes.ok

  def test_catalog_operation_fields(self, unique_database):
    """Verify the CREATE_DATABASE operation is consistent with the statement shown in the
       /queries page of impalad."""
    catalog_operations_page = requests.get("http://localhost:25020/operations?json").text
    catalog_operations = json.loads(catalog_operations_page)
    assert "finished_catalog_operations" in catalog_operations

    queries_page = requests.get("http://localhost:25000/queries?json").text
    queries = json.loads(queries_page)
    assert "completed_queries" in queries

    # Find the CREATE_DATABASE operation in catalogd
    ts_format = "%Y-%m-%d %H:%M:%S.%f"
    found = False
    for op in catalog_operations["finished_catalog_operations"]:
      if op["target_name"] == unique_database \
          and op["catalog_op_name"] == "CREATE_DATABASE":
        catalog_op_query_id = op["query_id"]
        catalog_op_user = op["user"]
        catalog_op_start_time = datetime.strptime(op["start_time"], ts_format)
        catalog_op_end_time = datetime.strptime(op["finish_time"], ts_format)
        catalog_op_duration = parse_duration_string_ms(op["duration"])
        found = True
        LOG.info("Found query id in catalog operations: " + catalog_op_query_id)
        break
    assert found

    def verify_query_record(query):
      assert "CREATE DATABASE" in query["stmt"]
      assert unique_database in query["stmt"]
      assert catalog_op_user == query["effective_user"]
      assert datetime.strptime(query["start_time"], ts_format) <= catalog_op_start_time
      assert datetime.strptime(query["end_time"], ts_format) >= catalog_op_end_time
      assert parse_duration_string_ms(query["duration"]) >= catalog_op_duration

    # Find the query in impalad
    matched = False
    for query in queries["completed_queries"]:
      if query["query_id"] == catalog_op_query_id:
        verify_query_record(query)
        matched = True
    if not matched:
      LOG.info("Query id {0} not found in the completed queries list".format(
          catalog_op_query_id))
      # Try to find the query in the in-flight queries list. It could be waiting to
      # be closed.
      for query in queries["in_flight_queries"]:
        if query["query_id"] == catalog_op_query_id:
          verify_query_record(query)
          matched = True

    # Dump web pages for debug
    if not matched:
      LOG.info("Query id {0} not found in queries page!".format(catalog_op_query_id))
      LOG.info("Catalog operations: " + catalog_operations_page)
      LOG.info("Queries: " + queries_page)
    assert matched

  def test_catalog_metrics(self):
    """Test /metrics of catalogd"""
    url = self.METRICS_URL.format(*self.CATALOG_TEST_PORT) + "?json"
    json_res = json.loads(requests.get(url).text)
    metric_keys = {m["name"] for m in json_res["metric_group"]["metrics"]}
    assert "catalog-server.metadata.file.num-loading-threads" in metric_keys
    assert "catalog-server.metadata.file.num-loading-tasks" in metric_keys
    assert "catalog-server.metadata.table.num-loading-file-metadata" in metric_keys
    assert "catalog-server.metadata.table.num-loading-metadata" in metric_keys
    assert "catalog-server.metadata.table.async-loading.num-in-progress" in metric_keys
    assert "catalog-server.metadata.table.async-loading.queue-len" in metric_keys
    assert "catalog.num-databases" in metric_keys
    assert "catalog.num-tables" in metric_keys
    assert "catalog.num-functions" in metric_keys
    assert "catalog.hms-client-pool.num-idle" in metric_keys
    assert "catalog.hms-client-pool.num-in-use" in metric_keys

  def test_query_progress(self):
    """Tests that /queries page shows query progress."""
    query = "select count(*) from functional_parquet.alltypes where bool_col = sleep(100)"
    response_json = self.__run_query_and_get_debug_page(
      query, self.QUERIES_URL, expected_state=self.client.QUERY_STATES["RUNNING"])
    for json_part in response_json['in_flight_queries']:
      if query in json_part['stmt']:
        assert json_part["query_progress"] == "0 / 4 ( 0%)"

  def try_until(self, desc, run, check, timeout=10, interval=0.1):
    start_time = time()
    while (time() - start_time < timeout):
      result = run()
      if check(result):
        return result
      sleep(interval)
    assert False, "Timed out waiting for " + desc

  def get_queries(self):
    responses = self.get_and_check_status(
      self.QUERIES_URL + "?json", ports_to_test=[25000])
    assert len(responses) == 1
    response_json = json.loads(responses[0].text)
    return response_json

  @pytest.mark.execute_serially
  def test_query_cancel_created(self):
    """Tests that if we cancel a query in the CREATED state, it still finishes and we can
    cancel it."""
    query = "select count(*) from functional_parquet.alltypes"
    delay_created_action = "impalad_load_tables_delay:SLEEP@1000"

    response_json = self.get_queries()
    assert response_json['num_in_flight_queries'] == 0

    # Start the query completely async. The server doesn't return a response until
    # the query has exited the CREATED state, so we need to get the query ID another way.
    self.client.set_configuration(dict(debug_action=delay_created_action))
    proc = Process(target=lambda cli, q: cli.execute_async(q), args=(self.client, query))
    proc.start()

    response_json = self.try_until("query creation", self.get_queries,
        lambda resp: resp['num_in_flight_queries'] > 0)
    assert len(response_json['in_flight_queries']) == 1
    assert response_json['in_flight_queries'][0]['state'] == 'CREATED'
    query_id = response_json['in_flight_queries'][0]['query_id']

    cancel_query_url = "{0}cancel_query?json&query_id={1}".format(self.ROOT_URL.format
      ("25000"), query_id)
    response = requests.get(cancel_query_url)
    assert response.status_code == requests.codes.ok
    response_json = json.loads(response.text)
    assert response_json['error'] == "Query not yet running\n"

    # Wait for query to start running. It should finish soon after.
    proc.join()
    response_json = self.try_until("query finished", self.get_queries,
        lambda resp: resp['in_flight_queries'][0]['state'] == 'FINISHED')
    assert response_json['num_in_flight_queries'] == 1

    # We never fetch results for the async query, so it stays in-flight until cancelled.
    response = requests.get(cancel_query_url)
    assert response.status_code == requests.codes.ok
    response_json = json.loads(response.text)
    assert response_json['contents'] == "Query cancellation successful"

    # Cancel request can return before cancellation is finalized. Retry for slow
    # environments like ASAN.
    response_json = self.try_until("query cancellation", self.get_queries,
        lambda resp: resp['num_in_flight_queries'] == 0)
    assert response_json['num_in_flight_queries'] == 0
    assert response_json['num_waiting_queries'] == 0

    expected_queries = [q for q in response_json['completed_queries']
                        if q['query_id'] == query_id]
    assert len(expected_queries) == 1

  @pytest.mark.execute_serially
  def test_query_cancel_exception(self):
    """Tests that if we cancel a query in the CREATED state and it has an exception, we
    can cancel it."""
    # Trigger UDF ERROR: Cannot divide decimal by zero
    query = "select *, 1.0/0 from functional_parquet.alltypes limit 10"
    delay_created_action = "impalad_load_tables_delay:SLEEP@1000"

    response_json = self.get_queries()
    assert response_json['num_in_flight_queries'] == 0

    def run(queue, client, query):
      queue.put(client.execute_async(query))

    # Start the query completely async. The server doesn't return a response until
    # the query has exited the CREATED state, so we need to get the query ID another way.
    self.client.set_configuration(dict(debug_action=delay_created_action))
    queue = Queue()
    proc = Process(target=run, args=(queue, self.client, query))
    proc.start()

    response_json = self.try_until("query creation", self.get_queries,
        lambda resp: resp['num_in_flight_queries'] > 0)
    assert len(response_json['in_flight_queries']) == 1
    assert response_json['in_flight_queries'][0]['state'] == 'CREATED'
    query_id = response_json['in_flight_queries'][0]['query_id']

    cancel_query_url = "{0}cancel_query?json&query_id={1}".format(self.ROOT_URL.format
      ("25000"), query_id)
    response = requests.get(cancel_query_url)
    assert response.status_code == requests.codes.ok
    response_json = json.loads(response.text)
    assert response_json['error'] == "Query not yet running\n"

    # Fetch query results.
    query_handle = queue.get()
    proc.join()
    assert query_handle
    try:
      self.client.fetch(query, query_handle)
    except Exception as e:
      re.match("UDF ERROR: Cannot divide decimal by zero", str(e))

    # Failed query should be completed.
    response_json = self.get_queries()
    assert response_json['num_in_flight_queries'] == 0
    assert response_json['num_waiting_queries'] == 0

    expected_queries = [q for q in response_json['completed_queries']
                        if q['query_id'] == query_id]
    assert len(expected_queries) == 1

  @pytest.mark.execute_serially
  def test_hadoop_varz_page(self):
    """test for /hadoop-var to check availablity of haqoop configuration like
    hive warehouse dir, fs.defaultFS"""
    responses = self.get_and_check_status(self.HADOOP_VARZ_URL,
        "hive.metastore.warehouse.dir", ports_to_test=self.TEST_PORTS_WITHOUT_SS)
    responses = self.get_and_check_status(self.HADOOP_VARZ_URL,
        "hive.metastore.warehouse.external.dir", ports_to_test=self.TEST_PORTS_WITHOUT_SS)
    responses = self.get_and_check_status(self.HADOOP_VARZ_URL,
        "fs.defaultFS", ports_to_test=self.TEST_PORTS_WITHOUT_SS)
    # check if response size is 2 , for both catalog and impalad webUI
    assert len(responses) == 2

class TestWebPageAndCloseSession(ImpalaTestSuite):
  ROOT_URL = "http://localhost:{0}/"

  @SkipIfDockerizedCluster.daemon_logs_not_exposed
  def test_display_src_socket_in_query_cause(self):
    # Execute a long running query then cancel it from the WebUI.
    # Check the runtime profile and the INFO logs for the cause message.
    query = "select sleep(10000)"
    query_id = self.execute_query_async(query).get_handle().id
    cancel_query_url = "{0}cancel_query?query_id={1}".format(self.ROOT_URL.format
      ("25000"), query_id)
    text_profile_url = "{0}query_profile_plain_text?query_id={1}".format(self.ROOT_URL
      .format("25000"), query_id)
    requests.get(cancel_query_url)
    response = requests.get(text_profile_url)
    cancel_status = "Cancelled from Impala&apos;s debug web interface by user: " \
                    "&apos;anonymous&apos; at"
    assert cancel_status in response.text
    self.assert_impalad_log_contains("INFO", "Cancelled from Impala\'s debug web "
      "interface by user: 'anonymous' at", expected_count=-1)
    # Session closing from the WebUI does not produce the cause message in the profile,
    # so we will skip checking the runtime profile.
    results = self.execute_query("select current_session()")
    session_id = results.data[0]
    close_session_url = "{0}close_session?session_id={1}".format(self.ROOT_URL.format
      ("25000"), session_id)
    requests.get(close_session_url)
    self.assert_impalad_log_contains("INFO", "Session closed from Impala\'s debug "
      "web interface by user: 'anonymous' at", expected_count=-1)
