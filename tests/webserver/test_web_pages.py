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

  GET_JAVA_LOGLEVEL_URL = "http://localhost:{0}/get_java_loglevel"
  SET_JAVA_LOGLEVEL_URL = "http://localhost:{0}/set_java_loglevel"
  RESET_JAVA_LOGLEVEL_URL = "http://localhost:{0}/reset_java_loglevel"
  SET_GLOG_LOGLEVEL_URL = "http://localhost:{0}/set_glog_level"
  RESET_GLOG_LOGLEVEL_URL = "http://localhost:{0}/reset_glog_level"
  CATALOG_URL = "http://localhost:{0}/catalog"
  CATALOG_OBJECT_URL = "http://localhost:{0}/catalog_object"
  # log4j changes do not apply to the statestore since it doesn't
  # have an embedded JVM. So we make two sets of ports to test the
  # log level endpoints, one without the statestore port and the
  # one with it.
  TEST_PORTS_WITHOUT_SS = ["25000", "25020"]
  TEST_PORTS_WITH_SS = ["25000", "25010", "25020"]

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

  def get_and_check_status(self, url, string_to_search = "", without_ss = True):
    """Helper method that polls a given url and asserts the return code is ok and
    the response contains the input string. 'without_ss', when true, excludes the
    statestore endpoint of the url. Should be applied only for log4j logging changes."""
    ports_to_test = self.TEST_PORTS_WITHOUT_SS if without_ss else self.TEST_PORTS_WITH_SS
    for port in ports_to_test:
      input_url = url.format(port)
      response = requests.get(input_url)
      assert response.status_code == requests.codes.ok\
          and string_to_search in response.text, "Offending url: " + input_url

  def test_log_level(self):
    """Test that the /log_level page outputs are as expected and work well on basic and
    malformed inputs. This however does not test that the log level changes are actually
    in effect."""
    # Check that the log_level end points are accessible.
    self.get_and_check_status(self.GET_JAVA_LOGLEVEL_URL)
    self.get_and_check_status(self.SET_JAVA_LOGLEVEL_URL)
    self.get_and_check_status(self.RESET_JAVA_LOGLEVEL_URL)
    self.get_and_check_status(self.SET_GLOG_LOGLEVEL_URL, without_ss=False)
    self.get_and_check_status(self.RESET_GLOG_LOGLEVEL_URL, without_ss=False)
    # Try getting log level of a class.
    get_loglevel_url = (self.GET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsTable")
    self.get_and_check_status(get_loglevel_url, "DEBUG")

    # Set the log level of a class to TRACE and confirm the setting is in place
    set_loglevel_url = (self.SET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsTable&level=trace")
    self.get_and_check_status(set_loglevel_url, "Effective log level: TRACE")

    get_loglevel_url = (self.GET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsTable")
    self.get_and_check_status(get_loglevel_url, "TRACE")
    # Check the log level of a different class and confirm it is still DEBUG
    get_loglevel_url = (self.GET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsPartition")
    self.get_and_check_status(get_loglevel_url, "DEBUG")

    # Reset Java logging levels and check the logging level of the class again
    self.get_and_check_status(self.RESET_JAVA_LOGLEVEL_URL, "Java log levels reset.")
    get_loglevel_url = (self.GET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsTable")
    self.get_and_check_status(get_loglevel_url, "DEBUG")

    # Set a new glog level and make sure the setting has been applied.
    set_glog_url = (self.SET_GLOG_LOGLEVEL_URL + "?glog=3")
    self.get_and_check_status(set_glog_url, "v set to 3", False)

    # Try resetting the glog logging defaults again.
    self.get_and_check_status( self.RESET_GLOG_LOGLEVEL_URL, "v set to ", False)

    # Try to get the log level of an empty class input
    get_loglevel_url = (self.GET_JAVA_LOGLEVEL_URL + "?class=")
    self.get_and_check_status(get_loglevel_url, without_ss=True)

    # Same as above, for set log level request
    set_loglevel_url = (self.SET_JAVA_LOGLEVEL_URL + "?class=")
    self.get_and_check_status(get_loglevel_url, without_ss=True)

    # Empty input for setting a glog level request
    set_glog_url = (self.SET_GLOG_LOGLEVEL_URL + "?glog=")
    self.get_and_check_status(set_glog_url, without_ss=False)

    # Try setting a non-existent log level on a valid class. In such cases,
    # log4j automatically sets it as DEBUG. This is the behavior of
    # Level.toLevel() method.
    set_loglevel_url = (self.SET_JAVA_LOGLEVEL_URL + "?class" +
        "=org.apache.impala.catalog.HdfsTable&level=foo&")
    self.get_and_check_status(set_loglevel_url, "Effective log level: DEBUG")

    # Try setting an invalid glog level.
    set_glog_url = self.SET_GLOG_LOGLEVEL_URL + "?glog=foo"
    self.get_and_check_status(set_glog_url, "Bad glog level input", False)

    # Try a non-existent endpoint on log_level URL.
    bad_loglevel_url = self.SET_GLOG_LOGLEVEL_URL + "?badurl=foo"
    self.get_and_check_status(bad_loglevel_url, without_ss=False)

  def test_catalog(self):
    """Tests the /catalog and /catalog_object endpoints."""
    self.get_and_check_status(self.CATALOG_URL, "functional", without_ss=True)
    self.get_and_check_status(self.CATALOG_URL, "alltypes", without_ss=True)
    # IMPALA-5028: Test toThrift() of a partitioned table via the WebUI code path.
    self.__test_catalog_object("functional", "alltypes")
    self.__test_catalog_object("functional_parquet", "alltypes")
    self.__test_catalog_object("functional", "alltypesnopart")
    self.__test_catalog_object("functional_kudu", "alltypes")

  def __test_catalog_object(self, db_name, tbl_name):
    """Tests the /catalog_object endpoint for the given db/table. Runs
    against an unloaded as well as a loaded table."""
    self.client.execute("invalidate metadata %s.%s" % (db_name, tbl_name))
    self.get_and_check_status(self.CATALOG_OBJECT_URL +
      "?object_type=TABLE&object_name=%s.%s" % (db_name, tbl_name), tbl_name,
      without_ss=True)
    self.client.execute("select count(*) from %s.%s" % (db_name, tbl_name))
    self.get_and_check_status(self.CATALOG_OBJECT_URL +
      "?object_type=TABLE&object_name=%s.%s" % (db_name, tbl_name), tbl_name,
      without_ss=True)
