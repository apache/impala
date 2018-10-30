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

import logging
import pytest
from kudu.schema import INT32

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.kudu_test_suite import KuduTestSuite
from tests.common.skip import SkipIfKudu
from tests.common.test_dimensions import add_exec_option_dimension

KUDU_MASTER_HOSTS = pytest.config.option.kudu_master_hosts
LOG = logging.getLogger(__name__)

class TestKuduOperations(CustomClusterTestSuite, KuduTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestKuduOperations, cls).add_test_dimensions()
    # The default read mode of READ_LATEST does not provide high enough consistency for
    # these tests.
    add_exec_option_dimension(cls, "kudu_read_mode", "READ_AT_SNAPSHOT")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=\
      "--use_local_tz_for_unix_timestamp_conversions=true")
  @SkipIfKudu.no_hybrid_clock
  def test_local_tz_conversion_ops(self, vector, unique_database):
    """IMPALA-5539: Test Kudu timestamp reads/writes are correct with the
       use_local_tz_for_unix_timestamp_conversions flag."""
    # These tests provide enough coverage of queries with timestamps.
    self.run_test_case('QueryTest/kudu-scan-node', vector, use_db=unique_database)
    self.run_test_case('QueryTest/kudu_insert', vector, use_db=unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_master_hosts=")
  def test_kudu_master_hosts(self, cursor, kudu_client):
    """Check behavior when -kudu_master_hosts is not provided to catalogd."""
    with self.temp_kudu_table(kudu_client, [INT32]) as kudu_table:
      table_name = self.get_kudu_table_base_name(kudu_table.name)
      props = "TBLPROPERTIES('kudu.table_name'='%s')" % (kudu_table.name)
      try:
        cursor.execute("CREATE EXTERNAL TABLE %s STORED AS KUDU %s" % (table_name,
            props))
        assert False
      except Exception as e:
        assert "Table property 'kudu.master_addresses' is required" in str(e)

      cursor.execute("""
          CREATE EXTERNAL TABLE %s STORED AS KUDU
          TBLPROPERTIES ('kudu.master_addresses' = '%s',
          'kudu.table_name'='%s')
          """ % (table_name, KUDU_MASTER_HOSTS, kudu_table.name))
      cursor.execute("DROP TABLE %s" % table_name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_error_buffer_size=1024")
  def test_error_buffer_size(self, cursor, unique_database):
    """Check that queries fail if the size of the Kudu client errors they generate is
    greater than kudu_error_buffer_size."""
    table_name = "%s.test_error_buffer_size" % unique_database
    cursor.execute("create table %s (a bigint primary key) stored as kudu" % table_name)
    # Insert a large number of a constant value into the table to generate many "Key
    # already present" errors. 50 errors should fit inside the 1024 byte limit.
    cursor.execute(
        "insert into %s select 1 from functional.alltypes limit 50" % table_name)
    try:
      # 200 errors should overflow the 1024 byte limit.
      cursor.execute(
          "insert into %s select 1 from functional.alltypes limit 200" % table_name)
      assert False, "Expected: 'Error overflow in Kudu session.'"
    except Exception as e:
      assert "Error overflow in Kudu session." in str(e)

class TestKuduClientTimeout(CustomClusterTestSuite, KuduTestSuite):
  """Kudu tests that set the Kudu client operation timeout to 1ms and expect
     specific timeout exceptions. While we expect all exercised operations to take at
     least 1ms, it is possible that some may not and thus the test could be flaky. If
     this turns out to be the case, specific tests may need to be re-considered or
     removed."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-kudu_operation_timeout_ms=1")
  def test_impalad_timeout(self, vector):
    """Check impalad behavior when -kudu_operation_timeout_ms is too low."""
    self.run_test_case('QueryTest/kudu-timeouts-impalad', vector)
