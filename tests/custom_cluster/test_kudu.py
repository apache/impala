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

KUDU_MASTER_HOSTS = pytest.config.option.kudu_master_hosts
LOG = logging.getLogger(__name__)

class TestKuduOperations(CustomClusterTestSuite, KuduTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

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

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args="-kudu_operation_timeout_ms=1")
  def test_catalogd_timeout(self, vector):
    """Check catalogd behavior when -kudu_operation_timeout_ms is too low."""
    self.run_test_case('QueryTest/kudu-timeouts-catalogd', vector)
