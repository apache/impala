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
import pytest

from time import sleep

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_connection import RUNNING
from tests.common.skip import SkipIfExploration
from tests.util.retry import retry


@SkipIfExploration.is_not_exhaustive()
class TestConjuncts(CustomClusterTestSuite):

  @pytest.mark.stress
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_create_cache_many_tables(self, unique_database):
    """Replicates the situation behund IMPALA-14863 where processing a row batch can cause
       the memory usage of a fragment instance on an executor to consume up to the process
       memory limit. Must be a custom cluster test because the fragment instances from the
       bad query continues to run for a long time after the query is cancelled."""
    self.client.execute("CREATE TABLE {}.test_left_fact (product_sk BIGINT,"
        "drv_event_type STRING)".format(unique_database))
    self.client.execute("CREATE TABLE {}.test_right_cte (product_sk BIGINT,"
        "product_type STRING)".format(unique_database))

    # Insert 1,024 rows into the Left Fact table (Exactly 1 Impala Batch). All rows share
    # the same join key (999).
    self.client.execute("""WITH ten AS (
          SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL
          SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL
          SELECT 8 UNION ALL SELECT 9
        )
        INSERT INTO {}.test_left_fact
          SELECT 999 AS product_sk, 'fail_event' AS drv_event_type  FROM ten a
          CROSS JOIN ten b CROSS JOIN ten c LIMIT 1024""".format(unique_database))

    # Insert 1,000,000 rows into the Right CTE table. All 1 Million rows share the exact
    # same join key (999).
    self.client.execute("""WITH ten AS (
          SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL
          SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL
          SELECT 8 UNION ALL SELECT 9
        )
        INSERT INTO {}.test_right_cte
        SELECT 999 AS product_sk, 'wrong_type' AS product_type FROM ten a
        CROSS JOIN ten b CROSS JOIN ten c CROSS JOIN ten d CROSS JOIN ten e
        CROSS JOIN ten f""".format(unique_database))

    # Set a very low memory limit that will quickly be exceeded by the join conjuncts.
    self.client.set_configuration_option("mem_limit_executors", "185mb")

    # Run a query that will cause the join conjuncts to exceed the memory limit.
    handle = self.client.execute_async("""SELECT STRAIGHT_JOIN count(1)
        FROM {0}.test_left_fact f
        LEFT JOIN {0}.test_right_cte p2
          ON f.product_sk = p2.product_sk
        WHERE (
          UPPER(f.drv_event_type) LIKE '%_CONF'
          OR
          (
            CASE
              WHEN UPPER(p2.product_type) = 'COMMODITY-SWAP'
                THEN UPPER(CONCAT(f.drv_event_type, '_SWAP_1'))
              WHEN UPPER(p2.product_type) = 'METAL-FUTURE'
                THEN UPPER(CONCAT(f.drv_event_type, '_METAL_2'))
              WHEN UPPER(p2.product_type) = 'ENERGY-FUTURE'
                THEN UPPER(CONCAT(f.drv_event_type, '_ENERGY_3'))
              WHEN UPPER(p2.product_type) = 'AGRICULTURAL-FUTURE'
                THEN UPPER(CONCAT(f.drv_event_type, '_AGRI_4'))
              WHEN UPPER(p2.product_type) = 'SOFT-FUTURE'
                THEN UPPER(CONCAT(f.drv_event_type, '_SOFT_5'))
              WHEN UPPER(p2.product_type) = 'OTHER-FUTURE'
                THEN UPPER(CONCAT(f.drv_event_type, '_OTHER_6'))
              WHEN UPPER(p2.product_type) = 'METAL-OPTION'
                THEN UPPER(CONCAT(f.drv_event_type, '_MOPT_7'))
              WHEN UPPER(p2.product_type) = 'ENERGY-OPTION'
                THEN UPPER(CONCAT(f.drv_event_type, '_EOPT_8'))
              WHEN UPPER(p2.product_type) = 'SPREAD-OPTION'
                THEN UPPER(CONCAT(f.drv_event_type, '_SOPT_9'))
              WHEN UPPER(p2.product_type) = 'COMMODITY-INDEX'
                THEN UPPER(CONCAT(f.drv_event_type, '_INDEX_10'))
              ELSE UPPER(CONCAT(UPPER(f.drv_event_type), '_UNKNOWN'))
            END = 'COMMODITY-FORWARD'
          )
        )""".format(unique_database))
    self.client.wait_for_impala_state(handle, RUNNING, 10)

    # Wait for the query to consume more than 270mb of memory indicating it has started
    # evaluating the join conjuncts.
    def __wait_for_270mb():
      return self.get_metric("memory.total-used") > 270 * 1024 * 1024
    retry(__wait_for_270mb, 10, 1, 1)
    mem_before = self.get_metric("memory.total-used")

    # Give the query some time to continue to run.
    sleep(10)

    # Assert the memory usage did not increase more than 25mb during the sleep. If the
    # expression results pool is not being cleared properly, the memory usage will
    # continue to grow as more rows are processed.
    mem_after = self.get_metric("memory.total-used")
    assert mem_after - mem_before < 25 * 1024 * 1024, "Memory usage increased during " \
        "sleep. Before: {} After: {}".format(mem_before, mem_after)
