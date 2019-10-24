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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

import pytest
from os import getenv

HIVE_SITE_EXT_DIR = getenv('IMPALA_HOME') + '/fe/src/test/resources/hive-site-ext'


class TestCustomHiveConfigs(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(TestCustomHiveConfigs, cls).setup_class()

  # TODO: Remove the xfail marker after bumping CDP_BUILD_NUMBER to contain HIVE-22158
  @pytest.mark.xfail(run=True, reason="May fail on Hive3 versions without HIVE-22158")
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(hive_conf_dir=HIVE_SITE_EXT_DIR)
  def test_ctas_read_write_consistence(self, unique_database):
    """
    IMPALA-9071: Check that CTAS inserts data to the correct directory when
    'metastore.warehouse.external.dir' is different from 'metastore.warehouse.dir'
    in Hive.
    """
    self.execute_query_expect_success(
        self.client, 'create table %s.ctas_tbl as select 1, 2, "name"' %
                     unique_database)
    res = self.execute_query_expect_success(
        self.client, 'select * from %s.ctas_tbl' % unique_database)
    assert '1\t2\tname' == res.get_data()

    self.execute_query_expect_success(
        self.client, 'create external table %s.ctas_ext_tbl as select 1, 2, "name"' %
                     unique_database)
    # Set "external.table.purge"="true" so we can clean files of the external table
    # finally.
    self.execute_query_expect_success(
        self.client, 'alter table %s.ctas_ext_tbl set tblproperties'
                     '("external.table.purge"="true")' % unique_database)
    res = self.execute_query_expect_success(
        self.client, 'select * from %s.ctas_ext_tbl' % unique_database)
    assert '1\t2\tname' == res.get_data()

    # Explicitly drop the database with CASCADE to clean files of the external table
    self.execute_query_expect_success(
        self.client, 'drop database if exists cascade' + unique_database)
