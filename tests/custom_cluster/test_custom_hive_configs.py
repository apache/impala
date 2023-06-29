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
from os import getenv

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfHive2, SkipIf

HIVE_SITE_EXT_DIR = getenv('IMPALA_HOME') + '/fe/src/test/resources/hive-site-ext'


class TestCustomHiveConfigs(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(TestCustomHiveConfigs, cls).setup_class()

  @SkipIfHive2.acid
  @SkipIf.is_test_jdk
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(hive_conf_dir=HIVE_SITE_EXT_DIR)
  def test_ctas_read_write_consistence(self, unique_database):
    """
    IMPALA-9071: Check that CTAS inserts data to the correct directory when
    'metastore.warehouse.external.dir' is different from 'metastore.warehouse.dir'
    in Hive.
    """
    # Test creating non-ACID managed table by CTAS. The HMS transformer will translate it
    # into an external table. But we should still be able to read/write it correctly.
    self.__check_query_results(
      unique_database + '.ctas_tbl', '1\t2\tname',
      'create table %s as select 1, 2, "name"')

    # Test creating non-ACID external table by CTAS.
    self.__check_query_results(
      unique_database + '.ctas_ext_tbl', '1\t2\tname',
      'create external table %s as select 1, 2, "name"')
    # Set "external.table.purge"="true" so we can clean files of the external table
    # finally.
    self.execute_query_expect_success(
        self.client, 'alter table %s.ctas_ext_tbl set tblproperties'
                     '("external.table.purge"="true")' % unique_database)

    # Test creating insert-only ACID managed table by CTAS.
    self.__check_query_results(
      unique_database + '.insertonly_acid_ctas', '1\t2\tname',
      'create table %s '
      'tblproperties("transactional"="true", "transactional_properties"="insert_only") '
      'as select 1, 2, "name"')

    # Test creating insert-only ACID external table by CTAS. Should not be allowed.
    self.execute_query_expect_failure(
      self.client,
      'create external table %s.insertonly_acid_ext_ctas '
      'tblproperties("transactional"="true", "transactional_properties"="insert_only") '
      'as select 1, 2, "name"' % unique_database)

  def __check_query_results(self, table, expected_results, query_format):
    self.execute_query_expect_success(self.client, query_format % table)
    res = self.execute_query_expect_success(self.client, "select * from " + table)
    assert expected_results == res.get_data()
