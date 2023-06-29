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
import os

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfHive3, SkipIf

IMPALA_HOME = os.getenv('IMPALA_HOME')
HIVE_SITE_WITHOUT_HMS_DIR = IMPALA_HOME + '/fe/src/test/resources/hive-site-without-hms'
TBL_NAME = "test_kudu_table_create_without_hms"


class TestCreatingKuduTableWithoutHMS(CustomClusterTestSuite):
  """Test creating kudu managed table without hms"""

  @SkipIfHive3.without_hms_not_supported
  @SkipIf.is_test_jdk
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(hive_conf_dir=HIVE_SITE_WITHOUT_HMS_DIR)
  def test_kudu_table_create_without_hms(self, unique_database):
    table_name = unique_database + "." + TBL_NAME
    self.execute_query_expect_success(self.client, "create table %s "
        "(id int, primary key(id)) stored as kudu" % table_name)
    self.execute_query_expect_success(self.client, "select * from %s" % table_name)
    self.execute_query("drop table if exists %s" % table_name)
