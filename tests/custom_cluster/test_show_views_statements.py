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

# Impala tests for queries that query metadata and set session settings

from __future__ import absolute_import

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

LOCAL_CATALOG_IMPALAD_ARGS = "--use_local_catalog=true"
LOCAL_CATALOG_CATALOGD_ARGS = "--catalog_topic_mode=minimal"
PULL_TABLE_TYPES_FLAG = "--pull_table_types_and_comments=true"


class TestShowViewsStatements(CustomClusterTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(
    catalogd_args=PULL_TABLE_TYPES_FLAG)
  def test_show_views(self, vector):
    self.run_test_case('QueryTest/show_views', vector)

  @CustomClusterTestSuite.with_args(
    impalad_args=LOCAL_CATALOG_IMPALAD_ARGS,
    catalogd_args="{0} {1}".format(LOCAL_CATALOG_CATALOGD_ARGS, PULL_TABLE_TYPES_FLAG))
  def test_show_views_local_catalog(self, vector):
    self.run_test_case('QueryTest/show_views', vector)
