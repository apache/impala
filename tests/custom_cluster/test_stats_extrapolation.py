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

from os import path
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import (
    create_exec_option_dimension,
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.util.hdfs_util import NAMENODE


class TestStatsExtrapolation(CustomClusterTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestStatsExtrapolation, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @CustomClusterTestSuite.with_args(impalad_args=('--enable_stats_extrapolation=true'))
  def test_stats_extrapolation(self, vector, unique_database):
    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['explain_level'] = 2
    self.run_test_case('QueryTest/stats-extrapolation', vector, unique_database)

  @CustomClusterTestSuite.with_args(impalad_args=('--enable_stats_extrapolation=true'))
  def test_compute_stats_tablesample(self, vector, unique_database):
    # Create a partitioned and unpartitioned text table. Use the existing files from
    # functional.alltypes as data because those have a known, stable file size. This
    # test is sensitive to changes in file sizes across test runs because the sampling
    # is file based. Creating test tables with INSERT does not guarantee that the same
    # file sample is selected across test runs, even with REPEATABLE.

    # Create partitioned test table. External to avoid dropping files from alltypes.
    part_test_tbl = unique_database + ".alltypes"
    self.client.execute(
      "create external table %s like functional.alltypes" % part_test_tbl)
    alltypes_loc = self._get_table_location("functional.alltypes", vector)
    for m in xrange(1, 13):
      part_loc = path.join(alltypes_loc, "year=2009/month=%s" % m)
      self.client.execute(
        "alter table %s add partition (year=2009,month=%s) location '%s'"
        % (part_test_tbl, m, part_loc))

    # Create unpartitioned test table.
    nopart_test_tbl = unique_database + ".alltypesnopart"
    self.client.execute("drop table if exists %s" % nopart_test_tbl)
    self.client.execute(
      "create table %s like functional.alltypesnopart" % nopart_test_tbl)
    nopart_test_tbl_loc = self._get_table_location(nopart_test_tbl, vector)
    # Remove NameNode prefix and first '/' because PyWebHdfs expects that
    if nopart_test_tbl_loc.startswith(NAMENODE):
      nopart_test_tbl_loc = nopart_test_tbl_loc[len(NAMENODE)+1:]
    for m in xrange(1, 13):
      src_part_loc = alltypes_loc + "/year=2009/month=%s" % m
      # Remove NameNode prefix and first '/' because PyWebHdfs expects that
      if src_part_loc.startswith(NAMENODE): src_part_loc = src_part_loc[len(NAMENODE)+1:]
      file_names = self.filesystem_client.ls(src_part_loc)
      for f in file_names:
        self.filesystem_client.copy(path.join(src_part_loc, f),
                                    path.join(nopart_test_tbl_loc, f))
    self.client.execute("refresh %s" % nopart_test_tbl)

    self.run_test_case('QueryTest/compute-stats-tablesample', vector, unique_database)
