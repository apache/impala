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
#
# Tests to validate the Catalog Service works properly when partitions
# need to be fetched in multiple batches.

from __future__ import absolute_import, division, print_function
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension

class TestLargeNumPartitions(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLargeNumPartitions, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  def test_list_partitions(self, vector):
    full_tbl_name = 'scale_db.num_partitions_1234_blocks_per_partition_1'
    result = self.client.execute("show table stats %s" % full_tbl_name)
    # Should list all 1,234 partitions + 1 result for the summary
    assert len(result.data) == 1234 + 1

    count = self.execute_scalar("select count(*) from %s" % full_tbl_name)
    assert int(count) == 1234

    # Should still get the same results after a refresh or invalidate
    self.client.execute("refresh %s" % full_tbl_name)
    result = self.client.execute("show table stats %s" % full_tbl_name)
    assert len(result.data) == 1234 + 1

    self.client.execute("invalidate metadata %s" % full_tbl_name)
    result = self.client.execute("show table stats %s" % full_tbl_name)
    assert len(result.data) == 1234 + 1

  def test_predicates_on_partition_attributes(self, vector):
    # Test predicate evaluation on partition columns for a table with more
    # than 1024 partitions (see IMPALA-887)
    full_tbl_name = 'scale_db.num_partitions_1234_blocks_per_partition_1'
    # Binary predicate
    result = self.client.execute("select * from %s where j = 1" % full_tbl_name)
    assert len(result.data) == 1

    # Compound predicate with OR
    result = self.client.execute(
        "select * from %s where j = 1 or j = 2" % full_tbl_name)
    assert len(result.data) == 2

    # Conjunction of binary predicates
    result = self.client.execute(
        "select * from %s where j = 1 and j = 2" % full_tbl_name)
    assert len(result.data) == 0

    # TODO: Insert some data into an existing partition and validate it can be
    # read once IMPALA-624 is committed.
